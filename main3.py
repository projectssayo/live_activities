import asyncio
import os
from datetime import datetime, timezone
from typing import Dict, List, Optional, Set

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from pydantic import BaseModel
from pymongo import MongoClient, ReturnDocument


MONGO_USERNAME = "suyognegi_global"
MONGO_PASSWORD = "Oj5eGphIUUud9YvY"

url = (
    f"mongodb+srv://{MONGO_USERNAME}:{MONGO_PASSWORD}"
    f"@cluster0.hzyekeb.mongodb.net/?appName=Cluster0"
)

client = MongoClient(
    url,
    serverSelectionTimeoutMS=10000,
    connectTimeoutMS=10000,
    socketTimeoutMS=10000,
)

# ---- live_activities db ----------------------------------------------
db = client["live_activities"]
last_seen_col = db["last_seen"]
last_clicked_col = db["last_clicked_on_table"]
logged_in_col = db["logged_in_at"]

# ---- user_db (friend / block / request lists) -------------------------
user_db = client["user_db"]
all_type_list_col = user_db["all_type_list_table"]

app = FastAPI(title="Zyro Live Activity Service")

connected_users: Dict[str, WebSocket] = {}
socket_mac: Dict[str, str] = {}

# watchers[X] = set of emails that have X in their friend_list and are
# currently connected. Used to broadcast X's presence changes to everyone
# who cares, not just whoever has a chat window open with X.
watchers: Dict[str, Set[str]] = {}
# friend_list cache per connected user, so we can tear down `watchers`
# cleanly on disconnect without re-querying mongo.
friend_list_cache: Dict[str, List[str]] = {}

HEARTBEAT_IDLE_TIMEOUT = 2.0
HEARTBEAT_PING_TIMEOUT = 1.0


@app.on_event("startup")
def strip_legacy_presence_fields():
    last_seen_col.update_many({}, {"$unset": {"online": "", "last_seen": ""}})


def sanitize_email(email: str) -> str:
    return email.replace(".", "dot").replace("@", "at")


def get_chat_id(email1: str, email2: str) -> str:
    if email1 < email2:
        return f"chat_{email2}_{email1}"
    return f"chat_{email1}_{email2}"


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


LEGACY_PRESENCE_FIELDS = {"online": "", "last_seen": ""}


def set_user_online(email: str) -> None:
    last_seen_col.update_one(
        {"_id": email},
        {"$set": {"is_online": True, "last_seen_at": now_utc()}, "$unset": LEGACY_PRESENCE_FIELDS},
        upsert=True,
    )


def set_user_offline(email: str, at: Optional[datetime] = None) -> dict:
    ts = at or now_utc()
    return last_seen_col.find_one_and_update(
        {"_id": email},
        {
            "$set": {"is_online": False, "last_seen_at": ts, "user_is_on": None},
            "$unset": LEGACY_PRESENCE_FIELDS,
        },
        upsert=True,
        return_document=ReturnDocument.AFTER,
    )


def set_user_chat_target(email: str, target_email: Optional[str]) -> None:
    last_seen_col.update_one({"_id": email}, {"$set": {"user_is_on": target_email}}, upsert=True)


def touch_last_clicked(email: str, target_email: str, at: Optional[datetime] = None) -> None:
    chat_id = get_chat_id(email, target_email)
    field = sanitize_email(email)
    other_field = sanitize_email(target_email)

    last_clicked_col.update_one({"_id": chat_id}, {"$set": {field: at or now_utc()}}, upsert=True)
    last_clicked_col.update_one(
        {"_id": chat_id, other_field: {"$exists": False}}, {"$set": {other_field: None}}
    )


def record_login(email: str, mac_id: str) -> None:
    logged_in_col.update_one(
        {"_id": email}, {"$set": {"logged_in_at": now_utc(), "mac_id": mac_id}}, upsert=True
    )


def get_registered_mac(email: str) -> Optional[str]:
    doc = logged_in_col.find_one({"_id": email})
    return doc.get("mac_id") if doc else None


def get_friend_list(email: str) -> List[str]:
    doc = all_type_list_col.find_one({"_id": email}, {"friend_list": 1})
    return doc.get("friend_list", []) if doc else []


# ---- watcher bookkeeping ----------------------------------------------

def register_watchers(email: str, friends: List[str]) -> None:
    """email is now connected and watching each of `friends` for presence."""
    friend_list_cache[email] = friends
    for f in friends:
        watchers.setdefault(f, set()).add(email)


def unregister_watchers(email: str) -> None:
    for f in friend_list_cache.pop(email, []):
        s = watchers.get(f)
        if s is not None:
            s.discard(email)
            if not s:
                watchers.pop(f, None)


def refresh_watchers(email: str) -> None:
    """Re-pull friend_list from mongo, e.g. after a friend is added while
    the user is already connected, so new friends start being watched."""
    unregister_watchers(email)
    register_watchers(email, get_friend_list(email))


async def notify_peer(peer_email: str, changed_email: str) -> None:
    peer_ws = connected_users.get(peer_email)
    if peer_ws is None:
        return
    doc = last_seen_col.find_one({"_id": changed_email}) or {}
    last_seen_at = doc.get("last_seen_at")

    # "when did changed_email (the friend, from peer_email's point of view)
    # last click into this chat" -- read straight from last_clicked_on_table
    # using the same chat_id / sanitized-field scheme as touch_last_clicked.
    chat_id = get_chat_id(peer_email, changed_email)
    clicked_doc = last_clicked_col.find_one({"_id": chat_id}) or {}
    changed_field = sanitize_email(changed_email)
    friend_last_clicked = clicked_doc.get(changed_field)  # None if never clicked

    payload = {
        "type": "presence_update",
        "email": changed_email,
        "is_online": doc.get("is_online", False),
        "user_is_on": doc.get("user_is_on"),
        "last_seen_at": last_seen_at.isoformat() if last_seen_at else None,
        "friend_last_clicked": friend_last_clicked.isoformat() if friend_last_clicked else None,
    }
    try:
        await peer_ws.send_json(payload)
    except Exception:
        pass


async def broadcast_presence(changed_email: str) -> None:
    """Notify every connected user who has `changed_email` as a friend."""
    for watcher_email in list(watchers.get(changed_email, ())):
        await notify_peer(watcher_email, changed_email)


async def handle_event(email: str, data: dict) -> None:
    event_type = data.get("type")

    if event_type == "opened_chat":
        target_email = data.get("target_email")
        if not target_email:
            return
        set_user_chat_target(email, target_email)
        touch_last_clicked(email, target_email)
        await notify_peer(target_email, email)

    elif event_type == "closed_chat":
        prev = last_seen_col.find_one({"_id": email}) or {}
        target_email = prev.get("user_is_on")
        set_user_chat_target(email, None)
        if target_email:
            touch_last_clicked(email, target_email)
            await notify_peer(target_email, email)

    elif event_type == "refresh_friend_list":
        # client tells us its friend_list changed (new friend accepted etc.)
        refresh_watchers(email)
        # send back current presence snapshot for the (possibly new) friends
        friends = friend_list_cache.get(email, [])
        docs = last_seen_col.find({"_id": {"$in": friends}})
        snapshot = []
        for d in docs:
            ts = d.get("last_seen_at")
            snapshot.append(
                {
                    "email": d["_id"],
                    "is_online": d.get("is_online", False),
                    "user_is_on": d.get("user_is_on"),
                    "last_seen_at": ts.isoformat() if ts else None,
                }
            )
        ws = connected_users.get(email)
        if ws is not None:
            try:
                await ws.send_json({"type": "presence_snapshot", "friends": snapshot})
            except Exception:
                pass


async def cleanup_user(email: str) -> None:
    if connected_users.get(email) is None and socket_mac.get(email) is None:
        return

    connected_users.pop(email, None)
    socket_mac.pop(email, None)
    unregister_watchers(email)

    prev = last_seen_col.find_one({"_id": email}) or {}
    target_email = prev.get("user_is_on")

    ts = now_utc()
    set_user_offline(email, at=ts)

    if target_email:
        touch_last_clicked(email, target_email, at=ts)
        await notify_peer(target_email, email)

    # anyone who has this user as a friend also needs to know they dropped
    await broadcast_presence(email)


@app.websocket("/ws/{email}/{mac_id}")
async def websocket_endpoint(websocket: WebSocket, email: str, mac_id: str):
    await websocket.accept()

    connected_users[email] = websocket
    socket_mac[email] = mac_id

    set_user_online(email)
    record_login(email, mac_id)

    # start watching this user's friends, and tell those friends (if online)
    # that this user just came online.
    register_watchers(email, get_friend_list(email))
    await broadcast_presence(email)

    try:
        while True:
            try:
                data = await asyncio.wait_for(
                    websocket.receive_json(), timeout=HEARTBEAT_IDLE_TIMEOUT
                )
            except asyncio.TimeoutError:
                try:
                    await websocket.send_json({"type": "ping"})
                    reply = await asyncio.wait_for(
                        websocket.receive_json(), timeout=HEARTBEAT_PING_TIMEOUT
                    )
                except Exception:
                    break
                if reply.get("type") != "pong":
                    await handle_event(email, reply)
                continue

            if data.get("type") == "pong":
                continue

            await handle_event(email, data)
    except WebSocketDisconnect:
        pass
    except Exception:
        pass
    finally:
        await cleanup_user(email)


@app.get("/")
@app.head("/")
def root():
    return {"success": True}


@app.get("/status")
def get_status(me: str, friend: str):
    friend_doc = last_seen_col.find_one({"_id": friend}) or {}
    chat_id = get_chat_id(me, friend)
    clicked_doc = last_clicked_col.find_one({"_id": chat_id}) or {}

    me_field = sanitize_email(me)
    friend_field = sanitize_email(friend)

    my_last_clicked = clicked_doc[me_field] if me_field in clicked_doc else None
    friend_last_clicked = clicked_doc[friend_field] if friend_field in clicked_doc else None

    return {
        "friend_email": friend,
        "is_online": friend_doc.get("is_online", False),
        "user_is_on": friend_doc.get("user_is_on"),
        "last_seen_at": friend_doc.get("last_seen_at"),
        "is_on_same_chat_as_me": friend_doc.get("user_is_on") == me,
        "my_last_clicked": my_last_clicked,
        "friend_last_clicked": friend_last_clicked,
    }


@app.get("/friend_presence_bulk")
def friend_presence_bulk(me: str):
    """Full presence snapshot for every entry in `me`'s friend_list.
    Used by the client to seed/repair its local SQLite cache on startup
    or after a period offline."""
    friends = get_friend_list(me)
    docs = {d["_id"]: d for d in last_seen_col.find({"_id": {"$in": friends}})}
    me_field_for_chat = {f: get_chat_id(me, f) for f in friends}
    clicked_docs = {
        d["_id"]: d
        for d in last_clicked_col.find({"_id": {"$in": list(me_field_for_chat.values())}})
    }
    out = []
    for f in friends:
        d = docs.get(f, {})
        ts = d.get("last_seen_at")
        clicked_doc = clicked_docs.get(me_field_for_chat[f], {})
        friend_last_clicked = clicked_doc.get(sanitize_email(f))
        out.append(
            {
                "email": f,
                "is_online": d.get("is_online", False),
                "user_is_on": d.get("user_is_on"),
                "last_seen_at": ts.isoformat() if ts else None,
                "friend_last_clicked": friend_last_clicked.isoformat() if friend_last_clicked else None,
            }
        )
    return {"friends": out}


class ClickRecord(BaseModel):
    friend_email: str
    clicked_at: str  # ISO-8601 UTC string


class SyncClicksRequest(BaseModel):
    me: str
    clicks: List[ClickRecord]


@app.post("/sync_clicked")
def sync_clicked(payload: SyncClicksRequest):
    """Batch endpoint the client's SQLite sync thread calls to push
    click records that were made while offline (or just to keep mongo
    current). Idempotent: re-pushing the same record is harmless."""
    results = []
    for rec in payload.clicks:
        try:
            at = datetime.fromisoformat(rec.clicked_at)
        except ValueError:
            results.append({"friend_email": rec.friend_email, "ok": False, "error": "bad timestamp"})
            continue
        touch_last_clicked(payload.me, rec.friend_email, at=at)
        results.append({"friend_email": rec.friend_email, "ok": True})
    return {"results": results}


@app.get("/check_mac/{email}")
def check_mac(email: str, mac_id: str):
    registered = get_registered_mac(email)
    return {"match": registered == mac_id, "registered_mac": registered}


@app.get("/health")
def health():
    return {"ok": True}
