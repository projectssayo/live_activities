import asyncio
import os
from datetime import datetime, timezone
from typing import Dict, List, Optional


from fastapi import FastAPI, WebSocket, WebSocketDisconnect
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

# ---- presence db (unchanged) ----
db = client["live_activities"]
last_seen_col = db["last_seen"]
last_clicked_col = db["last_clicked_on_table"]
logged_in_col = db["logged_in_at"]

# ---- friend-list db, used to figure out who to notify on presence change ----
user_db = client["user_db"]
all_type_list_col = user_db["all_type_list_table"]

app = FastAPI(title="Zyro Live Activity Service")

connected_users: Dict[str, WebSocket] = {}
socket_mac: Dict[str, str] = {}

HEARTBEAT_IDLE_TIMEOUT = 2.0   # seconds of silence before we actively probe the socket
HEARTBEAT_PING_TIMEOUT = 1.0   # seconds to wait for any reply to that probe


@app.on_event("startup")
def strip_legacy_presence_fields():
    last_seen_col.update_many(
        {},
        {"$unset": {"online": "", "last_seen": ""}},
    )
    # speeds up "who has this email in their friend_list" lookups used by
    # broadcast_presence_to_friends() below, which runs on every
    # connect/disconnect, not just on chat-open/close.
    all_type_list_col.create_index("friend_list")


def sanitize_email(email: str) -> str:
    return email.replace(".", "dot").replace("@", "at")


def get_chat_id(email1: str, email2: str) -> str:
    if email1 < email2:
        return f"chat_{email2}_{email1}"
    return f"chat_{email1}_{email2}"


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


# Legacy field names from an older version of this service. If any older
# process is still writing "online" / "last_seen" to this same collection,
# those fields go stale and disagree with "is_online" / "last_seen_at"
# (which this file actually maintains). We proactively $unset them on
# every write we make so documents self-heal back to the current schema
# instead of keeping two conflicting presence fields forever.
LEGACY_PRESENCE_FIELDS = {"online": "", "last_seen": ""}


def set_user_online(email: str) -> None:
    last_seen_col.update_one(
        {"_id": email},
        {
            "$set": {"is_online": True, "last_seen_at": now_utc()},
            "$unset": LEGACY_PRESENCE_FIELDS,
        },
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
    last_seen_col.update_one(
        {"_id": email},
        {"$set": {"user_is_on": target_email}},
        upsert=True,
    )


def touch_last_clicked(email: str, target_email: str, at: Optional[datetime] = None) -> None:
    chat_id = get_chat_id(email, target_email)
    field = sanitize_email(email)
    other_field = sanitize_email(target_email)

    last_clicked_col.update_one(
        {"_id": chat_id},
        {"$set": {field: at or now_utc()}},
        upsert=True,
    )

    last_clicked_col.update_one(
        {"_id": chat_id, other_field: {"$exists": False}},
        {"$set": {other_field: None}},
    )


def record_login(email: str, mac_id: str) -> None:
    logged_in_col.update_one(
        {"_id": email},
        {"$set": {"logged_in_at": now_utc(), "mac_id": mac_id}},
        upsert=True,
    )


def get_registered_mac(email: str) -> Optional[str]:
    doc = logged_in_col.find_one({"_id": email})
    return doc.get("mac_id") if doc else None


# ---------------------------------------------------------------------
# friend-list aware broadcast
# ---------------------------------------------------------------------

def get_watchers_of(email: str) -> List[str]:
    """Return every user who has `email` in their own friend_list, i.e.
    everyone who should be told when `email`'s presence changes."""
    cursor = all_type_list_col.find({"friend_list": email}, {"_id": 1})
    return [doc["_id"] for doc in cursor]


def _presence_payload(changed_email: str) -> dict:
    doc = last_seen_col.find_one({"_id": changed_email}) or {}
    last_seen_at = doc.get("last_seen_at")
    return {
        "type": "presence_update",
        "email": changed_email,
        "is_online": doc.get("is_online", False),
        "user_is_on": doc.get("user_is_on"),
        "last_seen_at": last_seen_at.isoformat() if last_seen_at else None,
    }


async def notify_peer(peer_email: str, changed_email: str) -> None:
    """Notify a single peer (used for chat-open/close 'user_is_on' updates)."""
    peer_ws = connected_users.get(peer_email)
    if peer_ws is None:
        return
    try:
        await peer_ws.send_json(_presence_payload(changed_email))
    except Exception:
        pass


async def broadcast_presence_to_friends(changed_email: str) -> None:
    """Notify every connected user who has `changed_email` as a friend that
    their is_online / last_seen_at (and current user_is_on) just changed.
    Called whenever `changed_email` connects, disconnects, or its presence
    doc is otherwise updated -- not just when someone opens/closes a chat."""
    watchers = get_watchers_of(changed_email)
    if not watchers:
        return
    payload = _presence_payload(changed_email)
    for watcher_email in watchers:
        ws = connected_users.get(watcher_email)
        if ws is None:
            continue
        try:
            await ws.send_json(payload)
        except Exception:
            pass


# ---------------------------------------------------------------------
# NEW: bulk snapshot reply, used the instant a client's socket connects
# so it doesn't have to wait for the next presence change / next poll
# cycle to find out where its friends currently stand.
# ---------------------------------------------------------------------
async def send_bulk_presence(requester_email: str, friend_emails: List[str]) -> None:
    ws = connected_users.get(requester_email)
    if ws is None or not friend_emails:
        return

    docs = last_seen_col.find({"_id": {"$in": friend_emails}})
    updates = []
    for doc in docs:
        last_seen_at = doc.get("last_seen_at")
        updates.append({
            "email": doc["_id"],
            "is_online": doc.get("is_online", False),
            "user_is_on": doc.get("user_is_on"),
            "last_seen_at": last_seen_at.isoformat() if last_seen_at else None,
        })

    try:
        await ws.send_json({"type": "bulk_presence", "updates": updates})
    except Exception:
        pass


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

    # NEW: client asks "give me my friends' current status right now" --
    # sent right after the socket connects so the UI doesn't sit stale
    # waiting on the next change event or the periodic sqlite re-sync.
    elif event_type == "sync_request":
        friend_emails = data.get("friend_list") or []
        await send_bulk_presence(email, friend_emails)


async def cleanup_user(email: str) -> None:
    if connected_users.get(email) is None and socket_mac.get(email) is None:
        return

    connected_users.pop(email, None)
    socket_mac.pop(email, None)

    prev = last_seen_col.find_one({"_id": email}) or {}
    target_email = prev.get("user_is_on")

    ts = now_utc()
    set_user_offline(email, at=ts)

    if target_email:
        touch_last_clicked(email, target_email, at=ts)
        await notify_peer(target_email, email)

    # tell everyone who has this user as a friend that they just went offline
    await broadcast_presence_to_friends(email)


@app.websocket("/ws/{email}/{mac_id}")
async def websocket_endpoint(websocket: WebSocket, email: str, mac_id: str):
    await websocket.accept()

    connected_users[email] = websocket
    socket_mac[email] = mac_id

    set_user_online(email)
    record_login(email, mac_id)

    # tell everyone who has this user as a friend that they just came online
    await broadcast_presence_to_friends(email)

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


# ---------------------------------------------------------------------
# bulk REST fetch used by the client's LastSeenSyncThread as a periodic
# safety-net reconciliation (the websocket sync_request/bulk_presence
# path above is now the primary, low-latency path).
# ---------------------------------------------------------------------
@app.get("/friends_last_seen")
def friends_last_seen(emails: str):
    """
    emails: comma-separated list of friend emails, e.g.
      /friends_last_seen?emails=a@x.com,b@x.com,c@x.com

    Returns: { "<email>": {"is_online": bool, "last_seen_at": iso_str|None}, ... }
    Only emails that exist in the last_seen collection are included.
    """
    email_list = [e.strip() for e in emails.split(",") if e.strip()]
    if not email_list:
        return {}

    docs = last_seen_col.find({"_id": {"$in": email_list}})
    result = {}
    for doc in docs:
        last_seen_at = doc.get("last_seen_at")
        result[doc["_id"]] = {
            "is_online": doc.get("is_online", False),
            "last_seen_at": last_seen_at.isoformat() if last_seen_at else None,
        }
    return result


@app.get("/check_mac/{email}")
def check_mac(email: str, mac_id: str):
    registered = get_registered_mac(email)
    return {"match": registered == mac_id, "registered_mac": registered}


@app.get("/health")
def health():
    return {"ok": True}
