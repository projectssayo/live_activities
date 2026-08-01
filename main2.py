import asyncio
import os
from datetime import datetime, timezone
from typing import Dict, Optional


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

db = client["live_activities"]
last_seen_col = db["last_seen"]
last_clicked_col = db["last_clicked_on_table"]
logged_in_col = db["logged_in_at"]

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


async def notify_peer(peer_email: str, changed_email: str) -> None:
    peer_ws = connected_users.get(peer_email)
    if peer_ws is None:
        return
    doc = last_seen_col.find_one({"_id": changed_email}) or {}
    last_seen_at = doc.get("last_seen_at")
    payload = {
        "type": "presence_update",
        "email": changed_email,
        "is_online": doc.get("is_online", False),
        "user_is_on": doc.get("user_is_on"),
        "last_seen_at": last_seen_at.isoformat() if last_seen_at else None,
    }
    try:
        await peer_ws.send_json(payload)
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


@app.websocket("/ws/{email}/{mac_id}")
async def websocket_endpoint(websocket: WebSocket, email: str, mac_id: str):
    await websocket.accept()

    connected_users[email] = websocket
    socket_mac[email] = mac_id

    set_user_online(email)
    record_login(email, mac_id)

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


@app.get("/check_mac/{email}")
def check_mac(email: str, mac_id: str):
    registered = get_registered_mac(email)
    return {"match": registered == mac_id, "registered_mac": registered}


@app.get("/health")
def health():
    return {"ok": True}
