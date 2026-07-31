"""
live_activity_api.py
---------------------------------------------------------------------------
Deployable FastAPI service that tracks:
  - who is online / offline
  - which friend-chat a user currently has open ("user_is_on")
  - last-seen timestamps
  - per-chat "last clicked" timestamps (per participant)
  - which mac_id is currently authorised for a given account (multi-login
    detection)

DB: live_activites
Collections:
  last_seen             _id=email  {is_online, user_is_on, last_seen_at}
  last_clicked_on_table _id=chat{...} {<sanitized_email>: datetime, ...}
  logged_in_at          _id=email  {logged_in_at, mac_id}

Run locally:
    pip install fastapi "uvicorn[standard]" pymongo python-dotenv dnspython
    uvicorn live_activity_api:app --host 0.0.0.0 --port 8000

.env (same folder) should contain:
    MONGO_USERNAME=your_mongo_user
    MONGO_PASSWORD=your_mongo_password

Deploy behind your reverse proxy at:
    https://www.zyro.chat.just.co
so that:
    REST         ->  https://www.zyro.chat.just.co/...
    WebSocket    ->  wss://www.zyro.chat.just.co/ws/{email}/{mac_id}
---------------------------------------------------------------------------
"""

import os
from datetime import datetime, timezone
from typing import Dict, Optional


from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from pymongo import MongoClient, ReturnDocument


# ---------------------------------------------------------------------------
# Config / Mongo connection
# ---------------------------------------------------------------------------
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

db = client["live_activites"]
last_seen_col = db["last_seen"]
last_clicked_col = db["last_clicked_on_table"]
logged_in_col = db["logged_in_at"]

app = FastAPI(title="Zyro Live Activity Service")

# email -> live websocket connection (single active session per email)
connected_users: Dict[str, WebSocket] = {}
# email -> mac_id currently bound to the live socket
socket_mac: Dict[str, str] = {}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def sanitize_email(email: str) -> str:
    """Mongo field names can't contain '.' -- make an email field-safe."""
    return email.replace(".", "dot").replace("@", "at")


def get_chat_id(email1: str, email2: str) -> str:
    """Canonical, order-stable id for a 1-1 chat between two emails."""
    e1 = sanitize_email(email1)
    e2 = sanitize_email(email2)
    if email1 < email2:
        return f"chat{e2}_{e1}"
    return f"chat{e1}_{e2}"


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# Mongo writers (all upserts, as requested)
# ---------------------------------------------------------------------------
def set_user_online(email: str) -> None:
    last_seen_col.update_one(
        {"_id": email},
        {"$set": {"is_online": True, "last_seen_at": now_utc()}},
        upsert=True,
    )


def set_user_offline(email: str) -> dict:
    return last_seen_col.find_one_and_update(
        {"_id": email},
        {"$set": {"is_online": False, "last_seen_at": now_utc(), "user_is_on": None}},
        upsert=True,
        return_document=ReturnDocument.AFTER,
    )


def set_user_chat_target(email: str, target_email: Optional[str]) -> None:
    last_seen_col.update_one(
        {"_id": email},
        {"$set": {"user_is_on": target_email}},
        upsert=True,
    )


def touch_last_clicked(email: str, target_email: str) -> None:
    """Stamp `email`'s side of the 1-1 chat with `target_email` as 'now'."""
    chat_id = get_chat_id(email, target_email)
    field = sanitize_email(email)
    last_clicked_col.update_one(
        {"_id": chat_id},
        {"$set": {field: now_utc()}},
        upsert=True,
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


# ---------------------------------------------------------------------------
# WebSocket: presence + chat-open/close events
# ---------------------------------------------------------------------------
async def notify_peer(peer_email: str, changed_email: str) -> None:
    """Push changed_email's fresh presence doc to peer_email if connected."""
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

    # unknown event types are ignored on purpose -- keep this forward-compatible


async def cleanup_user(email: str) -> None:
    """Runs on clean disconnect AND on crash/dead-socket detection alike."""
    connected_users.pop(email, None)
    socket_mac.pop(email, None)

    prev = last_seen_col.find_one({"_id": email}) or {}
    target_email = prev.get("user_is_on")

    set_user_offline(email)

    if target_email:
        touch_last_clicked(email, target_email)
        await notify_peer(target_email, email)


@app.websocket("/ws/{email}/{mac_id}")
async def websocket_endpoint(websocket: WebSocket, email: str, mac_id: str):
    await websocket.accept()

    # if this email already has a live socket (e.g. reconnect race), the old
    # one will error out naturally on its next send and hit cleanup_user()
    connected_users[email] = websocket
    socket_mac[email] = mac_id

    set_user_online(email)
    record_login(email, mac_id)

    try:
        while True:
            # `websockets` (the library uvicorn uses under the hood) sends/expects
            # protocol-level ping/pong automatically and raises a close/error
            # here the moment the peer goes dark -- no custom heartbeat needed.
            data = await websocket.receive_json()
            await handle_event(email, data)
    except WebSocketDisconnect:
        await cleanup_user(email)
    except Exception:
        # covers crashed clients / dropped connections detected by ping/pong
        await cleanup_user(email)


# ---------------------------------------------------------------------------
# REST endpoints
# ---------------------------------------------------------------------------
@app.get("/status")
def get_status(me: str, friend: str):
    """Everything the UI needs to render a friend's presence + chat context."""
    friend_doc = last_seen_col.find_one({"_id": friend}) or {}
    chat_id = get_chat_id(me, friend)
    clicked_doc = last_clicked_col.find_one({"_id": chat_id}) or {}

    me_field = sanitize_email(me)
    friend_field = sanitize_email(friend)

    return {
        "friend_email": friend,
        "is_online": friend_doc.get("is_online", False),
        "user_is_on": friend_doc.get("user_is_on"),
        "last_seen_at": friend_doc.get("last_seen_at"),
        "is_on_same_chat_as_me": friend_doc.get("user_is_on") == me,
        "my_last_clicked": clicked_doc.get(me_field),
        "friend_last_clicked": clicked_doc.get(friend_field),
    }


@app.get("/check_mac/{email}")
def check_mac(email: str, mac_id: str):
    """Used by the client's mac-watcher thread to detect another login."""
    registered = get_registered_mac(email)
    return {"match": registered == mac_id, "registered_mac": registered}


@app.get("/health")
def health():
    return {"ok": True}
