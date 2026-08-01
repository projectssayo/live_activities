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

DB: live_activities
Collections:
  last_seen             _id=email  {is_online, user_is_on, last_seen_at}
  last_clicked_on_table _id=chat_{email1}_{email2} {<sanitized_email>: datetime, ...}
  logged_in_at          _id=email  {logged_in_at, mac_id}

Run locally:
    pip install fastapi "uvicorn[standard]" pymongo python-dotenv dnspython
    uvicorn live_activity_api:app --host 0.0.0.0 --port 8000

.env (same folder) should contain:
    MONGO_USERNAME=your_mongo_user
    MONGO_PASSWORD=your_mongo_password

Deploy at:
    https://zyro-main-app-live-activities-api.onrender.com
so that:
    REST         ->  https://zyro-main-app-live-activities-api.onrender.com/...
    WebSocket    ->  wss://zyro-main-app-live-activities-api.onrender.com/ws/{email}/{mac_id}
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

db = client["live_activities"]
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
    """Mongo field *names* (keys inside a document) can't contain '.' --
    make an email field-safe for use as a document key. NOT used for the
    chat_id itself, since _id values are free-form strings and can contain
    '.' and '@' just fine."""
    return email.replace(".", "dot").replace("@", "at")


def get_chat_id(email1: str, email2: str) -> str:
    """Canonical, order-stable, human-readable id for a 1-1 chat between
    two emails, e.g. chat_suyognegi1@gmail.com_dacida3565@fishnon.com"""
    if email1 < email2:
        return f"chat_{email2}_{email1}"
    return f"chat_{email1}_{email2}"


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


def set_user_offline(email: str, at: Optional[datetime] = None) -> dict:
    """Flip is_online False as fast as possible. `at` lets the caller pin
    this write to the exact same timestamp used elsewhere (e.g. the matching
    last_clicked stamp), so the two stay perfectly in sync."""
    ts = at or now_utc()
    return last_seen_col.find_one_and_update(
        {"_id": email},
        {"$set": {"is_online": False, "last_seen_at": ts, "user_is_on": None}},
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
    """Stamp `email`'s side of the 1-1 chat with `target_email`.
    Pass `at` to reuse an exact timestamp already generated elsewhere
    (e.g. the same instant the user was marked offline) so the two
    records agree precisely instead of drifting by a few ms.

    Also guarantees the chat document always has a field for BOTH
    participants: if `target_email`'s field isn't present on this
    document yet, it's created and initialized to None (null) instead
    of being left missing.
    """
    chat_id = get_chat_id(email, target_email)
    field = sanitize_email(email)
    other_field = sanitize_email(target_email)

    last_clicked_col.update_one(
        {"_id": chat_id},
        {"$set": {field: at or now_utc()}},
        upsert=True,
    )
    # Only initialize the other participant's field if it doesn't already
    # exist -- never clobber a real timestamp they already have.
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
    """Runs on clean disconnect AND on crash/dead-socket detection alike.

    Ordering matters here for both speed and accuracy:
      1. Drop the in-memory socket refs immediately (no DB round trip).
      2. Flip is_online -> False right away -- this is the single most
         important, time-sensitive write, so it goes out first and on
         its own, before anything else touches the DB.
      3. Only after that, backfill the last_clicked table for whichever
         chat the user had open, using the *exact same timestamp* that
         was just written to last_seen_at, so "last seen" and "last
         clicked" agree to the microsecond instead of two separate
         now_utc() calls drifting apart.
    """
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
@app.get("/")
@app.head("/")
def root():
    """Simple liveness/ping endpoint (also handy for uptime monitors / Render health checks)."""
    return {"success": True}


@app.get("/status")
def get_status(me: str, friend: str):
    """Everything the UI needs to render a friend's presence + chat context."""
    friend_doc = last_seen_col.find_one({"_id": friend}) or {}
    chat_id = get_chat_id(me, friend)
    clicked_doc = last_clicked_col.find_one({"_id": chat_id}) or {}

    me_field = sanitize_email(me)
    friend_field = sanitize_email(friend)

    # Explicit: if this participant has never clicked into this chat yet,
    # their field simply won't exist in clicked_doc -- make sure that comes
    # back as a clean None rather than relying on an implicit default.
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
    """Used by the client's mac-watcher thread to detect another login."""
    registered = get_registered_mac(email)
    return {"match": registered == mac_id, "registered_mac": registered}


@app.get("/health")
def health():
    return {"ok": True}
