import asyncio
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from typing import Dict, List, Optional, Set

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

user_db = client["user_db"]
all_type_list_col = user_db["all_type_list_table"]

app = FastAPI(title="Zyro Live Activity Service")

connected_users: Dict[str, WebSocket] = {}
socket_mac: Dict[str, str] = {}

HEARTBEAT_IDLE_TIMEOUT = 2.0
HEARTBEAT_PING_TIMEOUT = 1.0

# ---------------------------------------------------------------------
# NEW: everything the hot (broadcast) path needs lives in memory.
# pymongo is a *blocking* driver -- calling it directly inside an async
# def freezes the single-threaded event loop, which stalls delivery to
# EVERY connected socket, not just the two users involved in that one
# event. These two structures are the fix: they let connect/disconnect/
# chat events be handled with zero Mongo round trips on the hot path.
# ---------------------------------------------------------------------

# email -> {"is_online": bool, "last_seen_at": iso_str|None, "user_is_on": str|None}
presence_state: Dict[str, dict] = {}

# email -> set of emails who have this email in THEIR friend_list, i.e.
# "who needs to be told when this email's presence changes"
friend_watchers: Dict[str, Set[str]] = {}

# All Mongo calls run here so they never block the event loop.
EXECUTOR = ThreadPoolExecutor(max_workers=8)


def run_blocking(fn, *args, **kwargs):
    loop = asyncio.get_event_loop()
    return loop.run_in_executor(EXECUTOR, lambda: fn(*args, **kwargs))


def sanitize_email(email: str) -> str:
    return email.replace(".", "dot").replace("@", "at")


def get_chat_id(email1: str, email2: str) -> str:
    if email1 < email2:
        return f"chat_{email2}_{email1}"
    return f"chat_{email1}_{email2}"


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


LEGACY_PRESENCE_FIELDS = {"online": "", "last_seen": ""}


# ---------------------------------------------------------------------
# blocking Mongo functions -- ONLY ever called via run_blocking()
# ---------------------------------------------------------------------

def _strip_legacy_presence_fields_blocking():
    last_seen_col.update_many({}, {"$unset": LEGACY_PRESENCE_FIELDS})


def _create_friend_list_index_blocking():
    all_type_list_col.create_index("friend_list")


def _persist_user_online_blocking(email: str, ts: datetime):
    last_seen_col.update_one(
        {"_id": email},
        {"$set": {"is_online": True, "last_seen_at": ts}, "$unset": LEGACY_PRESENCE_FIELDS},
        upsert=True,
    )


def _persist_user_offline_blocking(email: str, ts: datetime):
    last_seen_col.update_one(
        {"_id": email},
        {
            "$set": {"is_online": False, "last_seen_at": ts, "user_is_on": None},
            "$unset": LEGACY_PRESENCE_FIELDS,
        },
        upsert=True,
    )


def _persist_chat_target_blocking(email: str, target_email: Optional[str]):
    last_seen_col.update_one(
        {"_id": email}, {"$set": {"user_is_on": target_email}}, upsert=True
    )


def _persist_last_clicked_blocking(email: str, target_email: str, ts: datetime):
    chat_id = get_chat_id(email, target_email)
    field = sanitize_email(email)
    other_field = sanitize_email(target_email)
    last_clicked_col.update_one({"_id": chat_id}, {"$set": {field: ts}}, upsert=True)
    last_clicked_col.update_one(
        {"_id": chat_id, other_field: {"$exists": False}},
        {"$set": {other_field: None}},
    )


def _record_login_blocking(email: str, mac_id: str, ts: datetime):
    logged_in_col.update_one(
        {"_id": email}, {"$set": {"logged_in_at": ts, "mac_id": mac_id}}, upsert=True
    )


def _get_registered_mac_blocking(email: str) -> Optional[str]:
    doc = logged_in_col.find_one({"_id": email})
    return doc.get("mac_id") if doc else None


def _load_all_presence_blocking() -> Dict[str, dict]:
    result = {}
    for doc in last_seen_col.find({}):
        last_seen_at = doc.get("last_seen_at")
        result[doc["_id"]] = {
            "is_online": doc.get("is_online", False),
            "last_seen_at": last_seen_at.isoformat() if last_seen_at else None,
            "user_is_on": doc.get("user_is_on"),
        }
    return result


def _load_missing_presence_blocking(emails: List[str]) -> Dict[str, dict]:
    result = {}
    for doc in last_seen_col.find({"_id": {"$in": emails}}):
        last_seen_at = doc.get("last_seen_at")
        result[doc["_id"]] = {
            "is_online": doc.get("is_online", False),
            "last_seen_at": last_seen_at.isoformat() if last_seen_at else None,
            "user_is_on": doc.get("user_is_on"),
        }
    return result


def _build_friend_watchers_blocking() -> Dict[str, Set[str]]:
    """For every user doc, invert friend_list into 'who watches whom'."""
    index: Dict[str, Set[str]] = {}
    for doc in all_type_list_col.find({}, {"friend_list": 1}):
        watcher = doc["_id"]
        for friend_email in doc.get("friend_list", []):
            index.setdefault(friend_email, set()).add(watcher)
    return index


def _get_status_blocking(me: str, friend: str) -> dict:
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


# ---------------------------------------------------------------------
# startup: warm the in-memory caches, then keep the friend graph fresh
# ---------------------------------------------------------------------

@app.on_event("startup")
async def startup_event():
    await run_blocking(_strip_legacy_presence_fields_blocking)
    await run_blocking(_create_friend_list_index_blocking)

    global presence_state, friend_watchers
    presence_state = await run_blocking(_load_all_presence_blocking)
    friend_watchers = await run_blocking(_build_friend_watchers_blocking)

    asyncio.create_task(_periodic_friend_graph_refresh())


async def _periodic_friend_graph_refresh():
    """friend_watchers is only rebuilt from Mongo periodically (adding a
    friend doesn't push a live update to this server). 20s keeps 'new
    friend added' -> 'live presence starts flowing' lag small without
    hammering Mongo. Call POST /refresh_friend_graph for an instant
    rebuild right after your app's add-friend flow completes."""
    global friend_watchers
    while True:
        await asyncio.sleep(20)
        try:
            friend_watchers = await run_blocking(_build_friend_watchers_blocking)
        except Exception:
            pass


@app.post("/refresh_friend_graph")
async def refresh_friend_graph():
    global friend_watchers
    friend_watchers = await run_blocking(_build_friend_watchers_blocking)
    return {"ok": True, "tracked_emails": len(friend_watchers)}


# ---------------------------------------------------------------------
# fast, non-blocking helpers used on the hot path
# ---------------------------------------------------------------------

async def safe_send(ws: Optional[WebSocket], payload: dict) -> None:
    if ws is None:
        return
    try:
        await ws.send_json(payload)
    except Exception:
        pass


async def broadcast_presence_to_friends(changed_email: str) -> None:
    watchers = friend_watchers.get(changed_email)
    if not watchers:
        return
    new_state = presence_state.get(changed_email, {})
    payload = {"type": "presence_update", "email": changed_email, **new_state}

    # debug: what changed and who's getting told
    print(f"[presence] {changed_email} -> {new_state} | notifying {len(watchers & connected_users.keys())} watchers")

    await asyncio.gather(
        *(safe_send(connected_users.get(w), payload) for w in watchers if w in connected_users),
        return_exceptions=True,
    )
    

async def notify_peer(peer_email: str, changed_email: str) -> None:
    payload = {"type": "presence_update", "email": changed_email, **presence_state.get(changed_email, {})}
    await safe_send(connected_users.get(peer_email), payload)


async def send_bulk_presence(requester_email: str, friend_emails: List[str]) -> None:
    """Served straight from presence_state -- no Mongo round trip in the
    common case. Only falls back to Mongo for emails we've genuinely
    never seen (e.g. a friend who hasn't connected since this process
    started), and caches the result for next time."""
    ws = connected_users.get(requester_email)
    if ws is None or not friend_emails:
        return

    updates = []
    missing = []
    for email in friend_emails:
        state = presence_state.get(email)
        if state is None:
            missing.append(email)
        else:
            updates.append({"email": email, **state})

    if missing:
        fetched = await run_blocking(_load_missing_presence_blocking, missing)
        for email, state in fetched.items():
            presence_state[email] = state
            updates.append({"email": email, **state})

    await safe_send(ws, {"type": "bulk_presence", "updates": updates})


# ---------------------------------------------------------------------
# state transitions: update memory immediately, persist to Mongo async
# (fire-and-forget) so nobody's notification ever waits on a DB write
# ---------------------------------------------------------------------

async def mark_user_online(email: str) -> None:
    ts = now_utc()
    prev = presence_state.get(email, {})
    presence_state[email] = {
        "is_online": True,
        "last_seen_at": ts.isoformat(),
        "user_is_on": prev.get("user_is_on"),
    }
    print(f"[presence] {email}: {prev} -> {presence_state[email]}")
    asyncio.create_task(run_blocking(_persist_user_online_blocking, email, ts))


async def mark_user_offline(email: str) -> None:
    ts = now_utc()
    presence_state[email] = {
        "is_online": False,
        "last_seen_at": ts.isoformat(),
        "user_is_on": None,
    }
    asyncio.create_task(run_blocking(_persist_user_offline_blocking, email, ts))


async def mark_chat_target(email: str, target_email: Optional[str]) -> None:
    state = presence_state.setdefault(email, {"is_online": True, "last_seen_at": None, "user_is_on": None})
    state["user_is_on"] = target_email
    asyncio.create_task(run_blocking(_persist_chat_target_blocking, email, target_email))


async def touch_last_clicked(email: str, target_email: str) -> None:
    ts = now_utc()
    asyncio.create_task(run_blocking(_persist_last_clicked_blocking, email, target_email, ts))


# ---------------------------------------------------------------------
# event handling
# ---------------------------------------------------------------------

async def handle_event(email: str, data: dict) -> None:
    event_type = data.get("type")

    if event_type == "opened_chat":
        target_email = data.get("target_email")
        if not target_email:
            return
        await mark_chat_target(email, target_email)
        await touch_last_clicked(email, target_email)
        await notify_peer(target_email, email)

    elif event_type == "closed_chat":
        target_email = presence_state.get(email, {}).get("user_is_on")
        await mark_chat_target(email, None)
        if target_email:
            await touch_last_clicked(email, target_email)
            await notify_peer(target_email, email)

    elif event_type == "sync_request":
        friend_emails = data.get("friend_list") or []
        await send_bulk_presence(email, friend_emails)


async def cleanup_user(email: str) -> None:
    if connected_users.get(email) is None and socket_mac.get(email) is None:
        return

    connected_users.pop(email, None)
    socket_mac.pop(email, None)

    target_email = presence_state.get(email, {}).get("user_is_on")
    await mark_user_offline(email)

    if target_email:
        await touch_last_clicked(email, target_email)
        await notify_peer(target_email, email)

    await broadcast_presence_to_friends(email)


@app.websocket("/ws/{email}/{mac_id}")
async def websocket_endpoint(websocket: WebSocket, email: str, mac_id: str):
    await websocket.accept()

    connected_users[email] = websocket
    socket_mac[email] = mac_id

    await mark_user_online(email)
    asyncio.create_task(run_blocking(_record_login_blocking, email, mac_id, now_utc()))

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
async def get_status(me: str, friend: str):
    return await run_blocking(_get_status_blocking, me, friend)


@app.get("/friends_last_seen")
def friends_last_seen(emails: str):
    """Kept as a REST fallback for the client's periodic LastSeenSyncThread
    safety-net reconciliation. The live path is now the websocket
    sync_request/bulk_presence flow above, served from presence_state."""
    email_list = [e.strip() for e in emails.split(",") if e.strip()]
    if not email_list:
        return {}
    result = {}
    for email in email_list:
        state = presence_state.get(email)
        if state is not None:
            result[email] = {"is_online": state["is_online"], "last_seen_at": state["last_seen_at"]}
    return result


@app.get("/check_mac/{email}")
async def check_mac(email: str, mac_id: str):
    registered = await run_blocking(_get_registered_mac_blocking, email)
    return {"match": registered == mac_id, "registered_mac": registered}


@app.get("/health")
def health():
    return {"ok": True, "tracked_users": len(presence_state), "connected": len(connected_users)}
