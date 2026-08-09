import asyncio
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from typing import Dict, List, Optional, Set

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
from pymongo import MongoClient, UpdateOne


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

scheduled_msg_db = client["scheduled_messages"]
messages_to_send_col = scheduled_msg_db["messages_to_send"]

app = FastAPI(title="Zyro Live Activity Service")

connected_users: Dict[str, WebSocket] = {}
socket_mac: Dict[str, str] = {}

HEARTBEAT_IDLE_TIMEOUT =5.0
HEARTBEAT_PING_TIMEOUT =3.0

# ---------------------------------------------------------------------
# In-memory state. Everything on the hot path reads/writes these --
# zero blocking Mongo calls in the request/event path. Mongo writes are
# fire-and-forget (asyncio.create_task) and Mongo reads only happen at
# startup or as a cache-miss fallback.
# ---------------------------------------------------------------------

# email -> {"is_online": bool, "last_seen_at": iso_str|None, "user_is_on": str|None}
presence_state: Dict[str, dict] = {}

# email -> set of emails who have this email in THEIR friend_list, i.e.
# "who needs to be told when this email's presence changes"
friend_watchers: Dict[str, Set[str]] = {}

# email -> the set of friend emails currently in THAT email's friend_list
# (needed so the friend_list change-stream handler can diff old vs new)
user_friend_lists: Dict[str, Set[str]] = {}

# the running asyncio loop, captured at startup so the background watcher
# threads (which are NOT asyncio, see below) can hand work back to it
MAIN_LOOP: Optional[asyncio.AbstractEventLoop] = None

# All one-shot/blocking Mongo calls run here so they never block the loop.
EXECUTOR = ThreadPoolExecutor(max_workers=8)


async def run_blocking(fn, *args, **kwargs):
    """Must be `async def`, not a plain function returning the executor
    Future directly -- asyncio.create_task() requires an actual coroutine
    object. run_in_executor() returns a Future, which create_task rejected
    outright on newer Python/uvloop ("a coroutine was expected, got
    <Future...>"). Awaiting it here makes run_blocking(...) itself produce
    a coroutine, so every `asyncio.create_task(run_blocking(...))`
    fire-and-forget call below works again."""
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(EXECUTOR, lambda: fn(*args, **kwargs))


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
# blocking Mongo functions -- ONLY ever called via run_blocking() or
# from inside the dedicated watcher threads below (never on the loop)
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


def _push_last_clicked_batch_blocking(entries: List[dict]) -> List[str]:
    """
    Batched write-through target for the client's LastClickedManager.
    entries: [{"chat_id": str, "updates": {sanitized_field: iso_str|None, ...}}, ...]

    One bulk_write round trip to Mongo covers the entire batch, no matter
    how many chats or how many offline clicks are queued up client-side.
    Returns the chat_ids actually included, so the caller can ack exactly
    those back to the client.
    """
    ops = []
    chat_ids = []
    for entry in entries:
        chat_id = entry.get("chat_id")
        updates = entry.get("updates") or {}
        if not chat_id or not updates:
            continue

        parsed = {}
        for field, value in updates.items():
            if value:
                try:
                    parsed[field] = datetime.fromisoformat(value.replace("Z", "+00:00"))
                except Exception:
                    parsed[field] = None
            else:
                parsed[field] = None

        ops.append(UpdateOne({"_id": chat_id}, {"$set": parsed}, upsert=True))
        chat_ids.append(chat_id)

    if ops:
        last_clicked_col.bulk_write(ops, ordered=False)

    return chat_ids


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


def _build_friend_watchers_and_lists_blocking():
    """For every user doc in all_type_list_table, invert friend_list into
    'who watches whom' (friend_watchers) and also snapshot each user's own
    friend_list (user_friend_lists) so the change-stream handler can diff
    old vs new the next time that array changes."""
    watchers: Dict[str, Set[str]] = {}
    lists: Dict[str, Set[str]] = {}
    for doc in all_type_list_col.find({}, {"friend_list": 1}):
        user_email = doc["_id"]
        friends = set(doc.get("friend_list", []))
        lists[user_email] = friends
        for friend_email in friends:
            watchers.setdefault(friend_email, set()).add(user_email)
    return watchers, lists


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
    """O(1) index lookup + concurrent fan-out, zero Mongo calls."""
    watchers = friend_watchers.get(changed_email)
    if not watchers:
        return
    payload = {"type": "presence_update", "email": changed_email, **presence_state.get(changed_email, {})}
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
    never seen, and caches the result for next time."""
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

    print(f"[bulk_presence] -> {requester_email}: {len(updates)} entries ({len(missing)} fetched from mongo)")
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
    prev = presence_state.get(email, {})
    presence_state[email] = {
        "is_online": False,
        "last_seen_at": ts.isoformat(),
        "user_is_on": None,
    }
    print(f"[presence] {email}: {prev} -> {presence_state[email]}")
    asyncio.create_task(run_blocking(_persist_user_offline_blocking, email, ts))


async def mark_chat_target(email: str, target_email: Optional[str]) -> None:
    state = presence_state.setdefault(email, {"is_online": True, "last_seen_at": None, "user_is_on": None})
    state["user_is_on"] = target_email
    asyncio.create_task(run_blocking(_persist_chat_target_blocking, email, target_email))


async def touch_last_clicked(email: str, target_email: str) -> None:
    ts = now_utc()
    asyncio.create_task(run_blocking(_persist_last_clicked_blocking, email, target_email, ts))


# ---------------------------------------------------------------------
# websocket event handling (client -> server messages)
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

    elif event_type == "sync_last_clicked":
        # Batched write-through from the client's local sqlite queue --
        # covers offline clicks, clicks made under a previous login on
        # this machine, and anything the write-through push missed.
        # One message in, one bulk_write to Mongo, one ack back out.
        entries = data.get("entries") or []
        if not entries:
            return
        synced_chat_ids = await run_blocking(_push_last_clicked_batch_blocking, entries)
        await safe_send(connected_users.get(email), {
            "type": "sync_last_clicked_ack",
            "chat_ids": synced_chat_ids,
        })


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


# ---------------------------------------------------------------------
# Mongo change streams -- these are what make everything automatic.
# .watch() is a BLOCKING generator, so each of these runs in its own
# daemon thread (never on the asyncio loop) and hands results back to
# the loop via asyncio.run_coroutine_threadsafe. Requires the Mongo
# deployment to be a replica set (Atlas clusters satisfy this).
# ---------------------------------------------------------------------

def _watch_last_seen_changes():
    """Watches db.last_seen for ANY change to ANY document. On a change,
    diffs the incoming doc against what we have cached in presence_state,
    prints the diff, updates the cache, and broadcasts to that email's
    friend_watchers -- all without the client ever polling."""
    while True:
        try:
            print("[last_seen watcher] change stream connected")
            with last_seen_col.watch(full_document="updateLookup") as stream:
                for change in stream:
                    email = change["documentKey"]["_id"]
                    full_doc = change.get("fullDocument")
                    if not full_doc:
                        continue

                    last_seen_at = full_doc.get("last_seen_at")
                    new_state = {
                        "is_online": full_doc.get("is_online", False),
                        "last_seen_at": last_seen_at.isoformat() if last_seen_at else None,
                        "user_is_on": full_doc.get("user_is_on"),
                    }
                    old_state = presence_state.get(email)

                    if old_state == new_state:
                        continue  # no real change, e.g. a re-set of the same values

                    print(f"[last_seen CHANGED] {email}: {old_state} -> {new_state}")
                    presence_state[email] = new_state

                    if MAIN_LOOP is not None:
                        asyncio.run_coroutine_threadsafe(
                            broadcast_presence_to_friends(email), MAIN_LOOP
                        )
        except Exception as e:
            print(f"[last_seen watcher] stream error, retrying in 3s: {e}")
            time.sleep(3)


def _watch_friend_list_changes():
    """Watches user_db.all_type_list_table for ANY change to ANY document.
    On a change, diffs friend_list against our cached copy for that user.
    New friends are wired into friend_watchers immediately (so presence
    starts flowing right away) and, if that user is currently connected,
    we push them the new friend's current status right now via
    bulk_presence -- no waiting on the client to ask."""
    while True:
        try:
            print("[friend_list watcher] change stream connected")
            with all_type_list_col.watch(full_document="updateLookup") as stream:
                for change in stream:
                    user_email = change["documentKey"]["_id"]
                    full_doc = change.get("fullDocument")
                    if not full_doc:
                        continue

                    new_friends = set(full_doc.get("friend_list", []))
                    old_friends = user_friend_lists.get(user_email, set())

                    if new_friends == old_friends:
                        continue

                    added = new_friends - old_friends
                    removed = old_friends - new_friends
                    print(f"[friend_list CHANGED] {user_email}: +{added} -{removed}")

                    user_friend_lists[user_email] = new_friends

                    for friend_email in added:
                        friend_watchers.setdefault(friend_email, set()).add(user_email)
                    for friend_email in removed:
                        watchers = friend_watchers.get(friend_email)
                        if watchers:
                            watchers.discard(user_email)

                    if added and MAIN_LOOP is not None:
                        asyncio.run_coroutine_threadsafe(
                            send_bulk_presence(user_email, list(added)), MAIN_LOOP
                        )
        except Exception as e:
            print(f"[friend_list watcher] stream error, retrying in 3s: {e}")
            time.sleep(3)


# ---------------------------------------------------------------------
# startup: warm the in-memory caches, then start the change-stream
# watcher threads. No polling timers left anywhere.
# ---------------------------------------------------------------------

@app.on_event("startup")
async def startup_event():
    global MAIN_LOOP, presence_state, friend_watchers, user_friend_lists

    MAIN_LOOP = asyncio.get_event_loop()

    await run_blocking(_strip_legacy_presence_fields_blocking)
    await run_blocking(_create_friend_list_index_blocking)

    presence_state = await run_blocking(_load_all_presence_blocking)
    friend_watchers, user_friend_lists = await run_blocking(_build_friend_watchers_and_lists_blocking)

    threading.Thread(target=_watch_last_seen_changes, daemon=True, name="last_seen-watcher").start()
    threading.Thread(target=_watch_friend_list_changes, daemon=True, name="friend_list-watcher").start()

    print(f"[startup] warmed presence_state({len(presence_state)}) "
          f"friend_watchers({len(friend_watchers)}) user_friend_lists({len(user_friend_lists)})")


@app.post("/refresh_friend_graph")
async def refresh_friend_graph():
    """Manual escape hatch -- not needed in normal operation since the
    change stream keeps friend_watchers live, but handy if you ever
    suspect drift (e.g. after a stream reconnect gap)."""
    global friend_watchers, user_friend_lists
    friend_watchers, user_friend_lists = await run_blocking(_build_friend_watchers_and_lists_blocking)
    return {"ok": True, "tracked_emails": len(friend_watchers)}


@app.post("/push_scheduled_message")
async def push_scheduled_message(request: Request):
    """Client's ScheduleSyncThread posts here for every row sitting in
    its local unsent_schedule_messages table. operation is one of
    'send' / 'edit' / 'delete'. The write logic is a closure so nothing
    new is added to the module's global function namespace."""
    try:
        payload = await request.json()

        def do_write():
            op = payload.get("operation")
            _id = payload.get("_id")

            if op == "delete":
                messages_to_send_col.delete_one({"_id": _id})
                return True

            doc = {
                "_id": _id,
                "scheduled_at": payload.get("scheduled_at"),
                "from": payload.get("from"),
                "to": payload.get("to"),
                "sent": payload.get("sent"),
                "sent_at": payload.get("sent_at"),
                "message_type": payload.get("message_type"),
                "message_content": payload.get("message_content"),
            }
            messages_to_send_col.update_one({"_id": _id}, {"$set": doc}, upsert=True)
            return True

        ok = await run_blocking(do_write)
        return {"ok": ok}
    except Exception as e:
        print(f"[push_scheduled_message] error: {e}")
        return {"ok": False, "error": str(e)}


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
@app.get("/friends_last_seen")
async def friends_last_seen(emails: str):
    """REST equivalent of send_bulk_presence, for the client's one-shot
    reconciliation pass after coming back online. Served from
    presence_state where possible; falls back to Mongo for anything
    not yet cached, and backfills the cache with what it finds."""
    email_list = [e for e in emails.split(",") if e]
    if not email_list:
        return {}

    result = {}
    missing = []
    for email in email_list:
        state = presence_state.get(email)
        if state is None:
            missing.append(email)
        else:
            result[email] = {
                "is_online": state.get("is_online", False),
                "last_seen_at": state.get("last_seen_at"),
            }

    if missing:
        fetched = await run_blocking(_load_missing_presence_blocking, missing)
        for email, state in fetched.items():
            presence_state[email] = state
            result[email] = {
                "is_online": state.get("is_online", False),
                "last_seen_at": state.get("last_seen_at"),
            }

    return result

@app.get("/")
@app.head("/")
def root():
    return {"success": True}


@app.get("/status")
async def get_status(me: str, friend: str):
    """Kept only as a manual debug endpoint. Not called by the client
    anymore -- bulk_presence at connect + presence_update pushes cover
    the live case."""
    return await run_blocking(_get_status_blocking, me, friend)


@app.get("/check_mac/{email}")
async def check_mac(email: str, mac_id: str):
    registered = await run_blocking(_get_registered_mac_blocking, email)
    return {"match": registered == mac_id, "registered_mac": registered}


@app.get("/health")
def health():
    return {
        "ok": True,
        "tracked_users": len(presence_state),
        "connected": len(connected_users),
        "tracked_friend_graph_entries": len(friend_watchers),
    }
