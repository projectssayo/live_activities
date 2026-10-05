import asyncio
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from typing import Dict, List, Optional, Set

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
from fastapi import UploadFile, File, Form
from pydantic import BaseModel, Field
from pymongo import MongoClient, UpdateOne, ReturnDocument

import cloudinary
import cloudinary.uploader

cloudinary.config(cloud_name='dbiifyr5m', api_key='931832746959244', api_secret='W4-z0i5yUemucfL_uBPTFoDgH00', secure=True)
MONGO_USERNAME = "suyognegi_global"
MONGO_PASSWORD = "Oj5eGphIUUud9YvY"

url = (f"mongodb+srv://{MONGO_USERNAME}:{MONGO_PASSWORD}" f"@cluster0.hzyekeb.mongodb.net/?appName=Cluster0")

client = MongoClient(url, serverSelectionTimeoutMS=10000, connectTimeoutMS=10000, socketTimeoutMS=10000, )

db = client["live_activities"]
last_seen_col = db["last_seen"]
last_clicked_col = db["last_clicked_on_table"]
logged_in_col = db["logged_in_at"]
messages_col = db["messages"]
messages_col.create_index([("sent_by", 1), ("sent_to", 1), ("sent_at", -1)])
messages_col.create_index([("sent_to", 1), ("received_at", 1)])
messages_col.create_index([("sent_by", 1), ("received_at", 1)])        

user_db = client["user_db"]
all_type_list_col = user_db["all_type_list_table"]

scheduled_msg_db = client["scheduled_messages"]
messages_to_send_col = scheduled_msg_db["messages_to_send"]

app = FastAPI(title="Zyro Live Activity Service")

# CHANGED: one websocket per DEVICE (mac_id) per user.  email -> {mac_id: websocket}
connected_users: Dict[str, Dict[str, WebSocket]] = {}

HEARTBEAT_IDLE_TIMEOUT = 5.0
HEARTBEAT_PING_TIMEOUT = 3.0

presence_state: Dict[str, dict] = {}

friend_watchers: Dict[str, Set[str]] = {}

user_friend_lists: Dict[str, Set[str]] = {}
last_clicked_mem: Dict[str, Dict[str, datetime]] = {}   # chat_id -> {sanitized_email: ts}
dirty_clicks: Dict[str, Set[str]] = {}                  # email -> chat_ids needing a Mongo flush

MAIN_LOOP: Optional[asyncio.AbstractEventLoop] = None



def _dfm_path(email: str) -> str:
    return f"delete_from_me.{sanitize_email(email)}"


def _default_dfm(a: str, b: str) -> Dict[str, bool]:
    return {sanitize_email(a): False, sanitize_email(b): False}


def _save_message_blocking(msg: dict):
    server_ts = now_utc().isoformat(timespec="microseconds")
    insert_only_keys = ("is_edited", "delete_from_all")
    insert_only = {k: msg[k] for k in insert_only_keys if k in msg}
    # per-user delete flags are created only on first insert, so a retry can never un-delete
    insert_only["delete_from_me"] = _default_dfm(msg["sent_by"], msg["sent_to"])
    doc = {k: v for k, v in msg.items()
           if k not in ("_id", "received_at", "delete_from_me") and k not in insert_only_keys}
    saved = messages_col.find_one_and_update(
        {"_id": msg["_id"]},
        {"$set": doc, "$setOnInsert": {"received_at": server_ts, **insert_only}},
        upsert=True,
        return_document=ReturnDocument.AFTER,
        projection={"received_at": 1},
    ) or {}
    return saved.get("received_at") or server_ts





def _eff_expr(viewer: str):
    return {"$cond": [{"$eq": ["$sent_by", viewer]},
                      "$sent_at",
                      {"$ifNull": ["$received_at", "$sent_at"]}]}


def _get_messages_page_blocking(user_a: str, user_b: str, before_sent_at, limit: int = 10):
    match = {"$or": [{"sent_by": user_a, "sent_to": user_b},
                     {"sent_by": user_b, "sent_to": user_a}]}
    pipeline = [
        {"$match": match},
        {"$addFields": {"_eff": _eff_expr(user_a)}},
    ]
    if before_sent_at:
        pipeline.append({"$match": {"_eff": {"$lt": before_sent_at}}})
    pipeline += [
        {"$sort": {"_eff": -1}},
        {"$limit": limit},
        {"$project": {"_eff": 0}},
    ]
    docs = list(messages_col.aggregate(pipeline, allowDiskUse=True))
    docs.reverse()
    return docs


def _mark_deleted_for_all_blocking(msg_id: str):
    messages_col.update_one({"_id": msg_id}, {"$set": {"delete_from_all": True}})



def _mark_deleted_for_me_blocking(msg_id: str, who: str):
    messages_col.update_one({"_id": msg_id}, {"$set": {_dfm_path(who): True}})



def _edit_message_blocking(msg_id: str, new_content: str):
    messages_col.update_one({"_id": msg_id}, {"$set": {"msg_content": new_content, "is_edited": True}})


EXECUTOR = ThreadPoolExecutor(max_workers=8)

# REMOVED: stray top-level do_write() (referenced undefined `payload`, never used)


async def run_blocking(fn, *args, **kwargs):
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



recent_api_pushes: Dict[str, float] = {}
API_PUSH_TTL = 15.0


def _mark_api_push(kind: str, key: str):
    now = time.monotonic()
    recent_api_pushes[f"{kind}:{key}"] = now
    if len(recent_api_pushes) > 2000:
        for k, t in list(recent_api_pushes.items()):
            if now - t > API_PUSH_TTL:
                recent_api_pushes.pop(k, None)


def _was_api_pushed(kind: str, key: str) -> bool:
    t = recent_api_pushes.get(f"{kind}:{key}")
    return t is not None and (time.monotonic() - t) < API_PUSH_TTL


def _json_safe(o):
    if isinstance(o, datetime):
        return o.isoformat()
    if isinstance(o, dict):
        return {k: _json_safe(v) for k, v in o.items()}
    if isinstance(o, (list, tuple)):
        return [_json_safe(v) for v in o]
    return o


def _remember_click(viewer: str, peer: str, ts: datetime):
    chat_id = get_chat_id(viewer, peer)
    last_clicked_mem.setdefault(chat_id, {})[sanitize_email(viewer)] = ts
    dirty_clicks.setdefault(viewer, set()).add(chat_id)


def _flush_clicks_blocking(email: str):
    field = sanitize_email(email)
    for chat_id in list(dirty_clicks.pop(email, ())):
        ts = last_clicked_mem.get(chat_id, {}).get(field)
        if ts:
            # $max: an older value can never overwrite a newer one
            last_clicked_col.update_one({"_id": chat_id}, {"$max": {field: ts}}, upsert=True)


def _presence_fields(email: str, viewer: str) -> dict:
    s = presence_state.get(email, {})
    return {
        "is_online": s.get("is_online", False),
        "last_seen_at": s.get("last_seen_at"),
        "on_chat_with_you": bool(s.get("is_online") and s.get("user_is_on") == viewer),
    }


LEGACY_PRESENCE_FIELDS = {"online": "", "last_seen": "", "user_is_on": ""}


def _strip_legacy_presence_fields_blocking():
    last_seen_col.update_many({}, {"$unset": LEGACY_PRESENCE_FIELDS})


def _create_friend_list_index_blocking():
    all_type_list_col.create_index("friend_list")


def _persist_user_online_blocking(email: str, ts: datetime):
    last_seen_col.update_one(
        {"_id": email},
        {"$set": {"is_online": True, "last_seen_at": ts},
         "$unset": LEGACY_PRESENCE_FIELDS},
        upsert=True,
    )


def _persist_user_offline_blocking(email: str, ts: datetime):
    last_seen_col.update_one(
        {"_id": email},
        {"$set": {"is_online": False, "last_seen_at": ts},
         "$unset": LEGACY_PRESENCE_FIELDS},
        upsert=True,
    )


def _persist_chat_target_blocking(email: str, target_email: Optional[str]):
    last_seen_col.update_one({"_id": email}, {"$set": {"user_is_on": target_email}}, upsert=True)


def _persist_last_clicked_blocking(email: str, target_email: str, ts: datetime):
    chat_id = get_chat_id(email, target_email)
    field = sanitize_email(email)
    other_field = sanitize_email(target_email)
    last_clicked_col.update_one({"_id": chat_id}, {"$set": {field: ts}}, upsert=True)
    last_clicked_col.update_one({"_id": chat_id, other_field: {"$exists": False}}, {"$set": {other_field: None}}, )


def _push_last_clicked_batch_blocking(entries: List[dict]) -> List[str]:
    ops = []
    chat_ids = []
    for entry in entries:
        chat_id = entry.get("chat_id")
        updates = entry.get("updates") or {}
        if not chat_id or not updates:
            continue

        parsed = {}
        for field, value in updates.items():
            if not value:
                continue                      # never push None: $max would ignore it anyway
            try:
                dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
                parsed[field] = dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
            except Exception:
                continue

        if parsed:
            ops.append(UpdateOne({"_id": chat_id}, {"$max": parsed}, upsert=True))
        chat_ids.append(chat_id)

    if ops:
        last_clicked_col.bulk_write(ops, ordered=False)

    return chat_ids


def _record_login_blocking(email: str, mac_id: str, ts: datetime):
    logged_in_col.update_one({"_id": email}, {"$set": {"logged_in_at": ts, "mac_id": mac_id}}, upsert=True)


def _get_registered_mac_blocking(email: str) -> Optional[str]:
    doc = logged_in_col.find_one({"_id": email})
    return doc.get("mac_id") if doc else None


def _load_all_presence_blocking() -> Dict[str, dict]:
    result = {}
    for doc in last_seen_col.find({}):
        last_seen_at = doc.get("last_seen_at")
        result[doc["_id"]] = {
            "is_online": False,   # nobody is connected at server start
            "last_seen_at": last_seen_at.isoformat() if last_seen_at else None,
            "user_is_on": None,
        }
    return result


def _load_missing_presence_blocking(emails: List[str]) -> Dict[str, dict]:
    result = {}
    for doc in last_seen_col.find({"_id": {"$in": emails}}):
        last_seen_at = doc.get("last_seen_at")
        result[doc["_id"]] = {
            "is_online": doc.get("is_online", False),
            "last_seen_at": last_seen_at.isoformat() if last_seen_at else None,
            "user_is_on": None,
        }
    return result


def _build_friend_watchers_and_lists_blocking():
    watchers: Dict[str, Set[str]] = {}
    lists: Dict[str, Set[str]] = {}
    for doc in all_type_list_col.find({}, {"friend_list": 1}):
        user_email = doc["_id"]
        friends = set(doc.get("friend_list", []))
        lists[user_email] = friends
        for friend_email in friends:
            watchers.setdefault(friend_email, set()).add(user_email)

    return watchers, lists



def _get_friend_list_blocking(user_email: str) -> List[str]:
    doc = all_type_list_col.find_one({"_id": user_email}, {"friend_list": 1})
    return (doc or {}).get("friend_list", [])


def _build_unread_counts_blocking(user_email: str, friend_list: List[str]) -> Dict[str, int]:
    me_field = sanitize_email(user_email)
    chat_ids = {f: get_chat_id(user_email, f) for f in friend_list}

    clicked = {}
    if chat_ids:
        for d in last_clicked_col.find({"_id": {"$in": list(chat_ids.values())}}, {me_field: 1}):
            clicked[d["_id"]] = d.get(me_field)

    out: Dict[str, int] = {}
    for friend, cid in chat_ids.items():
        q = {
            "sent_by": friend,
            "sent_to": user_email,
            "delete_from_all": {"$ne": True},
            _dfm_path(user_email): {"$ne": True},
        }
        ts = clicked.get(cid)
        if ts:
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            q["received_at"] = {"$gt": ts.isoformat(timespec="microseconds")}
        out[friend] = messages_col.count_documents(q, limit=100)
    return out






def _get_status_blocking(me: str, friend: str) -> dict:
    friend_doc = last_seen_col.find_one({"_id": friend}) or {}
    chat_id = get_chat_id(me, friend)
    clicked_doc = last_clicked_col.find_one({"_id": chat_id}) or {}
    me_field = sanitize_email(me)
    friend_field = sanitize_email(friend)
    return {"friend_email": friend, "is_online": friend_doc.get("is_online", False),
            "user_is_on": friend_doc.get("user_is_on"), "last_seen_at": friend_doc.get("last_seen_at"),
            "is_on_same_chat_as_me": friend_doc.get("user_is_on") == me,
            "my_last_clicked": clicked_doc.get(me_field), "friend_last_clicked": clicked_doc.get(friend_field), }


async def safe_send(ws: Optional[WebSocket], payload: dict) -> None:
    if ws is None:
        return
    try:
        await ws.send_json(payload)
    except Exception:
        pass


# NEW: send to EVERY connected device of a user (optionally skipping one device)
async def send_to_user(email: str, payload: dict, exclude_mac: Optional[str] = None) -> None:
    socks = [ws for mac, ws in list((connected_users.get(email) or {}).items())
             if mac != exclude_mac]
    if not socks:
        return
    await asyncio.gather(*(safe_send(ws, payload) for ws in socks),
                         return_exceptions=True)


# CHANGED: uses send_to_user
async def broadcast_presence_to_friends(changed_email: str) -> None:
    watchers = friend_watchers.get(changed_email)
    if not watchers:
        return
    await asyncio.gather(
        *(send_to_user(w, {"type": "presence_update", "email": changed_email,
                           **_presence_fields(changed_email, w)})
          for w in list(watchers) if w in connected_users),
        return_exceptions=True,
    )


# CHANGED: uses send_to_user
async def notify_peer(peer_email: str, changed_email: str) -> None:
    await send_to_user(
        peer_email,
        {"type": "presence_update", "email": changed_email,
         **_presence_fields(changed_email, peer_email)},
    )


# CHANGED: uses send_to_user
async def send_seen_receipt(viewer: str, peer: str, ts: datetime) -> None:
    await send_to_user(
        peer,
        {"type": "seen_receipt", "by": viewer, "seen_at": ts.isoformat()},
    )


async def leave_chat(email: str) -> None:
    state = presence_state.get(email, {})
    prev = state.get("user_is_on")
    if not prev:
        return
    ts = now_utc()
    state["user_is_on"] = None
    _remember_click(email, prev, ts)
    await send_seen_receipt(email, prev, ts)
    await notify_peer(prev, email)


async def enter_chat(email: str, target: str) -> None:
    state = presence_state.setdefault(
        email, {"is_online": True, "last_seen_at": None, "user_is_on": None})
    if state.get("user_is_on") and state["user_is_on"] != target:
        await leave_chat(email)
    ts = now_utc()
    state["user_is_on"] = target
    _remember_click(email, target, ts)
    await send_seen_receipt(email, target, ts)
    await notify_peer(target, email)


# CHANGED: uses send_to_user
async def send_bulk_presence(requester_email: str, friend_emails: List[str]) -> None:
    if not connected_users.get(requester_email) or not friend_emails:
        return

    missing = [e for e in friend_emails if presence_state.get(e) is None]
    if missing:
        fetched = await run_blocking(_load_missing_presence_blocking, missing)
        for email, state in fetched.items():
            presence_state[email] = state

    updates = []
    for email in friend_emails:
        if presence_state.get(email) is None:
            continue
        updates.append({"email": email, **_presence_fields(email, requester_email)})

    print(f"[bulk_presence] -> {requester_email}: {len(updates)} entries ({len(missing)} fetched from mongo)")
    await send_to_user(requester_email, {"type": "bulk_presence", "updates": updates})


async def mark_user_online(email: str) -> None:
    ts = now_utc()
    presence_state[email] = {"is_online": True, "last_seen_at": ts.isoformat(), "user_is_on": None}
    asyncio.create_task(run_blocking(_persist_user_online_blocking, email, ts))


async def mark_user_offline(email: str) -> None:
    ts = now_utc()
    presence_state[email] = {"is_online": False, "last_seen_at": ts.isoformat(), "user_is_on": None}
    asyncio.create_task(run_blocking(_persist_user_offline_blocking, email, ts))


async def mark_chat_target(email: str, target_email: Optional[str]) -> None:
    state = presence_state.setdefault(email, {"is_online": True, "last_seen_at": None, "user_is_on": None})
    state["user_is_on"] = target_email
    asyncio.create_task(run_blocking(_persist_chat_target_blocking, email, target_email))


async def touch_last_clicked(email: str, target_email: str) -> None:
    ts = now_utc()
    asyncio.create_task(run_blocking(_persist_last_clicked_blocking, email, target_email, ts))


# CHANGED: ack goes to all devices of the user
async def handle_event(email: str, data: dict) -> None:
    event_type = data.get("type")

    if event_type == "opened_chat":
        target = data.get("target_email")
        if target:
            await enter_chat(email, target)

    elif event_type == "closed_chat":
        await leave_chat(email)

    elif event_type == "sync_request":
        friend_emails = data.get("friend_list") or []
        await send_bulk_presence(email, friend_emails)

    elif event_type == "sync_last_clicked":
        entries = data.get("entries") or []
        if not entries:
            return
        synced_chat_ids = await run_blocking(_push_last_clicked_batch_blocking, entries)
        await send_to_user(email, {"type": "sync_last_clicked_ack", "chat_ids": synced_chat_ids})


# CHANGED: takes mac_id; user goes offline only when the LAST device disconnects
async def cleanup_user(email: str, mac_id: str, ws: WebSocket) -> None:
    conns = connected_users.get(email)
    if not conns or conns.get(mac_id) is not ws:     # stale socket: this device already reconnected
        return

    conns.pop(mac_id, None)
    if conns:                                        # another device of this user is still connected
        return
    connected_users.pop(email, None)

    await leave_chat(email)
    await mark_user_offline(email)
    await run_blocking(_flush_clicks_blocking, email)
    await broadcast_presence_to_friends(email)


def _watch_last_seen_changes():
    while True:
        try:
            print("[last_seen watcher] change stream connected")
            with last_seen_col.watch(full_document="updateLookup") as stream:
                for change in stream:
                    email = change["documentKey"]["_id"]
                    full_doc = change.get("fullDocument")
                    if not full_doc:
                        continue

                    old_state = presence_state.get(email)
                    last_seen_at = full_doc.get("last_seen_at")
                    new_state = {
                        "is_online": full_doc.get("is_online", False),
                        "last_seen_at": last_seen_at.isoformat() if last_seen_at else None,
                        "user_is_on": (old_state or {}).get("user_is_on"),
                    }

                    if old_state == new_state:
                        continue

                    print(f"[last_seen CHANGED] {email}: {old_state} -> {new_state}")
                    presence_state[email] = new_state

                    if MAIN_LOOP is not None:
                        asyncio.run_coroutine_threadsafe(
                            broadcast_presence_to_friends(email), MAIN_LOOP)
        except Exception as e:
            print(f"[last_seen watcher] stream error, retrying in 3s: {e}")
            time.sleep(3)


def _watch_friend_list_changes():
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
                        asyncio.run_coroutine_threadsafe(send_bulk_presence(user_email, list(added)), MAIN_LOOP)
        except Exception as e:
            print(f"[friend_list watcher] stream error, retrying in 3s: {e}")
            time.sleep(3)


async def _push_external_new(doc: dict):
    note = {"type": "new_message", "message": _json_safe(doc)}
    to, by = doc.get("sent_to"), doc.get("sent_by")
    if to:
        await send_to_user(to, note)
    if by and by != to:
        await send_to_user(by, note)


async def _push_external_update(doc: dict, notes: List[dict], extra_users: Optional[List[str]] = None):
    if extra_users is None:
        users = {doc.get("sent_to"), doc.get("sent_by")}
    else:
        users = set(extra_users)
    for u in [x for x in users if x]:
        for n in notes:
            await send_to_user(u, n)


def _watch_messages_changes():
    pipeline = [{"$match": {"operationType": {"$in": ["insert", "update"]}}}]
    while True:
        try:
            print("[messages watcher] change stream connected")
            with messages_col.watch(pipeline, full_document="updateLookup") as stream:
                for change in stream:
                    if MAIN_LOOP is None:
                        continue
                    op = change["operationType"]
                    msg_id = change["documentKey"]["_id"]
                    doc = change.get("fullDocument")
                    if not doc:
                        continue

                    def run(coro):
                        asyncio.run_coroutine_threadsafe(coro, MAIN_LOOP)

                    if op == "insert":
                        if not doc.get("received_at"):
                            stamp = now_utc().isoformat(timespec="microseconds")
                            messages_col.update_one(
                                {"_id": msg_id, "received_at": {"$in": [None]}},
                                {"$set": {"received_at": stamp}})
                            doc["received_at"] = stamp
                        if _was_api_pushed("new", msg_id):
                            continue
                        print(f"[messages watcher] external insert {msg_id}")
                        run(_push_external_new(doc))
                        continue

                    updated = (change.get("updateDescription") or {}).get("updatedFields") or {}

                    if updated.get("delete_from_all") is True:
                        if not _was_api_pushed("del_all", msg_id):
                            run(_push_external_update(doc, [{"type": "message_deleted_all", "_id": msg_id}]))
                        continue

                    if "msg_content" in updated or "is_edited" in updated:
                        if not _was_api_pushed("edit", msg_id):
                            run(_push_external_update(doc, [{"type": "message_edited", "_id": msg_id,
                                                            "msg_content": doc.get("msg_content")}]))

                    if any(k.startswith("delete_from_me") for k in updated):
                        flags = doc.get("delete_from_me") or {}
                        whole = "delete_from_me" in updated          # whole dict replaced
                        whos = []
                        for w in {doc.get("sent_by"), doc.get("sent_to")}:
                            if not w:
                                continue
                            if not (whole or _dfm_path(w) in updated):
                                continue
                            if flags.get(sanitize_email(w)) is True \
                                    and not _was_api_pushed("del_me", f"{msg_id}:{w}"):
                                whos.append(w)
                        if whos:
                            run(_push_external_update(doc, [{"type": "message_deleted_me", "_id": msg_id}],
                                                      extra_users=whos))
        except Exception as e:
            print(f"[messages watcher] stream error, retrying in 3s: {e}")
            time.sleep(3)


def _migrate_delete_from_me_blocking():
    """One time: bool delete_from_me + deleted_for[] -> delete_from_me {sanitized_email: bool}."""
    ops, total = [], 0
    cursor = messages_col.find({"delete_from_me": {"$not": {"$type": "object"}}},
                               {"sent_by": 1, "sent_to": 1, "deleted_for": 1})
    for d in cursor:
        if not d.get("sent_by") or not d.get("sent_to"):
            continue
        gone = set(d.get("deleted_for") or [])
        dfm = {sanitize_email(d["sent_by"]): d["sent_by"] in gone,
               sanitize_email(d["sent_to"]): d["sent_to"] in gone}
        ops.append(UpdateOne({"_id": d["_id"]},
                             {"$set": {"delete_from_me": dfm}, "$unset": {"deleted_for": ""}}))
        if len(ops) >= 500:
            messages_col.bulk_write(ops, ordered=False)
            total += len(ops)
            ops = []
    if ops:
        messages_col.bulk_write(ops, ordered=False)
        total += len(ops)
    print(f"[migrate] delete_from_me converted on {total} messages")

@app.on_event("startup")
async def startup_event():
    global MAIN_LOOP, presence_state, friend_watchers, user_friend_lists

    MAIN_LOOP = asyncio.get_event_loop()

    await run_blocking(_strip_legacy_presence_fields_blocking)
    await run_blocking(_create_friend_list_index_blocking)
    await run_blocking(_migrate_delete_from_me_blocking)     # NEW: must run before the watchers start

    presence_state = await run_blocking(_load_all_presence_blocking)
    friend_watchers, user_friend_lists = await run_blocking(_build_friend_watchers_and_lists_blocking)

    threading.Thread(target=_watch_last_seen_changes, daemon=True, name="last_seen-watcher").start()
    threading.Thread(target=_watch_friend_list_changes, daemon=True, name="friend_list-watcher").start()
    threading.Thread(target=_watch_messages_changes, daemon=True, name="messages-watcher").start()

    print(f"[startup] warmed presence_state({len(presence_state)}) "
          f"friend_watchers({len(friend_watchers)}) user_friend_lists({len(user_friend_lists)})")





@app.post("/refresh_friend_graph")
async def refresh_friend_graph():
    global friend_watchers, user_friend_lists
    friend_watchers, user_friend_lists = await run_blocking(_build_friend_watchers_and_lists_blocking)
    return {"ok": True, "tracked_emails": len(friend_watchers)}


@app.post("/push_scheduled_message")
async def push_scheduled_message(request: Request):
    try:
        payload = await request.json()

        def do_write():
            op = payload.get("operation")
            _id = payload.get("_id")

            if op == "delete":
                messages_to_send_col.delete_one({"_id": _id})
                return True

            doc = {"_id": _id, "scheduled_at": payload.get("scheduled_at"), "from": payload.get("from"),
                   "to": payload.get("to"), "sent": payload.get("sent"), "sent_at": payload.get("sent_at"),
                   "message_type": payload.get("message_type"), "message_content": payload.get("message_content"),
                   "cloud_public_id": payload.get("cloud_public_id"), }
            messages_to_send_col.update_one({"_id": _id}, {"$set": doc}, upsert=True)
            return True

        ok = await run_blocking(do_write)
        return {"ok": ok}
    except Exception as e:
        print(f"[push_scheduled_message] error: {e}")
        return {"ok": False, "error": str(e)}






class MessagePayload(BaseModel):
    id: str = Field(..., alias="_id")
    msg_type: str
    msg_content: str
    is_edited: bool = False
    sent_by: str
    sent_to: str
    sent_at: str
    received_at: Optional[str] = None      # ignored on input, server sets it
    delete_from_me: Optional[Any] = None   # ignored on input, server builds the per-user dict
    delete_from_all: bool = False
    thumbnail_url: Optional[str] = None
    width: Optional[int] = None
    height: Optional[int] = None
    origin_mac: Optional[str] = None       # which device sent it (never stored)

    class Config:
        allow_population_by_field_name = True





@app.post("/send_message")
async def send_message(payload: MessagePayload):
    msg = payload.dict(by_alias=True)
    origin_mac = msg.pop("origin_mac", None)

    _mark_api_push("new", msg["_id"])

    received_at = await run_blocking(_save_message_blocking, msg)
    msg["received_at"] = received_at
    msg["delete_from_me"] = _default_dfm(msg["sent_by"], msg["sent_to"])

    note = {"type": "new_message", "message": msg}
    await send_to_user(msg["sent_to"], note)
    if msg["sent_by"] != msg["sent_to"]:
        await send_to_user(msg["sent_by"], note, exclude_mac=origin_mac)

    return {"ok": True, "received_at": received_at}





@app.get("/get_messages_page")
async def get_messages_page(user_a: str, user_b: str, before_sent_at: Optional[str] = None, limit: int = 20):
    docs = await run_blocking(_get_messages_page_blocking, user_a, user_b, before_sent_at, limit)
    return {"ok": True, "messages": docs, "has_more": len(docs) == limit}


@app.post("/mark_delete_for_all/{msg_id}")
async def mark_delete_for_all(msg_id: str, request: Request):
    body = await request.json()

    _mark_api_push("del_all", msg_id)          # NEW

    await run_blocking(_mark_deleted_for_all_blocking, msg_id)
    note = {"type": "message_deleted_all", "_id": msg_id}

    peer = body.get("notify_email")
    if peer:
        await send_to_user(peer, note)
    who = body.get("who")
    if who:
        await send_to_user(who, note, exclude_mac=body.get("origin_mac"))
    return {"ok": True}



@app.post("/mark_delete_for_me/{msg_id}")
async def mark_delete_for_me(msg_id: str, request: Request):
    body = await request.json()
    who = body.get("who")

    _mark_api_push("del_me", f"{msg_id}:{who}")   # NEW

    await run_blocking(_mark_deleted_for_me_blocking, msg_id, who)
    if who:
        await send_to_user(who, {"type": "message_deleted_me", "_id": msg_id},
                           exclude_mac=body.get("origin_mac"))
    return {"ok": True}




@app.post("/edit_message/{msg_id}")
async def edit_message(msg_id: str, request: Request):
    body = await request.json()

    _mark_api_push("edit", msg_id)             # NEW

    await run_blocking(_edit_message_blocking, msg_id, body.get("msg_content"))
    note = {"type": "message_edited", "_id": msg_id, "msg_content": body.get("msg_content")}

    peer = body.get("notify_email")
    if peer:
        await send_to_user(peer, note)
    who = body.get("who")
    if who:
        await send_to_user(who, note, exclude_mac=body.get("origin_mac"))
    return {"ok": True}




@app.post("/upload_chat_image")
async def upload_chat_image(msg_id: str = Form(...), file: UploadFile = File(...)):
    try:
        contents = await file.read()

        def do_upload():
            full = cloudinary.uploader.upload(contents, public_id=f"{msg_id}_full", folder="chat_images",
                                              overwrite=True, resource_type="image")
            thumb = cloudinary.uploader.upload(contents, public_id=f"{msg_id}_thumb", folder="chat_images",
                                               overwrite=True, resource_type="image",
                                               transformation=[{"width": 1200, "quality": "auto:good", "crop": "limit"}])
            return full, thumb

        full, thumb = await run_blocking(do_upload)
        return {"ok": True, "url": full.get("secure_url"), "thumbnail_url": thumb.get("secure_url")}
    except Exception as e:
        return {"ok": False, "error": str(e)}


# CHANGED: multi-device websocket endpoint
@app.websocket("/ws/{email}/{mac_id}")
async def websocket_endpoint(websocket: WebSocket, email: str, mac_id: str):
    await websocket.accept()

    conns = connected_users.setdefault(email, {})
    was_empty = not conns                      # True only for the user's first device
    old = conns.get(mac_id)
    conns[mac_id] = websocket
    if old is not None and old is not websocket:   # same device reconnecting: drop its old socket
        try:
            await old.close()
        except Exception:
            pass

    if was_empty:
        await mark_user_online(email)
    asyncio.create_task(run_blocking(_record_login_blocking, email, mac_id, now_utc()))
    if was_empty:
        await broadcast_presence_to_friends(email)

    try:
        while True:
            try:
                data = await asyncio.wait_for(websocket.receive_json(), timeout=HEARTBEAT_IDLE_TIMEOUT)
            except asyncio.TimeoutError:
                try:
                    await websocket.send_json({"type": "ping"})
                    reply = await asyncio.wait_for(websocket.receive_json(), timeout=HEARTBEAT_PING_TIMEOUT)
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
        await cleanup_user(email, mac_id, websocket)


@app.get("/friends_last_seen")
async def friends_last_seen(emails: str):
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
            result[email] = {"is_online": state.get("is_online", False), "last_seen_at": state.get("last_seen_at"), }

    if missing:
        fetched = await run_blocking(_load_missing_presence_blocking, missing)
        for email, state in fetched.items():
            presence_state[email] = state
            result[email] = {"is_online": state.get("is_online", False), "last_seen_at": state.get("last_seen_at"), }

    return result


@app.get("/")
@app.head("/")
def root():
    return {"success": True}


@app.get("/status")
async def get_status(me: str, friend: str):
    out = await run_blocking(_get_status_blocking, me, friend)

    mem = last_clicked_mem.get(get_chat_id(me, friend), {}).get(sanitize_email(friend))
    if mem:
        out["friend_last_clicked"] = mem.isoformat()

    s = presence_state.get(friend, {})
    out["is_online"] = s.get("is_online", out.get("is_online", False))
    out["last_seen_at"] = s.get("last_seen_at", out.get("last_seen_at"))
    out["is_on_same_chat_as_me"] = bool(s.get("is_online") and s.get("user_is_on") == me)
    out.pop("user_is_on", None)

    # Mongo datetimes are not JSON friendly
    for k in ("last_seen_at", "my_last_clicked", "friend_last_clicked"):
        v = out.get(k)
        if isinstance(v, datetime):
            out[k] = v.isoformat()
    return out


@app.get("/check_mac/{email}")
async def check_mac(email: str, mac_id: str):
    registered = await run_blocking(_get_registered_mac_blocking, email)
    return {"match": registered == mac_id, "registered_mac": registered}


# CHANGED: counts devices, not users
@app.get("/health")
def health():
    return {"ok": True,
            "tracked_users": len(presence_state),
            "connected": sum(len(v) for v in connected_users.values()),
            "tracked_friend_graph_entries": len(friend_watchers), }


@app.get("/get_scheduled_messages")
async def get_scheduled_messages(from_user: str):
    try:
        def do_read():
            docs = list(messages_to_send_col.find({"from": from_user}))
            for d in docs:
                d["_id"] = str(d["_id"])
            return docs

        docs = await run_blocking(do_read)
        return {"ok": True, "messages": docs}
    except Exception as e:
        print(f"[get_scheduled_messages] error: {e}")
        return {"ok": False, "error": str(e), "messages": []}


@app.post("/upload_scheduled_image")
async def upload_scheduled_image(widget_id: str = Form(...), file: UploadFile = File(...)):
    try:
        contents = await file.read()
        result = cloudinary.uploader.upload(contents, public_id=widget_id, folder="scheduled_msgs", overwrite=True,
                                            resource_type="image")
        return {"ok": True, "public_id": result.get("public_id"), "url": result.get("secure_url")}
    except Exception as e:
        return {"ok": False, "error": str(e)}


class DeleteImageRequest(BaseModel):
    public_id: str


@app.post("/delete_scheduled_image")
async def delete_scheduled_image(req: DeleteImageRequest):
    try:
        result = cloudinary.uploader.destroy(req.public_id, resource_type="image")
        return {"ok": True, "result": result.get("result")}
    except Exception as e:
        return {"ok": False, "error": str(e)}



def _get_last_visible_message_blocking(user_a: str, user_b: str, viewer: str) -> Optional[dict]:
    match = {
        "$or": [{"sent_by": user_a, "sent_to": user_b}, {"sent_by": user_b, "sent_to": user_a}],
        _dfm_path(viewer): {"$ne": True},
    }
    pipeline = [
        {"$match": match},
        {"$addFields": {"_eff": _eff_expr(viewer)}},
        {"$sort": {"_eff": -1}},
        {"$limit": 1},
    ]
    docs = list(messages_col.aggregate(pipeline, allowDiskUse=True))
    return docs[0] if docs else None




def _build_last_messages_preview_blocking(user_email: str) -> Dict[str, Optional[dict]]:
    friend_doc = all_type_list_col.find_one({"_id": user_email})
    friend_list = friend_doc.get("friend_list", []) if friend_doc else []

    result: Dict[str, Optional[dict]] = {}
    for friend_email in friend_list:
        msg = _get_last_visible_message_blocking(user_email, friend_email, user_email)
        if msg is None:
            result[friend_email] = None
            continue

        is_deleted_all = bool(msg.get("delete_from_all", False))
        entry = {
            "_id": msg.get("_id"),
            "sent_by": msg.get("sent_by"),
            "sent_to": msg.get("sent_to"),
            "sent_at": msg.get("sent_at"),
            "received_at": msg.get("received_at"),   # may be None on old messages
            "delete_from_all": is_deleted_all,
        }

        if is_deleted_all:
            entry["msg_type"] = "delete_from_everyone"
            entry["msg_content"] = None
        else:
            msg_type = msg.get("msg_type")
            entry["msg_type"] = msg_type
            if msg_type == "text":
                entry["msg_content"] = (msg.get("msg_content") or "")[:300]
            else:
                entry["msg_content"] = None

        result[friend_email] = entry

    return result


@app.get("/last_messages_preview")
async def last_messages_preview(user_email: str):
    try:
        data = await run_blocking(_build_last_messages_preview_blocking, user_email)
        friends = await run_blocking(_get_friend_list_blocking, user_email)
        unread = await run_blocking(_build_unread_counts_blocking, user_email, friends)
        return {"ok": True, "friend_list_last_messages": data, "unread_counts": unread}
    except Exception as e:
        print(f"[last_messages_preview] error: {e}")
        return {"ok": False, "error": str(e),
                "friend_list_last_messages": {}, "unread_counts": {}}



def _sync_incoming_blocking(user_email: str, since, limit: int = 200):
    if not since:
        return [], False
    q = {"$or": [{"sent_to": user_email}, {"sent_by": user_email}],
         "received_at": {"$gt": since},
         _dfm_path(user_email): {"$ne": True}}
    docs = list(messages_col.find(q).sort("received_at", 1).limit(limit + 1))
    return docs[:limit], len(docs) > limit





@app.get("/sync_incoming")
async def sync_incoming(user_email: str, since: Optional[str] = None, limit: int = 200):
    docs, has_more = await run_blocking(_sync_incoming_blocking, user_email, since, limit)
    return {"ok": True, "messages": docs, "has_more": has_more}
