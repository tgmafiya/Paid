from __future__ import annotations
import asyncio
import json
import logging
import os
import secrets
import time
from collections import defaultdict
from pathlib import Path
from typing import Any

from aiohttp import web
from dotenv import load_dotenv

from aiogram import Bot, Dispatcher, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ChatMemberStatus, ChatType, ParseMode
from aiogram.exceptions import (
    TelegramBadRequest,
    TelegramForbiddenError,
    TelegramNetworkError,
    TelegramRetryAfter,
)
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
    Update,
)

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
ADMIN_ID_RAW = os.getenv("ADMIN_ID", "").strip()
PORT = int(os.getenv("PORT", "10000"))
DATA_FILE = Path(os.getenv("DATA_FILE", "data.json"))
RENDER_URL = os.getenv("RENDER_URL", "").strip().rstrip("/")
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "").strip()

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is missing.")
if not ADMIN_ID_RAW.lstrip("-").isdigit():
    raise RuntimeError("ADMIN_ID must be numeric.")
if not RENDER_URL:
    raise RuntimeError("RENDER_URL is missing.")
if not WEBHOOK_SECRET or WEBHOOK_SECRET == "change-this-secret":
    raise RuntimeError("WEBHOOK_SECRET must be changed in Render Environment.")

ADMIN_ID = int(ADMIN_ID_RAW)
WEBHOOK_PATH = f"/telegram/webhook/{WEBHOOK_SECRET}"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
log = logging.getLogger("route-copy-bot")

DEFAULT_SETTINGS = {
    "interval": 1.0,
    "retries": 8,
    "album_wait": 1.5,
    "max_queue": 10000,
    "enabled": True,
}

def fresh_db() -> dict[str, Any]:
    return {
        "settings": DEFAULT_SETTINGS.copy(),
        "sources": [],
        "destinations": [],
        "routes": {},
        "stats": {
            "received": 0,
            "enqueued": 0,
            "sent": 0,
            "failed": 0,
            "albums": 0,
            "last_received": None,
            "last_sent": None,
            "last_error": None,
        },
        "seen": {},
    }

def load_db() -> dict[str, Any]:
    if not DATA_FILE.exists():
        return fresh_db()
    try:
        raw = json.loads(DATA_FILE.read_text(encoding="utf-8"))
    except Exception:
        log.exception("data.json is unreadable; starting fresh")
        return fresh_db()

    db = fresh_db()
    if isinstance(raw.get("settings"), dict):
        db["settings"].update(raw["settings"])
    if isinstance(raw.get("stats"), dict):
        db["stats"].update(raw["stats"])
    if isinstance(raw.get("seen"), dict):
        db["seen"] = raw["seen"]

    old_sources = raw.get("sources", [])
    old_destinations = raw.get("destinations", [])
    if isinstance(old_sources, list):
        db["sources"] = [x for x in old_sources if isinstance(x, dict)]
    if isinstance(old_destinations, list):
        db["destinations"] = [x for x in old_destinations if isinstance(x, dict)]

    routes = raw.get("routes")
    if isinstance(routes, dict):
        db["routes"] = routes
    else:
        # Compatibility with the old global source -> all destinations format.
        for source in db["sources"]:
            sid = str(source.get("id", ""))
            if sid:
                db["routes"][sid] = {
                    "source": source,
                    "destinations": list(db["destinations"]),
                }

    # Normalize and rebuild registries.
    source_map: dict[str, dict[str, Any]] = {}
    dest_map: dict[str, dict[str, Any]] = {}

    for source in db["sources"]:
        sid = str(source.get("id", ""))
        if sid:
            source_map[sid] = source

    for sid, route in list(db["routes"].items()):
        if not isinstance(route, dict):
            db["routes"].pop(sid, None)
            continue
        source = route.get("source")
        if isinstance(source, dict):
            source_map[str(sid)] = source
        else:
            source = source_map.get(str(sid))
        route["source"] = source or {
            "id": str(sid), "title": str(sid), "username": ""
        }
        dests = route.get("destinations", [])
        route["destinations"] = [
            x for x in dests if isinstance(x, dict)
        ] if isinstance(dests, list) else []
        for dest in route["destinations"]:
            did = str(dest.get("id", ""))
            if did:
                dest_map[did] = dest

    for dest in db["destinations"]:
        did = str(dest.get("id", ""))
        if did:
            dest_map[did] = dest

    db["sources"] = list(source_map.values())
    db["destinations"] = list(dest_map.values())
    return db

db = load_db()
db_lock = asyncio.Lock()
seen_lock = asyncio.Lock()

async def save_db() -> None:
    async with db_lock:
        DATA_FILE.parent.mkdir(parents=True, exist_ok=True)
        tmp = DATA_FILE.with_suffix(".tmp")
        tmp.write_text(
            json.dumps(db, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        tmp.replace(DATA_FILE)

router = Router()
bot: Bot | None = None
dp: Dispatcher | None = None
server_runner: web.AppRunner | None = None

route_queues: dict[str, asyncio.Queue] = {}
route_tasks: dict[str, asyncio.Task] = {}
album_buffer: dict[tuple[str, str], list[int]] = defaultdict(list)
album_tasks: dict[tuple[str, str], asyncio.Task] = {}

class AddSource(StatesGroup):
    value = State()

class AddDestination(StatesGroup):
    value = State()

class SetInterval(StatesGroup):
    value = State()

class SetRetries(StatesGroup):
    value = State()

class SetAlbumWait(StatesGroup):
    value = State()

class SetQueue(StatesGroup):
    value = State()

def is_admin(obj: Message | CallbackQuery) -> bool:
    return bool(obj.from_user and obj.from_user.id == ADMIN_ID)

def source_ids() -> set[str]:
    return {str(x["id"]) for x in db["sources"] if isinstance(x, dict) and "id" in x}

def destination_ids() -> set[str]:
    return {str(x["id"]) for x in db["destinations"] if isinstance(x, dict) and "id" in x}

def get_source(sid: str) -> dict[str, Any] | None:
    return next((x for x in db["sources"] if str(x.get("id")) == str(sid)), None)

def get_destination(did: str) -> dict[str, Any] | None:
    return next((x for x in db["destinations"] if str(x.get("id")) == str(did)), None)

def name_of(item: dict[str, Any] | None) -> str:
    if not item:
        return "Unknown"
    return str(item.get("title") or item.get("username") or item.get("id", "Unknown"))

def route_dests(sid: str) -> list[dict[str, Any]]:
    route = db["routes"].get(str(sid), {})
    if not isinstance(route, dict):
        return []
    value = route.get("destinations", [])
    return value if isinstance(value, list) else []

def route_dest_ids(sid: str) -> set[str]:
    return {str(x["id"]) for x in route_dests(sid) if isinstance(x, dict) and "id" in x}

def route_key(sid: str, did: str) -> str:
    return f"{sid}|{did}"

def split_key(key: str) -> tuple[str, str]:
    return key.split("|", 1)

def chat_type(chat: Any) -> str:
    value = getattr(chat, "type", "")
    return getattr(value, "value", str(value))

def member_status(member: Any) -> str:
    value = getattr(member, "status", "")
    return getattr(value, "value", str(value))

async def get_chat(value: str) -> Any:
    if bot is None:
        raise RuntimeError("Bot is not initialized.")
    value = value.strip()
    if not value:
        raise ValueError("Username or chat ID is empty.")
    lookup: Any = int(value) if value.lstrip("-").isdigit() else value
    return await bot.get_chat(lookup)

async def resolve_source(value: str) -> dict[str, str]:
    if bot is None:
        raise RuntimeError("Bot is not initialized.")
    chat = await get_chat(value)
    if chat_type(chat) != "channel":
        raise ValueError("Source must be a Telegram channel.")
    member = await bot.get_chat_member(chat.id, bot.id)
    if member_status(member) not in {"administrator", "creator"}:
        raise PermissionError("Bot must be administrator in the source channel.")
    return {
        "id": str(chat.id),
        "title": str(chat.title or chat.username or chat.id),
        "username": str(chat.username or ""),
    }

async def resolve_destination(value: str) -> dict[str, str]:
    if bot is None:
        raise RuntimeError("Bot is not initialized.")
    chat = await get_chat(value)
    ctype = chat_type(chat)
    if ctype not in {"channel", "supergroup", "group"}:
        raise ValueError("Destination must be a channel or group.")
    member = await bot.get_chat_member(chat.id, bot.id)
    status = member_status(member)
    if status not in {"administrator", "creator"}:
        raise PermissionError("Bot must be administrator in the destination.")
    if ctype == "channel" and status == "administrator":
        if getattr(member, "can_post_messages", True) is False:
            raise PermissionError("Bot needs Post Messages permission.")
    return {
        "id": str(chat.id),
        "title": str(chat.title or chat.username or chat.id),
        "username": str(chat.username or ""),
    }

def home_text() -> str:
    s, st = db["settings"], db["stats"]
    status = "🟢 RUNNING" if s["enabled"] else "🔴 STOPPED"
    return (
        "🤖 <b>Route Channel Copy Bot</b>\n\n"
        f"Status: <b>{status}</b>\n"
        f"Sources: <b>{len(db['sources'])}</b>\n"
        f"Destinations: <b>{len(db['destinations'])}</b>\n"
        f"Routes: <b>{sum(bool(route_dests(str(x['id']))) for x in db['sources'])}</b>\n\n"
        f"⏱ Interval: <b>{s['interval']}s</b>\n"
        f"📦 Album wait: <b>{s['album_wait']}s</b>\n"
        f"🔁 Retries: <b>{s['retries']}</b>\n"
        f"📚 Queue limit: <b>{s['max_queue']}</b>\n\n"
        f"📥 Received: <b>{st['received']}</b>\n"
        f"📦 Enqueued: <b>{st['enqueued']}</b>\n"
        f"📤 Sent: <b>{st['sent']}</b>\n"
        f"❌ Failed: <b>{st['failed']}</b>"
    )

def home_kb() -> InlineKeyboardMarkup:
    enabled = db["settings"]["enabled"]
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⏹ Stop" if enabled else "▶️ Start", callback_data="toggle")],
        [
            InlineKeyboardButton(text="🔀 Routes", callback_data="sources"),
            InlineKeyboardButton(text="📤 Destinations", callback_data="destinations"),
        ],
        [
            InlineKeyboardButton(text="⚙️ Settings", callback_data="settings"),
            InlineKeyboardButton(text="📊 Status", callback_data="status"),
        ],
        [InlineKeyboardButton(text="🔄 Refresh", callback_data="home")],
    ])

def sources_kb() -> InlineKeyboardMarkup:
    rows = []
    for i, source in enumerate(db["sources"]):
        sid = str(source["id"])
        rows.append([InlineKeyboardButton(
            text=f"🔀 {name_of(source)} [{len(route_dests(sid))}]",
            callback_data=f"route:{i}",
        )])
    rows += [
        [InlineKeyboardButton(text="➕ Add Source", callback_data="add_source")],
        [InlineKeyboardButton(text="⬅️ Back", callback_data="home")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)

def route_kb(sid: str) -> InlineKeyboardMarkup:
    selected = route_dest_ids(sid)
    rows = []
    for i, dest in enumerate(db["destinations"]):
        did = str(dest["id"])
        rows.append([InlineKeyboardButton(
            text=("✅ " if did in selected else "⬜ ") + name_of(dest),
            callback_data=f"route_dest:{sid}:{i}",
        )])
    rows += [
        [InlineKeyboardButton(text="➕ Add Destination", callback_data=f"new_dest_for:{sid}")],
        [InlineKeyboardButton(text="❌ Remove Source", callback_data=f"remove_source:{sid}")],
        [InlineKeyboardButton(text="⬅️ Routes", callback_data="sources")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)

def dest_kb() -> InlineKeyboardMarkup:
    rows = []
    for i, dest in enumerate(db["destinations"]):
        did = str(dest["id"])
        used = sum(did in route_dest_ids(str(s["id"])) for s in db["sources"])
        rows.append([InlineKeyboardButton(
            text=f"❌ {name_of(dest)} [{used} routes]",
            callback_data=f"delete_destination:{i}",
        )])
    rows += [
        [InlineKeyboardButton(text="➕ Add Destination", callback_data="add_destination")],
        [InlineKeyboardButton(text="⬅️ Back", callback_data="home")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)

def settings_kb() -> InlineKeyboardMarkup:
    s = db["settings"]
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"⏱ Interval: {s['interval']}s", callback_data="set:interval")],
        [InlineKeyboardButton(text=f"📦 Album wait: {s['album_wait']}s", callback_data="set:album")],
        [InlineKeyboardButton(text=f"🔁 Retries: {s['retries']}", callback_data="set:retries")],
        [InlineKeyboardButton(text=f"📚 Queue: {s['max_queue']}", callback_data="set:queue")],
        [InlineKeyboardButton(text="⬅️ Back", callback_data="home")],
    ])

async def start_worker(sid: str, did: str) -> None:
    key = route_key(sid, did)
    task = route_tasks.get(key)
    if task and not task.done():
        return
    if key not in route_queues:
        route_queues[key] = asyncio.Queue(
            maxsize=max(100, int(db["settings"].get("max_queue", 10000)))
        )
    route_tasks[key] = asyncio.create_task(route_worker(sid, did))

async def stop_worker(sid: str, did: str) -> None:
    key = route_key(sid, did)
    task = route_tasks.pop(key, None)
    if task:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
    route_queues.pop(key, None)

async def ensure_workers() -> None:
    wanted = set()
    if db["settings"]["enabled"]:
        for sid, route in db["routes"].items():
            if not isinstance(route, dict):
                continue
            for dest in route.get("destinations", []):
                if isinstance(dest, dict) and dest.get("id") is not None:
                    did = str(dest["id"])
                    wanted.add(route_key(str(sid), did))
                    await start_worker(str(sid), did)
    for key in list(route_tasks):
        if key not in wanted:
            sid, did = split_key(key)
            await stop_worker(sid, did)

async def copy_message_retry(sid: str, did: str, mid: int) -> bool:
    if bot is None:
        return False
    retries = max(0, int(db["settings"].get("retries", 8)))
    for attempt in range(retries + 1):
        try:
            log.info("COPY %s -> %s message=%s", sid, did, mid)
            await bot.copy_message(
                chat_id=int(did),
                from_chat_id=int(sid),
                message_id=int(mid),
            )
            log.info("COPY OK %s -> %s message=%s", sid, did, mid)
            return True
        except TelegramRetryAfter as exc:
            await asyncio.sleep(int(exc.retry_after) + 1)
        except TelegramNetworkError as exc:
            db["stats"]["last_error"] = str(exc)
            if attempt >= retries:
                return False
            await asyncio.sleep(min(2 ** attempt, 30))
        except TelegramForbiddenError as exc:
            db["stats"]["last_error"] = str(exc)
            log.error("Forbidden %s -> %s: %s", sid, did, exc)
            return False
        except TelegramBadRequest as exc:
            db["stats"]["last_error"] = str(exc)
            log.error("Bad request %s -> %s message=%s: %s", sid, did, mid, exc)
            return False
        except Exception as exc:
            db["stats"]["last_error"] = str(exc)
            if attempt >= retries:
                log.exception("Copy failed")
                return False
            await asyncio.sleep(min(2 ** attempt, 30))
    return False

async def route_worker(sid: str, did: str) -> None:
    key = route_key(sid, did)
    queue = route_queues[key]
    log.info("WORKER STARTED %s -> %s", sid, did)
    while True:
        item = await queue.get()
        try:
            mids = [int(x) for x in item.get("message_ids", [])]
            for mid in mids:
                ok = await copy_message_retry(sid, did, mid)
                if ok:
                    db["stats"]["sent"] += 1
                    db["stats"]["last_sent"] = int(time.time())
                else:
                    db["stats"]["failed"] += 1
                interval = float(db["settings"].get("interval", 1.0))
                if interval > 0:
                    await asyncio.sleep(interval)
            await save_db()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            db["stats"]["failed"] += 1
            db["stats"]["last_error"] = str(exc)
            log.exception("Worker item error; worker continues")
        finally:
            queue.task_done()

async def queue_delivery(sid: str, mids: list[int], album: bool) -> None:
    for dest in route_dests(sid):
        did = str(dest["id"])
        await start_worker(sid, did)
        await route_queues[route_key(sid, did)].put({
            "message_ids": mids,
            "album": album,
            "time": int(time.time()),
        })
        db["stats"]["enqueued"] += len(mids)
    await save_db()

async def flush_album(sid: str, gid: str) -> None:
    key = (sid, gid)
    try:
        await asyncio.sleep(float(db["settings"].get("album_wait", 1.5)))
        mids = sorted(set(album_buffer.pop(key, [])))
        if mids:
            db["stats"]["albums"] += 1
            await queue_delivery(sid, mids, True)
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        db["stats"]["last_error"] = str(exc)
        log.exception("Album flush failed")
    finally:
        album_tasks.pop(key, None)

@router.channel_post()
async def channel_post(message: Message) -> None:
    sid = str(message.chat.id)
    mid = int(message.message_id)
    log.info("NEW POST source=%s message=%s type=%s", sid, mid, message.content_type)

    if sid not in source_ids() or not db["settings"]["enabled"]:
        return
    if not route_dests(sid):
        return

    dedupe = f"{sid}:{mid}"
    async with seen_lock:
        if dedupe in db["seen"]:
            return
        db["seen"][dedupe] = int(time.time())
        if len(db["seen"]) > 20000:
            old = sorted(db["seen"].items(), key=lambda x: x[1])[:5000]
            for k, _ in old:
                db["seen"].pop(k, None)

    db["stats"]["received"] += 1
    db["stats"]["last_received"] = int(time.time())

    gid = message.media_group_id
    if gid:
        key = (sid, str(gid))
        album_buffer[key].append(mid)
        old = album_tasks.get(key)
        if old:
            old.cancel()
        album_tasks[key] = asyncio.create_task(flush_album(sid, str(gid)))
    else:
        # copy_message copies videos, photos, documents, audio, text, etc.
        await queue_delivery(sid, [mid], False)

@router.message(CommandStart())
async def start_cmd(message: Message, state: FSMContext) -> None:
    await state.clear()
    if not is_admin(message):
        await message.answer("Only admin can access this bot.")
        return
    await message.answer(home_text(), reply_markup=home_kb())

@router.message(Command("admin"))
async def admin_cmd(message: Message, state: FSMContext) -> None:
    await state.clear()
    if not is_admin(message):
        await message.answer("Only admin can access this bot.")
        return
    await message.answer(home_text(), reply_markup=home_kb())

@router.message(Command("cancel"))
async def cancel_cmd(message: Message, state: FSMContext) -> None:
    await state.clear()
    if is_admin(message):
        await message.answer("❌ Cancelled.", reply_markup=home_kb())

@router.callback_query()
async def callbacks(q: CallbackQuery, state: FSMContext) -> None:
    if not is_admin(q):
        await q.answer("Access denied.", show_alert=True)
        return
    if not q.message:
        await q.answer()
        return
    data = q.data or ""

    if data == "home":
        await state.clear()
        await q.answer()
        await q.message.edit_text(home_text(), reply_markup=home_kb())
        return

    if data == "toggle":
        db["settings"]["enabled"] = not bool(db["settings"]["enabled"])
        await save_db()
        await ensure_workers()
        await q.answer("Started." if db["settings"]["enabled"] else "Stopped.")
        await q.message.edit_text(home_text(), reply_markup=home_kb())
        return

    if data == "sources":
        await q.answer()
        lines = ["🔀 <b>Source → Destination Routes</b>", ""]
        if not db["sources"]:
            lines.append("No source channels configured.")
        for i, s in enumerate(db["sources"], 1):
            sid = str(s["id"])
            lines.append(f"{i}. <b>{name_of(s)}</b>")
            ds = route_dests(sid)
            if ds:
                lines.extend(f"   └ {name_of(d)}" for d in ds)
            else:
                lines.append("   └ ⚠️ No destination")
            lines.append("")
        await q.message.edit_text("\n".join(lines), reply_markup=sources_kb())
        return

    if data.startswith("route:"):
        try:
            s = db["sources"][int(data.split(":", 1)[1])]
        except (ValueError, IndexError):
            await q.answer("Invalid source.", show_alert=True)
            return
        sid = str(s["id"])
        await q.answer()
        await q.message.edit_text(
            f"🔀 <b>Route Configuration</b>\n\n"
            f"Source: <b>{name_of(s)}</b>\n"
            f"<code>{sid}</code>\n\n"
            f"Selected destinations: <b>{len(route_dest_ids(sid))}</b>",
            reply_markup=route_kb(sid),
        )
        return

    if data.startswith("route_dest:"):
        try:
            _, sid, idx = data.split(":", 2)
            dest = db["destinations"][int(idx)]
        except (ValueError, IndexError):
            await q.answer("Invalid destination.", show_alert=True)
            return
        if sid not in source_ids():
            await q.answer("Source not found.", show_alert=True)
            return
        did = str(dest["id"])
        route = db["routes"].setdefault(
            sid, {"source": get_source(sid), "destinations": []}
        )
        current = {str(x["id"]) for x in route.get("destinations", [])}
        if did in current:
            route["destinations"] = [
                x for x in route["destinations"] if str(x["id"]) != did
            ]
            result = "removed"
        else:
            route["destinations"].append(dest)
            result = "added"
        await save_db()
        await ensure_workers()
        await q.answer(f"{name_of(dest)} {result}")
        await q.message.edit_reply_markup(reply_markup=route_kb(sid))
        return

    if data == "destinations":
        await q.answer()
        lines = ["📤 <b>Destinations</b>", ""]
        if not db["destinations"]:
            lines.append("No destinations configured.")
        for i, d in enumerate(db["destinations"], 1):
            did = str(d["id"])
            used = sum(did in route_dest_ids(str(s["id"])) for s in db["sources"])
            lines += [f"{i}. <b>{name_of(d)}</b>", f"Used by: <b>{used}</b> route(s)", f"<code>{did}</code>", ""]
        await q.message.edit_text("\n".join(lines), reply_markup=dest_kb())
        return

    if data == "add_source":
        await q.answer()
        await state.set_state(AddSource.value)
        await q.message.edit_text(
            "📥 <b>Add Source Channel</b>\n\n"
            "Send @username or numeric channel ID.\n"
            "<code>@channelusername</code>\n"
            "<code>-1001234567890</code>\n\n"
            "⚠️ Bot must be administrator in the source."
        )
        return

    if data == "add_destination":
        await q.answer()
        await state.update_data(route_source_id=None)
        await state.set_state(AddDestination.value)
        await q.message.edit_text(
            "📤 <b>Add Destination</b>\n\n"
            "Send @username or numeric channel/group ID.\n\n"
            "⚠️ Bot must be administrator."
        )
        return

    if data.startswith("new_dest_for:"):
        sid = data.split(":", 1)[1]
        if sid not in source_ids():
            await q.answer("Source not found.", show_alert=True)
            return
        await q.answer()
        await state.update_data(route_source_id=sid)
        await state.set_state(AddDestination.value)
        await q.message.edit_text("📤 <b>Add Destination to Route</b>\n\nSend @username or numeric channel/group ID.")
        return

    if data.startswith("remove_source:"):
        sid = data.split(":", 1)[1]
        if sid in source_ids():
            db["sources"] = [x for x in db["sources"] if str(x["id"]) != sid]
            db["routes"].pop(sid, None)
            for key in list(route_tasks):
                rsid, did = split_key(key)
                if rsid == sid:
                    await stop_worker(rsid, did)
            await save_db()
        await q.answer("Source removed.")
        await q.message.edit_text("🔀 <b>Source → Destination Routes</b>", reply_markup=sources_kb())
        return

    if data.startswith("delete_destination:"):
        try:
            dest = db["destinations"].pop(int(data.split(":", 1)[1]))
        except (ValueError, IndexError):
            await q.answer("Invalid destination.", show_alert=True)
            return
        did = str(dest["id"])
        for route in db["routes"].values():
            if isinstance(route, dict):
                route["destinations"] = [x for x in route.get("destinations", []) if str(x["id"]) != did]
        for key in list(route_tasks):
            sid, rd = split_key(key)
            if rd == did:
                await stop_worker(sid, rd)
        await save_db()
        await ensure_workers()
        await q.answer("Destination removed.")
        await q.message.edit_text("📤 <b>Destinations</b>", reply_markup=dest_kb())
        return

    if data == "settings":
        await q.answer()
        await q.message.edit_text("⚙️ <b>Settings</b>", reply_markup=settings_kb())
        return

    if data == "status":
        await q.answer()
        st = db["stats"]
        queues = []
        for key, queue in route_queues.items():
            sid, did = split_key(key)
            queues.append(
                f"• {name_of(get_source(sid))} → {name_of(get_destination(did))}: <b>{queue.qsize()}</b>"
            )
        await q.message.edit_text(
            "📊 <b>Status</b>\n\n"
            f"Running: <b>{db['settings']['enabled']}</b>\n"
            f"Workers: <b>{len(route_tasks)}</b>\n"
            f"Queues: <b>{len(route_queues)}</b>\n\n"
            f"📥 Received: <b>{st['received']}</b>\n"
            f"📦 Enqueued: <b>{st['enqueued']}</b>\n"
            f"📤 Sent: <b>{st['sent']}</b>\n"
            f"❌ Failed: <b>{st['failed']}</b>\n"
            f"🖼 Albums: <b>{st['albums']}</b>\n\n"
            "<b>Queues</b>\n" + ("\n".join(queues) if queues else "-") + "\n\n"
            "<b>Last error</b>\n"
            f"<code>{str(st['last_error'] or '-')[:1200]}</code>",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="⬅️ Back", callback_data="home")]
            ]),
        )
        return

    if data.startswith("set:"):
        kind = data.split(":", 1)[1]
        mapping = {
            "interval": (SetInterval.value, "⏱ <b>Interval</b>\n\nEnter seconds: 0.1–3600."),
            "album": (SetAlbumWait.value, "📦 <b>Album Wait</b>\n\nEnter seconds: 0.2–10."),
            "retries": (SetRetries.value, "🔁 <b>Retries</b>\n\nEnter integer: 0–20."),
            "queue": (SetQueue.value, "📚 <b>Queue Limit</b>\n\nEnter integer: 100–50000."),
        }
        if kind in mapping:
            await q.answer()
            await state.set_state(mapping[kind][0])
            await q.message.edit_text(mapping[kind][1])
            return

    await q.answer()

@router.message(AddSource.value)
async def add_source(message: Message, state: FSMContext) -> None:
    if not is_admin(message):
        return
    try:
        source = await resolve_source((message.text or "").strip())
        sid = str(source["id"])
        if sid in source_ids():
            await message.answer("⚠️ Source already exists.", reply_markup=sources_kb())
        else:
            db["sources"].append(source)
            db["routes"][sid] = {"source": source, "destinations": []}
            await save_db()
            await message.answer(
                f"✅ <b>Source Added</b>\n\n<b>{name_of(source)}</b>\n<code>{sid}</code>\n\n"
                "📡 Only NEW posts will be monitored.\nSelect destinations.",
                reply_markup=route_kb(sid),
            )
    except Exception as exc:
        log.exception("Add source failed")
        await message.answer(f"❌ <b>Could not add source</b>\n\n<code>{str(exc)[:1000]}</code>")
    finally:
        await state.clear()

@router.message(AddDestination.value)
async def add_destination(message: Message, state: FSMContext) -> None:
    if not is_admin(message):
        return
    data = await state.get_data()
    route_sid = data.get("route_source_id")
    try:
        dest = await resolve_destination((message.text or "").strip())
        did = str(dest["id"])
        if did not in destination_ids():
            db["destinations"].append(dest)
        else:
            dest = get_destination(did) or dest

        if route_sid and route_sid in source_ids():
            route = db["routes"].setdefault(
                route_sid, {"source": get_source(route_sid), "destinations": []}
            )
            if did not in {str(x["id"]) for x in route["destinations"]}:
                route["destinations"].append(dest)
            await save_db()
            await ensure_workers()
            await message.answer(
                f"✅ <b>Destination Added</b>\n\n"
                f"Source: <b>{name_of(get_source(route_sid))}</b>\n"
                f"Destination: <b>{name_of(dest)}</b>\n\n🚀 Route active.",
                reply_markup=route_kb(route_sid),
            )
        else:
            await save_db()
            await message.answer(
                f"✅ <b>Destination Added</b>\n\n<b>{name_of(dest)}</b>\n\n"
                "Open Routes and select its source(s).",
                reply_markup=dest_kb(),
            )
    except Exception as exc:
        log.exception("Add destination failed")
        await message.answer(f"❌ <b>Could not add destination</b>\n\n<code>{str(exc)[:1000]}</code>")
    finally:
        await state.clear()

@router.message(SetInterval.value)
async def set_interval(message: Message, state: FSMContext) -> None:
    try:
        value = float((message.text or "").strip())
        if not 0.1 <= value <= 3600:
            raise ValueError
        db["settings"]["interval"] = value
        await save_db()
        await message.answer(f"✅ Interval: <b>{value}s</b>", reply_markup=settings_kb())
    except ValueError:
        await message.answer("❌ Enter 0.1–3600.")
        return
    finally:
        await state.clear()

@router.message(SetAlbumWait.value)
async def set_album(message: Message, state: FSMContext) -> None:
    try:
        value = float((message.text or "").strip())
        if not 0.2 <= value <= 10:
            raise ValueError
        db["settings"]["album_wait"] = value
        await save_db()
        await message.answer(f"✅ Album wait: <b>{value}s</b>", reply_markup=settings_kb())
    except ValueError:
        await message.answer("❌ Enter 0.2–10.")
        return
    finally:
        await state.clear()

@router.message(SetRetries.value)
async def set_retries(message: Message, state: FSMContext) -> None:
    try:
        value = int((message.text or "").strip())
        if not 0 <= value <= 20:
            raise ValueError
        db["settings"]["retries"] = value
        await save_db()
        await message.answer("✅ Retries updated.", reply_markup=settings_kb())
    except ValueError:
        await message.answer("❌ Enter 0–20.")
        return
    finally:
        await state.clear()

@router.message(SetQueue.value)
async def set_queue(message: Message, state: FSMContext) -> None:
    try:
        value = int((message.text or "").strip())
        if not 100 <= value <= 50000:
            raise ValueError
        db["settings"]["max_queue"] = value
        await save_db()
        await message.answer("✅ Queue limit updated.", reply_markup=settings_kb())
    except ValueError:
        await message.answer("❌ Enter 100–50000.")
        return
    finally:
        await state.clear()

async def health(request: web.Request) -> web.Response:
    return web.json_response({
        "ok": True,
        "mode": "webhook",
        "running": bool(db["settings"]["enabled"]),
        "sources": len(db["sources"]),
        "destinations": len(db["destinations"]),
        "workers": len(route_tasks),
        "queues": len(route_queues),
        "received": db["stats"]["received"],
        "enqueued": db["stats"]["enqueued"],
        "sent": db["stats"]["sent"],
        "failed": db["stats"]["failed"],
        "time": int(time.time()),
    })

async def webhook(request: web.Request) -> web.Response:
    incoming = request.headers.get("X-Telegram-Bot-Api-Secret-Token", "")
    if not secrets.compare_digest(incoming, WEBHOOK_SECRET):
        return web.Response(status=403, text="Forbidden")
    try:
        raw = await request.read()
        if not raw:
            return web.Response(status=400, text="Empty update")
        update = Update.model_validate_json(raw)
        if not bot or not dp:
            return web.Response(status=503, text="Bot not ready")
        await dp.feed_update(bot, update)
        return web.Response(text="OK")
    except Exception:
        log.exception("Webhook update error")
        return web.Response(status=500, text="ERROR")

async def start_server() -> None:
    global server_runner
    app = web.Application()
    app.router.add_get("/", health)
    app.router.add_get("/health", health)
    app.router.add_get("/status", health)
    app.router.add_post(WEBHOOK_PATH, webhook)
    server_runner = web.AppRunner(app)
    await server_runner.setup()
    site = web.TCPSite(server_runner, "0.0.0.0", PORT)
    await site.start()
    log.info("HTTP server started on port %s", PORT)

async def setup_webhook() -> None:
    if bot is None:
        return
    url = RENDER_URL + WEBHOOK_PATH
    await bot.set_webhook(
        url=url,
        secret_token=WEBHOOK_SECRET,
        allowed_updates=["message", "callback_query", "channel_post"],
        drop_pending_updates=False,
    )
    info = await bot.get_webhook_info()
    log.info("Webhook=%s pending=%s", info.url, info.pending_update_count)
    if info.last_error_message:
        log.error("Telegram webhook error: %s", info.last_error_message)

async def shutdown() -> None:
    for task in list(album_tasks.values()):
        task.cancel()
    for task in list(album_tasks.values()):
        try:
            await task
        except asyncio.CancelledError:
            pass
    album_tasks.clear()

    for key in list(route_tasks):
        try:
            sid, did = split_key(key)
            await stop_worker(sid, did)
        except Exception:
            log.exception("Worker shutdown error: %s", key)

    if server_runner:
        await server_runner.cleanup()

async def main() -> None:
    global bot, dp
    bot = Bot(
        token=BOT_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )
    dp = Dispatcher()
    dp.include_router(router)

    me = await bot.get_me()
    log.info("Bot connected @%s | id=%s", me.username, me.id)

    await start_server()
    await setup_webhook()
    await ensure_workers()

    log.info("READY: webhook + channel_post + copy_message")
    await asyncio.Event().wait()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
