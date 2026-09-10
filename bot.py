from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from contextlib import suppress
from pathlib import Path
from typing import Any

from aiohttp import web
from aiogram import Bot, Dispatcher, F, Router
from aiogram.filters import CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)
from aiogram.utils.keyboard import InlineKeyboardBuilder
from dotenv import load_dotenv

from telethon import TelegramClient
from telethon.errors import (
    ChatAdminRequiredError,
    ChannelPrivateError,
    FloodWaitError,
    RPCError,
    UserDeactivatedBanError,
)
from telethon.sessions import StringSession


# ============================================================
# ENV
# ============================================================

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))

API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH", "").strip()
TG_STRING_SESSION = os.getenv("TG_STRING_SESSION", "").strip()

PORT = int(os.getenv("PORT", "10000"))
DATA_FILE = Path(os.getenv("DATA_FILE", "data.json"))


if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN missing")

if not ADMIN_ID:
    raise RuntimeError("ADMIN_ID missing")

if not API_ID:
    raise RuntimeError("API_ID missing")

if not API_HASH:
    raise RuntimeError("API_HASH missing")

if not TG_STRING_SESSION:
    raise RuntimeError("TG_STRING_SESSION missing")


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

log = logging.getLogger("channel-loop")


# ============================================================
# DEFAULT SETTINGS
# ============================================================

DEFAULTS: dict[str, Any] = {
    "enabled": False,

    # Time between posts by one worker.
    "interval": 60,

    # Minimum delay after each successful send.
    "send_gap": 1.5,

    # Retry count.
    "retries": 5,

    # 0 = all available messages.
    "history_limit": 0,

    # Restart from beginning after reaching latest.
    "loop": True,

    # If one message cannot be sent, continue with next message.
    "skip_failed": True,

    # Maximum simultaneous sends.
    "max_concurrent": 3,
}


# ============================================================
# DATABASE
# ============================================================

def empty_db() -> dict[str, Any]:
    return {
        "settings": DEFAULTS.copy(),

        "sources": [],

        "destinations": [],

        # source_id -> destination_id -> next message index
        "positions": {},

        "stats": {
            "sent": 0,
            "failed": 0,
            "skipped": 0,
            "cycles": 0,
            "last_success": None,
            "last_error": None,
        },
    }


def load_db() -> dict[str, Any]:

    if not DATA_FILE.exists():
        return empty_db()

    try:
        data = json.loads(
            DATA_FILE.read_text(
                encoding="utf-8"
            )
        )
    except Exception:

        log.exception(
            "Invalid data.json. Starting fresh."
        )

        return empty_db()

    fresh = empty_db()

    fresh["settings"] = {
        **DEFAULTS,
        **data.get("settings", {}),
    }

    fresh["sources"] = data.get(
        "sources",
        [],
    )

    fresh["destinations"] = data.get(
        "destinations",
        [],
    )

    fresh["positions"] = data.get(
        "positions",
        {},
    )

    fresh["stats"] = {
        **fresh["stats"],
        **data.get("stats", {}),
    }

    return fresh


db = load_db()

db_lock = asyncio.Lock()


async def save_db():

    async with db_lock:

        DATA_FILE.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        temp_file = DATA_FILE.with_suffix(
            ".tmp"
        )

        temp_file.write_text(
            json.dumps(
                db,
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )

        temp_file.replace(
            DATA_FILE
        )


# ============================================================
# RUNTIME
# ============================================================

worker_tasks: dict[
    str,
    asyncio.Task,
] = {}

history_cache: dict[
    str,
    list[int],
] = {}

destination_locks: dict[
    str,
    asyncio.Lock,
] = {}

send_semaphore = asyncio.Semaphore(
    int(
        db["settings"].get(
            "max_concurrent",
            3,
        )
    )
)

health_started = time.time()


# ============================================================
# TELEGRAM USER CLIENT
# ============================================================

telethon_client = TelegramClient(
    StringSession(
        TG_STRING_SESSION
    ),
    API_ID,
    API_HASH,
)


# ============================================================
# HELPERS
# ============================================================

def is_admin(user_id: int) -> bool:
    return user_id == ADMIN_ID


def title_of(item: dict[str, str]) -> str:
    return (
        item.get("title")
        or item.get("username")
        or item["id"]
    )


def entity_input(value: str) -> str | int:

    value = str(value).strip()

    if (
        value.startswith("-100")
        and value[4:].isdigit()
    ):
        return int(value)

    if value.lstrip("-").isdigit():
        return int(value)

    return value


def parse_float(
    text: str,
    minimum: float,
) -> float:

    value = float(
        text.strip()
    )

    if value < minimum:
        raise ValueError

    return value


# ============================================================
# KEYBOARDS
# ============================================================

def main_kb():

    enabled = db["settings"]["enabled"]

    builder = InlineKeyboardBuilder()

    builder.button(
        text=(
            "⏹ Stop"
            if enabled
            else
            "▶️ Start"
        ),
        callback_data="toggle",
    )

    builder.button(
        text="⚙️ Settings",
        callback_data="settings",
    )

    builder.button(
        text="📥 Sources",
        callback_data="sources",
    )

    builder.button(
        text="📤 Destinations",
        callback_data="destinations",
    )

    builder.button(
        text="📊 Status",
        callback_data="status",
    )

    builder.button(
        text="🔄 Refresh",
        callback_data="home",
    )

    builder.adjust(
        2,
        2,
        2,
    )

    return builder.as_markup()


def back_kb(
    target: str = "home",
):

    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="⬅️ Back",
                    callback_data=target,
                )
            ]
        ]
    )


def settings_kb():

    s = db["settings"]

    builder = InlineKeyboardBuilder()

    builder.button(
        text=f"⏱ Interval: {s['interval']}s",
        callback_data="set:interval",
    )

    builder.button(
        text=f"🚦 Send gap: {s['send_gap']}s",
        callback_data="set:gap",
    )

    builder.button(
        text=(
            f"🔁 Loop: "
            f"{'ON' if s['loop'] else 'OFF'}"
        ),
        callback_data="set:loop",
    )

    builder.button(
        text=f"🔄 Retries: {s['retries']}",
        callback_data="set:retries",
    )

    builder.button(
        text=(
            "📚 History: ALL"
            if not s["history_limit"]
            else
            f"📚 History: {s['history_limit']}"
        ),
        callback_data="set:history",
    )

    builder.button(
        text=(
            "⏭ Skip failed: "
            f"{'ON' if s['skip_failed'] else 'OFF'}"
        ),
        callback_data="set:skip",
    )

    builder.button(
        text=(
            f"⚡ Concurrent: "
            f"{s['max_concurrent']}"
        ),
        callback_data="set:concurrent",
    )

    builder.button(
        text="⬅️ Back",
        callback_data="home",
    )

    builder.adjust(
        2,
        2,
        2,
        1,
    )

    return builder.as_markup()


def list_kb(
    kind: str,
):

    items = db[kind]

    prefix = (
        "src"
        if kind == "sources"
        else
        "dst"
    )

    builder = InlineKeyboardBuilder()

    for index, item in enumerate(items):

        title = title_of(item)

        if len(title) > 28:
            title = (
                title[:25]
                + "..."
            )

        builder.button(
            text=f"❌ {title}",
            callback_data=(
                f"del:{prefix}:{index}"
            ),
        )

    builder.button(
        text="➕ Add",
        callback_data=(
            f"add:{prefix}"
        ),
    )

    builder.button(
        text="⬅️ Back",
        callback_data="home",
    )

    builder.adjust(1)

    return builder.as_markup()


# ============================================================
# FSM
# ============================================================

class Form(StatesGroup):

    add_source = State()

    add_destination = State()

    set_interval = State()

    set_gap = State()

    set_retries = State()

    set_history = State()

    set_concurrent = State()


# ============================================================
# TEXT
# ============================================================

def menu_text():

    s = db["settings"]

    return (
        "🤖 <b>Channel Loop Manager</b>\n\n"

        f"Status: "
        f"<b>"
        f"{'RUNNING 🟢' if s['enabled'] else 'STOPPED 🔴'}"
        f"</b>\n"

        f"Sources: "
        f"<b>{len(db['sources'])}</b>\n"

        f"Destinations: "
        f"<b>{len(db['destinations'])}</b>\n"

        f"Interval: "
        f"<b>{s['interval']} sec</b>\n"

        f"Send gap: "
        f"<b>{s['send_gap']} sec</b>\n"

        f"Loop: "
        f"<b>{'ON' if s['loop'] else 'OFF'}</b>\n"

        f"Workers: "
        f"<b>{len(worker_tasks)}</b>"
    )


# ============================================================
# RESOLVE CHANNEL
# ============================================================

async def resolve_chat(
    raw: str,
) -> dict[str, str]:

    entity = await telethon_client.get_entity(
        entity_input(raw)
    )

    raw_id = str(
        entity.id
    )

    if (
        getattr(entity, "broadcast", False)
        or
        getattr(entity, "megagroup", False)
    ):
        chat_id = (
            "-100"
            + raw_id
        )
    else:
        chat_id = raw_id

    title = (
        getattr(
            entity,
            "title",
            None,
        )
        or
        getattr(
            entity,
            "username",
            None,
        )
        or
        raw_id
    )

    username = (
        getattr(
            entity,
            "username",
            None,
        )
        or ""
    )

    return {
        "id": chat_id,
        "title": str(title),
        "username": username,
    }


# ============================================================
# HISTORY
# ============================================================

async def refresh_history(
    source: dict[str, str],
) -> list[int]:

    source_id = source["id"]

    entity = await telethon_client.get_entity(
        entity_input(source_id)
    )

    limit = int(
        db["settings"]["history_limit"]
    )

    ids: list[int] = []

    async for message in telethon_client.iter_messages(
        entity,
        limit=(
            limit
            if limit > 0
            else None
        ),
    ):

        # Ignore service/action messages.
        if getattr(
            message,
            "action",
            None,
        ) is not None:
            continue

        if getattr(
            message,
            "empty",
            False,
        ):
            continue

        ids.append(
            int(message.id)
        )

    # Telethon normally yields newest first.
    ids.reverse()

    history_cache[
        source_id
    ] = ids

    log.info(
        "Loaded %s messages from %s",
        len(ids),
        title_of(source),
    )

    return ids


async def get_message_for_source(
    source: dict[str, str],
    index: int,
):

    source_id = source["id"]

    ids = history_cache.get(
        source_id
    )

    if ids is None:
        ids = await refresh_history(
            source
        )

    if index >= len(ids):

        ids = await refresh_history(
            source
        )

    if (
        not ids
        or
        index >= len(ids)
    ):
        return None, len(ids)

    message_id = ids[index]

    entity = await telethon_client.get_entity(
        entity_input(source_id)
    )

    message = await telethon_client.get_messages(
        entity,
        ids=message_id,
    )

    return (
        message,
        len(ids),
    )


# ============================================================
# COPY MESSAGE
# ============================================================

async def send_copy(
    destination: dict[str, str],
    message: Any,
) -> bool:

    destination_id = destination["id"]

    lock = destination_locks.setdefault(
        destination_id,
        asyncio.Lock(),
    )

    retries = int(
        db["settings"]["retries"]
    )

    async with lock:

        async with send_semaphore:

            for attempt in range(
                retries + 1
            ):

                try:

                    # Passing the Telethon Message object to
                    # send_message copies its content instead
                    # of using Telegram's forward operation.

                    await telethon_client.send_message(
                        entity_input(
                            destination_id
                        ),
                        message,
                    )

                    await asyncio.sleep(
                        float(
                            db["settings"][
                                "send_gap"
                            ]
                        )
                    )

                    return True

                except FloodWaitError as exc:

                    wait_seconds = (
                        int(exc.seconds)
                        + 1
                    )

                    log.warning(
                        "FloodWait %s sec -> %s",
                        wait_seconds,
                        destination_id,
                    )

                    await asyncio.sleep(
                        wait_seconds
                    )

                except (
                    ChatAdminRequiredError,
                    ChannelPrivateError,
                    UserDeactivatedBanError,
                ) as exc:

                    log.error(
                        "Permanent error for %s: %s",
                        destination_id,
                        exc,
                    )

                    return False

                except RPCError as exc:

                    if attempt >= retries:

                        log.error(
                            "RPC error after retries: %s",
                            exc,
                        )

                        return False

                    await asyncio.sleep(
                        min(
                            2 ** attempt,
                            30,
                        )
                    )

                except Exception as exc:

                    if attempt >= retries:

                        log.exception(
                            "Send failed: %s",
                            exc,
                        )

                        return False

                    await asyncio.sleep(
                        min(
                            2 ** attempt,
                            30,
                        )
                    )

    return False


# ============================================================
# SOURCE -> DESTINATION WORKER
# ============================================================

async def source_destination_worker(
    source_id: str,
    destination_id: str,
):

    worker_key = (
        f"{source_id}|{destination_id}"
    )

    log.info(
        "Worker started: %s",
        worker_key,
    )

    while True:

        try:

            if not db["settings"]["enabled"]:

                await asyncio.sleep(
                    1
                )

                continue

            source = next(
                (
                    x
                    for x in db["sources"]
                    if x["id"] == source_id
                ),
                None,
            )

            destination = next(
                (
                    x
                    for x in db["destinations"]
                    if x["id"] == destination_id
                ),
                None,
            )

            if not source or not destination:
                return

            positions = db[
                "positions"
            ].setdefault(
                source_id,
                {},
            )

            index = int(
                positions.get(
                    destination_id,
                    0,
                )
            )

            started = time.monotonic()

            message, total = (
                await get_message_for_source(
                    source,
                    index,
                )
            )

            # Empty source.
            if message is None:

                await asyncio.sleep(
                    10
                )

                continue

            success = await send_copy(
                destination,
                message,
            )

            if success:

                db["stats"]["sent"] += 1

                db["stats"][
                    "last_success"
                ] = int(
                    time.time()
                )

                positions[
                    destination_id
                ] = index + 1

                # Reached latest message.
                if index + 1 >= total:

                    if db["settings"]["loop"]:

                        positions[
                            destination_id
                        ] = 0

                        db["stats"][
                            "cycles"
                        ] += 1

                        # Re-read source next cycle.
                        history_cache.pop(
                            source_id,
                            None,
                        )

                    else:

                        positions[
                            destination_id
                        ] = total

                        await save_db()

                        return

                await save_db()

            else:

                db["stats"]["failed"] += 1

                db["stats"][
                    "last_error"
                ] = (
                    f"Failed: "
                    f"{title_of(source)} -> "
                    f"{title_of(destination)}"
                )

                if db["settings"][
                    "skip_failed"
                ]:

                    positions[
                        destination_id
                    ] = index + 1

                    db["stats"][
                        "skipped"
                    ] += 1

                await save_db()

            # Interval is measured from the beginning
            # of this worker cycle.
            elapsed = (
                time.monotonic()
                - started
            )

            interval = float(
                db["settings"]["interval"]
            )

            delay = max(
                0.1,
                interval - elapsed,
            )

            await asyncio.sleep(
                delay
            )

        except asyncio.CancelledError:

            raise

        except FloodWaitError as exc:

            await asyncio.sleep(
                int(exc.seconds)
                + 1
            )

        except Exception as exc:

            db["stats"][
                "last_error"
            ] = str(exc)[:500]

            await save_db()

            log.exception(
                "Worker error %s",
                worker_key,
            )

            await asyncio.sleep(
                5
            )


# ============================================================
# WORKER RECONCILER
# ============================================================

async def reconcile_workers():

    while True:

        try:

            desired = {
                (
                    f"{source['id']}|"
                    f"{destination['id']}"
                )
                for source in db["sources"]
                for destination in db["destinations"]
            }

            # Remove unwanted/dead workers.
            for key in list(
                worker_tasks
            ):

                task = worker_tasks[
                    key
                ]

                if (
                    key not in desired
                    or task.done()
                ):

                    if not task.done():
                        task.cancel()

                    with suppress(
                        asyncio.CancelledError
                    ):
                        await task

                    worker_tasks.pop(
                        key,
                        None,
                    )

            # Create missing workers.
            if db["settings"][
                "enabled"
            ]:

                for source in db[
                    "sources"
                ]:

                    for destination in db[
                        "destinations"
                    ]:

                        key = (
                            f"{source['id']}|"
                            f"{destination['id']}"
                        )

                        if key not in worker_tasks:

                            worker_tasks[
                                key
                            ] = asyncio.create_task(
                                source_destination_worker(
                                    source["id"],
                                    destination["id"],
                                )
                            )

            await asyncio.sleep(
                1
            )

        except asyncio.CancelledError:

            raise

        except Exception:

            log.exception(
                "Reconciler error"
            )

            await asyncio.sleep(
                3
            )


async def stop_all_workers():

    tasks = list(
        worker_tasks.values()
    )

    for task in tasks:
        task.cancel()

    for task in tasks:

        with suppress(
            asyncio.CancelledError
        ):
            await task

    worker_tasks.clear()


# ============================================================
# WEB HEALTH SERVER
# ============================================================

async def health(
    request: web.Request,
):

    return web.json_response(
        {
            "ok": True,
            "running": bool(
                db["settings"]["enabled"]
            ),
            "uptime": int(
                time.time()
                - health_started
            ),
            "sources": len(
                db["sources"]
            ),
            "destinations": len(
                db["destinations"]
            ),
            "workers": len(
                worker_tasks
            ),
            "sent": db["stats"][
                "sent"
            ],
            "failed": db["stats"][
                "failed"
            ],
        }
    )


async def start_http_server():

    app = web.Application()

    app.router.add_get(
        "/",
        health,
    )

    app.router.add_get(
        "/health",
        health,
    )

    app.router.add_get(
        "/status",
        health,
    )

    runner = web.AppRunner(
        app
    )

    await runner.setup()

    site = web.TCPSite(
        runner,
        "0.0.0.0",
        PORT,
    )

    await site.start()

    return runner


# ============================================================
# BOT ROUTER
# ============================================================

router = Router()


# ============================================================
# /START
# ============================================================

@router.message(
    CommandStart()
)
async def start_handler(
    message: Message,
):

    if not is_admin(
        message.from_user.id
    ):

        await message.answer(
            "Only admin can access this bot."
        )

        return

    await message.answer(
        menu_text(),
        reply_markup=main_kb(),
    )


# ============================================================
# CALLBACK HANDLER
# ============================================================

@router.callback_query(
    F.data
)
async def callback_handler(
    query: CallbackQuery,
    state: FSMContext,
):

    if not is_admin(
        query.from_user.id
    ):

        await query.answer(
            "Not allowed.",
            show_alert=True,
        )

        return

    await query.answer()

    data = query.data or ""

    # ---------------- HOME ----------------

    if data == "home":

        await state.clear()

        await query.message.edit_text(
            menu_text(),
            reply_markup=main_kb(),
        )

        return

    # ---------------- START/STOP ----------------

    if data == "toggle":

        db["settings"][
            "enabled"
        ] = not db["settings"][
            "enabled"
        ]

        await save_db()

        await query.message.edit_text(
            menu_text(),
            reply_markup=main_kb(),
        )

        return

    # ---------------- SETTINGS ----------------

    if data == "settings":

        await query.message.edit_text(
            "⚙️ <b>Settings</b>",
            reply_markup=settings_kb(),
        )

        return

    # ---------------- SOURCES ----------------

    if data == "sources":

        text = (
            "📥 <b>Source Channels</b>\n\n"
        )

        if db["sources"]:

            text += "\n".join(
                (
                    f"{i + 1}. "
                    f"{title_of(item)}\n"
                    f"<code>{item['id']}</code>"
                )
                for i, item in enumerate(
                    db["sources"]
                )
            )

        else:

            text += (
                "No source channels configured."
            )

        await query.message.edit_text(
            text,
            reply_markup=list_kb(
                "sources"
            ),
        )

        return

    # ---------------- DESTINATIONS ----------------

    if data == "destinations":

        text = (
            "📤 <b>Destination Channels</b>\n\n"
        )

        if db["destinations"]:

            text += "\n".join(
                (
                    f"{i + 1}. "
                    f"{title_of(item)}\n"
                    f"<code>{item['id']}</code>"
                )
                for i, item in enumerate(
                    db["destinations"]
                )
            )

        else:

            text += (
                "No destination channels configured."
            )

        await query.message.edit_text(
            text,
            reply_markup=list_kb(
                "destinations"
            ),
        )

        return

    # ---------------- STATUS ----------------

    if data == "status":

        stats = db["stats"]

        text = (
            "📊 <b>System Status</b>\n\n"

            f"Running: "
            f"<b>{db['settings']['enabled']}</b>\n"

            f"Workers: "
            f"<b>{len(worker_tasks)}</b>\n"

            f"Sources: "
            f"<b>{len(db['sources'])}</b>\n"

            f"Destinations: "
            f"<b>{len(db['destinations'])}</b>\n\n"

            f"Sent: "
            f"<b>{stats['sent']}</b>\n"

            f"Failed: "
            f"<b>{stats['failed']}</b>\n"

            f"Skipped: "
            f"<b>{stats['skipped']}</b>\n"

            f"Cycles: "
            f"<b>{stats['cycles']}</b>\n\n"

            f"Last success: "
            f"<code>{stats['last_success']}</code>\n"

            f"Last error: "
            f"<code>{stats['last_error'] or '-'}</code>"
        )

        await query.message.edit_text(
            text,
            reply_markup=back_kb(),
        )

        return

    # ---------------- LOOP ----------------

    if data == "set:loop":

        db["settings"]["loop"] = not (
            db["settings"]["loop"]
        )

        await save_db()

        await query.message.edit_text(
            "⚙️ <b>Settings</b>",
            reply_markup=settings_kb(),
        )

        return

    # ---------------- SKIP FAILED ----------------

    if data == "set:skip":

        db["settings"][
            "skip_failed"
        ] = not (
            db["settings"][
                "skip_failed"
            ]
        )

        await save_db()

        await query.message.edit_text(
            "⚙️ <b>Settings</b>",
            reply_markup=settings_kb(),
        )

        return

    # ---------------- INTERVAL ----------------

    if data == "set:interval":

        await state.set_state(
            Form.set_interval
        )

        await query.message.edit_text(
            "⏱ Send interval in seconds.\n\n"
            "Example:\n"
            "<code>60</code>"
        )

        return

    # ---------------- SEND GAP ----------------

    if data == "set:gap":

        await state.set_state(
            Form.set_gap
        )

        await query.message.edit_text(
            "🚦 Send gap in seconds.\n\n"
            "Example:\n"
            "<code>1.5</code>"
        )

        return

    # ---------------- RETRIES ----------------

    if data == "set:retries":

        await state.set_state(
            Form.set_retries
        )

        await query.message.edit_text(
            "🔄 Retry count.\n\n"
            "Example:\n"
            "<code>5</code>"
        )

        return

    # ---------------- HISTORY ----------------

    if data == "set:history":

        await state.set_state(
            Form.set_history
        )

        await query.message.edit_text(
            "📚 History limit.\n\n"
            "<code>0</code> = all available messages\n"
            "<code>5000</code> = latest 5000 messages"
        )

        return

    # ---------------- CONCURRENT ----------------

    if data == "set:concurrent":

        await state.set_state(
            Form.set_concurrent
        )

        await query.message.edit_text(
            "⚡ Maximum simultaneous sends.\n\n"
            "Recommended: <code>1-5</code>\n\n"
            "Example:\n"
            "<code>3</code>"
        )

        return

    # ---------------- ADD SOURCE ----------------

    if data == "add:src":

        await state.set_state(
            Form.add_source
        )

        await query.message.edit_text(
            "📥 Send source channel.\n\n"
            "Examples:\n"
            "<code>@mychannel</code>\n"
            "<code>-1001234567890</code>\n\n"
            "The logged-in Telegram account "
            "must have access to the channel."
        )

        return

    # ---------------- ADD DESTINATION ----------------

    if data == "add:dst":

        await state.set_state(
            Form.add_destination
        )

        await query.message.edit_text(
            "📤 Send destination channel.\n\n"
            "Examples:\n"
            "<code>@mychannel</code>\n"
            "<code>-1001234567890</code>\n\n"
            "The logged-in Telegram account "
            "must be able to post there."
        )

        return

    # ---------------- DELETE SOURCE ----------------

    if data.startswith(
        "del:src:"
    ):

        index = int(
            data.split(":")[-1]
        )

        if (
            0 <= index
            < len(db["sources"])
        ):

            source_id = db[
                "sources"
            ][index]["id"]

            db["sources"].pop(
                index
            )

            db["positions"].pop(
                source_id,
                None,
            )

            history_cache.pop(
                source_id,
                None,
            )

            await save_db()

        await query.message.edit_text(
            "📥 <b>Source Channels</b>",
            reply_markup=list_kb(
                "sources"
            ),
        )

        return

    # ---------------- DELETE DESTINATION ----------------

    if data.startswith(
        "del:dst:"
    ):

        index = int(
            data.split(":")[-1]
        )

        if (
            0 <= index
            < len(db["destinations"])
        ):

            destination_id = db[
                "destinations"
            ][index]["id"]

            db[
                "destinations"
            ].pop(index)

            for positions in db[
                "positions"
            ].values():

                positions.pop(
                    destination_id,
                    None,
                )

            destination_locks.pop(
                destination_id,
                None,
            )

            await save_db()

        await query.message.edit_text(
            "📤 <b>Destination Channels</b>",
            reply_markup=list_kb(
                "destinations"
            ),
        )

        return


# ============================================================
# ADD SOURCE
# ============================================================

@router.message(
    Form.add_source
)
async def add_source(
    message: Message,
    state: FSMContext,
):

    if not is_admin(
        message.from_user.id
    ):
        return

    raw = (
        message.text or ""
    ).strip()

    try:

        item = await resolve_chat(
            raw
        )

        if any(
            x["id"] == item["id"]
            for x in db["sources"]
        ):

            await message.answer(
                "⚠️ Source already added.",
                reply_markup=back_kb(
                    "sources"
                ),
            )

        else:

            db["sources"].append(
                item
            )

            db[
                "positions"
            ].setdefault(
                item["id"],
                {},
            )

            history_cache.pop(
                item["id"],
                None,
            )

            await save_db()

            await message.answer(
                (
                    "✅ <b>Source added</b>\n\n"
                    f"Name: {item['title']}\n"
                    f"ID: <code>{item['id']}</code>"
                ),
                reply_markup=back_kb(
                    "sources"
                ),
            )

    except Exception as exc:

        await message.answer(
            (
                "❌ Could not resolve source.\n\n"
                f"<code>{str(exc)[:700]}</code>"
            )
        )

    finally:

        await state.clear()


# ============================================================
# ADD DESTINATION
# ============================================================

@router.message(
    Form.add_destination
)
async def add_destination(
    message: Message,
    state: FSMContext,
):

    if not is_admin(
        message.from_user.id
    ):
        return

    raw = (
        message.text or ""
    ).strip()

    try:

        item = await resolve_chat(
            raw
        )

        if any(
            x["id"] == item["id"]
            for x in db["destinations"]
        ):

            await message.answer(
                "⚠️ Destination already added.",
                reply_markup=back_kb(
                    "destinations"
                ),
            )

        else:

            db[
                "destinations"
            ].append(item)

            await save_db()

            await message.answer(
                (
                    "✅ <b>Destination added</b>\n\n"
                    f"Name: {item['title']}\n"
                    f"ID: <code>{item['id']}</code>"
                ),
                reply_markup=back_kb(
                    "destinations"
                ),
            )

    except Exception as exc:

        await message.answer(
            (
                "❌ Could not resolve destination.\n\n"
                f"<code>{str(exc)[:700]}</code>"
            )
        )

    finally:

        await state.clear()


# ============================================================
# SET INTERVAL
# ============================================================

@router.message(
    Form.set_interval
)
async def set_interval(
    message: Message,
    state: FSMContext,
):

    try:

        value = int(
            parse_float(
                message.text or "",
                1,
            )
        )

        db["settings"][
            "interval"
        ] = value

        await save_db()

        await message.answer(
            "✅ Interval updated.",
            reply_markup=settings_kb(),
        )

    except ValueError:

        await message.answer(
            "❌ Enter a number >= 1."
        )

    finally:

        await state.clear()


# ============================================================
# SET GAP
# ============================================================

@router.message(
    Form.set_gap
)
async def set_gap(
    message: Message,
    state: FSMContext,
):

    try:

        value = parse_float(
            message.text or "",
            0.1,
        )

        db["settings"][
            "send_gap"
        ] = value

        await save_db()

        await message.answer(
            "✅ Send gap updated.",
            reply_markup=settings_kb(),
        )

    except ValueError:

        await message.answer(
            "❌ Enter a number >= 0.1."
        )

    finally:

        await state.clear()


# ============================================================
# SET RETRIES
# ============================================================

@router.message(
    Form.set_retries
)
async def set_retries(
    message: Message,
    state: FSMContext,
):

    try:

        value = int(
            message.text or ""
        )

        if not 0 <= value <= 20:
            raise ValueError

        db["settings"][
            "retries"
        ] = value

        await save_db()

        await message.answer(
            "✅ Retries updated.",
            reply_markup=settings_kb(),
        )

    except ValueError:

        await message.answer(
            "❌ Enter an integer from 0 to 20."
        )

    finally:

        await state.clear()


# ============================================================
# SET HISTORY
# ============================================================

@router.message(
    Form.set_history
)
async def set_history(
    message: Message,
    state: FSMContext,
):

    try:

        value = int(
            message.text or ""
        )

        if value < 0:
            raise ValueError

        db["settings"][
            "history_limit"
        ] = value

        history_cache.clear()

        await save_db()

        await message.answer(
            "✅ History limit updated.",
            reply_markup=settings_kb(),
        )

    except ValueError:

        await message.answer(
            "❌ Enter 0 or a positive integer."
        )

    finally:

        await state.clear()


# ============================================================
# SET CONCURRENT
# ============================================================

@router.message(
    Form.set_concurrent
)
async def set_concurrent(
    message: Message,
    state: FSMContext,
):

    global send_semaphore

    try:

        value = int(
            message.text or ""
        )

        if not 1 <= value <= 20:
            raise ValueError

        db["settings"][
            "max_concurrent"
        ] = value

        # Replace semaphore for new sends.
        send_semaphore = asyncio.Semaphore(
            value
        )

        await save_db()

        await message.answer(
            "✅ Concurrent send limit updated.",
            reply_markup=settings_kb(),
        )

    except ValueError:

        await message.answer(
            "❌ Enter an integer from 1 to 20."
        )

    finally:

        await state.clear()


# ============================================================
# MAIN
# ============================================================

async def main():

    # Connect Telethon user account.
    await telethon_client.connect()

    if not await telethon_client.is_user_authorized():

        raise RuntimeError(
            "TG_STRING_SESSION is invalid or expired."
        )

    me = await telethon_client.get_me()

    log.info(
        "Telegram user connected: %s",
        getattr(
            me,
            "username",
            None,
        ),
    )

    # Start Render health server.
    http_runner = await start_http_server()

    bot = Bot(
        BOT_TOKEN
    )

    dispatcher = Dispatcher()

    dispatcher.include_router(
        router
    )

    reconciler = asyncio.create_task(
        reconcile_workers()
    )

    try:

        await dispatcher.start_polling(
            bot,
            allowed_updates=(
                dispatcher.resolve_used_update_types()
            ),
        )

    finally:

        reconciler.cancel()

        with suppress(
            asyncio.CancelledError
        ):
            await reconciler

        await stop_all_workers()

        await http_runner.cleanup()

        await bot.session.close()

        await telethon_client.disconnect()


if __name__ == "__main__":

    try:

        asyncio.run(
            main()
        )

    except KeyboardInterrupt:

        pass
