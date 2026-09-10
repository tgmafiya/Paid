from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from collections import defaultdict
from pathlib import Path
from typing import Any

from aiohttp import web
from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.exceptions import (
    TelegramBadRequest,
    TelegramForbiddenError,
    TelegramNetworkError,
    TelegramRetryAfter,
)
from aiogram.filters import CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)
from dotenv import load_dotenv


# ============================================================
# CONFIG
# ============================================================

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
ADMIN_ID_RAW = os.getenv("ADMIN_ID", "").strip()

PORT = int(os.getenv("PORT", "10000"))
DATA_FILE = Path(
    os.getenv("DATA_FILE", "data.json")
)

if not BOT_TOKEN:
    raise RuntimeError(
        "BOT_TOKEN is missing in .env"
    )

if not ADMIN_ID_RAW.isdigit():
    raise RuntimeError(
        "ADMIN_ID must be a numeric Telegram user ID"
    )

ADMIN_ID = int(ADMIN_ID_RAW)


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format=(
        "%(asctime)s | "
        "%(levelname)s | "
        "%(name)s | "
        "%(message)s"
    ),
)

log = logging.getLogger(
    "latest-channel-copy"
)


# ============================================================
# DEFAULT SETTINGS
# ============================================================

DEFAULT_SETTINGS = {
    # Seconds between messages to the same destination.
    "interval": 2.0,

    # Maximum number of messages waiting in one queue.
    "max_queue": 5000,

    # Retry attempts.
    "retries": 5,

    # Wait before grouping media-group posts.
    "album_wait": 1.5,

    # If True, failed messages are skipped after retries.
    "skip_failed": True,

    # Bot enabled/disabled.
    "enabled": True,
}


# ============================================================
# DATABASE
# ============================================================

def default_db() -> dict[str, Any]:
    return {
        "settings": DEFAULT_SETTINGS.copy(),
        "sources": [],
        "destinations": [],
        "stats": {
            "received": 0,
            "queued": 0,
            "sent": 0,
            "failed": 0,
            "skipped": 0,
            "albums": 0,
            "last_received": None,
            "last_sent": None,
            "last_error": None,
        },
        "seen": {},
    }


def load_db() -> dict[str, Any]:

    if not DATA_FILE.exists():
        return default_db()

    try:
        data = json.loads(
            DATA_FILE.read_text(
                encoding="utf-8"
            )
        )
    except Exception:
        log.exception(
            "Could not read data.json"
        )
        return default_db()

    fresh = default_db()

    fresh["settings"].update(
        data.get("settings", {})
    )

    fresh["sources"] = data.get(
        "sources", []
    )

    fresh["destinations"] = data.get(
        "destinations", []
    )

    fresh["stats"].update(
        data.get("stats", {})
    )

    fresh["seen"] = data.get(
        "seen", {}
    )

    return fresh


db = load_db()

db_lock = asyncio.Lock()


async def save_db() -> None:

    async with db_lock:

        DATA_FILE.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        temp = DATA_FILE.with_suffix(
            ".tmp"
        )

        temp.write_text(
            json.dumps(
                db,
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )

        temp.replace(
            DATA_FILE
        )


# ============================================================
# RUNTIME
# ============================================================

router = Router()

bot: Bot | None = None

destination_queues: dict[
    str,
    asyncio.Queue,
] = {}

destination_tasks: dict[
    str,
    asyncio.Task,
] = {}

destination_locks: dict[
    str,
    asyncio.Lock,
] = {}

album_tasks: dict[
    tuple[str, str],
    asyncio.Task,
] = {}

# source_id -> media_group_id -> message IDs
album_buffer: dict[
    tuple[str, str],
    list[int],
] = defaultdict(list)

# Prevent duplicate processing of an update/message.
seen_lock = asyncio.Lock()

server_runner: web.AppRunner | None = None


# ============================================================
# FSM
# ============================================================

class AddSource(StatesGroup):
    value = State()


class AddDestination(StatesGroup):
    value = State()


class SetInterval(StatesGroup):
    value = State()


class SetQueue(StatesGroup):
    value = State()


class SetRetries(StatesGroup):
    value = State()


class SetAlbumWait(StatesGroup):
    value = State()


# ============================================================
# ADMIN CHECK
# ============================================================

def is_admin(
    message_or_query: Message | CallbackQuery,
) -> bool:

    user = (
        message_or_query.from_user
    )

    return bool(
        user
        and user.id == ADMIN_ID
    )


# ============================================================
# HELPERS
# ============================================================

def source_ids() -> set[str]:
    return {
        str(x["id"])
        for x in db["sources"]
    }


def destination_ids() -> set[str]:
    return {
        str(x["id"])
        for x in db["destinations"]
    }


def find_source(
    chat_id: str,
) -> dict[str, Any] | None:

    for item in db["sources"]:
        if str(item["id"]) == str(chat_id):
            return item

    return None


def find_destination(
    chat_id: str,
) -> dict[str, Any] | None:

    for item in db["destinations"]:
        if str(item["id"]) == str(chat_id):
            return item

    return None


def display_name(
    item: dict[str, Any],
) -> str:

    return (
        item.get("title")
        or item.get("username")
        or item.get("id", "Unknown")
    )


def home_text() -> str:

    s = db["settings"]
    st = db["stats"]

    running = (
        "🟢 RUNNING"
        if s["enabled"]
        else "🔴 STOPPED"
    )

    return (
        "🤖 <b>Latest Channel Copy Bot</b>\n\n"
        f"Status: <b>{running}</b>\n"
        f"Sources: <b>{len(db['sources'])}</b>\n"
        f"Destinations: <b>{len(db['destinations'])}</b>\n\n"
        f"⏱ Interval: <b>{s['interval']}s</b>\n"
        f"📦 Album wait: <b>{s['album_wait']}s</b>\n"
        f"🔁 Retries: <b>{s['retries']}</b>\n\n"
        f"📥 Received: <b>{st['received']}</b>\n"
        f"📤 Sent: <b>{st['sent']}</b>\n"
        f"❌ Failed: <b>{st['failed']}</b>\n"
        f"⏭ Skipped: <b>{st['skipped']}</b>"
    )


# ============================================================
# KEYBOARDS
# ============================================================

def home_keyboard() -> InlineKeyboardMarkup:

    enabled = db["settings"]["enabled"]

    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text=(
                        "⏹ Stop"
                        if enabled
                        else "▶️ Start"
                    ),
                    callback_data="toggle",
                ),
            ],
            [
                InlineKeyboardButton(
                    text="📥 Sources",
                    callback_data="sources",
                ),
                InlineKeyboardButton(
                    text="📤 Destinations",
                    callback_data="destinations",
                ),
            ],
            [
                InlineKeyboardButton(
                    text="⚙️ Settings",
                    callback_data="settings",
                ),
                InlineKeyboardButton(
                    text="📊 Status",
                    callback_data="status",
                ),
            ],
            [
                InlineKeyboardButton(
                    text="🔄 Refresh",
                    callback_data="home",
                ),
            ],
        ]
    )


def settings_keyboard() -> InlineKeyboardMarkup:

    s = db["settings"]

    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text=f"⏱ Interval: {s['interval']}s",
                    callback_data="setting:interval",
                ),
            ],
            [
                InlineKeyboardButton(
                    text=(
                        f"📦 Album wait: "
                        f"{s['album_wait']}s"
                    ),
                    callback_data="setting:album",
                ),
            ],
            [
                InlineKeyboardButton(
                    text=f"🔁 Retries: {s['retries']}",
                    callback_data="setting:retries",
                ),
            ],
            [
                InlineKeyboardButton(
                    text=(
                        f"📚 Queue limit: "
                        f"{s['max_queue']}"
                    ),
                    callback_data="setting:queue",
                ),
            ],
            [
                InlineKeyboardButton(
                    text=(
                        "⏭ Skip failed: "
                        + (
                            "ON"
                            if s["skip_failed"]
                            else "OFF"
                        )
                    ),
                    callback_data="setting:skip",
                ),
            ],
            [
                InlineKeyboardButton(
                    text="⬅️ Back",
                    callback_data="home",
                ),
            ],
        ]
    )


def sources_keyboard() -> InlineKeyboardMarkup:

    rows = []

    for index, item in enumerate(
        db["sources"]
    ):

        rows.append(
            [
                InlineKeyboardButton(
                    text=(
                        "❌ "
                        + display_name(item)
                    ),
                    callback_data=(
                        f"delete_source:{index}"
                    ),
                )
            ]
        )

    rows.append(
        [
            InlineKeyboardButton(
                text="➕ Add Source",
                callback_data="add_source",
            )
        ]
    )

    rows.append(
        [
            InlineKeyboardButton(
                text="⬅️ Back",
                callback_data="home",
            )
        ]
    )

    return InlineKeyboardMarkup(
        inline_keyboard=rows
    )


def destinations_keyboard() -> InlineKeyboardMarkup:

    rows = []

    for index, item in enumerate(
        db["destinations"]
    ):

        rows.append(
            [
                InlineKeyboardButton(
                    text=(
                        "❌ "
                        + display_name(item)
                    ),
                    callback_data=(
                        f"delete_destination:{index}"
                    ),
                )
            ]
        )

    rows.append(
        [
            InlineKeyboardButton(
                text="➕ Add Destination",
                callback_data="add_destination",
            )
        ]
    )

    rows.append(
        [
            InlineKeyboardButton(
                text="⬅️ Back",
                callback_data="home",
            )
        ]
    )

    return InlineKeyboardMarkup(
        inline_keyboard=rows
    )


# ============================================================
# CHAT RESOLVER
# ============================================================

async def resolve_chat(
    value: str,
) -> dict[str, str]:

    value = value.strip()

    if not value:
        raise ValueError(
            "Channel username or ID is empty."
        )

    if bot is None:
        raise RuntimeError(
            "Bot is not initialized."
        )

    chat = await bot.get_chat(
        value
    )

    chat_id = str(chat.id)

    title = (
        chat.title
        or chat.username
        or chat.first_name
        or chat_id
    )

    username = (
        chat.username
        or ""
    )

    return {
        "id": chat_id,
        "title": title,
        "username": username,
    }


# ============================================================
# QUEUE MANAGEMENT
# ============================================================

def get_queue(
    destination_id: str,
) -> asyncio.Queue:

    if destination_id not in destination_queues:

        destination_queues[
            destination_id
        ] = asyncio.Queue(
            maxsize=int(
                db["settings"][
                    "max_queue"
                ]
            )
        )

    return destination_queues[
        destination_id
    ]


async def start_destination_worker(
    destination_id: str,
):

    if (
        destination_id in destination_tasks
        and not destination_tasks[
            destination_id
        ].done()
    ):
        return

    task = asyncio.create_task(
        destination_worker(
            destination_id
        )
    )

    destination_tasks[
        destination_id
    ] = task


async def stop_destination_worker(
    destination_id: str,
):

    task = destination_tasks.pop(
        destination_id,
        None,
    )

    if task is None:
        return

    task.cancel()

    try:
        await task
    except asyncio.CancelledError:
        pass


async def ensure_workers():

    if not db["settings"]["enabled"]:
        return

    wanted = destination_ids()

    for destination_id in wanted:

        await start_destination_worker(
            destination_id
        )

    for destination_id in list(
        destination_tasks
    ):

        if destination_id not in wanted:

            await stop_destination_worker(
                destination_id
            )


# ============================================================
# RETRY SEND
# ============================================================

async def copy_single_message(
    source_id: str,
    destination_id: str,
    message_id: int,
) -> bool:

    if bot is None:
        return False

    retries = int(
        db["settings"]["retries"]
    )

    for attempt in range(
        retries + 1
    ):

        try:

            await bot.copy_message(
                chat_id=int(
                    destination_id
                ),
                from_chat_id=int(
                    source_id
                ),
                message_id=message_id,
            )

            return True

        except TelegramRetryAfter as exc:

            wait = (
                int(exc.retry_after)
                + 1
            )

            log.warning(
                "FloodWait: sleeping %ss",
                wait,
            )

            await asyncio.sleep(
                wait
            )

        except TelegramForbiddenError as exc:

            log.error(
                "Destination forbidden %s: %s",
                destination_id,
                exc,
            )

            return False

        except TelegramBadRequest as exc:

            log.error(
                "BadRequest source=%s destination=%s "
                "message=%s error=%s",
                source_id,
                destination_id,
                message_id,
                exc,
            )

            return False

        except TelegramNetworkError as exc:

            if attempt >= retries:

                log.error(
                    "Network error after retries: %s",
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
                    "Unexpected send error"
                )

                return False

            await asyncio.sleep(
                min(
                    2 ** attempt,
                    30,
                )
            )

    return False


async def copy_album(
    source_id: str,
    destination_id: str,
    message_ids: list[int],
) -> bool:

    if bot is None:
        return False

    retries = int(
        db["settings"]["retries"]
    )

    for attempt in range(
        retries + 1
    ):

        try:

            # copyMessages preserves an album as a group
            # when Telegram allows the original grouping.
            await bot.copy_messages(
                chat_id=int(
                    destination_id
                ),
                from_chat_id=int(
                    source_id
                ),
                message_ids=message_ids,
            )

            return True

        except TelegramRetryAfter as exc:

            wait = (
                int(exc.retry_after)
                + 1
            )

            await asyncio.sleep(
                wait
            )

        except TelegramForbiddenError as exc:

            log.error(
                "Destination forbidden: %s",
                exc,
            )

            return False

        except TelegramBadRequest as exc:

            log.error(
                "Album copy BadRequest: %s",
                exc,
            )

            # If bulk copy fails, fall back to
            # individual copies so one unsupported
            # message does not kill the entire album.
            all_ok = True

            for message_id in message_ids:

                ok = await copy_single_message(
                    source_id,
                    destination_id,
                    message_id,
                )

                if not ok:
                    all_ok = False

                await asyncio.sleep(
                    float(
                        db["settings"][
                            "interval"
                        ]
                    )
                )

            return all_ok

        except TelegramNetworkError:

            if attempt >= retries:
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
                    "Album copy failed: %s",
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
# DESTINATION WORKER
# ============================================================

async def destination_worker(
    destination_id: str,
):

    queue = get_queue(
        destination_id
    )

    log.info(
        "Worker started: %s",
        destination_id,
    )

    while True:

        try:

            item = await queue.get()

            try:

                if not db["settings"][
                    "enabled"
                ]:

                    continue

                source_id = item[
                    "source_id"
                ]

                message_ids = item[
                    "message_ids"
                ]

                is_album = item[
                    "is_album"
                ]

                if is_album:

                    ok = await copy_album(
                        source_id,
                        destination_id,
                        message_ids,
                    )

                else:

                    ok = await copy_single_message(
                        source_id,
                        destination_id,
                        message_ids[0],
                    )

                if ok:

                    db["stats"][
                        "sent"
                    ] += len(
                        message_ids
                    )

                    db["stats"][
                        "last_sent"
                    ] = int(
                        time.time()
                    )

                else:

                    db["stats"][
                        "failed"
                    ] += len(
                        message_ids
                    )

                    if db["settings"][
                        "skip_failed"
                    ]:

                        db["stats"][
                            "skipped"
                        ] += len(
                            message_ids
                        )

                await save_db()

                # Rate limit per destination.
                await asyncio.sleep(
                    float(
                        db["settings"][
                            "interval"
                        ]
                    )
                )

            finally:

                queue.task_done()

        except asyncio.CancelledError:

            raise

        except Exception as exc:

            log.exception(
                "Destination worker crashed: %s",
                exc,
            )

            await asyncio.sleep(
                2
            )


# ============================================================
# QUEUE MESSAGE
# ============================================================

async def queue_for_all_destinations(
    source_id: str,
    message_ids: list[int],
    is_album: bool,
):

    if not db["settings"]["enabled"]:
        return

    destinations = list(
        db["destinations"]
    )

    for destination in destinations:

        destination_id = str(
            destination["id"]
        )

        queue = get_queue(
            destination_id
        )

        item = {
            "source_id": str(
                source_id
            ),
            "message_ids": [
                int(x)
                for x in message_ids
            ],
            "is_album": bool(
                is_album
            ),
            "created_at": int(
                time.time()
            ),
        }

        try:

            queue.put_nowait(
                item
            )

            db["stats"][
                "queued"
            ] += len(
                message_ids
            )

        except asyncio.QueueFull:

            db["stats"][
                "failed"
            ] += len(
                message_ids
            )

            db["stats"][
                "last_error"
            ] = (
                f"Queue full for "
                f"{destination_id}"
            )

            log.error(
                "Queue full: %s",
                destination_id,
            )

    await save_db()


# ============================================================
# MEDIA GROUP DEBOUNCER
# ============================================================

async def flush_album(
    source_id: str,
    media_group_id: str,
):

    key = (
        source_id,
        media_group_id,
    )

    try:

        await asyncio.sleep(
            float(
                db["settings"][
                    "album_wait"
                ]
            )
        )

        message_ids = album_buffer.pop(
            key,
            [],
        )

        if not message_ids:
            return

        # Telegram normally gives album messages
        # in increasing message ID order.
        message_ids = sorted(
            set(message_ids)
        )

        db["stats"][
            "albums"
        ] += 1

        await queue_for_all_destinations(
            source_id,
            message_ids,
            True,
        )

    except asyncio.CancelledError:

        raise

    except Exception:

        log.exception(
            "Album flush failed"
        )

    finally:

        album_tasks.pop(
            key,
            None,
        )


# ============================================================
# CHANNEL POST HANDLER
# ============================================================

@router.channel_post()
async def channel_post_handler(
    message: Message,
):

    source_id = str(
        message.chat.id
    )

    # Only configured sources are accepted.
    if source_id not in source_ids():
        return

    if not db["settings"]["enabled"]:
        return

    message_id = int(
        message.message_id
    )

    # Persistent dedupe key.
    dedupe_key = (
        f"{source_id}:{message_id}"
    )

    async with seen_lock:

        if dedupe_key in db["seen"]:
            return

        db["seen"][
            dedupe_key
        ] = int(time.time())

        # Keep seen database bounded.
        if len(db["seen"]) > 10000:

            oldest = sorted(
                db["seen"].items(),
                key=lambda x: x[1],
            )[:2000]

            for key, _ in oldest:
                db["seen"].pop(
                    key,
                    None,
                )

    db["stats"][
        "received"
    ] += 1

    db["stats"][
        "last_received"
    ] = int(
        time.time()
    )

    await save_db()

    media_group_id = (
        message.media_group_id
    )

    if media_group_id:

        key = (
            source_id,
            str(media_group_id),
        )

        album_buffer[
            key
        ].append(
            message_id
        )

        # Restart debounce timer so all album
        # messages can arrive before copying.
        old_task = album_tasks.get(
            key
        )

        if old_task:

            old_task.cancel()

            try:
                await old_task
            except asyncio.CancelledError:
                pass

        album_tasks[
            key
        ] = asyncio.create_task(
            flush_album(
                source_id,
                str(media_group_id),
            )
        )

    else:

        await queue_for_all_destinations(
            source_id,
            [message_id],
            False,
        )


# ============================================================
# ADMIN COMMANDS
# ============================================================

@router.message(
    CommandStart()
)
async def start_command(
    message: Message,
    state: FSMContext,
):

    await state.clear()

    if not is_admin(message):

        await message.answer(
            "Only admin can access this bot."
        )

        return

    await message.answer(
        home_text(),
        reply_markup=home_keyboard(),
    )


# ============================================================
# CALLBACKS
# ============================================================

@router.callback_query()
async def callback_handler(
    query: CallbackQuery,
    state: FSMContext,
):

    if not is_admin(query):

        await query.answer(
            "Access denied.",
            show_alert=True,
        )

        return

    data = query.data or ""

    await query.answer()

    if data == "home":

        await state.clear()

        await query.message.edit_text(
            home_text(),
            reply_markup=home_keyboard(),
        )

        return

    if data == "toggle":

        db["settings"][
            "enabled"
        ] = not db["settings"][
            "enabled"
        ]

        await save_db()

        if db["settings"]["enabled"]:
            await ensure_workers()

        await query.message.edit_text(
            home_text(),
            reply_markup=home_keyboard(),
        )

        return

    if data == "sources":

        text = (
            "📥 <b>Source Channels</b>\n\n"
        )

        if not db["sources"]:

            text += (
                "No sources configured."
            )

        else:

            for index, item in enumerate(
                db["sources"],
                start=1,
            ):

                text += (
                    f"{index}. "
                    f"<b>{display_name(item)}</b>\n"
                    f"<code>{item['id']}</code>\n\n"
                )

        await query.message.edit_text(
            text,
            reply_markup=sources_keyboard(),
        )

        return

    if data == "destinations":

        text = (
            "📤 <b>Destination Channels</b>\n\n"
        )

        if not db["destinations"]:

            text += (
                "No destinations configured."
            )

        else:

            for index, item in enumerate(
                db["destinations"],
                start=1,
            ):

                queue = get_queue(
                    str(item["id"])
                )

                text += (
                    f"{index}. "
                    f"<b>{display_name(item)}</b>\n"
                    f"<code>{item['id']}</code>\n"
                    f"Queue: <b>{queue.qsize()}</b>\n\n"
                )

        await query.message.edit_text(
            text,
            reply_markup=destinations_keyboard(),
        )

        return

    if data == "settings":

        await query.message.edit_text(
            "⚙️ <b>Settings</b>",
            reply_markup=settings_keyboard(),
        )

        return

    if data == "status":

        st = db["stats"]

        queue_text = ""

        for destination in db[
            "destinations"
        ]:

            destination_id = str(
                destination["id"]
            )

            queue = get_queue(
                destination_id
            )

            queue_text += (
                f"\n• "
                f"{display_name(destination)}: "
                f"<b>{queue.qsize()}</b>"
            )

        text = (
            "📊 <b>Status</b>\n\n"

            f"Running: "
            f"<b>{db['settings']['enabled']}</b>\n"

            f"Active workers: "
            f"<b>{len(destination_tasks)}</b>\n\n"

            f"📥 Received: "
            f"<b>{st['received']}</b>\n"

            f"📦 Queued: "
            f"<b>{st['queued']}</b>\n"

            f"📤 Sent: "
            f"<b>{st['sent']}</b>\n"

            f"❌ Failed: "
            f"<b>{st['failed']}</b>\n"

            f"⏭ Skipped: "
            f"<b>{st['skipped']}</b>\n"

            f"🖼 Albums: "
            f"<b>{st['albums']}</b>\n"

            f"{queue_text}\n\n"

            f"Last error:\n"
            f"<code>"
            f"{st['last_error'] or '-'}"
            f"</code>"
        )

        await query.message.edit_text(
            text,
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[
                    [
                        InlineKeyboardButton(
                            text="⬅️ Back",
                            callback_data="home",
                        )
                    ]
                ]
            ),
        )

        return

    # --------------------------------------------------------
    # ADD SOURCE
    # --------------------------------------------------------

    if data == "add_source":

        await state.set_state(
            AddSource.value
        )

        await query.message.edit_text(
            "📥 <b>Add Source Channel</b>\n\n"
            "Send:\n"
            "<code>@channelusername</code>\n"
            "or\n"
            "<code>-1001234567890</code>\n\n"
            "Bot must be admin in the source channel."
        )

        return

    # --------------------------------------------------------
    # ADD DESTINATION
    # --------------------------------------------------------

    if data == "add_destination":

        await state.set_state(
            AddDestination.value
        )

        await query.message.edit_text(
            "📤 <b>Add Destination Channel</b>\n\n"
            "Send:\n"
            "<code>@channelusername</code>\n"
            "or\n"
            "<code>-1001234567890</code>\n\n"
            "Bot must be admin and have permission "
            "to post."
        )

        return

    # --------------------------------------------------------
    # DELETE SOURCE
    # --------------------------------------------------------

    if data.startswith(
        "delete_source:"
    ):

        index = int(
            data.split(":")[1]
        )

        if (
            0 <= index
            < len(db["sources"])
        ):

            removed = db[
                "sources"
            ].pop(index)

            source_id = str(
                removed["id"]
            )

            # Remove seen messages belonging
            # to this source.
            prefix = (
                source_id + ":"
            )

            db["seen"] = {
                key: value
                for key, value
                in db["seen"].items()
                if not key.startswith(prefix)
            }

            await save_db()

        await query.message.edit_text(
            "📥 <b>Source Channels</b>",
            reply_markup=sources_keyboard(),
        )

        return

    # --------------------------------------------------------
    # DELETE DESTINATION
    # --------------------------------------------------------

    if data.startswith(
        "delete_destination:"
    ):

        index = int(
            data.split(":")[1]
        )

        if (
            0 <= index
            < len(db["destinations"])
        ):

            removed = db[
                "destinations"
            ].pop(index)

            destination_id = str(
                removed["id"]
            )

            await stop_destination_worker(
                destination_id
            )

            destination_queues.pop(
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
            reply_markup=destinations_keyboard(),
        )

        return

    # --------------------------------------------------------
    # SETTINGS
    # --------------------------------------------------------

    if data == "setting:interval":

        await state.set_state(
            SetInterval.value
        )

        await query.message.edit_text(
            "⏱ <b>Interval</b>\n\n"
            "Enter seconds.\n"
            "Example: <code>2</code>"
        )

        return

    if data == "setting:album":

        await state.set_state(
            SetAlbumWait.value
        )

        await query.message.edit_text(
            "📦 <b>Album wait</b>\n\n"
            "How long to wait for the remaining "
            "photos/videos of an album.\n\n"
            "Recommended: <code>1.5</code>"
        )

        return

    if data == "setting:retries":

        await state.set_state(
            SetRetries.value
        )

        await query.message.edit_text(
            "🔁 <b>Retries</b>\n\n"
            "Enter 0-20.\n"
            "Example: <code>5</code>"
        )

        return

    if data == "setting:queue":

        await state.set_state(
            SetQueue.value
        )

        await query.message.edit_text(
            "📚 <b>Queue limit</b>\n\n"
            "Example: <code>5000</code>"
        )

        return

    if data == "setting:skip":

        db["settings"][
            "skip_failed"
        ] = not db["settings"][
            "skip_failed"
        ]

        await save_db()

        await query.message.edit_text(
            "⚙️ <b>Settings</b>",
            reply_markup=settings_keyboard(),
        )

        return


# ============================================================
# FSM: ADD SOURCE
# ============================================================

@router.message(
    AddSource.value
)
async def add_source_handler(
    message: Message,
    state: FSMContext,
):

    if not is_admin(message):
        return

    value = (
        message.text or ""
    ).strip()

    try:

        item = await resolve_chat(
            value
        )

        chat_id = str(
            item["id"]
        )

        if chat_id in source_ids():

            await message.answer(
                "⚠️ This source is already added."
            )

        else:

            db["sources"].append(
                item
            )

            await save_db()

            await message.answer(
                (
                    "✅ <b>Source added</b>\n\n"
                    f"Name: {item['title']}\n"
                    f"ID: <code>{item['id']}</code>\n\n"
                    "Only new posts will be processed."
                ),
                reply_markup=sources_keyboard(),
            )

    except Exception as exc:

        await message.answer(
            (
                "❌ Could not add source.\n\n"
                f"<code>{str(exc)[:800]}</code>"
            )
        )

    await state.clear()


# ============================================================
# FSM: ADD DESTINATION
# ============================================================

@router.message(
    AddDestination.value
)
async def add_destination_handler(
    message: Message,
    state: FSMContext,
):

    if not is_admin(message):
        return

    value = (
        message.text or ""
    ).strip()

    try:

        item = await resolve_chat(
            value
        )

        chat_id = str(
            item["id"]
        )

        if chat_id in destination_ids():

            await message.answer(
                "⚠️ This destination is already added."
            )

        else:

            db[
                "destinations"
            ].append(item)

            await save_db()

            await start_destination_worker(
                chat_id
            )

            await message.answer(
                (
                    "✅ <b>Destination added</b>\n\n"
                    f"Name: {item['title']}\n"
                    f"ID: <code>{item['id']}</code>"
                ),
                reply_markup=destinations_keyboard(),
            )

    except Exception as exc:

        await message.answer(
            (
                "❌ Could not add destination.\n\n"
                f"<code>{str(exc)[:800]}</code>"
            )
        )

    await state.clear()


# ============================================================
# FSM: INTERVAL
# ============================================================

@router.message(
    SetInterval.value
)
async def set_interval_handler(
    message: Message,
    state: FSMContext,
):

    try:

        value = float(
            (
                message.text or ""
            ).strip()
        )

        if value < 0.1 or value > 3600:
            raise ValueError

        db["settings"][
            "interval"
        ] = value

        await save_db()

        await message.answer(
            f"✅ Interval set to <b>{value}s</b>.",
            reply_markup=settings_keyboard(),
        )

    except ValueError:

        await message.answer(
            "❌ Enter a value between 0.1 and 3600."
        )

    await state.clear()


# ============================================================
# FSM: ALBUM WAIT
# ============================================================

@router.message(
    SetAlbumWait.value
)
async def set_album_wait_handler(
    message: Message,
    state: FSMContext,
):

    try:

        value = float(
            (
                message.text or ""
            ).strip()
        )

        if value < 0.2 or value > 10:
            raise ValueError

        db["settings"][
            "album_wait"
        ] = value

        await save_db()

        await message.answer(
            f"✅ Album wait set to <b>{value}s</b>.",
            reply_markup=settings_keyboard(),
        )

    except ValueError:

        await message.answer(
            "❌ Enter a value between 0.2 and 10."
        )

    await state.clear()


# ============================================================
# FSM: RETRIES
# ============================================================

@router.message(
    SetRetries.value
)
async def set_retries_handler(
    message: Message,
    state: FSMContext,
):

    try:

        value = int(
            (
                message.text or ""
            ).strip()
        )

        if value < 0 or value > 20:
            raise ValueError

        db["settings"][
            "retries"
        ] = value

        await save_db()

        await message.answer(
            "✅ Retry setting updated.",
            reply_markup=settings_keyboard(),
        )

    except ValueError:

        await message.answer(
            "❌ Enter an integer from 0 to 20."
        )

    await state.clear()


# ============================================================
# FSM: QUEUE
# ============================================================

@router.message(
    SetQueue.value
)
async def set_queue_handler(
    message: Message,
    state: FSMContext,
):

    try:

        value = int(
            (
                message.text or ""
            ).strip()
        )

        if value < 10 or value > 50000:
            raise ValueError

        db["settings"][
            "max_queue"
        ] = value

        await save_db()

        await message.answer(
            "✅ Queue limit updated.\n\n"
            "Restart the bot if you want existing "
            "queues to use the new maximum.",
            reply_markup=settings_keyboard(),
        )

    except ValueError:

        await message.answer(
            "❌ Enter an integer from 10 to 50000."
        )

    await state.clear()


# ============================================================
# HEALTH SERVER
# ============================================================

async def health_handler(
    request: web.Request,
):

    return web.json_response(
        {
            "ok": True,
            "running": bool(
                db["settings"]["enabled"]
            ),
            "sources": len(
                db["sources"]
            ),
            "destinations": len(
                db["destinations"]
            ),
            "workers": len(
                destination_tasks
            ),
            "received": db["stats"][
                "received"
            ],
            "sent": db["stats"][
                "sent"
            ],
            "failed": db["stats"][
                "failed"
            ],
            "uptime": int(
                time.time()
            ),
        }
    )


async def start_web_server():

    global server_runner

    app = web.Application()

    app.router.add_get(
        "/",
        health_handler,
    )

    app.router.add_get(
        "/health",
        health_handler,
    )

    app.router.add_get(
        "/status",
        health_handler,
    )

    server_runner = web.AppRunner(
        app
    )

    await server_runner.setup()

    site = web.TCPSite(
        server_runner,
        "0.0.0.0",
        PORT,
    )

    await site.start()

    log.info(
        "Health server running on port %s",
        PORT,
    )


# ============================================================
# CLEANUP
# ============================================================

async def shutdown():

    for task in list(
        album_tasks.values()
    ):

        task.cancel()

    for task in list(
        album_tasks.values()
    ):

        try:
            await task
        except asyncio.CancelledError:
            pass

    album_tasks.clear()

    for destination_id in list(
        destination_tasks
    ):

        await stop_destination_worker(
            destination_id
        )

    if server_runner:

        await server_runner.cleanup()


# ============================================================
# MAIN
# ============================================================

async def main():

    global bot

    bot = Bot(
        token=BOT_TOKEN,
        default=DefaultBotProperties(
            parse_mode=ParseMode.HTML
        ),
    )

    dp = Dispatcher()

    dp.include_router(
        router
    )

    # Remove any old webhook before polling.
    await bot.delete_webhook(
        drop_pending_updates=False
    )

    me = await bot.get_me()

    log.info(
        "Bot started: @%s",
        me.username,
    )

    await start_web_server()

    await ensure_workers()

    try:

        await dp.start_polling(
            bot,
            allowed_updates=[
                "message",
                "callback_query",
                "channel_post",
                "edited_channel_post",
            ],
        )

    finally:

        await shutdown()

        await bot.session.close()


if __name__ == "__main__":

    try:
        asyncio.run(
            main()
        )
    except KeyboardInterrupt:
        pass
