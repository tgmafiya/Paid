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
from dotenv import load_dotenv


# ============================================================
# CONFIG
# ============================================================

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
ADMIN_ID_RAW = os.getenv("ADMIN_ID", "").strip()

PORT = int(os.getenv("PORT", "10000"))
DATA_FILE = Path(os.getenv("DATA_FILE", "data.json"))

# Render public URL
RENDER_URL = os.getenv(
    "RENDER_URL",
    "https://paid-rczj.onrender.com",
).strip().rstrip("/")

# Optional Telegram webhook secret
WEBHOOK_SECRET = os.getenv(
    "WEBHOOK_SECRET",
    "latest-channel-copy-secret",
).strip()

WEBHOOK_PATH = f"/telegram/webhook/{WEBHOOK_SECRET}"


if not BOT_TOKEN:
    raise RuntimeError(
        "BOT_TOKEN is missing."
    )

if not ADMIN_ID_RAW.isdigit():
    raise RuntimeError(
        "ADMIN_ID must be numeric."
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
# SETTINGS
# ============================================================

DEFAULT_SETTINGS = {
    "interval": 2.0,
    "max_queue": 5000,
    "retries": 5,
    "album_wait": 1.5,
    "skip_failed": True,
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
            "Failed to read data file."
        )

        return default_db()

    fresh = default_db()

    if isinstance(
        data.get("settings"),
        dict,
    ):
        fresh["settings"].update(
            data["settings"]
        )

    if isinstance(
        data.get("sources"),
        list,
    ):
        fresh["sources"] = data["sources"]

    if isinstance(
        data.get("destinations"),
        list,
    ):
        fresh["destinations"] = data[
            "destinations"
        ]

    if isinstance(
        data.get("stats"),
        dict,
    ):
        fresh["stats"].update(
            data["stats"]
        )

    if isinstance(
        data.get("seen"),
        dict,
    ):
        fresh["seen"] = data["seen"]

    return fresh


db = load_db()

db_lock = asyncio.Lock()


async def save_db():

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
# GLOBAL RUNTIME
# ============================================================

router = Router()

bot: Bot | None = None

server_runner: web.AppRunner | None = None

destination_queues: dict[
    str,
    asyncio.Queue,
] = {}

destination_tasks: dict[
    str,
    asyncio.Task,
] = {}

album_tasks: dict[
    tuple[str, str],
    asyncio.Task,
] = {}

album_buffer: dict[
    tuple[str, str],
    list[int],
] = defaultdict(list)

seen_lock = asyncio.Lock()


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
# ADMIN
# ============================================================

def is_admin(
    obj: Message | CallbackQuery,
) -> bool:

    user = obj.from_user

    return bool(
        user
        and user.id == ADMIN_ID
    )


# ============================================================
# HELPERS
# ============================================================

def source_ids() -> set[str]:

    return {
        str(item["id"])
        for item in db["sources"]
    }


def destination_ids() -> set[str]:

    return {
        str(item["id"])
        for item in db["destinations"]
    }


def display_name(
    item: dict[str, Any],
) -> str:

    return (
        item.get("title")
        or item.get("username")
        or str(item.get("id", "Unknown"))
    )


def home_text() -> str:

    settings = db["settings"]
    stats = db["stats"]

    status = (
        "🟢 RUNNING"
        if settings["enabled"]
        else "🔴 STOPPED"
    )

    return (
        "🤖 <b>Latest Channel Copy Bot</b>\n\n"

        f"Status: <b>{status}</b>\n"
        f"Sources: <b>{len(db['sources'])}</b>\n"
        f"Destinations: <b>{len(db['destinations'])}</b>\n\n"

        f"⏱ Interval: "
        f"<b>{settings['interval']}s</b>\n"

        f"📦 Album wait: "
        f"<b>{settings['album_wait']}s</b>\n"

        f"🔁 Retries: "
        f"<b>{settings['retries']}</b>\n\n"

        f"📥 Received: "
        f"<b>{stats['received']}</b>\n"

        f"📦 Queued: "
        f"<b>{stats['queued']}</b>\n"

        f"📤 Sent: "
        f"<b>{stats['sent']}</b>\n"

        f"❌ Failed: "
        f"<b>{stats['failed']}</b>\n"

        f"⏭ Skipped: "
        f"<b>{stats['skipped']}</b>"
    )


# ============================================================
# KEYBOARDS
# ============================================================

def home_keyboard():

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
                )
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
                )
            ],
        ]
    )


def sources_keyboard():

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


def destinations_keyboard():

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


def settings_keyboard():

    s = db["settings"]

    return InlineKeyboardMarkup(
        inline_keyboard=[

            [
                InlineKeyboardButton(
                    text=(
                        f"⏱ Interval: "
                        f"{s['interval']}s"
                    ),
                    callback_data="setting:interval",
                )
            ],

            [
                InlineKeyboardButton(
                    text=(
                        f"📦 Album wait: "
                        f"{s['album_wait']}s"
                    ),
                    callback_data="setting:album",
                )
            ],

            [
                InlineKeyboardButton(
                    text=(
                        f"🔁 Retries: "
                        f"{s['retries']}"
                    ),
                    callback_data="setting:retries",
                )
            ],

            [
                InlineKeyboardButton(
                    text=(
                        f"📚 Queue: "
                        f"{s['max_queue']}"
                    ),
                    callback_data="setting:queue",
                )
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
                )
            ],

            [
                InlineKeyboardButton(
                    text="⬅️ Back",
                    callback_data="home",
                )
            ],
        ]
    )


# ============================================================
# CHAT RESOLUTION + PERMISSION CHECK
# ============================================================

async def resolve_chat(
    value: str,
) -> dict[str, str]:

    if bot is None:
        raise RuntimeError(
            "Bot is not initialized."
        )

    value = value.strip()

    if not value:
        raise ValueError(
            "Channel username or ID is empty."
        )

    chat = await bot.get_chat(
        value
    )

    if chat.type != ChatType.CHANNEL:

        raise ValueError(
            "This is not a Telegram channel."
        )

    member = await bot.get_chat_member(
        chat.id,
        bot.id,
    )

    if member.status not in {
        ChatMemberStatus.ADMINISTRATOR,
        ChatMemberStatus.CREATOR,
    }:

        raise PermissionError(
            "Bot must be administrator "
            "in this channel."
        )

    title = (
        chat.title
        or chat.username
        or str(chat.id)
    )

    username = (
        chat.username
        or ""
    )

    return {
        "id": str(chat.id),
        "title": title,
        "username": username,
    }


async def verify_destination(
    chat_id: str,
):

    if bot is None:
        return

    member = await bot.get_chat_member(
        int(chat_id),
        bot.id,
    )

    if member.status not in {
        ChatMemberStatus.ADMINISTRATOR,
        ChatMemberStatus.CREATOR,
    }:

        raise PermissionError(
            "Bot is not administrator "
            "in destination channel."
        )

    if (
        member.status
        == ChatMemberStatus.ADMINISTRATOR
    ):

        if not member.can_post_messages:

            raise PermissionError(
                "Bot does not have "
                "'Post Messages' permission."
            )


# ============================================================
# QUEUES
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


async def start_worker(
    destination_id: str,
):

    task = destination_tasks.get(
        destination_id
    )

    if task and not task.done():
        return

    destination_tasks[
        destination_id
    ] = asyncio.create_task(
        destination_worker(
            destination_id
        )
    )


async def stop_worker(
    destination_id: str,
):

    task = destination_tasks.pop(
        destination_id,
        None,
    )

    if not task:
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

        await start_worker(
            destination_id
        )

    for destination_id in list(
        destination_tasks
    ):

        if destination_id not in wanted:

            await stop_worker(
                destination_id
            )


# ============================================================
# COPY SINGLE
# ============================================================

async def copy_single(
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

            log.info(
                "COPY source=%s destination=%s message=%s",
                source_id,
                destination_id,
                message_id,
            )

            await bot.copy_message(
                chat_id=int(
                    destination_id
                ),
                from_chat_id=int(
                    source_id
                ),
                message_id=message_id,
            )

            log.info(
                "COPY SUCCESS source=%s destination=%s message=%s",
                source_id,
                destination_id,
                message_id,
            )

            return True

        except TelegramRetryAfter as exc:

            wait = (
                int(exc.retry_after)
                + 1
            )

            log.warning(
                "FloodWait: %ss",
                wait,
            )

            await asyncio.sleep(
                wait
            )

        except TelegramForbiddenError as exc:

            error = str(exc)

            db["stats"][
                "last_error"
            ] = error

            log.error(
                "FORBIDDEN destination=%s: %s",
                destination_id,
                error,
            )

            return False

        except TelegramBadRequest as exc:

            error = str(exc)

            db["stats"][
                "last_error"
            ] = error

            log.error(
                "BAD REQUEST source=%s destination=%s message=%s: %s",
                source_id,
                destination_id,
                message_id,
                error,
            )

            return False

        except TelegramNetworkError as exc:

            if attempt >= retries:

                db["stats"][
                    "last_error"
                ] = str(exc)

                log.error(
                    "Network error after retries."
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

                db["stats"][
                    "last_error"
                ] = str(exc)

                log.exception(
                    "Unexpected copy error."
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
# COPY ALBUM
# ============================================================

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

            log.info(
                "COPY ALBUM source=%s destination=%s messages=%s",
                source_id,
                destination_id,
                message_ids,
            )

            await bot.copy_messages(
                chat_id=int(
                    destination_id
                ),
                from_chat_id=int(
                    source_id
                ),
                message_ids=message_ids,
            )

            log.info(
                "ALBUM SUCCESS source=%s destination=%s",
                source_id,
                destination_id,
            )

            return True

        except TelegramRetryAfter as exc:

            await asyncio.sleep(
                int(
                    exc.retry_after
                ) + 1
            )

        except TelegramBadRequest as exc:

            log.warning(
                "Bulk album copy failed: %s",
                exc,
            )

            # Fallback to individual copies.
            success = True

            for message_id in message_ids:

                ok = await copy_single(
                    source_id,
                    destination_id,
                    message_id,
                )

                if not ok:
                    success = False

                await asyncio.sleep(
                    float(
                        db["settings"][
                            "interval"
                        ]
                    )
                )

            return success

        except TelegramForbiddenError as exc:

            db["stats"][
                "last_error"
            ] = str(exc)

            log.error(
                "Album destination forbidden: %s",
                exc,
            )

            return False

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

                db["stats"][
                    "last_error"
                ] = str(exc)

                log.exception(
                    "Album copy failed."
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
        "Worker started destination=%s",
        destination_id,
    )

    while True:

        item = await queue.get()

        try:

            if not db["settings"]["enabled"]:
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

                ok = await copy_single(
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

            await asyncio.sleep(
                float(
                    db["settings"][
                        "interval"
                    ]
                )
            )

        except asyncio.CancelledError:

            raise

        except Exception as exc:

            log.exception(
                "Worker error: %s",
                exc,
            )

            db["stats"][
                "last_error"
            ] = str(exc)

            await asyncio.sleep(2)

        finally:

            queue.task_done()


# ============================================================
# QUEUE FOR DESTINATIONS
# ============================================================

async def queue_for_destinations(
    source_id: str,
    message_ids: list[int],
    is_album: bool,
):

    if not db["settings"]["enabled"]:
        return

    for destination in list(
        db["destinations"]
    ):

        destination_id = str(
            destination["id"]
        )

        await start_worker(
            destination_id
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

            log.info(
                "QUEUED source=%s destination=%s messages=%s",
                source_id,
                destination_id,
                message_ids,
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
                "Queue full for "
                + destination_id
            )

            log.error(
                "Queue full destination=%s",
                destination_id,
            )

    await save_db()


# ============================================================
# ALBUM
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

        message_ids = sorted(
            set(message_ids)
        )

        db["stats"][
            "albums"
        ] += 1

        await queue_for_destinations(
            source_id,
            message_ids,
            True,
        )

    except asyncio.CancelledError:

        raise

    except Exception as exc:

        log.exception(
            "Album flush error."
        )

        db["stats"][
            "last_error"
        ] = str(exc)

    finally:

        album_tasks.pop(
            key,
            None,
        )


# ============================================================
# CHANNEL POST
# ============================================================

@router.channel_post()
async def channel_post_handler(
    message: Message,
):

    source_id = str(
        message.chat.id
    )

    log.info(
        "CHANNEL POST received source=%s message=%s",
        source_id,
        message.message_id,
    )

    # Ignore unconfigured channels.
    if source_id not in source_ids():

        log.info(
            "Ignoring unconfigured source=%s",
            source_id,
        )

        return

    if not db["settings"]["enabled"]:

        log.info(
            "Bot is stopped."
        )

        return

    message_id = int(
        message.message_id
    )

    dedupe_key = (
        f"{source_id}:{message_id}"
    )

    async with seen_lock:

        if dedupe_key in db["seen"]:

            log.info(
                "Duplicate ignored: %s",
                dedupe_key,
            )

            return

        db["seen"][
            dedupe_key
        ] = int(
            time.time()
        )

        # Keep memory/database small.
        if len(db["seen"]) > 10000:

            oldest = sorted(
                db["seen"].items(),
                key=lambda x: x[1],
            )[:2000]

            for key, _ in oldest:

                db["seen"].pop(
                    key,
                    None
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

        old_task = album_tasks.get(
            key
        )

        if old_task:

            old_task.cancel()

        album_tasks[
            key
        ] = asyncio.create_task(
            flush_album(
                source_id,
                str(media_group_id),
            )
        )

    else:

        await queue_for_destinations(
            source_id,
            [message_id],
            False,
        )


# ============================================================
# START / ADMIN
# ============================================================

@router.message(CommandStart())
async def start_handler(
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


@router.message(Command("admin"))
async def admin_handler(
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


@router.message(Command("cancel"))
async def cancel_handler(
    message: Message,
    state: FSMContext,
):

    await state.clear()

    if is_admin(message):

        await message.answer(
            "❌ Cancelled.",
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

    if not query.message:
        return

    # --------------------------------------------------------
    # HOME
    # --------------------------------------------------------

    if data == "home":

        await state.clear()

        await query.message.edit_text(
            home_text(),
            reply_markup=home_keyboard(),
        )

        return

    # --------------------------------------------------------
    # TOGGLE
    # --------------------------------------------------------

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

    # --------------------------------------------------------
    # SOURCES
    # --------------------------------------------------------

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
                1,
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

    # --------------------------------------------------------
    # DESTINATIONS
    # --------------------------------------------------------

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
                1,
            ):

                destination_id = str(
                    item["id"]
                )

                queue = get_queue(
                    destination_id
                )

                text += (
                    f"{index}. "
                    f"<b>{display_name(item)}</b>\n"
                    f"<code>{destination_id}</code>\n"
                    f"Queue: <b>{queue.qsize()}</b>\n\n"
                )

        await query.message.edit_text(
            text,
            reply_markup=destinations_keyboard(),
        )

        return

    # --------------------------------------------------------
    # SETTINGS
    # --------------------------------------------------------

    if data == "settings":

        await query.message.edit_text(
            "⚙️ <b>Settings</b>",
            reply_markup=settings_keyboard(),
        )

        return

    # --------------------------------------------------------
    # STATUS
    # --------------------------------------------------------

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
            "📊 <b>Bot Status</b>\n\n"

            f"Running: "
            f"<b>{db['settings']['enabled']}</b>\n"

            f"Workers: "
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
            "Send channel username or ID:\n\n"
            "<code>@channelusername</code>\n"
            "or\n"
            "<code>-1001234567890</code>\n\n"
            "⚠️ Bot must be administrator "
            "in the source channel."
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
            "Send channel username or ID:\n\n"
            "<code>@channelusername</code>\n"
            "or\n"
            "<code>-1001234567890</code>\n\n"
            "⚠️ Bot must be administrator "
            "with Post Messages permission."
        )

        return

    # --------------------------------------------------------
    # DELETE SOURCE
    # --------------------------------------------------------

    if data.startswith(
        "delete_source:"
    ):

        try:

            index = int(
                data.split(":")[1]
            )

        except Exception:

            return

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

            prefix = (
                source_id + ":"
            )

            db["seen"] = {
                key: value
                for key, value
                in db["seen"].items()
                if not key.startswith(
                    prefix
                )
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

        try:

            index = int(
                data.split(":")[1]
            )

        except Exception:

            return

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

            await stop_worker(
                destination_id
            )

            destination_queues.pop(
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
            "⏱ <b>Set Interval</b>\n\n"
            "Enter seconds.\n"
            "Example: <code>2</code>\n\n"
            "Recommended: 1-5 seconds."
        )

        return

    if data == "setting:album":

        await state.set_state(
            SetAlbumWait.value
        )

        await query.message.edit_text(
            "📦 <b>Album Wait</b>\n\n"
            "Enter seconds.\n"
            "Example: <code>1.5</code>"
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
            "📚 <b>Queue Limit</b>\n\n"
            "Enter 10-50000.\n"
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
# ADD SOURCE
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

    if not value:

        await message.answer(
            "❌ Send a channel username or ID."
        )

        return

    try:

        item = await resolve_chat(
            value
        )

        chat_id = str(
            item["id"]
        )

        if chat_id in source_ids():

            await message.answer(
                "⚠️ Source already exists."
            )

        else:

            db["sources"].append(
                item
            )

            await save_db()

            await message.answer(
                (
                    "✅ <b>Source Added</b>\n\n"
                    f"Name: "
                    f"<b>{item['title']}</b>\n"
                    f"ID: <code>{item['id']}</code>\n\n"
                    "📡 Only NEW posts will be monitored."
                ),
                reply_markup=sources_keyboard(),
            )

    except Exception as exc:

        log.error(
            "Add source failed: %s",
            exc,
        )

        await message.answer(
            (
                "❌ <b>Could not add source</b>\n\n"
                f"<code>{str(exc)[:1000]}</code>"
            )
        )

    finally:

        await state.clear()


# ============================================================
# ADD DESTINATION
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

    if not value:

        await message.answer(
            "❌ Send a channel username or ID."
        )

        return

    try:

        item = await resolve_chat(
            value
        )

        chat_id = str(
            item["id"]
        )

        await verify_destination(
            chat_id
        )

        if chat_id in destination_ids():

            await message.answer(
                "⚠️ Destination already exists."
            )

        else:

            db[
                "destinations"
            ].append(item)

            await save_db()

            await start_worker(
                chat_id
            )

            await message.answer(
                (
                    "✅ <b>Destination Added</b>\n\n"
                    f"Name: "
                    f"<b>{item['title']}</b>\n"
                    f"ID: <code>{item['id']}</code>\n\n"
                    "🚀 Ready to receive copied posts."
                ),
                reply_markup=destinations_keyboard(),
            )

    except Exception as exc:

        log.error(
            "Add destination failed: %s",
            exc,
        )

        await message.answer(
            (
                "❌ <b>Could not add destination</b>\n\n"
                f"<code>{str(exc)[:1000]}</code>"
            )
        )

    finally:

        await state.clear()


# ============================================================
# SET INTERVAL
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

        if not 0.1 <= value <= 3600:
            raise ValueError

        db["settings"][
            "interval"
        ] = value

        await save_db()

        await message.answer(
            (
                f"✅ Interval set to "
                f"<b>{value}s</b>."
            ),
            reply_markup=settings_keyboard(),
        )

    except ValueError:

        await message.answer(
            "❌ Enter a number between 0.1 and 3600."
        )

        return

    finally:

        await state.clear()


# ============================================================
# SET ALBUM WAIT
# ============================================================

@router.message(
    SetAlbumWait.value
)
async def set_album_handler(
    message: Message,
    state: FSMContext,
):

    try:

        value = float(
            (
                message.text or ""
            ).strip()
        )

        if not 0.2 <= value <= 10:
            raise ValueError

        db["settings"][
            "album_wait"
        ] = value

        await save_db()

        await message.answer(
            (
                f"✅ Album wait set to "
                f"<b>{value}s</b>."
            ),
            reply_markup=settings_keyboard(),
        )

    except ValueError:

        await message.answer(
            "❌ Enter a number between 0.2 and 10."
        )

        return

    finally:

        await state.clear()


# ============================================================
# SET RETRIES
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

        if not 0 <= value <= 20:
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

        return

    finally:

        await state.clear()


# ============================================================
# SET QUEUE
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

        if not 10 <= value <= 50000:
            raise ValueError

        db["settings"][
            "max_queue"
        ] = value

        await save_db()

        # Recreate queues with new size.
        for destination_id in list(
            destination_queues
        ):

            queue = destination_queues[
                destination_id
            ]

            if queue.empty():

                destination_queues[
                    destination_id
                ] = asyncio.Queue(
                    maxsize=value
                )

        await message.answer(
            "✅ Queue limit updated.",
            reply_markup=settings_keyboard(),
        )

    except ValueError:

        await message.answer(
            "❌ Enter an integer from 10 to 50000."
        )

        return

    finally:

        await state.clear()


# ============================================================
# HEALTH
# ============================================================

async def health_handler(
    request: web.Request,
):

    return web.json_response(
        {
            "ok": True,
            "mode": "webhook",
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
            "time": int(
                time.time()
            ),
        }
    )


# ============================================================
# TELEGRAM WEBHOOK
# ============================================================

async def telegram_webhook(
    request: web.Request,
):

    try:

        raw = await request.read()

        if not raw:

            return web.Response(
                status=400,
                text="Empty update",
            )

        update = Update.model_validate_json(
            raw
        )

        await dp.feed_update(
            bot,
            update,
        )

        return web.Response(
            text="OK"
        )

    except Exception as exc:

        log.exception(
            "Webhook update error: %s",
            exc,
        )

        return web.Response(
            status=500,
            text="ERROR",
        )


# ============================================================
# START WEB SERVER
# ============================================================

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

    app.router.add_post(
        WEBHOOK_PATH,
        telegram_webhook,
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
        "HTTP server started on port %s",
        PORT,
    )


# ============================================================
# SET WEBHOOK
# ============================================================

async def setup_webhook():

    if bot is None:
        return

    webhook_url = (
        RENDER_URL
        + WEBHOOK_PATH
    )

    log.info(
        "Setting Telegram webhook: %s",
        webhook_url,
    )

    await bot.set_webhook(
        url=webhook_url,
        secret_token=WEBHOOK_SECRET,
        allowed_updates=[
            "message",
            "callback_query",
            "channel_post",
        ],
        drop_pending_updates=False,
    )

    info = await bot.get_webhook_info()

    log.info(
        "Webhook active: %s",
        info.url,
    )

    if info.last_error_message:

        log.error(
            "Telegram webhook last error: %s",
            info.last_error_message,
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

        await stop_worker(
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

    global dp

    dp = Dispatcher()

    dp.include_router(
        router
    )

    me = await bot.get_me()

    log.info(
        "Bot connected: @%s | ID=%s",
        me.username,
        me.id,
    )

    await start_web_server()

    await setup_webhook()

    await ensure_workers()

    log.info(
        "========================================"
    )

    log.info(
        "BOT RUNNING IN WEBHOOK MODE"
    )

    log.info(
        "Only NEW channel posts are monitored."
    )

    log.info(
        "No API ID / API HASH required."
    )

    log.info(
        "========================================"
    )

    try:

        # Keep Render process alive.
        await asyncio.Event().wait()

    finally:

        await shutdown()

        try:

            await bot.delete_webhook(
                drop_pending_updates=False
            )

        except Exception:
            pass

        await bot.session.close()


if __name__ == "__main__":

    try:

        asyncio.run(
            main()
        )

    except KeyboardInterrupt:

        pass
