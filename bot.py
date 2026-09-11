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

DATA_FILE = Path(
    os.getenv("DATA_FILE", "data.json")
)

RENDER_URL = os.getenv(
    "RENDER_URL",
    "https://paid-rczj.onrender.com",
).strip().rstrip("/")

WEBHOOK_SECRET = os.getenv(
    "WEBHOOK_SECRET",
    "change-this-secret",
).strip()

WEBHOOK_PATH = (
    f"/telegram/webhook/{WEBHOOK_SECRET}"
)


if not BOT_TOKEN:
    raise RuntimeError(
        "BOT_TOKEN is missing."
    )

if not ADMIN_ID_RAW.isdigit():
    raise RuntimeError(
        "ADMIN_ID must be numeric."
    )

if (
    not WEBHOOK_SECRET
    or WEBHOOK_SECRET == "change-this-secret"
):
    raise RuntimeError(
        "Please set a real WEBHOOK_SECRET in Render Environment Variables."
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
    "route-channel-cycle-bot"
)


# ============================================================
# DEFAULT SETTINGS
# ============================================================

DEFAULT_SETTINGS = {
    # Bot accepts new posts.
    "enabled": True,

    # Delay between copies.
    "interval": 1.0,

    # Temporary error retries.
    "retries": 8,

    # Album collection time.
    "album_wait": 1.5,

    # Maximum waiting queue size.
    "max_queue": 10000,

    # --------------------------------------------------------
    # CYCLING
    # --------------------------------------------------------

    # Repeat saved posts forever.
    "cycle_enabled": True,

    # Delay before starting next cycle.
    "cycle_delay": 1.0,

    # Delay between posts during cycle.
    "cycle_interval": 1.0,
}


# ============================================================
# DATABASE
# ============================================================

def default_db() -> dict[str, Any]:

    return {
        "settings": DEFAULT_SETTINGS.copy(),

        "sources": [],

        "destinations": [],

        "routes": {},

        # ----------------------------------------------------
        # POST HISTORY
        # ----------------------------------------------------
        #
        # source_id:
        # [
        #     {
        #         "message_ids": [1],
        #         "is_album": false
        #     },
        #     {
        #         "message_ids": [2, 3],
        #         "is_album": true
        #     }
        # ]
        #
        "history": {},

        "stats": {
            "received": 0,
            "queued": 0,
            "sent": 0,
            "failed": 0,
            "albums": 0,
            "cycles": 0,

            "last_received": None,
            "last_sent": None,
            "last_cycle": None,
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
            "Could not read data.json. "
            "Starting fresh."
        )

        return default_db()

    fresh = default_db()

    # --------------------------------------------------------
    # SETTINGS
    # --------------------------------------------------------

    if isinstance(
        data.get("settings"),
        dict,
    ):

        fresh["settings"].update(
            data["settings"]
        )

    # --------------------------------------------------------
    # SOURCES
    # --------------------------------------------------------

    if isinstance(
        data.get("sources"),
        list,
    ):

        fresh["sources"] = data[
            "sources"
        ]

    # --------------------------------------------------------
    # DESTINATIONS
    # --------------------------------------------------------

    if isinstance(
        data.get("destinations"),
        list,
    ):

        fresh["destinations"] = data[
            "destinations"
        ]

    # --------------------------------------------------------
    # ROUTES
    # --------------------------------------------------------

    if isinstance(
        data.get("routes"),
        dict,
    ):

        fresh["routes"] = data[
            "routes"
        ]

    # --------------------------------------------------------
    # OLD DATABASE MIGRATION
    # --------------------------------------------------------

    elif (
        fresh["sources"]
        and fresh["destinations"]
    ):

        log.warning(
            "Migrating old global source/destination database."
        )

        for source in fresh[
            "sources"
        ]:

            sid = str(
                source["id"]
            )

            fresh["routes"][sid] = {
                "source": source,
                "destinations": list(
                    fresh["destinations"]
                ),
            }

    # --------------------------------------------------------
    # HISTORY
    # --------------------------------------------------------

    if isinstance(
        data.get("history"),
        dict,
    ):

        fresh["history"] = data[
            "history"
        ]

    # --------------------------------------------------------
    # STATS
    # --------------------------------------------------------

    if isinstance(
        data.get("stats"),
        dict,
    ):

        fresh["stats"].update(
            data["stats"]
        )

    # --------------------------------------------------------
    # SEEN
    # --------------------------------------------------------

    if isinstance(
        data.get("seen"),
        dict,
    ):

        fresh["seen"] = data[
            "seen"
        ]

    return fresh


db = load_db()

db_lock = asyncio.Lock()
seen_lock = asyncio.Lock()


async def save_db():

    async with db_lock:

        DATA_FILE.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        temporary_file = (
            DATA_FILE.with_suffix(
                ".tmp"
            )
        )

        temporary_file.write_text(
            json.dumps(
                db,
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )

        temporary_file.replace(
            DATA_FILE
        )


# ============================================================
# RUNTIME
# ============================================================

router = Router()

bot: Bot | None = None

dp: Dispatcher | None = None

server_runner: web.AppRunner | None = None


# ============================================================
# SOURCE QUEUES
# ============================================================

source_queues: dict[
    str,
    asyncio.Queue,
] = {}

source_tasks: dict[
    str,
    asyncio.Task,
] = {}


# ============================================================
# ALBUM RUNTIME
# ============================================================

album_tasks: dict[
    tuple[str, str],
    asyncio.Task,
] = {}

album_buffer: dict[
    tuple[str, str],
    list[int],
] = defaultdict(list)


# ============================================================
# FSM
# ============================================================

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


class SetCycleDelay(StatesGroup):
    value = State()


class SetCycleInterval(StatesGroup):
    value = State()


# ============================================================
# ADMIN
# ============================================================

def is_admin(
    obj: Message | CallbackQuery,
) -> bool:

    return bool(
        obj.from_user
        and obj.from_user.id == ADMIN_ID
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


def get_source(
    source_id: str,
) -> dict[str, Any] | None:

    source_id = str(source_id)

    for source in db["sources"]:

        if str(
            source["id"]
        ) == source_id:

            return source

    return None


def get_destination(
    destination_id: str,
) -> dict[str, Any] | None:

    destination_id = str(destination_id)

    for destination in db["destinations"]:

        if str(
            destination["id"]
        ) == destination_id:

            return destination

    return None


def display_name(
    item: dict[str, Any],
) -> str:

    return (
        item.get("title")
        or item.get("username")
        or str(
            item.get("id", "Unknown")
        )
    )


def route_destinations(
    source_id: str,
) -> list[dict[str, Any]]:

    route = db[
        "routes"
    ].get(
        str(source_id),
        {},
    )

    destinations = route.get(
        "destinations",
        [],
    )

    if not isinstance(
        destinations,
        list,
    ):

        return []

    return destinations


def route_destination_ids(
    source_id: str,
) -> set[str]:

    return {
        str(item["id"])
        for item in route_destinations(
            source_id
        )
    }


def route_count() -> int:

    return sum(
        1
        for source in db["sources"]
        if route_destinations(
            str(source["id"])
        )
    )


def history_count(
    source_id: str,
) -> int:

    history = db[
        "history"
    ].get(
        str(source_id),
        [],
    )

    return len(history)


# ============================================================
# HOME
# ============================================================

def home_text() -> str:

    settings = db[
        "settings"
    ]

    stats = db[
        "stats"
    ]

    running = (
        "🟢 RUNNING"
        if settings["enabled"]
        else "🔴 STOPPED"
    )

    cycling = (
        "🟢 ON"
        if settings["cycle_enabled"]
        else "🔴 OFF"
    )

    return (
        "🤖 <b>Route Channel Cycle Bot</b>\n\n"

        f"Status: <b>{running}</b>\n"
        f"Sources: <b>{len(db['sources'])}</b>\n"
        f"Destinations: "
        f"<b>{len(db['destinations'])}</b>\n"
        f"Routes: <b>{route_count()}</b>\n\n"

        f"🔁 Cycle: <b>{cycling}</b>\n"
        f"⏱ Copy interval: "
        f"<b>{settings['interval']}s</b>\n"
        f"🔄 Cycle delay: "
        f"<b>{settings['cycle_delay']}s</b>\n"
        f"📦 Album wait: "
        f"<b>{settings['album_wait']}s</b>\n"
        f"🔁 Retries: "
        f"<b>{settings['retries']}</b>\n\n"

        f"📥 Received: "
        f"<b>{stats['received']}</b>\n"
        f"📤 Sent: "
        f"<b>{stats['sent']}</b>\n"
        f"❌ Failed: "
        f"<b>{stats['failed']}</b>\n"
        f"♻️ Cycles: "
        f"<b>{stats['cycles']}</b>"
    )


# ============================================================
# KEYBOARDS
# ============================================================

def home_keyboard():

    enabled = db[
        "settings"
    ]["enabled"]

    cycle = db[
        "settings"
    ]["cycle_enabled"]

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

                InlineKeyboardButton(
                    text=(
                        "♻️ Cycle ON"
                        if cycle
                        else "♻️ Cycle OFF"
                    ),
                    callback_data="cycle_toggle",
                ),
            ],

            [
                InlineKeyboardButton(
                    text="🔀 Routes",
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

    for index, source in enumerate(
        db["sources"]
    ):

        sid = str(
            source["id"]
        )

        count = len(
            route_destinations(
                sid
            )
        )

        history = history_count(
            sid
        )

        rows.append(
            [
                InlineKeyboardButton(
                    text=(
                        f"🔀 "
                        f"{display_name(source)} "
                        f"[{count}] "
                        f"♻️{history}"
                    ),
                    callback_data=(
                        f"route:{index}"
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


def route_keyboard(
    source_id: str,
):

    selected = route_destination_ids(
        source_id
    )

    rows = []

    for index, destination in enumerate(
        db["destinations"]
    ):

        did = str(
            destination["id"]
        )

        mark = (
            "✅"
            if did in selected
            else "⬜"
        )

        rows.append(
            [
                InlineKeyboardButton(
                    text=(
                        f"{mark} "
                        f"{display_name(destination)}"
                    ),
                    callback_data=(
                        f"route_dest:"
                        f"{source_id}:"
                        f"{index}"
                    ),
                )
            ]
        )

    rows.append(
        [
            InlineKeyboardButton(
                text="➕ Add Destination",
                callback_data=(
                    f"new_dest_for:"
                    f"{source_id}"
                ),
            )
        ]
    )

    rows.append(
        [
            InlineKeyboardButton(
                text="🗑 Clear History",
                callback_data=(
                    f"clear_history:"
                    f"{source_id}"
                ),
            )
        ]
    )

    rows.append(
        [
            InlineKeyboardButton(
                text="❌ Remove Source",
                callback_data=(
                    f"remove_source:"
                    f"{source_id}"
                ),
            )
        ]
    )

    rows.append(
        [
            InlineKeyboardButton(
                text="⬅️ Routes",
                callback_data="sources",
            )
        ]
    )

    return InlineKeyboardMarkup(
        inline_keyboard=rows
    )


def destinations_keyboard():

    rows = []

    for index, destination in enumerate(
        db["destinations"]
    ):

        did = str(
            destination["id"]
        )

        used_by = sum(
            1
            for source in db["sources"]
            if did
            in route_destination_ids(
                str(source["id"])
            )
        )

        rows.append(
            [
                InlineKeyboardButton(
                    text=(
                        f"❌ "
                        f"{display_name(destination)} "
                        f"[{used_by} routes]"
                    ),
                    callback_data=(
                        f"delete_destination:"
                        f"{index}"
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

    settings = db[
        "settings"
    ]

    return InlineKeyboardMarkup(
        inline_keyboard=[

            [
                InlineKeyboardButton(
                    text=(
                        f"⏱ Copy interval: "
                        f"{settings['interval']}s"
                    ),
                    callback_data=(
                        "setting:interval"
                    ),
                )
            ],

            [
                InlineKeyboardButton(
                    text=(
                        f"🔄 Cycle delay: "
                        f"{settings['cycle_delay']}s"
                    ),
                    callback_data=(
                        "setting:cycle_delay"
                    ),
                )
            ],

            [
                InlineKeyboardButton(
                    text=(
                        f"📦 Album wait: "
                        f"{settings['album_wait']}s"
                    ),
                    callback_data=(
                        "setting:album"
                    ),
                )
            ],

            [
                InlineKeyboardButton(
                    text=(
                        f"🔁 Retries: "
                        f"{settings['retries']}"
                    ),
                    callback_data=(
                        "setting:retries"
                    ),
                )
            ],

            [
                InlineKeyboardButton(
                    text=(
                        f"📚 Queue: "
                        f"{settings['max_queue']}"
                    ),
                    callback_data=(
                        "setting:queue"
                    ),
                ],

                InlineKeyboardButton(
                    text=(
                        f"♻️ Cycle "
                        f"{'ON' if settings['cycle_enabled'] else 'OFF'}"
                    ),
                    callback_data="cycle_toggle",
                ),
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
# TELEGRAM CHAT RESOLUTION
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
            "Only Telegram channels are supported."
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

    return {
        "id": str(chat.id),

        "title": (
            chat.title
            or chat.username
            or str(chat.id)
        ),

        "username": (
            chat.username
            or ""
        ),
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
            "in destination."
        )

    if (
        member.status
        == ChatMemberStatus.ADMINISTRATOR
        and not member.can_post_messages
    ):

        raise PermissionError(
            "Bot needs Post Messages "
            "permission."
        )


# ============================================================
# HISTORY
# ============================================================

async def add_history(
    source_id: str,
    message_ids: list[int],
    is_album: bool,
):

    source_id = str(
        source_id
    )

    history = db[
        "history"
    ].setdefault(
        source_id,
        [],
    )

    clean_ids = sorted(
        {
            int(x)
            for x in message_ids
        }
    )

    if not clean_ids:
        return

    # --------------------------------------------------------
    # Do not add duplicate history item.
    # --------------------------------------------------------

    existing_keys = {
        tuple(
            int(x)
            for x in item.get(
                "message_ids",
                [],
            )
        )
        for item in history
        if isinstance(item, dict)
    }

    key = tuple(clean_ids)

    if key in existing_keys:
        return

    history.append(
        {
            "message_ids": clean_ids,
            "is_album": bool(
                is_album
            ),
            "created_at": int(
                time.time()
            ),
        }
    )

    # --------------------------------------------------------
    # Keep history reasonable.
    # 50,000 posts/albums per source.
    # --------------------------------------------------------

    if len(history) > 50000:

        del history[
            :len(history) - 50000
        ]

    await save_db()


def get_history(
    source_id: str,
) -> list[dict[str, Any]]:

    history = db[
        "history"
    ].get(
        str(source_id),
        [],
    )

    if not isinstance(
        history,
        list,
    ):

        return []

    return history


# ============================================================
# WORKERS
# ============================================================

async def start_source_worker(
    source_id: str,
):

    source_id = str(
        source_id
    )

    if source_id not in source_queues:

        source_queues[
            source_id
        ] = asyncio.Queue(
            maxsize=int(
                db["settings"][
                    "max_queue"
                ]
            )
        )

    task = source_tasks.get(
        source_id
    )

    if (
        task
        and not task.done()
    ):

        return

    source_tasks[
        source_id
    ] = asyncio.create_task(
        source_worker(
            source_id
        )
    )

    log.info(
        "SOURCE WORKER STARTED "
        "source=%s",
        source_id,
    )


async def stop_source_worker(
    source_id: str,
):

    source_id = str(
        source_id
    )

    task = source_tasks.pop(
        source_id,
        None,
    )

    if task:

        task.cancel()

        try:

            await task

        except asyncio.CancelledError:

            pass

    source_queues.pop(
        source_id,
        None,
    )


async def ensure_workers():

    wanted = set()

    if db["settings"]["enabled"]:

        for source in db["sources"]:

            sid = str(
                source["id"]
            )

            if route_destinations(
                sid
            ):

                wanted.add(
                    sid
                )

    for sid in wanted:

        await start_source_worker(
            sid
        )

    for sid in list(
        source_tasks
    ):

        if sid not in wanted:

            await stop_source_worker(
                sid
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
        db["settings"][
            "retries"
        ]
    )

    for attempt in range(
        retries + 1
    ):

        try:

            log.info(
                "COPY source=%s "
                "destination=%s "
                "message=%s",
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
                message_id=int(
                    message_id
                ),
            )

            return True

        except TelegramRetryAfter as exc:

            wait = (
                int(
                    exc.retry_after
                )
                + 1
            )

            log.warning(
                "Flood wait %ss "
                "destination=%s",
                wait,
                destination_id,
            )

            await asyncio.sleep(
                wait
            )

        except TelegramNetworkError as exc:

            if attempt >= retries:

                db["stats"][
                    "last_error"
                ] = str(exc)

                return False

            await asyncio.sleep(
                min(
                    2 ** attempt,
                    30,
                )
            )

        except TelegramForbiddenError as exc:

            db["stats"][
                "last_error"
            ] = str(exc)

            log.error(
                "Forbidden destination=%s: %s",
                destination_id,
                exc,
            )

            return False

        except TelegramBadRequest as exc:

            db["stats"][
                "last_error"
            ] = str(exc)

            log.error(
                "BadRequest source=%s "
                "destination=%s "
                "message=%s: %s",
                source_id,
                destination_id,
                message_id,
                exc,
            )

            return False

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
) -> int:

    total = 0

    for message_id in message_ids:

        success = await copy_single(
            source_id,
            destination_id,
            message_id,
        )

        if success:

            total += 1

        await asyncio.sleep(
            float(
                db["settings"][
                    "interval"
                ]
            )
        )

    return total


# ============================================================
# SEND HISTORY ITEM
# ============================================================

async def send_history_item(
    source_id: str,
    item: dict[str, Any],
) -> int:

    message_ids = [
        int(x)
        for x in item.get(
            "message_ids",
            [],
        )
    ]

    if not message_ids:
        return 0

    destinations = list(
        route_destinations(
            source_id
        )
    )

    if not destinations:
        return 0

    total_sent = 0

    is_album = bool(
        item.get(
            "is_album",
            False,
        )
    )

    # --------------------------------------------------------
    # Every destination is independent.
    # --------------------------------------------------------

    for destination in destinations:

        destination_id = str(
            destination["id"]
        )

        try:

            if is_album:

                sent = await copy_album(
                    source_id,
                    destination_id,
                    message_ids,
                )

                total_sent += sent

                if sent < len(
                    message_ids
                ):

                    db["stats"][
                        "failed"
                    ] += (
                        len(message_ids)
                        - sent
                    )

            else:

                success = await copy_single(
                    source_id,
                    destination_id,
                    message_ids[0],
                )

                if success:

                    total_sent += 1

                else:

                    db["stats"][
                        "failed"
                    ] += 1

        except Exception as exc:

            db["stats"][
                "failed"
            ] += len(
                message_ids
            )

            db["stats"][
                "last_error"
            ] = str(exc)

            log.exception(
                "Destination failed but "
                "source cycle continues. "
                "source=%s destination=%s",
                source_id,
                destination_id,
            )

        await asyncio.sleep(
            float(
                db["settings"][
                    "interval"
                ]
            )
        )

    return total_sent


# ============================================================
# SOURCE WORKER
# ============================================================

async def source_worker(
    source_id: str,
):

    source_id = str(
        source_id
    )

    queue = source_queues[
        source_id
    ]

    while True:

        try:

            # =================================================
            # 1. PROCESS NEW POSTS FIRST
            # =================================================

            try:

                item = await asyncio.wait_for(
                    queue.get(),
                    timeout=1.0,
                )

                try:

                    sent = await send_history_item(
                        source_id,
                        item,
                    )

                    db["stats"][
                        "sent"
                    ] += sent

                    if sent:

                        db["stats"][
                            "last_sent"
                        ] = int(
                            time.time()
                        )

                    await save_db()

                finally:

                    queue.task_done()

                continue

            except asyncio.TimeoutError:

                pass

            # =================================================
            # 2. NO NEW POST IN QUEUE
            # =================================================

            if not db["settings"][
                "enabled"
            ]:

                await asyncio.sleep(
                    1
                )

                continue

            if not db["settings"][
                "cycle_enabled"
            ]:

                await asyncio.sleep(
                    1
                )

                continue

            history = get_history(
                source_id
            )

            if not history:

                # No saved post yet.
                await asyncio.sleep(
                    1
                )

                continue

            # =================================================
            # 3. WAIT BEFORE NEXT CYCLE
            # =================================================

            cycle_delay = float(
                db["settings"][
                    "cycle_delay"
                ]
            )

            if cycle_delay > 0:

                await asyncio.sleep(
                    cycle_delay
                )

            # =================================================
            # 4. CYCLE FROM FIRST POST
            # =================================================

            log.info(
                "STARTING CYCLE "
                "source=%s "
                "history=%s",
                source_id,
                len(history),
            )

            db["stats"][
                "cycles"
            ] += 1

            db["stats"][
                "last_cycle"
            ] = int(
                time.time()
            )

            await save_db()

            # -------------------------------------------------
            # IMPORTANT:
            #
            # We make a snapshot.
            #
            # New posts arriving during this cycle
            # will go into queue and will be processed
            # before the next cycle.
            # -------------------------------------------------

            cycle_snapshot = list(
                get_history(
                    source_id
                )
            )

            for history_item in cycle_snapshot:

                # ---------------------------------------------
                # If a new post arrives while cycling,
                # stop current cycle and process the new
                # post first.
                # ---------------------------------------------

                if not queue.empty():

                    log.info(
                        "New posts waiting. "
                        "Breaking cycle early. "
                        "source=%s",
                        source_id,
                    )

                    break

                if not db["settings"][
                    "enabled"
                ]:

                    break

                if not db["settings"][
                    "cycle_enabled"
                ]:

                    break

                sent = await send_history_item(
                    source_id,
                    history_item,
                )

                db["stats"][
                    "sent"
                ] += sent

                if sent:

                    db["stats"][
                        "last_sent"
                    ] = int(
                        time.time()
                    )

                await save_db()

                await asyncio.sleep(
                    float(
                        db["settings"][
                            "cycle_interval"
                        ]
                    )
                )

        except asyncio.CancelledError:

            raise

        except Exception as exc:

            db["stats"][
                "last_error"
            ] = str(exc)

            log.exception(
                "SOURCE WORKER ERROR "
                "source=%s. "
                "Worker will continue.",
                source_id,
            )

            await asyncio.sleep(
                2
            )


# ============================================================
# QUEUE NEW POST
# ============================================================

async def queue_new_post(
    source_id: str,
    message_ids: list[int],
    is_album: bool,
):

    source_id = str(
        source_id
    )

    if not route_destinations(
        source_id
    ):

        return

    await start_source_worker(
        source_id
    )

    queue = source_queues[
        source_id
    ]

    item = {
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

    # --------------------------------------------------------
    # IMPORTANT:
    #
    # put(), NOT put_nowait().
    #
    # This prevents QueueFull from silently dropping
    # a post.
    # --------------------------------------------------------

    await queue.put(
        item
    )

    db["stats"][
        "queued"
    ] += len(
        message_ids
    )

    log.info(
        "NEW POST QUEUED "
        "source=%s "
        "messages=%s",
        source_id,
        message_ids,
    )

    await save_db()


# ============================================================
# ALBUM FLUSH
# ============================================================

async def flush_album(
    source_id: str,
    media_group_id: str,
):

    key = (
        str(source_id),
        str(media_group_id),
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
            set(
                message_ids
            )
        )

        # ----------------------------------------------------
        # SAVE ALBUM PERMANENTLY
        # ----------------------------------------------------

        await add_history(
            source_id,
            message_ids,
            True,
        )

        db["stats"][
            "albums"
        ] += 1

        await queue_new_post(
            source_id,
            message_ids,
            True,
        )

    except asyncio.CancelledError:

        raise

    except Exception as exc:

        db["stats"][
            "last_error"
        ] = str(exc)

        log.exception(
            "Album flush error."
        )

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

    message_id = int(
        message.message_id
    )

    log.info(
        "NEW CHANNEL POST "
        "source=%s message=%s",
        source_id,
        message_id,
    )

    # --------------------------------------------------------
    # Only configured sources.
    # --------------------------------------------------------

    if source_id not in source_ids():

        return

    if not db["settings"][
        "enabled"
    ]:

        return

    if not route_destinations(
        source_id
    ):

        return

    # --------------------------------------------------------
    # DEDUPE
    # --------------------------------------------------------

    dedupe_key = (
        f"{source_id}:{message_id}"
    )

    async with seen_lock:

        if dedupe_key in db[
            "seen"
        ]:

            log.info(
                "Duplicate ignored %s",
                dedupe_key,
            )

            return

        db["seen"][
            dedupe_key
        ] = int(
            time.time()
        )

        # Keep seen DB small.
        if len(
            db["seen"]
        ) > 50000:

            oldest = sorted(
                db["seen"].items(),
                key=lambda x: x[1],
            )[:10000]

            for key, _ in oldest:

                db["seen"].pop(
                    key,
                    None
                )

    # --------------------------------------------------------
    # STATS
    # --------------------------------------------------------

    db["stats"][
        "received"
    ] += 1

    db["stats"][
        "last_received"
    ] = int(
        time.time()
    )

    # --------------------------------------------------------
    # ALBUM
    # --------------------------------------------------------

    media_group_id = (
        message.media_group_id
    )

    if media_group_id:

        album_key = (
            source_id,
            str(media_group_id),
        )

        album_buffer[
            album_key
        ].append(
            message_id
        )

        old_task = album_tasks.get(
            album_key
        )

        if old_task:

            old_task.cancel()

        album_tasks[
            album_key
        ] = asyncio.create_task(
            flush_album(
                source_id,
                str(media_group_id),
            )
        )

        await save_db()

        return

    # --------------------------------------------------------
    # NORMAL POST
    # --------------------------------------------------------

    await add_history(
        source_id,
        [message_id],
        False,
    )

    await queue_new_post(
        source_id,
        [message_id],
        False,
    )


# ============================================================
# START
# ============================================================

@router.message(
    CommandStart()
)
async def start_handler(
    message: Message,
    state: FSMContext,
):

    await state.clear()

    if not is_admin(
        message
    ):

        await message.answer(
            "Only admin can access this bot."
        )

        return

    await message.answer(
        home_text(),
        reply_markup=home_keyboard(),
    )


# ============================================================
# ADMIN
# ============================================================

@router.message(
    Command("admin")
)
async def admin_handler(
    message: Message,
    state: FSMContext,
):

    await state.clear()

    if not is_admin(
        message
    ):

        await message.answer(
            "Only admin can access this bot."
        )

        return

    await message.answer(
        home_text(),
        reply_markup=home_keyboard(),
    )


# ============================================================
# CANCEL
# ============================================================

@router.message(
    Command("cancel")
)
async def cancel_handler(
    message: Message,
    state: FSMContext,
):

    await state.clear()

    if is_admin(
        message
    ):

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

    if not is_admin(
        query
    ):

        await query.answer(
            "Access denied.",
            show_alert=True,
        )

        return

    data = query.data or ""

    await query.answer()

    if not query.message:
        return

    # ========================================================
    # HOME
    # ========================================================

    if data == "home":

        await state.clear()

        await query.message.edit_text(
            home_text(),
            reply_markup=home_keyboard(),
        )

        return

    # ========================================================
    # TOGGLE RUNNING
    # ========================================================

    if data == "toggle":

        db["settings"][
            "enabled"
        ] = not db["settings"][
            "enabled"
        ]

        await save_db()

        if db["settings"][
            "enabled"
        ]:

            await ensure_workers()

        else:

            log.info(
                "Bot stopped accepting new posts."
            )

        await query.message.edit_text(
            home_text(),
            reply_markup=home_keyboard(),
        )

        return

    # ========================================================
    # CYCLE TOGGLE
    # ========================================================

    if data == "cycle_toggle":

        db["settings"][
            "cycle_enabled"
        ] = not db["settings"][
            "cycle_enabled"
        ]

        await save_db()

        await query.message.edit_text(
            home_text(),
            reply_markup=home_keyboard(),
        )

        return

    # ========================================================
    # ROUTES
    # ========================================================

    if data == "sources":

        text = (
            "🔀 "
            "<b>Source → Destination Routes</b>\n\n"
        )

        if not db["sources"]:

            text += (
                "No source channels configured."
            )

        else:

            for index, source in enumerate(
                db["sources"],
                1,
            ):

                sid = str(
                    source["id"]
                )

                destinations = (
                    route_destinations(
                        sid
                    )
                )

                history = history_count(
                    sid
                )

                text += (
                    f"{index}. "
                    f"<b>{display_name(source)}</b>\n"
                    f"   ♻️ Saved posts: "
                    f"<b>{history}</b>\n"
                )

                if destinations:

                    for destination in destinations:

                        text += (
                            f"   └ "
                            f"{display_name(destination)}\n"
                        )

                else:

                    text += (
                        "   └ ⚠️ "
                        "No destination\n"
                    )

                text += "\n"

        await query.message.edit_text(
            text,
            reply_markup=sources_keyboard(),
        )

        return

    # ========================================================
    # OPEN ROUTE
    # ========================================================

    if data.startswith(
        "route:"
    ):

        try:

            index = int(
                data.split(
                    ":",
                    1,
                )[1]
            )

            source = db[
                "sources"
            ][index]

        except Exception:

            return

        source_id = str(
            source["id"]
        )

        selected = route_destination_ids(
            source_id
        )

        text = (
            "🔀 <b>Route Configuration</b>\n\n"

            f"Source:\n"
            f"<b>{display_name(source)}</b>\n"
            f"<code>{source_id}</code>\n\n"

            f"Saved posts: "
            f"<b>{history_count(source_id)}</b>\n"

            f"Selected destinations: "
            f"<b>{len(selected)}</b>\n\n"

            "Tap destinations to enable/disable."
        )

        await query.message.edit_text(
            text,
            reply_markup=route_keyboard(
                source_id
            ),
        )

        return

    # ========================================================
    # ROUTE DESTINATION
    # ========================================================

    if data.startswith(
        "route_dest:"
    ):

        try:

            _, source_id, index = (
                data.split(
                    ":",
                    2,
                )
            )

            destination = db[
                "destinations"
            ][int(index)]

        except Exception:

            return

        source_id = str(
            source_id
        )

        if source_id not in source_ids():
            return

        destination_id = str(
            destination["id"]
        )

        source = get_source(
            source_id
        )

        if source is None:
            return

        route = db[
            "routes"
        ].setdefault(
            source_id,
            {
                "source": source,
                "destinations": [],
            },
        )

        current_ids = {
            str(item["id"])
            for item in route[
                "destinations"
            ]
        }

        if destination_id in current_ids:

            route[
                "destinations"
            ] = [
                item
                for item in route[
                    "destinations"
                ]
                if str(
                    item["id"]
                ) != destination_id
            ]

            action = "removed"

        else:

            route[
                "destinations"
            ].append(
                destination
            )

            action = "added"

        await save_db()

        await ensure_workers()

        await query.message.edit_reply_markup(
            reply_markup=route_keyboard(
                source_id
            )
        )

        await query.answer(
            f"{display_name(destination)} {action}"
        )

        return

    # ========================================================
    # DESTINATIONS
    # ========================================================

    if data == "destinations":

        text = (
            "📤 <b>Destination Channels</b>\n\n"
        )

        if not db[
            "destinations"
        ]:

            text += (
                "No destinations configured."
            )

        else:

            for index, destination in enumerate(
                db["destinations"],
                1,
            ):

                did = str(
                    destination["id"]
                )

                used_by = sum(
                    1
                    for source in db["sources"]
                    if did
                    in route_destination_ids(
                        str(
                            source["id"]
                        )
                    )
                )

                text += (
                    f"{index}. "
                    f"<b>{display_name(destination)}</b>\n"
                    f"Used by: <b>{used_by}</b> route(s)\n"
                    f"<code>{did}</code>\n\n"
                )

        await query.message.edit_text(
            text,
            reply_markup=destinations_keyboard(),
        )

        return

    # ========================================================
    # ADD SOURCE
    # ========================================================

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

            "Bot must be administrator "
            "in source channel."
        )

        return

    # ========================================================
    # ADD DESTINATION
    # ========================================================

    if data == "add_destination":

        await state.set_state(
            AddDestination.value
        )

        await query.message.edit_text(
            "📤 <b>Add Destination Channel</b>\n\n"

            "Send @username or channel ID.\n\n"

            "Bot must be administrator "
            "with Post Messages permission."
        )

        return

    # ========================================================
    # ADD DESTINATION TO ROUTE
    # ========================================================

    if data.startswith(
        "new_dest_for:"
    ):

        source_id = data.split(
            ":",
            1,
        )[1]

        if source_id not in source_ids():
            return

        await state.update_data(
            route_source_id=source_id
        )

        await state.set_state(
            AddDestination.value
        )

        await query.message.edit_text(
            "📤 <b>Add Destination</b>\n\n"
            "Send @username or channel ID.\n\n"
            "It will be connected only "
            "to this source."
        )

        return

    # ========================================================
    # CLEAR HISTORY
    # ========================================================

    if data.startswith(
        "clear_history:"
    ):

        source_id = data.split(
            ":",
            1,
        )[1]

        if source_id not in source_ids():
            return

        db["history"][
            source_id
        ] = []

        await save_db()

        await query.message.edit_text(
            (
                "🗑 <b>History Cleared</b>\n\n"
                "Future NEW posts will again be "
                "saved and used for cycling."
            ),
            reply_markup=route_keyboard(
                source_id
            ),
        )

        return

    # ========================================================
    # REMOVE SOURCE
    # ========================================================

    if data.startswith(
        "remove_source:"
    ):

        source_id = data.split(
            ":",
            1,
        )[1]

        if source_id in source_ids():

            db["sources"] = [
                source
                for source in db["sources"]
                if str(
                    source["id"]
                ) != source_id
            ]

            db["routes"].pop(
                source_id,
                None,
            )

            db["history"].pop(
                source_id,
                None,
            )

            await stop_source_worker(
                source_id
            )

            prefix = (
                source_id
                + ":"
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
            "🔀 <b>Source → Destination Routes</b>",
            reply_markup=sources_keyboard(),
        )

        return

    # ========================================================
    # DELETE DESTINATION
    # ========================================================

    if data.startswith(
        "delete_destination:"
    ):

        try:

            index = int(
                data.split(
                    ":",
                    1,
                )[1]
            )

            destination = db[
                "destinations"
            ].pop(
                index
            )

        except Exception:

            return

        destination_id = str(
            destination["id"]
        )

        for route in db[
            "routes"
        ].values():

            route[
                "destinations"
            ] = [
                item
                for item in route.get(
                    "destinations",
                    [],
                )
                if str(
                    item["id"]
                ) != destination_id
            ]

        await save_db()

        await ensure_workers()

        await query.message.edit_text(
            "📤 <b>Destination Channels</b>",
            reply_markup=destinations_keyboard(),
        )

        return

    # ========================================================
    # SETTINGS
    # ========================================================

    if data == "settings":

        await query.message.edit_text(
            "⚙️ <b>Settings</b>",
            reply_markup=settings_keyboard(),
        )

        return

    # ========================================================
    # STATUS
    # ========================================================

    if data == "status":

        stats = db[
            "stats"
        ]

        lines = []

        for source in db[
            "sources"
        ]:

            sid = str(
                source["id"]
            )

            queue = source_queues.get(
                sid
            )

            qsize = (
                queue.qsize()
                if queue
                else 0
            )

            history = history_count(
                sid
            )

            lines.append(
                (
                    f"• <b>{display_name(source)}</b>\n"
                    f"  Queue: <b>{qsize}</b> | "
                    f"History: <b>{history}</b>"
                )
            )

        route_text = (
            "\n".join(lines)
            if lines
            else "-"
        )

        text = (
            "📊 <b>Bot Status</b>\n\n"

            f"Running: "
            f"<b>{db['settings']['enabled']}</b>\n"

            f"Cycle: "
            f"<b>{db['settings']['cycle_enabled']}</b>\n"

            f"Workers: "
            f"<b>{len(source_tasks)}</b>\n\n"

            f"📥 Received: "
            f"<b>{stats['received']}</b>\n"

            f"📤 Sent: "
            f"<b>{stats['sent']}</b>\n"

            f"❌ Failed: "
            f"<b>{stats['failed']}</b>\n"

            f"♻️ Cycles: "
            f"<b>{stats['cycles']}</b>\n\n"

            "<b>Source Queues</b>\n"
            f"{route_text}\n\n"

            "<b>Last Error</b>\n"
            f"<code>"
            f"{stats['last_error'] or '-'}"
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

    # ========================================================
    # COPY INTERVAL
    # ========================================================

    if data == "setting:interval":

        await state.set_state(
            SetInterval.value
        )

        await query.message.edit_text(
            "⏱ <b>Copy Interval</b>\n\n"
            "Enter seconds.\n\n"
            "Example: <code>1</code>"
        )

        return

    # ========================================================
    # CYCLE DELAY
    # ========================================================

    if data == "setting:cycle_delay":

        await state.set_state(
            SetCycleDelay.value
        )

        await query.message.edit_text(
            "🔄 <b>Cycle Delay</b>\n\n"
            "Time before starting from "
            "the first saved post again.\n\n"
            "Example: <code>5</code>"
        )

        return

    # ========================================================
    # ALBUM WAIT
    # ========================================================

    if data == "setting:album":

        await state.set_state(
            SetAlbumWait.value
        )

        await query.message.edit_text(
            "📦 <b>Album Wait</b>\n\n"
            "Enter seconds.\n\n"
            "Example: <code>1.5</code>"
        )

        return

    # ========================================================
    # RETRIES
    # ========================================================

    if data == "setting:retries":

        await state.set_state(
            SetRetries.value
        )

        await query.message.edit_text(
            "🔁 <b>Retries</b>\n\n"
            "Enter 0–20."
        )

        return

    # ========================================================
    # QUEUE
    # ========================================================

    if data == "setting:queue":

        await state.set_state(
            SetQueue.value
        )

        await query.message.edit_text(
            "📚 <b>Queue Limit</b>\n\n"
            "Enter 100–50000."
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
            "❌ Send channel username or ID."
        )

        return

    try:

        source = await resolve_chat(
            value
        )

        source_id = str(
            source["id"]
        )

        if source_id in source_ids():

            await message.answer(
                "⚠️ Source already exists."
            )

        else:

            db["sources"].append(
                source
            )

            db["routes"][
                source_id
            ] = {
                "source": source,
                "destinations": [],
            }

            db["history"].setdefault(
                source_id,
                [],
            )

            await save_db()

            await message.answer(
                (
                    "✅ <b>Source Added</b>\n\n"
                    f"<b>{display_name(source)}</b>\n"
                    f"<code>{source_id}</code>\n\n"
                    "📡 Bot will monitor NEW posts.\n"
                    "♻️ Received posts are saved for "
                    "automatic cycling.\n\n"
                    "Now select destinations."
                ),
                reply_markup=route_keyboard(
                    source_id
                ),
            )

    except Exception as exc:

        log.exception(
            "Add source failed."
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
            "❌ Send channel username or ID."
        )

        return

    state_data = await state.get_data()

    route_source_id = state_data.get(
        "route_source_id"
    )

    try:

        destination = await resolve_chat(
            value
        )

        destination_id = str(
            destination["id"]
        )

        await verify_destination(
            destination_id
        )

        if destination_id not in destination_ids():

            db[
                "destinations"
            ].append(
                destination
            )

        if (
            route_source_id
            and route_source_id in source_ids()
        ):

            source = get_source(
                route_source_id
            )

            route = db[
                "routes"
            ].setdefault(
                route_source_id,
                {
                    "source": source,
                    "destinations": [],
                },
            )

            existing = {
                str(item["id"])
                for item in route[
                    "destinations"
                ]
            }

            if destination_id not in existing:

                route[
                    "destinations"
                ].append(
                    destination
                )

            await save_db()

            await start_source_worker(
                route_source_id
            )

            await message.answer(
                (
                    "✅ <b>Destination Added</b>\n\n"
                    f"Source:\n"
                    f"<b>{display_name(source)}</b>\n\n"
                    f"Destination:\n"
                    f"<b>{display_name(destination)}</b>\n\n"
                    "🚀 Route active.\n"
                    "♻️ Cycling enabled."
                ),
                reply_markup=route_keyboard(
                    route_source_id
                ),
            )

        else:

            await save_db()

            await message.answer(
                (
                    "✅ <b>Destination Added</b>\n\n"
                    f"<b>{display_name(destination)}</b>\n\n"
                    "Open Routes and select "
                    "the source(s)."
                ),
                reply_markup=destinations_keyboard(),
            )

    except Exception as exc:

        log.exception(
            "Add destination failed."
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

        if not (
            0.1 <= value <= 3600
        ):

            raise ValueError

        db["settings"][
            "interval"
        ] = value

        await save_db()

        await message.answer(
            f"✅ Copy interval: <b>{value}s</b>",
            reply_markup=settings_keyboard(),
        )

    except ValueError:

        await message.answer(
            "❌ Enter 0.1–3600."
        )

        return

    finally:

        await state.clear()


# ============================================================
# SET CYCLE DELAY
# ============================================================

@router.message(
    SetCycleDelay.value
)
async def set_cycle_delay_handler(
    message: Message,
    state: FSMContext,
):

    try:

        value = float(
            (
                message.text or ""
            ).strip()
        )

        if not (
            0 <= value <= 86400
        ):

            raise ValueError

        db["settings"][
            "cycle_delay"
        ] = value

        await save_db()

        await message.answer(
            (
                f"✅ Cycle delay set to "
                f"<b>{value}s</b>."
            ),
            reply_markup=settings_keyboard(),
        )

    except ValueError:

        await message.answer(
            "❌ Enter 0–86400."
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

        if not (
            0.2 <= value <= 10
        ):

            raise ValueError

        db["settings"][
            "album_wait"
        ] = value

        await save_db()

        await message.answer(
            (
                f"✅ Album wait: "
                f"<b>{value}s</b>"
            ),
            reply_markup=settings_keyboard(),
        )

    except ValueError:

        await message.answer(
            "❌ Enter 0.2–10."
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

        if not (
            0 <= value <= 20
        ):

            raise ValueError

        db["settings"][
            "retries"
        ] = value

        await save_db()

        await message.answer(
            "✅ Retries updated.",
            reply_markup=settings_keyboard(),
        )

    except ValueError:

        await message.answer(
            "❌ Enter 0–20."
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

        if not (
            100 <= value <= 50000
        ):

            raise ValueError

        db["settings"][
            "max_queue"
        ] = value

        await save_db()

        await message.answer(
            (
                "✅ Queue limit updated.\n\n"
                "Existing queues are preserved."
            ),
            reply_markup=settings_keyboard(),
        )

    except ValueError:

        await message.answer(
            "❌ Enter 100–50000."
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

    stats = db["stats"]

    return web.json_response(
        {
            "ok": True,
            "mode": "webhook",
            "running": bool(
                db["settings"]["enabled"]
            ),
            "cycle": bool(
                db["settings"]["cycle_enabled"]
            ),
            "sources": len(
                db["sources"]
            ),
            "destinations": len(
                db["destinations"]
            ),
            "routes": route_count(),
            "workers": len(
                source_tasks
            ),
            "history": sum(
                history_count(
                    str(source["id"])
                )
                for source in db["sources"]
            ),
            "received": stats[
                "received"
            ],
            "sent": stats[
                "sent"
            ],
            "failed": stats[
                "failed"
            ],
            "cycles": stats[
                "cycles"
            ],
            "time": int(
                time.time()
            ),
        }
    )


# ============================================================
# WEBHOOK
# ============================================================

async def telegram_webhook(
    request: web.Request,
):

    try:

        incoming_secret = (
            request.headers.get(
                "X-Telegram-Bot-Api-Secret-Token",
                "",
            )
        )

        if not secrets.compare_digest(
            incoming_secret,
            WEBHOOK_SECRET,
        ):

            log.warning(
                "Invalid webhook secret."
            )

            return web.Response(
                status=403,
                text="Forbidden",
            )

        raw = await request.read()

        if not raw:

            return web.Response(
                status=400,
                text="Empty update",
            )

        update = (
            Update.model_validate_json(
                raw
            )
        )

        if (
            dp is None
            or bot is None
        ):

            return web.Response(
                status=503,
                text="Bot not ready",
            )

        await dp.feed_update(
            bot,
            update,
        )

        return web.Response(
            text="OK"
        )

    except Exception:

        log.exception(
            "Webhook update error."
        )

        return web.Response(
            status=500,
            text="ERROR",
        )


# ============================================================
# WEB SERVER
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
        "HTTP server started port=%s",
        PORT,
    )


# ============================================================
# WEBHOOK SETUP
# ============================================================

async def setup_webhook():

    if bot is None:
        return

    webhook_url = (
        RENDER_URL
        + WEBHOOK_PATH
    )

    log.info(
        "Setting webhook."
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
            "Webhook error: %s",
            info.last_error_message,
        )

    log.info(
        "Pending Telegram updates: %s",
        info.pending_update_count,
    )


# ============================================================
# SHUTDOWN
# ============================================================

async def shutdown():

    # --------------------------------------------------------
    # Album timers
    # --------------------------------------------------------

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

    # --------------------------------------------------------
    # Workers
    # --------------------------------------------------------

    for source_id in list(
        source_tasks
    ):

        await stop_source_worker(
            source_id
        )

    # --------------------------------------------------------
    # Web server
    # --------------------------------------------------------

    if server_runner:

        await server_runner.cleanup()


# ============================================================
# MAIN
# ============================================================

async def main():

    global bot
    global dp

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

    me = await bot.get_me()

    log.info(
        "Bot connected @%s | ID=%s",
        me.username,
        me.id,
    )

    await start_web_server()

    await setup_webhook()

    await ensure_workers()

    log.info(
        "=========================================="
    )

    log.info(
        "BOT RUNNING IN WEBHOOK MODE"
    )

    log.info(
        "NEW POSTS ARE SAVED"
    )

    log.info(
        "AUTOMATIC CYCLING ENABLED"
    )

    log.info(
        "FIRST POST -> LAST POST -> FIRST POST"
    )

    log.info(
        "INDEPENDENT SOURCE WORKERS"
    )

    log.info(
        "ROUTE BASED DESTINATIONS"
    )

    log.info(
        "NO API ID / API HASH"
    )

    log.info(
        "=========================================="
    )

    try:

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


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":

    try:

        asyncio.run(
            main()
        )

    except KeyboardInterrupt:

        pass
