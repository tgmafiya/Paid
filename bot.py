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


ADMIN_ID = int(
    ADMIN_ID_RAW
)


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
    "route-channel-copy"
)


# ============================================================
# DEFAULT SETTINGS
# ============================================================

DEFAULT_SETTINGS = {
    # Delay between destination copies.
    "interval": 1.0,

    # Retry count for temporary/network errors.
    "retries": 5,

    # Time to wait for all album messages.
    "album_wait": 1.5,

    # Maximum pending posts per source route.
    "max_queue": 10000,

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

        # IMPORTANT:
        #
        # source_id:
        # {
        #   "source": {...},
        #   "destinations": [...]
        # }
        #
        "routes": {},

        "stats": {
            "received": 0,
            "queued": 0,
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

    else:

        # Old version had all sources and all destinations
        # globally connected.
        #
        # Convert that into:
        #
        # Every old source -> every old destination.
        #

        if (
            fresh["sources"]
            and fresh["destinations"]
        ):

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
# GLOBAL RUNTIME
# ============================================================

router = Router()

bot: Bot | None = None

dp: Dispatcher | None = None

server_runner: (
    web.AppRunner | None
) = None


# ============================================================
# ROUTE QUEUES
# ============================================================

#
# ONE QUEUE PER SOURCE
#
# Source 1 -> Queue 1 -> Worker 1
# Source 2 -> Queue 2 -> Worker 2
# Source 3 -> Queue 3 -> Worker 3
#
# This is the important fix.
#

route_queues: dict[
    str,
    asyncio.Queue,
] = {}


route_tasks: dict[
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


class SetRetries(StatesGroup):

    value = State()


class SetAlbumWait(StatesGroup):

    value = State()


class SetQueue(StatesGroup):

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
# DATABASE HELPERS
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

    source_id = str(
        source_id
    )

    for source in db[
        "sources"
    ]:

        if str(
            source["id"]
        ) == source_id:

            return source

    return None


def get_destination(
    destination_id: str,
) -> dict[str, Any] | None:

    destination_id = str(
        destination_id
    )

    for destination in db[
        "destinations"
    ]:

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
            item.get(
                "id",
                "Unknown",
            )
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

    count = 0

    for source in db[
        "sources"
    ]:

        sid = str(
            source["id"]
        )

        if route_destinations(
            sid
        ):

            count += 1

    return count


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

    status = (
        "🟢 RUNNING"
        if settings["enabled"]
        else "🔴 STOPPED"
    )

    return (
        "🤖 <b>Route Channel Copy Bot</b>\n\n"

        f"Status: <b>{status}</b>\n"
        f"Sources: <b>{len(db['sources'])}</b>\n"
        f"Destinations: "
        f"<b>{len(db['destinations'])}</b>\n"
        f"Routes: <b>{route_count()}</b>\n\n"

        f"⏱ Interval: "
        f"<b>{settings['interval']}s</b>\n"

        f"📦 Album wait: "
        f"<b>{settings['album_wait']}s</b>\n"

        f"🔁 Retries: "
        f"<b>{settings['retries']}</b>\n"

        f"📚 Queue: "
        f"<b>{settings['max_queue']}</b>\n\n"

        f"📥 Received: "
        f"<b>{stats['received']}</b>\n"

        f"📦 Queued: "
        f"<b>{stats['queued']}</b>\n"

        f"📤 Sent: "
        f"<b>{stats['sent']}</b>\n"

        f"❌ Failed: "
        f"<b>{stats['failed']}</b>"
    )


# ============================================================
# KEYBOARDS
# ============================================================

def home_keyboard():

    enabled = db[
        "settings"
    ]["enabled"]

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

        rows.append(
            [
                InlineKeyboardButton(
                    text=(
                        f"🔀 "
                        f"{display_name(source)} "
                        f"[{count}]"
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
            for source in db[
                "sources"
            ]
            if did
            in route_destination_ids(
                str(
                    source["id"]
                )
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
                        f"⏱ Interval: "
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
        "id": str(
            chat.id
        ),

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
# ROUTE WORKERS
# ============================================================

async def start_route_worker(
    source_id: str,
):

    source_id = str(
        source_id
    )

    if source_id not in route_queues:

        route_queues[
            source_id
        ] = asyncio.Queue(
            maxsize=int(
                db["settings"][
                    "max_queue"
                ]
            )
        )

    existing_task = route_tasks.get(
        source_id
    )

    if (
        existing_task
        and not existing_task.done()
    ):

        return

    route_tasks[
        source_id
    ] = asyncio.create_task(
        route_worker(
            source_id
        )
    )


async def stop_route_worker(
    source_id: str,
):

    source_id = str(
        source_id
    )

    task = route_tasks.pop(
        source_id,
        None,
    )

    if task:

        task.cancel()

        try:

            await task

        except asyncio.CancelledError:

            pass

    route_queues.pop(
        source_id,
        None,
    )


async def ensure_route_workers():

    if not db["settings"]["enabled"]:

        return

    wanted = set()

    for source in db[
        "sources"
    ]:

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

        await start_route_worker(
            sid
        )

    for sid in list(
        route_tasks
    ):

        if sid not in wanted:

            await stop_route_worker(
                sid
            )


# ============================================================
# COPY SINGLE MESSAGE
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
                "COPY "
                "source=%s "
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

            log.info(
                "COPY SUCCESS "
                "source=%s "
                "destination=%s "
                "message=%s",
                source_id,
                destination_id,
                message_id,
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
                "FloodWait %ss "
                "source=%s "
                "destination=%s",
                wait,
                source_id,
                destination_id,
            )

            await asyncio.sleep(
                wait
            )

        except TelegramForbiddenError as exc:

            db["stats"][
                "last_error"
            ] = str(exc)

            log.error(
                "Forbidden "
                "source=%s "
                "destination=%s: %s",
                source_id,
                destination_id,
                exc,
            )

            return False

        except TelegramBadRequest as exc:

            db["stats"][
                "last_error"
            ] = str(exc)

            log.error(
                "Bad request "
                "source=%s "
                "destination=%s "
                "message=%s: %s",
                source_id,
                destination_id,
                message_id,
                exc,
            )

            return False

        except TelegramNetworkError as exc:

            if attempt >= retries:

                db["stats"][
                    "last_error"
                ] = str(exc)

                log.error(
                    "Network error after retries: %s",
                    exc,
                )

                return False

            wait = min(
                2 ** attempt,
                30,
            )

            await asyncio.sleep(
                wait
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
) -> int:

    sent = 0

    for message_id in message_ids:

        success = await copy_single(
            source_id,
            destination_id,
            message_id,
        )

        if success:

            sent += 1

        await asyncio.sleep(
            float(
                db["settings"][
                    "interval"
                ]
            )
        )

    return sent


# ============================================================
# ROUTE WORKER
# ============================================================

async def route_worker(
    source_id: str,
):

    source_id = str(
        source_id
    )

    queue = route_queues[
        source_id
    ]

    log.info(
        "ROUTE WORKER STARTED "
        "source=%s",
        source_id,
    )

    while True:

        item = await queue.get()

        try:

            if not db["settings"][
                "enabled"
            ]:

                continue

            destinations = list(
                route_destinations(
                    source_id
                )
            )

            if not destinations:

                log.warning(
                    "No destinations "
                    "for source=%s",
                    source_id,
                )

                continue

            message_ids = item[
                "message_ids"
            ]

            is_album = item[
                "is_album"
            ]

            #
            # IMPORTANT:
            #
            # Every destination is attempted
            # independently.
            #
            # If A fails, B/C/D still get
            # their copy attempt.
            #

            total_sent = 0

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
                                len(
                                    message_ids
                                )
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

                    #
                    # VERY IMPORTANT:
                    #
                    # A single destination
                    # must NEVER kill the
                    # route worker.
                    #

                    db["stats"][
                        "last_error"
                    ] = str(exc)

                    db["stats"][
                        "failed"
                    ] += len(
                        message_ids
                    )

                    log.exception(
                        "Destination failed "
                        "but route continues. "
                        "source=%s "
                        "destination=%s",
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

            db["stats"][
                "sent"
            ] += total_sent

            if total_sent:

                db["stats"][
                    "last_sent"
                ] = int(
                    time.time()
                )

            await save_db()

        except asyncio.CancelledError:

            raise

        except Exception as exc:

            #
            # Worker itself crashed:
            # log it, sleep, then CONTINUE.
            #

            db["stats"][
                "last_error"
            ] = str(exc)

            log.exception(
                "ROUTE WORKER ERROR "
                "source=%s. "
                "Worker continues.",
                source_id,
            )

            await asyncio.sleep(
                2
            )

        finally:

            queue.task_done()


# ============================================================
# QUEUE NEW POST
# ============================================================

async def queue_route(
    source_id: str,
    message_ids: list[int],
    is_album: bool,
):

    source_id = str(
        source_id
    )

    destinations = route_destinations(
        source_id
    )

    if not destinations:

        log.warning(
            "No route destinations "
            "source=%s",
            source_id,
        )

        return

    await start_route_worker(
        source_id
    )

    queue = route_queues[
        source_id
    ]

    item = {
        "source_id": source_id,

        "message_ids": [
            int(message_id)
            for message_id in message_ids
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
            "QUEUED "
            "source=%s "
            "messages=%s "
            "destinations=%s",
            source_id,
            message_ids,
            len(
                destinations
            ),
        )

        await save_db()

    except asyncio.QueueFull:

        db["stats"][
            "failed"
        ] += len(
            message_ids
        )

        db["stats"][
            "last_error"
        ] = (
            "Route queue full "
            + source_id
        )

        log.error(
            "QUEUE FULL source=%s",
            source_id,
        )

        await save_db()


# ============================================================
# ALBUM HANDLING
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

        db["stats"][
            "albums"
        ] += 1

        await queue_route(
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
# NEW CHANNEL POST
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
        "source=%s "
        "message=%s",
        source_id,
        message_id,
    )

    #
    # Only configured sources.
    #

    if source_id not in source_ids():

        log.info(
            "Ignoring unconfigured "
            "source=%s",
            source_id,
        )

        return

    #
    # Bot stopped.
    #

    if not db["settings"][
        "enabled"
    ]:

        return

    #
    # No route.
    #

    if not route_destinations(
        source_id
    ):

        log.warning(
            "Source has no destinations "
            "source=%s",
            source_id,
        )

        return

    #
    # DEDUPE
    #

    dedupe_key = (
        f"{source_id}:"
        f"{message_id}"
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

        #
        # Keep JSON small.
        #

        if len(
            db["seen"]
        ) > 20000:

            oldest = sorted(
                db["seen"].items(),
                key=lambda item: item[1],
            )[:5000]

            for key, _ in oldest:

                db["seen"].pop(
                    key,
                    None
                )

    #
    # STATS
    #

    db["stats"][
        "received"
    ] += 1

    db["stats"][
        "last_received"
    ] = int(
        time.time()
    )

    await save_db()

    #
    # ALBUM
    #

    media_group_id = (
        message.media_group_id
    )

    if media_group_id:

        album_key = (
            source_id,
            str(
                media_group_id
            ),
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
                str(
                    media_group_id
                ),
            )
        )

    #
    # NORMAL POST
    #

    else:

        await queue_route(
            source_id,
            [message_id],
            False,
        )


# ============================================================
# /START
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
# /ADMIN
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
# /CANCEL
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
# CALLBACK HANDLER
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

    data = (
        query.data
        or ""
    )

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
    # TOGGLE
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

            await ensure_route_workers()

        else:

            log.info(
                "Bot stopped."
            )

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

                text += (
                    f"{index}. "
                    f"<b>{display_name(source)}</b>\n"
                )

                if destinations:

                    for destination in destinations:

                        text += (
                            f"   └ "
                            f"{display_name(destination)}\n"
                        )

                else:

                    text += (
                        "   └ "
                        "⚠️ No destination\n"
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

        selected = (
            route_destination_ids(
                source_id
            )
        )

        text = (
            "🔀 <b>Route Configuration</b>\n\n"

            f"Source:\n"
            f"<b>{display_name(source)}</b>\n"
            f"<code>{source_id}</code>\n\n"

            f"Selected destinations: "
            f"<b>{len(selected)}</b>\n\n"

            "Tap a destination to "
            "enable/disable this route."
        )

        await query.message.edit_text(
            text,
            reply_markup=route_keyboard(
                source_id
            ),
        )

        return

    # ========================================================
    # ROUTE DESTINATION TOGGLE
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

        await ensure_route_workers()

        await query.message.edit_reply_markup(
            reply_markup=route_keyboard(
                source_id
            )
        )

        await query.answer(
            (
                f"{display_name(destination)} "
                f"{action}"
            )
        )

        return

    # ========================================================
    # ALL DESTINATIONS
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
                    for source in db[
                        "sources"
                    ]
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

            "⚠️ Bot must be administrator "
            "in the source channel.\n\n"

            "After adding, select the "
            "destination channels for this source."
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

            "Send channel username or ID:\n\n"

            "<code>@channelusername</code>\n"
            "or\n"
            "<code>-1001234567890</code>\n\n"

            "⚠️ Bot must be administrator "
            "with Post Messages permission."
        )

        return

    # ========================================================
    # ADD DESTINATION DIRECTLY TO ROUTE
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

            "It will automatically be "
            "connected to this source."
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
                for source in db[
                    "sources"
                ]
                if str(
                    source["id"]
                ) != source_id
            ]

            db["routes"].pop(
                source_id,
                None,
            )

            await stop_route_worker(
                source_id
            )

            prefix = (
                source_id
                + ":"
            )

            db["seen"] = {
                key: value
                for key, value
                in db[
                    "seen"
                ].items()
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

        #
        # Remove this destination
        # from EVERY route.
        #

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

        await ensure_route_workers()

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

        queue_lines = []

        for source in db[
            "sources"
        ]:

            sid = str(
                source["id"]
            )

            queue = route_queues.get(
                sid
            )

            if queue:

                queue_lines.append(
                    f"• "
                    f"{display_name(source)}: "
                    f"<b>{queue.qsize()}</b>"
                )

        queue_text = (
            "\n".join(
                queue_lines
            )
            if queue_lines
            else "-"
        )

        text = (
            "📊 <b>Bot Status</b>\n\n"

            f"Running: "
            f"<b>{db['settings']['enabled']}</b>\n"

            f"Route workers: "
            f"<b>{len(route_tasks)}</b>\n\n"

            f"📥 Received: "
            f"<b>{stats['received']}</b>\n"

            f"📦 Queued: "
            f"<b>{stats['queued']}</b>\n"

            f"📤 Sent: "
            f"<b>{stats['sent']}</b>\n"

            f"❌ Failed: "
            f"<b>{stats['failed']}</b>\n"

            f"🖼 Albums: "
            f"<b>{stats['albums']}</b>\n\n"

            "<b>Route Queues</b>\n"
            f"{queue_text}\n\n"

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
    # SET INTERVAL
    # ========================================================

    if data == "setting:interval":

        await state.set_state(
            SetInterval.value
        )

        await query.message.edit_text(
            "⏱ <b>Set Copy Interval</b>\n\n"

            "Enter seconds.\n\n"

            "Example:\n"
            "<code>1</code>\n\n"

            "Recommended: 1–3 seconds."
        )

        return

    # ========================================================
    # SET ALBUM WAIT
    # ========================================================

    if data == "setting:album":

        await state.set_state(
            SetAlbumWait.value
        )

        await query.message.edit_text(
            "📦 <b>Album Wait</b>\n\n"

            "Enter seconds.\n\n"

            "Example:\n"
            "<code>1.5</code>"
        )

        return

    # ========================================================
    # SET RETRIES
    # ========================================================

    if data == "setting:retries":

        await state.set_state(
            SetRetries.value
        )

        await query.message.edit_text(
            "🔁 <b>Retries</b>\n\n"

            "Enter 0–20.\n\n"

            "Example:\n"
            "<code>5</code>"
        )

        return

    # ========================================================
    # SET QUEUE
    # ========================================================

    if data == "setting:queue":

        await state.set_state(
            SetQueue.value
        )

        await query.message.edit_text(
            "📚 <b>Queue Limit</b>\n\n"

            "Enter 100–50000.\n\n"

            "Example:\n"
            "<code>10000</code>"
        )

        return


# ============================================================
# ADD SOURCE MESSAGE
# ============================================================

@router.message(
    AddSource.value
)
async def add_source_handler(
    message: Message,
    state: FSMContext,
):

    if not is_admin(
        message
    ):

        return

    value = (
        message.text
        or ""
    ).strip()

    if not value:

        await message.answer(
            "❌ Send a channel username or ID."
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
                "⚠️ Source already exists.",
            )

        else:

            db[
                "sources"
            ].append(
                source
            )

            db[
                "routes"
            ][source_id] = {
                "source": source,
                "destinations": [],
            }

            await save_db()

            await message.answer(
                (
                    "✅ <b>Source Added</b>\n\n"

                    f"Source:\n"
                    f"<b>{display_name(source)}</b>\n"
                    f"<code>{source_id}</code>\n\n"

                    "📡 Only NEW posts will be monitored.\n\n"

                    "Now select destination channels."
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
# ADD DESTINATION MESSAGE
# ============================================================

@router.message(
    AddDestination.value
)
async def add_destination_handler(
    message: Message,
    state: FSMContext,
):

    if not is_admin(
        message
    ):

        return

    value = (
        message.text
        or ""
    ).strip()

    if not value:

        await message.answer(
            "❌ Send a channel username or ID."
        )

        return

    state_data = await state.get_data()

    route_source_id = (
        state_data.get(
            "route_source_id"
        )
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

        #
        # Add to global destination
        # registry only if new.
        #

        if destination_id not in destination_ids():

            db[
                "destinations"
            ].append(
                destination
            )

        #
        # If opened from a route,
        # connect destination directly.
        #

        if (
            route_source_id
            and route_source_id
            in source_ids()
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

            await start_route_worker(
                route_source_id
            )

            await message.answer(
                (
                    "✅ <b>Destination Added</b>\n\n"

                    f"Source:\n"
                    f"<b>{display_name(source)}</b>\n\n"

                    f"Destination:\n"
                    f"<b>{display_name(destination)}</b>\n\n"

                    "🚀 Route is active."
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
                    "Go to Routes and select "
                    "which sources should send here."
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
                message.text
                or ""
            ).strip()
        )

        if not (
            0.1
            <= value
            <= 3600
        ):

            raise ValueError

        db[
            "settings"
        ][
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
async def set_album_wait_handler(
    message: Message,
    state: FSMContext,
):

    try:

        value = float(
            (
                message.text
                or ""
            ).strip()
        )

        if not (
            0.2
            <= value
            <= 10
        ):

            raise ValueError

        db[
            "settings"
        ][
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
                message.text
                or ""
            ).strip()
        )

        if not (
            0
            <= value
            <= 20
        ):

            raise ValueError

        db[
            "settings"
        ][
            "retries"
        ] = value

        await save_db()

        await message.answer(
            "✅ Retries updated.",
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
                message.text
                or ""
            ).strip()
        )

        if not (
            100
            <= value
            <= 50000
        ):

            raise ValueError

        db[
            "settings"
        ][
            "max_queue"
        ] = value

        await save_db()

        #
        # Existing queues are intentionally
        # not destroyed because that would
        # delete pending posts.
        #
        # New queues will use the new size.
        #

        await message.answer(
            (
                "✅ Queue limit updated.\n\n"
                "Existing pending queues were preserved."
            ),
            reply_markup=settings_keyboard(),
        )

    except ValueError:

        await message.answer(
            "❌ Enter an integer from 100 to 50000."
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
                db["settings"][
                    "enabled"
                ]
            ),

            "sources": len(
                db["sources"]
            ),

            "destinations": len(
                db["destinations"]
            ),

            "routes": route_count(),

            "workers": len(
                route_tasks
            ),

            "received": db[
                "stats"
            ][
                "received"
            ],

            "sent": db[
                "stats"
            ][
                "sent"
            ],

            "failed": db[
                "stats"
            ][
                "failed"
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

        #
        # Verify Telegram secret.
        #

        incoming_secret = (
            request.headers.get(
                "X-Telegram-Bot-Api-Secret-Token",
                "",
            )
        )

        if incoming_secret != WEBHOOK_SECRET:

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

    except Exception as exc:

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
        "HTTP server started "
        "port=%s",
        PORT,
    )


# ============================================================
# SET TELEGRAM WEBHOOK
# ============================================================

async def setup_webhook():

    if bot is None:

        return

    webhook_url = (
        RENDER_URL
        + WEBHOOK_PATH
    )

    log.info(
        "Setting webhook: %s",
        webhook_url,
    )

    await bot.set_webhook(
        url=webhook_url,

        secret_token=(
            WEBHOOK_SECRET
        ),

        allowed_updates=[
            "message",
            "callback_query",
            "channel_post",
        ],

        drop_pending_updates=False,
    )

    info = (
        await bot.get_webhook_info()
    )

    log.info(
        "Webhook active: %s",
        info.url,
    )

    if info.last_error_message:

        log.error(
            "Telegram webhook error: %s",
            info.last_error_message,
        )

        log.error(
            "Webhook pending updates: %s",
            info.pending_update_count,
        )


# ============================================================
# SHUTDOWN
# ============================================================

async def shutdown():

    #
    # Cancel album timers.
    #

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

    #
    # Stop route workers.
    #

    for source_id in list(
        route_tasks
    ):

        await stop_route_worker(
            source_id
        )

    #
    # Web server.
    #

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
        "Bot connected "
        "@%s | ID=%s",
        me.username,
        me.id,
    )

    await start_web_server()

    await setup_webhook()

    await ensure_route_workers()

    log.info(
        "=========================================="
    )

    log.info(
        "BOT RUNNING IN WEBHOOK MODE"
    )

    log.info(
        "ROUTE MODE ENABLED"
    )

    log.info(
        "NEW POSTS ONLY"
    )

    log.info(
        "NO OLD HISTORY SCAN"
    )

    log.info(
        "NO API ID / API HASH"
    )

    log.info(
        "INDEPENDENT SOURCE WORKERS"
    )

    log.info(
        "=========================================="
    )

    try:

        #
        # Keep Render process alive forever.
        #

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
