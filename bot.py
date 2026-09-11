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
    ""
).strip().rstrip("/")

WEBHOOK_SECRET = os.getenv(
    "WEBHOOK_SECRET",
    ""
).strip()


if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is missing.")

if not ADMIN_ID_RAW.lstrip("-").isdigit():
    raise RuntimeError("ADMIN_ID must be numeric.")

if not WEBHOOK_SECRET:
    raise RuntimeError(
        "WEBHOOK_SECRET is missing."
    )

if WEBHOOK_SECRET == "change-this-secret":
    raise RuntimeError(
        "WEBHOOK_SECRET must be changed."
    )

if not RENDER_URL:
    raise RuntimeError(
        "RENDER_URL is missing."
    )


ADMIN_ID = int(ADMIN_ID_RAW)

WEBHOOK_PATH = (
    f"/telegram/webhook/{WEBHOOK_SECRET}"
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
    "interval": 1.0,
    "retries": 8,
    "album_wait": 1.5,
    "max_queue": 10000,
    "enabled": True,
}


# ============================================================
# DATABASE
# ============================================================

def default_db() -> dict[str, Any]:

    return {
        "settings": (
            DEFAULT_SETTINGS.copy()
        ),

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
        return default_db()

    try:

        data = json.loads(
            DATA_FILE.read_text(
                encoding="utf-8"
            )
        )

    except Exception:

        log.exception(
            "Could not read data file."
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

        fresh["sources"] = (
            data["sources"]
        )

    # --------------------------------------------------------
    # DESTINATIONS
    # --------------------------------------------------------

    if isinstance(
        data.get("destinations"),
        list,
    ):

        fresh["destinations"] = (
            data["destinations"]
        )

    # --------------------------------------------------------
    # ROUTES
    # --------------------------------------------------------

    if isinstance(
        data.get("routes"),
        dict,
    ):

        fresh["routes"] = (
            data["routes"]
        )

    else:

        # Old structure migration.
        # Every old source gets every old destination.

        for source in fresh["sources"]:

            if (
                isinstance(source, dict)
                and source.get("id") is not None
            ):

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
    # NORMALIZE ROUTES
    # --------------------------------------------------------

    route_sources = {}
    route_destinations = {}

    for sid, route in list(
        fresh["routes"].items()
    ):

        if not isinstance(
            route,
            dict,
        ):

            fresh["routes"].pop(
                sid,
                None,
            )

            continue

        source = route.get(
            "source"
        )

        if (
            isinstance(source, dict)
            and source.get("id") is not None
        ):

            route_sources[
                str(sid)
            ] = source

        destinations = route.get(
            "destinations",
            [],
        )

        if not isinstance(
            destinations,
            list,
        ):

            route["destinations"] = []

            continue

        clean = []

        for destination in destinations:

            if (
                isinstance(
                    destination,
                    dict,
                )
                and destination.get("id")
                is not None
            ):

                did = str(
                    destination["id"]
                )

                route_destinations[
                    did
                ] = destination

                clean.append(
                    destination
                )

        route["destinations"] = clean

    # Existing registries.

    for source in fresh["sources"]:

        if (
            isinstance(source, dict)
            and source.get("id") is not None
        ):

            route_sources[
                str(source["id"])
            ] = source

    for destination in fresh[
        "destinations"
    ]:

        if (
            isinstance(
                destination,
                dict,
            )
            and destination.get("id")
            is not None
        ):

            route_destinations[
                str(destination["id"])
            ] = destination

    fresh["sources"] = list(
        route_sources.values()
    )

    fresh["destinations"] = list(
        route_destinations.values()
    )

    # Make sure every source has route.

    for source in fresh["sources"]:

        sid = str(
            source["id"]
        )

        fresh["routes"].setdefault(
            sid,
            {
                "source": source,
                "destinations": [],
            },
        )

        fresh["routes"][sid][
            "source"
        ] = source

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

        fresh["seen"] = (
            data["seen"]
        )

    return fresh


db = load_db()

db_lock = asyncio.Lock()

seen_lock = asyncio.Lock()


# ============================================================
# GLOBAL
# ============================================================

router = Router()

bot: Bot | None = None

dp: Dispatcher | None = None

server_runner: (
    web.AppRunner | None
) = None


# ============================================================
# ROUTE WORKERS
# ============================================================

route_queues: dict[
    str,
    asyncio.Queue,
] = {}

route_tasks: dict[
    str,
    asyncio.Task,
] = {}


# ============================================================
# ALBUMS
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
# VIDEO MESSAGE CACHE
# ============================================================

# Telegram copy_message is preferred.
#
# If Telegram rejects copying a video, we keep the incoming
# Message object temporarily and can send its file_id directly.

message_cache: dict[
    tuple[str, int],
    Message,
] = {}

message_cache_order: list[
    tuple[str, int]
] = []

MESSAGE_CACHE_LIMIT = 5000


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
# SAVE DATABASE
# ============================================================

async def save_db() -> None:

    async with db_lock:

        DATA_FILE.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        temp_file = (
            DATA_FILE.with_suffix(
                ".tmp"
            )
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
        if (
            isinstance(item, dict)
            and item.get("id") is not None
        )
    }


def destination_ids() -> set[str]:

    return {
        str(item["id"])
        for item in db["destinations"]
        if (
            isinstance(item, dict)
            and item.get("id") is not None
        )
    }


def get_source(
    source_id: str,
) -> dict[str, Any] | None:

    for item in db["sources"]:

        if (
            isinstance(item, dict)
            and str(item.get("id"))
            == str(source_id)
        ):

            return item

    return None


def get_destination(
    destination_id: str,
) -> dict[str, Any] | None:

    for item in db["destinations"]:

        if (
            isinstance(item, dict)
            and str(item.get("id"))
            == str(destination_id)
        ):

            return item

    return None


def display_name(
    item: dict[str, Any] | None,
) -> str:

    if not item:
        return "Unknown"

    return str(
        item.get("title")
        or item.get("username")
        or item.get(
            "id",
            "Unknown",
        )
    )


def route_destinations(
    source_id: str,
) -> list[dict[str, Any]]:

    route = db["routes"].get(
        str(source_id),
        {},
    )

    if not isinstance(
        route,
        dict,
    ):

        return []

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
        if (
            isinstance(item, dict)
            and item.get("id") is not None
        )
    }


def route_count() -> int:

    count = 0

    for source in db["sources"]:

        sid = str(
            source["id"]
        )

        if route_destinations(sid):
            count += 1

    return count


def route_key(
    source_id: str,
    destination_id: str,
) -> str:

    return (
        f"{source_id}|{destination_id}"
    )


def parse_route_key(
    key: str,
) -> tuple[str, str]:

    return key.split(
        "|",
        1,
    )


# ============================================================
# HOME
# ============================================================

def home_text() -> str:

    settings = db["settings"]

    stats = db["stats"]

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

        f"📚 Queue limit: "
        f"<b>{settings['max_queue']}</b>\n\n"

        f"📥 Received: "
        f"<b>{stats['received']}</b>\n"

        f"📦 Enqueued: "
        f"<b>{stats['enqueued']}</b>\n"

        f"📤 Sent: "
        f"<b>{stats['sent']}</b>\n"

        f"❌ Failed: "
        f"<b>{stats['failed']}</b>"
    )


# ============================================================
# KEYBOARDS
# ============================================================

def home_keyboard():

    enabled = bool(
        db["settings"]["enabled"]
    )

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
            route_destinations(sid)
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
                    f"new_dest_for:{source_id}"
                ),
            )
        ]
    )

    rows.append(
        [
            InlineKeyboardButton(
                text="❌ Remove Source",
                callback_data=(
                    f"remove_source:{source_id}"
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

        used_by = 0

        for source in db["sources"]:

            sid = str(
                source["id"]
            )

            if did in route_destination_ids(
                sid
            ):

                used_by += 1

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

    settings = db["settings"]

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
# CHAT ID PARSER
# ============================================================

def parse_chat_ref(
    value: str,
) -> int | str:

    value = value.strip()

    # Telegram public URL
    if value.startswith(
        "https://t.me/"
    ):

        value = value.split(
            "https://t.me/",
            1,
        )[1].strip("/")

        if (
            value.startswith("+")
            or value.startswith("c/")
        ):

            raise ValueError(
                "Private invite links are not supported. "
                "Use numeric chat ID."
            )

        value = value.split(
            "/",
            1,
        )[0]

    # @username
    if value.startswith("@"):
        return value

    # NUMERIC ID
    #
    # IMPORTANT:
    # Convert numeric IDs to INT.
    #
    # This fixes:
    # "'str' object has no attribute 'value'"
    #
    if value.lstrip("-").isdigit():

        return int(value)

    # Bare username
    return "@" + value


# ============================================================
# RESOLVE SOURCE / DESTINATION
# ============================================================

async def resolve_chat(
    value: str,
    allow_group: bool = False,
) -> dict[str, Any]:

    if bot is None:
        raise RuntimeError(
            "Bot is not initialized."
        )

    ref = parse_chat_ref(
        value
    )

    log.info(
        "Resolving chat ref=%r type=%s",
        ref,
        type(ref).__name__,
    )

    chat = await bot.get_chat(
        chat_id=ref
    )

    chat_type = (
        chat.type.value
        if hasattr(
            chat.type,
            "value",
        )
        else str(chat.type)
    )

    if allow_group:

        if chat_type not in {
            "channel",
            "supergroup",
        }:

            raise ValueError(
                "Destination must be a Telegram "
                "channel or supergroup."
            )

    else:

        if chat_type != "channel":

            raise ValueError(
                "Source must be a Telegram channel."
            )

    member = await bot.get_chat_member(
        chat_id=chat.id,
        user_id=bot.id,
    )

    if member.status not in {
        ChatMemberStatus.ADMINISTRATOR,
        ChatMemberStatus.CREATOR,
    }:

        raise PermissionError(
            "Bot must be administrator in this chat."
        )

    # Channel posting permission.
    if chat_type == "channel":

        if (
            member.status
            == ChatMemberStatus.ADMINISTRATOR
            and getattr(
                member,
                "can_post_messages",
                True,
            )
            is False
        ):

            raise PermissionError(
                "Bot needs Post Messages permission."
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
        "type": chat_type,
    }


# ============================================================
# WORKER MANAGEMENT
# ============================================================

async def start_route_worker(
    source_id: str,
    destination_id: str,
) -> None:

    source_id = str(
        source_id
    )

    destination_id = str(
        destination_id
    )

    key = route_key(
        source_id,
        destination_id,
    )

    if key not in route_queues:

        route_queues[key] = asyncio.Queue(
            maxsize=max(
                100,
                int(
                    db["settings"][
                        "max_queue"
                    ]
                ),
            )
        )

    task = route_tasks.get(
        key
    )

    if (
        task
        and not task.done()
    ):

        return

    route_tasks[key] = (
        asyncio.create_task(
            route_worker(
                source_id,
                destination_id,
            ),
            name=f"route-{key}",
        )
    )


async def stop_route_worker(
    source_id: str,
    destination_id: str,
) -> None:

    key = route_key(
        source_id,
        destination_id,
    )

    task = route_tasks.pop(
        key,
        None,
    )

    if task:

        task.cancel()

        try:
            await task

        except asyncio.CancelledError:
            pass

    route_queues.pop(
        key,
        None,
    )


async def ensure_route_workers():

    wanted = set()

    if db["settings"]["enabled"]:

        for source_id, route in db[
            "routes"
        ].items():

            if not isinstance(
                route,
                dict,
            ):

                continue

            destinations = route.get(
                "destinations",
                [],
            )

            if not isinstance(
                destinations,
                list,
            ):

                continue

            for destination in destinations:

                if (
                    not isinstance(
                        destination,
                        dict,
                    )
                    or destination.get(
                        "id"
                    )
                    is None
                ):

                    continue

                destination_id = str(
                    destination["id"]
                )

                key = route_key(
                    str(source_id),
                    destination_id,
                )

                wanted.add(
                    key
                )

                await start_route_worker(
                    str(source_id),
                    destination_id,
                )

    # Remove unused workers.

    for key in list(
        route_tasks
    ):

        if key not in wanted:

            sid, did = parse_route_key(
                key
            )

            await stop_route_worker(
                sid,
                did,
            )


# ============================================================
# VIDEO CACHE
# ============================================================

def cache_message(
    message: Message,
) -> None:

    key = (
        str(message.chat.id),
        int(message.message_id),
    )

    message_cache[key] = message

    message_cache_order.append(
        key
    )

    while len(
        message_cache_order
    ) > MESSAGE_CACHE_LIMIT:

        old = message_cache_order.pop(
            0
        )

        message_cache.pop(
            old,
            None,
        )


def get_cached_message(
    source_id: str,
    message_id: int,
) -> Message | None:

    return message_cache.get(
        (
            str(source_id),
            int(message_id),
        )
    )


# ============================================================
# VIDEO FALLBACK
# ============================================================

async def send_video_fallback(
    source_id: str,
    destination_id: str,
    message_id: int,
) -> bool:

    if bot is None:
        return False

    message = get_cached_message(
        source_id,
        message_id,
    )

    if (
        message is None
        or message.video is None
    ):

        return False

    video = message.video

    try:

        log.warning(
            "Using VIDEO FALLBACK "
            "source=%s destination=%s message=%s",
            source_id,
            destination_id,
            message_id,
        )

        await bot.send_video(
            chat_id=int(
                destination_id
            ),

            video=video.file_id,

            duration=video.duration,

            width=video.width,

            height=video.height,

            caption=message.caption,

            caption_entities=(
                message.caption_entities
            ),

            has_spoiler=(
                message.has_media_spoiler
            ),

            supports_streaming=True,
        )

        return True

    except TelegramRetryAfter as exc:

        await asyncio.sleep(
            int(exc.retry_after) + 1
        )

        try:

            await bot.send_video(
                chat_id=int(
                    destination_id
                ),
                video=video.file_id,
                duration=video.duration,
                width=video.width,
                height=video.height,
                caption=message.caption,
                caption_entities=(
                    message.caption_entities
                ),
                has_spoiler=(
                    message.has_media_spoiler
                ),
                supports_streaming=True,
            )

            return True

        except Exception as retry_exc:

            log.error(
                "Video fallback retry failed: %s",
                retry_exc,
            )

            return False

    except Exception as exc:

        log.error(
            "Video fallback failed: %s",
            exc,
        )

        return False


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

    retries = max(
        0,
        int(
            db["settings"][
                "retries"
            ]
        ),
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

            # MAIN METHOD
            #
            # This copies the Telegram message
            # WITHOUT "Forwarded from".

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
                "FloodWait %ss",
                wait,
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

            wait = min(
                2 ** attempt,
                30,
            )

            log.warning(
                "Network error. "
                "Retrying in %ss",
                wait,
            )

            await asyncio.sleep(
                wait
            )

        except TelegramForbiddenError as exc:

            db["stats"][
                "last_error"
            ] = str(exc)

            log.error(
                "Forbidden source=%s destination=%s: %s",
                source_id,
                destination_id,
                exc,
            )

            return False

        except TelegramBadRequest as exc:

            # ------------------------------------------------
            # VIDEO FALLBACK
            # ------------------------------------------------

            cached = get_cached_message(
                source_id,
                message_id,
            )

            if (
                cached is not None
                and cached.video is not None
            ):

                success = (
                    await send_video_fallback(
                        source_id,
                        destination_id,
                        message_id,
                    )
                )

                if success:
                    return True

            db["stats"][
                "last_error"
            ] = str(exc)

            log.error(
                "Telegram copy rejected "
                "source=%s "
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

    sent = 0

    for message_id in message_ids:

        success = await copy_single(
            source_id,
            destination_id,
            int(message_id),
        )

        if success:
            sent += 1

        interval = float(
            db["settings"][
                "interval"
            ]
        )

        if interval > 0:

            await asyncio.sleep(
                interval
            )

    return sent


# ============================================================
# ROUTE WORKER
# ============================================================

async def route_worker(
    source_id: str,
    destination_id: str,
) -> None:

    source_id = str(
        source_id
    )

    destination_id = str(
        destination_id
    )

    key = route_key(
        source_id,
        destination_id,
    )

    queue = route_queues[
        key
    ]

    log.info(
        "WORKER STARTED "
        "source=%s "
        "destination=%s",
        source_id,
        destination_id,
    )

    while True:

        item = await queue.get()

        try:

            message_ids = [
                int(x)
                for x in item.get(
                    "message_ids",
                    [],
                )
            ]

            is_album = bool(
                item.get(
                    "is_album",
                    False,
                )
            )

            if not message_ids:
                continue

            if is_album:

                sent = await copy_album(
                    source_id,
                    destination_id,
                    message_ids,
                )

                db["stats"][
                    "sent"
                ] += sent

                db["stats"][
                    "failed"
                ] += (
                    len(message_ids)
                    - sent
                )

                if sent:

                    db["stats"][
                        "last_sent"
                    ] = int(
                        time.time()
                    )

            else:

                success = (
                    await copy_single(
                        source_id,
                        destination_id,
                        message_ids[0],
                    )
                )

                if success:

                    db["stats"][
                        "sent"
                    ] += 1

                    db["stats"][
                        "last_sent"
                    ] = int(
                        time.time()
                    )

                else:

                    db["stats"][
                        "failed"
                    ] += 1

            await save_db()

        except asyncio.CancelledError:

            raise

        except Exception as exc:

            db["stats"][
                "last_error"
            ] = str(exc)

            log.exception(
                "Worker item error. "
                "Worker continues."
            )

        finally:

            queue.task_done()

        interval = float(
            db["settings"][
                "interval"
            ]
        )

        if interval > 0:

            await asyncio.sleep(
                interval
            )


# ============================================================
# QUEUE ROUTE
# ============================================================

async def queue_route(
    source_id: str,
    message_ids: list[int],
    is_album: bool,
) -> None:

    source_id = str(
        source_id
    )

    destinations = (
        route_destinations(
            source_id
        )
    )

    if not destinations:

        log.warning(
            "No destinations for source=%s",
            source_id,
        )

        return

    # One queue item for each destination.

    for destination in destinations:

        destination_id = str(
            destination["id"]
        )

        await start_route_worker(
            source_id,
            destination_id,
        )

        key = route_key(
            source_id,
            destination_id,
        )

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

        try:

            # IMPORTANT:
            # Never use put_nowait().
            #
            # If queue is full, this waits.
            # Message is not silently dropped.

            await route_queues[
                key
            ].put(item)

            db["stats"][
                "enqueued"
            ] += len(
                message_ids
            )

            log.info(
                "QUEUED "
                "source=%s "
                "destination=%s "
                "messages=%s",
                source_id,
                destination_id,
                message_ids,
            )

        except asyncio.CancelledError:

            raise

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
                "Could not queue route."
            )

    await save_db()


# ============================================================
# ALBUM FLUSH
# ============================================================

async def flush_album(
    source_id: str,
    media_group_id: str,
) -> None:

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

        message_ids = (
            album_buffer.pop(
                key,
                [],
            )
        )

        if not message_ids:
            return

        message_ids = sorted(
            set(message_ids)
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
) -> None:

    source_id = str(
        message.chat.id
    )

    message_id = int(
        message.message_id
    )

    log.info(
        "NEW CHANNEL POST "
        "source=%s "
        "message=%s "
        "video=%s "
        "album=%s",
        source_id,
        message_id,
        bool(message.video),
        bool(message.media_group_id),
    )

    # Only configured source.

    if source_id not in source_ids():

        log.info(
            "Ignoring unconfigured source=%s",
            source_id,
        )

        return

    # Bot stopped.

    if not db["settings"][
        "enabled"
    ]:

        return

    # No destination.

    if not route_destinations(
        source_id
    ):

        log.warning(
            "Source has no destinations."
        )

        return

    # --------------------------------------------------------
    # CACHE MESSAGE
    # --------------------------------------------------------

    cache_message(
        message
    )

    # --------------------------------------------------------
    # DEDUPE
    # --------------------------------------------------------

    dedupe_key = (
        f"{source_id}:{message_id}"
    )

    async with seen_lock:

        if dedupe_key in db["seen"]:

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

        # Limit JSON size.

        if len(
            db["seen"]
        ) > 20000:

            oldest = sorted(
                db["seen"].items(),
                key=lambda x: x[1],
            )[:5000]

            for old_key, _ in oldest:

                db["seen"].pop(
                    old_key,
                    None,
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
    # NORMAL MESSAGE
    # --------------------------------------------------------

    await queue_route(
        source_id,
        [message_id],
        False,
    )

    await save_db()


# ============================================================
# START
# ============================================================

@router.message(
    CommandStart()
)
async def start_handler(
    message: Message,
    state: FSMContext,
) -> None:

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
# ADMIN
# ============================================================

@router.message(
    Command("admin")
)
async def admin_handler(
    message: Message,
    state: FSMContext,
) -> None:

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
# CANCEL
# ============================================================

@router.message(
    Command("cancel")
)
async def cancel_handler(
    message: Message,
    state: FSMContext,
) -> None:

    await state.clear()

    if is_admin(message):

        await message.answer(
            "❌ Cancelled.",
            reply_markup=home_keyboard(),
        )


# ============================================================
# CALLBACK
# ============================================================

@router.callback_query()
async def callback_handler(
    query: CallbackQuery,
    state: FSMContext,
) -> None:

    if not is_admin(query):

        await query.answer(
            "Access denied.",
            show_alert=True,
        )

        return

    data = query.data or ""

    if not query.message:

        await query.answer()

        return

    # --------------------------------------------------------
    # HOME
    # --------------------------------------------------------

    if data == "home":

        await state.clear()

        await query.answer()

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
        ] = not bool(
            db["settings"][
                "enabled"
            ]
        )

        await save_db()

        if db["settings"][
            "enabled"
        ]:

            await ensure_route_workers()

            text = "▶️ Bot started."

        else:

            text = (
                "⏹ Bot stopped.\n\n"
                "New posts will not be accepted."
            )

        await query.answer(
            text
        )

        await query.message.edit_text(
            home_text(),
            reply_markup=home_keyboard(),
        )

        return

    # --------------------------------------------------------
    # ROUTES
    # --------------------------------------------------------

    if data == "sources":

        await query.answer()

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
                            "   └ "
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

    # --------------------------------------------------------
    # OPEN ROUTE
    # --------------------------------------------------------

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

            await query.answer(
                "Invalid source.",
                show_alert=True,
            )

            return

        await query.answer()

        sid = str(
            source["id"]
        )

        selected = (
            route_destination_ids(
                sid
            )
        )

        text = (
            "🔀 "
            "<b>Route Configuration</b>\n\n"

            "Source:\n"
            f"<b>{display_name(source)}</b>\n"
            f"<code>{sid}</code>\n\n"

            "Selected destinations: "
            f"<b>{len(selected)}</b>\n\n"

            "Tap destination to "
            "enable/disable."
        )

        await query.message.edit_text(
            text,
            reply_markup=route_keyboard(
                sid
            ),
        )

        return

    # --------------------------------------------------------
    # DESTINATION TOGGLE
    # --------------------------------------------------------

    if data.startswith(
        "route_dest:"
    ):

        try:

            _,
            source_id,
            index = data.split(
                ":",
                2,
            )

            destination = db[
                "destinations"
            ][int(index)]

        except Exception:

            await query.answer(
                "Invalid route.",
                show_alert=True,
            )

            return

        if source_id not in source_ids():

            await query.answer(
                "Source not found.",
                show_alert=True,
            )

            return

        destination_id = str(
            destination["id"]
        )

        source = get_source(
            source_id
        )

        if source is None:

            await query.answer(
                "Source not found.",
                show_alert=True,
            )

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

        current = {
            str(item["id"])
            for item in route[
                "destinations"
            ]
        }

        if destination_id in current:

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

        await query.answer(
            f"{display_name(destination)} {action}"
        )

        await query.message.edit_reply_markup(
            reply_markup=route_keyboard(
                source_id
            )
        )

        return

    # --------------------------------------------------------
    # DESTINATIONS
    # --------------------------------------------------------

    if data == "destinations":

        await query.answer()

        text = (
            "📤 <b>Destination Channels</b>\n\n"
        )

        if not db["destinations"]:

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

                used_by = 0

                for source in db["sources"]:

                    if did in route_destination_ids(
                        str(
                            source["id"]
                        )
                    ):

                        used_by += 1

                text += (
                    f"{index}. "
                    f"<b>{display_name(destination)}</b>\n"
                    f"Used by: "
                    f"<b>{used_by}</b> route(s)\n"
                    f"<code>{did}</code>\n\n"
                )

        await query.message.edit_text(
            text,
            reply_markup=destinations_keyboard(),
        )

        return

    # --------------------------------------------------------
    # ADD SOURCE
    # --------------------------------------------------------

    if data == "add_source":

        await query.answer()

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
            "in source channel."
        )

        return

    # --------------------------------------------------------
    # ADD DESTINATION
    # --------------------------------------------------------

    if data == "add_destination":

        await query.answer()

        await state.set_state(
            AddDestination.value
        )

        await query.message.edit_text(
            "📤 <b>Add Destination</b>\n\n"

            "Send username or numeric ID:\n\n"

            "<code>@channelusername</code>\n"
            "or\n"
            "<code>-1001234567890</code>\n\n"

            "Channel: Bot needs "
            "Post Messages permission.\n\n"

            "Group: Bot must be administrator."
        )

        return

    # --------------------------------------------------------
    # ADD DESTINATION FOR ROUTE
    # --------------------------------------------------------

    if data.startswith(
        "new_dest_for:"
    ):

        await query.answer()

        source_id = data.split(
            ":",
            1,
        )[1]

        if source_id not in source_ids():

            await query.answer(
                "Source not found.",
                show_alert=True,
            )

            return

        await state.update_data(
            route_source_id=source_id
        )

        await state.set_state(
            AddDestination.value
        )

        await query.message.edit_text(
            "📤 <b>Add Destination</b>\n\n"

            "Send @username or numeric ID.\n\n"

            "It will automatically be "
            "connected to this source."
        )

        return

    # --------------------------------------------------------
    # REMOVE SOURCE
    # --------------------------------------------------------

    if data.startswith(
        "remove_source:"
    ):

        await query.answer()

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

            for key in list(
                route_tasks
            ):

                sid, did = (
                    parse_route_key(
                        key
                    )
                )

                if sid == source_id:

                    await stop_route_worker(
                        sid,
                        did,
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
            "🔀 "
            "<b>Source → Destination Routes</b>",
            reply_markup=sources_keyboard(),
        )

        return

    # --------------------------------------------------------
    # DELETE DESTINATION
    # --------------------------------------------------------

    if data.startswith(
        "delete_destination:"
    ):

        await query.answer()

        try:

            index = int(
                data.split(
                    ":",
                    1,
                )[1]
            )

            destination = db[
                "destinations"
            ].pop(index)

        except Exception:

            await query.answer(
                "Invalid destination.",
                show_alert=True,
            )

            return

        destination_id = str(
            destination["id"]
        )

        for route in db[
            "routes"
        ].values():

            if isinstance(
                route,
                dict,
            ):

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

        for key in list(
            route_tasks
        ):

            sid, did = (
                parse_route_key(
                    key
                )
            )

            if did == destination_id:

                await stop_route_worker(
                    sid,
                    did,
                )

        await save_db()

        await ensure_route_workers()

        await query.message.edit_text(
            "📤 "
            "<b>Destination Channels</b>",
            reply_markup=destinations_keyboard(),
        )

        return

    # --------------------------------------------------------
    # SETTINGS
    # --------------------------------------------------------

    if data == "settings":

        await query.answer()

        await query.message.edit_text(
            "⚙️ <b>Settings</b>",
            reply_markup=settings_keyboard(),
        )

        return

    # --------------------------------------------------------
    # STATUS
    # --------------------------------------------------------

    if data == "status":

        await query.answer()

        stats = db["stats"]

        queue_lines = []

        for key, queue in (
            route_queues.items()
        ):

            try:

                sid, did = (
                    parse_route_key(
                        key
                    )
                )

            except ValueError:

                continue

            queue_lines.append(
                f"• "
                f"{display_name(get_source(sid))}"
                f" → "
                f"{display_name(get_destination(did))}"
                f": <b>{queue.qsize()}</b>"
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

            f"Workers: "
            f"<b>{len(route_tasks)}</b>\n"

            f"Queues: "
            f"<b>{len(route_queues)}</b>\n\n"

            f"📥 Received: "
            f"<b>{stats['received']}</b>\n"

            f"📦 Enqueued: "
            f"<b>{stats['enqueued']}</b>\n"

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
            f"{str(stats['last_error'] or '-')[:3000]}"
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
    # INTERVAL
    # --------------------------------------------------------

    if data == "setting:interval":

        await query.answer()

        await state.set_state(
            SetInterval.value
        )

        await query.message.edit_text(
            "⏱ <b>Set Copy Interval</b>\n\n"
            "Enter seconds.\n\n"
            "Example:\n"
            "<code>1</code>"
        )

        return

    # --------------------------------------------------------
    # ALBUM
    # --------------------------------------------------------

    if data == "setting:album":

        await query.answer()

        await state.set_state(
            SetAlbumWait.value
        )

        await query.message.edit_text(
            "📦 <b>Album Wait</b>\n\n"
            "Enter seconds.\
