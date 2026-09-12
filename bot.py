from __future__ import annotations

import asyncio
import json
import logging
import os
import secrets
import time
from pathlib import Path
from typing import Any

from aiohttp import web
from dotenv import load_dotenv

from aiogram import Bot, Dispatcher, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
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
# ENV
# ============================================================

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
ADMIN_ID_RAW = os.getenv("ADMIN_ID", "").strip()

PORT_RAW = os.getenv("PORT", "10000").strip()
DATA_FILE = Path(
    os.getenv("DATA_FILE", "data.json").strip()
)

RENDER_URL = (
    os.getenv("RENDER_URL", "")
    .strip()
    .rstrip("/")
)

WEBHOOK_SECRET = (
    os.getenv("WEBHOOK_SECRET", "")
    .strip()
)

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is missing.")

if not ADMIN_ID_RAW.lstrip("-").isdigit():
    raise RuntimeError("ADMIN_ID must be numeric.")

try:
    PORT = int(PORT_RAW)
except ValueError:
    raise RuntimeError("PORT must be numeric.")

if not RENDER_URL:
    raise RuntimeError("RENDER_URL is missing.")

if not WEBHOOK_SECRET:
    raise RuntimeError("WEBHOOK_SECRET is missing.")

if WEBHOOK_SECRET == BOT_TOKEN:
    raise RuntimeError(
        "WEBHOOK_SECRET must NOT be the BOT_TOKEN."
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

log = logging.getLogger("route-repeat-bot")

# ============================================================
# ROUTER
# ============================================================

# IMPORTANT:
# This was missing in your previous code.
router = Router()

# ============================================================
# DEFAULT SETTINGS
# ============================================================

DEFAULT_SETTINGS: dict[str, Any] = {
    "enabled": True,
    "repeat_enabled": True,

    # Seconds between playlist items.
    "interval": 10.0,

    # Retry attempts.
    "retries": 8,

    # Album collection delay.
    "album_wait": 1.5,

    # Maximum playlist entries per source.
    "max_playlist": 50000,
}

# ============================================================
# DATABASE
# ============================================================


def fresh_db() -> dict[str, Any]:
    return {
        "settings": DEFAULT_SETTINGS.copy(),

        "sources": [],

        "destinations": [],

        # source_id -> {
        #     source: {...},
        #     destinations: [...]
        # }
        "routes": {},

        # source_id -> [
        #     {
        #         "id": 123,
        #         "ids": [123],
        #         "album": false,
        #         "created": 1234567890
        #     }
        # ]
        "playlists": {},

        # IMPORTANT:
        #
        # Position is now stored per:
        #
        # source_id|destination_id
        #
        # Example:
        #
        # "-100111|-100222": 0
        #
        # "-100111|-100333": 0
        #
        # This prevents destination workers
        # from interfering with each other.
        "positions": {},

        "stats": {
            "received": 0,
            "sent": 0,
            "failed": 0,
            "albums": 0,
            "cycles": 0,
            "last_received": None,
            "last_sent": None,
            "last_error": None,
        },
    }


def load_db() -> dict[str, Any]:

    if not DATA_FILE.exists():
        return fresh_db()

    try:
        raw = json.loads(
            DATA_FILE.read_text(
                encoding="utf-8"
            )
        )

        if not isinstance(raw, dict):
            return fresh_db()

    except Exception:
        log.exception(
            "Could not load data.json"
        )
        return fresh_db()

    db = fresh_db()

    # --------------------------------------------------------
    # SETTINGS
    # --------------------------------------------------------

    settings = raw.get("settings")

    if isinstance(settings, dict):
        db["settings"].update(settings)

    # Normalize settings.
    try:
        db["settings"]["interval"] = float(
            db["settings"].get(
                "interval",
                10.0,
            )
        )
    except Exception:
        db["settings"]["interval"] = 10.0

    try:
        db["settings"]["retries"] = int(
            db["settings"].get(
                "retries",
                8,
            )
        )
    except Exception:
        db["settings"]["retries"] = 8

    try:
        db["settings"]["album_wait"] = float(
            db["settings"].get(
                "album_wait",
                1.5,
            )
        )
    except Exception:
        db["settings"]["album_wait"] = 1.5

    try:
        db["settings"]["max_playlist"] = int(
            db["settings"].get(
                "max_playlist",
                50000,
            )
        )
    except Exception:
        db["settings"]["max_playlist"] = 50000

    # --------------------------------------------------------
    # SOURCES
    # --------------------------------------------------------

    sources = raw.get("sources")

    if isinstance(sources, list):

        for item in sources:

            if (
                isinstance(item, dict)
                and item.get("id") is not None
            ):
                db["sources"].append(item)

    # --------------------------------------------------------
    # DESTINATIONS
    # --------------------------------------------------------

    destinations = raw.get(
        "destinations"
    )

    if isinstance(destinations, list):

        for item in destinations:

            if (
                isinstance(item, dict)
                and item.get("id") is not None
            ):
                db["destinations"].append(
                    item
                )

    # --------------------------------------------------------
    # ROUTES
    # --------------------------------------------------------

    routes = raw.get("routes")

    if isinstance(routes, dict):

        db["routes"] = routes

    # --------------------------------------------------------
    # PLAYLISTS
    # --------------------------------------------------------

    playlists = raw.get(
        "playlists"
    )

    if isinstance(playlists, dict):

        for sid, items in playlists.items():

            if not isinstance(
                items,
                list,
            ):
                continue

            cleaned: list[
                dict[str, Any]
            ] = []

            for item in items:

                # Old/simple format:
                #
                # 123
                #
                if isinstance(
                    item,
                    int,
                ):

                    cleaned.append(
                        {
                            "id": int(item),
                            "ids": [int(item)],
                            "album": False,
                            "created": 0,
                        }
                    )

                    continue

                if not isinstance(
                    item,
                    dict,
                ):
                    continue

                ids = item.get(
                    "ids",
                    [],
                )

                if not isinstance(
                    ids,
                    list,
                ):
                    ids = []

                clean_ids: list[int] = []

                for value in ids:

                    try:
                        clean_ids.append(
                            int(value)
                        )
                    except Exception:
                        pass

                if not clean_ids:

                    try:
                        if item.get(
                            "id"
                        ) is not None:
                            clean_ids = [
                                int(
                                    item["id"]
                                )
                            ]
                    except Exception:
                        pass

                if not clean_ids:
                    continue

                try:
                    created = int(
                        item.get(
                            "created",
                            0,
                        )
                        or 0
                    )
                except Exception:
                    created = 0

                cleaned.append(
                    {
                        "id": clean_ids[0],
                        "ids": clean_ids,
                        "album": bool(
                            item.get(
                                "album",
                                len(clean_ids) > 1,
                            )
                        ),
                        "created": created,
                    }
                )

            db["playlists"][
                str(sid)
            ] = cleaned

    # --------------------------------------------------------
    # POSITIONS
    # --------------------------------------------------------

    positions = raw.get(
        "positions"
    )

    if isinstance(positions, dict):

        for key, value in positions.items():

            try:
                db["positions"][
                    str(key)
                ] = max(
                    0,
                    int(value),
                )
            except Exception:
                db["positions"][
                    str(key)
                ] = 0

    # --------------------------------------------------------
    # STATS
    # --------------------------------------------------------

    stats = raw.get("stats")

    if isinstance(stats, dict):
        db["stats"].update(stats)

    # --------------------------------------------------------
    # NORMALIZE SOURCE MAP
    # --------------------------------------------------------

    source_map: dict[
        str,
        dict[str, Any],
    ] = {}

    for source in db["sources"]:

        if not isinstance(
            source,
            dict,
        ):
            continue

        if source.get("id") is None:
            continue

        sid = str(
            source["id"]
        )

        source["id"] = sid

        source_map[sid] = source

    # --------------------------------------------------------
    # NORMALIZE DESTINATION MAP
    # --------------------------------------------------------

    destination_map: dict[
        str,
        dict[str, Any],
    ] = {}

    for destination in db[
        "destinations"
    ]:

        if not isinstance(
            destination,
            dict,
        ):
            continue

        if destination.get("id") is None:
            continue

        did = str(
            destination["id"]
        )

        destination["id"] = did

        destination_map[did] = (
            destination
        )

    # --------------------------------------------------------
    # NORMALIZE ROUTES
    # --------------------------------------------------------

    normalized_routes: dict[
        str,
        dict[str, Any],
    ] = {}

    for raw_sid, route in db[
        "routes"
    ].items():

        sid = str(raw_sid)

        if not isinstance(
            route,
            dict,
        ):
            continue

        source = route.get(
            "source"
        )

        if (
            not isinstance(
                source,
                dict,
            )
            or source.get("id") is None
        ):

            source = source_map.get(
                sid
            )

        if source is None:

            source = {
                "id": sid,
                "title": sid,
                "username": "",
            }

        source["id"] = sid

        source_map[sid] = source

        raw_dests = route.get(
            "destinations",
            [],
        )

        if not isinstance(
            raw_dests,
            list,
        ):
            raw_dests = []

        cleaned_dests = []

        seen_dests = set()

        for destination in raw_dests:

            if not isinstance(
                destination,
                dict,
            ):
                continue

            if destination.get(
                "id"
            ) is None:
                continue

            did = str(
                destination["id"]
            )

            destination["id"] = did

            destination_map[did] = (
                destination
            )

            if did in seen_dests:
                continue

            seen_dests.add(did)

            cleaned_dests.append(
                destination
            )

        normalized_routes[sid] = {
            "source": source,
            "destinations": cleaned_dests,
        }

    db["routes"] = normalized_routes

    db["sources"] = list(
        source_map.values()
    )

    db["destinations"] = list(
        destination_map.values()
    )

    # Make sure every source has route.
    for source in db[
        "sources"
    ]:

        sid = str(
            source["id"]
        )

        db["routes"].setdefault(
            sid,
            {
                "source": source,
                "destinations": [],
            },
        )

        db["playlists"].setdefault(
            sid,
            [],
        )

    # --------------------------------------------------------
    # MIGRATE OLD SOURCE POSITIONS
    # --------------------------------------------------------
    #
    # Old version:
    #
    # positions = {
    #     "source_id": 5
    # }
    #
    # New version:
    #
    # positions = {
    #     "source_id|destination_id": 5
    # }
    #
    # We reset route positions to zero during
    # migration because one source position cannot
    # safely represent multiple destination positions.
    #

    migrated_positions: dict[
        str,
        int,
    ] = {}

    for sid, route in db[
        "routes"
    ].items():

        for destination in route.get(
            "destinations",
            [],
        ):

            if not isinstance(
                destination,
                dict,
            ):
                continue

            did = destination.get(
                "id"
            )

            if did is None:
                continue

            key = route_key(
                sid,
                str(did),
            )

            old_value = db[
                "positions"
            ].get(
                key,
                0,
            )

            # If already in new format,
            # keep it.
            try:
                migrated_positions[
                    key
                ] = max(
                    0,
                    int(old_value),
                )
            except Exception:
                migrated_positions[
                    key
                ] = 0

    db["positions"] = (
        migrated_positions
    )

    return db


db = load_db()

db_lock = asyncio.Lock()
playlist_lock = asyncio.Lock()

# ============================================================
# GLOBAL BOT
# ============================================================

bot: Bot | None = None
dp: Dispatcher | None = None

server_runner: web.AppRunner | None = None

# ============================================================
# WORKERS
# ============================================================

# key = source_id|destination_id
worker_tasks: dict[
    str,
    asyncio.Task,
] = {}

worker_events: dict[
    str,
    asyncio.Event,
] = {}

# ============================================================
# ALBUM BUFFER
# ============================================================

album_buffer: dict[
    tuple[str, str],
    set[int],
] = {}

album_tasks: dict[
    tuple[str, str],
    asyncio.Task,
] = {}

# ============================================================
# FSM
# ============================================================


class AddSource(StatesGroup):
    value = State()


class AddDestination(StatesGroup):
    value = State()


class SetInterval(StatesGroup):
    value = State()


class SetAlbumWait(StatesGroup):
    value = State()


class SetRetries(StatesGroup):
    value = State()


class SetPlaylistLimit(StatesGroup):
    value = State()


# ============================================================
# GENERAL HELPERS
# ============================================================


def is_admin(
    obj: Message | CallbackQuery,
) -> bool:

    return bool(
        obj.from_user
        and obj.from_user.id == ADMIN_ID
    )


def name_of(
    item: dict[str, Any] | None,
) -> str:

    if not item:
        return "Unknown"

    return str(
        item.get("title")
        or item.get("username")
        or item.get("id")
        or "Unknown"
    )


def status_value(
    value: Any,
) -> str:

    return getattr(
        value,
        "value",
        str(value),
    )


def source_ids() -> set[str]:

    return {
        str(item["id"])
        for item in db["sources"]
        if isinstance(
            item,
            dict,
        )
        and item.get("id") is not None
    }


def destination_ids() -> set[str]:

    return {
        str(item["id"])
        for item in db["destinations"]
        if isinstance(
            item,
            dict,
        )
        and item.get("id") is not None
    }


def get_source(
    sid: str,
) -> dict[str, Any] | None:

    sid = str(sid)

    for source in db["sources"]:

        if str(
            source.get("id")
        ) == sid:
            return source

    return None


def get_destination(
    did: str,
) -> dict[str, Any] | None:

    did = str(did)

    for destination in db[
        "destinations"
    ]:

        if str(
            destination.get("id")
        ) == did:
            return destination

    return None


def route_dests(
    sid: str,
) -> list[dict[str, Any]]:

    route = db["routes"].get(
        str(sid)
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

    return [
        item
        for item in destinations
        if isinstance(
            item,
            dict,
        )
        and item.get("id") is not None
    ]


def route_dest_ids(
    sid: str,
) -> set[str]:

    return {
        str(item["id"])
        for item in route_dests(sid)
    }


def route_key(
    sid: str,
    did: str,
) -> str:

    return (
        f"{str(sid)}|{str(did)}"
    )


def split_route_key(
    key: str,
) -> tuple[str, str]:

    if "|" not in key:
        return key, ""

    return key.split(
        "|",
        1,
    )


def parse_chat_id(
    value: str,
) -> Any:

    value = value.strip()

    if value.lstrip("-").isdigit():
        return int(value)

    return value


async def get_chat(
    value: str,
) -> Any:

    if bot is None:
        raise RuntimeError(
            "Bot is not initialized."
        )

    value = value.strip()

    if not value:
        raise ValueError(
            "Username or chat ID is empty."
        )

    return await bot.get_chat(
        parse_chat_id(value)
    )


# ============================================================
# DATABASE SAVE
# ============================================================


async def save_db() -> None:

    async with db_lock:

        DATA_FILE.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        temp = DATA_FILE.with_name(
            DATA_FILE.name + ".tmp"
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
# RESOLVE SOURCE
# ============================================================


async def resolve_source(
    value: str,
) -> dict[str, str]:

    if bot is None:
        raise RuntimeError(
            "Bot is not initialized."
        )

    chat = await get_chat(
        value
    )

    chat_type = status_value(
        getattr(
            chat,
            "type",
            "",
        )
    )

    if chat_type != "channel":

        raise ValueError(
            "Source must be a Telegram channel."
        )

    me = await bot.get_me()

    member = await bot.get_chat_member(
        chat.id,
        me.id,
    )

    member_status = status_value(
        getattr(
            member,
            "status",
            "",
        )
    )

    if member_status not in {
        "administrator",
        "creator",
    }:

        raise PermissionError(
            "Bot must be administrator in source channel."
        )

    return {
        "id": str(chat.id),
        "title": str(
            chat.title
            or chat.username
            or chat.id
        ),
        "username": str(
            chat.username or ""
        ),
    }


# ============================================================
# RESOLVE DESTINATION
# ============================================================


async def resolve_destination(
    value: str,
) -> dict[str, str]:

    if bot is None:
        raise RuntimeError(
            "Bot is not initialized."
        )

    chat = await get_chat(
        value
    )

    chat_type = status_value(
        getattr(
            chat,
            "type",
            "",
        )
    )

    if chat_type not in {
        "channel",
        "group",
        "supergroup",
    }:

        raise ValueError(
            "Destination must be a channel, group or supergroup."
        )

    me = await bot.get_me()

    member = await bot.get_chat_member(
        chat.id,
        me.id,
    )

    member_status = status_value(
        getattr(
            member,
            "status",
            "",
        )
    )

    if member_status not in {
        "administrator",
        "creator",
    }:

        raise PermissionError(
            "Bot must be administrator in destination."
        )

    if (
        chat_type == "channel"
        and member_status == "administrator"
        and getattr(
            member,
            "can_post_messages",
            True,
        ) is False
    ):

        raise PermissionError(
            "Bot needs Post Messages permission in destination."
        )

    return {
        "id": str(chat.id),
        "title": str(
            chat.title
            or chat.username
            or chat.id
        ),
        "username": str(
            chat.username or ""
        ),
    }


# ============================================================
# PLAYLIST
# ============================================================


def get_playlist(
    sid: str,
) -> list[dict[str, Any]]:

    sid = str(sid)

    playlist = db[
        "playlists"
    ].get(
        sid,
        [],
    )

    if not isinstance(
        playlist,
        list,
    ):
        playlist = []

    return playlist


def playlist_length(
    sid: str,
) -> int:

    return len(
        get_playlist(sid)
    )


async def add_playlist_item(
    sid: str,
    message_ids: list[int],
    album: bool = False,
) -> bool:

    sid = str(sid)

    clean_ids: list[int] = []

    for mid in message_ids:

        try:
            clean_ids.append(
                int(mid)
            )
        except Exception:
            pass

    if not clean_ids:
        return False

    # Preserve Telegram message order.
    clean_ids = list(
        dict.fromkeys(
            clean_ids
        )
    )

    added = False

    async with playlist_lock:

        playlist = db[
            "playlists"
        ].setdefault(
            sid,
            [],
        )

        # ----------------------------------------------------
        # Check duplicate message IDs.
        # ----------------------------------------------------

        existing = set()

        for item in playlist:

            if not isinstance(
                item,
                dict,
            ):
                continue

            ids = item.get(
                "ids",
                [],
            )

            if not isinstance(
                ids,
                list,
            ):
                continue

            for value in ids:

                try:
                    existing.add(
                        int(value)
                    )
                except Exception:
                    pass

        if any(
            mid in existing
            for mid in clean_ids
        ):

            return False

        playlist.append(
            {
                "id": clean_ids[0],
                "ids": clean_ids,
                "album": bool(
                    album
                    or len(clean_ids) > 1
                ),
                "created": int(
                    time.time()
                ),
            }
        )

        added = True

        # ----------------------------------------------------
        # Limit playlist.
        # ----------------------------------------------------

        try:
            max_playlist = int(
                db["settings"].get(
                    "max_playlist",
                    50000,
                )
            )
        except Exception:
            max_playlist = 50000

        max_playlist = max(
            100,
            min(
                50000,
                max_playlist,
            ),
        )

        if len(playlist) > max_playlist:

            remove_count = (
                len(playlist)
                - max_playlist
            )

            del playlist[
                :remove_count
            ]

            # Correct every route position
            # for this source.
            for key in list(
                db["positions"]
            ):

                route_sid, _ = (
                    split_route_key(key)
                )

                if route_sid != sid:
                    continue

                try:
                    position = int(
                        db["positions"][
                            key
                        ]
                    )
                except Exception:
                    position = 0

                db["positions"][
                    key
                ] = max(
                    0,
                    position
                    - remove_count,
                )

    if added:
        await save_db()

    return added


def next_playlist_item(
    sid: str,
    did: str,
) -> tuple[
    dict[str, Any] | None,
    bool,
]:

    sid = str(sid)
    did = str(did)

    playlist = get_playlist(
        sid
    )

    if not playlist:
        return None, False

    key = route_key(
        sid,
        did,
    )

    try:
        position = int(
            db["positions"].get(
                key,
                0,
            )
        )
    except Exception:
        position = 0

    if position < 0:
        position = 0

    # If playlist was shortened/deleted.
    if position >= len(playlist):
        position = 0

    item = playlist[
        position
    ]

    # --------------------------------------------------------
    # IMPORTANT REPEAT LOGIC
    #
    # First = index 0
    # Second = index 1
    # ...
    # Last = index len-1
    #
    # After Last:
    # position becomes 0.
    #
    # Therefore:
    #
    # First → Second → ... → Last → First
    # --------------------------------------------------------

    next_position = (
        position + 1
    )

    cycle_finished = (
        next_position >= len(playlist)
    )

    if cycle_finished:
        next_position = 0

    db["positions"][
        key
    ] = next_position

    return item, cycle_finished


# ============================================================
# WAKE WORKERS
# ============================================================


def get_worker_event(
    key: str,
) -> asyncio.Event:

    event = worker_events.get(
        key
    )

    if event is None:

        event = asyncio.Event()

        worker_events[key] = event

    return event


def wake_route(
    sid: str,
    did: str,
) -> None:

    key = route_key(
        sid,
        did,
    )

    event = worker_events.get(
        key
    )

    if event:
        event.set()


def wake_source(
    sid: str,
) -> None:

    sid = str(sid)

    for key, event in list(
        worker_events.items()
    ):

        route_sid, _ = (
            split_route_key(key)
        )

        if route_sid == sid:
            event.set()


def wake_all_workers() -> None:

    for event in list(
        worker_events.values()
    ):
        event.set()


# ============================================================
# COPY MESSAGE
# ============================================================


async def copy_one(
    sid: str,
    did: str,
    mid: int,
) -> bool:

    if bot is None:
        return False

    try:
        retries = int(
            db["settings"].get(
                "retries",
                8,
            )
        )
    except Exception:
        retries = 8

    retries = max(
        0,
        min(
            20,
            retries,
        ),
    )

    for attempt in range(
        retries + 1
    ):

        try:

            log.info(
                "COPY %s -> %s | message=%s | attempt=%s",
                sid,
                did,
                mid,
                attempt + 1,
            )

            await bot.copy_message(
                chat_id=int(did),
                from_chat_id=int(sid),
                message_id=int(mid),
            )

            log.info(
                "COPY SUCCESS %s -> %s | message=%s",
                sid,
                did,
                mid,
            )

            return True

        except TelegramRetryAfter as exc:

            try:
                wait = int(
                    exc.retry_after
                ) + 1
            except Exception:
                wait = 5

            log.warning(
                "Telegram rate limit. Sleeping %ss.",
                wait,
            )

            await asyncio.sleep(
                wait
            )

        except TelegramNetworkError as exc:

            db["stats"][
                "last_error"
            ] = str(exc)

            if attempt >= retries:
                return False

            wait = min(
                2 ** attempt,
                30,
            )

            log.warning(
                "Network error. Retry in %ss.",
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
                "Forbidden %s -> %s: %s",
                sid,
                did,
                exc,
            )

            return False

        except TelegramBadRequest as exc:

            db["stats"][
                "last_error"
            ] = str(exc)

            log.error(
                "BadRequest %s -> %s | message=%s | %s",
                sid,
                did,
                mid,
                exc,
            )

            return False

        except Exception as exc:

            db["stats"][
                "last_error"
            ] = str(exc)

            log.exception(
                "Unexpected copy error."
            )

            if attempt >= retries:
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
    sid: str,
    did: str,
    message_ids: list[int],
) -> bool:

    success = True

    for index, mid in enumerate(
        message_ids
    ):

        ok = await copy_one(
            sid,
            did,
            mid,
        )

        if not ok:
            success = False

        if (
            index
            < len(message_ids) - 1
        ):

            try:
                interval = float(
                    db["settings"].get(
                        "interval",
                        10.0,
                    )
                )
            except Exception:
                interval = 10.0

            # Don't wait the full interval
            # between items of an album.
            await asyncio.sleep(
                min(
                    max(
                        interval,
                        0.1,
                    ),
                    1.0,
                )
            )

    return success


# ============================================================
# REPEAT WORKER
# ============================================================


async def repeat_worker(
    sid: str,
    did: str,
) -> None:

    sid = str(sid)
    did = str(did)

    key = route_key(
        sid,
        did,
    )

    event = get_worker_event(
        key
    )

    log.info(
        "WORKER STARTED | %s -> %s",
        sid,
        did,
    )

    while True:

        try:

            # ------------------------------------------------
            # GLOBAL STOP
            # ------------------------------------------------

            if not db["settings"].get(
                "enabled",
                True,
            ):

                event.clear()

                try:
                    await asyncio.wait_for(
                        event.wait(),
                        timeout=5,
                    )
                except asyncio.TimeoutError:
                    pass

                continue

            # ------------------------------------------------
            # SOURCE STILL EXISTS?
            # ------------------------------------------------

            if sid not in source_ids():

                await asyncio.sleep(
                    2
                )

                continue

            # ------------------------------------------------
            # DESTINATION STILL IN ROUTE?
            # ------------------------------------------------

            if did not in route_dest_ids(
                sid
            ):

                await asyncio.sleep(
                    2
                )

                continue

            # ------------------------------------------------
            # REPEAT OFF
            # ------------------------------------------------

            if not db[
                "settings"
            ].get(
                "repeat_enabled",
                True,
            ):

                event.clear()

                try:
                    await asyncio.wait_for(
                        event.wait(),
                        timeout=5,
                    )
                except asyncio.TimeoutError:
                    pass

                continue

            # ------------------------------------------------
            # PLAYLIST EMPTY
            # ------------------------------------------------

            if not get_playlist(sid):

                event.clear()

                try:
                    await asyncio.wait_for(
                        event.wait(),
                        timeout=10,
                    )
                except asyncio.TimeoutError:
                    pass

                continue

            # ------------------------------------------------
            # SELECT NEXT
            # ------------------------------------------------

            async with playlist_lock:

                item, cycle_finished = (
                    next_playlist_item(
                        sid,
                        did,
                    )
                )

                if cycle_finished:

                    db["stats"][
                        "cycles"
                    ] += 1

            if item is None:

                await asyncio.sleep(
                    2
                )

                continue

            # Save position immediately.
            await save_db()

            ids = item.get(
                "ids",
                [],
            )

            if not isinstance(
                ids,
                list,
            ):
                ids = []

            clean_ids = []

            for value in ids:

                try:
                    clean_ids.append(
                        int(value)
                    )
                except Exception:
                    pass

            if not clean_ids:
                continue

            log.info(
                "PLAYLIST ITEM | route=%s | message=%s | album=%s",
                key,
                clean_ids,
                len(clean_ids) > 1,
            )

            # ------------------------------------------------
            # COPY
            # ------------------------------------------------

            if len(clean_ids) == 1:

                success = await copy_one(
                    sid,
                    did,
                    clean_ids[0],
                )

            else:

                success = await copy_album(
                    sid,
                    did,
                    clean_ids,
                )

            # ------------------------------------------------
            # STATS
            # ------------------------------------------------

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

            # ------------------------------------------------
            # INTERVAL
            # ------------------------------------------------

            try:
                interval = float(
                    db["settings"].get(
                        "interval",
                        10.0,
                    )
                )
            except Exception:
                interval = 10.0

            interval = max(
                0.1,
                min(
                    3600.0,
                    interval,
                ),
            )

            # Wake instantly if a new item/settings
            # arrives; otherwise wait interval.
            event.clear()

            try:

                await asyncio.wait_for(
                    event.wait(),
                    timeout=interval,
                )

            except asyncio.TimeoutError:
                pass

        except asyncio.CancelledError:

            log.info(
                "WORKER STOPPED | %s -> %s",
                sid,
                did,
            )

            raise

        except Exception as exc:

            db["stats"][
                "last_error"
            ] = str(exc)

            log.exception(
                "Worker crashed internally | %s -> %s",
                sid,
                did,
            )

            await asyncio.sleep(
                3
            )


# ============================================================
# WORKER MANAGEMENT
# ============================================================


async def start_worker(
    sid: str,
    did: str,
) -> None:

    sid = str(sid)
    did = str(did)

    key = route_key(
        sid,
        did,
    )

    existing = worker_tasks.get(
        key
    )

    if (
        existing is not None
        and not existing.done()
    ):
        return

    worker_events[key] = (
        asyncio.Event()
    )

    worker_tasks[key] = (
        asyncio.create_task(
            repeat_worker(
                sid,
                did,
            ),
            name=(
                f"repeat-{sid}-{did}"
            ),
        )
    )


async def stop_worker(
    sid: str,
    did: str,
) -> None:

    key = route_key(
        sid,
        did,
    )

    task = worker_tasks.pop(
        key,
        None,
    )

    event = worker_events.pop(
        key,
        None,
    )

    if event:
        event.set()

    if task is None:
        return

    if not task.done():
        task.cancel()

    try:

        await task

    except asyncio.CancelledError:
        pass

    except Exception:

        log.exception(
            "Worker shutdown error | %s",
            key,
        )


async def ensure_workers() -> None:

    wanted: set[str] = set()

    if db["settings"].get(
        "enabled",
        True,
    ):

        for sid, route in list(
            db["routes"].items()
        ):

            sid = str(sid)

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

                if not isinstance(
                    destination,
                    dict,
                ):
                    continue

                did = destination.get(
                    "id"
                )

                if did is None:
                    continue

                did = str(did)

                key = route_key(
                    sid,
                    did,
                )

                wanted.add(key)

                await start_worker(
                    sid,
                    did,
                )

    # Stop workers that are no longer needed.
    for key in list(
        worker_tasks.keys()
    ):

        if key in wanted:
            continue

        sid, did = (
            split_route_key(key)
        )

        await stop_worker(
            sid,
            did,
        )


# ============================================================
# ALBUM FLUSH
# ============================================================


async def flush_album(
    sid: str,
    album_id: str,
) -> None:

    key = (
        str(sid),
        str(album_id),
    )

    try:

        try:
            wait = float(
                db["settings"].get(
                    "album_wait",
                    1.5,
                )
            )
        except Exception:
            wait = 1.5

        await asyncio.sleep(
            max(
                0.2,
                min(
                    10.0,
                    wait,
                ),
            )
        )

        mids = album_buffer.pop(
            key,
            set(),
        )

        if not mids:
            return

        ordered = sorted(
            int(mid)
            for mid in mids
        )

        added = await add_playlist_item(
            sid,
            ordered,
            album=True,
        )

        if added:

            db["stats"][
                "albums"
            ] += 1

            db["stats"][
                "received"
            ] += len(ordered)

            db["stats"][
                "last_received"
            ] = int(
                time.time()
            )

            await save_db()

            wake_source(
                sid
            )

    except asyncio.CancelledError:
        raise

    except Exception as exc:

        db["stats"][
            "last_error"
        ] = str(exc)

        log.exception(
            "Album flush failed."
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
async def channel_post(
    message: Message,
) -> None:

    sid = str(
        message.chat.id
    )

    mid = int(
        message.message_id
    )

    log.info(
        "NEW CHANNEL POST | source=%s | message=%s | type=%s",
        sid,
        mid,
        message.content_type,
    )

    # Only configured sources.
    if sid not in source_ids():
        return

    # --------------------------------------------------------
    # ALBUM
    # --------------------------------------------------------

    album_id = message.media_group_id

    if album_id:

        key = (
            sid,
            str(album_id),
        )

        album_buffer.setdefault(
            key,
            set(),
        ).add(mid)

        old_task = album_tasks.get(
            key
        )

        if old_task is not None:

            old_task.cancel()

        album_tasks[key] = (
            asyncio.create_task(
                flush_album(
                    sid,
                    str(album_id),
                )
            )
        )

        return

    # --------------------------------------------------------
    # NORMAL POST
    # --------------------------------------------------------

    added = await add_playlist_item(
        sid,
        [mid],
        album=False,
    )

    if not added:
        return

    db["stats"][
        "received"
    ] += 1

    db["stats"][
        "last_received"
    ] = int(
        time.time()
    )

    await save_db()

    # Wake all destinations for this source.
    wake_source(
        sid
    )


# ============================================================
# /START
# ============================================================


@router.message(
    CommandStart()
)
async def start_cmd(
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
        reply_markup=home_kb(),
    )


# ============================================================
# /ADMIN
# ============================================================


@router.message(
    Command("admin")
)
async def admin_cmd(
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
        reply_markup=home_kb(),
    )


# ============================================================
# /CANCEL
# ============================================================


@router.message(
    Command("cancel")
)
async def cancel_cmd(
    message: Message,
    state: FSMContext,
) -> None:

    await state.clear()

    if is_admin(message):

        await message.answer(
            "❌ Cancelled.",
            reply_markup=home_kb(),
        )


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
        if settings.get(
            "enabled",
            True,
        )
        else "🔴 STOPPED"
    )

    repeat = (
        "🟢 ON"
        if settings.get(
            "repeat_enabled",
            True,
        )
        else "🔴 OFF"
    )

    routes_count = 0

    for source in db[
        "sources"
    ]:

        sid = str(
            source["id"]
        )

        if route_dests(sid):
            routes_count += 1

    playlist_count = sum(
        playlist_length(
            str(source["id"])
        )
        for source in db["sources"]
    )

    return (
        "🤖 <b>Route Repeat Copy Bot</b>\n\n"
        f"Status: <b>{running}</b>\n"
        f"Repeat: <b>{repeat}</b>\n\n"
        f"📥 Sources: <b>{len(db['sources'])}</b>\n"
        f"📤 Destinations: <b>{len(db['destinations'])}</b>\n"
        f"🔀 Routes: <b>{routes_count}</b>\n"
        f"📚 Playlist items: <b>{playlist_count}</b>\n\n"
        f"⏱ Interval: <b>{settings['interval']}s</b>\n"
        f"📦 Album wait: <b>{settings['album_wait']}s</b>\n"
        f"🔁 Retries: <b>{settings['retries']}</b>\n"
        f"📚 Max playlist: <b>{settings['max_playlist']}</b>\n\n"
        f"📥 Received: <b>{stats['received']}</b>\n"
        f"📤 Sent: <b>{stats['sent']}</b>\n"
        f"❌ Failed: <b>{stats['failed']}</b>\n"
        f"🔄 Cycles: <b>{stats['cycles']}</b>"
    )


def home_kb() -> InlineKeyboardMarkup:

    running = bool(
        db["settings"].get(
            "enabled",
            True,
        )
    )

    repeat = bool(
        db["settings"].get(
            "repeat_enabled",
            True,
        )
    )

    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text=(
                        "⏹ Stop"
                        if running
                        else "▶️ Start"
                    ),
                    callback_data="toggle",
                )
            ],
            [
                InlineKeyboardButton(
                    text=(
                        "🔁 Repeat: ON"
                        if repeat
                        else "🔁 Repeat: OFF"
                    ),
                    callback_data="repeat",
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
                    text="📚 Playlists",
                    callback_data="playlists",
                ),
                InlineKeyboardButton(
                    text="⚙️ Settings",
                    callback_data="settings",
                ),
            ],
            [
                InlineKeyboardButton(
                    text="📊 Status",
                    callback_data="status",
                ),
                InlineKeyboardButton(
                    text="🔄 Refresh",
                    callback_data="home",
                ),
            ],
        ]
    )


# ============================================================
# SOURCE KEYBOARD
# ============================================================


def sources_kb() -> InlineKeyboardMarkup:

    rows = []

    for index, source in enumerate(
        db["sources"]
    ):

        sid = str(
            source["id"]
        )

        rows.append(
            [
                InlineKeyboardButton(
                    text=(
                        f"🔀 {name_of(source)} "
                        f"[{len(route_dests(sid))}] "
                        f"[📚 {playlist_length(sid)}]"
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


# ============================================================
# ROUTE KEYBOARD
# ============================================================


def route_kb(
    sid: str,
) -> InlineKeyboardMarkup:

    selected = route_dest_ids(
        sid
    )

    rows = []

    for index, destination in enumerate(
        db["destinations"]
    ):

        did = str(
            destination["id"]
        )

        rows.append(
            [
                InlineKeyboardButton(
                    text=(
                        "✅ "
                        if did in selected
                        else "⬜ "
                    )
                    + name_of(destination),
                    callback_data=(
                        f"route_dest:{sid}:{index}"
                    ),
                )
            ]
        )

    rows.append(
        [
            InlineKeyboardButton(
                text="➕ Add Destination",
                callback_data=(
                    f"new_dest_for:{sid}"
                ),
            )
        ]
    )

    rows.append(
        [
            InlineKeyboardButton(
                text="📚 Playlist",
                callback_data=(
                    f"playlist:{sid}"
                ),
            )
        ]
    )

    rows.append(
        [
            InlineKeyboardButton(
                text="❌ Remove Source",
                callback_data=(
                    f"remove_source:{sid}"
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


# ============================================================
# DESTINATION KEYBOARD
# ============================================================


def dest_kb() -> InlineKeyboardMarkup:

    rows = []

    for index, destination in enumerate(
        db["destinations"]
    ):

        did = str(
            destination["id"]
        )

        used = sum(
            1
            for source in db["sources"]
            if did
            in route_dest_ids(
                str(source["id"])
            )
        )

        rows.append(
            [
                InlineKeyboardButton(
                    text=(
                        f"❌ {name_of(destination)} "
                        f"[{used} routes]"
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
                callback_data=(
                    "add_destination"
                ),
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
# PLAYLIST KEYBOARD
# ============================================================


def playlist_kb(
    sid: str,
) -> InlineKeyboardMarkup:

    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="⏮ Reset ALL Route Positions",
                    callback_data=(
                        f"reset_playlist:{sid}"
                    ),
                )
            ],
            [
                InlineKeyboardButton(
                    text="🗑 Clear Playlist",
                    callback_data=(
                        f"clear_playlist:{sid}"
                    ),
                )
            ],
            [
                InlineKeyboardButton(
                    text="⬅️ Route",
                    callback_data=(
                        f"route_by_id:{sid}"
                    ),
                ),
            ],
        ]
    )


# ============================================================
# SETTINGS KEYBOARD
# ============================================================


def settings_kb() -> InlineKeyboardMarkup:

    s = db[
        "settings"
    ]

    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text=(
                        f"⏱ Interval: "
                        f"{s['interval']}s"
                    ),
                    callback_data=(
                        "set:interval"
                    ),
                )
            ],
            [
                InlineKeyboardButton(
                    text=(
                        f"📦 Album Wait: "
                        f"{s['album_wait']}s"
                    ),
                    callback_data=(
                        "set:album"
                    ),
                )
            ],
            [
                InlineKeyboardButton(
                    text=(
                        f"🔁 Retries: "
                        f"{s['retries']}"
                    ),
                    callback_data=(
                        "set:retries"
                    ),
                )
            ],
            [
                InlineKeyboardButton(
                    text=(
                        f"📚 Max Playlist: "
                        f"{s['max_playlist']}"
                    ),
                    callback_data=(
                        "set:playlist"
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
# CALLBACK HANDLER
# ============================================================


@router.callback_query()
async def callbacks(
    q: CallbackQuery,
    state: FSMContext,
) -> None:

    if not is_admin(q):

        await q.answer(
            "Access denied.",
            show_alert=True,
        )

        return

    if not q.message:

        await q.answer()
        return

    data = q.data or ""

    # --------------------------------------------------------
    # HOME
    # --------------------------------------------------------

    if data == "home":

        await state.clear()
        await q.answer()

        await q.message.edit_text(
            home_text(),
            reply_markup=home_kb(),
        )

        return

    # --------------------------------------------------------
    # START / STOP
    # --------------------------------------------------------

    if data == "toggle":

        db["settings"][
            "enabled"
        ] = not bool(
            db["settings"].get(
                "enabled",
                True,
            )
        )

        await save_db()

        await ensure_workers()

        if db["settings"][
            "enabled"
        ]:
            wake_all_workers()

        await q.answer(
            (
                "▶️ Bot started."
                if db["settings"]["enabled"]
                else "⏹ Bot stopped."
            )
        )

        await q.message.edit_text(
            home_text(),
            reply_markup=home_kb(),
        )

        return

    # --------------------------------------------------------
    # REPEAT
    # --------------------------------------------------------

    if data == "repeat":

        db["settings"][
            "repeat_enabled"
        ] = not bool(
            db["settings"].get(
                "repeat_enabled",
                True,
            )
        )

        await save_db()

        wake_all_workers()

        await q.answer(
            (
                "🔁 Repeat mode ON."
                if db["settings"][
                    "repeat_enabled"
                ]
                else "⏸ Repeat mode OFF."
            )
        )

        await q.message.edit_text(
            home_text(),
            reply_markup=home_kb(),
        )

        return

    # --------------------------------------------------------
    # SOURCES
    # --------------------------------------------------------

    if data == "sources":

        await q.answer()

        lines = [
            "🔀 <b>Source → Destination Routes</b>",
            "",
        ]

        if not db["sources"]:

            lines.append(
                "No source channels configured."
            )

        for index, source in enumerate(
            db["sources"],
            1,
        ):

            sid = str(
                source["id"]
            )

            lines.append(
                f"{index}. <b>{name_of(source)}</b>"
            )

            lines.append(
                f"   📚 Playlist: "
                f"<b>{playlist_length(sid)}</b>"
            )

            destinations = route_dests(
                sid
            )

            if destinations:

                for destination in destinations:

                    lines.append(
                        "   └ "
                        + name_of(destination)
                    )

            else:

                lines.append(
                    "   └ ⚠️ No destination"
                )

            lines.append("")

        await q.message.edit_text(
            "\n".join(lines),
            reply_markup=sources_kb(),
        )

        return

    # --------------------------------------------------------
    # SOURCE SELECT
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

        except (
            ValueError,
            IndexError,
        ):

            await q.answer(
                "Invalid source.",
                show_alert=True,
            )

            return

        sid = str(
            source["id"]
        )

        await q.answer()

        await q.message.edit_text(
            (
                "🔀 <b>Route Configuration</b>\n\n"
                f"Source: <b>{name_of(source)}</b>\n"
                f"ID: <code>{sid}</code>\n\n"
                f"📚 Playlist: "
                f"<b>{playlist_length(sid)}</b>\n"
                f"📤 Destinations: "
                f"<b>{len(route_dest_ids(sid))}</b>"
            ),
            reply_markup=route_kb(
                sid
            ),
        )

        return

    # --------------------------------------------------------
    # ROUTE BY ID
    # --------------------------------------------------------

    if data.startswith(
        "route_by_id:"
    ):

        sid = data.split(
            ":",
            1,
        )[1]

        source = get_source(
            sid
        )

        if source is None:

            await q.answer(
                "Source not found.",
                show_alert=True,
            )

            return

        await q.answer()

        await q.message.edit_text(
            (
                "🔀 <b>Route Configuration</b>\n\n"
                f"Source: <b>{name_of(source)}</b>\n"
                f"📚 Playlist: "
                f"<b>{playlist_length(sid)}</b>\n"
                f"📤 Destinations: "
                f"<b>{len(route_dest_ids(sid))}</b>"
            ),
            reply_markup=route_kb(
                sid
            ),
        )

        return

    # --------------------------------------------------------
    # ROUTE DESTINATION TOGGLE
    # --------------------------------------------------------

    if data.startswith(
        "route_dest:"
    ):

        try:

            _, sid, index_raw = (
                data.split(
                    ":",
                    2,
                )
            )

            index = int(
                index_raw
            )

            destination = db[
                "destinations"
            ][index]

        except (
            ValueError,
            IndexError,
        ):

            await q.answer(
                "Invalid destination.",
                show_alert=True,
            )

            return

        if sid not in source_ids():

            await q.answer(
                "Source not found.",
                show_alert=True,
            )

            return

        did = str(
            destination["id"]
        )

        route = db[
            "routes"
        ].setdefault(
            sid,
            {
                "source": get_source(
                    sid
                ),
                "destinations": [],
            },
        )

        current_ids = {
            str(item["id"])
            for item in route.get(
                "destinations",
                [],
            )
            if isinstance(
                item,
                dict,
            )
            and item.get("id")
            is not None
        }

        if did in current_ids:

            route[
                "destinations"
            ] = [
                item
                for item in route[
                    "destinations"
                ]
                if str(
                    item["id"]
                ) != did
            ]

            # Stop ONLY this route.
            await stop_worker(
                sid,
                did,
            )

            result = "removed"

        else:

            route[
                "destinations"
            ].append(
                destination
            )

            result = "added"

        await save_db()

        await ensure_workers()

        if result == "added":
            wake_route(
                sid,
                did,
            )

        await q.answer(
            f"{name_of(destination)} {result}"
        )

        await q.message.edit_reply_markup(
            reply_markup=route_kb(
                sid
            )
        )

        return

    # --------------------------------------------------------
    # DESTINATIONS
    # --------------------------------------------------------

    if data == "destinations":

        await q.answer()

        lines = [
            "📤 <b>Destinations</b>",
            "",
        ]

        if not db[
            "destinations"
        ]:

            lines.append(
                "No destinations configured."
            )

        for index, destination in enumerate(
            db["destinations"],
            1,
        ):

            did = str(
                destination["id"]
            )

            used = sum(
                1
                for source in db[
                    "sources"
                ]
                if did
                in route_dest_ids(
                    str(source["id"])
                )
            )

            lines.extend(
                [
                    f"{index}. <b>{name_of(destination)}</b>",
                    f"Used by: <b>{used}</b> route(s)",
                    f"ID: <code>{did}</code>",
                    "",
                ]
            )

        await q.message.edit_text(
            "\n".join(lines),
            reply_markup=dest_kb(),
        )

        return

    # --------------------------------------------------------
    # ADD SOURCE
    # --------------------------------------------------------

    if data == "add_source":

        await q.answer()

        await state.set_state(
            AddSource.value
        )

        await q.message.edit_text(
            (
                "📥 <b>Add Source Channel</b>\n\n"
                "Send @username or numeric channel ID.\n\n"
                "<code>@channelusername</code>\n"
                "<code>-1001234567890</code>\n\n"
                "⚠️ Bot must be administrator in the source."
            )
        )

        return

    # --------------------------------------------------------
    # ADD DESTINATION
    # --------------------------------------------------------

    if data == "add_destination":

        await q.answer()

        await state.update_data(
            route_source_id=None
        )

        await state.set_state(
            AddDestination.value
        )

        await q.message.edit_text(
            (
                "📤 <b>Add Destination</b>\n\n"
                "Send @username or numeric channel/group ID.\n\n"
                "⚠️ Bot must be administrator in destination."
            )
        )

        return

    # --------------------------------------------------------
    # ADD DESTINATION FOR ROUTE
    # --------------------------------------------------------

    if data.startswith(
        "new_dest_for:"
    ):

        sid = data.split(
            ":",
            1,
        )[1]

        if sid not in source_ids():

            await q.answer(
                "Source not found.",
                show_alert=True,
            )

            return

        await q.answer()

        await state.update_data(
            route_source_id=sid
        )

        await state.set_state(
            AddDestination.value
        )

        await q.message.edit_text(
            (
                "📤 <b>Add Destination to Route</b>\n\n"
                "Send @username or numeric channel/group ID.\n\n"
                "⚠️ Bot must be administrator."
            )
        )

        return

    # --------------------------------------------------------
    # REMOVE SOURCE
    # --------------------------------------------------------

    if data.startswith(
        "remove_source:"
    ):

        sid = data.split(
            ":",
            1,
        )[1]

        if sid not in source_ids():

            await q.answer(
                "Source not found.",
                show_alert=True,
            )

            return

        # Stop all workers of this source.
        for key in list(
            worker_tasks
        ):

            rsid, rdid = (
                split_route_key(key)
            )

            if rsid == sid:

                await stop_worker(
                    rsid,
                    rdid,
                )

        db["sources"] = [
            source
            for source in db[
                "sources"
            ]
            if str(
                source["id"]
            ) != sid
        ]

        db["routes"].pop(
            sid,
            None,
        )

        db["playlists"].pop(
            sid,
            None,
        )

        # Remove route positions.
        for key in list(
            db["positions"]
        ):

            rsid, _ = (
                split_route_key(key)
            )

            if rsid == sid:
                db["positions"].pop(
                    key,
                    None,
                )

        await save_db()

        await q.answer(
            "Source removed."
        )

        await q.message.edit_text(
            "🔀 <b>Source → Destination Routes</b>",
            reply_markup=sources_kb(),
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
                data.split(
                    ":",
                    1,
                )[1]
            )

            destination = db[
                "destinations"
            ][index]

        except (
            ValueError,
            IndexError,
        ):

            await q.answer(
                "Invalid destination.",
                show_alert=True,
            )

            return

        did = str(
            destination["id"]
        )

        # Stop route workers using it.
        for key in list(
            worker_tasks
        ):

            sid, route_did = (
                split_route_key(key)
            )

            if route_did == did:

                await stop_worker(
                    sid,
                    route_did,
                )

        # Remove from all routes.
        for route in db[
            "routes"
        ].values():

            if not isinstance(
                route,
                dict,
            ):
                continue

            route[
                "destinations"
            ] = [
                item
                for item in route.get(
                    "destinations",
                    [],
                )
                if str(
                    item.get("id")
                ) != did
            ]

        db["destinations"] = [
            item
            for item in db[
                "destinations"
            ]
            if str(
                item["id"]
            ) != did
        ]

        # Remove positions.
        for key in list(
            db["positions"]
        ):

            _, route_did = (
                split_route_key(key)
            )

            if route_did == did:

                db["positions"].pop(
                    key,
                    None,
                )

        await save_db()

        await ensure_workers()

        await q.answer(
            "Destination removed."
        )

        await q.message.edit_text(
            "📤 <b>Destinations</b>",
            reply_markup=dest_kb(),
        )

        return

    # --------------------------------------------------------
    # PLAYLISTS
    # --------------------------------------------------------

    if data == "playlists":

        await q.answer()

        lines = [
            "📚 <b>Source Playlists</b>",
            "",
        ]

        if not db["sources"]:

            lines.append(
                "No sources configured."
            )

        for index, source in enumerate(
            db["sources"],
            1,
        ):

            sid = str(
                source["id"]
            )

            length = playlist_length(
                sid
            )

            lines.append(
                f"{index}. <b>{name_of(source)}</b>"
            )

            lines.append(
                f"   Items: <b>{length}</b>"
            )

            # Show positions for every route.
            for destination in route_dests(
                sid
            ):

                did = str(
                    destination["id"]
                )

                key = route_key(
                    sid,
                    did,
                )

                position = int(
                    db["positions"].get(
                        key,
                        0,
                    )
                )

                next_number = (
                    position + 1
                    if length
                    else 0
                )

                if next_number > length:
                    next_number = 1

                lines.append(
                    f"   → {name_of(destination)}: "
                    f"<b>next #{next_number}</b>"
                )

            lines.append("")

        rows = []

        for source in db[
            "sources"
        ]:

            rows.append(
                [
                    InlineKeyboardButton(
                        text=(
                            f"📚 "
                            f"{name_of(source)}"
                        ),
                        callback_data=(
                            f"playlist:"
                            f"{source['id']}"
                        ),
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

        await q.message.edit_text(
            "\n".join(lines),
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=rows
            ),
        )

        return

    # --------------------------------------------------------
    # SINGLE PLAYLIST
    # --------------------------------------------------------

    if data.startswith(
        "playlist:"
    ):

        sid = data.split(
            ":",
            1,
        )[1]

        source = get_source(
            sid
        )

        if source is None:

            await q.answer(
                "Source not found.",
                show_alert=True,
            )

            return

        playlist = get_playlist(
            sid
        )

        text = (
            "📚 <b>Playlist</b>\n\n"
            f"Source: <b>{name_of(source)}</b>\n"
            f"Items: <b>{len(playlist)}</b>\n\n"
        )

        if playlist:

            for index, item in enumerate(
                playlist[:30],
                1,
            ):

                ids = item.get(
                    "ids",
                    [],
                )

                if not isinstance(
                    ids,
                    list,
                ):
                    ids = []

                text += (
                    f"{index}. Message "
                    f"<code>"
                    f"{ids[0] if ids else '?'}"
                    f"</code>"
                )

                if len(ids) > 1:

                    text += (
                        f" "
                        f"(album: {len(ids)} items)"
                    )

                text += "\n"

            if len(playlist) > 30:

                text += (
                    f"\n... and "
                    f"{len(playlist) - 30}"
                    f" more"
                )

        else:

            text += (
                "Playlist is empty.\n\n"
                "New posts from this source will "
                "automatically be added."
            )

        await q.answer()

        await q.message.edit_text(
            text,
            reply_markup=playlist_kb(
                sid
            ),
        )

        return

    # --------------------------------------------------------
    # RESET PLAYLIST POSITION
    # --------------------------------------------------------

    if data.startswith(
        "reset_playlist:"
    ):

        sid = data.split(
            ":",
            1,
        )[1]

        if sid not in source_ids():

            await q.answer(
                "Source not found.",
                show_alert=True,
            )

            return

        # Reset every destination independently.
        for destination in route_dests(
            sid
        ):

            did = str(
                destination["id"]
            )

            db["positions"][
                route_key(
                    sid,
                    did,
                )
            ] = 0

        await save_db()

        wake_source(
            sid
        )

        await q.answer(
            "⏮ All route positions reset to First."
        )

        return

    # --------------------------------------------------------
    # CLEAR PLAYLIST
    # --------------------------------------------------------

    if data.startswith(
        "clear_playlist:"
    ):

        sid = data.split(
            ":",
            1,
        )[1]

        if sid not in source_ids():

            await q.answer(
                "Source not found.",
                show_alert=True,
            )

            return

        db["playlists"].pop(
            sid,
            None
        )

        # Reset all route positions.
        for destination in route_dests(
            sid
        ):

            did = str(
                destination["id"]
            )

            db["positions"][
                route_key(
                    sid,
                    did,
                )
            ] = 0

        await save_db()

        await q.answer(
            "🗑 Playlist cleared."
        )

        return

    # --------------------------------------------------------
    # SETTINGS
    # --------------------------------------------------------

    if data == "settings":

        await q.answer()

        await q.message.edit_text(
            "⚙️ <b>Settings</b>",
            reply_markup=settings_kb(),
        )

        return

    # --------------------------------------------------------
    # STATUS
    # --------------------------------------------------------

    if data == "status":

        await q.answer()

        stats = db[
            "stats"
        ]

        workers = []

        for key, task in list(
            worker_tasks.items()
        ):

            sid, did = (
                split_route_key(key)
            )

            source = get_source(
                sid
            )

            destination = (
                get_destination(did)
            )

            status = (
                "🟢 RUNNING"
                if not task.done()
                else "🔴 STOPPED"
            )

            workers.append(
                (
                    f"• {name_of(source)}"
                    f" → "
                    f"{name_of(destination)}"
                    f": {status}"
                )
            )

        text = (
            "📊 <b>Bot Status</b>\n\n"
            f"Running: <b>"
            f"{db['settings']['enabled']}"
            f"</b>\n"
            f"Repeat: <b>"
            f"{db['settings']['repeat_enabled']}"
            f"</b>\n\n"
            f"Workers: <b>{len(worker_tasks)}</b>\n"
            f"📥 Received: <b>{stats['received']}</b>\n"
            f"📤 Sent: <b>{stats['sent']}</b>\n"
            f"❌ Failed: <b>{stats['failed']}</b>\n"
            f"🔄 Cycles: <b>{stats['cycles']}</b>\n\n"
            "<b>Workers</b>\n"
            + (
                "\n".join(workers)
                if workers
                else "-"
            )
            + "\n\n"
            "<b>Last error</b>\n"
            f"<code>"
            f"{str(stats['last_error'] or '-')[:1500]}"
            f"</code>"
        )

        await q.message.edit_text(
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
    # SETTINGS INPUT
    # --------------------------------------------------------

    if data.startswith(
        "set:"
    ):

        kind = data.split(
            ":",
            1,
        )[1]

        if kind == "interval":

            await q.answer()

            await state.set_state(
                SetInterval.value
            )

            await q.message.edit_text(
                (
                    "⏱ <b>Interval</b>\n\n"
                    "Enter seconds.\n\n"
                    "Example:\n"
                    "<code>30</code>\n\n"
                    "Minimum: 0.1\n"
                    "Maximum: 3600"
                )
            )

            return

        if kind == "album":

            await q.answer()

            await state.set_state(
                SetAlbumWait.value
            )

            await q.message.edit_text(
                (
                    "📦 <b>Album Wait</b>\n\n"
                    "Enter seconds.\n\n"
                    "Example:\n"
                    "<code>1.5</code>\n\n"
                    "Minimum: 0.2\n"
                    "Maximum: 10"
                )
            )

            return

        if kind == "retries":

            await q.answer()

            await state.set_state(
                SetRetries.value
            )

            await q.message.edit_text(
                (
                    "🔁 <b>Retries</b>\n\n"
                    "Enter an integer from 0 to 20."
                )
            )

            return

        if kind == "playlist":

            await q.answer()

            await state.set_state(
                SetPlaylistLimit.value
            )

            await q.message.edit_text(
                (
                    "📚 <b>Max Playlist</b>\n\n"
                    "Enter a number from 100 to 50000."
                )
            )

            return

    await q.answer()


# ============================================================
# ADD SOURCE
# ============================================================


@router.message(
    AddSource.value
)
async def add_source(
    message: Message,
    state: FSMContext,
) -> None:

    if not is_admin(message):
        return

    value = (
        message.text or ""
    ).strip()

    try:

        source = await resolve_source(
            value
        )

        sid = str(
            source["id"]
        )

        if sid in source_ids():

            await message.answer(
                "⚠️ Source already exists.",
                reply_markup=sources_kb(),
            )

            return

        db["sources"].append(
            source
        )

        db["routes"][sid] = {
            "source": source,
            "destinations": [],
        }

        db["playlists"].setdefault(
            sid,
            [],
        )

        await save_db()

        await message.answer(
            (
                "✅ <b>Source Added</b>\n\n"
                f"Source: <b>{name_of(source)}</b>\n"
                f"ID: <code>{sid}</code>\n\n"
                "📡 New posts will be saved to its playlist.\n"
                "🔁 Repeat mode will cycle the playlist."
            ),
            reply_markup=route_kb(
                sid
            ),
        )

    except Exception as exc:

        log.exception(
            "Add source failed."
        )

        await message.answer(
            (
                "❌ <b>Could not add source</b>\n\n"
                f"<code>{str(exc)[:1500]}</code>"
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
async def add_destination(
    message: Message,
    state: FSMContext,
) -> None:

    if not is_admin(message):
        return

    data = await state.get_data()

    route_sid = data.get(
        "route_source_id"
    )

    value = (
        message.text or ""
    ).strip()

    try:

        destination = (
            await resolve_destination(
                value
            )
        )

        did = str(
            destination["id"]
        )

        existing_destination = (
            get_destination(did)
        )

        if existing_destination:

            destination = (
                existing_destination
            )

        else:

            db[
                "destinations"
            ].append(
                destination
            )

        # Add directly to route if requested.
        if (
            route_sid
            and route_sid in source_ids()
        ):

            route = db[
                "routes"
            ].setdefault(
                route_sid,
                {
                    "source": get_source(
                        route_sid
                    ),
                    "destinations": [],
                },
            )

            existing = {
                str(item["id"])
                for item in route.get(
                    "destinations",
                    [],
                )
                if isinstance(
                    item,
                    dict,
                )
                and item.get("id")
                is not None
            }

            if did not in existing:

                route[
                    "destinations"
                ].append(
                    destination
                )

            await save_db()

            await ensure_workers()

            wake_route(
                route_sid,
                did,
            )

            await message.answer(
                (
                    "✅ <b>Destination Added</b>\n\n"
                    f"Source: <b>"
                    f"{name_of(get_source(route_sid))}"
                    f"</b>\n"
                    f"Destination: <b>"
                    f"{name_of(destination)}"
                    f"</b>\n\n"
                    "🚀 Route worker started."
                ),
                reply_markup=route_kb(
                    route_sid
                ),
            )

        else:

            await save_db()

            await message.answer(
                (
                    "✅ <b>Destination Added</b>\n\n"
                    f"<b>{name_of(destination)}</b>\n\n"
                    "Open Routes and select the source(s) "
                    "where you want to use it."
                ),
                reply_markup=dest_kb(),
            )

    except Exception as exc:

        log.exception(
            "Add destination failed."
        )

        await message.answer(
            (
                "❌ <b>Could not add destination</b>\n\n"
                f"<code>{str(exc)[:1500]}</code>"
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
async def set_interval(
    message: Message,
    state: FSMContext,
) -> None:

    try:

        value = float(
            (
                message.text
                or ""
            ).strip()
        )

        if not 0.1 <= value <= 3600:
            raise ValueError

        db["settings"][
            "interval"
        ] = value

        await save_db()

        wake_all_workers()

        await message.answer(
            (
                f"✅ Interval set to "
                f"<b>{value}s</b>."
            ),
            reply_markup=settings_kb(),
        )

    except ValueError:

        await message.answer(
            "❌ Enter a value between 0.1 and 3600."
        )

    finally:

        await state.clear()


# ============================================================
# SET ALBUM WAIT
# ============================================================


@router.message(
    SetAlbumWait.value
)
async def set_album_wait(
    message: Message,
    state: FSMContext,
) -> None:

    try:

        value = float(
            (
                message.text
                or ""
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
            reply_markup=settings_kb(),
        )

    except ValueError:

        await message.answer(
            "❌ Enter a value between 0.2 and 10."
        )

    finally:

        await state.clear()


# ============================================================
# SET RETRIES
# ============================================================


@router.message(
    SetRetries.value
)
async def set_retries(
    message: Message,
    state: FSMContext,
) -> None:

    try:

        value = int(
            (
                message.text
                or ""
            ).strip()
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
# SET PLAYLIST LIMIT
# ============================================================


@router.message(
    SetPlaylistLimit.value
)
async def set_playlist_limit(
    message: Message,
    state: FSMContext,
) -> None:

    try:

        value = int(
            (
                message.text
                or ""
            ).strip()
        )

        if not 100 <= value <= 50000:
            raise ValueError

        db["settings"][
            "max_playlist"
        ] = value

        # Trim existing playlists immediately.
        for sid in list(
            db["playlists"]
        ):

            playlist = db[
                "playlists"
            ].get(
                sid,
                [],
            )

            if len(playlist) <= value:
                continue

            remove_count = (
                len(playlist)
                - value
            )

            del playlist[
                :remove_count
            ]

            for key in list(
                db["positions"]
            ):

                route_sid, _ = (
                    split_route_key(key)
                )

                if route_sid != str(sid):
                    continue

                try:
                    position = int(
                        db["positions"][
                            key
                        ]
                    )
                except Exception:
                    position = 0

                db["positions"][
                    key
                ] = max(
                    0,
                    position
                    - remove_count,
                )

        await save_db()

        wake_all_workers()

        await message.answer(
            (
                "✅ Max playlist size: "
                f"<b>{value}</b>"
            ),
            reply_markup=settings_kb(),
        )

    except ValueError:

        await message.answer(
            "❌ Enter an integer from 100 to 50000."
        )

    finally:

        await state.clear()


# ============================================================
# HEALTH
# ============================================================


async def health(
    request: web.Request,
) -> web.Response:

    playlist_items = sum(
        playlist_length(
            str(source["id"])
        )
        for source in db[
            "sources"
        ]
    )

    return web.json_response(
        {
            "ok": True,
            "mode": "webhook",
            "running": bool(
                db["settings"].get(
                    "enabled",
                    True,
                )
            ),
            "repeat": bool(
                db["settings"].get(
                    "repeat_enabled",
                    True,
                )
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
            "playlist_items": playlist_items,
            "received": db[
                "stats"
            ]["received"],
            "sent": db[
                "stats"
            ]["sent"],
            "failed": db[
                "stats"
            ]["failed"],
            "cycles": db[
                "stats"
            ]["cycles"],
            "timestamp": int(
                time.time()
            ),
        }
    )


# ============================================================
# WEBHOOK
# ============================================================


async def webhook(
    request: web.Request,
) -> web.Response:

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

        return web.Response(
            status=403,
            text="Forbidden",
        )

    try:

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

        if bot is None or dp is None:

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
            "Webhook processing error: %s",
            exc,
        )

        return web.Response(
            status=500,
            text="ERROR",
        )


# ============================================================
# HTTP SERVER
# ============================================================


async def start_server() -> None:

    global server_runner

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

    app.router.add_post(
        WEBHOOK_PATH,
        webhook,
    )

    server_runner = (
        web.AppRunner(app)
    )

    await server_runner.setup()

    site = web.TCPSite(
        server_runner,
        "0.0.0.0",
        PORT,
    )

    await site.start()

    log.info(
        "HTTP SERVER READY | port=%s",
        PORT,
    )


# ============================================================
# WEBHOOK SETUP
# ============================================================


async def setup_webhook() -> None:

    if bot is None:
        return

    webhook_url = (
        RENDER_URL
        + WEBHOOK_PATH
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
        "WEBHOOK URL: %s",
        info.url,
    )

    log.info(
        "WEBHOOK PENDING: %s",
        info.pending_update_count,
    )

    if info.last_error_message:

        log.error(
            "TELEGRAM WEBHOOK ERROR: %s",
            info.last_error_message,
        )


# ============================================================
# SHUTDOWN
# ============================================================


async def shutdown() -> None:

    log.info(
        "Starting shutdown..."
    )

    # --------------------------------------------------------
    # Album tasks
    # --------------------------------------------------------

    for task in list(
        album_tasks.values()
    ):

        if not task.done():
            task.cancel()

    for task in list(
        album_tasks.values()
    ):

        try:
            await task

        except asyncio.CancelledError:
            pass

        except Exception:
            log.exception(
                "Album task shutdown error."
            )

    album_tasks.clear()
    album_buffer.clear()

    # --------------------------------------------------------
    # Workers
    # --------------------------------------------------------

    for key in list(
        worker_tasks
    ):

        sid, did = (
            split_route_key(key)
        )

        try:

            await stop_worker(
                sid,
                did,
            )

        except Exception:

            log.exception(
                "Could not stop worker %s",
                key,
            )

    # --------------------------------------------------------
    # Save final state
    # --------------------------------------------------------

    try:
        await save_db()
    except Exception:
        log.exception(
            "Final database save failed."
        )

    # --------------------------------------------------------
    # HTTP
    # --------------------------------------------------------

    if server_runner:

        try:
            await server_runner.cleanup()
        except Exception:
            log.exception(
                "HTTP server cleanup failed."
            )

    # --------------------------------------------------------
    # Bot session
    # --------------------------------------------------------

    if bot:

        try:
            await bot.session.close()
        except Exception:
            pass

    log.info(
        "Shutdown complete."
    )


# ============================================================
# MAIN
# ============================================================


async def main() -> None:

    global bot
    global dp

    # --------------------------------------------------------
    # BOT
    # --------------------------------------------------------

    bot = Bot(
        token=BOT_TOKEN,
        default=DefaultBotProperties(
            parse_mode=ParseMode.HTML
        ),
    )

    # --------------------------------------------------------
    # DISPATCHER
    # --------------------------------------------------------

    dp = Dispatcher()

    # IMPORTANT:
    # Router is now correctly defined above.
    dp.include_router(
        router
    )

    # --------------------------------------------------------
    # BOT CHECK
    # --------------------------------------------------------

    me = await bot.get_me()

    log.info(
        "BOT CONNECTED | @%s | id=%s",
        me.username,
        me.id,
    )

    # --------------------------------------------------------
    # HTTP SERVER
    # --------------------------------------------------------

    await start_server()

    # --------------------------------------------------------
    # WEBHOOK
    # --------------------------------------------------------

    await setup_webhook()

    # --------------------------------------------------------
    # WORKERS
    # --------------------------------------------------------

    await ensure_workers()

    # --------------------------------------------------------
    # READY
    # --------------------------------------------------------

    log.info(
        "=============================================="
    )

    log.info(
        "BOT READY"
    )

    log.info(
        "MODE: WEBHOOK"
    )

    log.info(
        "REPEAT: %s",
        db["settings"].get(
            "repeat_enabled",
            True,
        ),
    )

    log.info(
        "RUNNING: %s",
        db["settings"].get(
            "enabled",
            True,
        ),
    )

    log.info(
        "SOURCES: %s",
        len(db["sources"]),
    )

    log.info(
        "DESTINATIONS: %s",
        len(db["destinations"]),
    )

    log.info(
        "WORKERS: %s",
        len(worker_tasks),
    )

    log.info(
        "=============================================="
    )

    try:

        # Keep Render Web Service alive.
        await asyncio.Event().wait()

    finally:

        await shutdown()


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
