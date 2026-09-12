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

load_dotenv()

# ============================================================
# CONFIG
# ============================================================

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

if not WEBHOOK_SECRET:
    raise RuntimeError("WEBHOOK_SECRET is missing.")

if WEBHOOK_SECRET == BOT_TOKEN:
    raise RuntimeError("WEBHOOK_SECRET must NOT be the BOT_TOKEN.")

ADMIN_ID = int(ADMIN_ID_RAW)

WEBHOOK_PATH = f"/telegram/webhook/{WEBHOOK_SECRET}"

# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

log = logging.getLogger("repeat-route-bot")

# ============================================================
# DEFAULT DATABASE
# ============================================================

DEFAULT_SETTINGS = {
    "enabled": True,

    # Repeat mode.
    "repeat_enabled": True,

    # Seconds between copied posts.
    "interval": 1.0,

    # Retry count.
    "retries": 8,

    # Album grouping delay.
    "album_wait": 1.5,

    # Maximum stored playlist items per source.
    "max_playlist": 50000,
}


def fresh_db() -> dict[str, Any]:
    return {
        "settings": DEFAULT_SETTINGS.copy(),

        "sources": [],

        "destinations": [],

        # source_id -> {
        #   source: {...},
        #   destinations: [...]
        # }
        "routes": {},

        # source_id -> playlist
        #
        # Each playlist item:
        #
        # {
        #   "id": 123,
        #   "ids": [123],
        #   "album": false,
        #   "created": 1234567890
        # }
        #
        "playlists": {},

        # source_id -> current index
        "positions": {},

        "stats": {
            "received": 0,
            "enqueued": 0,
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
        raw = json.loads(DATA_FILE.read_text(encoding="utf-8"))

        if not isinstance(raw, dict):
            return fresh_db()

    except Exception:
        log.exception("Could not read data.json")
        return fresh_db()

    db = fresh_db()

    # --------------------------------------------------------
    # Settings
    # --------------------------------------------------------

    settings = raw.get("settings")

    if isinstance(settings, dict):
        db["settings"].update(settings)

    # --------------------------------------------------------
    # Sources
    # --------------------------------------------------------

    sources = raw.get("sources")

    if isinstance(sources, list):
        db["sources"] = [
            x for x in sources
            if isinstance(x, dict) and x.get("id") is not None
        ]

    # --------------------------------------------------------
    # Destinations
    # --------------------------------------------------------

    destinations = raw.get("destinations")

    if isinstance(destinations, list):
        db["destinations"] = [
            x for x in destinations
            if isinstance(x, dict) and x.get("id") is not None
        ]

    # --------------------------------------------------------
    # Routes
    # --------------------------------------------------------

    routes = raw.get("routes")

    if isinstance(routes, dict):
        db["routes"] = routes
    else:
        # Compatibility with previous version.
        for source in db["sources"]:
            sid = str(source["id"])

            db["routes"][sid] = {
                "source": source,
                "destinations": list(db["destinations"]),
            }

    # --------------------------------------------------------
    # Playlists
    # --------------------------------------------------------

    playlists = raw.get("playlists")

    if isinstance(playlists, dict):
        for sid, items in playlists.items():

            if not isinstance(items, list):
                continue

            cleaned: list[dict[str, Any]] = []

            for item in items:

                if isinstance(item, int):
                    cleaned.append({
                        "id": int(item),
                        "ids": [int(item)],
                        "album": False,
                        "created": 0,
                    })

                elif isinstance(item, dict):

                    ids = item.get("ids")

                    if not isinstance(ids, list):
                        ids = []

                    ids = [
                        int(x)
                        for x in ids
                        if str(x).lstrip("-").isdigit()
                    ]

                    if not ids and item.get("id") is not None:
                        try:
                            ids = [int(item["id"])]
                        except Exception:
                            ids = []

                    if ids:
                        cleaned.append({
                            "id": ids[0],
                            "ids": ids,
                            "album": bool(item.get("album", len(ids) > 1)),
                            "created": int(
                                item.get("created", 0) or 0
                            ),
                        })

            db["playlists"][str(sid)] = cleaned

    # --------------------------------------------------------
    # Positions
    # --------------------------------------------------------

    positions = raw.get("positions")

    if isinstance(positions, dict):

        for sid, value in positions.items():

            try:
                db["positions"][str(sid)] = int(value)
            except Exception:
                db["positions"][str(sid)] = 0

    # --------------------------------------------------------
    # Stats
    # --------------------------------------------------------

    stats = raw.get("stats")

    if isinstance(stats, dict):
        db["stats"].update(stats)

    # --------------------------------------------------------
    # Normalization
    # --------------------------------------------------------

    source_map: dict[str, dict[str, Any]] = {}

    for source in db["sources"]:

        sid = str(source.get("id", ""))

        if sid:
            source_map[sid] = source

    destination_map: dict[str, dict[str, Any]] = {}

    for destination in db["destinations"]:

        did = str(destination.get("id", ""))

        if did:
            destination_map[did] = destination

    for sid, route in list(db["routes"].items()):

        sid = str(sid)

        if not isinstance(route, dict):
            db["routes"].pop(sid, None)
            continue

        source = route.get("source")

        if isinstance(source, dict):
            source_map[sid] = source
        else:
            source = source_map.get(sid)

        if source is None:
            source = {
                "id": sid,
                "title": sid,
                "username": "",
            }

        route["source"] = source

        dests = route.get("destinations", [])

        if not isinstance(dests, list):
            dests = []

        cleaned_dests = []

        for dest in dests:

            if not isinstance(dest, dict):
                continue

            if dest.get("id") is None:
                continue

            did = str(dest["id"])

            destination_map[did] = dest
            cleaned_dests.append(dest)

        route["destinations"] = cleaned_dests

    db["sources"] = list(source_map.values())
    db["destinations"] = list(destination_map.values())

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
# REPEAT WORKERS
# ============================================================

# One worker for every:
#
# Source -> Destination
#
worker_tasks: dict[str, asyncio.Task] = {}

# Prevent duplicate workers.
worker_locks: dict[str, asyncio.Lock] = {}

# Used to wake sleeping workers when a new post arrives.
worker_events: dict[str, asyncio.Event] = {}

# ============================================================
# ALBUM BUFFER
# ============================================================

album_buffer: dict[
    tuple[str, str],
    list[int],
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
# HELPERS
# ============================================================


def is_admin(obj: Message | CallbackQuery) -> bool:
    return bool(
        obj.from_user
        and obj.from_user.id == ADMIN_ID
    )


def name_of(item: dict[str, Any] | None) -> str:

    if not item:
        return "Unknown"

    return str(
        item.get("title")
        or item.get("username")
        or item.get("id")
        or "Unknown"
    )


def source_ids() -> set[str]:

    return {
        str(x["id"])
        for x in db["sources"]
        if isinstance(x, dict)
        and x.get("id") is not None
    }


def destination_ids() -> set[str]:

    return {
        str(x["id"])
        for x in db["destinations"]
        if isinstance(x, dict)
        and x.get("id") is not None
    }


def get_source(sid: str) -> dict[str, Any] | None:

    sid = str(sid)

    for source in db["sources"]:

        if str(source.get("id")) == sid:
            return source

    return None


def get_destination(did: str) -> dict[str, Any] | None:

    did = str(did)

    for destination in db["destinations"]:

        if str(destination.get("id")) == did:
            return destination

    return None


def route_dests(sid: str) -> list[dict[str, Any]]:

    route = db["routes"].get(str(sid))

    if not isinstance(route, dict):
        return []

    dests = route.get("destinations", [])

    if not isinstance(dests, list):
        return []

    return [
        x for x in dests
        if isinstance(x, dict)
        and x.get("id") is not None
    ]


def route_dest_ids(sid: str) -> set[str]:

    return {
        str(x["id"])
        for x in route_dests(sid)
    }


def route_key(sid: str, did: str) -> str:

    return f"{sid}|{did}"


def split_key(key: str) -> tuple[str, str]:

    return key.split("|", 1)


def parse_chat_id(value: str) -> Any:

    value = value.strip()

    if value.lstrip("-").isdigit():
        return int(value)

    return value


async def get_chat(value: str) -> Any:

    if bot is None:
        raise RuntimeError("Bot is not initialized.")

    value = value.strip()

    if not value:
        raise ValueError("Username or chat ID is empty.")

    return await bot.get_chat(
        parse_chat_id(value)
    )


def status_value(value: Any) -> str:

    return getattr(
        value,
        "value",
        str(value),
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

        temp_file.replace(DATA_FILE)


# ============================================================
# RESOLVE TELEGRAM CHATS
# ============================================================


async def resolve_source(
    value: str,
) -> dict[str, str]:

    if bot is None:
        raise RuntimeError("Bot is not initialized.")

    chat = await get_chat(value)

    chat_type = status_value(
        getattr(chat, "type", "")
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
        getattr(member, "status", "")
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


async def resolve_destination(
    value: str,
) -> dict[str, str]:

    if bot is None:
        raise RuntimeError("Bot is not initialized.")

    chat = await get_chat(value)

    chat_type = status_value(
        getattr(chat, "type", "")
    )

    if chat_type not in {
        "channel",
        "group",
        "supergroup",
    }:
        raise ValueError(
            "Destination must be a channel or group."
        )

    me = await bot.get_me()

    member = await bot.get_chat_member(
        chat.id,
        me.id,
    )

    member_status = status_value(
        getattr(member, "status", "")
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
            "Bot needs Post Messages permission."
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
# PLAYLIST FUNCTIONS
# ============================================================


def get_playlist(
    sid: str,
) -> list[dict[str, Any]]:

    sid = str(sid)

    playlist = db["playlists"].get(
        sid,
        [],
    )

    if not isinstance(playlist, list):
        playlist = []

    return playlist


def playlist_length(sid: str) -> int:

    return len(
        get_playlist(sid)
    )


async def add_playlist_item(
    sid: str,
    message_ids: list[int],
    album: bool = False,
) -> None:

    sid = str(sid)

    clean_ids = []

    for mid in message_ids:

        try:
            clean_ids.append(
                int(mid)
            )
        except Exception:
            pass

    if not clean_ids:
        return

    async with playlist_lock:

        playlist = db["playlists"].setdefault(
            sid,
            [],
        )

        # Duplicate protection.
        existing_ids = set()

        for item in playlist:

            if not isinstance(item, dict):
                continue

            for mid in item.get(
                "ids",
                [],
            ):

                try:
                    existing_ids.add(
                        int(mid)
                    )
                except Exception:
                    pass

        if any(
            mid in existing_ids
            for mid in clean_ids
        ):
            return

        playlist.append(
            {
                "id": clean_ids[0],
                "ids": clean_ids,
                "album": bool(
                    album or len(clean_ids) > 1
                ),
                "created": int(
                    time.time()
                ),
            }
        )

        max_playlist = max(
            1,
            int(
                db["settings"].get(
                    "max_playlist",
                    50000,
                )
            ),
        )

        # Keep latest items.
        if len(playlist) > max_playlist:

            remove_count = (
                len(playlist)
                - max_playlist
            )

            del playlist[
                :remove_count
            ]

            current = int(
                db["positions"].get(
                    sid,
                    0,
                )
            )

            current -= remove_count

            db["positions"][sid] = max(
                0,
                current,
            )

    await save_db()


def next_playlist_item(
    sid: str,
) -> tuple[dict[str, Any] | None, bool]:

    sid = str(sid)

    playlist = get_playlist(sid)

    if not playlist:
        return None, False

    position = int(
        db["positions"].get(
            sid,
            0,
        )
    )

    if position < 0:
        position = 0

    if position >= len(playlist):

        position = 0

        db["positions"][sid] = 0

        return playlist[0], True

    item = playlist[position]

    position += 1

    cycle_finished = False

    if position >= len(playlist):

        # The LAST post has just been selected.
        #
        # Next call will start from First.
        #
        db["positions"][sid] = 0

        cycle_finished = True

    else:

        db["positions"][sid] = position

    return item, cycle_finished


# ============================================================
# HOME UI
# ============================================================


def home_text() -> str:

    s = db["settings"]
    st = db["stats"]

    running = (
        "🟢 RUNNING"
        if s["enabled"]
        else "🔴 STOPPED"
    )

    repeat = (
        "🟢 ON"
        if s["repeat_enabled"]
        else "🔴 OFF"
    )

    routes = sum(
        bool(
            route_dests(
                str(source["id"])
            )
        )
        for source in db["sources"]
    )

    total_playlist = sum(
        len(get_playlist(str(source["id"])))
        for source in db["sources"]
    )

    return (
        "🤖 <b>Route Repeat Copy Bot</b>\n\n"

        f"Status: <b>{running}</b>\n"
        f"Repeat: <b>{repeat}</b>\n\n"

        f"Sources: <b>{len(db['sources'])}</b>\n"
        f"Destinations: <b>{len(db['destinations'])}</b>\n"
        f"Routes: <b>{routes}</b>\n"
        f"Playlist items: <b>{total_playlist}</b>\n\n"

        f"⏱ Interval: <b>{s['interval']}s</b>\n"
        f"📦 Album wait: <b>{s['album_wait']}s</b>\n"
        f"🔁 Retries: <b>{s['retries']}</b>\n"
        f"📚 Max playlist: <b>{s['max_playlist']}</b>\n\n"

        f"📥 Received: <b>{st['received']}</b>\n"
        f"📤 Sent: <b>{st['sent']}</b>\n"
        f"❌ Failed: <b>{st['failed']}</b>\n"
        f"🔄 Cycles: <b>{st['cycles']}</b>"
    )


def home_kb() -> InlineKeyboardMarkup:

    running = db["settings"]["enabled"]
    repeat = db["settings"]["repeat_enabled"]

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
                ),
            ],

            [
                InlineKeyboardButton(
                    text=(
                        "🔁 Repeat: ON"
                        if repeat
                        else "🔁 Repeat: OFF"
                    ),
                    callback_data="repeat",
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
# SOURCE UI
# ============================================================


def sources_kb() -> InlineKeyboardMarkup:

    rows = []

    for index, source in enumerate(
        db["sources"]
    ):

        sid = str(source["id"])

        rows.append(
            [
                InlineKeyboardButton(
                    text=(
                        f"🔀 {name_of(source)} "
                        f"[{len(route_dests(sid))}] "
                        f"[📚 {playlist_length(sid)}]"
                    ),
                    callback_data=f"route:{index}",
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


def route_kb(
    sid: str,
) -> InlineKeyboardMarkup:

    selected = route_dest_ids(sid)

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
# DESTINATION UI
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
            did in route_dest_ids(
                str(source["id"])
            )
            for source in db["sources"]
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
# PLAYLIST UI
# ============================================================


def playlist_kb(
    sid: str,
) -> InlineKeyboardMarkup:

    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="⏮ Reset Position",
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
                )
            ],
        ]
    )


# ============================================================
# SETTINGS UI
# ============================================================


def settings_kb() -> InlineKeyboardMarkup:

    s = db["settings"]

    return InlineKeyboardMarkup(
        inline_keyboard=[

            [
                InlineKeyboardButton(
                    text=(
                        f"⏱ Interval: "
                        f"{s['interval']}s"
                    ),
                    callback_data="set:interval",
                )
            ],

            [
                InlineKeyboardButton(
                    text=(
                        f"📦 Album Wait: "
                        f"{s['album_wait']}s"
                    ),
                    callback_data="set:album",
                )
            ],

            [
                InlineKeyboardButton(
                    text=(
                        f"🔁 Retries: "
                        f"{s['retries']}"
                    ),
                    callback_data="set:retries",
                )
            ],

            [
                InlineKeyboardButton(
                    text=(
                        f"📚 Max Playlist: "
                        f"{s['max_playlist']}"
                    ),
                    callback_data="set:playlist",
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
# WORKER EVENT
# ============================================================


def get_worker_event(
    key: str,
) -> asyncio.Event:

    event = worker_events.get(key)

    if event is None:

        event = asyncio.Event()

        worker_events[key] = event

    return event


def wake_workers_for_source(
    sid: str,
) -> None:

    sid = str(sid)

    for key, event in worker_events.items():

        try:
            worker_sid, _ = split_key(key)

            if worker_sid == sid:
                event.set()

        except Exception:
            pass


# ============================================================
# COPY WITH RETRIES
# ============================================================


async def copy_one(
    sid: str,
    did: str,
    mid: int,
) -> bool:

    if bot is None:
        return False

    retries = max(
        0,
        int(
            db["settings"].get(
                "retries",
                8,
            )
        ),
    )

    for attempt in range(
        retries + 1
    ):

        try:

            log.info(
                "COPY %s -> %s | message=%s",
                sid,
                did,
                mid,
            )

            await bot.copy_message(
                chat_id=int(did),
                from_chat_id=int(sid),
                message_id=int(mid),
            )

            log.info(
                "COPY OK %s -> %s | message=%s",
                sid,
                did,
                mid,
            )

            return True

        except TelegramRetryAfter as exc:

            wait = int(
                exc.retry_after
            ) + 1

            log.warning(
                "Rate limited. Waiting %ss",
                wait,
            )

            await asyncio.sleep(
                wait
            )

        except TelegramNetworkError as exc:

            db["stats"]["last_error"] = str(
                exc
            )

            if attempt >= retries:
                return False

            await asyncio.sleep(
                min(
                    2 ** attempt,
                    30,
                )
            )

        except TelegramForbiddenError as exc:

            db["stats"]["last_error"] = str(
                exc
            )

            log.error(
                "Forbidden %s -> %s: %s",
                sid,
                did,
                exc,
            )

            return False

        except TelegramBadRequest as exc:

            db["stats"]["last_error"] = str(
                exc
            )

            log.error(
                "BadRequest %s -> %s | %s",
                sid,
                did,
                exc,
            )

            return False

        except Exception as exc:

            db["stats"]["last_error"] = str(
                exc
            )

            log.exception(
                "Unexpected copy error"
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
# ALBUM COPY
# ============================================================


async def copy_album(
    sid: str,
    did: str,
    message_ids: list[int],
) -> bool:

    success = True

    for mid in message_ids:

        ok = await copy_one(
            sid,
            did,
            mid,
        )

        if not ok:
            success = False

        interval = float(
            db["settings"].get(
                "interval",
                1.0,
            )
        )

        if interval > 0:
            await asyncio.sleep(
                interval
            )

    return success


# ============================================================
# REPEAT WORKER
# ============================================================


async def repeat_worker(
    sid: str,
    did: str,
) -> None:

    key = route_key(
        sid,
        did,
    )

    event = get_worker_event(
        key
    )

    log.info(
        "REPEAT WORKER STARTED %s -> %s",
        sid,
        did,
    )

    while True:

        try:

            # ------------------------------------------------
            # STOP MODE
            # ------------------------------------------------

            if not db["settings"]["enabled"]:

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
            # CHECK ROUTE
            # ------------------------------------------------

            if sid not in source_ids():

                await asyncio.sleep(2)

                continue

            if did not in route_dest_ids(
                sid
            ):

                await asyncio.sleep(2)

                continue

            # ------------------------------------------------
            # PLAYLIST
            # ------------------------------------------------

            playlist = get_playlist(
                sid
            )

            if not playlist:

                # No playlist yet.
                #
                # Wait for NEW channel post.
                #
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
            # REPEAT DISABLED
            # ------------------------------------------------

            if not db["settings"][
                "repeat_enabled"
            ]:

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
            # GET NEXT ITEM
            # ------------------------------------------------

            async with playlist_lock:

                item, cycle_finished = (
                    next_playlist_item(
                        sid
                    )
                )

                if cycle_finished:
                    db["stats"]["cycles"] += 1

                await save_db()

            if item is None:

                await asyncio.sleep(2)

                continue

            mids = item.get(
                "ids",
                [],
            )

            if not isinstance(
                mids,
                list,
            ):
                mids = []

            mids = [
                int(x)
                for x in mids
                if str(x).lstrip("-").isdigit()
            ]

            if not mids:

                continue

            # ------------------------------------------------
            # COPY ITEM
            # ------------------------------------------------

            if len(mids) == 1:

                ok = await copy_one(
                    sid,
                    did,
                    mids[0],
                )

            else:

                ok = await copy_album(
                    sid,
                    did,
                    mids,
                )

            # ------------------------------------------------
            # STATS
            # ------------------------------------------------

            if ok:

                db["stats"]["sent"] += 1
                db["stats"]["last_sent"] = int(
                    time.time()
                )

            else:

                db["stats"]["failed"] += 1

            await save_db()

            # ------------------------------------------------
            # IMPORTANT:
            #
            # This is what creates:
            #
            # First
            # Second
            # Third
            # ...
            # Last
            # First
            # Second
            #
            # The position is reset to 0 after Last.
            # ------------------------------------------------

            interval = float(
                db["settings"].get(
                    "interval",
                    1.0,
                )
            )

            if interval > 0:

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
                "REPEAT WORKER STOPPED %s -> %s",
                sid,
                did,
            )

            raise

        except Exception as exc:

            db["stats"]["last_error"] = str(
                exc
            )

            log.exception(
                "Worker error %s -> %s",
                sid,
                did,
            )

            await asyncio.sleep(3)


# ============================================================
# WORKER MANAGEMENT
# ============================================================


async def start_worker(
    sid: str,
    did: str,
) -> None:

    key = route_key(
        sid,
        did,
    )

    task = worker_tasks.get(
        key
    )

    if task and not task.done():
        return

    worker_events[key] = asyncio.Event()

    worker_tasks[key] = asyncio.create_task(
        repeat_worker(
            sid,
            did,
        ),
        name=f"worker-{sid}-{did}",
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

    if task:

        task.cancel()

        try:
            await task

        except asyncio.CancelledError:
            pass

        except Exception:
            log.exception(
                "Worker shutdown error"
            )

    worker_events.pop(
        key,
        None,
    )


async def ensure_workers() -> None:

    wanted: set[str] = set()

    if db["settings"]["enabled"]:

        for sid, route in db[
            "routes"
        ].items():

            if not isinstance(
                route,
                dict,
            ):
                continue

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

                did = str(did)

                key = route_key(
                    str(sid),
                    did,
                )

                wanted.add(key)

                await start_worker(
                    str(sid),
                    did,
                )

    for key in list(
        worker_tasks
    ):

        if key not in wanted:

            sid, did = split_key(
                key
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
        sid,
        album_id,
    )

    try:

        wait = float(
            db["settings"].get(
                "album_wait",
                1.5,
            )
        )

        await asyncio.sleep(
            wait
        )

        mids = album_buffer.pop(
            key,
            [],
        )

        mids = sorted(
            set(
                int(x)
                for x in mids
            )
        )

        if not mids:
            return

        db["stats"]["albums"] += 1

        await add_playlist_item(
            sid,
            mids,
            album=True,
        )

        db["stats"]["received"] += len(
            mids
        )

        db["stats"]["last_received"] = int(
            time.time()
        )

        wake_workers_for_source(
            sid
        )

        await save_db()

    except asyncio.CancelledError:
        raise

    except Exception as exc:

        db["stats"]["last_error"] = str(
            exc
        )

        log.exception(
            "Album flush error"
        )

    finally:

        album_tasks.pop(
            key,
            None,
        )


# ============================================================
# NEW CHANNEL POSTS
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
        "NEW POST | source=%s | message=%s | type=%s",
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
            [],
        ).append(mid)

        old_task = album_tasks.get(
            key
        )

        if old_task:

            old_task.cancel()

        album_tasks[key] = asyncio.create_task(
            flush_album(
                sid,
                str(album_id),
            )
        )

        return

    # --------------------------------------------------------
    # NORMAL POST
    # --------------------------------------------------------

    await add_playlist_item(
        sid,
        [mid],
        album=False,
    )

    db["stats"]["received"] += 1

    db["stats"]["last_received"] = int(
        time.time()
    )

    await save_db()

    # Wake repeat workers.
    wake_workers_for_source(
        sid
    )


# ============================================================
# START
# ============================================================


@router.message(CommandStart())
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
# ADMIN
# ============================================================


@router.message(Command("admin"))
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
# CANCEL
# ============================================================


@router.message(Command("cancel"))
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
# CALLBACKS
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

        db["settings"]["enabled"] = not bool(
            db["settings"]["enabled"]
        )

        await save_db()

        await ensure_workers()

        await q.answer(
            "Started."
            if db["settings"]["enabled"]
            else "Stopped."
        )

        await q.message.edit_text(
            home_text(),
            reply_markup=home_kb(),
        )

        return

    # --------------------------------------------------------
    # REPEAT ON / OFF
    # --------------------------------------------------------

    if data == "repeat":

        db["settings"]["repeat_enabled"] = not bool(
            db["settings"]["repeat_enabled"]
        )

        await save_db()

        if db["settings"]["repeat_enabled"]:

            for event in worker_events.values():
                event.set()

            text = "🔁 Repeat mode ON."

        else:

            text = "⏸ Repeat mode OFF."

        await q.answer(
            text
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
    # SELECT SOURCE
    # --------------------------------------------------------

    if data.startswith("route:"):

        try:

            index = int(
                data.split(
                    ":",
                    1,
                )[1]
            )

            source = db["sources"][
                index
            ]

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
                f"<code>{sid}</code>\n\n"

                f"Playlist: "
                f"<b>{playlist_length(sid)}</b>\n"

                f"Destinations: "
                f"<b>{len(route_dest_ids(sid))}</b>"
            ),
            reply_markup=route_kb(sid),
        )

        return

    # --------------------------------------------------------
    # ROUTE BY ID
    # --------------------------------------------------------

    if data.startswith("route_by_id:"):

        sid = data.split(
            ":",
            1,
        )[1]

        source = get_source(
            sid
        )

        if not source:

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
                f"Playlist: <b>{playlist_length(sid)}</b>"
            ),
            reply_markup=route_kb(sid),
        )

        return

    # --------------------------------------------------------
    # ROUTE DESTINATION TOGGLE
    # --------------------------------------------------------

    if data.startswith(
        "route_dest:"
    ):

        try:

            _, sid, index = data.split(
                ":",
                2,
            )

            destination = db[
                "destinations"
            ][
                int(index)
            ]

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

        current = {
            str(x["id"])
            for x in route.get(
                "destinations",
                []
            )
            if isinstance(
                x,
                dict,
            )
        }

        if did in current:

            route["destinations"] = [
                x
                for x in route[
                    "destinations"
                ]
                if str(
                    x["id"]
                ) != did
            ]

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

        if not db["destinations"]:

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
                did in route_dest_ids(
                    str(source["id"])
                )
                for source in db["sources"]
            )

            lines.extend(
                [
                    f"{index}. <b>{name_of(destination)}</b>",
                    f"Used by: <b>{used}</b> route(s)",
                    f"<code>{did}</code>",
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
                "⚠️ Bot must be administrator."
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
                "⚠️ Bot must be administrator."
            )
        )

        return

    # --------------------------------------------------------
    # ADD DESTINATION TO ROUTE
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
                "Send @username or numeric channel/group ID."
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

        if sid in source_ids():

            db["sources"] = [
                x
                for x in db["sources"]
                if str(
                    x["id"]
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

            db["positions"].pop(
                sid,
                None,
            )

            for key in list(
                worker_tasks
            ):

                rsid, did = split_key(
                    key
                )

                if rsid == sid:

                    await stop_worker(
                        rsid,
                        did,
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
            ].pop(
                index
            )

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

        for route in db[
            "routes"
        ].values():

            if not isinstance(
                route,
                dict,
            ):
                continue

            route["destinations"] = [
                x
                for x in route.get(
                    "destinations",
                    [],
                )
                if str(
                    x["id"]
                ) != did
            ]

        for key in list(
            worker_tasks
        ):

            sid, route_did = split_key(
                key
            )

            if route_did == did:

                await stop_worker(
                    sid,
                    route_did,
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

            position = int(
                db["positions"].get(
                    sid,
                    0,
                )
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

            if length:

                lines.append(
                    f"   Next position: "
                    f"<b>{position + 1}</b>"
                )

            lines.append("")

        rows = []

        for index, source in enumerate(
            db["sources"]
        ):

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

        if not source:

            await q.answer(
                "Source not found.",
                show_alert=True,
            )

            return

        playlist = get_playlist(
            sid
        )

        position = int(
            db["positions"].get(
                sid,
                0,
            )
        )

        await q.answer()

        text = (
            "📚 <b>Playlist</b>\n\n"
            f"Source: <b>{name_of(source)}</b>\n"
            f"Items: <b>{len(playlist)}</b>\n"
            f"Current next position: "
            f"<b>{position + 1 if playlist else 0}</b>\n\n"
        )

        if playlist:

            preview = playlist[
                :20
            ]

            for index, item in enumerate(
                preview,
                1,
            ):

                ids = item.get(
                    "ids",
                    [],
                )

                text += (
                    f"{index}. "
                    f"Message {ids[0] if ids else '?'}"
                )

                if len(ids) > 1:
                    text += (
                        f" + {len(ids)-1} album items"
                    )

                text += "\n"

            if len(playlist) > 20:

                text += (
                    f"\n... +"
                    f"{len(playlist)-20}"
                    f" more"
                )

        else:

            text += (
                "Playlist empty.\n"
                "New channel posts will be added automatically."
            )

        await q.message.edit_text(
            text,
            reply_markup=playlist_kb(
                sid
            ),
        )

        return

    # --------------------------------------------------------
    # RESET PLAYLIST
    # --------------------------------------------------------

    if data.startswith(
        "reset_playlist:"
    ):

        sid = data.split(
            ":",
            1,
        )[1]

        db["positions"][sid] = 0

        await save_db()

        event = get_worker_event(
            route_key(
                sid,
                str(
                    route_dests(sid)[0]["id"]
                )
            )
        ) if route_dests(sid) else None

        if event:
            event.set()

        await q.answer(
            "Playlist position reset."
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

        db["playlists"].pop(
            sid,
            None
        )

        db["positions"][sid] = 0

        await save_db()

        await q.answer(
            "Playlist cleared."
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

        st = db["stats"]

        queues = []

        for key, task in worker_tasks.items():

            sid, did = split_key(
                key
            )

            state_value = (
                "RUNNING"
                if not task.done()
                else "STOPPED"
            )

            queues.append(
                (
                    f"• {name_of(get_source(sid))}"
                    f" → "
                    f"{name_of(get_destination(did))}"
                    f": <b>{state_value}</b>"
                )
            )

        text = (
            "📊 <b>Status</b>\n\n"

            f"Running: "
            f"<b>{db['settings']['enabled']}</b>\n"

            f"Repeat: "
            f"<b>{db['settings']['repeat_enabled']}</b>\n\n"

            f"Workers: <b>{len(worker_tasks)}</b>\n"

            f"📥 Received: "
            f"<b>{st['received']}</b>\n"

            f"📤 Sent: "
            f"<b>{st['sent']}</b>\n"

            f"❌ Failed: "
            f"<b>{st['failed']}</b>\n"

            f"🔄 Cycles: "
            f"<b>{st['cycles']}</b>\n\n"

            "<b>Workers</b>\n"
            + (
                "\n".join(queues)
                if queues
                else "-"
            )
            + "\n\n"

            "<b>Last error</b>\n"
            f"<code>"
            f"{str(st['last_error'] or '-')[:1500]}"
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

        mapping = {

            "interval": (
                SetInterval.value,
                (
                    "⏱ <b>Interval</b>\n\n"
                    "Enter seconds.\n"
                    "Example: <code>30</code>"
                ),
            ),

            "album": (
                SetAlbumWait.value,
                (
                    "📦 <b>Album Wait</b>\n\n"
                    "Enter seconds.\n"
                    "Example: <code>1.5</code>"
                ),
            ),

            "retries": (
                SetRetries.value,
                (
                    "🔁 <b>Retries</b>\n\n"
                    "Enter integer 0–20."
                ),
            ),

            "playlist": (
                SetPlaylistLimit.value,
                (
                    "📚 <b>Max Playlist</b>\n\n"
                    "Enter 100–50000."
                ),
            ),
        }

        if kind in mapping:

            await q.answer()

            state_value, text = mapping[
                kind
            ]

            await state.set_state(
                state_value
            )

            await q.message.edit_text(
                text
            )

            return

    await q.answer()


# ============================================================
# ADD SOURCE MESSAGE
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

    try:

        source = await resolve_source(
            (message.text or "").strip()
        )

        sid = str(
            source["id"]
        )

        if sid in source_ids():

            await message.answer(
                "⚠️ Source already exists.",
                reply_markup=sources_kb(),
            )

        else:

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

            db["positions"].setdefault(
                sid,
                0,
            )

            await save_db()

            await message.answer(
                (
                    "✅ <b>Source Added</b>\n\n"
                    f"<b>{name_of(source)}</b>\n"
                    f"<code>{sid}</code>\n\n"
                    "📡 New posts will be added to playlist.\n"
                    "🔁 Playlist will repeat continuously.\n\n"
                    "Now select destinations."
                ),
                reply_markup=route_kb(
                    sid
                ),
            )

    except Exception as exc:

        log.exception(
            "Add source failed"
        )

        await message.answer(
            (
                "❌ <b>Could not add source</b>\n\n"
                f"<code>{str(exc)[:1200]}</code>"
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

    try:

        destination = await resolve_destination(
            (message.text or "").strip()
        )

        did = str(
            destination["id"]
        )

        if did not in destination_ids():

            db["destinations"].append(
                destination
            )

        else:

            destination = (
                get_destination(did)
                or destination
            )

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
                str(x["id"])
                for x in route.get(
                    "destinations",
                    []
                )
            }

            if did not in existing:

                route[
                    "destinations"
                ].append(
                    destination
                )

            await save_db()

            await ensure_workers()

            await message.answer(
                (
                    "✅ <b>Destination Added</b>\n\n"
                    f"Source: <b>"
                    f"{name_of(get_source(route_sid))}"
                    f"</b>\n"
                    f"Destination: <b>"
                    f"{name_of(destination)}"
                    f"</b>\n\n"
                    "🚀 Route active."
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
                    "Open Routes and select source(s)."
                ),
                reply_markup=dest_kb(),
            )

    except Exception as exc:

        log.exception(
            "Add destination failed"
        )

        await message.answer(
            (
                "❌ <b>Could not add destination</b>\n\n"
                f"<code>{str(exc)[:1200]}</code>"
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
            (message.text or "").strip()
        )

        if not 0.1 <= value <= 3600:
            raise ValueError

        db["settings"][
            "interval"
        ] = value

        await save_db()

        for event in worker_events.values():
            event.set()

        await message.answer(
            f"✅ Interval set to <b>{value}s</b>.",
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
            (message.text or "").strip()
        )

        if not 0.2 <= value <= 10:
            raise ValueError

        db["settings"][
            "album_wait"
        ] = value

        await save_db()

        await message.answer(
            f"✅ Album wait: <b>{value}s</b>.",
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
            (message.text or "").strip()
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
            (message.text or "").strip()
        )

        if not 100 <= value <= 50000:
            raise ValueError

        db["settings"][
            "max_playlist"
        ] = value

        await save_db()

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

    total_playlist = sum(
        len(get_playlist(str(source["id"])))
        for source in db["sources"]
    )

    return web.json_response(
        {
            "ok": True,
            "mode": "webhook",

            "running": bool(
                db["settings"]["enabled"]
            ),

            "repeat": bool(
                db["settings"]["repeat_enabled"]
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

            "playlist_items": total_playlist,

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

            "time": int(
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

    incoming = request.headers.get(
        "X-Telegram-Bot-Api-Secret-Token",
        "",
    )

    if not secrets.compare_digest(
        incoming,
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

        update = Update.model_validate_json(
            raw
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

    except Exception:

        log.exception(
            "Webhook update error"
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

        # Do not discard pending channel posts.
        drop_pending_updates=False,
    )

    info = await bot.get_webhook_info()

    log.info(
        "Webhook=%s | pending=%s",
        info.url,
        info.pending_update_count,
    )

    if info.last_error_message:

        log.error(
            "Telegram webhook error: %s",
            info.last_error_message,
        )


# ============================================================
# SHUTDOWN
# ============================================================


async def shutdown() -> None:

    # Album tasks.
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

        except Exception:
            log.exception(
                "Album task shutdown error"
            )

    album_tasks.clear()
    album_buffer.clear()

    # Workers.
    for key in list(
        worker_tasks
    ):

        try:

            sid, did = split_key(
                key
            )

            await stop_worker(
                sid,
                did,
            )

        except Exception:

            log.exception(
                "Worker shutdown error: %s",
                key,
            )

    if bot:

        try:

            await bot.delete_webhook(
                drop_pending_updates=False
            )

        except Exception:

            log.exception(
                "Could not delete webhook"
            )

        try:

            await bot.session.close()

        except Exception:

            pass

    if server_runner:

        await server_runner.cleanup()


# ============================================================
# MAIN
# ============================================================


async def main() -> None:

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
        "BOT CONNECTED @%s | id=%s",
        me.username,
        me.id,
    )

    # HTTP server first.
    await start_server()

    # Telegram webhook.
    await setup_webhook()

    # Start all configured route workers.
    await ensure_workers()

    log.info(
        "================================================"
    )

    log.info(
        "READY"
    )

    log.info(
        "Mode: WEBHOOK"
    )

    log.info(
        "Repeat: %s",
        db["settings"]["repeat_enabled"],
    )

    log.info(
        "Sources: %s",
        len(db["sources"]),
    )

    log.info(
        "Destinations: %s",
        len(db["destinations"]),
    )

    log.info(
        "Workers: %s",
        len(worker_tasks),
    )

    log.info(
        "================================================"
    )

    try:

        await asyncio.Event().wait()

    finally:

        await shutdown()


# ============================================================
# ENTRY
# ============================================================


if __name__ == "__main__":

    try:

        asyncio.run(
            main()
        )

    except KeyboardInterrupt:

        pass
