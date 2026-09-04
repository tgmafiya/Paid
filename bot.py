from __future__ import annotations

import asyncio
import html
import json
import logging
import os
import time
import uuid
from pathlib import Path
from typing import Any

from aiohttp import web

from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ChatMemberStatus, ParseMode
from aiogram.exceptions import (
    TelegramBadRequest,
    TelegramForbiddenError,
    TelegramNetworkError,
    TelegramRetryAfter,
)
from aiogram.filters import Command, CommandStart
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)

# ============================================================
# CONFIG
# ============================================================

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()

try:
    ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))
except ValueError:
    ADMIN_ID = 0

DATA_FILE = Path(
    os.getenv("DATA_FILE", "data.json")
)

HOST = "0.0.0.0"

try:
    PORT = int(os.getenv("PORT", "10000"))
except ValueError:
    PORT = 10000

# Invite link lifetime = 5 minutes
LINK_LIFETIME = 5 * 60

# Join check interval
JOIN_CHECK_INTERVAL = 15


if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is missing")

if not ADMIN_ID:
    raise RuntimeError("ADMIN_ID is missing")


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

logger = logging.getLogger(
    "paid-subscription-bot"
)


# ============================================================
# DEFAULT DATA
# ============================================================

DEFAULT_DATA: dict[str, Any] = {
    "settings": {
        "upi_id": "",
        "upi_name": "",
        "paid_channel_id": "",
        "paid_channel_title": "",
        "proof_channel_id": "",
        "currency": "INR",
    },
    "plans": {},
    "users": {},
    "payments": {},
    "subscriptions": {},
}


# ============================================================
# DATA LOCK
# ============================================================

data_lock = asyncio.Lock()


# ============================================================
# DEEP DEFAULT MERGE
# ============================================================

def merge_defaults(
    loaded: dict[str, Any],
) -> dict[str, Any]:
    """
    Makes sure old data.json files also receive
    newly added keys.
    """

    result = json.loads(
        json.dumps(DEFAULT_DATA)
    )

    if not isinstance(loaded, dict):
        return result

    for key, value in loaded.items():
        if (
            key in result
            and isinstance(result[key], dict)
            and isinstance(value, dict)
        ):
            result[key].update(value)
        else:
            result[key] = value

    return result


# ============================================================
# DATA LOAD / SAVE
# ============================================================

def save_data_sync(
    obj: dict[str, Any],
) -> None:

    DATA_FILE.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    temp = DATA_FILE.with_suffix(".tmp")

    with temp.open(
        "w",
        encoding="utf-8",
    ) as f:

        json.dump(
            obj,
            f,
            indent=2,
            ensure_ascii=False,
        )

    temp.replace(DATA_FILE)


def load_data() -> dict[str, Any]:

    if not DATA_FILE.exists():

        DATA_FILE.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        default = json.loads(
            json.dumps(DEFAULT_DATA)
        )

        save_data_sync(default)

        return default

    try:

        with DATA_FILE.open(
            "r",
            encoding="utf-8",
        ) as f:

            loaded = json.load(f)

        return merge_defaults(loaded)

    except Exception:

        logger.exception(
            "Could not load data.json"
        )

        return json.loads(
            json.dumps(DEFAULT_DATA)
        )


data = load_data()


async def save_data() -> None:

    async with data_lock:

        save_data_sync(data)


# ============================================================
# BOT
# ============================================================

bot = Bot(
    token=BOT_TOKEN,
    default=DefaultBotProperties(
        parse_mode=ParseMode.HTML,
    ),
)

dp = Dispatcher()


# ============================================================
# MEMORY
# ============================================================

# user_id -> payment_id
pending_payment: dict[int, str] = {}

# admin_id -> admin action
pending_admin_action: dict[int, str] = {}

# payment_id -> asyncio Task
invite_tasks: dict[str, asyncio.Task] = {}

# subscription_id -> asyncio Task
subscription_tasks: dict[str, asyncio.Task] = {}


# ============================================================
# HELPERS
# ============================================================

def is_admin(
    user_id: int,
) -> bool:

    return user_id == ADMIN_ID


def now_ts() -> int:
    return int(time.time())


def esc(
    value: Any,
) -> str:

    return html.escape(
        str(value)
    )


def money(
    amount: float | int,
) -> str:

    value = float(amount)

    if value.is_integer():
        return str(int(value))

    return (
        f"{value:.2f}"
        .rstrip("0")
        .rstrip(".")
    )


def format_duration(
    minutes: int,
) -> str:

    minutes = int(minutes)

    if minutes < 60:
        return f"{minutes} min"

    hours = minutes // 60
    remaining = minutes % 60

    if remaining == 0:

        if hours == 1:
            return "1 hour"

        return f"{hours} hours"

    return f"{hours}h {remaining}m"


# ============================================================
# USER MENU
# ============================================================

def main_menu() -> InlineKeyboardMarkup:

    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="💎 Buy Subscription",
                    callback_data="plans",
                )
            ],
            [
                InlineKeyboardButton(
                    text="ℹ️ My Subscription",
                    callback_data="my_subscription",
                )
            ],
        ]
    )


# ============================================================
# ADMIN MENU
# ============================================================

def admin_menu() -> InlineKeyboardMarkup:

    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="💎 Plans",
                    callback_data="admin_plans",
                ),
                InlineKeyboardButton(
                    text="➕ Add Plan",
                    callback_data="admin_add_plan",
                ),
            ],
            [
                InlineKeyboardButton(
                    text="💳 UPI Settings",
                    callback_data="admin_upi",
                ),
            ],
            [
                InlineKeyboardButton(
                    text="📢 Paid Channel",
                    callback_data="admin_channel",
                ),
            ],
            [
                InlineKeyboardButton(
                    text="🧾 Proof Channel",
                    callback_data="admin_proof",
                ),
            ],
            [
                InlineKeyboardButton(
                    text="👥 Users",
                    callback_data="admin_users",
                ),
                InlineKeyboardButton(
                    text="💰 Payments",
                    callback_data="admin_payments",
                ),
            ],
            [
                InlineKeyboardButton(
                    text="📊 Statistics",
                    callback_data="admin_stats",
                ),
            ],
        ]
    )


# ============================================================
# PAYMENT ACTION KEYBOARD
# ============================================================

def payment_keyboard(
    payment_id: str,
) -> InlineKeyboardMarkup:

    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="✅ I Paid",
                    callback_data=f"paid:{payment_id}",
                )
            ],
            [
                InlineKeyboardButton(
                    text="⬅️ Back",
                    callback_data="plans",
                )
            ],
        ]
    )


# ============================================================
# UPI URL
# ============================================================

def create_upi_url(
    upi_id: str,
    name: str,
    amount: float | int,
    payment_id: str,
) -> str:

    import urllib.parse

    params = {
        "pa": upi_id,
        "pn": name or "Subscription",
        "am": money(amount),
        "cu": "INR",
        "tn": f"Subscription {payment_id}",
    }

    return (
        "upi://pay?"
        + urllib.parse.urlencode(params)
    )


# ============================================================
# QR GENERATION
# ============================================================

async def create_qr_file(
    upi_id: str,
    name: str,
    amount: float | int,
    payment_id: str,
) -> str | None:

    try:

        import qrcode

        url = create_upi_url(
            upi_id=upi_id,
            name=name,
            amount=amount,
            payment_id=payment_id,
        )

        qr_dir = Path("qr")

        qr_dir.mkdir(
            parents=True,
            exist_ok=True,
        )

        path = (
            qr_dir
            / f"{payment_id}.png"
        )

        img = qrcode.make(url)

        img.save(path)

        return str(path)

    except Exception:

        logger.exception(
            "QR generation failed"
        )

        return None


# ============================================================
# USER DATA
# ============================================================

def ensure_user(
    message: Message,
) -> None:

    user = message.from_user

    if not user:
        return

    uid = str(user.id)

    if uid not in data["users"]:

        data["users"][uid] = {
            "user_id": user.id,
            "username": user.username or "",
            "first_name": user.first_name or "",
            "last_name": user.last_name or "",
            "created_at": now_ts(),
            "last_seen": now_ts(),
            "total_purchases": 0,
        }

    else:

        data["users"][uid][
            "username"
        ] = user.username or ""

        data["users"][uid][
            "first_name"
        ] = user.first_name or ""

        data["users"][uid][
            "last_name"
        ] = user.last_name or ""

        data["users"][uid][
            "last_seen"
        ] = now_ts()


# ============================================================
# START
# ============================================================

@dp.message(CommandStart())
async def start_handler(
    message: Message,
):

    ensure_user(message)

    await save_data()

    user_id = message.from_user.id

    # --------------------------------------------------------
    # ADMIN
    # --------------------------------------------------------

    if is_admin(user_id):

        logger.info(
            "Admin /start detected | user_id=%s | ADMIN_ID=%s",
            user_id,
            ADMIN_ID,
        )

        await message.answer(
            "🛠️ <b>Admin Panel</b>\n\n"
            "Welcome, Admin! 👑\n\n"
            "Manage your paid channel, plans, "
            "UPI, payments and users.",
            reply_markup=admin_menu(),
        )

        return

    # --------------------------------------------------------
    # NORMAL USER
    # --------------------------------------------------------

    await message.answer(
        "👋 <b>Welcome!</b>\n\n"
        "💎 Purchase a subscription to access "
        "the private channel.\n\n"
        "Choose a plan below:",
        reply_markup=main_menu(),
    )


# ============================================================
# SHOW TELEGRAM ID
# ============================================================

@dp.message(Command("id"))
async def id_handler(
    message: Message,
):

    await message.answer(
        "🆔 Your Telegram ID:\n\n"
        f"<code>{message.from_user.id}</code>"
    )


# ============================================================
# ADMIN COMMAND
# ============================================================

@dp.message(Command("admin"))
async def admin_handler(
    message: Message,
):

    if not is_admin(
        message.from_user.id
    ):

        await message.answer(
            "⛔ You are not authorized "
            "to access the admin panel."
        )

        return

    await message.answer(
        "🛠️ <b>Admin Panel</b>\n\n"
        "Choose an option:",
        reply_markup=admin_menu(),
    )


# ============================================================
# PLANS - USER
# ============================================================

@dp.callback_query(
    F.data == "plans"
)
async def plans_callback(
    callback: CallbackQuery,
):

    plans = data["plans"]

    buttons = []

    for plan_id, plan in plans.items():

        if not plan.get(
            "active",
            True,
        ):
            continue

        buttons.append(
            [
                InlineKeyboardButton(
                    text=(
                        f"💎 ₹{money(plan['price'])} / "
                        f"{format_duration(plan['minutes'])}"
                    ),
                    callback_data=f"buy:{plan_id}",
                )
            ]
        )

    if not buttons:

        await callback.answer(
            "No plans are available right now.",
            show_alert=True,
        )

        return

    buttons.append(
        [
            InlineKeyboardButton(
                text="⬅️ Back",
                callback_data="home",
            )
        ]
    )

    await callback.message.edit_text(
        "💎 <b>Available Plans</b>\n\n"
        "Select your subscription:",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=buttons
        ),
    )

    await callback.answer()


# ============================================================
# BUY PLAN
# ============================================================

@dp.callback_query(
    F.data.startswith("buy:")
)
async def buy_plan_callback(
    callback: CallbackQuery,
):

    plan_id = callback.data.split(
        ":",
        1,
    )[1]

    plan = data["plans"].get(
        plan_id
    )

    if not plan or not plan.get(
        "active",
        True,
    ):

        await callback.answer(
            "This plan is unavailable.",
            show_alert=True,
        )

        return

    settings = data["settings"]

    if not settings.get(
        "upi_id"
    ):

        await callback.answer(
            "Payment system is not configured yet.",
            show_alert=True,
        )

        return

    if not settings.get(
        "paid_channel_id"
    ):

        await callback.answer(
            "Paid channel is not configured yet.",
            show_alert=True,
        )

        return

    user_id = callback.from_user.id

    payment_id = (
        uuid.uuid4()
        .hex[:10]
        .upper()
    )

    # --------------------------------------------------------
    # Create payment
    # --------------------------------------------------------

    data["payments"][payment_id] = {

        "payment_id": payment_id,

        "user_id": user_id,

        "plan_id": plan_id,

        "amount": plan["price"],

        "minutes": plan["minutes"],

        "status": "awaiting_proof",

        "created_at": now_ts(),

        "proof_message_id": None,

        "approved_at": None,

        "rejected_at": None,

        "proof_requested_at": None,

        "subscription_started": False,

    }

    # Store currently selected payment
    pending_payment[user_id] = payment_id

    await save_data()

    # --------------------------------------------------------
    # Generate QR
    # --------------------------------------------------------

    qr_path = await create_qr_file(
        settings["upi_id"],
        settings.get(
            "upi_name",
            "Subscription",
        ),
        plan["price"],
        payment_id,
    )

    # --------------------------------------------------------
    # Payment message
    # --------------------------------------------------------

    text = (
        "💳 <b>Payment Details</b>\n\n"

        f"💎 Plan: "
        f"<b>₹{money(plan['price'])}</b>\n"

        f"⏱ Duration: "
        f"<b>{format_duration(plan['minutes'])}</b>\n\n"

        f"🏦 UPI ID:\n"
        f"<code>{esc(settings['upi_id'])}</code>\n\n"

        f"🧾 Payment ID:\n"
        f"<code>{payment_id}</code>\n\n"

        "📱 Pay the exact amount using UPI.\n\n"

        "After completing the payment, "
        "tap <b>✅ I Paid</b> below.\n\n"

        "You will then be asked to send "
        "your payment screenshot.\n\n"

        "⚠️ Your subscription will be activated "
        "only after admin approval."
    )

    keyboard = payment_keyboard(
        payment_id
    )

    # --------------------------------------------------------
    # Send QR
    # --------------------------------------------------------

    if qr_path:

        from aiogram.types import FSInputFile

        await callback.message.answer_photo(
            photo=FSInputFile(
                qr_path
            ),
            caption=text,
            reply_markup=keyboard,
        )

    else:

        await callback.message.answer(
            text,
            reply_markup=keyboard,
        )

    await callback.answer(
        "Payment details opened."
    )


# ============================================================
# I PAID
# ============================================================

@dp.callback_query(
    F.data.startswith("paid:")
)
async def paid_callback(
    callback: CallbackQuery,
):

    payment_id = callback.data.split(
        ":",
        1,
    )[1]

    payment = data["payments"].get(
        payment_id
    )

    if not payment:

        await callback.answer(
            "❌ Payment not found.",
            show_alert=True,
        )

        return

    # --------------------------------------------------------
    # Security: payment belongs to user
    # --------------------------------------------------------

    if (
        payment["user_id"]
        != callback.from_user.id
    ):

        await callback.answer(
            "⛔ This payment does not belong to you.",
            show_alert=True,
        )

        return

    # --------------------------------------------------------
    # Already approved
    # --------------------------------------------------------

    if payment["status"] == "approved":

        await callback.answer(
            "✅ This payment is already approved.",
            show_alert=True,
        )

        return

    # --------------------------------------------------------
    # Already rejected
    # --------------------------------------------------------

    if payment["status"] == "rejected":

        await callback.answer(
            "❌ This payment was rejected.",
            show_alert=True,
        )

        return

    # --------------------------------------------------------
    # Already submitted
    # --------------------------------------------------------

    if payment["status"] == "pending_admin":

        await callback.answer(
            "⏳ Your payment proof is already waiting for admin approval.",
            show_alert=True,
        )

        return

    # --------------------------------------------------------
    # Must be awaiting proof
    # --------------------------------------------------------

    if payment["status"] != "awaiting_proof":

        await callback.answer(
            "❌ This payment is no longer available.",
            show_alert=True,
        )

        return

    # --------------------------------------------------------
    # Mark proof requested
    # --------------------------------------------------------

    payment["proof_requested_at"] = now_ts()

    pending_payment[
        callback.from_user.id
    ] = payment_id

    await save_data()

    # --------------------------------------------------------
    # Ask screenshot
    # --------------------------------------------------------

    await callback.message.answer(
        "📸 <b>Send Payment Screenshot</b>\n\n"

        "Please send a clear screenshot of your "
        "successful UPI payment here.\n\n"

        f"🧾 Payment ID:\n"
        f"<code>{payment_id}</code>\n\n"

        f"💰 Amount: "
        f"<b>₹{money(payment['amount'])}</b>\n\n"

        "⚠️ Make sure the payment amount, "
        "transaction details and status are visible.\n\n"

        "After receiving your screenshot, "
        "the admin will manually verify it."
    )

    await callback.answer(
        "✅ Now send your payment screenshot."
    )


# ============================================================
# PAYMENT PROOF - PHOTO
# ============================================================

@dp.message(F.photo)
async def payment_photo_handler(
    message: Message,
):

    user_id = message.from_user.id

    payment_id = pending_payment.get(
        user_id
    )

    # --------------------------------------------------------
    # If memory was lost after restart,
    # find latest awaiting payment from JSON.
    # --------------------------------------------------------

    if not payment_id:

        latest_payment = None
        latest_id = None

        for pid, item in data[
            "payments"
        ].items():

            if (
                item.get("user_id")
                == user_id
                and item.get("status")
                == "awaiting_proof"
            ):

                if (
                    latest_payment is None
                    or item.get(
                        "created_at",
                        0,
                    )
                    > latest_payment.get(
                        "created_at",
                        0,
                    )
                ):

                    latest_payment = item
                    latest_id = pid

        if latest_id:

            payment_id = latest_id

            pending_payment[
                user_id
            ] = payment_id

    # --------------------------------------------------------
    # No payment
    # --------------------------------------------------------

    if not payment_id:

        await message.answer(
            "❌ No pending payment found.\n\n"
            "Please select a subscription plan first."
        )

        return

    payment = data[
        "payments"
    ].get(payment_id)

    if not payment:

        await message.answer(
            "❌ Payment not found.\n\n"
            "Please select the plan again."
        )

        return

    # --------------------------------------------------------
    # Check payment owner
    # --------------------------------------------------------

    if (
        payment["user_id"]
        != user_id
    ):

        await message.answer(
            "⛔ Invalid payment."
        )

        return

    # --------------------------------------------------------
    # Check status
    # --------------------------------------------------------

    if payment["status"] != "awaiting_proof":

        if payment["status"] == "pending_admin":

            await message.answer(
                "⏳ Your payment proof is already "
                "waiting for admin approval."
            )

        elif payment["status"] == "approved":

            await message.answer(
                "✅ Your payment is already approved."
            )

        elif payment["status"] == "rejected":

            await message.answer(
                "❌ This payment was rejected."
            )

        else:

            await message.answer(
                "❌ This payment is no longer active."
            )

        return

    # --------------------------------------------------------
    # Save proof
    # --------------------------------------------------------

    payment[
        "proof_message_id"
    ] = message.message_id

    payment[
        "status"
    ] = "pending_admin"

    await save_data()

    # --------------------------------------------------------
    # Admin caption
    # --------------------------------------------------------

    username = (
        f"@{esc(message.from_user.username)}"
        if message.from_user.username
        else "N/A"
    )

    admin_caption = (
        "🧾 <b>New Payment Proof</b>\n\n"

        f"🆔 Payment: "
        f"<code>{payment_id}</code>\n"

        f"👤 User ID: "
        f"<code>{user_id}</code>\n"

        f"👤 Name: "
        f"{esc(message.from_user.full_name)}\n"

        f"🔗 Username: "
        f"{username}\n\n"

        f"💰 Amount: "
        f"<b>₹{money(payment['amount'])}</b>\n"

        f"⏱ Duration: "
        f"<b>{format_duration(payment['minutes'])}</b>\n\n"

        "Choose an action:"
    )

    buttons = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="✅ APPROVE",
                    callback_data=(
                        f"approve:{payment_id}"
                    ),
                ),
                InlineKeyboardButton(
                    text="❌ REJECT",
                    callback_data=(
                        f"reject:{payment_id}"
                    ),
                ),
            ]
        ]
    )

    # --------------------------------------------------------
    # Send proof to admin
    # --------------------------------------------------------

    try:

        await bot.send_photo(
            chat_id=ADMIN_ID,
            photo=message.photo[-1].file_id,
            caption=admin_caption,
            reply_markup=buttons,
        )

    except Exception:

        logger.exception(
            "Could not send proof to admin"
        )

        payment[
            "status"
        ] = "awaiting_proof"

        await save_data()

        await message.answer(
            "⚠️ Could not send your proof to admin.\n"
            "Please try again."
        )

        return

    # --------------------------------------------------------
    # Optional proof channel
    # --------------------------------------------------------

    proof_channel_id = data[
        "settings"
    ].get(
        "proof_channel_id"
    )

    if proof_channel_id:

        try:

            await bot.send_photo(
                chat_id=int(
                    proof_channel_id
                ),
                photo=message.photo[-1].file_id,
                caption=admin_caption,
            )

        except Exception:

            logger.exception(
                "Could not send proof to proof channel"
            )

    # --------------------------------------------------------
    # User confirmation
    # --------------------------------------------------------

    pending_payment.pop(
        user_id,
        None,
    )

    await message.answer(
        "✅ <b>Payment Proof Received</b>\n\n"

        f"🧾 Payment ID: "
        f"<code>{payment_id}</code>\n\n"

        "⏳ Your payment is now waiting "
        "for admin approval.\n\n"

        "You will automatically receive "
        "the private channel invite after approval."
    )


# ============================================================
# APPROVE PAYMENT
# ============================================================

@dp.callback_query(
    F.data.startswith("approve:")
)
async def approve_payment(
    callback: CallbackQuery,
):

    if not is_admin(
        callback.from_user.id
    ):

        await callback.answer(
            "Unauthorized",
            show_alert=True,
        )

        return

    payment_id = callback.data.split(
        ":",
        1,
    )[1]

    payment = data[
        "payments"
    ].get(payment_id)

    if not payment:

        await callback.answer(
            "Payment not found.",
            show_alert=True,
        )

        return

    if payment["status"] == "approved":

        await callback.answer(
            "Already approved.",
            show_alert=True,
        )

        return

    if payment["status"] == "rejected":

        await callback.answer(
            "Already rejected.",
            show_alert=True,
        )

        return

    settings = data[
        "settings"
    ]

    channel_id = settings.get(
        "paid_channel_id"
    )

    if not channel_id:

        await callback.answer(
            "Paid channel is not configured.",
            show_alert=True,
        )

        return

    try:

        channel_id_int = int(
            channel_id
        )

        # ----------------------------------------------------
        # ONE USER / 5 MINUTE INVITE
        # ----------------------------------------------------

        invite = (
            await bot.create_chat_invite_link(
                chat_id=channel_id_int,
                name=f"payment-{payment_id}",
                expire_date=(
                    now_ts()
                    + LINK_LIFETIME
                ),
                member_limit=1,
            )
        )

    except Exception:

        logger.exception(
            "Invite creation failed"
        )

        await callback.answer(
            "Could not create invite link. "
            "Check bot channel permissions.",
            show_alert=True,
        )

        return

    # --------------------------------------------------------
    # Save approval
    # --------------------------------------------------------

    payment[
        "status"
    ] = "approved"

    payment[
        "approved_at"
    ] = now_ts()

    payment[
        "invite_link"
    ] = invite.invite_link

    user_id = payment[
        "user_id"
    ]

    data[
        "users"
    ].setdefault(
        str(user_id),
        {
            "user_id": user_id,
            "username": "",
            "first_name": "",
            "last_name": "",
            "created_at": now_ts(),
            "last_seen": now_ts(),
            "total_purchases": 0,
        },
    )

    data[
        "users"
    ][
        str(user_id)
    ][
        "total_purchases"
    ] += 1

    await save_data()

    # --------------------------------------------------------
    # Send invite
    # --------------------------------------------------------

    try:

        sent = await bot.send_message(
            chat_id=user_id,
            text=(
                "🎉 <b>Payment Approved!</b>\n\n"

                f"💎 Plan: "
                f"<b>₹{money(payment['amount'])}</b>\n"

                f"⏱ Duration: "
                f"<b>{format_duration(payment['minutes'])}</b>\n\n"

                "🔐 <b>Your private channel invite:</b>\n\n"

                f"👉 <a href=\"{invite.invite_link}\">"
                "JOIN PRIVATE CHANNEL"
                "</a>\n\n"

                "⚠️ <b>Important:</b>\n"

                "• This invite is valid for "
                "<b>5 minutes</b>.\n"

                "• The invite is limited to "
                "<b>1 user</b>.\n"

                "• Join the channel before "
                "the link expires.\n"

                "• After joining, your subscription "
                "timer starts.\n"

                f"• You will be removed automatically "
                f"after <b>{format_duration(payment['minutes'])}</b>.\n"

                "• After expiry, purchase a new plan "
                "to get access again."
            ),
        )

        # ----------------------------------------------------
        # Start invite monitor
        # ----------------------------------------------------

        old_task = invite_tasks.get(
            payment_id
        )

        if old_task and not old_task.done():
            old_task.cancel()

        task = asyncio.create_task(
            monitor_invite(
                payment_id=payment_id,
                user_id=user_id,
                channel_id=channel_id_int,
                invite_link=invite.invite_link,
                message_id=sent.message_id,
                minutes=payment["minutes"],
            )
        )

        invite_tasks[
            payment_id
        ] = task

    except Exception:

        logger.exception(
            "Could not send invite to user"
        )

    # --------------------------------------------------------
    # Update admin message
    # --------------------------------------------------------

    try:

        await callback.message.edit_reply_markup(
            reply_markup=None
        )

        if callback.message.caption:

            await callback.message.edit_caption(
                caption=(
                    callback.message.caption
                    + "\n\n"
                    "✅ <b>APPROVED</b>"
                )
            )

    except Exception:

        pass

    await callback.answer(
        "Payment approved."
    )


# ============================================================
# REJECT PAYMENT
# ============================================================

@dp.callback_query(
    F.data.startswith("reject:")
)
async def reject_payment(
    callback: CallbackQuery,
):

    if not is_admin(
        callback.from_user.id
    ):

        await callback.answer(
            "Unauthorized",
            show_alert=True,
        )

        return

    payment_id = callback.data.split(
        ":",
        1,
    )[1]

    payment = data[
        "payments"
    ].get(payment_id)

    if not payment:

        await callback.answer(
            "Payment not found.",
            show_alert=True,
        )

        return

    if payment["status"] == "approved":

        await callback.answer(
            "Already approved.",
            show_alert=True,
        )

        return

    if payment["status"] == "rejected":

        await callback.answer(
            "Already rejected.",
            show_alert=True,
        )

        return

    payment[
        "status"
    ] = "rejected"

    payment[
        "rejected_at"
    ] = now_ts()

    await save_data()

    pending_payment.pop(
        payment["user_id"],
        None,
    )

    # --------------------------------------------------------
    # Notify user
    # --------------------------------------------------------

    try:

        await bot.send_message(
            chat_id=payment["user_id"],
            text=(
                "❌ <b>Payment Rejected</b>\n\n"

                f"Payment ID: "
                f"<code>{payment_id}</code>\n\n"

                "Your payment proof was rejected "
                "by the admin.\n\n"

                "Please contact the admin if you "
                "believe this was a mistake."
            ),
            reply_markup=main_menu(),
        )

    except Exception:

        pass

    # --------------------------------------------------------
    # Update admin proof
    # --------------------------------------------------------

    try:

        await callback.message.edit_reply_markup(
            reply_markup=None
        )

        if callback.message.caption:

            await callback.message.edit_caption(
                caption=(
                    callback.message.caption
                    + "\n\n"
                    "❌ <b>REJECTED</b>"
                )
            )

    except Exception:

        pass

    await callback.answer(
        "Payment rejected."
    )


# ============================================================
# INVITE MONITOR
# ============================================================

async def monitor_invite(
    payment_id: str,
    user_id: int,
    channel_id: int,
    invite_link: str,
    message_id: int,
    minutes: int,
):
    """
    First invite:
    - Valid for 5 minutes.
    - Maximum 1 user.
    - Checks membership every 15 sec.
    - If user joins -> subscription starts.
    - If user does not join -> sends ONE fresh link.
    """

    try:

        deadline = (
            time.time()
            + LINK_LIFETIME
        )

        joined = False

        while time.time() < deadline:

            await asyncio.sleep(
                JOIN_CHECK_INTERVAL
            )

            try:

                member = (
                    await bot.get_chat_member(
                        chat_id=channel_id,
                        user_id=user_id,
                    )
                )

                if member.status in {
                    ChatMemberStatus.MEMBER,
                    ChatMemberStatus.ADMINISTRATOR,
                    ChatMemberStatus.CREATOR,
                }:

                    joined = True

                    break

            except TelegramBadRequest:

                # User has not joined yet.
                pass

            except TelegramRetryAfter as e:

                await asyncio.sleep(
                    e.retry_after
                )

            except TelegramNetworkError:

                await asyncio.sleep(5)

            except Exception:

                logger.exception(
                    "Join check failed"
                )

        # ----------------------------------------------------
        # User joined
        # ----------------------------------------------------

        if joined:

            await start_subscription(
                payment_id=payment_id,
                user_id=user_id,
                channel_id=channel_id,
                minutes=minutes,
            )

            try:

                await bot.delete_message(
                    chat_id=user_id,
                    message_id=message_id,
                )

            except Exception:

                pass

            return

        # ----------------------------------------------------
        # First link expired
        # ----------------------------------------------------

        try:

            await bot.delete_message(
                chat_id=user_id,
                message_id=message_id,
            )

        except Exception:

            pass

        # ----------------------------------------------------
        # Create ONE retry invite
        # ----------------------------------------------------

        try:

            new_invite = (
                await bot.create_chat_invite_link(
                    chat_id=channel_id,
                    name=f"retry-{payment_id}",
                    expire_date=(
                        now_ts()
                        + LINK_LIFETIME
                    ),
                    member_limit=1,
                )
            )

            data[
                "payments"
            ][
                payment_id
            ][
                "retry_invite_link"
            ] = new_invite.invite_link

            data[
                "payments"
            ][
                payment_id
            ][
                "retry_sent_at"
            ] = now_ts()

            await save_data()

            retry_message = (
                await bot.send_message(
                    chat_id=user_id,
                    text=(
                        "🔄 <b>New Join Link</b>\n\n"

                        "It looks like you did not "
                        "join using the previous link.\n\n"

                        "👉 <a href=\""
                        f"{new_invite.invite_link}"
                        "\">"
                        "JOIN PRIVATE CHANNEL"
                        "</a>\n\n"

                        "⚠️ <b>This link expires "
                        "in 5 minutes.</b>\n"

                        "⚠️ It can be used by "
                        "<b>1 user</b> only.\n\n"

                        "After joining, your selected "
                        "subscription duration will "
                        "begin automatically.\n\n"

                        "⏳ After the subscription "
                        "expires, you will be removed "
                        "from the channel."
                    ),
                )
            )

            await monitor_retry_invite(
                payment_id=payment_id,
                user_id=user_id,
                channel_id=channel_id,
                invite_link=new_invite.invite_link,
                message_id=retry_message.message_id,
                minutes=minutes,
            )

        except Exception:

            logger.exception(
                "Retry invite failed"
            )

    except asyncio.CancelledError:

        raise

    except Exception:

        logger.exception(
            "Invite monitor crashed"
        )


# ============================================================
# RETRY INVITE MONITOR
# ============================================================

async def monitor_retry_invite(
    payment_id: str,
    user_id: int,
    channel_id: int,
    invite_link: str,
    message_id: int,
    minutes: int,
):

    deadline = (
        time.time()
        + LINK_LIFETIME
    )

    while time.time() < deadline:

        await asyncio.sleep(
            JOIN_CHECK_INTERVAL
        )

        try:

            member = (
                await bot.get_chat_member(
                    chat_id=channel_id,
                    user_id=user_id,
                )
            )

            if member.status in {
                ChatMemberStatus.MEMBER,
                ChatMemberStatus.ADMINISTRATOR,
                ChatMemberStatus.CREATOR,
            }:

                await start_subscription(
                    payment_id=payment_id,
                    user_id=user_id,
                    channel_id=channel_id,
                    minutes=minutes,
                )

                try:

                    await bot.delete_message(
                        chat_id=user_id,
                        message_id=message_id,
                    )

                except Exception:

                    pass

                return

        except TelegramBadRequest:

            pass

        except TelegramRetryAfter as e:

            await asyncio.sleep(
                e.retry_after
            )

        except TelegramNetworkError:

            await asyncio.sleep(5)

        except Exception:

            logger.exception(
                "Retry join check failed"
            )

    # --------------------------------------------------------
    # Retry expired
    # --------------------------------------------------------

    try:

        await bot.delete_message(
            chat_id=user_id,
            message_id=message_id,
        )

    except Exception:

        pass

    try:

        await bot.send_message(
            chat_id=user_id,
            text=(
                "⌛ <b>Invite Link Expired</b>\n\n"

                "You did not join the channel "
                "within the allowed time.\n\n"

                "Please purchase a new subscription "
                "if you still want access."
            ),
            reply_markup=main_menu(),
        )

    except Exception:

        pass


# ============================================================
# START SUBSCRIPTION
# ============================================================

async def start_subscription(
    payment_id: str,
    user_id: int,
    channel_id: int,
    minutes: int,
):

    payment = data[
        "payments"
    ].get(payment_id)

    if not payment:
        return

    # --------------------------------------------------------
    # Prevent duplicate subscription
    # --------------------------------------------------------

    if payment.get(
        "subscription_started"
    ):

        return

    start = now_ts()

    expires = (
        start
        + (int(minutes) * 60)
    )

    subscription_id = (
        uuid.uuid4()
        .hex[:12]
    )

    data[
        "subscriptions"
    ][
        subscription_id
    ] = {

        "subscription_id":
            subscription_id,

        "payment_id":
            payment_id,

        "user_id":
            user_id,

        "channel_id":
            channel_id,

        "minutes":
            int(minutes),

        "started_at":
            start,

        "expires_at":
            expires,

        "status":
            "active",
    }

    payment[
        "subscription_started"
    ] = True

    payment[
        "subscription_id"
    ] = subscription_id

    await save_data()

    # --------------------------------------------------------
    # Notify user
    # --------------------------------------------------------

    try:

        await bot.send_message(
            chat_id=user_id,
            text=(
                "✅ <b>Subscription Activated</b>\n\n"

                f"⏱ Your access is valid for "
                f"<b>{format_duration(minutes)}</b>.\n\n"

                "The subscription timer has started.\n\n"

                "You will be automatically removed "
                "when it expires."
            ),
        )

    except Exception:

        pass

    # --------------------------------------------------------
    # Schedule expiry
    # --------------------------------------------------------

    task = asyncio.create_task(
        subscription_expiry_worker(
            subscription_id
        )
    )

    subscription_tasks[
        subscription_id
    ] = task


# ============================================================
# SUBSCRIPTION EXPIRY
# ============================================================

async def subscription_expiry_worker(
    subscription_id: str,
):

    try:

        subscription = data[
            "subscriptions"
        ].get(subscription_id)

        if not subscription:
            return

        remaining = (
            subscription[
                "expires_at"
            ]
            - now_ts()
        )

        if remaining > 0:

            await asyncio.sleep(
                remaining
            )

        subscription = data[
            "subscriptions"
        ].get(subscription_id)

        if not subscription:
            return

        if (
            subscription["status"]
            != "active"
        ):

            return

        user_id = subscription[
            "user_id"
        ]

        channel_id = subscription[
            "channel_id"
        ]

        # ----------------------------------------------------
        # Remove user
        # ----------------------------------------------------

        try:

            # Ban then unban.
            # This removes the user while allowing
            # future subscriptions.

            await bot.ban_chat_member(
                chat_id=channel_id,
                user_id=user_id,
            )

            await asyncio.sleep(1)

            try:

                await bot.unban_chat_member(
                    chat_id=channel_id,
                    user_id=user_id,
                    only_if_banned=True,
                )

            except Exception:

                pass

        except TelegramForbiddenError:

            logger.error(
                "Bot has no permission to remove user %s",
                user_id,
            )

        except TelegramBadRequest:

            logger.exception(
                "Could not remove expired user %s",
                user_id,
            )

        except Exception:

            logger.exception(
                "Unexpected removal error for user %s",
                user_id,
            )

        # ----------------------------------------------------
        # Mark expired
        # ----------------------------------------------------

        subscription[
            "status"
        ] = "expired"

        subscription[
            "expired_at"
        ] = now_ts()

        await save_data()

        # ----------------------------------------------------
        # Notify user
        # ----------------------------------------------------

        try:

            await bot.send_message(
                chat_id=user_id,
                text=(
                    "⌛ <b>Subscription Expired</b>\n\n"

                    "Your private channel access "
                    "has expired and you have been "
                    "removed from the channel.\n\n"

                    "To access the channel again, "
                    "purchase a new plan."
                ),
                reply_markup=main_menu(),
            )

        except Exception:

            pass

    except asyncio.CancelledError:

        raise

    except Exception:

        logger.exception(
            "Subscription worker crashed: %s",
            subscription_id,
        )

    finally:

        subscription_tasks.pop(
            subscription_id,
            None,
        )


# ============================================================
# MY SUBSCRIPTION
# ============================================================

@dp.callback_query(
    F.data == "my_subscription"
)
async def my_subscription(
    callback: CallbackQuery,
):

    user_id = callback.from_user.id

    active = []

    for sub in data[
        "subscriptions"
    ].values():

        if (
            sub["user_id"]
            == user_id
            and sub["status"]
            == "active"
        ):

            remaining = max(
                0,
                sub["expires_at"]
                - now_ts(),
            )

            active.append(
                (
                    sub,
                    remaining,
                )
            )

    if not active:

        await callback.message.edit_text(
            "ℹ️ <b>My Subscription</b>\n\n"
            "You don't have an active subscription.",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[
                    [
                        InlineKeyboardButton(
                            text="💎 Buy Subscription",
                            callback_data="plans",
                        )
                    ],
                    [
                        InlineKeyboardButton(
                            text="⬅️ Back",
                            callback_data="home",
                        )
                    ],
                ]
            ),
        )

        await callback.answer()

        return

    lines = [
        "ℹ️ <b>Active Subscription</b>\n"
    ]

    for sub, remaining in active:

        mins = remaining // 60
        secs = remaining % 60

        lines.append(
            f"💎 Plan: "
            f"<b>{format_duration(sub['minutes'])}</b>\n"
            f"⏳ Remaining: "
            f"<b>{mins}m {secs}s</b>\n"
        )

    await callback.message.edit_text(
        "\n".join(lines),
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

    await callback.answer()


# ============================================================
# HOME
# ============================================================

@dp.callback_query(
    F.data == "home"
)
async def home_callback(
    callback: CallbackQuery,
):

    await callback.message.edit_text(
        "👋 <b>Welcome!</b>\n\n"
        "💎 Purchase a subscription to access "
        "the private channel.",
        reply_markup=main_menu(),
    )

    await callback.answer()


# ============================================================
# ADMIN PLANS
# ============================================================

@dp.callback_query(
    F.data == "admin_plans"
)
async def admin_plans(
    callback: CallbackQuery,
):

    if not is_admin(
        callback.from_user.id
    ):
        return

    buttons = []

    for plan_id, plan in data[
        "plans"
    ].items():

        status = (
            "ON"
            if plan.get(
                "active",
                True,
            )
            else "OFF"
        )

        buttons.append(
            [
                InlineKeyboardButton(
                    text=(
                        f"{'🟢' if status == 'ON' else '🔴'} "
                        f"₹{money(plan['price'])} / "
                        f"{format_duration(plan['minutes'])}"
                    ),
                    callback_data=f"plan:{plan_id}",
                )
            ]
        )

    buttons.extend(
        [
            [
                InlineKeyboardButton(
                    text="➕ Add Plan",
                    callback_data="admin_add_plan",
                )
            ],
            [
                InlineKeyboardButton(
                    text="⬅️ Admin",
                    callback_data="admin_home",
                )
            ],
        ]
    )

    await callback.message.edit_text(
        "💎 <b>Subscription Plans</b>\n\n"
        "Tap a plan to manage it.",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=buttons
        ),
    )

    await callback.answer()


# ============================================================
# PLAN DETAILS
# ============================================================

@dp.callback_query(
    F.data.startswith("plan:")
)
async def plan_details(
    callback: CallbackQuery,
):

    if not is_admin(
        callback.from_user.id
    ):
        return

    plan_id = callback.data.split(
        ":",
        1,
    )[1]

    plan = data[
        "plans"
    ].get(plan_id)

    if not plan:

        await callback.answer(
            "Plan not found."
        )

        return

    status = (
        "Active"
        if plan.get(
            "active",
            True,
        )
        else "Disabled"
    )

    await callback.message.edit_text(
        "💎 <b>Plan</b>\n\n"

        f"🆔 ID: "
        f"<code>{plan_id}</code>\n"

        f"💰 Price: "
        f"₹{money(plan['price'])}\n"

        f"⏱ Duration: "
        f"{format_duration(plan['minutes'])}\n"

        f"📌 Status: "
        f"{status}",

        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text="🔄 Toggle",
                        callback_data=(
                            f"toggleplan:{plan_id}"
                        ),
                    ),
                    InlineKeyboardButton(
                        text="🗑 Delete",
                        callback_data=(
                            f"deleteplan:{plan_id}"
                        ),
                    ),
                ],
                [
                    InlineKeyboardButton(
                        text="⬅️ Plans",
                        callback_data="admin_plans",
                    )
                ],
            ]
        ),
    )

    await callback.answer()


# ============================================================
# ADD PLAN
# ============================================================

@dp.callback_query(
    F.data == "admin_add_plan"
)
async def admin_add_plan(
    callback: CallbackQuery,
):

    if not is_admin(
        callback.from_user.id
    ):
        return

    pending_admin_action[
        ADMIN_ID
    ] = "add_plan"

    await callback.message.answer(
        "➕ <b>Add Subscription Plan</b>\n\n"

        "Send in this format:\n\n"

        "<code>2 | 5</code>\n\n"

        "Meaning:\n"
        "₹2 for 5 minutes\n\n"

        "Another example:\n"
        "<code>49 | 60</code>\n"

        "₹49 for 60 minutes."
    )

    await callback.answer()


# ============================================================
# ADMIN TEXT INPUT
# ============================================================

@dp.message()
async def admin_text_handler(
    message: Message,
):

    if not is_admin(
        message.from_user.id
    ):
        return

    action = pending_admin_action.get(
        ADMIN_ID
    )

    if not action:
        return

    text = (
        message.text.strip()
        if message.text
        else ""
    )

    # ========================================================
    # ADD PLAN
    # ========================================================

    if action == "add_plan":

        try:

            parts = text.split("|")

            if len(parts) != 2:
                raise ValueError

            price_raw = parts[0].strip()
            minutes_raw = parts[1].strip()

            price = float(
                price_raw
            )

            minutes = int(
                minutes_raw
            )

            if price <= 0:
                raise ValueError

            if minutes <= 0:
                raise ValueError

            plan_id = (
                uuid.uuid4()
                .hex[:8]
            )

            data[
                "plans"
            ][
                plan_id
            ] = {

                "plan_id":
                    plan_id,

                "price":
                    price,

                "minutes":
                    minutes,

                "active":
                    True,

                "created_at":
                    now_ts(),
            }

            pending_admin_action.pop(
                ADMIN_ID,
                None,
            )

            await save_data()

            await message.answer(
                "✅ <b>Plan Added</b>\n\n"

                f"💰 ₹{money(price)}\n"
                f"⏱ {format_duration(minutes)}\n"
                f"🆔 <code>{plan_id}</code>",

                reply_markup=admin_menu(),
            )

        except Exception:

            await message.answer(
                "❌ Invalid format.\n\n"

                "Use:\n"
                "<code>2 | 5</code>\n\n"

                "Example:\n"
                "<code>99 | 60</code>"
            )

        return

    # ========================================================
    # UPI
    # ========================================================

    if action == "set_upi":

        parts = text.split("|")

        upi = parts[0].strip()

        if not upi:

            await message.answer(
                "❌ UPI ID cannot be empty."
            )

            return

        name = (
            parts[1].strip()
            if len(parts) > 1
            else "Subscription"
        )

        data[
            "settings"
        ][
            "upi_id"
        ] = upi

        data[
            "settings"
        ][
            "upi_name"
        ] = name

        pending_admin_action.pop(
            ADMIN_ID,
            None,
        )

        await save_data()

        await message.answer(
            "✅ <b>UPI Updated</b>\n\n"

            f"UPI: "
            f"<code>{esc(upi)}</code>\n"

            f"Name: "
            f"{esc(name)}",

            reply_markup=admin_menu(),
        )

        return

    # ========================================================
    # PAID CHANNEL
    # ========================================================

    if action == "set_channel":

        try:

            channel_id = int(text)

        except ValueError:

            await message.answer(
                "❌ Invalid channel ID.\n\n"

                "Example:\n"
                "<code>-1001234567890</code>"
            )

            return

        try:

            chat = await bot.get_chat(
                channel_id
            )

            data[
                "settings"
            ][
                "paid_channel_id"
            ] = str(
                channel_id
            )

            data[
                "settings"
            ][
                "paid_channel_title"
            ] = (
                chat.title or ""
            )

            pending_admin_action.pop(
                ADMIN_ID,
                None,
            )

            await save_data()

            await message.answer(
                "✅ <b>Paid Channel Saved</b>\n\n"

                f"📢 "
                f"{esc(chat.title)}\n"

                f"🆔 "
                f"<code>{channel_id}</code>",

                reply_markup=admin_menu(),
            )

        except Exception:

            await message.answer(
                "❌ Could not access this channel.\n\n"

                "Make sure the bot is an administrator "
                "in the channel."
            )

        return

    # ========================================================
    # PROOF CHANNEL
    # ========================================================

    if action == "set_proof":

        try:

            proof_id = int(text)

        except ValueError:

            await message.answer(
                "❌ Invalid channel ID."
            )

            return

        try:

            chat = await bot.get_chat(
                proof_id
            )

            data[
                "settings"
            ][
                "proof_channel_id"
            ] = str(
                proof_id
            )

            pending_admin_action.pop(
                ADMIN_ID,
                None,
            )

            await save_data()

            await message.answer(
                "✅ <b>Payment Proof Channel Saved</b>\n\n"

                f"📢 "
                f"{esc(chat.title)}\n"

                f"🆔 "
                f"<code>{proof_id}</code>",

                reply_markup=admin_menu(),
            )

        except Exception:

            await message.answer(
                "❌ Could not access the proof channel.\n\n"

                "Make sure the bot is an administrator "
                "there."
            )

        return


# ============================================================
# ADMIN UPI
# ============================================================

@dp.callback_query(
    F.data == "admin_upi"
)
async def admin_upi(
    callback: CallbackQuery,
):

    if not is_admin(
        callback.from_user.id
    ):
        return

    settings = data[
        "settings"
    ]

    await callback.message.edit_text(
        "💳 <b>UPI Settings</b>\n\n"

        "Current UPI:\n"
        f"<code>"
        f"{esc(settings.get('upi_id')) or 'Not set'}"
        f"</code>\n\n"

        "Name:\n"
        f"<code>"
        f"{esc(settings.get('upi_name')) or 'Not set'}"
        f"</code>",

        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text="✏️ Set UPI",
                        callback_data="set_upi",
                    )
                ],
                [
                    InlineKeyboardButton(
                        text="⬅️ Admin",
                        callback_data="admin_home",
                    )
                ],
            ]
        ),
    )

    await callback.answer()


# ============================================================
# SET UPI
# ============================================================

@dp.callback_query(
    F.data == "set_upi"
)
async def set_upi_callback(
    callback: CallbackQuery,
):

    if not is_admin(
        callback.from_user.id
    ):
        return

    pending_admin_action[
        ADMIN_ID
    ] = "set_upi"

    await callback.message.answer(
        "💳 Send UPI in this format:\n\n"

        "<code>yourupi@upi | Your Name</code>\n\n"

        "Example:\n"

        "<code>amrit@upi | Amrit</code>"
    )

    await callback.answer()


# ============================================================
# ADMIN CHANNEL
# ============================================================

@dp.callback_query(
    F.data == "admin_channel"
)
async def admin_channel(
    callback: CallbackQuery,
):

    if not is_admin(
        callback.from_user.id
    ):
        return

    settings = data[
        "settings"
    ]

    await callback.message.edit_text(
        "📢 <b>Paid Channel</b>\n\n"

        f"Title: "
        f"<b>"
        f"{esc(settings.get('paid_channel_title')) or 'Not set'}"
        f"</b>\n"

        f"ID: "
        f"<code>"
        f"{esc(settings.get('paid_channel_id')) or 'Not set'}"
        f"</code>",

        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text="✏️ Set Channel",
                        callback_data="set_channel",
                    )
                ],
                [
                    InlineKeyboardButton(
                        text="⬅️ Admin",
                        callback_data="admin_home",
                    )
                ],
            ]
        ),
    )

    await callback.answer()


# ============================================================
# SET CHANNEL
# ============================================================

@dp.callback_query(
    F.data == "set_channel"
)
async def set_channel_callback(
    callback: CallbackQuery,
):

    if not is_admin(
        callback.from_user.id
    ):
        return

    pending_admin_action[
        ADMIN_ID
    ] = "set_channel"

    await callback.message.answer(
        "📢 Send the private channel ID.\n\n"

        "Example:\n"
        "<code>-1001234567890</code>\n\n"

        "The bot must be an administrator "
        "in that channel."
    )

    await callback.answer()


# ============================================================
# ADMIN PROOF CHANNEL
# ============================================================

@dp.callback_query(
    F.data == "admin_proof"
)
async def admin_proof(
    callback: CallbackQuery,
):

    if not is_admin(
        callback.from_user.id
    ):
        return

    proof = data[
        "settings"
    ].get(
        "proof_channel_id"
    )

    await callback.message.edit_text(
        "🧾 <b>Payment Proof Channel</b>\n\n"

        "Current:\n"
        f"<code>"
        f"{esc(proof) or 'Not set'}"
        f"</code>",

        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text="✏️ Set Proof Channel",
                        callback_data="set_proof",
                    )
                ],
                [
                    InlineKeyboardButton(
                        text="⬅️ Admin",
                        callback_data="admin_home",
                    )
                ],
            ]
        ),
    )

    await callback.answer()


# ============================================================
# SET PROOF
# ============================================================

@dp.callback_query(
    F.data == "set_proof"
)
async def set_proof_callback(
    callback: CallbackQuery,
):

    if not is_admin(
        callback.from_user.id
    ):
        return

    pending_admin_action[
        ADMIN_ID
    ] = "set_proof"

    await callback.message.answer(
        "🧾 Send the payment proof channel ID.\n\n"

        "Example:\n"
        "<code>-1001234567890</code>\n\n"

        "The bot must be able to post there."
    )

    await callback.answer()


# ============================================================
# TOGGLE PLAN
# ============================================================

@dp.callback_query(
    F.data.startswith("toggleplan:")
)
async def toggle_plan(
    callback: CallbackQuery,
):

    if not is_admin(
        callback.from_user.id
    ):
        return

    plan_id = callback.data.split(
        ":",
        1,
    )[1]

    plan = data[
        "plans"
    ].get(plan_id)

    if not plan:

        await callback.answer(
            "Plan not found."
        )

        return

    plan[
        "active"
    ] = not plan.get(
        "active",
        True,
    )

    await save_data()

    await callback.answer(
        "Plan status updated."
    )

    await plan_details(
        callback
    )


# ============================================================
# DELETE PLAN
# ============================================================

@dp.callback_query(
    F.data.startswith("deleteplan:")
)
async def delete_plan(
    callback: CallbackQuery,
):

    if not is_admin(
        callback.from_user.id
    ):
        return

    plan_id = callback.data.split(
        ":",
        1,
    )[1]

    if plan_id in data[
        "plans"
    ]:

        del data[
            "plans"
        ][
            plan_id
        ]

    await save_data()

    await callback.answer(
        "Plan deleted."
    )

    await admin_plans(
        callback
    )


# ============================================================
# ADMIN USERS
# ============================================================

@dp.callback_query(
    F.data == "admin_users"
)
async def admin_users(
    callback: CallbackQuery,
):

    if not is_admin(
        callback.from_user.id
    ):
        return

    users = data[
        "users"
    ]

    await callback.message.edit_text(
        "👥 <b>Users</b>\n\n"

        f"Total users: "
        f"<b>{len(users)}</b>\n\n"

        "Use Statistics for detailed numbers.",

        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text="📊 Statistics",
                        callback_data="admin_stats",
                    )
                ],
                [
                    InlineKeyboardButton(
                        text="⬅️ Admin",
                        callback_data="admin_home",
                    )
                ],
            ]
        ),
    )

    await callback.answer()


# ============================================================
# ADMIN PAYMENTS
# ============================================================

@dp.callback_query(
    F.data == "admin_payments"
)
async def admin_payments(
    callback: CallbackQuery,
):

    if not is_admin(
        callback.from_user.id
    ):
        return

    payments = list(
        data[
            "payments"
        ].values()
    )

    total = len(
        payments
    )

    pending = sum(
        1
        for p in payments
        if p["status"]
        == "pending_admin"
    )

    approved = sum(
        1
        for p in payments
        if p["status"]
        == "approved"
    )

    rejected = sum(
        1
        for p in payments
        if p["status"]
        == "rejected"
    )

    revenue = sum(
        float(p["amount"])
        for p in payments
        if p["status"]
        == "approved"
    )

    await callback.message.edit_text(
        "💰 <b>Payments</b>\n\n"

        f"Total: "
        f"<b>{total}</b>\n"

        f"⏳ Pending: "
        f"<b>{pending}</b>\n"

        f"✅ Approved: "
        f"<b>{approved}</b>\n"

        f"❌ Rejected: "
        f"<b>{rejected}</b>\n\n"

        f"💵 Approved Revenue: "
        f"<b>₹{money(revenue)}</b>",

        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text="⬅️ Admin",
                        callback_data="admin_home",
                    )
                ]
            ]
        ),
    )

    await callback.answer()


# ============================================================
# ADMIN STATS
# ============================================================

@dp.callback_query(
    F.data == "admin_stats"
)
async def admin_stats(
    callback: CallbackQuery,
):

    if not is_admin(
        callback.from_user.id
    ):
        return

    users = len(
        data[
            "users"
        ]
    )

    payments = list(
        data[
            "payments"
        ].values()
    )

    subscriptions = list(
        data[
            "subscriptions"
        ].values()
    )

    approved = [
        p
        for p in payments
        if p["status"]
        == "approved"
    ]

    active = [
        s
        for s in subscriptions
        if s["status"]
        == "active"
    ]

    expired = [
        s
        for s in subscriptions
        if s["status"]
        == "expired"
    ]

    revenue = sum(
        float(p["amount"])
        for p in approved
    )

    await callback.message.edit_text(
        "📊 <b>Bot Statistics</b>\n\n"

        f"👥 Users: "
        f"<b>{users}</b>\n"

        f"💳 Payments: "
        f"<b>{len(payments)}</b>\n"

        f"✅ Approved: "
        f"<b>{len(approved)}</b>\n"

        f"🟢 Active subscriptions: "
        f"<b>{len(active)}</b>\n"

        f"⌛ Expired subscriptions: "
        f"<b>{len(expired)}</b>\n"

        f"💰 Revenue: "
        f"<b>₹{money(revenue)}</b>\n"

        f"💎 Plans: "
        f"<b>{len(data['plans'])}</b>",

        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text="⬅️ Admin",
                        callback_data="admin_home",
                    )
                ]
            ]
        ),
    )

    await callback.answer()


# ============================================================
# ADMIN HOME
# ============================================================

@dp.callback_query(
    F.data == "admin_home"
)
async def admin_home(
    callback: CallbackQuery,
):

    if not is_admin(
        callback.from_user.id
    ):
        return

    await callback.message.edit_text(
        "🛠️ <b>Admin Panel</b>\n\n"
        "Choose an option:",
        reply_markup=admin_menu(),
    )

    await callback.answer()


# ============================================================
# RESTORE SUBSCRIPTIONS
# ============================================================

async def restore_subscriptions():

    logger.info(
        "Restoring subscriptions..."
    )

    restored = 0

    for (
        subscription_id,
        subscription,
    ) in data[
        "subscriptions"
    ].items():

        if (
            subscription.get(
                "status"
            )
            != "active"
        ):

            continue

        remaining = (
            subscription[
                "expires_at"
            ]
            - now_ts()
        )

        task = asyncio.create_task(
            subscription_expiry_worker(
                subscription_id
            )
        )

        subscription_tasks[
            subscription_id
        ] = task

        restored += 1

        if remaining <= 0:

            logger.info(
                "Subscription %s already expired.",
                subscription_id,
            )

    logger.info(
        "Restored %s active subscription(s)",
        restored,
    )


# ============================================================
# RENDER HEALTH SERVER
# ============================================================

async def health(
    request: web.Request,
):

    return web.Response(
        text="OK",
        status=200,
    )


async def start_web_server():

    app = web.Application()

    app.router.add_get(
        "/",
        health,
    )

    app.router.add_get(
        "/health",
        health,
    )

    runner = web.AppRunner(
        app
    )

    await runner.setup()

    site = web.TCPSite(
        runner,
        HOST,
        PORT,
    )

    await site.start()

    logger.info(
        "Health server running on %s:%s",
        HOST,
        PORT,
    )

    return runner


# ============================================================
# MAIN
# ============================================================

async def main():

    logger.info(
        "Starting paid subscription bot..."
    )

    logger.info(
        "Configured ADMIN_ID=%s",
        ADMIN_ID,
    )

    # --------------------------------------------------------
    # Remove webhook
    # --------------------------------------------------------

    try:

        await bot.delete_webhook(
            drop_pending_updates=False
        )

    except Exception:

        logger.exception(
            "Could not delete webhook"
        )

    # --------------------------------------------------------
    # Restore subscriptions
    # --------------------------------------------------------

    await restore_subscriptions()

    # --------------------------------------------------------
    # Start Render web server
    # --------------------------------------------------------

    runner = await start_web_server()

    try:

        await dp.start_polling(
            bot,
            allowed_updates=(
                dp.resolve_used_update_types()
            ),
        )

    finally:

        # Cancel invite tasks
        for task in list(
            invite_tasks.values()
        ):

            if not task.done():
                task.cancel()

        # Cancel subscription tasks
        for task in list(
            subscription_tasks.values()
        ):

            if not task.done():
                task.cancel()

        await runner.cleanup()

        await bot.session.close()


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":

    asyncio.run(
        main()
)
