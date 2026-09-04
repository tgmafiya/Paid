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
    ChatMemberUpdated,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)

# ============================================================
# CONFIG
# ============================================================

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))
DATA_FILE = Path(os.getenv("DATA_FILE", "data.json"))

HOST = "0.0.0.0"
PORT = int(os.getenv("PORT", "10000"))

LINK_LIFETIME = 5 * 60

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

logger = logging.getLogger("paid-subscription-bot")


# ============================================================
# DATA
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


data_lock = asyncio.Lock()


def load_data() -> dict[str, Any]:
    if not DATA_FILE.exists():
        DATA_FILE.parent.mkdir(parents=True, exist_ok=True)
        save_data_sync(DEFAULT_DATA.copy())
        return json.loads(json.dumps(DEFAULT_DATA))

    try:
        with DATA_FILE.open("r", encoding="utf-8") as f:
            loaded = json.load(f)

        # Ensure missing top-level keys exist.
        for key, value in DEFAULT_DATA.items():
            if key not in loaded:
                loaded[key] = json.loads(json.dumps(value))

        return loaded

    except Exception:
        logger.exception("Could not load data.json")
        return json.loads(json.dumps(DEFAULT_DATA))


def save_data_sync(obj: dict[str, Any]) -> None:
    DATA_FILE.parent.mkdir(parents=True, exist_ok=True)

    temp = DATA_FILE.with_suffix(".tmp")

    with temp.open("w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, ensure_ascii=False)

    temp.replace(DATA_FILE)


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

pending_payment_plan: dict[int, str] = {}
pending_admin_action: dict[int, str] = {}

invite_tasks: dict[str, asyncio.Task] = {}
subscription_tasks: dict[str, asyncio.Task] = {}


# ============================================================
# HELPERS
# ============================================================

def is_admin(user_id: int) -> bool:
    return user_id == ADMIN_ID


def now_ts() -> int:
    return int(time.time())


def money(amount: float | int) -> str:
    value = float(amount)

    if value.is_integer():
        return str(int(value))

    return f"{value:.2f}".rstrip("0").rstrip(".")


def esc(value: Any) -> str:
    return html.escape(str(value))


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
# UPI
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

    return "upi://pay?" + urllib.parse.urlencode(params)


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
        qr_dir.mkdir(exist_ok=True)

        path = qr_dir / f"{payment_id}.png"

        img = qrcode.make(url)
        img.save(path)

        return str(path)

    except Exception:
        logger.exception("QR generation failed")
        return None


# ============================================================
# ADMIN / USER DATA
# ============================================================

def ensure_user(message: Message) -> None:
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
        data["users"][uid]["username"] = user.username or ""
        data["users"][uid]["first_name"] = user.first_name or ""
        data["users"][uid]["last_name"] = user.last_name or ""
        data["users"][uid]["last_seen"] = now_ts()


# ============================================================
# START
# ============================================================

@dp.message(CommandStart())
async def start_handler(message: Message):
    ensure_user(message)
    await save_data()

    text = (
        "👋 <b>Welcome!</b>\n\n"
        "💎 Purchase a subscription to access the private channel.\n\n"
        "Choose a plan below:"
    )

    await message.answer(
        text,
        reply_markup=main_menu(),
    )


# ============================================================
# ADMIN
# ============================================================

@dp.message(Command("admin"))
async def admin_handler(message: Message):
    if not is_admin(message.from_user.id):
        return

    await message.answer(
        "🛠️ <b>Admin Panel</b>\n\n"
        "Choose an option:",
        reply_markup=admin_menu(),
    )


# ============================================================
# PLANS
# ============================================================

@dp.callback_query(F.data == "plans")
async def plans_callback(callback: CallbackQuery):
    plans = data["plans"]

    if not plans:
        await callback.answer(
            "No plans are available right now.",
            show_alert=True,
        )
        return

    buttons = []

    for plan_id, plan in plans.items():
        if not plan.get("active", True):
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


def format_duration(minutes: int) -> str:
    minutes = int(minutes)

    if minutes < 60:
        return f"{minutes} min"

    hours = minutes // 60
    rem = minutes % 60

    if rem == 0:
        return f"{hours} hour" if hours == 1 else f"{hours} hours"

    return f"{hours}h {rem}m"


# ============================================================
# BUY PLAN
# ============================================================

@dp.callback_query(F.data.startswith("buy:"))
async def buy_plan_callback(callback: CallbackQuery):
    plan_id = callback.data.split(":", 1)[1]

    plan = data["plans"].get(plan_id)

    if not plan or not plan.get("active", True):
        await callback.answer(
            "This plan is unavailable.",
            show_alert=True,
        )
        return

    settings = data["settings"]

    if not settings["upi_id"]:
        await callback.answer(
            "Payment system is not configured yet.",
            show_alert=True,
        )
        return

    if not settings["paid_channel_id"]:
        await callback.answer(
            "Paid channel is not configured yet.",
            show_alert=True,
        )
        return

    user_id = callback.from_user.id

    pending_payment_plan[user_id] = plan_id

    payment_id = uuid.uuid4().hex[:10].upper()

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
    }

    await save_data()

    qr_path = await create_qr_file(
        settings["upi_id"],
        settings["upi_name"],
        plan["price"],
        payment_id,
    )

    text = (
        "💳 <b>Payment Details</b>\n\n"
        f"💎 Plan: <b>₹{money(plan['price'])}</b>\n"
        f"⏱ Duration: <b>{format_duration(plan['minutes'])}</b>\n\n"
        f"🏦 UPI ID:\n"
        f"<code>{esc(settings['upi_id'])}</code>\n\n"
        f"🧾 Payment ID:\n"
        f"<code>{payment_id}</code>\n\n"
        "📱 Pay the exact amount using UPI.\n"
        "Then send the <b>payment screenshot</b> here.\n\n"
        "⚠️ Your subscription will be activated only after admin approval."
    )

    if qr_path:
        from aiogram.types import FSInputFile

        await callback.message.answer_photo(
            photo=FSInputFile(qr_path),
            caption=text,
        )

    else:
        await callback.message.answer(text)

    await callback.answer()


# ============================================================
# PAYMENT PROOF
# ============================================================

@dp.message(F.photo)
async def payment_photo_handler(message: Message):
    user_id = message.from_user.id

    plan_id = pending_payment_plan.get(user_id)

    if not plan_id:
        await message.answer(
            "Please select a subscription plan first."
        )
        return

    # Find latest awaiting payment.
    payment = None
    payment_id = None

    for pid, item in reversed(list(data["payments"].items())):
        if (
            item["user_id"] == user_id
            and item["plan_id"] == plan_id
            and item["status"] == "awaiting_proof"
        ):
            payment = item
            payment_id = pid
            break

    if not payment:
        await message.answer(
            "No pending payment found. Please select the plan again."
        )
        return

    payment["proof_message_id"] = message.message_id
    payment["status"] = "pending_admin"

    await save_data()

    settings = data["settings"]

    proof_channel_id = settings.get("proof_channel_id")

    admin_caption = (
        "🧾 <b>New Payment Proof</b>\n\n"
        f"🆔 Payment: <code>{payment_id}</code>\n"
        f"👤 User ID: <code>{user_id}</code>\n"
        f"👤 Name: {esc(message.from_user.full_name)}\n"
        f"🔗 Username: "
        f"{'@' + esc(message.from_user.username) if message.from_user.username else 'N/A'}\n\n"
        f"💰 Amount: <b>₹{money(payment['amount'])}</b>\n"
        f"⏱ Duration: <b>{format_duration(payment['minutes'])}</b>\n\n"
        "Choose an action:"
    )

    buttons = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="✅ APPROVE",
                    callback_data=f"approve:{payment_id}",
                ),
                InlineKeyboardButton(
                    text="❌ REJECT",
                    callback_data=f"reject:{payment_id}",
                ),
            ]
        ]
    )

    # Send proof to admin.
    await bot.send_photo(
        chat_id=ADMIN_ID,
        photo=message.photo[-1].file_id,
        caption=admin_caption,
        reply_markup=buttons,
    )

    # Optional proof channel.
    if proof_channel_id:
        try:
            await bot.send_photo(
                chat_id=int(proof_channel_id),
                photo=message.photo[-1].file_id,
                caption=admin_caption,
            )
        except Exception:
            logger.exception("Could not send proof to proof channel")

    await message.answer(
        "✅ <b>Payment proof received.</b>\n\n"
        "Your payment is now waiting for admin approval.\n"
        "You will receive the channel link after approval."
    )


# ============================================================
# APPROVE
# ============================================================

@dp.callback_query(F.data.startswith("approve:"))
async def approve_payment(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("Unauthorized", show_alert=True)
        return

    payment_id = callback.data.split(":", 1)[1]

    payment = data["payments"].get(payment_id)

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

    settings = data["settings"]

    channel_id = settings.get("paid_channel_id")

    if not channel_id:
        await callback.answer(
            "Paid channel is not configured.",
            show_alert=True,
        )
        return

    try:
        channel_id_int = int(channel_id)

        # Make one-user invite link.
        invite = await bot.create_chat_invite_link(
            chat_id=channel_id_int,
            name=f"payment-{payment_id}",
            expire_date=now_ts() + LINK_LIFETIME,
            member_limit=1,
        )

    except Exception:
        logger.exception("Invite creation failed")

        await callback.answer(
            "Could not create invite link. Check bot channel permissions.",
            show_alert=True,
        )
        return

    payment["status"] = "approved"
    payment["approved_at"] = now_ts()
    payment["invite_link"] = invite.invite_link

    user_id = payment["user_id"]

    data["users"].setdefault(
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

    data["users"][str(user_id)]["total_purchases"] += 1

    await save_data()

    # Send link to user.
    try:
        sent = await bot.send_message(
            chat_id=user_id,
            text=(
                "🎉 <b>Payment Approved!</b>\n\n"
                f"💎 Plan: <b>₹{money(payment['amount'])}</b>\n"
                f"⏱ Duration: <b>{format_duration(payment['minutes'])}</b>\n\n"
                "🔐 <b>Your private channel invite:</b>\n\n"
                f"👉 <a href=\"{invite.invite_link}\">JOIN PRIVATE CHANNEL</a>\n\n"
                "⚠️ <b>Important:</b>\n"
                "• This invite is valid for <b>5 minutes</b>.\n"
                "• The invite is limited to <b>1 user</b>.\n"
                "• Join the channel before the link expires.\n"
                "• After joining, your subscription timer starts.\n"
                f"• You will be removed automatically after "
                f"<b>{format_duration(payment['minutes'])}</b>.\n"
                "• After expiry, contact the bot to purchase a new plan."
            ),
        )

        # Start invite monitoring.
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

        invite_tasks[payment_id] = task

    except Exception:
        logger.exception("Could not send invite to user")

    try:
        await callback.message.edit_reply_markup(
            reply_markup=None
        )

        await callback.message.edit_caption(
            caption=(
                callback.message.caption
                + "\n\n"
                "✅ <b>APPROVED</b>"
            )
        )

    except Exception:
        pass

    await callback.answer("Payment approved.")


# ============================================================
# REJECT
# ============================================================

@dp.callback_query(F.data.startswith("reject:"))
async def reject_payment(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("Unauthorized", show_alert=True)
        return

    payment_id = callback.data.split(":", 1)[1]

    payment = data["payments"].get(payment_id)

    if not payment:
        await callback.answer(
            "Payment not found.",
            show_alert=True,
        )
        return

    payment["status"] = "rejected"
    payment["rejected_at"] = now_ts()

    await save_data()

    try:
        await bot.send_message(
            chat_id=payment["user_id"],
            text=(
                "❌ <b>Payment Rejected</b>\n\n"
                f"Payment ID: <code>{payment_id}</code>\n\n"
                "Your payment proof was rejected by the admin.\n"
                "Please contact the admin if you believe this was a mistake."
            ),
        )
    except Exception:
        pass

    try:
        await callback.message.edit_reply_markup(
            reply_markup=None
        )

        await callback.message.edit_caption(
            caption=(
                callback.message.caption
                + "\n\n"
                "❌ <b>REJECTED</b>"
            )
        )
    except Exception:
        pass

    await callback.answer("Payment rejected.")


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
    Checks whether the user joined.

    If not joined after 5 minutes:
    - Delete the old message.
    - Create a fresh invite.
    - Send a fresh link once.

    This repeats only once, as requested.
    """

    try:
        # Check every 15 seconds for 5 minutes.
        deadline = time.time() + LINK_LIFETIME

        joined = False

        while time.time() < deadline:
            await asyncio.sleep(15)

            try:
                member = await bot.get_chat_member(
                    chat_id=channel_id,
                    user_id=user_id,
                )

                if member.status in {
                    ChatMemberStatus.MEMBER,
                    ChatMemberStatus.ADMINISTRATOR,
                    ChatMemberStatus.CREATOR,
                }:
                    joined = True
                    break

            except TelegramBadRequest:
                # User is not a member yet.
                pass

            except Exception:
                logger.exception("Join check failed")

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

        # Delete expired invite message.
        try:
            await bot.delete_message(
                chat_id=user_id,
                message_id=message_id,
            )
        except Exception:
            pass

        # Recreate a fresh one-time link.
        try:
            new_invite = await bot.create_chat_invite_link(
                chat_id=channel_id,
                name=f"retry-{payment_id}",
                expire_date=now_ts() + LINK_LIFETIME,
                member_limit=1,
            )

            data["payments"][payment_id][
                "retry_invite_link"
            ] = new_invite.invite_link

            data["payments"][payment_id][
                "retry_sent_at"
            ] = now_ts()

            await save_data()

            retry_message = await bot.send_message(
                chat_id=user_id,
                text=(
                    "🔄 <b>New Join Link</b>\n\n"
                    "It looks like you did not join using the previous link.\n\n"
                    "👉 <a href=\""
                    f"{new_invite.invite_link}"
                    "\">JOIN PRIVATE CHANNEL</a>\n\n"
                    "⚠️ <b>This link expires in 5 minutes.</b>\n"
                    "⚠️ It can be used by only 1 user.\n\n"
                    "After joining, your selected subscription duration "
                    "will begin automatically.\n\n"
                    "⏳ After the subscription expires, you will be "
                    "removed from the channel."
                ),
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
            logger.exception("Retry invite failed")

    except asyncio.CancelledError:
        raise

    except Exception:
        logger.exception("Invite monitor crashed")


async def monitor_retry_invite(
    payment_id: str,
    user_id: int,
    channel_id: int,
    invite_link: str,
    message_id: int,
    minutes: int,
):
    deadline = time.time() + LINK_LIFETIME

    while time.time() < deadline:
        await asyncio.sleep(15)

        try:
            member = await bot.get_chat_member(
                chat_id=channel_id,
                user_id=user_id,
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

        except Exception:
            logger.exception("Retry join check failed")

    # Expired again.
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
                "You did not join the channel within the allowed time.\n\n"
                "Please purchase a new subscription if you still want access."
            ),
        )
    except Exception:
        pass


# ============================================================
# SUBSCRIPTION
# ============================================================

async def start_subscription(
    payment_id: str,
    user_id: int,
    channel_id: int,
    minutes: int,
):
    payment = data["payments"].get(payment_id)

    if not payment:
        return

    # Prevent duplicate subscription.
    if payment.get("subscription_started"):
        return

    start = now_ts()
    expires = start + (minutes * 60)

    subscription_id = uuid.uuid4().hex[:12]

    data["subscriptions"][subscription_id] = {
        "subscription_id": subscription_id,
        "payment_id": payment_id,
        "user_id": user_id,
        "channel_id": channel_id,
        "minutes": minutes,
        "started_at": start,
        "expires_at": expires,
        "status": "active",
    }

    payment["subscription_started"] = True
    payment["subscription_id"] = subscription_id

    await save_data()

    # Notify user.
    try:
        await bot.send_message(
            chat_id=user_id,
            text=(
                "✅ <b>Subscription Activated</b>\n\n"
                f"⏱ Your access is valid for "
                f"<b>{format_duration(minutes)}</b>.\n\n"
                "The subscription timer has started.\n"
                "You will be automatically removed when it expires."
            ),
        )
    except Exception:
        pass

    # Schedule removal.
    task = asyncio.create_task(
        subscription_expiry_worker(
            subscription_id=subscription_id
        )
    )

    subscription_tasks[subscription_id] = task


async def subscription_expiry_worker(
    subscription_id: str,
):
    try:
        subscription = data["subscriptions"].get(
            subscription_id
        )

        if not subscription:
            return

        remaining = (
            subscription["expires_at"] - now_ts()
        )

        if remaining > 0:
            await asyncio.sleep(remaining)

        subscription = data["subscriptions"].get(
            subscription_id
        )

        if not subscription:
            return

        if subscription["status"] != "active":
            return

        user_id = subscription["user_id"]
        channel_id = subscription["channel_id"]

        try:
            # Ban then unban = remove user while allowing
            # future rejoining.
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

        subscription["status"] = "expired"
        subscription["expired_at"] = now_ts()

        await save_data()

        try:
            await bot.send_message(
                chat_id=user_id,
                text=(
                    "⌛ <b>Subscription Expired</b>\n\n"
                    "Your private channel access has expired "
                    "and you have been removed from the channel.\n\n"
                    "To access the channel again, purchase a new plan."
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


# ============================================================
# MY SUBSCRIPTION
# ============================================================

@dp.callback_query(F.data == "my_subscription")
async def my_subscription(callback: CallbackQuery):
    user_id = callback.from_user.id

    active = []

    for sub in data["subscriptions"].values():
        if (
            sub["user_id"] == user_id
            and sub["status"] == "active"
        ):
            remaining = max(
                0,
                sub["expires_at"] - now_ts(),
            )

            active.append(
                (sub, remaining)
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
            f"💎 Plan: <b>{format_duration(sub['minutes'])}</b>\n"
            f"⏳ Remaining: <b>{mins}m {secs}s</b>\n"
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


@dp.callback_query(F.data == "home")
async def home_callback(callback: CallbackQuery):
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

@dp.callback_query(F.data == "admin_plans")
async def admin_plans(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return

    buttons = []

    for plan_id, plan in data["plans"].items():
        status = "ON" if plan.get("active", True) else "OFF"

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


@dp.callback_query(F.data.startswith("plan:"))
async def plan_details(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return

    plan_id = callback.data.split(":", 1)[1]
    plan = data["plans"].get(plan_id)

    if not plan:
        await callback.answer("Plan not found.")
        return

    status = "Active" if plan.get("active", True) else "Disabled"

    await callback.message.edit_text(
        "💎 <b>Plan</b>\n\n"
        f"🆔 ID: <code>{plan_id}</code>\n"
        f"💰 Price: ₹{money(plan['price'])}\n"
        f"⏱ Duration: {format_duration(plan['minutes'])}\n"
        f"📌 Status: {status}",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text="🔄 Toggle",
                        callback_data=f"toggleplan:{plan_id}",
                    ),
                    InlineKeyboardButton(
                        text="🗑 Delete",
                        callback_data=f"deleteplan:{plan_id}",
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


@dp.callback_query(F.data == "admin_add_plan")
async def admin_add_plan(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return

    pending_admin_action[ADMIN_ID] = "add_plan"

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


@dp.message()
async def admin_text_handler(message: Message):
    if not is_admin(message.from_user.id):
        return

    action = pending_admin_action.get(ADMIN_ID)

    if not action:
        return

    text = message.text.strip() if message.text else ""

    # --------------------------------------------------------
    # ADD PLAN
    # --------------------------------------------------------

    if action == "add_plan":
        try:
            price_raw, minutes_raw = [
                x.strip()
                for x in text.split("|")
            ]

            price = float(price_raw)
            minutes = int(minutes_raw)

            if price <= 0 or minutes <= 0:
                raise ValueError

            plan_id = uuid.uuid4().hex[:8]

            data["plans"][plan_id] = {
                "plan_id": plan_id,
                "price": price,
                "minutes": minutes,
                "active": True,
                "created_at": now_ts(),
            }

            pending_admin_action.pop(ADMIN_ID, None)

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
                "<code>2 | 5</code>"
            )

        return

    # --------------------------------------------------------
    # UPI
    # --------------------------------------------------------

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

        data["settings"]["upi_id"] = upi
        data["settings"]["upi_name"] = name

        pending_admin_action.pop(ADMIN_ID, None)

        await save_data()

        await message.answer(
            "✅ <b>UPI Updated</b>\n\n"
            f"UPI: <code>{esc(upi)}</code>\n"
            f"Name: {esc(name)}",
            reply_markup=admin_menu(),
        )

        return

    # --------------------------------------------------------
    # CHANNEL
    # --------------------------------------------------------

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
            chat = await bot.get_chat(channel_id)

            data["settings"]["paid_channel_id"] = str(
                channel_id
            )

            data["settings"]["paid_channel_title"] = (
                chat.title or ""
            )

            pending_admin_action.pop(ADMIN_ID, None)

            await save_data()

            await message.answer(
                "✅ <b>Paid Channel Saved</b>\n\n"
                f"📢 {esc(chat.title)}\n"
                f"🆔 <code>{channel_id}</code>",
                reply_markup=admin_menu(),
            )

        except Exception:
            await message.answer(
                "❌ Could not access this channel.\n\n"
                "Make sure the bot is an administrator."
            )

        return

    # --------------------------------------------------------
    # PROOF CHANNEL
    # --------------------------------------------------------

    if action == "set_proof":
        try:
            proof_id = int(text)
        except ValueError:
            await message.answer(
                "❌ Invalid channel ID."
            )
            return

        try:
            chat = await bot.get_chat(proof_id)

            data["settings"]["proof_channel_id"] = str(
                proof_id
            )

            pending_admin_action.pop(ADMIN_ID, None)

            await save_data()

            await message.answer(
                "✅ <b>Payment Proof Channel Saved</b>\n\n"
                f"📢 {esc(chat.title)}\n"
                f"🆔 <code>{proof_id}</code>",
                reply_markup=admin_menu(),
            )

        except Exception:
            await message.answer(
                "❌ Could not access the proof channel.\n\n"
                "Make sure the bot is an administrator there."
            )

        return


# ============================================================
# ADMIN UPI
# ============================================================

@dp.callback_query(F.data == "admin_upi")
async def admin_upi(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return

    settings = data["settings"]

    await callback.message.edit_text(
        "💳 <b>UPI Settings</b>\n\n"
        f"Current UPI:\n"
        f"<code>{esc(settings['upi_id']) or 'Not set'}</code>\n\n"
        f"Name:\n"
        f"<code>{esc(settings['upi_name']) or 'Not set'}</code>",
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


@dp.callback_query(F.data == "set_upi")
async def set_upi_callback(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return

    pending_admin_action[ADMIN_ID] = "set_upi"

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

@dp.callback_query(F.data == "admin_channel")
async def admin_channel(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return

    settings = data["settings"]

    await callback.message.edit_text(
        "📢 <b>Paid Channel</b>\n\n"
        f"Title: "
        f"<b>{esc(settings['paid_channel_title']) or 'Not set'}</b>\n"
        f"ID: "
        f"<code>{esc(settings['paid_channel_id']) or 'Not set'}</code>",
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


@dp.callback_query(F.data == "set_channel")
async def set_channel_callback(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return

    pending_admin_action[ADMIN_ID] = "set_channel"

    await callback.message.answer(
        "📢 Send the private channel ID.\n\n"
        "Example:\n"
        "<code>-1001234567890</code>\n\n"
        "The bot must be an administrator in that channel."
    )

    await callback.answer()


# ============================================================
# ADMIN PROOF
# ============================================================

@dp.callback_query(F.data == "admin_proof")
async def admin_proof(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return

    proof = data["settings"]["proof_channel_id"]

    await callback.message.edit_text(
        "🧾 <b>Payment Proof Channel</b>\n\n"
        f"Current:\n"
        f"<code>{esc(proof) or 'Not set'}</code>",
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


@dp.callback_query(F.data == "set_proof")
async def set_proof_callback(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return

    pending_admin_action[ADMIN_ID] = "set_proof"

    await callback.message.answer(
        "🧾 Send the payment proof channel ID.\n\n"
        "Example:\n"
        "<code>-1001234567890</code>\n\n"
        "The bot must be able to post there."
    )

    await callback.answer()


# ============================================================
# TOGGLE / DELETE PLAN
# ============================================================

@dp.callback_query(F.data.startswith("toggleplan:"))
async def toggle_plan(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return

    plan_id = callback.data.split(":", 1)[1]

    plan = data["plans"].get(plan_id)

    if not plan:
        await callback.answer("Plan not found.")
        return

    plan["active"] = not plan.get("active", True)

    await save_data()

    await callback.answer("Plan status updated.")

    await plan_details(callback)


@dp.callback_query(F.data.startswith("deleteplan:"))
async def delete_plan(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return

    plan_id = callback.data.split(":", 1)[1]

    if plan_id in data["plans"]:
        del data["plans"][plan_id]

    await save_data()

    await callback.answer("Plan deleted.")

    await admin_plans(callback)


# ============================================================
# ADMIN USERS
# ============================================================

@dp.callback_query(F.data == "admin_users")
async def admin_users(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return

    users = data["users"]

    await callback.message.edit_text(
        "👥 <b>Users</b>\n\n"
        f"Total users: <b>{len(users)}</b>\n\n"
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

@dp.callback_query(F.data == "admin_payments")
async def admin_payments(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return

    payments = list(data["payments"].values())

    total = len(payments)

    pending = sum(
        1
        for p in payments
        if p["status"] == "pending_admin"
    )

    approved = sum(
        1
        for p in payments
        if p["status"] == "approved"
    )

    rejected = sum(
        1
        for p in payments
        if p["status"] == "rejected"
    )

    revenue = sum(
        float(p["amount"])
        for p in payments
        if p["status"] == "approved"
    )

    await callback.message.edit_text(
        "💰 <b>Payments</b>\n\n"
        f"Total: <b>{total}</b>\n"
        f"⏳ Pending: <b>{pending}</b>\n"
        f"✅ Approved: <b>{approved}</b>\n"
        f"❌ Rejected: <b>{rejected}</b>\n\n"
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

@dp.callback_query(F.data == "admin_stats")
async def admin_stats(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return

    users = len(data["users"])
    payments = list(data["payments"].values())
    subscriptions = list(
        data["subscriptions"].values()
    )

    approved = [
        p
        for p in payments
        if p["status"] == "approved"
    ]

    active = [
        s
        for s in subscriptions
        if s["status"] == "active"
    ]

    expired = [
        s
        for s in subscriptions
        if s["status"] == "expired"
    ]

    revenue = sum(
        float(p["amount"])
        for p in approved
    )

    await callback.message.edit_text(
        "📊 <b>Bot Statistics</b>\n\n"
        f"👥 Users: <b>{users}</b>\n"
        f"💳 Payments: <b>{len(payments)}</b>\n"
        f"✅ Approved: <b>{len(approved)}</b>\n"
        f"🟢 Active subscriptions: <b>{len(active)}</b>\n"
        f"⌛ Expired subscriptions: <b>{len(expired)}</b>\n"
        f"💰 Revenue: <b>₹{money(revenue)}</b>\n"
        f"💎 Plans: <b>{len(data['plans'])}</b>",
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

@dp.callback_query(F.data == "admin_home")
async def admin_home(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return

    await callback.message.edit_text(
        "🛠️ <b>Admin Panel</b>\n\n"
        "Choose an option:",
        reply_markup=admin_menu(),
    )

    await callback.answer()


# ============================================================
# RESTORE SUBSCRIPTIONS AFTER RESTART
# ============================================================

async def restore_subscriptions():
    logger.info("Restoring subscriptions...")

    for subscription_id, subscription in data[
        "subscriptions"
    ].items():

        if subscription["status"] != "active":
            continue

        remaining = (
            subscription["expires_at"] - now_ts()
        )

        if remaining <= 0:
            asyncio.create_task(
                subscription_expiry_worker(
                    subscription_id
                )
            )
            continue

        task = asyncio.create_task(
            subscription_expiry_worker(
                subscription_id
            )
        )

        subscription_tasks[subscription_id] = task

    logger.info(
        "Restored %s active subscription(s)",
        len(subscription_tasks),
    )


# ============================================================
# HEALTH SERVER FOR RENDER
# ============================================================

async def health(request: web.Request):
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

    runner = web.AppRunner(app)

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
    logger.info("Starting bot...")

    await bot.delete_webhook(
        drop_pending_updates=False
    )

    await restore_subscriptions()

    runner = await start_web_server()

    try:
        await dp.start_polling(
            bot,
            allowed_updates=dp.resolve_used_update_types(),
        )

    finally:
        for task in invite_tasks.values():
            task.cancel()

        for task in subscription_tasks.values():
            task.cancel()

        await runner.cleanup()
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
