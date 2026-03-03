import asyncio
import logging
import math
import os
import time

from aiohttp import web
from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command, CommandObject
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    BotCommand,
    BotCommandScopeAllGroupChats,
    BotCommandScopeDefault,
    CallbackQuery,
    ChatMemberUpdated,
    InlineKeyboardMarkup,
    Message,
)
from aiogram.utils.keyboard import InlineKeyboardBuilder
from dotenv import load_dotenv

from bot.callbacks import parse_settings_interval
from bot.config import load_config
from bot.db import Database
from bot.keyboards import dev_kb, premium_kb, start_kb
from bot.moderation import classify_member_or_error, reason_to_human, remove_member
from bot.texts.ru import PREMIUM_REQUIRED_ALERT, STATUS_DIAGNOSTICS
try:
    from bot.services.premium_guard import (
        FEATURE_FROZEN_DELETE,
        FEATURE_INACTIVE_DELETE,
        FEATURE_INTERVAL_FAST,
        FEATURE_KICK_MODE,
        can_use_feature,
    )
except ModuleNotFoundError:
    FEATURE_FROZEN_DELETE = "frozen_delete"
    FEATURE_INACTIVE_DELETE = "inactive_delete"
    FEATURE_INTERVAL_FAST = "interval_fast"
    FEATURE_KICK_MODE = "kick_mode"

    class _FeatureDecision:
        def __init__(self, allowed: bool, reason_code: str | None = None):
            self.allowed = allowed
            self.reason_code = reason_code

    def can_use_feature(owner_is_premium: bool, feature: str) -> _FeatureDecision:
        if feature in (FEATURE_FROZEN_DELETE, FEATURE_INACTIVE_DELETE, FEATURE_INTERVAL_FAST, FEATURE_KICK_MODE) and not owner_is_premium:
            return _FeatureDecision(False, "premium_required")
        return _FeatureDecision(True, None)

try:
    from bot.services.scan_scheduler import enqueue_scan_if_absent
except ModuleNotFoundError:
    async def enqueue_scan_if_absent(
        _db: Database,
        *,
        chat_id: int,
        interval_seconds: int,
        limit_count: int,
        priority: int = 5,
    ) -> bool:
        now = int(time.time())
        bucket = now // max(1, int(interval_seconds))
        window_key = f"{chat_id}:{interval_seconds}:{bucket}"
        return await _db.enqueue_scan_job_if_absent(chat_id, window_key, limit_count, priority)


class DevGrant(StatesGroup):
    waiting_user_id = State()
    waiting_months = State()
    waiting_revoke_user_id = State()


SETTINGS_PAGE_SIZE = 8
ADMIN_ACCESS_PAGE_SIZE = 6

load_dotenv()
cfg = load_config()
db = Database(
    path=cfg.db_path,
    backend=cfg.db_backend,
    cloudflare_account_id=cfg.cloudflare_account_id,
    cloudflare_d1_database_id=cfg.cloudflare_d1_database_id,
    cloudflare_api_token=cfg.cloudflare_api_token,
)
dp = Dispatcher(storage=MemoryStorage())
logger = logging.getLogger("delete_bot.main")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")


def _md_escape(value: str) -> str:
    return (
        value.replace("\\", "\\\\")
        .replace("_", "\\_")
        .replace("*", "\\*")
        .replace("`", "\\`")
        .replace("[", "\\[")
    )


def _interval_label(seconds: int) -> str:
    mapping = {
        30: "30 секунд",
        60: "1 минута",
        3600: "1 час",
        14400: "4 часа",
    }
    return mapping.get(seconds, f"{seconds} сек.")


def _plan_limits(is_premium: bool) -> tuple[int, int]:
    if is_premium:
        return (50, 45)  # chats, channels
    return (3, 2)


def _chat_kind(chat_type: str) -> str:
    return "канал" if chat_type == "channel" else "чат"



def _readd_link(chat_type: str) -> str:
    username = cfg.bot_username.strip().lstrip("@")
    if not username:
        return "https://t.me/"
    perms = "change_info+post_messages+edit_messages+delete_messages+invite_users+restrict_members+pin_messages+manage_topics+promote_members+manage_video_chats+anonymous"
    if chat_type == "channel":
        return f"https://t.me/{username}?startchannel=true&admin={perms}"
    return f"https://t.me/{username}?startgroup=true&admin={perms}"


def _readd_kb(chat_type: str) -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    label = "📣 Добавить обратно в канал" if chat_type == "channel" else "👥 Добавить обратно в чат"
    b.button(text=label, url=_readd_link(chat_type))
    return b.as_markup()


async def _guard_owner_chat_access(c: CallbackQuery, chat_id: int) -> tuple[int, str, str, int, int] | None:
    chat_data = await db.get_managed_chat(chat_id)
    if not chat_data:
        await c.answer("Чат не найден", show_alert=True)
        return None
    if chat_data[4] == 0:
        chat_title = _md_escape(chat_data[1])
        chat_type = chat_data[2]
        await c.answer("Бот удален из этого чата/канала", show_alert=True)
        await c.message.answer(
            f"Бот сейчас не подключен к *{chat_title}*.\n"
            "Добавьте бота обратно, чтобы продолжить настройку.",
            parse_mode="Markdown",
            reply_markup=_readd_kb(chat_type),
        )
        return None
    if not await has_management_access(c.bot, chat_id, c.from_user.id):
        await c.answer("Доступ только у владельца и синхронизированных админов", show_alert=True)
        return None
    return chat_data


async def is_telegram_admin(bot: Bot, chat_id: int, user_id: int) -> bool:
    try:
        member = await bot.get_chat_member(chat_id, user_id)
    except Exception:
        return False
    return member.status in {"creator", "administrator"}


async def has_management_access(bot: Bot, chat_id: int, user_id: int) -> bool:
    chat_data = await db.get_managed_chat(chat_id)
    if not chat_data or chat_data[4] == 0:
        return False
    owner_user_id = chat_data[3]
    if user_id == owner_user_id:
        return await is_telegram_admin(bot, chat_id, user_id)
    if not await db.has_chat_admin_access(chat_id, user_id):
        return False
    return await is_telegram_admin(bot, chat_id, user_id)


async def register_commands(bot: Bot):
    private_commands = [
        BotCommand(command="start", description="О боте и быстрый старт"),
        BotCommand(command="settings", description="Мои чаты и настройки"),
        BotCommand(command="premium", description="Тарифы Premium"),
        BotCommand(command="status", description="Статус и преимущества"),
        BotCommand(command="help", description="Справка по командам"),
    ]
    group_commands = [
        BotCommand(command="settings", description="Открыть панель настроек"),
        BotCommand(command="check", description="Проверить пользователя (reply)"),
        BotCommand(command="status", description="Статус подписки владельца"),
        BotCommand(command="help", description="Краткая справка"),
    ]
    await bot.set_my_commands(private_commands, scope=BotCommandScopeDefault())
    await bot.set_my_commands(group_commands, scope=BotCommandScopeAllGroupChats())


def _chat_settings_kb(
    chat_id: int,
    premium: bool,
    delete_deleted: bool,
    delete_frozen: bool,
    delete_inactive: bool,
    inactive_days: int,
    moderation_action: str,
    current_interval: int,
) -> InlineKeyboardMarkup:
    lock = "" if premium else "🔒 "
    b = InlineKeyboardBuilder()
    b.button(
        text=f"{'✅' if delete_deleted else '❌'} Удалять удаленные аккаунты",
        callback_data=f"settings:toggle_deleted:{chat_id}",
    )
    b.button(
        text=f"{lock}{'✅' if delete_frozen else '❌'} Удалять замороженные аккаунты",
        callback_data=f"settings:toggle_frozen:{chat_id}",
    )
    b.button(
        text=f"{lock}{'✅' if delete_inactive else '❌'} Удалять давно неактивные ({inactive_days} дн.)",
        callback_data=f"settings:toggle_inactive:{chat_id}",
    )
    b.button(
        text=f"{lock}Срок неактивности: {inactive_days} дн.",
        callback_data=f"settings:inactive_days:{chat_id}",
    )
    action_label = "КИК" if moderation_action == "kick" else "БАН"
    action_lock = "" if premium or moderation_action == "ban" else "🔒 "
    b.button(text=f"{action_lock}Режим удаления: {action_label}", callback_data=f"settings:toggle_action:{chat_id}")
    b.button(
        text=f"{'✅ ' if current_interval == 14400 else ''}⏱ Интервал: 4 часа",
        callback_data=f"settings:interval:{chat_id}:14400",
    )
    b.button(
        text=f"{lock}{'✅ ' if current_interval == 3600 else ''}⏱ Интервал: 1 час",
        callback_data=f"settings:interval:{chat_id}:3600",
    )
    b.button(
        text=f"{lock}{'✅ ' if current_interval == 60 else ''}⏱ Интервал: 1 минута",
        callback_data=f"settings:interval:{chat_id}:60",
    )
    b.button(
        text=f"{lock}{'✅ ' if current_interval == 30 else ''}⏱ Интервал: 30 секунд",
        callback_data=f"settings:interval:{chat_id}:30",
    )
    b.button(text="👮 Синхронизировать админов", callback_data=f"settings:sync_admins:{chat_id}")
    b.button(text="👥 Настройка админов", callback_data=f"settings:admins:{chat_id}:1")
    b.button(text="⬅️ К списку", callback_data="settings:list:page:1")
    b.adjust(1)
    return b.as_markup()


async def _settings_page_payload(user_id: int, page: int) -> tuple[str, InlineKeyboardMarkup]:
    owner_premium = await db.is_premium(user_id)
    chat_limit, channel_limit = _plan_limits(owner_premium)
    active_chats = (await db.count_owner_chats(user_id, "group")) + (
        await db.count_owner_chats(user_id, "supergroup")
    )
    active_channels = await db.count_owner_chats(user_id, "channel")
    total = await db.count_accessible_chats(user_id)
    trusted_total = max(0, total - await db.count_owner_chats(user_id))
    pages = max(1, math.ceil(total / SETTINGS_PAGE_SIZE))
    page_norm = min(max(1, page), pages)
    offset = (page_norm - 1) * SETTINGS_PAGE_SIZE
    rows = await db.list_accessible_chats_page(user_id, offset=offset, limit=SETTINGS_PAGE_SIZE)

    b = InlineKeyboardBuilder()
    for chat_id, title, chat_type, enabled, owner_user_id in rows:
        icon = "КАНАЛ" if chat_type == "channel" else "ЧАТ"
        state = "ВКЛ" if enabled else "ВЫКЛ"
        role = "ВЛАДЕЛЕЦ" if owner_user_id == user_id else "ДОВЕРЕННЫЙ"
        b.button(text=f"[{state}] [{role}] [{icon}] {title}", callback_data=f"settings:chat:{chat_id}")
    if pages > 1:
        b.button(text="<<", callback_data="settings:list:page:1")
        b.button(text="<", callback_data=f"settings:list:page:{max(1, page_norm - 1)}")
        b.button(text=f"{page_norm}/{pages}", callback_data=f"settings:list:page:{page_norm}")
        b.button(text=">", callback_data=f"settings:list:page:{min(pages, page_norm + 1)}")
        b.button(text=">>", callback_data=f"settings:list:page:{pages}")
    b.adjust(1)
    if pages > 1:
        b.adjust(1, 5)

    text = "\n".join(
        [
            "*Ваши чаты и каналы*",
            "",
            f"Ваши чаты: *{active_chats}/{chat_limit}*",
            f"Ваши каналы: *{active_channels}/{channel_limit}*",
            f"Доверенные чаты: *{trusted_total}*",
            f"Страница: *{page_norm}/{pages}*",
        ]
    )
    return text, b.as_markup()


async def _safe_edit_text(c: CallbackQuery, text: str, reply_markup: InlineKeyboardMarkup | None = None):
    try:
        await c.message.edit_text(text, parse_mode="Markdown", reply_markup=reply_markup)
    except Exception:
        # If message is unchanged or cannot be edited, send a fresh message.
        await c.message.answer(text, parse_mode="Markdown", reply_markup=reply_markup)


def _admin_display_name(username: str | None, full_name: str | None, user_id: int) -> str:
    if username:
        return f"@{username}"
    if full_name:
        return full_name
    return f"Пользователь {user_id}"


def _trim_button_label(text: str, max_len: int = 56) -> str:
    if len(text) <= max_len:
        return text
    return text[: max_len - 1] + "…"


async def _admin_access_payload(bot: Bot, chat_id: int, page: int) -> tuple[str, InlineKeyboardMarkup]:
    chat_data = await db.get_managed_chat(chat_id)
    if not chat_data:
        b = InlineKeyboardBuilder()
        b.button(text="⬅️ К списку", callback_data="settings:list:page:1")
        b.adjust(1)
        return ("Чат не найден.", b.as_markup())

    owner_user_id = chat_data[3]
    access_set = set(await db.list_chat_admins(chat_id))

    admins_live: list[tuple[int, str]] = []
    try:
        admins = await bot.get_chat_administrators(chat_id)
        for admin in admins:
            if admin.user.is_bot:
                continue
            admins_live.append(
                (
                    admin.user.id,
                    _admin_display_name(admin.user.username, admin.user.full_name, admin.user.id),
                )
            )
    except Exception:
        admins_live = []

    # Fallback: if Telegram admin list is unavailable, show saved access list.
    if not admins_live:
        admins_live = [(uid, f"Пользователь {uid}") for uid in sorted(access_set)]
        if owner_user_id not in access_set:
            admins_live.insert(0, (owner_user_id, f"Владелец ({owner_user_id})"))

    admins_live = sorted(admins_live, key=lambda x: x[1].lower())
    total = len(admins_live)
    pages = max(1, math.ceil(total / ADMIN_ACCESS_PAGE_SIZE))
    page_norm = min(max(1, page), pages)
    offset = (page_norm - 1) * ADMIN_ACCESS_PAGE_SIZE
    page_admins = admins_live[offset : offset + ADMIN_ACCESS_PAGE_SIZE]

    b = InlineKeyboardBuilder()
    if not page_admins:
        b.button(text="Администраторы не найдены", callback_data=f"settings:admins:{chat_id}:{page_norm}")
    else:
        for uid, display_name in page_admins:
            has_access = uid in access_set
            if uid == owner_user_id:
                has_access = True
            icon = "✅" if has_access else "❌"
            label = _trim_button_label(f"{icon} {display_name}")
            if uid == owner_user_id:
                b.button(
                    text=label,
                    callback_data=f"settings:adminsnoop:{chat_id}:{uid}:{page_norm}",
                )
            else:
                b.button(
                    text=label,
                    callback_data=f"settings:admin_toggle:{chat_id}:{uid}:{page_norm}",
                )

    if pages > 1:
        b.button(text="<<", callback_data=f"settings:admins:{chat_id}:1")
        b.button(text="<", callback_data=f"settings:admins:{chat_id}:{max(1, page_norm - 1)}")
        b.button(text=f"{page_norm}/{pages}", callback_data=f"settings:admins:{chat_id}:{page_norm}")
        b.button(text=">", callback_data=f"settings:admins:{chat_id}:{min(pages, page_norm + 1)}")
        b.button(text=">>", callback_data=f"settings:admins:{chat_id}:{pages}")
    b.button(text="⬅️ Назад к настройкам чата", callback_data=f"settings:chat:{chat_id}")
    b.adjust(1)
    if pages > 1:
        b.adjust(1, 5, 1)

    text = (
        "👥 *Доступ администраторов к настройкам*\n\n"
        f"Чат: `{chat_id}`\n"
        "✅ доступ включен\n"
        "❌ доступ выключен\n\n"
        "Нажмите на пользователя, чтобы переключить доступ."
    )
    return text, b.as_markup()


def _format_premium_text() -> str:
    return (
        "💎 <b>Premium для автоматической модерации</b>\n\n"
        "Преимущества:\n"
        "• интервалы 1 час / 1 минута / 30 секунд\n"
        "• удаление замороженных аккаунтов\n"
        "• режим КИК (удаление без черного списка)\n"
        "• приоритет в очереди worker\n"
        "• повышенный лимит проверки\n"
        "• больше подключений: до 50 чатов и 45 каналов\n\n"
        "<b>Free-план:</b>\n"
        "• интервал 4 часа\n"
        "• удаление удаленных аккаунтов\n"
        "• до 3 500 участников за цикл\n"
        "• до 3 чатов и 2 каналов\n\n"
        "<b>Тарифы:</b>\n"
        "• 1 месяц — <b>199₽</b>\n"
        "• 3 месяца — <b>499₽</b> <s>599₽</s>\n"
        "• 6 месяцев — <b>959₽</b> <s>1194₽</s>\n"
        "• 12 месяцев — <b>1999₽</b> <s>2388₽</s>\n\n"
        f"Поддержка: <a href=\"{cfg.support_url}\">{cfg.support_url}</a>"
    )


async def show_owner_chats(message: Message, page: int = 1):
    total = await db.count_accessible_chats(message.from_user.id)
    if total == 0:
        await message.answer(
            "📭 *Пока нет подключенных чатов*\n\n"
            "Добавьте бота в чат или канал как администратора и откройте `/settings` снова.",
            parse_mode="Markdown",
        )
        return
    text, kb = await _settings_page_payload(message.from_user.id, page)
    text += (
        "\n\n⚠️ *Важно:* авто-модерация участников работает в группах/супергруппах. "
        "Для каналов доступно подключение и управление лимитами."
    )
    await message.answer(text, parse_mode="Markdown", reply_markup=kb)


async def render_chat_settings_text(chat_id: int) -> str:
    chat_data = await db.get_managed_chat(chat_id)
    if not chat_data:
        return "Чат не найден."
    _, title, chat_type, owner_user_id, enabled = chat_data
    owner_premium = await db.is_premium(owner_user_id)
    interval, delete_deleted, delete_frozen, delete_inactive, inactive_days, moderation_action = await db.enforce_plan_limits(
        chat_id,
        owner_premium,
    )
    admin_count = len(await db.list_chat_admins(chat_id))
    safe_title = _md_escape(title)
    base_text = (
        f"⚙️ *{safe_title}* ({_chat_kind(chat_type)})\n\n"
        f"• Статус: *{'Активен' if enabled else 'Отключен'}*\n"
        f"• Интервал авто-проверки: *{_interval_label(interval)}*\n"
        f"• Удалять удаленные аккаунты: *{'ВКЛ' if delete_deleted else 'ВЫКЛ'}*\n"
        f"• Удалять замороженные аккаунты: *{'ВКЛ' if delete_frozen else 'ВЫКЛ'}*\n"
        f"• Удалять давно неактивные: *{'ВКЛ' if delete_inactive else 'ВЫКЛ'}* (порог: *{inactive_days} дн.*)\n"
        f"• Режим удаления: *{'КИК (с авто-разбаном)' if moderation_action == 'kick' else 'БАН'}*\n"
        f"• План владельца: *{'Премиум' if owner_premium else 'Бесплатный'}*\n"
        f"• Админов с доступом: *{admin_count}*"
    )
    if chat_type == "channel":
        base_text += (
            "\n\n⚠️ *Ограничение Telegram:* бот не может массово модерировать подписчиков канала "
            "так же, как участников группы."
        )
    return base_text


async def _compute_scan_limit(bot: Bot, db: Database, chat_id: int, premium: bool) -> int:
    tracked_count = await db.count_tracked_members(chat_id)
    try:
        member_count = int(await bot.get_chat_member_count(chat_id))
    except Exception:
        member_count = tracked_count
    return db.get_scan_target(
        chat_id=chat_id,
        owner_is_premium=premium,
        known_members=tracked_count,
        chat_member_count=member_count,
    )


async def auto_enqueue_loop(bot: Bot):
    while True:
        try:
            chat_ids = await db.list_chats_due_for_auto_enqueue(limit=150)
            for chat_id in chat_ids:
                chat_data = await db.get_managed_chat(chat_id)
                if not chat_data or chat_data[4] == 0:
                    continue
                owner_user_id = chat_data[3]
                premium = await db.is_premium(owner_user_id)
                interval, _dd, _df, _di, _days, _action = await db.enforce_plan_limits(chat_id, premium)
                limit_count = await _compute_scan_limit(bot, db, chat_id, premium)
                if limit_count > 0:
                    enqueued = await enqueue_scan_if_absent(
                        db=db,
                        chat_id=chat_id,
                        interval_seconds=interval,
                        limit_count=limit_count,
                        priority=1 if premium else 0,
                        source="auto",
                    )
                    logger.info(
                        "event=auto_enqueue chat_id=%s owner_premium=%s limit_count=%s interval=%s enqueued=%s",
                        chat_id,
                        int(premium),
                        limit_count,
                        interval,
                        int(enqueued),
                    )
                await db.touch_chat_auto_enqueue(chat_id)
        except Exception:
            logger.exception("event=auto_enqueue_loop_error")
        await asyncio.sleep(30)


@dp.message(Command("start"))
async def cmd_start(m: Message, command: CommandObject):
    if m.chat.type != "private":
        await m.answer("Откройте бота в личных сообщениях: `/settings` для управления чатами.", parse_mode="Markdown")
        return
    text = (
        "🛡️ *Delete Bot*\n\n"
        "*Что бот реально делает:*\n"
        "• автоматически проверяет участников в группах/супергруппах\n"
        "• удаляет *удаленные аккаунты*\n"
        "• удаляет *замороженные аккаунты* (опция Premium)\n"
        "• ведет очередь задач и отчеты по проверкам\n\n"
        "*Ограничения:*\n"
        "• Telegram не дает получить полный список всех пользователей канала через Bot API\n"
        "• поэтому канал подключается для управления, но логика авто-удаления работает в группах\n\n"
        "Если возникли трудности с добавлением в чат/канал — обратитесь в поддержку:\n"
        f"{cfg.support_url}\n\n"
        "Откройте `/settings`, чтобы управлять чатами."
    )
    await m.answer(text, parse_mode="Markdown", reply_markup=start_kb(cfg.bot_username))
    if command.args == "settings":
        await show_owner_chats(m)
        return
    if command.args and command.args.startswith("chat_"):
        try:
            chat_id = int(command.args.removeprefix("chat_"))
        except ValueError:
            return
        chat_data = await db.get_managed_chat(chat_id)
        if not chat_data:
            await m.answer("Чат не найден в настройках бота.")
            return
        if not await has_management_access(m.bot, chat_id, m.from_user.id):
            await m.answer("Доступ запрещен: этот чат доступен только владельцу и синхронизированным админам.")
            return
        premium = await db.is_premium(chat_data[3])
        interval, delete_deleted, delete_frozen, delete_inactive, inactive_days, moderation_action = await db.enforce_plan_limits(
            chat_id,
            premium,
        )
        await m.answer(
            await render_chat_settings_text(chat_id),
            parse_mode="Markdown",
            reply_markup=_chat_settings_kb(
                chat_id,
                premium,
                bool(delete_deleted),
                bool(delete_frozen),
                bool(delete_inactive),
                int(inactive_days),
                moderation_action,
                interval,
            ),
        )


@dp.message(Command("help"))
async def cmd_help(m: Message):
    text = (
        "📚 *Справка*\n\n"
        "*Что делает бот:*\n"
        "• авто-проверка участников в группах/супергруппах\n"
        "• удаление удаленных аккаунтов\n"
        "• удаление замороженных аккаунтов (Premium)\n"
        "• режимы удаления: БАН / КИК (КИК только Premium)\n\n"
        "*Что бот не делает:*\n"
        "• не может получить абсолютно всех подписчиков канала через Telegram Bot API\n"
        "• не модерирует администраторов и ботов\n\n"
        "*ЛС:*\n"
        "• `/settings` — ваши чаты и параметры модерации\n"
        "• `/premium` — тарифы\n"
        "• `/status` — статус и преимущества\n\n"
        "*Группа:*\n"
        "• `/check` — проверка пользователя (reply)\n"
        "• `/settings` — ссылка в панель настроек\n"
        "• `/status` — статус подписки владельца\n"
        "• `/help` — краткая справка"
    )
    await m.answer(text, parse_mode="Markdown")


@dp.message(Command("premium"))
async def cmd_premium(m: Message):
    await m.answer(
        _format_premium_text()
        + "\n\n⚠️ Ограничение платформы: для каналов Telegram не предоставляет боту полный контроль подписчиков.",
        parse_mode="HTML",
        disable_web_page_preview=True,
        reply_markup=premium_kb(
            cfg.tarif_message_1,
            cfg.tarif_message_3,
            cfg.tarif_message_6,
            cfg.tarif_message_12,
        ),
    )


@dp.message(Command("status"))
async def cmd_status(m: Message):
    target_user_id = m.from_user.id
    if m.chat.type != "private":
        if not await is_telegram_admin(m.bot, m.chat.id, m.from_user.id):
            await m.answer("⚙️ Настройки бота доступны только администраторам чата.")
            return
        chat_data = await db.get_managed_chat(m.chat.id)
        if not chat_data:
            await m.answer("Чат не подключен к панели владельца.")
            return
        if not await has_management_access(m.bot, m.chat.id, m.from_user.id):
            await m.answer("⛔ Команда доступна только назначенным администраторам.")
            return
        target_user_id = chat_data[3]

    premium = await db.is_premium(target_user_id)
    remaining_seconds = await db.premium_remaining_seconds(target_user_id)
    if premium:
        await m.answer(
            "✅ *Premium активен*\n\n"
            f"Осталось примерно: *{remaining_seconds // 86400} дн.*\n\n"
            "Преимущества Premium:\n"
            "• интервалы 1 час / 1 мин / 30 сек\n"
            "• режим КИК (удаление без черного списка)\n"
            "• удаление замороженных аккаунтов\n"
            "• приоритет задач в очереди\n"
            "• увеличенный лимит проверки\n"
            "• увеличенные лимиты подключений",
            parse_mode="Markdown",
        )
    else:
        await m.answer(
            "ℹ️ Сейчас активен план *Free*.\n\n"
            "В Free доступно:\n"
            "• интервал 4 часа\n"
            "• удаление удаленных аккаунтов\n"
            "• лимит 3 500 участников за цикл\n"
            "• до 3 чатов и 2 каналов\n\n"
            "Чтобы открыть расширенные функции, используйте /premium.",
            parse_mode="Markdown",
        )


    await m.answer(
        "ℹ️ *Как определяется статус аккаунтов*\n\n"
        "• *Удаленный аккаунт*: имя профиля совпадает с шаблоном удаленного аккаунта или Telegram возвращает признак деактивации при проверке.\n"
        "• *Замороженный аккаунт*: используются сигналы подозрительного профиля (fake/scam), если они доступны для данного пользователя.\n"
        "• Проверка выполняется только по участникам, которых бот уже видел в чате (ограничение Bot API).",
        parse_mode="Markdown",
    )


@dp.message(Command("settings"))
async def cmd_settings(m: Message):
    if m.chat.type == "private":
        await show_owner_chats(m, page=1)
        return
    if not await is_telegram_admin(m.bot, m.chat.id, m.from_user.id):
        await m.answer("⚙️ Настройки бота доступны только администраторам чата.")
        return
    if not await has_management_access(m.bot, m.chat.id, m.from_user.id):
        await m.answer("⛔ Команда доступна только назначенным администраторам.")
        return
    username = cfg.bot_username.strip().lstrip("@")
    await m.answer(f"⚙️ Настройки этого чата в ЛС:\nhttps://t.me/{username}?start=chat_{m.chat.id}")


@dp.callback_query(F.data.startswith("settings:list:page:"))
async def cb_settings_list_page(c: CallbackQuery):
    page = int(c.data.split(":")[-1])
    await c.answer()
    text, kb = await _settings_page_payload(c.from_user.id, page)
    await _safe_edit_text(c, text, kb)


@dp.callback_query(F.data.startswith("settings:chat:"))
async def cb_settings_chat(c: CallbackQuery):
    chat_id = int(c.data.split(":")[-1])
    chat_data = await _guard_owner_chat_access(c, chat_id)
    if not chat_data:
        return
    premium = await db.is_premium(chat_data[3])
    interval, delete_deleted, delete_frozen, delete_inactive, inactive_days, moderation_action = await db.enforce_plan_limits(
        chat_id,
        premium,
    )
    text = await render_chat_settings_text(chat_id)
    await c.answer()
    await _safe_edit_text(
        c,
        text,
        _chat_settings_kb(
            chat_id,
            premium,
            bool(delete_deleted),
            bool(delete_frozen),
            bool(delete_inactive),
            int(inactive_days),
            moderation_action,
            interval,
        ),
    )


@dp.callback_query(F.data.startswith("settings:toggle_deleted:"))
async def cb_toggle_deleted(c: CallbackQuery):
    chat_id = int(c.data.split(":")[-1])
    chat_data = await _guard_owner_chat_access(c, chat_id)
    if not chat_data:
        return
    await c.answer()
    premium = await db.is_premium(chat_data[3])
    interval, delete_deleted, delete_frozen, delete_inactive, inactive_days, moderation_action = await db.enforce_plan_limits(
        chat_id,
        premium,
    )
    await db.set_delete_deleted(chat_id, not bool(delete_deleted))
    text = await render_chat_settings_text(chat_id)
    interval, delete_deleted, delete_frozen, delete_inactive, inactive_days, moderation_action = await db.enforce_plan_limits(
        chat_id,
        premium,
    )
    await _safe_edit_text(
        c,
        text,
        _chat_settings_kb(
            chat_id,
            premium,
            bool(delete_deleted),
            bool(delete_frozen),
            bool(delete_inactive),
            int(inactive_days),
            moderation_action,
            interval,
        ),
    )


@dp.callback_query(F.data.startswith("settings:toggle_frozen:"))
async def cb_toggle_frozen(c: CallbackQuery):
    chat_id = int(c.data.split(":")[-1])
    chat_data = await _guard_owner_chat_access(c, chat_id)
    if not chat_data:
        return
    await c.answer()
    premium = await db.is_premium(chat_data[3])
    if not can_use_feature(premium, FEATURE_FROZEN_DELETE).allowed:
        await db.enforce_plan_limits(chat_id, False)
        await c.message.answer(PREMIUM_REQUIRED_ALERT)
        return
    interval, delete_deleted, delete_frozen, delete_inactive, inactive_days, moderation_action = await db.get_chat_settings(chat_id)
    await db.set_frozen(chat_id, not bool(delete_frozen))
    text = await render_chat_settings_text(chat_id)
    interval, delete_deleted, delete_frozen, delete_inactive, inactive_days, moderation_action = await db.get_chat_settings(chat_id)
    await _safe_edit_text(
        c,
        text,
        _chat_settings_kb(
            chat_id,
            True,
            bool(delete_deleted),
            bool(delete_frozen),
            bool(delete_inactive),
            int(inactive_days),
            moderation_action,
            interval,
        ),
    )


@dp.callback_query(F.data.startswith("settings:toggle_action:"))
async def cb_toggle_action(c: CallbackQuery):
    chat_id = int(c.data.split(":")[-1])
    chat_data = await _guard_owner_chat_access(c, chat_id)
    if not chat_data:
        return
    await c.answer()
    premium = await db.is_premium(chat_data[3])
    interval, delete_deleted, delete_frozen, delete_inactive, inactive_days, moderation_action = await db.enforce_plan_limits(
        chat_id,
        premium,
    )
    new_action = "kick" if moderation_action == "ban" else "ban"
    if new_action == "kick" and not can_use_feature(premium, FEATURE_KICK_MODE).allowed:
        await c.message.answer(PREMIUM_REQUIRED_ALERT)
        return
    await db.set_moderation_action(chat_id, new_action)
    text = await render_chat_settings_text(chat_id)
    interval, delete_deleted, delete_frozen, delete_inactive, inactive_days, moderation_action = await db.get_chat_settings(chat_id)
    await _safe_edit_text(
        c,
        text,
        _chat_settings_kb(
            chat_id,
            premium,
            bool(delete_deleted),
            bool(delete_frozen),
            bool(delete_inactive),
            int(inactive_days),
            moderation_action,
            interval,
        ),
    )


@dp.callback_query(F.data.startswith("settings:toggle_inactive:"))
async def cb_toggle_inactive(c: CallbackQuery):
    chat_id = int(c.data.split(":")[-1])
    chat_data = await _guard_owner_chat_access(c, chat_id)
    if not chat_data:
        return
    await c.answer()
    premium = await db.is_premium(chat_data[3])
    if not can_use_feature(premium, FEATURE_INACTIVE_DELETE).allowed:
        await db.enforce_plan_limits(chat_id, False)
        await c.message.answer(PREMIUM_REQUIRED_ALERT)
        return
    interval, delete_deleted, delete_frozen, delete_inactive, inactive_days, moderation_action = await db.get_chat_settings(chat_id)
    await db.set_inactive_cleanup(chat_id, not bool(delete_inactive))
    text = await render_chat_settings_text(chat_id)
    interval, delete_deleted, delete_frozen, delete_inactive, inactive_days, moderation_action = await db.get_chat_settings(chat_id)
    await _safe_edit_text(
        c,
        text,
        _chat_settings_kb(
            chat_id,
            premium,
            bool(delete_deleted),
            bool(delete_frozen),
            bool(delete_inactive),
            int(inactive_days),
            moderation_action,
            interval,
        ),
    )


@dp.callback_query(F.data.startswith("settings:inactive_days:"))
async def cb_inactive_days(c: CallbackQuery):
    chat_id = int(c.data.split(":")[-1])
    chat_data = await _guard_owner_chat_access(c, chat_id)
    if not chat_data:
        return
    await c.answer()
    premium = await db.is_premium(chat_data[3])
    if not can_use_feature(premium, FEATURE_INACTIVE_DELETE).allowed:
        await db.enforce_plan_limits(chat_id, False)
        await c.message.answer(PREMIUM_REQUIRED_ALERT)
        return
    interval, delete_deleted, delete_frozen, delete_inactive, inactive_days, moderation_action = await db.get_chat_settings(chat_id)
    options = [30, 90, 180, 365]
    try:
        idx = options.index(int(inactive_days))
    except ValueError:
        idx = 0
    new_days = options[(idx + 1) % len(options)]
    await db.set_inactive_days(chat_id, new_days)
    text = await render_chat_settings_text(chat_id)
    interval, delete_deleted, delete_frozen, delete_inactive, inactive_days, moderation_action = await db.get_chat_settings(chat_id)
    await _safe_edit_text(
        c,
        text,
        _chat_settings_kb(
            chat_id,
            premium,
            bool(delete_deleted),
            bool(delete_frozen),
            bool(delete_inactive),
            int(inactive_days),
            moderation_action,
            interval,
        ),
    )


@dp.callback_query(F.data.startswith("settings:interval:"))
async def cb_interval(c: CallbackQuery):
    parsed = parse_settings_interval(c.data)
    if not parsed:
        await c.answer("Некорректные параметры интервала", show_alert=True)
        return
    chat_id = parsed.chat_id
    seconds = parsed.seconds
    chat_data = await _guard_owner_chat_access(c, chat_id)
    if not chat_data:
        return
    await c.answer()
    premium = await db.is_premium(chat_data[3])
    if seconds in (3600, 60, 30) and not can_use_feature(premium, FEATURE_INTERVAL_FAST).allowed:
        await db.enforce_plan_limits(chat_id, False)
        await c.message.answer(PREMIUM_REQUIRED_ALERT)
        return
    await db.set_interval(chat_id, seconds)
    text = await render_chat_settings_text(chat_id)
    interval, delete_deleted, delete_frozen, delete_inactive, inactive_days, moderation_action = await db.enforce_plan_limits(
        chat_id,
        premium,
    )
    await _safe_edit_text(
        c,
        text,
        _chat_settings_kb(
            chat_id,
            premium,
            bool(delete_deleted),
            bool(delete_frozen),
            bool(delete_inactive),
            int(inactive_days),
            moderation_action,
            interval,
        ),
    )


@dp.callback_query(F.data.startswith("settings:sync_admins:"))
async def cb_sync_admins(c: CallbackQuery):
    chat_id = int(c.data.split(":")[-1])
    chat_data = await _guard_owner_chat_access(c, chat_id)
    if not chat_data:
        return
    await c.answer()
    try:
        admins = await c.bot.get_chat_administrators(chat_id)
    except Exception:
        await c.message.answer("Не удалось получить список админов.")
        return

    existing = set(await db.list_chat_admins(chat_id))
    for admin in admins:
        if admin.user.is_bot:
            continue
        uid = admin.user.id
        if uid not in existing:
            await db.grant_chat_admin(chat_id, uid, c.from_user.id)

    premium = await db.is_premium(chat_data[3])
    interval, delete_deleted, delete_frozen, delete_inactive, inactive_days, moderation_action = await db.enforce_plan_limits(
        chat_id,
        premium,
    )
    text = await render_chat_settings_text(chat_id)
    await _safe_edit_text(
        c,
        text,
        _chat_settings_kb(
            chat_id,
            premium,
            bool(delete_deleted),
            bool(delete_frozen),
            bool(delete_inactive),
            int(inactive_days),
            moderation_action,
            interval,
        ),
    )


@dp.callback_query(F.data.startswith("settings:admins:"))
async def cb_admins_page(c: CallbackQuery):
    _, _, chat_raw, page_raw = c.data.split(":")
    chat_id = int(chat_raw)
    page = int(page_raw)
    chat_data = await _guard_owner_chat_access(c, chat_id)
    if not chat_data:
        return
    await c.answer()
    text, kb = await _admin_access_payload(c.bot, chat_id, page)
    await _safe_edit_text(c, text, kb)


@dp.callback_query(F.data.startswith("settings:adminsnoop:"))
async def cb_admins_noop(c: CallbackQuery):
    await c.answer("Доступ владельца отключить нельзя.", show_alert=True)


async def _toggle_admin_access(c: CallbackQuery, chat_id: int, target_user_id: int, page: int):
    chat_data = await _guard_owner_chat_access(c, chat_id)
    if not chat_data:
        return

    owner_user_id = chat_data[3]
    if c.from_user.id != owner_user_id:
        await c.answer("Только владелец чата может менять доступ админов.", show_alert=True)
        return

    if target_user_id == owner_user_id:
        await c.answer("Доступ владельца отключить нельзя.", show_alert=True)
        return

    try:
        admins = await c.bot.get_chat_administrators(chat_id)
        live_admin_ids = {a.user.id for a in admins if not a.user.is_bot}
    except Exception:
        live_admin_ids = set()
    if live_admin_ids and target_user_id not in live_admin_ids:
        await c.answer("Пользователь больше не является администратором чата.", show_alert=True)
        return

    await c.answer()
    has_access = await db.has_chat_admin_access(chat_id, target_user_id)
    if has_access:
        await db.revoke_chat_admin(chat_id, target_user_id)
    else:
        await db.grant_chat_admin(chat_id, target_user_id, owner_user_id)
    text, kb = await _admin_access_payload(c.bot, chat_id, page)
    await _safe_edit_text(c, text, kb)


@dp.callback_query(F.data.startswith("settings:admin_toggle:"))
async def cb_admin_toggle(c: CallbackQuery):
    _, _, chat_raw, user_raw, page_raw = c.data.split(":")
    await _toggle_admin_access(c, int(chat_raw), int(user_raw), int(page_raw))


@dp.callback_query(F.data.startswith("settings:admin_revoke:"))
async def cb_admin_revoke_legacy(c: CallbackQuery):
    # Backward compatibility with old inline messages.
    _, _, chat_raw, user_raw, page_raw = c.data.split(":")
    await _toggle_admin_access(c, int(chat_raw), int(user_raw), int(page_raw))




@dp.message(Command("dev"))
async def cmd_dev(m: Message, state: FSMContext, command: CommandObject):
    if m.from_user.id not in cfg.dev_telegram_ids:
        await m.answer("⛔ Команда недоступна")
        return
    if m.chat.type != "private":
        await m.answer("Команда /dev доступна только в личных сообщениях с ботом.")
        return

    args_raw = (command.args or "").strip()
    args = args_raw.lower()
    if args == "subscribers":
        rows = await db.list_active_subscribers(limit=50)
        if not rows:
            await m.answer("Активных Premium-подписчиков нет.")
            return
        lines = ["📋 *Активные Premium-подписчики*"]
        for user_id, expires_at, plan_months in rows:
            lines.append(f"• `{user_id}` | {plan_months} мес. | до {expires_at}")
        await m.answer("\n".join(lines), parse_mode="Markdown")
        return


    if args == "queue":
        pending = await db.pending_jobs_count()
        await m.answer(f"?? Pending jobs: *{pending}*", parse_mode="Markdown")
        return

    if args == "last_runs":
        runs = await db.list_last_scan_runs(limit=10)
        if not runs:
            await m.answer("????????? ???????? ????? ???.")
            return
        lines = ["?? *????????? scan-runs*"]
        for run_id, chat_id, source, started_at, processed, report_total, tracked_total, removed_total, err, timed_out in runs:
            lines.append(
                f"? id `{run_id}` chat `{chat_id}` src `{source}`\n"
                f"  checked `{processed}/{report_total}` tracked `{tracked_total}` removed `{removed_total}` err `{err}` timeout `{timed_out}`\n"
                f"  at `{started_at}`"
            )
        await m.answer("\n".join(lines), parse_mode="Markdown")
        return

    if args.startswith("chat_health"):
        parts = args_raw.split()
        if len(parts) != 2:
            await m.answer("??????: /dev chat_health <chat_id>")
            return
        try:
            chat_id = int(parts[1])
        except ValueError:
            await m.answer("chat_id ?????? ???? ??????.")
            return
        chat_data = await db.get_managed_chat(chat_id)
        tracked = await db.count_tracked_members(chat_id)
        has_open = await db.has_open_scan_job(chat_id)
        health = await db.get_chat_health(chat_id)
        if not chat_data:
            await m.answer(
                f"??? `{chat_id}` ?? ?????? ? managed_chats.\ntracked=`{tracked}` open_job=`{int(has_open)}`",
                parse_mode="Markdown",
            )
            return
        interval, delete_deleted, delete_frozen, delete_inactive, inactive_days, action = await db.get_chat_settings(chat_id)
        health_line = "health: n/a"
        if health:
            last_event_at, last_external_sync_at, tracked_h, chat_total_h, coverage_ratio_h, cooldown_until = health
            health_line = (
                f"health: tracked `{tracked_h}` total `{chat_total_h}` "
                f"coverage `{coverage_ratio_h:.3f}` sync `{last_external_sync_at}` "
                f"last_event `{last_event_at}` cooldown `{cooldown_until}`"
            )
        await m.answer(
            "?? *Chat health*\n"
            f"chat_id: `{chat_id}`\n"
            f"title: `{_md_escape(chat_data[1])}`\n"
            f"type: `{chat_data[2]}` enabled: `{chat_data[4]}` owner: `{chat_data[3]}`\n"
            f"settings: interval `{interval}` deleted `{delete_deleted}` frozen `{delete_frozen}` inactive `{delete_inactive}` inactive_days `{inactive_days}` action `{action}`\n"
            f"tracked: `{tracked}` open_job: `{int(has_open)}`\n"
            f"{health_line}",
            parse_mode="Markdown",
        )
        return

    if args.startswith("revoke"):
        parts = args_raw.split()
        if len(parts) == 2:
            try:
                target = int(parts[1])
                await db.delete_subscription(target)
                await m.answer(f"✅ Premium снят у пользователя {target}.")
                return
            except ValueError:
                await m.answer("Неверный формат. Используйте: /dev revoke <user_id>")
                return
        await m.answer("Отправьте Telegram ID пользователя для снятия Premium:")
        await state.set_state(DevGrant.waiting_revoke_user_id)
        return

    await m.answer(
        "🧑‍💻 *Режим разработчика*\n\n"
        "• /dev — выдать Premium пользователю\n"
        "• /dev revoke — снять Premium\n"
        "• /dev subscribers — список активных Premium",
        parse_mode="Markdown",
    )
    await m.answer("Отправьте Telegram ID пользователя для выдачи Premium:")
    await state.set_state(DevGrant.waiting_user_id)


@dp.message(DevGrant.waiting_user_id)
async def dev_user_id(m: Message, state: FSMContext):
    if m.from_user.id not in cfg.dev_telegram_ids:
        return
    try:
        uid = int((m.text or "").strip())
    except ValueError:
        await m.answer("Нужен числовой ID.")
        return
    await state.update_data(target_user_id=uid)
    await state.set_state(DevGrant.waiting_months)
    await m.answer("Выберите срок подписки:", reply_markup=dev_kb())


@dp.message(DevGrant.waiting_revoke_user_id)
async def dev_revoke_user_id(m: Message, state: FSMContext):
    if m.from_user.id not in cfg.dev_telegram_ids:
        return
    try:
        uid = int((m.text or "").strip())
    except ValueError:
        await m.answer("Нужен числовой ID.")
        return
    await db.delete_subscription(uid)
    await m.answer(f"✅ Premium снят у пользователя {uid}.")
    await state.clear()


@dp.callback_query(F.data.startswith("dev:grant:"))
async def dev_grant(c: CallbackQuery, state: FSMContext):
    if c.from_user.id not in cfg.dev_telegram_ids:
        await c.answer("Недоступно", show_alert=True)
        return
    months = int(c.data.split(":")[-1])
    data = await state.get_data()
    uid = data.get("target_user_id")
    if not uid:
        await c.answer("Сначала укажите ID через /dev", show_alert=True)
        return
    await db.set_subscription(uid, months, granted_by=c.from_user.id)
    await c.message.answer(f"✅ Premium выдан пользователю {uid} на {months} мес.")
    await c.answer("Готово")
    await state.clear()


@dp.message(Command("check"))
async def cmd_check(m: Message):
    if m.chat.type == "private":
        await m.answer("Команда доступна только в группе.")
        return
    if not await is_telegram_admin(m.bot, m.chat.id, m.from_user.id):
        await m.answer("⚙️ Настройки бота доступны только администраторам чата.")
        return
    if not await has_management_access(m.bot, m.chat.id, m.from_user.id):
        await m.answer("⛔ Команда доступна только назначенным администраторам.")
        return
    if not m.reply_to_message or not m.reply_to_message.from_user:
        await m.answer("Используйте /check ответом на сообщение пользователя.")
        return

    target_id = m.reply_to_message.from_user.id
    chat_data = await db.get_managed_chat(m.chat.id)
    owner_user_id = chat_data[3] if chat_data else m.from_user.id
    owner_premium = await db.is_premium(owner_user_id)
    _, delete_deleted, delete_frozen, _delete_inactive, _inactive_days, moderation_action = await db.enforce_plan_limits(
        m.chat.id,
        owner_premium,
    )
    try:
        member = await m.bot.get_chat_member(m.chat.id, target_id)
        reason, _ = classify_member_or_error(member, bool(delete_deleted), bool(delete_frozen))
    except Exception as exc:
        reason, _ = classify_member_or_error(exc, bool(delete_deleted), bool(delete_frozen))
    await db.track_recent_activity(m.chat.id, target_id)
    await db.set_member_check_result(m.chat.id, target_id, reason=reason, removed=False)
    if not reason:
        await m.answer("✅ Для этого аккаунта нет активных правил удаления.")
        return
    await remove_member(m.bot, m.chat.id, target_id, moderation_action)
    await db.set_member_check_result(m.chat.id, target_id, reason=reason, removed=True)
    await m.answer(f"🚫 Пользователь удален. Причина: *{_md_escape(reason_to_human(reason))}*", parse_mode="Markdown")


@dp.message(F.chat.type.in_({"group", "supergroup"}))
async def track_message_authors(m: Message):
    if m.from_user:
        await db.track_recent_activity(m.chat.id, m.from_user.id)
    if m.reply_to_message and m.reply_to_message.from_user:
        await db.track_recent_activity(m.chat.id, m.reply_to_message.from_user.id)
    if m.forward_from:
        await db.track_recent_activity(m.chat.id, m.forward_from.id)
    if m.new_chat_members:
        for user in m.new_chat_members:
            if user and not user.is_bot:
                await db.track_recent_activity(m.chat.id, user.id)
    if m.left_chat_member and not m.left_chat_member.is_bot:
        await db.track_recent_activity(m.chat.id, m.left_chat_member.id)


@dp.edited_message(F.chat.type.in_({"group", "supergroup"}))
async def track_edited_message_authors(m: Message):
    if m.from_user:
        await db.track_recent_activity(m.chat.id, m.from_user.id)
    if m.reply_to_message and m.reply_to_message.from_user:
        await db.track_recent_activity(m.chat.id, m.reply_to_message.from_user.id)


@dp.chat_member()
async def on_chat_member(update: ChatMemberUpdated):
    if update.chat.type in {"group", "supergroup"}:
        await db.track_recent_activity(update.chat.id, update.new_chat_member.user.id)


@dp.my_chat_member()
async def on_my_chat_member(update: ChatMemberUpdated):
    chat_id = update.chat.id
    chat_type = update.chat.type
    if chat_type not in {"group", "supergroup", "channel"}:
        return

    title = update.chat.title or str(chat_id)
    new_status = update.new_chat_member.status
    if new_status in {"member", "administrator"}:
        owner_user_id = update.from_user.id
        existing = await db.get_managed_chat(chat_id)
        was_active = bool(existing and existing[4] == 1)
        if not was_active:
            owner_premium = await db.is_premium(owner_user_id)
            chat_limit, channel_limit = _plan_limits(owner_premium)
            active_chats = (await db.count_owner_chats(owner_user_id, "group")) + (
                await db.count_owner_chats(owner_user_id, "supergroup")
            )
            active_channels = await db.count_owner_chats(owner_user_id, "channel")
            if chat_type == "channel" and active_channels >= channel_limit:
                try:
                    await update.bot.send_message(
                        owner_user_id,
                        f"?? ????? ??????? ????????: {active_channels}/{channel_limit}. "
                        "????????? ?????? ?????? ??? ???????? Premium ????? /premium.",
                    )
                finally:
                    await update.bot.leave_chat(chat_id)
                return
            if chat_type in {"group", "supergroup"} and active_chats >= chat_limit:
                try:
                    await update.bot.send_message(
                        owner_user_id,
                        f"?? ????? ????? ????????: {active_chats}/{chat_limit}. "
                        "????????? ?????? ???? ??? ???????? Premium ????? /premium.",
                    )
                finally:
                    await update.bot.leave_chat(chat_id)
                return

        await db.upsert_managed_chat(chat_id, title, owner_user_id, chat_type)
        if chat_type in {"group", "supergroup"}:
            try:
                admins = await update.bot.get_chat_administrators(chat_id)
                for admin in admins:
                    if admin.user.is_bot:
                        continue
                    await db.track_recent_activity(chat_id, admin.user.id)
                    await db.grant_chat_admin(chat_id, admin.user.id, owner_user_id)
                await db.track_recent_activity(chat_id, owner_user_id)
                premium = await db.is_premium(owner_user_id)
                limit_count = await _compute_scan_limit(update.bot, db, chat_id, premium)
                if limit_count > 0:
                    enqueued = await enqueue_scan_if_absent(
                        db=db,
                        chat_id=chat_id,
                        interval_seconds=60,
                        limit_count=limit_count,
                        priority=1 if premium else 0,
                        source="onboard",
                    )
                    logger.info(
                        "event=onboard_enqueue chat_id=%s owner_id=%s owner_premium=%s limit_count=%s enqueued=%s",
                        chat_id,
                        owner_user_id,
                        int(premium),
                        limit_count,
                        int(enqueued),
                    )
            except Exception:
                logger.exception("event=onboard_prepare_error chat_id=%s", chat_id)

        try:
            await update.bot.send_message(
                owner_user_id,
                f"✅ Бот подключен к *{_md_escape(title)}* ({_chat_kind(chat_type)}).\n"
                "Откройте `/settings` для настройки.",
                parse_mode="Markdown",
            )
        except Exception:
            pass

    if new_status in {"kicked", "left"}:
        await db.disable_managed_chat(chat_id)


async def start_health_server() -> web.AppRunner | None:
    port_raw = os.getenv("PORT", "").strip()
    if not port_raw:
        return None
    try:
        port = int(port_raw)
    except ValueError:
        return None
    app = web.Application()

    async def health(_: web.Request):
        return web.Response(text="ok")

    app.add_routes([web.get("/", health), web.get("/healthz", health)])
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host="0.0.0.0", port=port)
    await site.start()
    return runner


async def main():
    if not cfg.bot_token:
        raise RuntimeError("BOT_TOKEN is not set")
    await db.init()
    bot = Bot(cfg.bot_token)
    await register_commands(bot)
    runner = await start_health_server()
    scheduler_task = asyncio.create_task(auto_enqueue_loop(bot))
    try:
        await dp.start_polling(bot)
    finally:
        scheduler_task.cancel()
        if runner:
            await runner.cleanup()


if __name__ == "__main__":
    asyncio.run(main())
