import asyncio
import os

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

from bot.config import load_config
from bot.db import Database
from bot.keyboards import dev_kb, premium_kb, start_kb
from bot.moderation import classify_member, remove_member


class DevGrant(StatesGroup):
    waiting_user_id = State()
    waiting_months = State()
    waiting_revoke_user_id = State()


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


async def is_telegram_admin(bot: Bot, chat_id: int, user_id: int) -> bool:
    member = await bot.get_chat_member(chat_id, user_id)
    return member.status in {"creator", "administrator"}


async def has_management_access(bot: Bot, chat_id: int, user_id: int) -> bool:
    chat_data = await db.get_managed_chat(chat_id)
    if not chat_data or chat_data[3] == 0:
        return False
    owner_user_id = chat_data[2]
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


def _settings_list_kb(chats: list[tuple[int, str, int]]) -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    for chat_id, title, enabled in chats:
        icon = "🟢" if enabled else "⚫"
        b.button(text=f"{icon} {title}", callback_data=f"settings:chat:{chat_id}")
    b.adjust(1)
    return b.as_markup()


def _chat_settings_kb(
    chat_id: int,
    premium: bool,
    delete_deleted: bool,
    delete_frozen: bool,
    moderation_action: str,
    current_interval: int,
) -> InlineKeyboardMarkup:
    lock = "" if premium else "🔒 "
    b = InlineKeyboardBuilder()
    b.button(
        text=f"{'✅' if delete_deleted else '❌'} Удалять Deleted",
        callback_data=f"settings:toggle_deleted:{chat_id}",
    )
    b.button(
        text=f"{lock}{'✅' if delete_frozen else '❌'} Удалять Frozen(Fake/Scam)",
        callback_data=f"settings:toggle_frozen:{chat_id}",
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
    b.button(text="👮 Синхронизировать админов чата", callback_data=f"settings:sync_admins:{chat_id}")
    b.button(text="⬅️ К списку чатов", callback_data="settings:list")
    b.adjust(1)
    return b.as_markup()


def _format_premium_text() -> str:
    return (
        "💎 *Premium для автоматической модерации*\n\n"
        "Преимущества:\n"
        "• интервалы 1 час / 1 минута / 30 секунд\n"
        "• удаление Frozen (Fake/Scam)\n"
        "• режим КИК (удаление без черного списка)\n"
        "• приоритет в очереди worker\n"
        "• повышенный лимит проверки\n\n"
        "*Free-план:*\n"
        "• интервал 4 часа\n"
        "• удаление Deleted\n"
        "• до 3 500 участников за цикл\n\n"
        "*Тарифы:*\n"
        "• 1 месяц — *199₽*\n"
        "• 3 месяца — *499₽* ~599₽~\n"
        "• 6 месяцев — *959₽* ~1194₽~\n"
        "• 12 месяцев — *1999₽* ~2388₽~\n\n"
        f"Поддержка: {cfg.support_url}"
    )


async def show_owner_chats(message: Message):
    chats = await db.list_owner_chats(message.from_user.id)
    if not chats:
        await message.answer(
            "📭 *Пока нет подключенных чатов*\n\n"
            "Добавьте бота в чат/группу как администратора и откройте `/settings` снова.",
            parse_mode="Markdown",
        )
        return
    await message.answer("⚙️ *Ваши чаты*:", parse_mode="Markdown", reply_markup=_settings_list_kb(chats))


async def render_chat_settings_text(chat_id: int) -> str:
    chat_data = await db.get_managed_chat(chat_id)
    if not chat_data:
        return "Чат не найден."
    _, title, owner_user_id, enabled = chat_data
    interval, delete_deleted, delete_frozen, moderation_action = await db.get_chat_settings(chat_id)
    owner_premium = await db.is_premium(owner_user_id)
    admin_count = len(await db.list_chat_admins(chat_id))
    safe_title = _md_escape(title)
    return (
        f"⚙️ *{safe_title}*\n\n"
        f"• Статус: *{'Активен' if enabled else 'Отключен'}*\n"
        f"• Интервал авто-проверки: *{_interval_label(interval)}*\n"
        f"• Удалять Deleted: *{'ON' if delete_deleted else 'OFF'}*\n"
        f"• Удалять Frozen(Fake/Scam): *{'ON' if delete_frozen else 'OFF'}*\n"
        f"• Режим удаления: *{'КИК (с авто-разбаном)' if moderation_action == 'kick' else 'БАН'}*\n"
        f"• План владельца: *{'Premium' if owner_premium else 'Free'}*\n"
        f"• Админов с доступом: *{admin_count}*"
    )


async def auto_enqueue_loop():
    while True:
        try:
            chat_ids = await db.list_chats_due_for_auto_enqueue(limit=150)
            for chat_id in chat_ids:
                chat_data = await db.get_managed_chat(chat_id)
                if not chat_data or chat_data[3] == 0:
                    continue
                owner_user_id = chat_data[2]
                premium = await db.is_premium(owner_user_id)
                limit_count = 50000 if premium else 3500
                await db.add_scan_job(chat_id, limit_count, priority=1 if premium else 0)
                await db.touch_chat_auto_enqueue(chat_id)
        except Exception:
            pass
        await asyncio.sleep(30)


@dp.message(Command("start"))
async def cmd_start(m: Message, command: CommandObject):
    if m.chat.type != "private":
        await m.answer("Откройте бота в личных сообщениях: `/settings` для управления чатами.", parse_mode="Markdown")
        return
    text = (
        "🛡️ *Delete Bot*\n\n"
        "Бот работает автоматически в подключенных чатах и каналах:\n"
        "• удаляет *Deleted Account*\n"
        "• удаляет *Frozen(Fake/Scam)* при включенной опции\n\n"
        "Откройте `/settings`, чтобы управлять чатами."
    )
    await m.answer(text, parse_mode="Markdown", reply_markup=start_kb(cfg.bot_username))
    if command.args == "settings":
        await show_owner_chats(m)


@dp.message(Command("help"))
async def cmd_help(m: Message):
    text = (
        "📚 *Справка*\n\n"
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
        _format_premium_text(),
        parse_mode="Markdown",
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
        chat_data = await db.get_managed_chat(m.chat.id)
        if not chat_data:
            await m.answer("Чат не подключен к панели владельца.")
            return
        if not await has_management_access(m.bot, m.chat.id, m.from_user.id):
            await m.answer("⛔ Команда доступна только назначенным администраторам.")
            return
        target_user_id = chat_data[2]

    premium = await db.is_premium(target_user_id)
    remaining_seconds = await db.premium_remaining_seconds(target_user_id)
    if premium:
        await m.answer(
            "✅ *Premium активен*\n\n"
            f"Осталось примерно: *{remaining_seconds // 86400} дн.*\n\n"
            "Преимущества Premium:\n"
            "• интервалы 1 час / 1 мин / 30 сек\n"
            "• режим КИК (удаление без черного списка)\n"
            "• удаление Frozen(Fake/Scam)\n"
            "• приоритет задач в очереди\n"
            "• увеличенный лимит проверки",
            parse_mode="Markdown",
        )
    else:
        await m.answer(
            "ℹ️ Сейчас активен план *Free*.\n\n"
            "В Free доступно:\n"
            "• интервал 4 часа\n"
            "• удаление Deleted\n"
            "• лимит 3 500 участников за цикл\n\n"
            "Чтобы открыть расширенные функции, используйте /premium.",
            parse_mode="Markdown",
        )


@dp.message(Command("settings"))
async def cmd_settings(m: Message):
    if m.chat.type == "private":
        await show_owner_chats(m)
        return
    if not await has_management_access(m.bot, m.chat.id, m.from_user.id):
        await m.answer("⛔ Команда доступна только назначенным администраторам.")
        return
    username = cfg.bot_username.strip().lstrip("@")
    await m.answer(f"⚙️ Настройки этого чата в ЛС:\nhttps://t.me/{username}?start=settings")


@dp.callback_query(F.data == "settings:list")
async def cb_settings_list(c: CallbackQuery):
    chats = await db.list_owner_chats(c.from_user.id)
    if not chats:
        await c.message.edit_text("📭 *У вас нет управляемых чатов.*", parse_mode="Markdown")
        await c.answer()
        return
    await c.message.edit_text("⚙️ *Ваши чаты*:", parse_mode="Markdown", reply_markup=_settings_list_kb(chats))
    await c.answer()


@dp.callback_query(F.data.startswith("settings:chat:"))
async def cb_settings_chat(c: CallbackQuery):
    chat_id = int(c.data.split(":")[-1])
    chat_data = await db.get_managed_chat(chat_id)
    if not chat_data:
        await c.answer("Чат не найден", show_alert=True)
        return
    if c.from_user.id != chat_data[2]:
        await c.answer("Доступ только владельцу чата", show_alert=True)
        return
    interval, delete_deleted, delete_frozen, moderation_action = await db.get_chat_settings(chat_id)
    premium = await db.is_premium(c.from_user.id)
    text = await render_chat_settings_text(chat_id)
    await c.message.edit_text(
        text,
        parse_mode="Markdown",
        reply_markup=_chat_settings_kb(
            chat_id,
            premium,
            bool(delete_deleted),
            bool(delete_frozen),
            moderation_action,
            interval,
        ),
    )
    await c.answer()


@dp.callback_query(F.data.startswith("settings:toggle_deleted:"))
async def cb_toggle_deleted(c: CallbackQuery):
    chat_id = int(c.data.split(":")[-1])
    chat_data = await db.get_managed_chat(chat_id)
    if not chat_data or c.from_user.id != chat_data[2]:
        await c.answer("Недоступно", show_alert=True)
        return
    interval, delete_deleted, delete_frozen, moderation_action = await db.get_chat_settings(chat_id)
    await db.set_delete_deleted(chat_id, not bool(delete_deleted))
    premium = await db.is_premium(c.from_user.id)
    text = await render_chat_settings_text(chat_id)
    interval, delete_deleted, delete_frozen, moderation_action = await db.get_chat_settings(chat_id)
    await c.message.edit_text(
        text,
        parse_mode="Markdown",
        reply_markup=_chat_settings_kb(
            chat_id,
            premium,
            bool(delete_deleted),
            bool(delete_frozen),
            moderation_action,
            interval,
        ),
    )
    await c.answer("Обновлено")


@dp.callback_query(F.data.startswith("settings:toggle_frozen:"))
async def cb_toggle_frozen(c: CallbackQuery):
    chat_id = int(c.data.split(":")[-1])
    chat_data = await db.get_managed_chat(chat_id)
    if not chat_data or c.from_user.id != chat_data[2]:
        await c.answer("Недоступно", show_alert=True)
        return
    if not await db.is_premium(c.from_user.id):
        await c.answer("Доступно только в Premium", show_alert=True)
        return
    interval, delete_deleted, delete_frozen, moderation_action = await db.get_chat_settings(chat_id)
    await db.set_frozen(chat_id, not bool(delete_frozen))
    premium = True
    text = await render_chat_settings_text(chat_id)
    interval, delete_deleted, delete_frozen, moderation_action = await db.get_chat_settings(chat_id)
    await c.message.edit_text(
        text,
        parse_mode="Markdown",
        reply_markup=_chat_settings_kb(
            chat_id,
            premium,
            bool(delete_deleted),
            bool(delete_frozen),
            moderation_action,
            interval,
        ),
    )
    await c.answer("Обновлено")


@dp.callback_query(F.data.startswith("settings:toggle_action:"))
async def cb_toggle_action(c: CallbackQuery):
    chat_id = int(c.data.split(":")[-1])
    chat_data = await db.get_managed_chat(chat_id)
    if not chat_data or c.from_user.id != chat_data[2]:
        await c.answer("Недоступно", show_alert=True)
        return
    premium = await db.is_premium(c.from_user.id)
    interval, delete_deleted, delete_frozen, moderation_action = await db.get_chat_settings(chat_id)
    new_action = "kick" if moderation_action == "ban" else "ban"
    if new_action == "kick" and not premium:
        await c.answer("🔒 Режим КИК доступен только в Premium. Откройте /premium", show_alert=True)
        return
    await db.set_moderation_action(chat_id, new_action)
    text = await render_chat_settings_text(chat_id)
    interval, delete_deleted, delete_frozen, moderation_action = await db.get_chat_settings(chat_id)
    await c.message.edit_text(
        text,
        parse_mode="Markdown",
        reply_markup=_chat_settings_kb(
            chat_id,
            premium,
            bool(delete_deleted),
            bool(delete_frozen),
            moderation_action,
            interval,
        ),
    )
    await c.answer("Режим удаления обновлен")


@dp.callback_query(F.data.startswith("settings:interval:"))
async def cb_interval(c: CallbackQuery):
    _, _, _, chat_raw, seconds_raw = c.data.split(":")
    chat_id = int(chat_raw)
    seconds = int(seconds_raw)
    chat_data = await db.get_managed_chat(chat_id)
    if not chat_data or c.from_user.id != chat_data[2]:
        await c.answer("Недоступно", show_alert=True)
        return
    premium = await db.is_premium(c.from_user.id)
    if seconds in (3600, 60, 30) and not premium:
        await c.answer("🔒 Интервалы 1ч/1м/30с доступны только в Premium", show_alert=True)
        return
    await db.set_interval(chat_id, seconds)
    text = await render_chat_settings_text(chat_id)
    interval, delete_deleted, delete_frozen, moderation_action = await db.get_chat_settings(chat_id)
    await c.message.edit_text(
        text,
        parse_mode="Markdown",
        reply_markup=_chat_settings_kb(
            chat_id,
            premium,
            bool(delete_deleted),
            bool(delete_frozen),
            moderation_action,
            interval,
        ),
    )
    await c.answer("Интервал обновлен")


@dp.callback_query(F.data.startswith("settings:sync_admins:"))
async def cb_sync_admins(c: CallbackQuery):
    chat_id = int(c.data.split(":")[-1])
    chat_data = await db.get_managed_chat(chat_id)
    if not chat_data or c.from_user.id != chat_data[2]:
        await c.answer("Недоступно", show_alert=True)
        return
    try:
        admins = await c.bot.get_chat_administrators(chat_id)
    except Exception:
        await c.answer("Не удалось получить список админов", show_alert=True)
        return

    existing = set(await db.list_chat_admins(chat_id))
    for admin in admins:
        uid = admin.user.id
        if uid not in existing:
            await db.grant_chat_admin(chat_id, uid, c.from_user.id)

    await c.answer("Админы синхронизированы")
    text = await render_chat_settings_text(chat_id)
    interval, delete_deleted, delete_frozen, moderation_action = await db.get_chat_settings(chat_id)
    premium = await db.is_premium(c.from_user.id)
    await c.message.edit_text(
        text,
        parse_mode="Markdown",
        reply_markup=_chat_settings_kb(
            chat_id,
            premium,
            bool(delete_deleted),
            bool(delete_frozen),
            moderation_action,
            interval,
        ),
    )


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
    if not await has_management_access(m.bot, m.chat.id, m.from_user.id):
        await m.answer("⛔ Команда доступна только назначенным администраторам.")
        return
    if not m.reply_to_message or not m.reply_to_message.from_user:
        await m.answer("Используйте /check ответом на сообщение пользователя.")
        return

    target_id = m.reply_to_message.from_user.id
    _, delete_deleted, delete_frozen, moderation_action = await db.get_chat_settings(m.chat.id)
    member = await m.bot.get_chat_member(m.chat.id, target_id)
    reason = classify_member(member, bool(delete_deleted), bool(delete_frozen))
    await db.track_member(m.chat.id, target_id)
    await db.set_member_check_result(m.chat.id, target_id, reason=reason, removed=False)
    if not reason:
        await m.answer("✅ Для этого аккаунта нет активных правил удаления.")
        return
    await remove_member(m.bot, m.chat.id, target_id, moderation_action)
    await db.set_member_check_result(m.chat.id, target_id, reason=reason, removed=True)
    await m.answer(f"🚫 Пользователь удален. Причина: *{_md_escape(reason)}*", parse_mode="Markdown")


@dp.message(F.chat.type.in_({"group", "supergroup"}))
async def track_message_authors(m: Message):
    if m.from_user:
        await db.track_member(m.chat.id, m.from_user.id)
    if m.reply_to_message and m.reply_to_message.from_user:
        await db.track_member(m.chat.id, m.reply_to_message.from_user.id)


@dp.chat_member()
async def on_chat_member(update: ChatMemberUpdated):
    await db.track_member(update.chat.id, update.new_chat_member.user.id)


@dp.my_chat_member()
async def on_my_chat_member(update: ChatMemberUpdated):
    chat_id = update.chat.id
    if update.chat.type not in {"group", "supergroup"}:
        return
    title = update.chat.title or str(chat_id)
    new_status = update.new_chat_member.status
    if new_status in {"member", "administrator"}:
        await db.upsert_managed_chat(chat_id, title, update.from_user.id)
        try:
            await update.bot.send_message(
                update.from_user.id,
                f"✅ Бот подключен к чату *{_md_escape(title)}*.\nОткройте `/settings` для настройки.",
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
    scheduler_task = asyncio.create_task(auto_enqueue_loop())
    try:
        await dp.start_polling(bot)
    finally:
        scheduler_task.cancel()
        if runner:
            await runner.cleanup()


if __name__ == "__main__":
    asyncio.run(main())
