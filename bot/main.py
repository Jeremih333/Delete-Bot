import asyncio
from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.types import Message, CallbackQuery
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from dotenv import load_dotenv

from bot.config import load_config
from bot.db import Database
from bot.keyboards import start_kb, premium_kb, settings_kb, dev_kb


class DevGrant(StatesGroup):
    waiting_user_id = State()
    waiting_months = State()


load_dotenv()
cfg = load_config()
db = Database(cfg.db_path)

dp = Dispatcher(storage=MemoryStorage())


@dp.message(Command("start"))
async def cmd_start(m: Message):
    text = (
        "🛡️ <b>Delete Bot</b>\n\n"
        "Я помогаю удалять deactivated/frozen аккаунты в чатах.\n"
        "Добавьте меня в группу и дайте права администратора."
    )
    await m.answer(text, reply_markup=start_kb(cfg.bot_username), parse_mode="HTML")


@dp.message(Command("premium"))
async def cmd_premium(m: Message):
    text = (
        "💎 <b>Premium тарифы</b>\n\n"
        "• 1 месяц — <b>199₽</b>\n"
        "• 3 месяца — <b>499₽</b> <s>599₽</s>\n"
        "• 6 месяцев — <b>959₽</b> <s>1194₽</s>\n"
        "• 12 месяцев — <b>1999₽</b> <s>2388₽</s>\n\n"
        f"Если не получается оформить подписку — обратитесь в <a href='{cfg.support_url}'>поддержку бота</a>."
    )
    await m.answer(
        text,
        parse_mode="HTML",
        disable_web_page_preview=True,
        reply_markup=premium_kb(
            cfg.tarif_message_1,
            cfg.tarif_message_3,
            cfg.tarif_message_6,
            cfg.tarif_message_12,
        ),
    )


@dp.message(Command("settings"))
async def cmd_settings(m: Message):
    if m.chat.type == "private":
        await m.answer("⚙️ Откройте /settings в группе, где бот является админом.")
        return
    user_id = m.from_user.id
    premium = await db.is_premium(user_id)
    interval, frozen = await db.get_chat_settings(m.chat.id)
    text = (
        "⚙️ <b>Настройки чата</b>\n"
        f"Интервал: <b>{interval} сек.</b>\n"
        f"Удаление frozen: <b>{'ON' if frozen else 'OFF'}</b>\n"
        f"План: <b>{'Premium' if premium else 'Free'}</b>"
    )
    await m.answer(text, parse_mode="HTML", reply_markup=settings_kb(premium))


@dp.callback_query(F.data.startswith("set:"))
async def cb_settings(c: CallbackQuery):
    user_id = c.from_user.id
    premium = await db.is_premium(user_id)
    _, kind, value = c.data.split(":")

    if kind == "interval":
        if value == "hours":
            await db.set_interval(c.message.chat.id, 3600)
            await c.answer("Интервал: 1 час", show_alert=False)
        elif value in ("minutes", "seconds") and not premium:
            await c.answer("🔒 Функция доступна только в Premium. Откройте /premium", show_alert=True)
        elif value == "minutes":
            await db.set_interval(c.message.chat.id, 60)
            await c.answer("Интервал: 1 минута", show_alert=False)
        elif value == "seconds":
            await db.set_interval(c.message.chat.id, 30)
            await c.answer("Интервал: 30 секунд", show_alert=False)

    if kind == "frozen":
        if not premium:
            await c.answer("🔒 Auto-delete frozen доступно только в Premium", show_alert=True)
        else:
            interval, frozen = await db.get_chat_settings(c.message.chat.id)
            await db.set_frozen(c.message.chat.id, not bool(frozen))
            await c.answer("Настройка frozen обновлена", show_alert=False)


@dp.message(Command("dev"))
async def cmd_dev(m: Message, state: FSMContext):
    if m.from_user.id != cfg.dev_telegram_id:
        await m.answer("⛔ Команда недоступна")
        return
    await m.answer("🧑‍💻 Введите Telegram ID пользователя для выдачи premium:")
    await state.set_state(DevGrant.waiting_user_id)


@dp.message(DevGrant.waiting_user_id)
async def dev_user_id(m: Message, state: FSMContext):
    if m.from_user.id != cfg.dev_telegram_id:
        return
    try:
        uid = int(m.text.strip())
    except ValueError:
        await m.answer("Нужен числовой ID.")
        return
    await state.update_data(target_user_id=uid)
    await state.set_state(DevGrant.waiting_months)
    await m.answer("Выберите срок подписки:", reply_markup=dev_kb())


@dp.callback_query(F.data.startswith("dev:grant:"))
async def dev_grant(c: CallbackQuery, state: FSMContext):
    if c.from_user.id != cfg.dev_telegram_id:
        await c.answer("Недоступно", show_alert=True)
        return
    months = int(c.data.split(":")[-1])
    data = await state.get_data()
    uid = data.get("target_user_id")
    if not uid:
        await c.answer("Сначала укажите ID через /dev", show_alert=True)
        return
    await db.set_subscription(uid, months)
    await c.message.answer(f"✅ Premium выдан пользователю {uid} на {months} мес.")
    await c.answer("Готово")
    await state.clear()


@dp.message(Command("scan"))
async def cmd_scan(m: Message):
    premium = await db.is_premium(m.from_user.id)
    limit_count = 50000 if premium else 2000
    pending = await db.pending_jobs_count()
    await db.add_scan_job(m.chat.id, limit_count)
    mode = "HYBRID" if pending >= cfg.hybrid_queue_threshold else "LOCAL"
    await m.answer(f"📊 Задача сканирования поставлена в очередь. Режим: {mode}. Лимит: {limit_count}")


async def main():
    if not cfg.bot_token:
        raise RuntimeError("BOT_TOKEN is not set")
    await db.init()
    bot = Bot(cfg.bot_token)
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
