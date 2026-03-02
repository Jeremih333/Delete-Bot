from aiogram.utils.keyboard import InlineKeyboardBuilder


def start_kb(bot_username: str):
    b = InlineKeyboardBuilder()
    b.button(text="➕ Добавить бота в чат", url=f"https://t.me/{bot_username}?startgroup=true")
    return b.as_markup()


def premium_kb(url1: str, url3: str, url6: str, url12: str):
    b = InlineKeyboardBuilder()
    b.button(text="💎 1 месяц", url=url1)
    b.button(text="💎 3 месяца", url=url3)
    b.button(text="💎 6 месяцев", url=url6)
    b.button(text="💎 12 месяцев", url=url12)
    b.adjust(2)
    return b.as_markup()


def settings_kb(is_premium: bool):
    b = InlineKeyboardBuilder()
    b.button(text="⏱️ Интервал: часы", callback_data="set:interval:hours")
    b.button(text="🔒 Интервал: минуты", callback_data="set:interval:minutes")
    b.button(text="🔒 Интервал: секунды", callback_data="set:interval:seconds")
    b.button(text="❄️ Frozen автоудаление", callback_data="set:frozen:toggle")
    b.adjust(1)
    return b.as_markup()


def dev_kb():
    b = InlineKeyboardBuilder()
    b.button(text="Выдать 1 месяц", callback_data="dev:grant:1")
    b.button(text="Выдать 3 месяца", callback_data="dev:grant:3")
    b.button(text="Выдать 6 месяцев", callback_data="dev:grant:6")
    b.button(text="Выдать 12 месяцев", callback_data="dev:grant:12")
    b.adjust(2)
    return b.as_markup()
