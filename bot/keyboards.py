from aiogram.utils.keyboard import InlineKeyboardBuilder


def start_kb(bot_username: str):
    username = bot_username.strip().lstrip("@")
    b = InlineKeyboardBuilder()
    b.button(
        text="➕ Добавить в группу",
        url=f"https://t.me/{username}?startgroup=true" if username else "https://t.me/",
    )
    b.button(text="⚙️ Открыть настройки", switch_inline_query_current_chat="/settings")
    b.adjust(1)
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
    lock = "" if is_premium else "🔒 "
    b = InlineKeyboardBuilder()
    b.button(text="⏱ Интервал: 1 час", callback_data="set:interval:hours")
    b.button(text=f"{lock}Интервал: 1 минута", callback_data="set:interval:minutes")
    b.button(text=f"{lock}Интервал: 30 секунд", callback_data="set:interval:seconds")
    b.button(text=f"{lock}Удалять fake/scam", callback_data="set:frozen:toggle")
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
