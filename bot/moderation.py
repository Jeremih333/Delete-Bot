from datetime import datetime, timedelta, timezone

from aiogram import Bot
from aiogram.types import ChatMember


DELETED_ACCOUNT_NAMES = {
    "deleted account",
    "удаленный аккаунт",
    "удалённый аккаунт",
}


def classify_member(
    member: ChatMember,
    delete_deleted_enabled: bool,
    delete_frozen_enabled: bool,
) -> str | None:
    user = member.user
    status = getattr(member, "status", "")
    if status in {"creator", "administrator"}:
        return None
    if bool(getattr(user, "is_bot", False)):
        return None

    first_name = (user.first_name or "").strip().lower()
    if delete_deleted_enabled and first_name in DELETED_ACCOUNT_NAMES:
        return "deleted"

    is_frozen_signal = bool(getattr(user, "is_fake", False)) or bool(getattr(user, "is_scam", False))
    if delete_frozen_enabled and is_frozen_signal:
        return "frozen"

    return None


async def kick_member(bot: Bot, chat_id: int, user_id: int) -> None:
    until = datetime.now(timezone.utc) + timedelta(seconds=60)
    await bot.ban_chat_member(chat_id=chat_id, user_id=user_id, until_date=until, revoke_messages=False)
    await bot.unban_chat_member(chat_id=chat_id, user_id=user_id, only_if_banned=True)


async def ban_member(bot: Bot, chat_id: int, user_id: int) -> None:
    await bot.ban_chat_member(chat_id=chat_id, user_id=user_id, revoke_messages=False)


async def remove_member(bot: Bot, chat_id: int, user_id: int, action: str) -> None:
    if action == "kick":
        await kick_member(bot, chat_id, user_id)
        return
    await ban_member(bot, chat_id, user_id)


def reason_to_human(reason: str | None) -> str:
    if reason == "deleted":
        return "удаленный аккаунт"
    if reason == "frozen":
        return "замороженный аккаунт"
    return "не определена"
