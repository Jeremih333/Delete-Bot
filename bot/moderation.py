from datetime import datetime, timedelta, timezone
import re

from aiogram import Bot
from aiogram.types import ChatMember


DELETED_ACCOUNT_NAMES = {
    "deleted account",
    "deactivated account",
    "удаленный аккаунт",
    "удалённый аккаунт",
}

DELETED_ERROR_PATTERNS = (
    "user not found",
    "input user deactivated",
    "user is deactivated",
    "peer_id_invalid",
)


def _normalize_text(value: str) -> str:
    return re.sub(r"\s+", " ", value.strip().lower())


def classify_exception_as_reason(exc: Exception, delete_deleted_enabled: bool) -> str | None:
    if not delete_deleted_enabled:
        return None
    text = _normalize_text(str(exc))
    for pattern in DELETED_ERROR_PATTERNS:
        if pattern in text:
            return "deleted"
    return None


def get_account_state(
    member: ChatMember,
    delete_deleted_enabled: bool,
    delete_frozen_enabled: bool,
) -> tuple[str | None, dict[str, bool]]:
    user = member.user
    status = getattr(member, "status", "")
    signals = {
        "is_admin": status in {"creator", "administrator"},
        "is_bot": bool(getattr(user, "is_bot", False)),
        "name_deleted": False,
        "is_fake": bool(getattr(user, "is_fake", False)),
        "is_scam": bool(getattr(user, "is_scam", False)),
    }

    if signals["is_admin"] or signals["is_bot"]:
        return None, signals

    first_name = _normalize_text(user.first_name or "")
    signals["name_deleted"] = first_name in DELETED_ACCOUNT_NAMES
    if delete_deleted_enabled and signals["name_deleted"]:
        return "deleted", signals

    is_frozen_signal = signals["is_fake"] or signals["is_scam"]
    if delete_frozen_enabled and is_frozen_signal:
        return "frozen", signals

    return None, signals


def classify_member(
    member: ChatMember,
    delete_deleted_enabled: bool,
    delete_frozen_enabled: bool,
) -> str | None:
    reason, _ = get_account_state(member, delete_deleted_enabled, delete_frozen_enabled)
    return reason


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
