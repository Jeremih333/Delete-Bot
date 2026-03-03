import asyncio
import logging
from datetime import datetime, timedelta, timezone

from aiogram import Bot
from dotenv import load_dotenv

from bot.config import load_config
from bot.db import Database
from bot.services.scan_scheduler import enqueue_scan_if_absent


logger = logging.getLogger("delete_bot.enqueue")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")


def _parse_iso_utc(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


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


async def _maybe_sync_chat_admins(bot: Bot, db: Database, chat_id: int, owner_user_id: int) -> None:
    health = await db.get_chat_health(chat_id)
    last_sync_at = _parse_iso_utc(health[1]) if health else None
    now = datetime.now(timezone.utc)
    if last_sync_at and (now - last_sync_at) < timedelta(hours=6):
        return
    try:
        admins = await bot.get_chat_administrators(chat_id)
    except Exception:
        return
    for admin in admins:
        if admin.user.is_bot:
            continue
        await db.track_recent_activity(chat_id, admin.user.id)
        await db.grant_chat_admin(chat_id, admin.user.id, owner_user_id)
    await db.touch_chat_health(chat_id, last_external_sync_at=now.isoformat())


async def run_enqueue():
    load_dotenv()
    cfg = load_config()
    if not cfg.bot_token:
        raise RuntimeError("BOT_TOKEN is not set")

    db = Database(
        path=cfg.db_path,
        backend=cfg.db_backend,
        cloudflare_account_id=cfg.cloudflare_account_id,
        cloudflare_d1_database_id=cfg.cloudflare_d1_database_id,
        cloudflare_api_token=cfg.cloudflare_api_token,
    )
    await db.init()

    enqueued_total = 0
    async with Bot(cfg.bot_token) as bot:
        chat_ids = await db.list_chats_due_for_auto_enqueue(limit=300)
        for chat_id in chat_ids:
            chat_data = await db.get_managed_chat(chat_id)
            if not chat_data or chat_data[4] == 0:
                continue
            owner_user_id = chat_data[3]
            await _maybe_sync_chat_admins(bot, db, chat_id, owner_user_id)
            premium = await db.is_premium(owner_user_id)
            interval, _dd, _df, _di, _days, _action = await db.enforce_plan_limits(chat_id, premium)
            limit_count = await _compute_scan_limit(bot, db, chat_id, premium)
            if limit_count > 0:
                ok = await enqueue_scan_if_absent(
                    db=db,
                    chat_id=chat_id,
                    interval_seconds=interval,
                    limit_count=limit_count,
                    priority=1 if premium else 0,
                    source="gh_enqueue",
                )
                if ok:
                    enqueued_total += 1
                logger.info(
                    "event=gh_enqueue chat_id=%s owner_premium=%s limit_count=%s interval=%s enqueued=%s",
                    chat_id,
                    int(premium),
                    limit_count,
                    interval,
                    int(ok),
                )
            await db.touch_chat_auto_enqueue(chat_id)

    print(f"Enqueued jobs: {enqueued_total}")


if __name__ == "__main__":
    asyncio.run(run_enqueue())
