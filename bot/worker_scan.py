import asyncio
from time import monotonic

from aiogram import Bot
from dotenv import load_dotenv

from bot.config import load_config
from bot.db import Database
from bot.moderation import classify_member, remove_member


def _interval_label(seconds: int) -> str:
    mapping = {
        30: "30 секунд",
        60: "1 минута",
        3600: "1 час",
        14400: "4 часа",
    }
    return mapping.get(seconds, f"{seconds} сек.")


async def _delete_message_later(bot: Bot, chat_id: int, message_id: int, seconds: int = 30):
    await asyncio.sleep(seconds)
    try:
        await bot.delete_message(chat_id=chat_id, message_id=message_id)
    except Exception:
        pass


async def process_job(bot: Bot, db: Database, job_id: int, chat_id: int, limit_count: int, soft_timeout_ms: int):
    chat_data = await db.get_managed_chat(chat_id)
    owner_user_id = chat_data[3] if chat_data else 0
    owner_premium = await db.is_premium(owner_user_id) if owner_user_id else False
    interval, delete_deleted, delete_frozen, moderation_action = await db.enforce_plan_limits(chat_id, owner_premium)
    candidate_ids = await db.get_tracked_members_for_scan(chat_id, limit_count)
    processed = 0
    removed_deleted = 0
    removed_frozen = 0
    errors = 0

    deadline = monotonic() + (soft_timeout_ms / 1000.0)
    for user_id in candidate_ids:
        if monotonic() >= deadline:
            break
        try:
            member = await bot.get_chat_member(chat_id, user_id)
            reason = classify_member(member, bool(delete_deleted), bool(delete_frozen))
            await db.set_member_check_result(chat_id, user_id, reason=reason, removed=False)
            if reason:
                await remove_member(bot, chat_id, user_id, moderation_action)
                await db.set_member_check_result(chat_id, user_id, reason=reason, removed=True)
                if reason == "deleted":
                    removed_deleted += 1
                elif reason == "frozen":
                    removed_frozen += 1
            processed += 1
        except Exception:
            errors += 1
        await asyncio.sleep(0.03)

    summary = (
        "✅ *Авто-проверка завершена*\n\n"
        f"⚙️ Интервал: *{_interval_label(interval)}*\n"
        f"🛡️ Режим удаления: *{'КИК' if moderation_action == 'kick' else 'БАН'}*\n"
        f"🧩 Правила: удаленные *{'ON' if delete_deleted else 'OFF'}*, "
        f"замороженные *{'ON' if delete_frozen else 'OFF'}*\n\n"
        f"👥 Проверено: *{processed}*\n"
        f"🗑️ Удалено удаленных аккаунтов: *{removed_deleted}*\n"
        f"🧊 Удалено замороженных аккаунтов: *{removed_frozen}*\n"
        f"⚠️ Ошибок: *{errors}*\n\n"
        "_Это сообщение удалится через 30 секунд._"
    )
    if (removed_deleted + removed_frozen) == 0:
        summary = (
            "✅ *Авто-проверка завершена*\n\n"
            f"⚙️ Интервал: *{_interval_label(interval)}*\n"
            f"🛡️ Режим удаления: *{'КИК' if moderation_action == 'kick' else 'БАН'}*\n"
            f"🧩 Правила: удаленные *{'ON' if delete_deleted else 'OFF'}*, "
            f"замороженные *{'ON' if delete_frozen else 'OFF'}*\n\n"
            f"👥 Проверено: *{processed}*\n"
            "✨ Ничего подозрительного не найдено.\n"
            f"⚠️ Ошибок: *{errors}*\n\n"
            "_Это сообщение удалится через 30 секунд._"
        )
    delete_task = None
    try:
        chat = await bot.get_chat(chat_id)
        if chat.type in {"group", "supergroup"}:
            sent = await bot.send_message(chat_id, summary, parse_mode="Markdown")
            message_id = getattr(sent, "message_id", None)
            if message_id:
                delete_task = asyncio.create_task(_delete_message_later(bot, chat_id, int(message_id), 30))
    except Exception:
        pass

    await db.set_scan_job_status(job_id, "done", set_finished_at=True)
    return processed, removed_deleted + removed_frozen, errors, delete_task


async def run_worker():
    load_dotenv()
    cfg = load_config()
    db = Database(
        path=cfg.db_path,
        backend=cfg.db_backend,
        cloudflare_account_id=cfg.cloudflare_account_id,
        cloudflare_d1_database_id=cfg.cloudflare_d1_database_id,
        cloudflare_api_token=cfg.cloudflare_api_token,
    )
    await db.init()
    if not cfg.bot_token:
        raise RuntimeError("BOT_TOKEN is not set")

    jobs = await db.claim_pending_scan_jobs(limit=20)
    if not jobs:
        print("Processed jobs: 0 | removed: 0 | errors: 0")
        return

    removed_total = 0
    errors_total = 0
    delete_tasks: list[asyncio.Task] = []
    async with Bot(cfg.bot_token) as bot:
        for job_id, chat_id, limit_count in jobs:
            try:
                _, removed, errors, delete_task = await process_job(
                    bot=bot,
                    db=db,
                    job_id=job_id,
                    chat_id=chat_id,
                    limit_count=limit_count,
                    soft_timeout_ms=cfg.hybrid_scan_soft_timeout_ms,
                )
                removed_total += removed
                errors_total += errors
                if delete_task:
                    delete_tasks.append(delete_task)
            except Exception:
                errors_total += 1
                await db.set_scan_job_status(job_id, "failed", set_finished_at=True)

        if delete_tasks:
            await asyncio.gather(*delete_tasks, return_exceptions=True)

    print(f"Processed jobs: {len(jobs)} | removed: {removed_total} | errors: {errors_total}")


if __name__ == "__main__":
    asyncio.run(run_worker())
