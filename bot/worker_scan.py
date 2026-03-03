import asyncio
import logging
from time import monotonic

from aiogram import Bot
from dotenv import load_dotenv

from bot.config import load_config
from bot.db import Database
from bot.moderation import classify_exception_kind, classify_member_or_error, remove_member


logger = logging.getLogger("delete_bot.worker")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")


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

    tracked_total = await db.count_tracked_members(chat_id)

    processed = 0
    removed_deleted = 0
    removed_frozen = 0
    errors = 0

    chunk_size = 1000
    offset = 0
    timed_out = False
    deadline = monotonic() + (soft_timeout_ms / 1000.0) if soft_timeout_ms > 0 else float("inf")
    try:
        member_count = int(await bot.get_chat_member_count(chat_id))
    except Exception:
        member_count = limit_count
    report_total = member_count if owner_premium else min(member_count, 3500)

    run_id = await db.start_scan_run(
        job_id=job_id,
        chat_id=chat_id,
        source="auto",
        requested_limit=limit_count,
        report_total=report_total,
        tracked_total=tracked_total,
    )

    while processed < limit_count and monotonic() < deadline:
        batch_limit = min(chunk_size, limit_count - processed)
        candidate_ids = await db.get_tracked_members_for_scan(chat_id, batch_limit, offset=offset)
        if not candidate_ids:
            break

        for user_id in candidate_ids:
            if monotonic() >= deadline:
                break
            try:
                member = await bot.get_chat_member(chat_id, user_id)
                reason, _ = classify_member_or_error(member, bool(delete_deleted), bool(delete_frozen))
                await db.set_member_check_result(chat_id, user_id, reason=reason, removed=False)
                if reason:
                    await remove_member(bot, chat_id, user_id, moderation_action)
                    await db.set_member_check_result(chat_id, user_id, reason=reason, removed=True)
                    if reason == "deleted":
                        removed_deleted += 1
                    elif reason == "frozen":
                        removed_frozen += 1
                processed += 1
            except Exception as exc:
                kind = classify_exception_kind(exc)
                reason, _ = classify_member_or_error(exc, bool(delete_deleted), bool(delete_frozen))
                if reason:
                    await db.set_member_check_result(chat_id, user_id, reason=reason, removed=False)
                if kind == "transient":
                    await asyncio.sleep(0.4)
                    try:
                        member = await bot.get_chat_member(chat_id, user_id)
                        reason, _ = classify_member_or_error(member, bool(delete_deleted), bool(delete_frozen))
                        await db.set_member_check_result(chat_id, user_id, reason=reason, removed=False)
                        if reason:
                            await remove_member(bot, chat_id, user_id, moderation_action)
                            await db.set_member_check_result(chat_id, user_id, reason=reason, removed=True)
                            if reason == "deleted":
                                removed_deleted += 1
                            elif reason == "frozen":
                                removed_frozen += 1
                        processed += 1
                        continue
                    except Exception:
                        pass
                errors += 1
            await asyncio.sleep(0.005)

        offset += len(candidate_ids)

    if processed < limit_count and monotonic() >= deadline:
        timed_out = True

    summary = (
        "✅ *Авто-проверка завершена*\n\n"
        f"⚙️ Интервал: *{_interval_label(interval)}*\n"
        f"🛡️ Режим: *{'КИК' if moderation_action == 'kick' else 'БАН'}*\n"
        f"🧩 Правила: удаленные *{'ВКЛ' if delete_deleted else 'ВЫКЛ'}*, замороженные *{'ВКЛ' if delete_frozen else 'ВЫКЛ'}*\n\n"
        f"👥 Проверено: *{processed}/{max(1, report_total)}*\n"
        f"📚 Проверка по известным участникам: *{processed}/{max(1, tracked_total)}*\n"
        f"🗑️ Удалено удаленных аккаунтов: *{removed_deleted}*\n"
        f"🧊 Удалено замороженных аккаунтов: *{removed_frozen}*\n"
        f"⚠️ Ошибок: *{errors}*\n\n"
        "_Это сообщение удалится через 30 секунд._"
    )
    if (removed_deleted + removed_frozen) == 0:
        summary = (
            "✅ *Авто-проверка завершена*\n\n"
            f"⚙️ Интервал: *{_interval_label(interval)}*\n"
            f"🛡️ Режим: *{'КИК' if moderation_action == 'kick' else 'БАН'}*\n"
            f"🧩 Правила: удаленные *{'ВКЛ' if delete_deleted else 'ВЫКЛ'}*, замороженные *{'ВКЛ' if delete_frozen else 'ВЫКЛ'}*\n\n"
            f"👥 Проверено: *{processed}/{max(1, report_total)}*\n"
            f"📚 Проверка по известным участникам: *{processed}/{max(1, tracked_total)}*\n"
            "✨ Подозрительных аккаунтов не найдено.\n"
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

    if timed_out and processed < limit_count:
        remainder = max(0, limit_count - processed)
        continuation_key = f"cont:{job_id}:{processed}"
        await db.enqueue_scan_job_if_absent(chat_id, continuation_key, remainder, 1 if owner_premium else 0)
        await db.set_scan_job_status(job_id, "done_partial", set_finished_at=True)
    else:
        await db.set_scan_job_status(job_id, "done", set_finished_at=True)

    await db.finish_scan_run(
        run_id=run_id,
        processed=processed,
        removed_deleted=removed_deleted,
        removed_frozen=removed_frozen,
        errors=errors,
        timed_out=timed_out,
        error_code=None,
    )

    logger.info(
        "event=scan_job_completed chat_id=%s job_id=%s processed=%s report_total=%s tracked_total=%s removed_deleted=%s removed_frozen=%s errors=%s timed_out=%s",
        chat_id,
        job_id,
        processed,
        report_total,
        tracked_total,
        removed_deleted,
        removed_frozen,
        errors,
        int(timed_out),
    )

    return processed, removed_deleted + removed_frozen, errors, delete_task, report_total, timed_out


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
                _, removed, errors, delete_task, _report_total, _timed_out = await process_job(
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
