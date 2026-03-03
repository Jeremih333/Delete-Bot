import asyncio
import logging
from dataclasses import dataclass
from datetime import timedelta
from time import monotonic

from aiogram import Bot
from dotenv import load_dotenv

from bot.config import load_config
from bot.db import Database
from bot.moderation import classify_exception_kind, classify_member_or_error, remove_member


logger = logging.getLogger("delete_bot.worker")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")


@dataclass
class ScanJobResult:
    processed: int
    removed_deleted: int
    removed_frozen: int
    errors: int
    rate_limited_count: int
    delete_task: asyncio.Task | None
    report_tracked_total: int
    report_chat_total: int
    timed_out: bool


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


async def process_job(
    bot: Bot,
    db: Database,
    job_id: int,
    chat_id: int,
    limit_count: int,
    soft_timeout_ms: int,
    max_concurrency: int = 16,
):
    chat_data = await db.get_managed_chat(chat_id)
    owner_user_id = chat_data[3] if chat_data else 0
    owner_premium = await db.is_premium(owner_user_id) if owner_user_id else False
    interval, delete_deleted, delete_frozen, delete_inactive, inactive_days, moderation_action = await db.enforce_plan_limits(
        chat_id,
        owner_premium,
    )

    tracked_total = await db.count_tracked_members(chat_id)

    processed = 0
    removed_deleted = 0
    removed_frozen = 0
    errors = 0
    rate_limited_count = 0

    chunk_size = 1000
    timed_out = False
    deadline = monotonic() + (soft_timeout_ms / 1000.0) if soft_timeout_ms > 0 else float("inf")
    try:
        chat_total = int(await bot.get_chat_member_count(chat_id))
    except Exception:
        chat_total = tracked_total

    report_chat_total = max(chat_total, 0)
    report_total = db.get_scan_target(
        chat_id=chat_id,
        owner_is_premium=owner_premium,
        known_members=tracked_total,
        chat_member_count=report_chat_total,
    )
    report_tracked_total = max(1, tracked_total)
    coverage_ratio = (tracked_total / chat_total) if chat_total > 0 else 0

    run_id = await db.start_scan_run(
        job_id=job_id,
        chat_id=chat_id,
        source="auto",
        requested_limit=limit_count,
        report_total=max(1, report_total),
        tracked_total=tracked_total,
        chat_total=report_chat_total,
    )

    sem = asyncio.Semaphore(max(1, int(max_concurrency)))

    async def handle_candidate(user_id: int) -> tuple[int, int, int, int, int]:
        # processed_delta, removed_deleted_delta, removed_frozen_delta, errors_delta, rate_limited_delta
        async with sem:
            try:
                member = await bot.get_chat_member(chat_id, user_id)
                reason, _ = classify_member_or_error(member, bool(delete_deleted), bool(delete_frozen))
                if not reason and bool(delete_inactive):
                    health = await db.get_tracked_member_activity(chat_id, user_id)
                    if health and health[0] is not None:
                        inactive_seconds = max(0, int(health[0]))
                        if inactive_seconds >= int(inactive_days) * 86400:
                            reason = "inactive"
                await db.set_member_check_result(chat_id, user_id, reason=reason, removed=False)
                if reason:
                    await remove_member(bot, chat_id, user_id, moderation_action)
                    await db.set_member_check_result(chat_id, user_id, reason=reason, removed=True)
                    if reason == "deleted":
                        return 1, 1, 0, 0, 0
                    if reason == "frozen":
                        return 1, 0, 1, 0, 0
                    if reason == "inactive":
                        return 1, 0, 0, 0, 0
                return 1, 0, 0, 0, 0
            except Exception as exc:
                kind = classify_exception_kind(exc)
                reason, _ = classify_member_or_error(exc, bool(delete_deleted), bool(delete_frozen))
                if kind == "transient":
                    await db.set_member_check_result(
                        chat_id,
                        user_id,
                        reason=reason,
                        removed=False,
                        error_kind="transient",
                        error_code="transient",
                    )
                    return 0, 0, 0, 0, 1
                if reason:
                    try:
                        await remove_member(bot, chat_id, user_id, moderation_action)
                        await db.set_member_check_result(chat_id, user_id, reason=reason, removed=True, error_kind=kind)
                        if reason == "deleted":
                            return 1, 1, 0, 0, 0
                        if reason == "frozen":
                            return 1, 0, 1, 0, 0
                    except Exception:
                        await db.set_member_check_result(chat_id, user_id, reason=reason, removed=False, error_kind=kind)
                        return 1, 0, 0, 1, 0
                await db.set_member_check_result(chat_id, user_id, reason=None, removed=False, error_kind=kind)
                return 1, 0, 0, 1, 0

    while processed < limit_count and monotonic() < deadline:
        batch_limit = min(chunk_size, limit_count - processed)
        candidate_ids = await db.claim_scan_candidates(chat_id, batch_limit)
        if not candidate_ids:
            break

        results = await asyncio.gather(*(handle_candidate(user_id) for user_id in candidate_ids))
        for p, d, f, e, rl in results:
            processed += p
            removed_deleted += d
            removed_frozen += f
            errors += e
            rate_limited_count += rl

        if rate_limited_count > 0:
            cooldown_until = (timedelta(seconds=15) + timedelta()).total_seconds()
            _ = cooldown_until  # keep deterministic variable for future expansion
            await asyncio.sleep(0.4)

    if processed < limit_count and monotonic() >= deadline:
        timed_out = True

    coverage_line = f"📈 Охват базы: *{tracked_total}/{max(1, report_chat_total)}*"
    summary = (
        "✅ *Авто-проверка завершена*\n\n"
        f"⚙️ Интервал: *{_interval_label(interval)}*\n"
        f"🛡️ Режим: *{'КИК' if moderation_action == 'kick' else 'БАН'}*\n"
        f"🧩 Правила: удаленные *{'ВКЛ' if delete_deleted else 'ВЫКЛ'}*, замороженные *{'ВКЛ' if delete_frozen else 'ВЫКЛ'}*, давно неактивные *{'ВКЛ' if delete_inactive else 'ВЫКЛ'}*\n\n"
        f"👥 Проверено: *{processed}/{report_tracked_total}* (известные участники)\n"
        f"🌐 Всего участников в чате: *{max(1, report_chat_total)}*\n"
        f"{coverage_line}\n"
        f"🗑️ Удалено удаленных аккаунтов: *{removed_deleted}*\n"
        f"🧊 Удалено замороженных аккаунтов: *{removed_frozen}*\n"
        f"⏳ Rate limit событий: *{rate_limited_count}*\n"
        f"⚠️ Ошибок: *{errors}*\n\n"
        "_Это сообщение удалится через 30 секунд._"
    )

    delete_task = None
    if (removed_deleted + removed_frozen) > 0:
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
        rate_limited_count=rate_limited_count,
        timed_out=timed_out,
        error_code=None,
    )
    await db.touch_chat_health(
        chat_id,
        tracked_total=tracked_total,
        chat_total=report_chat_total,
        coverage_ratio=coverage_ratio,
    )

    logger.info(
        "event=scan_job_completed chat_id=%s job_id=%s processed=%s tracked_total=%s chat_total=%s removed_deleted=%s removed_frozen=%s rate_limited=%s errors=%s timed_out=%s",
        chat_id,
        job_id,
        processed,
        tracked_total,
        report_chat_total,
        removed_deleted,
        removed_frozen,
        rate_limited_count,
        errors,
        int(timed_out),
    )

    return (
        processed,
        removed_deleted,
        removed_frozen,
        errors,
        rate_limited_count,
        delete_task,
        report_tracked_total,
        report_chat_total,
        timed_out,
    )


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
                (
                    _processed,
                    removed_deleted,
                    removed_frozen,
                    errors,
                    _rate_limited,
                    delete_task,
                    _report_tracked_total,
                    _report_chat_total,
                    _timed_out,
                ) = await process_job(
                    bot=bot,
                    db=db,
                    job_id=job_id,
                    chat_id=chat_id,
                    limit_count=limit_count,
                    soft_timeout_ms=cfg.hybrid_scan_soft_timeout_ms,
                    max_concurrency=cfg.worker_chat_concurrency,
                )
                removed_total += removed_deleted + removed_frozen
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
