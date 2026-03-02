import asyncio

from dotenv import load_dotenv

from bot.config import load_config
from bot.db import Database


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

    jobs = await db.claim_pending_scan_jobs(limit=20)
    failed = 0
    for job_id, chat_id, limit_count in jobs:
        try:
            # Здесь интегрируется реальная логика проверки/удаления участников.
            _ = (chat_id, limit_count)
            await db.set_scan_job_status(job_id, "done", set_finished_at=True)
        except Exception:
            failed += 1
            await db.set_scan_job_status(job_id, "failed", set_finished_at=True)

    print(f"Processed jobs: {len(jobs)} | failed: {failed}")


if __name__ == "__main__":
    asyncio.run(run_worker())
