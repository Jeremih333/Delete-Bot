import asyncio

from bot.config import load_config
from bot.db import build_database


async def run_worker():
    cfg = load_config()
    db = build_database(cfg)
    await db.init()

    jobs = await db.fetch_pending_scan_jobs(limit=20)
    for job_id, chat_id, limit_count in jobs:
        # Здесь интегрируется реальная логика проверки/удаления участников.
        await db.mark_scan_job_done(job_id)

    print(f"Processed jobs: {len(jobs)}")


if __name__ == "__main__":
    asyncio.run(run_worker())
