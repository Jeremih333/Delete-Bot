import asyncio
import aiosqlite
from datetime import datetime, timezone
from bot.config import load_config


async def run_worker():
    cfg = load_config()
    async with aiosqlite.connect(cfg.db_path) as db:
        cur = await db.execute(
            "SELECT id, chat_id, limit_count FROM scan_jobs WHERE status='pending' ORDER BY id LIMIT 20"
        )
        jobs = await cur.fetchall()
        for job_id, chat_id, limit_count in jobs:
            # Здесь интегрируется реальная логика проверки/удаления участников.
            await db.execute("UPDATE scan_jobs SET status='done', finished_at=? WHERE id=?", (datetime.now(timezone.utc).isoformat(), job_id))
        await db.commit()
    print(f"Processed jobs: {len(jobs)}")


if __name__ == "__main__":
    asyncio.run(run_worker())
