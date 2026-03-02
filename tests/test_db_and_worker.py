import os
import tempfile
import unittest
from datetime import datetime

from bot.db import Database
from bot.worker_scan import run_worker


class TestDatabaseAndWorker(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        fd, path = tempfile.mkstemp(prefix="delete_bot_test_", suffix=".sqlite3")
        os.close(fd)
        self.db_path = path
        self.db = Database(path=self.db_path, backend="sqlite")
        await self.db.init()

    async def asyncTearDown(self):
        try:
            os.remove(self.db_path)
        except FileNotFoundError:
            pass

    async def test_subscription_is_extended_when_active(self):
        await self.db.set_subscription(user_id=1001, months=1, granted_by=777)
        first = await self.db.get_subscription(1001)
        await self.db.set_subscription(user_id=1001, months=1, granted_by=777)
        second = await self.db.get_subscription(1001)

        first_exp = datetime.fromisoformat(first[2])
        second_exp = datetime.fromisoformat(second[2])
        self.assertGreater(second_exp, first_exp)

    async def test_remaining_and_active_subscribers(self):
        await self.db.set_subscription(user_id=2002, months=1, granted_by=777)
        remaining = await self.db.premium_remaining_seconds(2002)
        active = await self.db.list_active_subscribers(limit=10)

        self.assertGreater(remaining, 0)
        self.assertTrue(any(row[0] == 2002 for row in active))

    async def test_chat_settings_default_and_update(self):
        interval, frozen = await self.db.get_chat_settings(chat_id=-100123)
        self.assertEqual(interval, 3600)
        self.assertEqual(frozen, 0)

        await self.db.set_interval(chat_id=-100123, seconds=60)
        await self.db.set_frozen(chat_id=-100123, enabled=True)
        interval2, frozen2 = await self.db.get_chat_settings(chat_id=-100123)
        self.assertEqual(interval2, 60)
        self.assertEqual(frozen2, 1)

    async def test_scan_jobs_claim_and_complete(self):
        await self.db.add_scan_job(chat_id=-100001, limit_count=20)
        await self.db.add_scan_job(chat_id=-100002, limit_count=30)

        claimed = await self.db.claim_pending_scan_jobs(limit=20)
        self.assertEqual(len(claimed), 2)

        for job_id, _, _ in claimed:
            await self.db.set_scan_job_status(job_id, "done", set_finished_at=True)

        pending = await self.db.pending_jobs_count()
        self.assertEqual(pending, 0)

    async def test_worker_processes_pending_jobs(self):
        await self.db.add_scan_job(chat_id=-100111, limit_count=50)
        await self.db.add_scan_job(chat_id=-100222, limit_count=60)

        prev_backend = os.environ.get("DB_BACKEND")
        prev_db_path = os.environ.get("DB_PATH")
        try:
            os.environ["DB_BACKEND"] = "sqlite"
            os.environ["DB_PATH"] = self.db_path
            await run_worker()
        finally:
            if prev_backend is None:
                os.environ.pop("DB_BACKEND", None)
            else:
                os.environ["DB_BACKEND"] = prev_backend
            if prev_db_path is None:
                os.environ.pop("DB_PATH", None)
            else:
                os.environ["DB_PATH"] = prev_db_path

        pending = await self.db.pending_jobs_count()
        self.assertEqual(pending, 0)


if __name__ == "__main__":
    unittest.main()
