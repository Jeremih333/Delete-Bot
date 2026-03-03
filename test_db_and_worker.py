import os
import tempfile
import unittest
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from bot.db import Database
from bot.moderation import classify_member
from bot.worker_scan import process_job, run_worker


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
        self.assertGreater(datetime.fromisoformat(second[2]), datetime.fromisoformat(first[2]))

    async def test_managed_chat_and_admin_access(self):
        await self.db.upsert_managed_chat(
            chat_id=-100123,
            title="Test Chat",
            owner_user_id=42,
            chat_type="supergroup",
        )
        row = await self.db.get_managed_chat(-100123)
        self.assertIsNotNone(row)
        self.assertEqual(row[3], 42)
        self.assertTrue(await self.db.has_chat_admin_access(-100123, 42))

        await self.db.grant_chat_admin(-100123, 99, 42)
        self.assertTrue(await self.db.has_chat_admin_access(-100123, 99))
        await self.db.revoke_chat_admin(-100123, 99)
        self.assertFalse(await self.db.has_chat_admin_access(-100123, 99))

    async def test_chat_settings_default_and_update(self):
        await self.db.ensure_chat_settings(-100123)
        interval, delete_deleted, delete_frozen, delete_inactive, inactive_days, moderation_action = await self.db.get_chat_settings(chat_id=-100123)
        self.assertEqual(interval, 14400)
        self.assertEqual(delete_deleted, 1)
        self.assertEqual(delete_frozen, 0)
        self.assertEqual(delete_inactive, 0)
        self.assertEqual(inactive_days, 180)
        self.assertEqual(moderation_action, "ban")

        await self.db.set_interval(chat_id=-100123, seconds=60)
        await self.db.set_delete_deleted(chat_id=-100123, enabled=False)
        await self.db.set_frozen(chat_id=-100123, enabled=True)
        await self.db.set_moderation_action(chat_id=-100123, action="kick")
        interval2, deleted2, frozen2, inactive2, inactive_days2, action2 = await self.db.get_chat_settings(chat_id=-100123)
        self.assertEqual(interval2, 60)
        self.assertEqual(deleted2, 0)
        self.assertEqual(frozen2, 1)
        self.assertEqual(inactive2, 0)
        self.assertEqual(inactive_days2, 180)
        self.assertEqual(action2, "kick")

    async def test_enforce_plan_limits_downgrades_expired_premium_settings(self):
        await self.db.ensure_chat_settings(-100888)
        await self.db.set_interval(-100888, 30)
        await self.db.set_frozen(-100888, True)
        await self.db.set_inactive_cleanup(-100888, True)
        await self.db.set_inactive_days(-100888, 30)
        await self.db.set_moderation_action(-100888, "kick")

        interval, delete_deleted, delete_frozen, delete_inactive, inactive_days, action = await self.db.enforce_plan_limits(
            chat_id=-100888,
            owner_is_premium=False,
        )
        self.assertEqual(interval, 14400)
        self.assertEqual(delete_deleted, 1)
        self.assertEqual(delete_frozen, 0)
        self.assertEqual(delete_inactive, 0)
        self.assertEqual(inactive_days, 180)
        self.assertEqual(action, "ban")

    async def test_scan_jobs_priority_order(self):
        await self.db.add_scan_job(chat_id=-1, limit_count=10, priority=0)
        await self.db.add_scan_job(chat_id=-2, limit_count=10, priority=1)
        claimed = await self.db.claim_pending_scan_jobs(limit=2)
        self.assertEqual(claimed[0][1], -2)
        self.assertEqual(claimed[1][1], -1)

    async def test_enqueue_scan_job_if_absent(self):
        ok1 = await self.db.enqueue_scan_job_if_absent(chat_id=-10, window_key="w:1", limit_count=100, priority=0)
        ok2 = await self.db.enqueue_scan_job_if_absent(chat_id=-10, window_key="w:1", limit_count=100, priority=0)
        claimed = await self.db.claim_pending_scan_jobs(limit=10)
        self.assertTrue(ok1)
        self.assertTrue(ok2)  # conflict is ignored but key exists after first enqueue
        self.assertEqual(len([j for j in claimed if j[1] == -10]), 1)

    async def test_get_scan_target(self):
        self.assertEqual(self.db.get_scan_target(chat_id=-1, owner_is_premium=False, known_members=2000, chat_member_count=5000), 2000)
        self.assertEqual(self.db.get_scan_target(chat_id=-1, owner_is_premium=False, known_members=5000, chat_member_count=5000), 3500)
        self.assertEqual(self.db.get_scan_target(chat_id=-1, owner_is_premium=True, known_members=5000, chat_member_count=5000), 5000)

    async def test_due_auto_enqueue(self):
        await self.db.upsert_managed_chat(chat_id=-100001, title="A", owner_user_id=1, chat_type="supergroup")
        due = await self.db.list_chats_due_for_auto_enqueue(limit=10)
        self.assertIn(-100001, due)
        await self.db.touch_chat_auto_enqueue(-100001)
        due2 = await self.db.list_chats_due_for_auto_enqueue(limit=10)
        self.assertNotIn(-100001, due2)

    async def test_owner_chat_counts_by_type(self):
        await self.db.upsert_managed_chat(chat_id=-101, title="G1", owner_user_id=7, chat_type="group")
        await self.db.upsert_managed_chat(chat_id=-102, title="SG1", owner_user_id=7, chat_type="supergroup")
        await self.db.upsert_managed_chat(chat_id=-103, title="C1", owner_user_id=7, chat_type="channel")
        self.assertEqual(await self.db.count_owner_chats(7, "group"), 1)
        self.assertEqual(await self.db.count_owner_chats(7, "supergroup"), 1)
        self.assertEqual(await self.db.count_owner_chats(7, "channel"), 1)

    async def test_track_members(self):
        await self.db.track_member(chat_id=-100001, user_id=10)
        await self.db.track_member(chat_id=-100001, user_id=11)
        members = await self.db.get_tracked_members_for_scan(chat_id=-100001, limit_count=10)
        self.assertEqual(set(members), {10, 11})

    async def test_claim_scan_candidates_prefers_unchecked(self):
        await self.db.track_member(chat_id=-100001, user_id=10)
        await self.db.track_member(chat_id=-100001, user_id=11)
        await self.db.set_member_check_result(chat_id=-100001, user_id=10, reason=None, removed=False)
        candidates = await self.db.claim_scan_candidates(chat_id=-100001, limit_count=10)
        self.assertIn(11, candidates)

    async def test_classify_member_rules(self):
        deleted_member = SimpleNamespace(user=SimpleNamespace(first_name="Deleted Account"))
        frozen_member = SimpleNamespace(user=SimpleNamespace(first_name="John", is_fake=True, is_scam=False))
        normal_member = SimpleNamespace(user=SimpleNamespace(first_name="Alice", is_fake=False, is_scam=False))

        self.assertEqual(classify_member(deleted_member, delete_deleted_enabled=True, delete_frozen_enabled=False), "deleted")
        self.assertEqual(classify_member(frozen_member, delete_deleted_enabled=False, delete_frozen_enabled=True), "frozen")
        self.assertIsNone(classify_member(normal_member, delete_deleted_enabled=True, delete_frozen_enabled=True))

    async def test_process_job_removes_deleted(self):
        await self.db.ensure_chat_settings(-100001)
        await self.db.track_member(chat_id=-100001, user_id=42)
        await self.db.add_scan_job(chat_id=-100001, limit_count=100, priority=0)
        claimed = await self.db.claim_pending_scan_jobs(limit=1)
        job_id, chat_id, limit_count = claimed[0]

        fake_member = SimpleNamespace(user=SimpleNamespace(first_name="Deleted Account", is_fake=False, is_scam=False))
        fake_bot = AsyncMock()
        fake_bot.get_chat_member.return_value = fake_member

        (
            processed,
            removed_deleted,
            removed_frozen,
            errors,
            rate_limited_count,
            delete_task,
            report_tracked_total,
            report_chat_total,
            timed_out,
        ) = await process_job(
            bot=fake_bot,
            db=self.db,
            job_id=job_id,
            chat_id=chat_id,
            limit_count=limit_count,
            soft_timeout_ms=2000,
        )
        self.assertEqual(processed, 1)
        self.assertEqual(removed_deleted + removed_frozen, 1)
        self.assertEqual(errors, 0)
        self.assertGreaterEqual(report_tracked_total, 1)
        self.assertGreaterEqual(report_chat_total, 1)
        self.assertEqual(rate_limited_count, 0)
        self.assertFalse(timed_out)
        if delete_task:
            delete_task.cancel()

    async def test_process_job_no_report_when_nothing_removed(self):
        await self.db.ensure_chat_settings(-100777)
        await self.db.track_member(chat_id=-100777, user_id=77)
        await self.db.add_scan_job(chat_id=-100777, limit_count=10, priority=0)
        claimed = await self.db.claim_pending_scan_jobs(limit=1)
        job_id, chat_id, limit_count = claimed[0]

        fake_member = SimpleNamespace(user=SimpleNamespace(first_name="Alice", is_fake=False, is_scam=False))
        fake_bot = AsyncMock()
        fake_bot.get_chat_member.return_value = fake_member
        fake_bot.get_chat_member_count.return_value = 1
        fake_bot.get_chat.return_value = SimpleNamespace(type="supergroup")

        result = await process_job(
            bot=fake_bot,
            db=self.db,
            job_id=job_id,
            chat_id=chat_id,
            limit_count=limit_count,
            soft_timeout_ms=1000,
        )
        self.assertEqual(result[1] + result[2], 0)
        self.assertEqual(fake_bot.send_message.await_count, 0)

    async def test_run_worker_processes_queue(self):
        await self.db.ensure_chat_settings(-100555)
        await self.db.track_member(chat_id=-100555, user_id=55)
        await self.db.add_scan_job(chat_id=-100555, limit_count=100, priority=0)

        prev = {k: os.environ.get(k) for k in ("DB_BACKEND", "DB_PATH", "BOT_TOKEN")}
        os.environ["DB_BACKEND"] = "sqlite"
        os.environ["DB_PATH"] = self.db_path
        os.environ["BOT_TOKEN"] = "123456:TEST"

        fake_member = SimpleNamespace(user=SimpleNamespace(first_name="Deleted Account", is_fake=False, is_scam=False))
        fake_bot = AsyncMock()
        fake_bot.get_chat_member.return_value = fake_member

        class FakeBotCM:
            def __init__(self, *_args, **_kwargs):
                self.inner = fake_bot

            async def __aenter__(self):
                return self.inner

            async def __aexit__(self, exc_type, exc, tb):
                return False

        try:
            with patch("bot.worker_scan.Bot", FakeBotCM):
                await run_worker()
        finally:
            for key, value in prev.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value

        pending = await self.db.pending_jobs_count()
        self.assertEqual(pending, 0)


if __name__ == "__main__":
    unittest.main()
