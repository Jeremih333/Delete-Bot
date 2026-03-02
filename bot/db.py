import aiosqlite
from datetime import datetime, timedelta, timezone


SCHEMA = """
CREATE TABLE IF NOT EXISTS subscriptions (
  user_id INTEGER PRIMARY KEY,
  plan_months INTEGER NOT NULL,
  started_at TEXT NOT NULL,
  expires_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS chat_settings (
  chat_id INTEGER PRIMARY KEY,
  check_interval_seconds INTEGER NOT NULL DEFAULT 3600,
  delete_frozen_enabled INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS roles (
  chat_id INTEGER NOT NULL,
  user_id INTEGER NOT NULL,
  role TEXT NOT NULL,
  PRIMARY KEY (chat_id, user_id)
);

CREATE TABLE IF NOT EXISTS scan_jobs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  chat_id INTEGER NOT NULL,
  limit_count INTEGER NOT NULL,
  status TEXT NOT NULL DEFAULT 'pending',
  created_at TEXT NOT NULL,
  finished_at TEXT
);
"""


class Database:
    def __init__(self, path: str):
        self.path = path

    async def init(self):
        async with aiosqlite.connect(self.path) as db:
            await db.executescript(SCHEMA)
            await db.commit()

    async def get_subscription(self, user_id: int):
        async with aiosqlite.connect(self.path) as db:
            cur = await db.execute(
                "SELECT plan_months, started_at, expires_at FROM subscriptions WHERE user_id = ?",
                (user_id,),
            )
            return await cur.fetchone()

    async def set_subscription(self, user_id: int, months: int):
        now = datetime.now(timezone.utc)
        exp = now + timedelta(days=30 * months)
        async with aiosqlite.connect(self.path) as db:
            await db.execute(
                """
                INSERT INTO subscriptions(user_id, plan_months, started_at, expires_at)
                VALUES(?, ?, ?, ?)
                ON CONFLICT(user_id) DO UPDATE SET
                  plan_months=excluded.plan_months,
                  started_at=excluded.started_at,
                  expires_at=excluded.expires_at
                """,
                (user_id, months, now.isoformat(), exp.isoformat()),
            )
            await db.commit()

    async def is_premium(self, user_id: int) -> bool:
        row = await self.get_subscription(user_id)
        if not row:
            return False
        _, _, expires_at = row
        return datetime.fromisoformat(expires_at) > datetime.now(timezone.utc)

    async def add_helper(self, chat_id: int, user_id: int):
        async with aiosqlite.connect(self.path) as db:
            await db.execute(
                "INSERT OR REPLACE INTO roles(chat_id, user_id, role) VALUES(?, ?, 'helper')",
                (chat_id, user_id),
            )
            await db.commit()

    async def get_role(self, chat_id: int, user_id: int):
        async with aiosqlite.connect(self.path) as db:
            cur = await db.execute(
                "SELECT role FROM roles WHERE chat_id = ? AND user_id = ?", (chat_id, user_id)
            )
            row = await cur.fetchone()
            return row[0] if row else None

    async def ensure_chat_settings(self, chat_id: int):
        async with aiosqlite.connect(self.path) as db:
            await db.execute(
                "INSERT OR IGNORE INTO chat_settings(chat_id) VALUES(?)", (chat_id,)
            )
            await db.commit()

    async def get_chat_settings(self, chat_id: int):
        await self.ensure_chat_settings(chat_id)
        async with aiosqlite.connect(self.path) as db:
            cur = await db.execute(
                "SELECT check_interval_seconds, delete_frozen_enabled FROM chat_settings WHERE chat_id = ?",
                (chat_id,),
            )
            return await cur.fetchone()

    async def set_interval(self, chat_id: int, seconds: int):
        await self.ensure_chat_settings(chat_id)
        async with aiosqlite.connect(self.path) as db:
            await db.execute(
                "UPDATE chat_settings SET check_interval_seconds = ? WHERE chat_id = ?",
                (seconds, chat_id),
            )
            await db.commit()

    async def set_frozen(self, chat_id: int, enabled: bool):
        await self.ensure_chat_settings(chat_id)
        async with aiosqlite.connect(self.path) as db:
            await db.execute(
                "UPDATE chat_settings SET delete_frozen_enabled = ? WHERE chat_id = ?",
                (1 if enabled else 0, chat_id),
            )
            await db.commit()

    async def add_scan_job(self, chat_id: int, limit_count: int):
        async with aiosqlite.connect(self.path) as db:
            await db.execute(
                "INSERT INTO scan_jobs(chat_id, limit_count, created_at) VALUES(?, ?, ?)",
                (chat_id, limit_count, datetime.now(timezone.utc).isoformat()),
            )
            await db.commit()

    async def pending_jobs_count(self) -> int:
        async with aiosqlite.connect(self.path) as db:
            cur = await db.execute("SELECT COUNT(*) FROM scan_jobs WHERE status = 'pending'")
            row = await cur.fetchone()
            return row[0]
