from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

import aiohttp
import aiosqlite

from bot.config import Config

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


class SQLiteDatabase:
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
            await db.execute("INSERT OR IGNORE INTO chat_settings(chat_id) VALUES(?)", (chat_id,))
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

    async def fetch_pending_scan_jobs(self, limit: int = 20):
        async with aiosqlite.connect(self.path) as db:
            cur = await db.execute(
                "SELECT id, chat_id, limit_count FROM scan_jobs WHERE status='pending' ORDER BY id LIMIT ?",
                (limit,),
            )
            return await cur.fetchall()

    async def mark_scan_job_done(self, job_id: int):
        async with aiosqlite.connect(self.path) as db:
            await db.execute(
                "UPDATE scan_jobs SET status='done', finished_at=? WHERE id=?",
                (datetime.now(timezone.utc).isoformat(), job_id),
            )
            await db.commit()


class D1Database:
    def __init__(self, account_id: str, database_id: str, api_token: str):
        self.account_id = account_id
        self.database_id = database_id
        self.api_token = api_token
        self.endpoint = (
            f"https://api.cloudflare.com/client/v4/accounts/{self.account_id}/d1/database/{self.database_id}/query"
        )

    async def _query(self, sql: str, params: list[Any] | None = None) -> dict[str, Any]:
        payload = {"sql": sql, "params": params or []}
        headers = {
            "Authorization": f"Bearer {self.api_token}",
            "Content-Type": "application/json",
        }
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as session:
            async with session.post(self.endpoint, json=payload, headers=headers) as resp:
                data = await resp.json()
                if resp.status >= 400 or not data.get("success", False):
                    raise RuntimeError(f"D1 query failed: {data}")
                result = data.get("result", [])
                return result[0] if result else {}

    async def _execute_script(self, script: str):
        for stmt in [s.strip() for s in script.split(";") if s.strip()]:
            await self._query(stmt)

    async def init(self):
        await self._execute_script(SCHEMA)

    async def get_subscription(self, user_id: int):
        res = await self._query(
            "SELECT plan_months, started_at, expires_at FROM subscriptions WHERE user_id = ?",
            [user_id],
        )
        rows = res.get("results", [])
        if not rows:
            return None
        row = rows[0]
        return row["plan_months"], row["started_at"], row["expires_at"]

    async def set_subscription(self, user_id: int, months: int):
        now = datetime.now(timezone.utc)
        exp = now + timedelta(days=30 * months)
        await self._query(
            """
            INSERT INTO subscriptions(user_id, plan_months, started_at, expires_at)
            VALUES(?, ?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
              plan_months=excluded.plan_months,
              started_at=excluded.started_at,
              expires_at=excluded.expires_at
            """,
            [user_id, months, now.isoformat(), exp.isoformat()],
        )

    async def is_premium(self, user_id: int) -> bool:
        row = await self.get_subscription(user_id)
        if not row:
            return False
        _, _, expires_at = row
        return datetime.fromisoformat(expires_at) > datetime.now(timezone.utc)

    async def add_helper(self, chat_id: int, user_id: int):
        await self._query(
            "INSERT OR REPLACE INTO roles(chat_id, user_id, role) VALUES(?, ?, 'helper')",
            [chat_id, user_id],
        )

    async def get_role(self, chat_id: int, user_id: int):
        res = await self._query(
            "SELECT role FROM roles WHERE chat_id = ? AND user_id = ?",
            [chat_id, user_id],
        )
        rows = res.get("results", [])
        return rows[0]["role"] if rows else None

    async def ensure_chat_settings(self, chat_id: int):
        await self._query("INSERT OR IGNORE INTO chat_settings(chat_id) VALUES(?)", [chat_id])

    async def get_chat_settings(self, chat_id: int):
        await self.ensure_chat_settings(chat_id)
        res = await self._query(
            "SELECT check_interval_seconds, delete_frozen_enabled FROM chat_settings WHERE chat_id = ?",
            [chat_id],
        )
        rows = res.get("results", [])
        if not rows:
            return 3600, 0
        row = rows[0]
        return row["check_interval_seconds"], row["delete_frozen_enabled"]

    async def set_interval(self, chat_id: int, seconds: int):
        await self.ensure_chat_settings(chat_id)
        await self._query(
            "UPDATE chat_settings SET check_interval_seconds = ? WHERE chat_id = ?",
            [seconds, chat_id],
        )

    async def set_frozen(self, chat_id: int, enabled: bool):
        await self.ensure_chat_settings(chat_id)
        await self._query(
            "UPDATE chat_settings SET delete_frozen_enabled = ? WHERE chat_id = ?",
            [1 if enabled else 0, chat_id],
        )

    async def add_scan_job(self, chat_id: int, limit_count: int):
        await self._query(
            "INSERT INTO scan_jobs(chat_id, limit_count, created_at) VALUES(?, ?, ?)",
            [chat_id, limit_count, datetime.now(timezone.utc).isoformat()],
        )

    async def pending_jobs_count(self) -> int:
        res = await self._query("SELECT COUNT(*) AS count FROM scan_jobs WHERE status = 'pending'")
        rows = res.get("results", [])
        return int(rows[0]["count"]) if rows else 0

    async def fetch_pending_scan_jobs(self, limit: int = 20):
        res = await self._query(
            "SELECT id, chat_id, limit_count FROM scan_jobs WHERE status='pending' ORDER BY id LIMIT ?",
            [limit],
        )
        rows = res.get("results", [])
        return [(row["id"], row["chat_id"], row["limit_count"]) for row in rows]

    async def mark_scan_job_done(self, job_id: int):
        await self._query(
            "UPDATE scan_jobs SET status='done', finished_at=? WHERE id=?",
            [datetime.now(timezone.utc).isoformat(), job_id],
        )


def build_database(cfg: Config):
    if cfg.db_backend == "d1":
        if not cfg.cf_account_id or not cfg.d1_database_id or not cfg.d1_api_token:
            raise RuntimeError(
                "DB_BACKEND=d1 requires CF_ACCOUNT_ID, D1_DATABASE_ID and D1_API_TOKEN"
            )
        return D1Database(
            account_id=cfg.cf_account_id,
            database_id=cfg.d1_database_id,
            api_token=cfg.d1_api_token,
        )

    return SQLiteDatabase(cfg.db_path)
