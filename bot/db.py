import json
from datetime import datetime, timedelta, timezone
from typing import Any

import aiohttp
import aiosqlite


SCHEMA_STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS subscriptions (
      user_id INTEGER PRIMARY KEY,
      plan_months INTEGER NOT NULL,
      started_at TEXT NOT NULL,
      expires_at TEXT NOT NULL,
      granted_by INTEGER,
      updated_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS chat_settings (
      chat_id INTEGER PRIMARY KEY,
      check_interval_seconds INTEGER NOT NULL DEFAULT 3600,
      delete_frozen_enabled INTEGER NOT NULL DEFAULT 0
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS roles (
      chat_id INTEGER NOT NULL,
      user_id INTEGER NOT NULL,
      role TEXT NOT NULL,
      PRIMARY KEY (chat_id, user_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS scan_jobs (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      chat_id INTEGER NOT NULL,
      limit_count INTEGER NOT NULL,
      status TEXT NOT NULL DEFAULT 'pending',
      created_at TEXT NOT NULL,
      finished_at TEXT
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_scan_jobs_status_id ON scan_jobs(status, id)",
    "CREATE INDEX IF NOT EXISTS idx_subscriptions_expires ON subscriptions(expires_at)",
]


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _iso_now() -> str:
    return _utc_now().isoformat()


def _add_months_from(base: datetime, months: int) -> datetime:
    return base + timedelta(days=30 * months)


class _SQLiteBackend:
    def __init__(self, path: str):
        self.path = path

    async def init(self):
        async with aiosqlite.connect(self.path) as db:
            for statement in SCHEMA_STATEMENTS:
                await db.execute(statement)
            await db.commit()

    async def fetchone(self, sql: str, params: tuple[Any, ...] = ()) -> tuple[Any, ...] | None:
        async with aiosqlite.connect(self.path) as db:
            cur = await db.execute(sql, params)
            row = await cur.fetchone()
            return row

    async def fetchall(self, sql: str, params: tuple[Any, ...] = ()) -> list[tuple[Any, ...]]:
        async with aiosqlite.connect(self.path) as db:
            cur = await db.execute(sql, params)
            rows = await cur.fetchall()
            return rows

    async def execute(self, sql: str, params: tuple[Any, ...] = ()):
        async with aiosqlite.connect(self.path) as db:
            await db.execute(sql, params)
            await db.commit()


class _CloudflareD1Backend:
    def __init__(self, account_id: str, database_id: str, api_token: str):
        self.account_id = account_id
        self.database_id = database_id
        self.api_token = api_token
        self.base_url = (
            "https://api.cloudflare.com/client/v4/accounts/"
            f"{self.account_id}/d1/database/{self.database_id}/query"
        )

    def _headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self.api_token}",
            "Content-Type": "application/json",
        }

    async def _request(self, sql: str, params: tuple[Any, ...] = ()) -> dict[str, Any]:
        payload: dict[str, Any] = {"sql": sql}
        if params:
            payload["params"] = list(params)

        timeout = aiohttp.ClientTimeout(total=30)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(self.base_url, headers=self._headers(), json=payload) as resp:
                text = await resp.text()
                if resp.status >= 400:
                    raise RuntimeError(f"D1 HTTP {resp.status}: {text[:300]}")

        data = json.loads(text)
        if not data.get("success", False):
            errors = data.get("errors", [])
            raise RuntimeError(f"D1 request failed: {errors}")

        result = data.get("result") or []
        if not result:
            return {"results": []}

        first = result[0]
        if not first.get("success", True):
            raise RuntimeError(f"D1 SQL failed: {first.get('error') or first}")
        return first

    async def init(self):
        for statement in SCHEMA_STATEMENTS:
            await self._request(statement)

    async def fetchone(self, sql: str, params: tuple[Any, ...] = ()) -> tuple[Any, ...] | None:
        rows = await self.fetchall(sql, params)
        return rows[0] if rows else None

    async def fetchall(self, sql: str, params: tuple[Any, ...] = ()) -> list[tuple[Any, ...]]:
        result = await self._request(sql, params)
        rows = result.get("results", [])
        if not rows:
            return []
        keys = list(rows[0].keys())
        return [tuple(row.get(key) for key in keys) for row in rows]

    async def execute(self, sql: str, params: tuple[Any, ...] = ()):
        await self._request(sql, params)


class Database:
    def __init__(
        self,
        path: str,
        backend: str = "sqlite",
        cloudflare_account_id: str = "",
        cloudflare_d1_database_id: str = "",
        cloudflare_api_token: str = "",
    ):
        mode = (backend or "sqlite").strip().lower()
        if mode == "cloudflare_d1":
            if not (cloudflare_account_id and cloudflare_d1_database_id and cloudflare_api_token):
                raise RuntimeError(
                    "Cloudflare D1 backend requires CLOUDFLARE_ACCOUNT_ID, "
                    "CLOUDFLARE_D1_DATABASE_ID and CLOUDFLARE_API_TOKEN"
                )
            self._backend = _CloudflareD1Backend(
                cloudflare_account_id,
                cloudflare_d1_database_id,
                cloudflare_api_token,
            )
        else:
            self._backend = _SQLiteBackend(path)

    async def init(self):
        await self._backend.init()

    async def get_subscription(self, user_id: int):
        return await self._backend.fetchone(
            "SELECT plan_months, started_at, expires_at FROM subscriptions WHERE user_id = ?",
            (user_id,),
        )

    async def set_subscription(self, user_id: int, months: int, granted_by: int | None = None):
        existing = await self._backend.fetchone(
            "SELECT expires_at FROM subscriptions WHERE user_id = ?",
            (user_id,),
        )
        now = _utc_now()
        start_base = now
        if existing and existing[0]:
            try:
                existing_expiry = datetime.fromisoformat(existing[0])
                if existing_expiry > now:
                    start_base = existing_expiry
            except ValueError:
                pass

        exp = _add_months_from(start_base, months)
        now_iso = now.isoformat()
        await self._backend.execute(
            """
            INSERT INTO subscriptions(user_id, plan_months, started_at, expires_at, granted_by, updated_at)
            VALUES(?, ?, ?, ?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
              plan_months=excluded.plan_months,
              started_at=excluded.started_at,
              expires_at=excluded.expires_at,
              granted_by=excluded.granted_by,
              updated_at=excluded.updated_at
            """,
            (user_id, months, now_iso, exp.isoformat(), granted_by, now_iso),
        )

    async def is_premium(self, user_id: int) -> bool:
        row = await self.get_subscription(user_id)
        if not row:
            return False
        _, _, expires_at = row
        try:
            return datetime.fromisoformat(expires_at) > _utc_now()
        except ValueError:
            return False

    async def premium_remaining_seconds(self, user_id: int) -> int:
        row = await self.get_subscription(user_id)
        if not row:
            return 0
        _, _, expires_at = row
        try:
            delta = datetime.fromisoformat(expires_at) - _utc_now()
            return max(0, int(delta.total_seconds()))
        except ValueError:
            return 0

    async def list_active_subscribers(self, limit: int = 1000) -> list[tuple[int, str, int]]:
        rows = await self._backend.fetchall(
            """
            SELECT user_id, expires_at, plan_months
            FROM subscriptions
            WHERE expires_at > ?
            ORDER BY expires_at DESC
            LIMIT ?
            """,
            (_iso_now(), limit),
        )
        return [(int(r[0]), str(r[1]), int(r[2])) for r in rows]

    async def add_helper(self, chat_id: int, user_id: int):
        await self._backend.execute(
            "INSERT OR REPLACE INTO roles(chat_id, user_id, role) VALUES(?, ?, 'helper')",
            (chat_id, user_id),
        )

    async def get_role(self, chat_id: int, user_id: int):
        row = await self._backend.fetchone(
            "SELECT role FROM roles WHERE chat_id = ? AND user_id = ?",
            (chat_id, user_id),
        )
        return row[0] if row else None

    async def ensure_chat_settings(self, chat_id: int):
        await self._backend.execute(
            "INSERT OR IGNORE INTO chat_settings(chat_id) VALUES(?)",
            (chat_id,),
        )

    async def get_chat_settings(self, chat_id: int):
        await self.ensure_chat_settings(chat_id)
        row = await self._backend.fetchone(
            "SELECT check_interval_seconds, delete_frozen_enabled FROM chat_settings WHERE chat_id = ?",
            (chat_id,),
        )
        return row if row else (3600, 0)

    async def set_interval(self, chat_id: int, seconds: int):
        await self.ensure_chat_settings(chat_id)
        await self._backend.execute(
            "UPDATE chat_settings SET check_interval_seconds = ? WHERE chat_id = ?",
            (seconds, chat_id),
        )

    async def set_frozen(self, chat_id: int, enabled: bool):
        await self.ensure_chat_settings(chat_id)
        await self._backend.execute(
            "UPDATE chat_settings SET delete_frozen_enabled = ? WHERE chat_id = ?",
            (1 if enabled else 0, chat_id),
        )

    async def add_scan_job(self, chat_id: int, limit_count: int):
        await self._backend.execute(
            "INSERT INTO scan_jobs(chat_id, limit_count, created_at) VALUES(?, ?, ?)",
            (chat_id, limit_count, _iso_now()),
        )

    async def pending_jobs_count(self) -> int:
        row = await self._backend.fetchone(
            "SELECT COUNT(*) FROM scan_jobs WHERE status = 'pending'",
        )
        return int(row[0]) if row else 0

    async def claim_pending_scan_jobs(self, limit: int = 20) -> list[tuple[int, int, int]]:
        try:
            rows = await self._backend.fetchall(
                """
                WITH picked AS (
                  SELECT id FROM scan_jobs
                  WHERE status = 'pending'
                  ORDER BY id
                  LIMIT ?
                )
                UPDATE scan_jobs
                SET status = 'processing'
                WHERE id IN (SELECT id FROM picked)
                RETURNING id, chat_id, limit_count
                """,
                (limit,),
            )
            return [(int(r[0]), int(r[1]), int(r[2])) for r in rows]
        except Exception:
            rows = await self._backend.fetchall(
                "SELECT id, chat_id, limit_count FROM scan_jobs WHERE status = 'pending' ORDER BY id LIMIT ?",
                (limit,),
            )
            if not rows:
                return []
            ids = [int(r[0]) for r in rows]
            placeholders = ",".join("?" for _ in ids)
            await self._backend.execute(
                f"UPDATE scan_jobs SET status = 'processing' WHERE status = 'pending' AND id IN ({placeholders})",
                tuple(ids),
            )
            return [(int(r[0]), int(r[1]), int(r[2])) for r in rows]

    async def set_scan_job_status(self, job_id: int, status: str, set_finished_at: bool = False):
        if set_finished_at:
            await self._backend.execute(
                "UPDATE scan_jobs SET status = ?, finished_at = ? WHERE id = ?",
                (status, _iso_now(), job_id),
            )
            return
        await self._backend.execute(
            "UPDATE scan_jobs SET status = ? WHERE id = ?",
            (status, job_id),
        )
