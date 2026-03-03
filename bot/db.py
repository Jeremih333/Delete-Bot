import json
from datetime import datetime, timedelta, timezone
from typing import Any

import aiohttp
import aiosqlite


TABLE_STATEMENTS = [
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
    CREATE TABLE IF NOT EXISTS managed_chats (
      chat_id INTEGER PRIMARY KEY,
      title TEXT,
      owner_user_id INTEGER NOT NULL,
      enabled INTEGER NOT NULL DEFAULT 1,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      last_auto_enqueue_at TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS chat_admin_access (
      chat_id INTEGER NOT NULL,
      user_id INTEGER NOT NULL,
      granted_by INTEGER NOT NULL,
      created_at TEXT NOT NULL,
      PRIMARY KEY (chat_id, user_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS chat_settings (
      chat_id INTEGER PRIMARY KEY,
      check_interval_seconds INTEGER NOT NULL DEFAULT 3600,
      delete_deleted_enabled INTEGER NOT NULL DEFAULT 1,
      delete_frozen_enabled INTEGER NOT NULL DEFAULT 0,
      moderation_action TEXT NOT NULL DEFAULT 'ban'
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS tracked_members (
      chat_id INTEGER NOT NULL,
      user_id INTEGER NOT NULL,
      first_seen_at TEXT NOT NULL,
      last_seen_at TEXT NOT NULL,
      last_checked_at TEXT,
      last_reason TEXT,
      removed_at TEXT,
      PRIMARY KEY (chat_id, user_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS scan_jobs (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      chat_id INTEGER NOT NULL,
      limit_count INTEGER NOT NULL,
      priority INTEGER NOT NULL DEFAULT 0,
      status TEXT NOT NULL DEFAULT 'pending',
      created_at TEXT NOT NULL,
      finished_at TEXT
    )
    """,
]

INDEX_STATEMENTS = [
    "CREATE INDEX IF NOT EXISTS idx_scan_jobs_status_priority_id ON scan_jobs(status, priority, id)",
    "CREATE INDEX IF NOT EXISTS idx_subscriptions_expires ON subscriptions(expires_at)",
    "CREATE INDEX IF NOT EXISTS idx_tracked_members_chat_last_seen ON tracked_members(chat_id, last_seen_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_managed_chats_owner ON managed_chats(owner_user_id, enabled)",
]

MIGRATION_STATEMENTS = [
    "ALTER TABLE scan_jobs ADD COLUMN priority INTEGER NOT NULL DEFAULT 0",
    "ALTER TABLE chat_settings ADD COLUMN delete_deleted_enabled INTEGER NOT NULL DEFAULT 1",
    "ALTER TABLE chat_settings ADD COLUMN moderation_action TEXT NOT NULL DEFAULT 'ban'",
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
            for statement in TABLE_STATEMENTS:
                await db.execute(statement)
            for statement in MIGRATION_STATEMENTS:
                try:
                    await db.execute(statement)
                except aiosqlite.OperationalError:
                    pass
            for statement in INDEX_STATEMENTS:
                await db.execute(statement)
            await db.commit()

    async def fetchone(self, sql: str, params: tuple[Any, ...] = ()) -> tuple[Any, ...] | None:
        async with aiosqlite.connect(self.path) as db:
            cur = await db.execute(sql, params)
            return await cur.fetchone()

    async def fetchall(self, sql: str, params: tuple[Any, ...] = ()) -> list[tuple[Any, ...]]:
        async with aiosqlite.connect(self.path) as db:
            cur = await db.execute(sql, params)
            return await cur.fetchall()

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
            raise RuntimeError(f"D1 request failed: {data.get('errors', [])}")

        result = data.get("result") or []
        if not result:
            return {"results": []}

        first = result[0]
        if not first.get("success", True):
            raise RuntimeError(f"D1 SQL failed: {first.get('error') or first}")
        return first

    async def init(self):
        for statement in TABLE_STATEMENTS:
            await self._request(statement)
        for statement in MIGRATION_STATEMENTS:
            try:
                await self._request(statement)
            except RuntimeError:
                pass
        for statement in INDEX_STATEMENTS:
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

    async def upsert_managed_chat(self, chat_id: int, title: str, owner_user_id: int):
        now_iso = _iso_now()
        await self._backend.execute(
            """
            INSERT INTO managed_chats(chat_id, title, owner_user_id, enabled, created_at, updated_at)
            VALUES(?, ?, ?, 1, ?, ?)
            ON CONFLICT(chat_id) DO UPDATE SET
              title=excluded.title,
              owner_user_id=excluded.owner_user_id,
              enabled=1,
              updated_at=excluded.updated_at
            """,
            (chat_id, title, owner_user_id, now_iso, now_iso),
        )
        await self.ensure_chat_settings(chat_id)
        await self.grant_chat_admin(chat_id, owner_user_id, owner_user_id)

    async def disable_managed_chat(self, chat_id: int):
        await self._backend.execute(
            "UPDATE managed_chats SET enabled = 0, updated_at = ? WHERE chat_id = ?",
            (_iso_now(), chat_id),
        )

    async def list_owner_chats(self, owner_user_id: int) -> list[tuple[int, str, int]]:
        rows = await self._backend.fetchall(
            """
            SELECT chat_id, COALESCE(title, CAST(chat_id AS TEXT)), enabled
            FROM managed_chats
            WHERE owner_user_id = ?
            ORDER BY updated_at DESC
            """,
            (owner_user_id,),
        )
        return [(int(r[0]), str(r[1]), int(r[2])) for r in rows]

    async def get_managed_chat(self, chat_id: int) -> tuple[int, str, int, int] | None:
        row = await self._backend.fetchone(
            """
            SELECT chat_id, COALESCE(title, CAST(chat_id AS TEXT)), owner_user_id, enabled
            FROM managed_chats
            WHERE chat_id = ?
            """,
            (chat_id,),
        )
        if not row:
            return None
        return (int(row[0]), str(row[1]), int(row[2]), int(row[3]))

    async def ensure_chat_settings(self, chat_id: int):
        await self._backend.execute("INSERT OR IGNORE INTO chat_settings(chat_id) VALUES(?)", (chat_id,))

    async def get_chat_settings(self, chat_id: int) -> tuple[int, int, int, str]:
        await self.ensure_chat_settings(chat_id)
        row = await self._backend.fetchone(
            """
            SELECT check_interval_seconds, delete_deleted_enabled, delete_frozen_enabled, moderation_action
            FROM chat_settings
            WHERE chat_id = ?
            """,
            (chat_id,),
        )
        if not row:
            return (3600, 1, 0, "ban")
        interval, delete_deleted, delete_frozen, action = row
        action_norm = str(action or "ban").lower()
        if action_norm not in {"ban", "kick"}:
            action_norm = "ban"
        return (int(interval), int(delete_deleted), int(delete_frozen), action_norm)

    async def set_interval(self, chat_id: int, seconds: int):
        await self.ensure_chat_settings(chat_id)
        await self._backend.execute(
            "UPDATE chat_settings SET check_interval_seconds = ? WHERE chat_id = ?",
            (seconds, chat_id),
        )

    async def set_delete_deleted(self, chat_id: int, enabled: bool):
        await self.ensure_chat_settings(chat_id)
        await self._backend.execute(
            "UPDATE chat_settings SET delete_deleted_enabled = ? WHERE chat_id = ?",
            (1 if enabled else 0, chat_id),
        )

    async def set_frozen(self, chat_id: int, enabled: bool):
        await self.ensure_chat_settings(chat_id)
        await self._backend.execute(
            "UPDATE chat_settings SET delete_frozen_enabled = ? WHERE chat_id = ?",
            (1 if enabled else 0, chat_id),
        )

    async def set_moderation_action(self, chat_id: int, action: str):
        await self.ensure_chat_settings(chat_id)
        action_norm = action.lower().strip()
        if action_norm not in {"ban", "kick"}:
            action_norm = "ban"
        await self._backend.execute(
            "UPDATE chat_settings SET moderation_action = ? WHERE chat_id = ?",
            (action_norm, chat_id),
        )

    async def grant_chat_admin(self, chat_id: int, user_id: int, granted_by: int):
        await self._backend.execute(
            """
            INSERT INTO chat_admin_access(chat_id, user_id, granted_by, created_at)
            VALUES(?, ?, ?, ?)
            ON CONFLICT(chat_id, user_id) DO NOTHING
            """,
            (chat_id, user_id, granted_by, _iso_now()),
        )

    async def revoke_chat_admin(self, chat_id: int, user_id: int):
        await self._backend.execute(
            "DELETE FROM chat_admin_access WHERE chat_id = ? AND user_id = ?",
            (chat_id, user_id),
        )

    async def list_chat_admins(self, chat_id: int) -> list[int]:
        rows = await self._backend.fetchall(
            "SELECT user_id FROM chat_admin_access WHERE chat_id = ? ORDER BY created_at ASC",
            (chat_id,),
        )
        return [int(r[0]) for r in rows]

    async def has_chat_admin_access(self, chat_id: int, user_id: int) -> bool:
        row = await self._backend.fetchone(
            "SELECT 1 FROM chat_admin_access WHERE chat_id = ? AND user_id = ?",
            (chat_id, user_id),
        )
        return bool(row)

    async def track_member(self, chat_id: int, user_id: int):
        now_iso = _iso_now()
        await self._backend.execute(
            """
            INSERT INTO tracked_members(chat_id, user_id, first_seen_at, last_seen_at)
            VALUES(?, ?, ?, ?)
            ON CONFLICT(chat_id, user_id) DO UPDATE SET
              last_seen_at = excluded.last_seen_at
            """,
            (chat_id, user_id, now_iso, now_iso),
        )

    async def get_tracked_members_for_scan(self, chat_id: int, limit_count: int) -> list[int]:
        rows = await self._backend.fetchall(
            """
            SELECT user_id
            FROM tracked_members
            WHERE chat_id = ?
            ORDER BY last_seen_at DESC
            LIMIT ?
            """,
            (chat_id, limit_count),
        )
        return [int(r[0]) for r in rows]

    async def set_member_check_result(
        self,
        chat_id: int,
        user_id: int,
        reason: str | None = None,
        removed: bool = False,
    ):
        await self.track_member(chat_id, user_id)
        removed_at = _iso_now() if removed else None
        await self._backend.execute(
            """
            UPDATE tracked_members
            SET last_checked_at = ?, last_reason = ?, removed_at = ?
            WHERE chat_id = ? AND user_id = ?
            """,
            (_iso_now(), reason, removed_at, chat_id, user_id),
        )

    async def add_scan_job(self, chat_id: int, limit_count: int, priority: int = 0):
        await self._backend.execute(
            "INSERT INTO scan_jobs(chat_id, limit_count, priority, created_at) VALUES(?, ?, ?, ?)",
            (chat_id, limit_count, priority, _iso_now()),
        )

    async def pending_jobs_count(self) -> int:
        row = await self._backend.fetchone("SELECT COUNT(*) FROM scan_jobs WHERE status = 'pending'")
        return int(row[0]) if row else 0

    async def claim_pending_scan_jobs(self, limit: int = 20) -> list[tuple[int, int, int]]:
        try:
            rows = await self._backend.fetchall(
                """
                WITH picked AS (
                  SELECT id, priority
                  FROM scan_jobs
                  WHERE status = 'pending'
                  ORDER BY priority DESC, id
                  LIMIT ?
                )
                UPDATE scan_jobs
                SET status = 'processing'
                WHERE id IN (SELECT id FROM picked)
                RETURNING id, chat_id, limit_count, priority
                """,
                (limit,),
            )
            normalized = [(int(r[0]), int(r[1]), int(r[2]), int(r[3])) for r in rows]
            normalized.sort(key=lambda row: (-row[3], row[0]))
            return [(row[0], row[1], row[2]) for row in normalized]
        except Exception:
            rows = await self._backend.fetchall(
                """
                SELECT id, chat_id, limit_count
                FROM scan_jobs
                WHERE status = 'pending'
                ORDER BY priority DESC, id
                LIMIT ?
                """,
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
        await self._backend.execute("UPDATE scan_jobs SET status = ? WHERE id = ?", (status, job_id))

    async def list_chats_due_for_auto_enqueue(self, limit: int = 100) -> list[int]:
        rows = await self._backend.fetchall(
            """
            SELECT mc.chat_id, mc.last_auto_enqueue_at, cs.check_interval_seconds
            FROM managed_chats mc
            JOIN chat_settings cs ON cs.chat_id = mc.chat_id
            WHERE mc.enabled = 1
            LIMIT ?
            """,
            (limit,),
        )
        now = _utc_now()
        due: list[int] = []
        for chat_id_raw, last_auto, interval_raw in rows:
            chat_id = int(chat_id_raw)
            interval = int(interval_raw) if interval_raw is not None else 3600
            if not last_auto:
                due.append(chat_id)
                continue
            try:
                last_dt = datetime.fromisoformat(str(last_auto))
            except ValueError:
                due.append(chat_id)
                continue
            if last_dt.tzinfo is None:
                last_dt = last_dt.replace(tzinfo=timezone.utc)
            if (now - last_dt).total_seconds() >= interval:
                due.append(chat_id)
        return due

    async def touch_chat_auto_enqueue(self, chat_id: int):
        await self._backend.execute(
            "UPDATE managed_chats SET last_auto_enqueue_at = ?, updated_at = ? WHERE chat_id = ?",
            (_iso_now(), _iso_now(), chat_id),
        )
