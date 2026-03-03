from datetime import datetime, timezone

from bot.db import Database


def make_window_key(chat_id: int, interval_seconds: int, source: str = "auto") -> str:
    # Keep source argument for backward compatibility, but do not include it in dedupe key.
    # This guarantees that Render and GitHub worker share the same idempotency window.
    _ = source
    now_bucket = int(datetime.now(timezone.utc).timestamp() // max(1, interval_seconds))
    return f"{chat_id}:{now_bucket}"


async def enqueue_scan_if_absent(
    db: Database,
    chat_id: int,
    interval_seconds: int,
    limit_count: int,
    priority: int,
    source: str = "auto",
) -> bool:
    return await db.enqueue_scan_job_if_absent(
        chat_id=chat_id,
        window_key=make_window_key(chat_id, interval_seconds, source=source),
        limit_count=limit_count,
        priority=priority,
    )
