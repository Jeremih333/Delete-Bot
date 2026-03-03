from dataclasses import dataclass
import os
import re


@dataclass
class Config:
    bot_token: str
    bot_username: str
    dev_telegram_ids: tuple[int, ...]
    support_url: str

    tarif_message_1: str
    tarif_message_3: str
    tarif_message_6: str
    tarif_message_12: str

    hybrid_queue_threshold: int
    hybrid_scan_soft_timeout_ms: int

    db_backend: str
    db_path: str
    cloudflare_account_id: str
    cloudflare_d1_database_id: str
    cloudflare_api_token: str


def _parse_int_env(name: str, default: int) -> int:
    raw = os.getenv(name, str(default)).strip()
    try:
        return int(raw)
    except ValueError:
        return default


def _parse_dev_telegram_ids() -> tuple[int, ...]:
    raw_ids = os.getenv("DEV_TELEGRAM_IDS", "").strip()
    if raw_ids:
        parsed: list[int] = []
        for item in (chunk.strip() for chunk in raw_ids.split(",")):
            if not item:
                continue
            try:
                parsed.append(int(item))
            except ValueError:
                continue
        return tuple(parsed)

    collected: list[int] = []
    for key, value in os.environ.items():
        if not re.fullmatch(r"DEV_TELEGRAM_ID(?:_\d+)?", key):
            continue
        raw = value.strip()
        if not raw or raw == "0":
            continue
        try:
            collected.append(int(raw))
        except ValueError:
            continue
    # Preserve deterministic order and remove duplicates.
    seen: set[int] = set()
    result: list[int] = []
    for item in sorted(collected):
        if item in seen:
            continue
        seen.add(item)
        result.append(item)
    return tuple(result)



def load_config() -> Config:
    return Config(
        bot_token=os.getenv("BOT_TOKEN", ""),
        bot_username=os.getenv("BOT_USERNAME", ""),
        dev_telegram_ids=_parse_dev_telegram_ids(),
        support_url=os.getenv("SUPPORT_URL", "https://t.me/kiojomi"),
        tarif_message_1=os.getenv("TARIF_MESSAGE_1", "https://t.me/"),
        tarif_message_3=os.getenv("TARIF_MESSAGE_3", "https://t.me/"),
        tarif_message_6=os.getenv("TARIF_MESSAGE_6", "https://t.me/"),
        tarif_message_12=os.getenv("TARIF_MESSAGE_12", "https://t.me/"),
        hybrid_queue_threshold=_parse_int_env("HYBRID_QUEUE_THRESHOLD", 1000),
        hybrid_scan_soft_timeout_ms=_parse_int_env("HYBRID_SCAN_SOFT_TIMEOUT_MS", 300000),
        db_backend=os.getenv("DB_BACKEND", "sqlite").strip().lower(),
        db_path=os.getenv("DB_PATH", "bot.db"),
        cloudflare_account_id=os.getenv("CLOUDFLARE_ACCOUNT_ID", "").strip(),
        cloudflare_d1_database_id=os.getenv("CLOUDFLARE_D1_DATABASE_ID", "").strip(),
        cloudflare_api_token=os.getenv("CLOUDFLARE_API_TOKEN", "").strip(),
    )
