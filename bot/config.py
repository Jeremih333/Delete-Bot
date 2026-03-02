from dataclasses import dataclass
import os


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

    db_path: str


def _to_int(value: str | None) -> int | None:
    if not value:
        return None
    try:
        return int(value.strip())
    except ValueError:
        return None


def _build_dev_ids() -> tuple[int, ...]:
    primary = _to_int(os.getenv("DEV_TELEGRAM_ID"))
    secondary = _to_int(os.getenv("DEV_TELEGRAM_ID_2"))

    ids: list[int] = []
    if primary:
        ids.append(primary)
    if secondary and secondary not in ids:
        ids.append(secondary)

    # Backward compatibility for older comma-separated setup
    fallback_csv = os.getenv("DEV_TELEGRAM_IDS", "")
    for raw in fallback_csv.split(","):
        parsed = _to_int(raw)
        if parsed and parsed not in ids:
            ids.append(parsed)

    return tuple(ids)


def load_config() -> Config:
    return Config(
        bot_token=os.getenv("BOT_TOKEN", ""),
        bot_username=os.getenv("BOT_USERNAME", ""),
        dev_telegram_ids=_build_dev_ids(),
        support_url=os.getenv("SUPPORT_URL", "https://t.me/kiojomi"),
        tarif_message_1=os.getenv("TARIF_MESSAGE_1", "https://t.me/"),
        tarif_message_3=os.getenv("TARIF_MESSAGE_3", "https://t.me/"),
        tarif_message_6=os.getenv("TARIF_MESSAGE_6", "https://t.me/"),
        tarif_message_12=os.getenv("TARIF_MESSAGE_12", "https://t.me/"),
        hybrid_queue_threshold=int(os.getenv("HYBRID_QUEUE_THRESHOLD", "1000")),
        hybrid_scan_soft_timeout_ms=int(os.getenv("HYBRID_SCAN_SOFT_TIMEOUT_MS", "30000")),
        db_path=os.getenv("DB_PATH", "bot.db"),
    )
