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


def _parse_dev_ids(raw_ids: str) -> tuple[int, ...]:
    ids: list[int] = []
    for item in raw_ids.split(","):
        value = item.strip()
        if not value:
            continue
        try:
            ids.append(int(value))
        except ValueError:
            continue
    return tuple(ids)


def load_config() -> Config:
    raw_dev_ids = os.getenv("DEV_TELEGRAM_IDS") or os.getenv("DEV_TELEGRAM_ID", "")
    dev_ids = _parse_dev_ids(raw_dev_ids)

    return Config(
        bot_token=os.getenv("BOT_TOKEN", ""),
        bot_username=os.getenv("BOT_USERNAME", ""),
        dev_telegram_ids=dev_ids,
        support_url=os.getenv("SUPPORT_URL", "https://t.me/kiojomi"),
        tarif_message_1=os.getenv("TARIF_MESSAGE_1", "https://t.me/"),
        tarif_message_3=os.getenv("TARIF_MESSAGE_3", "https://t.me/"),
        tarif_message_6=os.getenv("TARIF_MESSAGE_6", "https://t.me/"),
        tarif_message_12=os.getenv("TARIF_MESSAGE_12", "https://t.me/"),
        hybrid_queue_threshold=int(os.getenv("HYBRID_QUEUE_THRESHOLD", "1000")),
        hybrid_scan_soft_timeout_ms=int(os.getenv("HYBRID_SCAN_SOFT_TIMEOUT_MS", "30000")),
        db_path=os.getenv("DB_PATH", "bot.db"),
    )
