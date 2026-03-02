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

    def is_dev(self, user_id: int | None) -> bool:
        return bool(user_id) and user_id in self.dev_telegram_ids


def _to_int(value: str | None) -> int | None:
    if value is None:
        return None
    cleaned = value.strip()
    if not cleaned:
        return None
    try:
        return int(cleaned)
    except ValueError:
        return None


def _collect_dev_ids() -> tuple[int, ...]:
    """
    Canonical order:
    1) DEV_TELEGRAM_ID
    2) DEV_TELEGRAM_ID_2
    3) DEV_TELEGRAM_IDS (legacy CSV fallback)
    """
    unique_ids: list[int] = []

    for env_name in ("DEV_TELEGRAM_ID", "DEV_TELEGRAM_ID_2"):
        parsed = _to_int(os.getenv(env_name))
        if parsed and parsed not in unique_ids:
            unique_ids.append(parsed)

    legacy_csv = os.getenv("DEV_TELEGRAM_IDS", "")
    for raw in legacy_csv.split(","):
        parsed = _to_int(raw)
        if parsed and parsed not in unique_ids:
            unique_ids.append(parsed)

    return tuple(unique_ids)


def load_config() -> Config:
    return Config(
        bot_token=os.getenv("BOT_TOKEN", ""),
        bot_username=os.getenv("BOT_USERNAME", ""),
        dev_telegram_ids=_collect_dev_ids(),
        support_url=os.getenv("SUPPORT_URL", "https://t.me/kiojomi"),
        tarif_message_1=os.getenv("TARIF_MESSAGE_1", "https://t.me/"),
        tarif_message_3=os.getenv("TARIF_MESSAGE_3", "https://t.me/"),
        tarif_message_6=os.getenv("TARIF_MESSAGE_6", "https://t.me/"),
        tarif_message_12=os.getenv("TARIF_MESSAGE_12", "https://t.me/"),
        hybrid_queue_threshold=int(os.getenv("HYBRID_QUEUE_THRESHOLD", "1000")),
        hybrid_scan_soft_timeout_ms=int(os.getenv("HYBRID_SCAN_SOFT_TIMEOUT_MS", "30000")),
        db_path=os.getenv("DB_PATH", "bot.db"),
    )
