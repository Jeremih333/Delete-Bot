import os
import unittest

from bot.config import load_config


class TestConfig(unittest.TestCase):
    def test_parse_dev_ids_and_int_fallback(self):
        prev = {
            k: os.environ.get(k)
            for k in ("DEV_TELEGRAM_IDS", "HYBRID_QUEUE_THRESHOLD", "DEV_TELEGRAM_ID", "DEV_TELEGRAM_ID_2")
        }
        try:
            os.environ["DEV_TELEGRAM_IDS"] = "123, abc, 456, , 789"
            os.environ["DEV_TELEGRAM_ID"] = "7777777"
            os.environ["DEV_TELEGRAM_ID_2"] = "8888888"
            os.environ["HYBRID_QUEUE_THRESHOLD"] = "bad-int"
            cfg = load_config()
            self.assertEqual(cfg.dev_telegram_ids, (123, 456, 789))
            self.assertEqual(cfg.hybrid_queue_threshold, 1000)
        finally:
            for key, val in prev.items():
                if val is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = val

    def test_parse_legacy_multiple_dev_ids(self):
        prev = {k: os.environ.get(k) for k in ("DEV_TELEGRAM_IDS", "DEV_TELEGRAM_ID", "DEV_TELEGRAM_ID_2")}
        try:
            os.environ["DEV_TELEGRAM_IDS"] = ""
            os.environ["DEV_TELEGRAM_ID"] = "1001"
            os.environ["DEV_TELEGRAM_ID_2"] = "1002"
            cfg = load_config()
            self.assertEqual(cfg.dev_telegram_ids, (1001, 1002))
        finally:
            for key, val in prev.items():
                if val is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = val

    def test_cloudflare_fields_loaded(self):
        keys = (
            "DB_BACKEND",
            "CLOUDFLARE_ACCOUNT_ID",
            "CLOUDFLARE_D1_DATABASE_ID",
            "CLOUDFLARE_API_TOKEN",
        )
        prev = {k: os.environ.get(k) for k in keys}
        try:
            os.environ["DB_BACKEND"] = "cloudflare_d1"
            os.environ["CLOUDFLARE_ACCOUNT_ID"] = "acc"
            os.environ["CLOUDFLARE_D1_DATABASE_ID"] = "db"
            os.environ["CLOUDFLARE_API_TOKEN"] = "token"
            cfg = load_config()
            self.assertEqual(cfg.db_backend, "cloudflare_d1")
            self.assertEqual(cfg.cloudflare_account_id, "acc")
            self.assertEqual(cfg.cloudflare_d1_database_id, "db")
            self.assertEqual(cfg.cloudflare_api_token, "token")
        finally:
            for key, val in prev.items():
                if val is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = val


if __name__ == "__main__":
    unittest.main()
