import tempfile
import unittest
from pathlib import Path

from fastapi.testclient import TestClient

from marketcow.api import create_app
from marketcow.config import Settings
from marketcow.storage import Warehouse


def bars():
    return [
        {"timestamp": 100, "bar_at": "1970-01-01T00:01:40Z", "open": 1,
         "high": 2, "low": 0.5, "close": 1.5, "raw_close": None,
         "adjustment_factor": None, "volume": 10, "amount": None},
        {"timestamp": 200, "bar_at": "1970-01-01T00:03:20Z", "open": 2,
         "high": 3, "low": 1.5, "close": 2.5, "raw_close": 5,
         "adjustment_factor": 0.5, "volume": 20, "amount": 50},
        {"timestamp": 300, "bar_at": "1970-01-01T00:05:00Z", "open": 3,
         "high": 4, "low": 2.5, "close": 3.5, "raw_close": 7,
         "adjustment_factor": 0.5, "volume": 30, "amount": 105},
    ]


class RangeService:
    def __init__(self, repository):
        self.market_bar_repository = repository
        self.refresh_calls = 0

    def refresh_quote_history(self, *args):
        self.refresh_calls += 1
        return {"bars": []}

    def close(self):
        pass


class MarketBarRangeTest(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        root = Path(self.folder.name)
        self.warehouse = Warehouse(root / "warehouse.duckdb")
        self.warehouse.upsert_price_bars(
            "AAPL", "1m", "raw", "fixture", "2026-07-20T00:00:00Z", bars()
        )

    def tearDown(self):
        self.folder.cleanup()

    def test_duckdb_closed_range_limit_truncation_utc_and_empty(self):
        rows, truncated = self.warehouse.get_price_bars_range(
            "AAPL", "1m", "raw", "1970-01-01T08:01:40+08:00",
            "1970-01-01T00:05:00Z", 2,
        )
        self.assertEqual([row["timestamp"] for row in rows], [100, 200])
        self.assertTrue(truncated)
        self.assertEqual(rows[0]["bar_at"], "1970-01-01T00:01:40+00:00")
        self.assertIsNone(rows[0]["raw_close"])
        empty, truncated = self.warehouse.get_price_bars_range(
            "AAPL", "1m", "raw", "1970-01-02T00:00:00Z",
            "1970-01-02T01:00:00Z", 10,
        )
        self.assertEqual(empty, [])
        self.assertFalse(truncated)
        with self.assertRaisesRegex(ValueError, "include a timezone"):
            self.warehouse.get_price_bars_range(
                "AAPL", "1m", "raw", "1970-01-01T00:00:00",
                "1970-01-01T01:00:00", 10,
            )

    def test_api_range_is_cached_and_old_request_behavior_is_unchanged(self):
        service = RangeService(self.warehouse)
        settings = Settings(
            Path(self.folder.name) / "db", Path(self.folder.name) / "raw"
        )
        with TestClient(create_app(settings, service)) as client:
            response = client.get(
                "/v1/quotes/AAPL/history?interval=1m&adjustment=raw"
                "&start=1970-01-01T00:01:40Z&end=1970-01-01T00:05:00Z&limit=2"
            )
            self.assertEqual(response.status_code, 200, response.text)
            payload = response.json()
            self.assertTrue(payload["cached"])
            self.assertTrue(payload["truncated"])
            self.assertEqual(payload["count"], 2)
            self.assertEqual(service.refresh_calls, 0)
            invalid = client.get(
                "/v1/quotes/AAPL/history?interval=1m&adjustment=raw"
                "&start=1970-01-01T00:00:00Z"
            )
            self.assertEqual(invalid.status_code, 400)
            reversed_range = client.get(
                "/v1/quotes/AAPL/history?interval=1m&adjustment=raw"
                "&start=1970-01-01T01:00:00Z&end=1970-01-01T00:00:00Z"
            )
            self.assertEqual(reversed_range.status_code, 400)
            old = client.get(
                "/v1/quotes/AAPL/history?interval=1m&adjustment=raw"
            )
            self.assertEqual(old.status_code, 200)
            self.assertEqual(service.refresh_calls, 1)


if __name__ == "__main__":
    unittest.main()
