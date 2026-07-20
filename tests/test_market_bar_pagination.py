import base64
import hashlib
import hmac
import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from fastapi.testclient import TestClient

from marketcow.api import create_app
from marketcow.config import Settings
from marketcow.storage import Warehouse


NOW = datetime(2026, 7, 20, 12, 0, tzinfo=timezone.utc)


def bar(timestamp):
    return {
        "timestamp": timestamp,
        "bar_at": datetime.fromtimestamp(timestamp, timezone.utc).isoformat(),
        "open": float(timestamp), "high": float(timestamp + 1),
        "low": float(timestamp - 1), "close": float(timestamp) + 0.5,
        "volume": 1.0, "amount": None,
    }


class Service:
    def __init__(self, repository):
        self.market_bar_repository = repository

    def close(self):
        pass


class CountingRepository:
    def __init__(self, warehouse):
        self.warehouse = warehouse
        self.page_calls = 0

    def get_price_bars_page(self, *args):
        self.page_calls += 1
        return self.warehouse.get_price_bars_page(*args)

    def __getattr__(self, name):
        return getattr(self.warehouse, name)


class MarketBarPaginationTest(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        root = Path(self.folder.name)
        self.warehouse = Warehouse(root / "warehouse.duckdb")
        self.repository = CountingRepository(self.warehouse)
        self.settings = Settings(
            root / "warehouse.duckdb", root / "raw",
            market_bar_cursor_secret="pagination-test-secret-123",
            market_bar_cursor_ttl_seconds=3600,
        )

    def tearDown(self):
        self.folder.cleanup()

    def client(self):
        return TestClient(create_app(
            self.settings, Service(self.repository), lambda: NOW
        ))

    def seed(self, timestamps):
        self.warehouse.upsert_price_bars(
            "AAPL", "1m", "raw", "fixture", "2026-07-20T11:59:00Z",
            [bar(value) for value in timestamps],
        )

    def path(self, page_size, cursor=None, symbol="AAPL", start=100, end=110):
        start_text = datetime.fromtimestamp(start, timezone.utc).isoformat().replace(
            "+00:00", "Z"
        )
        end_text = datetime.fromtimestamp(end, timezone.utc).isoformat().replace(
            "+00:00", "Z"
        )
        path = (
            f"/v1/quotes/{symbol}/history?interval=1m&adjustment=raw"
            f"&start={start_text}&end={end_text}"
            f"&page_size={page_size}"
        )
        return path if cursor is None else path + "&cursor=" + cursor

    def test_multi_page_closed_boundaries_exact_and_short_last_page(self):
        self.seed(range(100, 111))
        timestamps, cursor = [], None
        with self.client() as client:
            while True:
                payload = client.get(self.path(4, cursor)).json()
                timestamps.extend(row["timestamp"] for row in payload["bars"])
                self.assertIn("cache_status", payload)
                cursor = payload["next_cursor"]
                if cursor is None:
                    self.assertFalse(payload["truncated"])
                    self.assertEqual(payload["count"], 3)
                    break
        self.assertEqual(timestamps, list(range(100, 111)))
        self.assertEqual(len(timestamps), len(set(timestamps)))

    def test_exact_page_empty_and_utc_offset_query_binding(self):
        self.seed(range(100, 108))
        with self.client() as client:
            first = client.get(self.path(4)).json()
            second = client.get(self.path(4, first["next_cursor"])).json()
            self.assertIsNone(second["next_cursor"])
            self.assertEqual(second["count"], 4)
            empty = client.get(self.path(4, start=1000, end=1100)).json()
            self.assertEqual(empty["bars"], [])
            self.assertIsNone(empty["next_cursor"])
            offset_path = (
                "/v1/quotes/AAPL/history?interval=1m&adjustment=raw"
                "&start=1970-01-01T08:01:40%2B08:00"
                "&end=1970-01-01T08:01:47%2B08:00&page_size=4"
            )
            offset = client.get(offset_path).json()
            self.assertEqual(offset["bars"], first["bars"])

    def test_tamper_wrong_query_expired_and_unknown_version_do_not_query(self):
        self.seed(range(100, 105))
        with self.client() as client:
            token = client.get(self.path(2)).json()["next_cursor"]
            calls = self.repository.page_calls
            tampered = token[:-1] + ("A" if token[-1] != "A" else "B")
            self.assertEqual(client.get(self.path(2, tampered)).status_code, 400)
            self.assertEqual(client.get(self.path(2, token, symbol="MSFT")).status_code, 400)
            self.assertEqual(client.get(self.path(3, token)).status_code, 400)
            self.assertEqual(self.repository.page_calls, calls)

        expired_now = datetime(2026, 7, 20, 14, 0, tzinfo=timezone.utc)
        with TestClient(create_app(
            self.settings, Service(self.repository), lambda: expired_now
        )) as client:
            self.assertEqual(client.get(self.path(2, token)).status_code, 400)
        self.assertEqual(self.repository.page_calls, calls)

        payload_part = token.split(".")[0]
        payload = json.loads(base64.urlsafe_b64decode(
            payload_part + "=" * (-len(payload_part) % 4)
        ))
        payload["v"] = 999
        encoded = json.dumps(
            payload, sort_keys=True, separators=(",", ":")
        ).encode()
        signature = hmac.new(
            self.settings.market_bar_cursor_secret.encode(), encoded, hashlib.sha256
        ).digest()
        unknown = (
            base64.urlsafe_b64encode(encoded).rstrip(b"=").decode() + "." +
            base64.urlsafe_b64encode(signature).rstrip(b"=").decode()
        )
        with self.client() as client:
            self.assertEqual(client.get(self.path(2, unknown)).status_code, 400)
        self.assertEqual(self.repository.page_calls, calls)

    def test_large_sample_keyset_pages_are_bounded_without_offset(self):
        self.seed(range(10_000, 20_001))
        seen, after = 0, None
        while True:
            rows, more = self.warehouse.get_price_bars_page(
                "AAPL", "1m", "raw", "1970-01-01T00:00:00Z",
                "1970-01-02T00:00:00Z", 777, after,
            )
            self.assertLessEqual(len(rows), 777)
            if rows:
                if after is not None:
                    self.assertGreater(rows[0]["timestamp"], after)
                after = rows[-1]["timestamp"]
            seen += len(rows)
            if not more:
                break
        self.assertEqual(seen, 10_001)
        self.assertTrue(all(
            "OFFSET" not in str(value)
            for value in Warehouse.get_price_bars_page.__code__.co_consts
        ))


if __name__ == "__main__":
    unittest.main()
