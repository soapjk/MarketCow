import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from fastapi.testclient import TestClient

from marketcow.api import create_app
from marketcow.clickhouse_repositories import ClickHouseMarketBarRepository
from marketcow.config import Settings
from marketcow.storage import Warehouse


NOW = datetime(2026, 7, 20, 12, 0, tzinfo=timezone.utc)
BAR_AT = "1970-01-01T00:03:20Z"


def bar(close=2.5):
    return {
        "timestamp": 200, "bar_at": BAR_AT, "open": 2.0, "high": 3.0,
        "low": 1.5, "close": close, "raw_close": 5.0,
        "adjustment_factor": 0.5, "volume": 20.0, "amount": 50.0,
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

    def get_price_bars_cross_section_page(self, *args):
        self.page_calls += 1
        return self.warehouse.get_price_bars_cross_section_page(*args)

    def __getattr__(self, name):
        return getattr(self.warehouse, name)


class CrossSectionPaginationTest(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        self.root = Path(self.folder.name)
        self.warehouse = Warehouse(self.root / "warehouse.duckdb")
        self.repository = CountingRepository(self.warehouse)
        self.settings = Settings(
            self.root / "warehouse.duckdb", self.root / "raw",
            market_bar_cursor_secret="cross-section-secret-1234567890abcdef",
            market_bar_cursor_ttl_seconds=3600,
            storage_root=self.root / "data-development",
        )
        for symbol in ("AAPL", "GOOG", "MSFT", "NVDA", "TSLA"):
            self.warehouse.upsert_price_bars(
                symbol, "1m", "raw", "fixture", "2026-07-20T11:59:00Z",
                [bar()], {"observed_at": "2026-07-20T11:58:00Z",
                          "raw_artifact_id": f"artifact-{symbol}"},
            )
        self.warehouse.upsert_price_bars(
            "STALE", "1m", "raw", "fixture", "2026-07-20T11:59:00Z",
            [{**bar(), "timestamp": 199, "bar_at": "1970-01-01T00:03:19Z"}],
        )

    def tearDown(self):
        self.folder.cleanup()

    def client(self, now=NOW):
        return TestClient(create_app(
            self.settings, Service(self.repository), lambda: now
        ))

    @staticmethod
    def path(page_size=None, cursor=None, symbols=None, adjustment="raw"):
        path = (
            "/v1/quotes/cross-section?interval=1m"
            f"&adjustment={adjustment}&bar_at=1970-01-01T08:03:20%2B08:00"
        )
        if page_size is not None:
            path += f"&page_size={page_size}"
        if symbols is not None:
            path += f"&symbols={symbols}"
        if cursor is not None:
            path += f"&cursor={cursor}"
        return path

    def test_exact_time_pages_are_stable_complete_and_cache_compatible(self):
        symbols = []
        cursor = None
        with self.client() as client:
            while True:
                response = client.get(self.path(2, cursor))
                self.assertEqual(response.status_code, 200, response.text)
                payload = response.json()
                symbols.extend(row["symbol"] for row in payload["bars"])
                self.assertEqual(payload["bar_at"], "1970-01-01T00:03:20+00:00")
                self.assertIn("cache_status", payload)
                self.assertIn("served_at", payload)
                cursor = payload["next_cursor"]
                if cursor is None:
                    self.assertFalse(payload["truncated"])
                    self.assertEqual(payload["count"], 1)
                    break
        self.assertEqual(symbols, ["AAPL", "GOOG", "MSFT", "NVDA", "TSLA"])
        self.assertNotIn("STALE", symbols)
        self.assertEqual(len(symbols), len(set(symbols)))

    def test_symbols_dedupe_binding_empty_exact_page_and_old_api(self):
        with self.client() as client:
            first = client.get(self.path(2, symbols="MSFT,AAPL,AAPL,GOOG")).json()
            self.assertEqual([row["symbol"] for row in first["bars"]],
                             ["AAPL", "GOOG"])
            second = client.get(self.path(
                2, first["next_cursor"], symbols="AAPL,GOOG,MSFT"
            )).json()
            self.assertEqual([row["symbol"] for row in second["bars"]], ["MSFT"])
            self.assertIsNone(second["next_cursor"])
            exact = client.get(self.path(5)).json()
            self.assertEqual(exact["count"], 5)
            self.assertIsNone(exact["next_cursor"])
            empty = client.get(self.path(2, adjustment="adjusted")).json()
            self.assertEqual(empty["bars"], [])
            self.assertIsNone(empty["next_cursor"])
            old = client.get(self.path(symbols="AAPL,GOOG"))
            self.assertEqual(old.status_code, 200)
            self.assertNotIn("next_cursor", old.json())

    def test_invalid_cursors_are_rejected_before_repository_call(self):
        with self.client() as client:
            token = client.get(self.path(2)).json()["next_cursor"]
            calls = self.repository.page_calls
            tampered = token[:-1] + ("A" if token[-1] != "A" else "B")
            self.assertEqual(client.get(self.path(2, tampered)).status_code, 400)
            self.assertEqual(client.get(self.path(3, token)).status_code, 400)
            self.assertEqual(client.get(self.path(
                2, token, symbols="AAPL,GOOG"
            )).status_code, 400)
            self.assertEqual(client.get(self.path(
                2, token, adjustment="adjusted"
            )).status_code, 400)
            self.assertEqual(client.get(self.path(2, "A" * 2049)).status_code, 400)
            self.assertEqual(self.repository.page_calls, calls)
        expired = datetime(2026, 7, 20, 14, 0, tzinfo=timezone.utc)
        with self.client(expired) as client:
            self.assertEqual(client.get(self.path(2, token)).status_code, 400)
        self.assertEqual(self.repository.page_calls, calls)

    def test_duckdb_page_uses_canonical_source_priority(self):
        self.warehouse.upsert_price_bars(
            "PRIORITY", "1m", "raw", "tushare", "2026-07-20T10:00:00Z",
            [bar(88.0)], {"observed_at": "2026-07-20T09:00:00Z"},
        )
        self.warehouse.upsert_price_bars(
            "PRIORITY", "1m", "raw", "yahoo_chart", "2026-07-20T11:00:00Z",
            [bar(99.0)], {"observed_at": "2026-07-20T10:30:00Z"},
        )
        rows, more = self.warehouse.get_price_bars_cross_section_page(
            "1m", "raw", BAR_AT, 10, ["PRIORITY"]
        )
        self.assertFalse(more)
        self.assertEqual(rows[0]["source"], "tushare")
        self.assertEqual(rows[0]["close"], 88.0)

    def test_large_sample_keyset_traversal_is_bounded_without_offset(self):
        with self.warehouse.connect() as con:
            con.executemany(
                "INSERT INTO market_price_bar "
                "(symbol, interval, adjustment, timestamp, bar_at, open, high, low, "
                "close, volume, source, ingested_at, observed_at) "
                "VALUES (?, '5m', 'raw', 200, ?, 1, 2, 0.5, 1.5, 10, "
                "'fixture', '2026-07-20T11:59:00Z', '2026-07-20T11:58:00Z')",
                [(f"LARGE-{index:05d}", BAR_AT) for index in range(10_001)],
            )
        seen = []
        after = None
        while True:
            rows, more = self.warehouse.get_price_bars_cross_section_page(
                "5m", "raw", BAR_AT, 777, None, after
            )
            self.assertLessEqual(len(rows), 777)
            seen.extend(row["symbol"] for row in rows)
            if rows:
                after = rows[-1]["symbol"]
            if not more:
                break
        self.assertEqual(len(seen), 10_001)
        self.assertEqual(len(seen), len(set(seen)))
        clickhouse_page = (
            ClickHouseMarketBarRepository.get_canonical_price_bars_cross_section_page
        )
        for method in (Warehouse.get_price_bars_cross_section_page, clickhouse_page):
            self.assertTrue(all(
                "OFFSET" not in str(value) for value in method.__code__.co_consts
            ))


if __name__ == "__main__":
    unittest.main()
