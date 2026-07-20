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


def bar(timestamp, close=None):
    value = float(timestamp)
    return {
        "timestamp": timestamp,
        "bar_at": datetime.fromtimestamp(timestamp, timezone.utc).isoformat(),
        "open": value, "high": value + 1, "low": value - 1,
        "close": value + 0.5 if close is None else close,
        "raw_close": value, "adjustment_factor": 1.0,
        "volume": 10.0, "amount": value * 10,
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

    def get_price_bars_matrix_page(self, *args):
        self.page_calls += 1
        return self.warehouse.get_price_bars_matrix_page(*args)

    def __getattr__(self, name):
        return getattr(self.warehouse, name)


class CrossSectionMatrixTest(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        self.root = Path(self.folder.name)
        self.warehouse = Warehouse(self.root / "warehouse.duckdb")
        self.repository = CountingRepository(self.warehouse)
        self.settings = Settings(
            self.root / "warehouse.duckdb", self.root / "raw",
            market_bar_cursor_secret="matrix-pagination-secret-1234567890abcdef",
            market_bar_cursor_ttl_seconds=3600,
            storage_root=self.root / "data-development",
        )
        for symbol in ("AAPL", "GOOG"):
            self.warehouse.upsert_price_bars(
                symbol, "1m", "raw", "fixture", "2026-07-20T11:59:00Z",
                [bar(200), bar(300)],
                {"observed_at": "2026-07-20T11:58:00Z",
                 "raw_artifact_id": f"artifact-{symbol}"},
            )
        self.warehouse.upsert_price_bars(
            "MSFT", "1m", "raw", "fixture", "2026-07-20T11:59:00Z",
            [bar(200)], {"observed_at": "2026-07-20T11:58:00Z"},
        )
        self.warehouse.upsert_price_bars(
            "STALE", "1m", "raw", "fixture", "2026-07-20T11:59:00Z",
            [bar(199)],
        )

    def tearDown(self):
        self.folder.cleanup()

    def client(self, now=NOW):
        return TestClient(create_app(
            self.settings, Service(self.repository), lambda: now
        ))

    @staticmethod
    def path(page_size=2, cursor=None, bar_ats=None, symbols=None):
        points = bar_ats or (
            "1970-01-01T08:03:20%2B08:00,1970-01-01T00:05:00Z"
        )
        selected = symbols or "MSFT,AAPL,AAPL,GOOG"
        path = (
            "/v1/quotes/cross-section/matrix?interval=1m&adjustment=raw"
            f"&bar_ats={points}&symbols={selected}&page_size={page_size}"
        )
        return path if cursor is None else path + "&cursor=" + cursor

    def test_sparse_exact_matrix_pages_in_stable_order(self):
        seen = []
        cursor = None
        with self.client() as client:
            while True:
                response = client.get(self.path(cursor=cursor))
                self.assertEqual(response.status_code, 200, response.text)
                payload = response.json()
                seen.extend((row["timestamp"], row["symbol"]) for row in payload["bars"])
                self.assertEqual(payload["matrix_cells"], 6)
                self.assertIn("cache_status", payload)
                cursor = payload["next_cursor"]
                if cursor is None:
                    self.assertFalse(payload["truncated"])
                    break
        self.assertEqual(seen, [
            (200, "AAPL"), (200, "GOOG"), (200, "MSFT"),
            (300, "AAPL"), (300, "GOOG"),
        ])
        self.assertNotIn((300, "MSFT"), seen)
        self.assertEqual(len(seen), len(set(seen)))

    def test_time_and_symbol_dedupe_utc_binding_empty_and_terminal(self):
        duplicate_points = (
            "1970-01-01T00:03:20Z,1970-01-01T08:03:20%2B08:00,"
            "1970-01-01T00:05:00Z"
        )
        with self.client() as client:
            payload = client.get(self.path(
                page_size=5, bar_ats=duplicate_points
            )).json()
            self.assertEqual(payload["count"], 5)
            self.assertEqual(len(payload["bar_ats"]), 2)
            self.assertEqual(payload["symbols"], ["AAPL", "GOOG", "MSFT"])
            self.assertIsNone(payload["next_cursor"])
            empty = client.get(self.path(
                bar_ats="1970-01-02T00:00:00Z"
            )).json()
            self.assertEqual(empty["bars"], [])
            self.assertIsNone(empty["next_cursor"])

    def test_cursor_and_bounds_rejected_before_repository_query(self):
        with self.client() as client:
            token = client.get(self.path()).json()["next_cursor"]
            calls = self.repository.page_calls
            tampered = token[:-1] + ("A" if token[-1] != "A" else "B")
            self.assertEqual(client.get(self.path(cursor=tampered)).status_code, 400)
            self.assertEqual(client.get(self.path(page_size=3, cursor=token)).status_code, 400)
            self.assertEqual(client.get(self.path(
                cursor=token, symbols="AAPL,GOOG"
            )).status_code, 400)
            self.assertEqual(client.get(self.path(
                cursor=token, bar_ats="1970-01-01T00:03:20Z"
            )).status_code, 400)
            self.assertEqual(client.get(self.path(cursor="A" * 2049)).status_code, 400)
            self.assertEqual(self.repository.page_calls, calls)
            too_many_points = ",".join(
                datetime.fromtimestamp(value, timezone.utc).isoformat()
                for value in range(101)
            )
            self.assertEqual(client.get(self.path(
                bar_ats=too_many_points
            )).status_code, 400)
            too_many_symbols = ",".join(f"S{value}" for value in range(1001))
            self.assertEqual(client.get(self.path(symbols=too_many_symbols)).status_code, 400)
            self.assertEqual(self.repository.page_calls, calls)
        expired = datetime(2026, 7, 20, 14, 0, tzinfo=timezone.utc)
        with self.client(expired) as client:
            self.assertEqual(client.get(self.path(cursor=token)).status_code, 400)
        self.assertEqual(self.repository.page_calls, calls)

    def test_duckdb_matrix_uses_canonical_source_priority(self):
        self.warehouse.upsert_price_bars(
            "PRIORITY", "1m", "raw", "tushare", "2026-07-20T10:00:00Z",
            [bar(200, 88.0)], {"observed_at": "2026-07-20T09:00:00Z"},
        )
        self.warehouse.upsert_price_bars(
            "PRIORITY", "1m", "raw", "yahoo_chart", "2026-07-20T11:00:00Z",
            [bar(200, 99.0)], {"observed_at": "2026-07-20T10:30:00Z"},
        )
        rows, more = self.warehouse.get_price_bars_matrix_page(
            "1m", "raw", ["1970-01-01T00:03:20Z"], ["PRIORITY"], 10
        )
        self.assertFalse(more)
        self.assertEqual(rows[0]["source"], "tushare")
        self.assertEqual(rows[0]["close"], 88.0)

    def test_10001_row_matrix_traversal_is_bounded_without_offset(self):
        points = list(range(10_000, 10_100))
        symbols = [f"MATRIX-{index:03d}" for index in range(101)]
        with self.warehouse.connect() as con:
            con.executemany(
                "INSERT INTO market_price_bar "
                "(symbol, interval, adjustment, timestamp, bar_at, open, high, low, "
                "close, volume, source, ingested_at, observed_at) "
                "VALUES (?, '5m', 'raw', ?, ?, 1, 2, 0.5, 1.5, 10, "
                "'fixture', '2026-07-20T11:59:00Z', '2026-07-20T11:58:00Z')",
                [(symbol, point, datetime.fromtimestamp(point, timezone.utc).isoformat())
                 for point in points for symbol in symbols],
            )
        bar_ats = [datetime.fromtimestamp(value, timezone.utc).isoformat()
                   for value in points]
        seen = []
        after = None
        while True:
            rows, more = self.warehouse.get_price_bars_matrix_page(
                "5m", "raw", bar_ats, symbols, 777, after
            )
            self.assertLessEqual(len(rows), 777)
            seen.extend((row["timestamp"], row["symbol"]) for row in rows)
            if rows:
                after = (rows[-1]["timestamp"], rows[-1]["symbol"])
            if not more:
                break
        self.assertEqual(len(seen), 10_100)
        self.assertEqual(len(seen), len(set(seen)))
        clickhouse_page = ClickHouseMarketBarRepository.get_canonical_price_bars_matrix_page
        for method in (Warehouse.get_price_bars_matrix_page, clickhouse_page):
            self.assertTrue(all(
                "OFFSET" not in str(value) for value in method.__code__.co_consts
            ))


if __name__ == "__main__":
    unittest.main()
