import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from fastapi.testclient import TestClient

from marketcow.api import create_app
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
        self.refresh_calls = 0

    def close(self):
        pass


class CountingRepository:
    def __init__(self, warehouse):
        self.warehouse = warehouse
        self.single_calls = 0
        self.page_calls = 0

    def get_price_bar_as_of(self, *args):
        self.single_calls += 1
        return self.warehouse.get_price_bar_as_of(*args)

    def get_price_bars_as_of_page(self, *args):
        self.page_calls += 1
        return self.warehouse.get_price_bars_as_of_page(*args)

    def __getattr__(self, name):
        return getattr(self.warehouse, name)


class MarketBarAsOfTest(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        self.root = Path(self.folder.name)
        self.warehouse = Warehouse(self.root / "warehouse.duckdb")
        self.repository = CountingRepository(self.warehouse)
        self.settings = Settings(
            self.root / "warehouse.duckdb", self.root / "raw",
            market_bar_cursor_secret="as-of-pagination-secret-1234567890abcdef",
            market_bar_cursor_ttl_seconds=3600,
            storage_root=self.root / "data-development",
        )
        self.warehouse.upsert_price_bars(
            "AAPL", "1m", "raw", "fixture", "2026-07-20T11:59:00Z",
            [bar(100), bar(200), bar(300)],
            {"observed_at": "2026-07-20T11:58:00Z"},
        )
        self.warehouse.upsert_price_bars(
            "GOOG", "1m", "raw", "fixture", "2026-07-20T11:59:00Z",
            [bar(250)],
        )
        self.warehouse.upsert_price_bars(
            "MSFT", "1m", "raw", "fixture", "2026-07-20T11:59:00Z",
            [bar(100)],
        )
        self.warehouse.upsert_price_bars(
            "NVDA", "1m", "raw", "fixture", "2026-07-20T11:59:00Z",
            [bar(240)],
        )

    def tearDown(self):
        self.folder.cleanup()

    def client(self, now=NOW):
        return TestClient(create_app(
            self.settings, Service(self.repository), lambda: now
        ))

    @staticmethod
    def cross_path(page_size=2, cursor=None, lookback=100, symbols=None, as_of=None):
        selected = symbols or "NVDA,MSFT,AAPL,AAPL,GOOG"
        point = as_of or "1970-01-01T08:04:10%2B08:00"
        path = (
            "/v1/quotes/cross-section/as-of?interval=1m&adjustment=raw"
            f"&as_of={point}&max_lookback_seconds={lookback}"
            f"&symbols={selected}&page_size={page_size}"
        )
        return path if cursor is None else path + "&cursor=" + cursor

    def test_single_symbol_excludes_future_and_exposes_staleness(self):
        with self.client() as client:
            payload = client.get(
                "/v1/quotes/AAPL/as-of?interval=1m&adjustment=raw"
                "&as_of=1970-01-01T08:04:10%2B08:00&max_lookback_seconds=100"
            ).json()
        self.assertEqual(payload["bar"]["timestamp"], 200)
        self.assertEqual(payload["bar"]["effective_bar_at"],
                         "1970-01-01T00:03:20+00:00")
        self.assertEqual(payload["bar"]["staleness_seconds"], 50)
        self.assertEqual(payload["bar"]["effective_status"], "prior_within_lookback")
        self.assertEqual(payload["max_staleness_seconds"], 50)
        self.assertIn("cache_status", payload)

    def test_lookback_boundary_gap_closure_and_no_result_are_deterministic(self):
        at_boundary = self.warehouse.get_price_bar_as_of(
            "AAPL", "1m", "raw", "1970-01-01T00:04:10Z", 50
        )
        self.assertEqual(at_boundary["timestamp"], 200)
        self.assertIsNone(self.warehouse.get_price_bar_as_of(
            "AAPL", "1m", "raw", "1970-01-01T00:04:10Z", 49
        ))
        exact = self.warehouse.get_price_bar_as_of(
            "GOOG", "1m", "raw", "1970-01-01T00:04:10Z", 100
        )
        self.assertEqual(exact["effective_status"], "exact")
        self.assertEqual(exact["staleness_seconds"], 0)
        # Market closures, suspensions, and gaps share the same explicit rule:
        # return the prior bar only while it remains inside max_lookback.
        gap = self.warehouse.get_price_bar_as_of(
            "MSFT", "1m", "raw", "1970-01-01T00:04:10Z", 150
        )
        self.assertEqual(gap["staleness_seconds"], 150)

    def test_bounded_cross_section_pages_are_sorted_sparse_and_complete(self):
        seen = []
        cursor = None
        with self.client() as client:
            while True:
                response = client.get(self.cross_path(cursor=cursor))
                self.assertEqual(response.status_code, 200, response.text)
                payload = response.json()
                seen.extend((row["symbol"], row["timestamp"]) for row in payload["bars"])
                self.assertLessEqual(payload["max_staleness_seconds"], 100)
                cursor = payload["next_cursor"]
                if cursor is None:
                    self.assertFalse(payload["truncated"])
                    break
        self.assertEqual(seen, [("AAPL", 200), ("GOOG", 250), ("NVDA", 240)])
        self.assertEqual(len(seen), len(set(seen)))

    def test_invalid_time_lookback_symbols_and_cursor_do_not_query(self):
        with self.client() as client:
            token = client.get(self.cross_path()).json()["next_cursor"]
            calls = self.repository.page_calls
            tampered = token[:-1] + ("A" if token[-1] != "A" else "B")
            self.assertEqual(client.get(self.cross_path(cursor=tampered)).status_code, 400)
            self.assertEqual(client.get(self.cross_path(
                cursor=token, lookback=99
            )).status_code, 400)
            self.assertEqual(client.get(self.cross_path(
                cursor=token, symbols="AAPL,GOOG"
            )).status_code, 400)
            self.assertEqual(client.get(self.cross_path(
                cursor=token, as_of="1970-01-01T00:04:11Z"
            )).status_code, 400)
            self.assertEqual(client.get(self.cross_path(cursor="A" * 2049)).status_code, 400)
            self.assertEqual(client.get(self.cross_path(
                as_of="1970-01-01T00:04:10"
            )).status_code, 400)
            too_many = ",".join(f"S{value}" for value in range(1001))
            self.assertEqual(client.get(self.cross_path(symbols=too_many)).status_code, 400)
            self.assertEqual(self.repository.page_calls, calls)
            self.assertEqual(client.get(
                "/v1/quotes/AAPL/as-of?as_of=1970-01-01T00:04:10Z"
                "&max_lookback_seconds=31536001"
            ).status_code, 422)
            self.assertEqual(self.repository.single_calls, 0)
        expired = datetime(2026, 7, 20, 14, 0, tzinfo=timezone.utc)
        with self.client(expired) as client:
            self.assertEqual(client.get(self.cross_path(cursor=token)).status_code, 400)
        self.assertEqual(self.repository.page_calls, calls)

    def test_canonical_source_priority_and_1000_symbol_bound(self):
        self.warehouse.upsert_price_bars(
            "PRIORITY", "1m", "raw", "tushare", "2026-07-20T10:00:00Z",
            [bar(200, 88.0)], {"observed_at": "2026-07-20T09:00:00Z"},
        )
        self.warehouse.upsert_price_bars(
            "PRIORITY", "1m", "raw", "yahoo_chart", "2026-07-20T11:00:00Z",
            [bar(200, 99.0)], {"observed_at": "2026-07-20T10:30:00Z"},
        )
        row = self.warehouse.get_price_bar_as_of(
            "PRIORITY", "1m", "raw", "1970-01-01T00:04:10Z", 100
        )
        self.assertEqual(row["source"], "tushare")
        self.assertEqual(row["close"], 88.0)

        symbols = [f"BOUND-{index:04d}" for index in range(1000)]
        with self.warehouse.connect() as con:
            con.executemany(
                "INSERT INTO market_price_bar "
                "(symbol, interval, adjustment, timestamp, bar_at, open, high, low, "
                "close, volume, source, ingested_at, observed_at) "
                "VALUES (?, '5m', 'raw', 200, '1970-01-01T00:03:20Z', "
                "1, 2, 0.5, 1.5, 10, 'fixture', "
                "'2026-07-20T11:59:00Z', '2026-07-20T11:58:00Z')",
                [(symbol,) for symbol in symbols],
            )
        rows, more = self.warehouse.get_price_bars_as_of_page(
            "5m", "raw", "1970-01-01T00:04:10Z", 100,
            symbols, 1000,
        )
        self.assertEqual(len(rows), 1000)
        self.assertFalse(more)


if __name__ == "__main__":
    unittest.main()
