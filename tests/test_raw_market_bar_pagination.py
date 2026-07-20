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
        "raw_close": value + 0.25, "adjustment_factor": 1.0,
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
        self.raw_page_calls = 0

    def get_raw_price_bars_page(self, *args):
        self.raw_page_calls += 1
        return self.warehouse.get_raw_price_bars_page(*args)

    def __getattr__(self, name):
        return getattr(self.warehouse, name)


class RawMarketBarPaginationTest(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        self.root = Path(self.folder.name)
        self.warehouse = Warehouse(self.root / "warehouse.duckdb")
        self.repository = CountingRepository(self.warehouse)
        self.settings = Settings(
            self.root / "warehouse.duckdb", self.root / "raw",
            market_bar_cursor_secret="raw-pagination-test-secret-1234567890abcdef",
            market_bar_cursor_ttl_seconds=3600,
            storage_root=self.root / "data-development",
        )

    def tearDown(self):
        self.folder.cleanup()

    def client(self, now=NOW):
        return TestClient(create_app(
            self.settings, Service(self.repository), lambda: now
        ))

    def seed(self, timestamps, sources=("alpha", "beta")):
        for source in sources:
            self.warehouse.upsert_price_bars(
                "AAPL", "1m", "raw", source, "2026-07-20T11:59:00.123Z",
                [bar(value) for value in timestamps],
                {"observed_at": "2026-07-20T11:58:00.456Z",
                 "raw_artifact_id": f"artifact-{source}"},
            )

    @staticmethod
    def path(page_size=None, cursor=None, sources=None, symbol="AAPL"):
        path = (
            f"/v1/quotes/{symbol}/raw-history?interval=1m&adjustment=raw"
            "&start=1970-01-01T08:01:40%2B08:00"
            "&end=1970-01-01T00:01:42Z"
        )
        if page_size is not None:
            path += f"&page_size={page_size}"
        if sources is not None:
            path += f"&sources={sources}"
        if cursor is not None:
            path += f"&cursor={cursor}"
        return path

    def test_same_time_multisource_pages_have_no_gaps_or_duplicates(self):
        self.seed((100, 101, 102))
        seen = []
        cursor = None
        with self.client() as client:
            while True:
                response = client.get(self.path(1, cursor))
                self.assertEqual(response.status_code, 200, response.text)
                payload = response.json()
                seen.extend((row["timestamp"], row["source"]) for row in payload["bars"])
                for row in payload["bars"]:
                    self.assertIn("observed_at", row)
                    self.assertIn("ingested_at", row)
                    self.assertIn("raw_artifact_id", row)
                    self.assertIn("source_sequence", row)
                cursor = payload["next_cursor"]
                if cursor is None:
                    self.assertFalse(payload["truncated"])
                    break
        expected = [(timestamp, source) for timestamp in (100, 101, 102)
                    for source in ("alpha", "beta")]
        self.assertEqual(seen, expected)
        self.assertEqual(len(seen), len(set(seen)))

    def test_sources_normalize_bind_cursor_and_old_api_remains_compatible(self):
        self.seed((100, 101, 102))
        with self.client() as client:
            first = client.get(self.path(1, sources="beta,beta")).json()
            self.assertEqual(first["bars"][0]["source"], "beta")
            second = client.get(self.path(
                1, first["next_cursor"], sources="beta"
            ))
            self.assertEqual(second.status_code, 200)
            calls = self.repository.raw_page_calls
            wrong_filter = client.get(self.path(
                1, first["next_cursor"], sources="alpha"
            ))
            self.assertEqual(wrong_filter.status_code, 400)
            self.assertEqual(self.repository.raw_page_calls, calls)
            old = client.get(self.path(sources="alpha"))
            self.assertEqual(old.status_code, 200)
            self.assertNotIn("next_cursor", old.json())
            self.assertEqual(old.json()["count"], 3)

    def test_tampered_wrong_query_expired_and_bad_position_do_not_query(self):
        self.seed((100, 101))
        with self.client() as client:
            token = client.get(self.path(1)).json()["next_cursor"]
            calls = self.repository.raw_page_calls
            tampered = token[:-1] + ("A" if token[-1] != "A" else "B")
            self.assertEqual(client.get(self.path(1, tampered)).status_code, 400)
            self.assertEqual(client.get(self.path(2, token)).status_code, 400)
            self.assertEqual(client.get(self.path(1, token, symbol="MSFT")).status_code, 400)
            self.assertEqual(client.get(self.path(1, "A" * 2049)).status_code, 400)
            self.assertEqual(self.repository.raw_page_calls, calls)
        with self.client(datetime(2026, 7, 20, 14, 0, tzinfo=timezone.utc)) as client:
            self.assertEqual(client.get(self.path(1, token)).status_code, 400)
        self.assertEqual(self.repository.raw_page_calls, calls)

    def test_latest_raw_version_wins_and_empty_page_is_terminal(self):
        self.warehouse.upsert_price_bars(
            "AAPL", "1m", "raw", "alpha", "2026-07-20T11:59:02Z",
            [bar(100, 20.0)], {"observed_at": "2026-07-20T11:59:01Z"},
        )
        self.warehouse.upsert_price_bars(
            "AAPL", "1m", "raw", "alpha", "2026-07-20T11:59:01Z",
            [bar(100, 10.0)], {"observed_at": "2026-07-20T11:59:00Z"},
        )
        with self.client() as client:
            payload = client.get(self.path(10)).json()
            self.assertEqual(payload["count"], 1)
            self.assertEqual(payload["bars"][0]["close"], 20.0)
            self.assertIsNone(payload["next_cursor"])
            empty = client.get(
                "/v1/quotes/MSFT/raw-history?interval=1m&adjustment=raw"
                "&start=1970-01-01T00:01:40Z&end=1970-01-01T00:01:42Z&page_size=2"
            ).json()
            self.assertEqual(empty["bars"], [])
            self.assertFalse(empty["truncated"])
            self.assertIsNone(empty["next_cursor"])

    def test_large_sample_keyset_pages_are_bounded_without_offset(self):
        self.seed(range(10_000, 20_001), sources=("alpha",))
        seen = []
        after = None
        while True:
            rows, more = self.warehouse.get_raw_price_bars_page(
                "AAPL", "1m", "raw", "1970-01-01T00:00:00Z",
                "1970-01-02T00:00:00Z", 777, None, after,
            )
            self.assertLessEqual(len(rows), 777)
            seen.extend((row["timestamp"], row["source"]) for row in rows)
            if rows:
                after = (rows[-1]["timestamp"], rows[-1]["source"])
            if not more:
                break
        self.assertEqual(len(seen), 10_001)
        self.assertEqual(len(seen), len(set(seen)))
        self.assertTrue(all(
            "OFFSET" not in str(value)
            for value in Warehouse.get_raw_price_bars_page.__code__.co_consts
        ))
        clickhouse_page = ClickHouseMarketBarRepository.get_raw_price_bars_page
        self.assertTrue(all(
            "OFFSET" not in str(value)
            for value in clickhouse_page.__code__.co_consts
        ))


if __name__ == "__main__":
    unittest.main()
