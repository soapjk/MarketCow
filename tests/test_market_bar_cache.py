import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

from fastapi.testclient import TestClient

from marketcow.api import create_app
from marketcow.clickhouse_shadow import ShadowMarketBarRepository
from marketcow.config import Settings


NOW = datetime(2026, 7, 20, 0, 10, tzinfo=timezone.utc)
ROW = {
    "symbol": "AAPL", "interval": "1m", "adjustment": "raw",
    "timestamp": 100, "bar_at": "1970-01-01T00:01:40+00:00",
    "open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5,
    "volume": 10.0, "source": "fixture",
    "ingested_at": "2026-07-20T08:05:00+08:00",
    "source_payload": {},
}


class Repository:
    def __init__(self, rows=None):
        self.rows = list(ROW for _ in range(1)) if rows is None else rows

    def get_price_bars(self, *args):
        return list(self.rows)

    def get_price_bars_range(self, *args):
        return list(self.rows), False

    def get_price_bars_page(self, *args):
        return list(self.rows), True

    def get_price_bars_cross_section(self, *args):
        return list(self.rows), False

    def get_raw_price_bars_range(self, *args):
        return list(self.rows), False


class ClickHouseRepository(Repository):
    def __init__(self, rows=None, error=None):
        super().__init__(rows)
        self.error = error

    def _result(self, ranged=False):
        if self.error:
            raise self.error
        return (list(self.rows), False) if ranged else list(self.rows)

    def get_canonical_price_bars(self, *args):
        return self._result()

    def get_canonical_price_bars_range(self, *args):
        return self._result(True)

    def get_canonical_price_bars_page(self, *args):
        if self.error:
            raise self.error
        return list(self.rows), True

    def get_canonical_price_bars_cross_section(self, *args):
        return self._result(True)

    def get_raw_price_bars_range(self, *args):
        return self._result(True)


class Service:
    def __init__(self, repository, refresh_result=None, refresh_error=None):
        self.market_bar_repository = repository
        self.refresh_result = refresh_result
        self.refresh_error = refresh_error
        self.refresh_calls = 0

    def refresh_quote_history(self, *args):
        self.refresh_calls += 1
        if self.refresh_error:
            raise self.refresh_error
        return dict(self.refresh_result or {"bars": []})

    def close(self):
        pass


class MarketBarCacheContractTest(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        root = Path(self.folder.name)
        self.settings = Settings(
            root / "warehouse.duckdb", root / "raw",
            market_bar_cache_freshness_seconds=300,
            market_bar_cursor_secret="market-cache-test-secret-1234567890abcdef",
            storage_root=root / "data-development",
        )

    def tearDown(self):
        self.folder.cleanup()

    def client(self, service):
        return TestClient(create_app(self.settings, service, lambda: NOW))

    def test_fixed_clock_fresh_stale_empty_and_read_only_paths(self):
        service = Service(Repository())
        with self.client(service) as client:
            paths = [
                "/v1/quotes/AAPL/history?interval=1m&adjustment=raw&refresh=false",
                "/v1/quotes/AAPL/history?interval=1m&adjustment=raw"
                "&start=1970-01-01T00:00:00Z&end=1970-01-01T00:02:00Z",
                "/v1/quotes/cross-section?interval=1m&adjustment=raw"
                "&bar_at=1970-01-01T00:01:40Z",
                "/v1/quotes/AAPL/raw-history?interval=1m&adjustment=raw"
                "&start=1970-01-01T00:00:00Z&end=1970-01-01T00:02:00Z",
            ]
            for path in paths:
                payload = client.get(path).json()
                self.assertEqual(payload["cache_status"], "fresh")
                self.assertEqual(payload["newest_ingested_at"],
                                 "2026-07-20T00:05:00+00:00")
                self.assertEqual(payload["cache_age_seconds"], 300.0)
                self.assertEqual(payload["served_at"], "2026-07-20T00:10:00+00:00")
            self.assertEqual(service.refresh_calls, 0)

        stale = {**ROW, "ingested_at": "2026-07-19T23:00:00Z"}
        with self.client(Service(Repository([stale]))) as client:
            self.assertEqual(client.get(
                "/v1/quotes/AAPL/history?refresh=false"
            ).json()["cache_status"], "stale")
        with self.client(Service(Repository([]))) as client:
            payload = client.get("/v1/quotes/AAPL/history?refresh=false").json()
            self.assertEqual(payload["cache_status"], "empty")
            self.assertIsNone(payload["newest_ingested_at"])
            self.assertIsNone(payload["cache_age_seconds"])

    def test_refresh_success_failure_degradation_and_empty_error(self):
        refreshed = Service(Repository(), refresh_result={
            "bars": [{**ROW, "ingested_at": None}],
            "ingested_at": "2026-07-20T00:09:30Z",
        })
        with self.client(refreshed) as client:
            payload = client.get("/v1/quotes/AAPL/history").json()
            self.assertFalse(payload["cached"])
            self.assertEqual(payload["cache_status"], "fresh")
            self.assertEqual(payload["cache_age_seconds"], 30.0)

        degraded = Service(Repository(), refresh_error=RuntimeError("upstream timed out"))
        with self.client(degraded) as client:
            response = client.get("/v1/quotes/AAPL/history")
            self.assertEqual(response.status_code, 200)
            self.assertTrue(response.json()["cache_degraded"])
            self.assertEqual(response.json()["cache_reason"], "upstream timed out")

        empty = Service(Repository([]), refresh_error=RuntimeError("upstream timed out"))
        with self.client(empty) as client:
            response = client.get("/v1/quotes/AAPL/history")
            self.assertEqual(response.status_code, 502)
            self.assertEqual(response.json()["detail"], "upstream timed out")

    def test_clickhouse_success_and_duckdb_fallback_have_equal_cache_semantics(self):
        primary = Repository()

        def adapter(clickhouse, canonical=True, raw=True):
            writer = SimpleNamespace(repository=clickhouse, spool=SimpleNamespace(
                diagnostics=lambda: {}
            ))
            return ShadowMarketBarRepository(
                primary, writer, canonical_reads_enabled=canonical,
                raw_reads_enabled=raw,
            )

        successful = Service(adapter(ClickHouseRepository()))
        fallback = Service(adapter(ClickHouseRepository(
            error=ConnectionError("clickhouse unavailable")
        )))
        paths = [
            "/v1/quotes/AAPL/history?refresh=false",
            "/v1/quotes/AAPL/history?start=1970-01-01T00:00:00Z"
            "&end=1970-01-01T00:02:00Z&page_size=10",
            "/v1/quotes/AAPL/raw-history?start=1970-01-01T00:00:00Z"
            "&end=1970-01-01T00:02:00Z",
        ]
        with self.client(successful) as first, self.client(fallback) as second:
            for path in paths:
                left, right = first.get(path).json(), second.get(path).json()
                for field in ("bars", "cache_status", "newest_ingested_at",
                              "cache_age_seconds", "served_at", "next_cursor"):
                    if field not in left and field not in right:
                        continue
                    self.assertEqual(left[field], right[field])

    def test_freshness_threshold_is_bounded(self):
        with self.assertRaisesRegex(ValueError, "between 1 and 86400"):
            Settings(
                self.settings.database_path, self.settings.raw_path,
                market_bar_cache_freshness_seconds=0,
            ).validate_runtime_isolation()


if __name__ == "__main__":
    unittest.main()
