import tempfile
import threading
import time
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from fastapi.testclient import TestClient

from marketcow.api import create_app
from marketcow.config import Settings
from marketcow.service import FundamentalService
from marketcow.storage import Warehouse


class QuoteProvider:
    name = "quote_fixture"

    def __init__(self, delay=0.0):
        self.calls = 0
        self.delay = delay
        self.fail = False
        self.price = 100.0
        self._lock = threading.Lock()
        self.active = 0
        self.max_active = 0

    def fetch_quote(self, symbol):
        with self._lock:
            self.calls += 1
            self.active += 1
            self.max_active = max(self.max_active, self.active)
        try:
            if self.delay:
                time.sleep(self.delay)
            if self.fail:
                raise RuntimeError("upstream unavailable")
        finally:
            with self._lock:
                self.active -= 1
        return {
            "instrument_id": "TEST." + symbol,
            "symbol": symbol,
            "name": "Test " + symbol,
            "market": "HK" if symbol.endswith(".HK") else "US",
            "exchange": "TEST",
            "currency": "HKD",
            "price": self.price,
            "previous_close": 99.0,
            "change": self.price - 99.0,
            "change_pct": 1.0,
            "session": "regular",
            "quote_at": "2026-07-20T10:00:00+00:00",
            "source": self.name,
            "source_url": "https://example.test/quote",
            "raw_response_locator": "fixture",
            "_raw_payload": {"symbol": symbol, "price": self.price},
        }


class QuoteCacheContractTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        self.provider = QuoteProvider()
        self.settings = Settings(
            root / "test.duckdb",
            root / "raw",
            quote_cache_ttl_seconds=60,
            quote_stale_max_seconds=3600,
            quote_refresh_workers=8,
        )
        self.service = FundamentalService(
            self.settings,
            warehouse=Warehouse(self.settings.database_path),
            quote_provider=self.provider,
        )
        self.client = TestClient(create_app(self.settings, self.service))

    def tearDown(self):
        self.tmp.cleanup()

    def expire_cache(self, symbol):
        row = self.service.warehouse.get_latest_quotes([symbol])[0]
        row["ingested_at"] = (
            datetime.now(timezone.utc) - timedelta(seconds=120)
        ).isoformat(timespec="seconds")
        self.service.warehouse.upsert_quote(row)

    def test_single_cache_miss_auto_refreshes_and_persists(self):
        response = self.client.get("/v1/quotes/2400.HK")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["symbol"], "2400.HK")
        self.assertEqual(response.json()["cache_status"], "refreshed")
        self.assertEqual(len(self.service.warehouse.get_latest_quotes(["2400.HK"])), 1)

        cached = self.client.get("/v1/quotes/2400.HK")
        self.assertEqual(cached.json()["cache_status"], "fresh")
        self.assertEqual(self.provider.calls, 1)

    def test_expired_cache_refreshes_and_failed_refresh_is_marked_stale(self):
        self.client.get("/v1/quotes/2400.HK")
        self.expire_cache("2400.HK")
        self.provider.price = 101.0
        refreshed = self.client.get("/v1/quotes/2400.HK")
        self.assertEqual(refreshed.json()["price"], 101.0)
        self.assertEqual(refreshed.json()["cache_status"], "refreshed")

        self.expire_cache("2400.HK")
        self.provider.fail = True
        fallback = self.client.get("/v1/quotes/2400.HK")
        self.assertEqual(fallback.status_code, 200)
        self.assertTrue(fallback.json()["cached"])
        self.assertTrue(fallback.json()["stale"])
        self.assertEqual(fallback.json()["cache_status"], "stale_fallback")

    def test_no_cache_and_upstream_failure_is_structured_unavailable(self):
        self.provider.fail = True
        response = self.client.get("/v1/quotes/2400.HK")
        self.assertEqual(response.status_code, 503)
        self.assertEqual(response.json()["detail"]["status"], "unavailable")
        self.assertNotEqual(response.status_code, 404)

    def test_batch_refreshes_misses_concurrently(self):
        self.provider.delay = 0.15
        started = time.monotonic()
        response = self.client.get(
            "/v1/quotes?symbols=2400.HK,0700.HK,9988.HK,AAPL"
        )
        elapsed = time.monotonic() - started
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["count"], 4)
        self.assertEqual(response.json()["errors"], [])
        self.assertGreaterEqual(self.provider.max_active, 2)
        self.assertLess(elapsed, 0.75)

    def test_refresh_parameter_remains_force_refresh_compatible(self):
        self.client.get("/v1/quotes/2400.HK")
        response = self.client.get("/v1/quotes/2400.HK?refresh=true")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(self.provider.calls, 2)


if __name__ == "__main__":
    unittest.main()
