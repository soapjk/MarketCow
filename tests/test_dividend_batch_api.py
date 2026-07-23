from __future__ import annotations

import threading
import time
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace

from fastapi.testclient import TestClient

from marketcow.api import create_app
from marketcow.config import Settings


def dividend_data(
    symbol: str, count: int = 1, cache_status: str = "fresh"
) -> dict:
    return {
        "symbol": symbol,
        "fiscal_year": 2026,
        "announcements": ([{"dividend_id": symbol}] if count else []),
        "announced_count": count,
        "amount_per_share_total": 1 if count else 0,
        "data_status": cache_status,
        "last_refreshed_at": "2026-07-23T00:00:00+00:00",
    }


class BatchDividendService:
    market_bar_repository = SimpleNamespace()

    def __init__(self, outcomes=None, delay: float = 0.0):
        self.outcomes = outcomes or {}
        self.delay = delay
        self.calls = []
        self.active = 0
        self.max_active = 0
        self.lock = threading.Lock()

    def close(self):
        pass

    def get_dividends(self, symbol, fiscal_year):
        with self.lock:
            self.calls.append((symbol, fiscal_year))
            self.active += 1
            self.max_active = max(self.max_active, self.active)
        try:
            if self.delay:
                time.sleep(self.delay)
            outcome = self.outcomes.get(symbol, dividend_data(symbol))
            if isinstance(outcome, Exception):
                raise outcome
            return outcome
        finally:
            with self.lock:
                self.active -= 1


class DividendBatchApiTest(unittest.TestCase):
    def setUp(self):
        self.folder = TemporaryDirectory()
        root = Path(self.folder.name)
        self.settings = Settings(
            raw_path=root / "raw",
            storage_root=root / "test",
            allowed_root=root,
            postgres_dsn="postgresql://u:p@127.0.0.1/marketcow_test",
            clickhouse_password="secret",
            profile="test",
            port=8793,
            postgres_schema="marketcow_test",
            clickhouse_database="marketcow_test",
            clickhouse_spool_path=root / "test/spool/clickhouse",
            dividend_batch_workers=4,
            dividend_batch_timeout_seconds=1,
        )

    def tearDown(self):
        self.folder.cleanup()

    def test_multiple_symbols_preserve_request_order_and_mapping(self):
        service = BatchDividendService()
        client = TestClient(create_app(self.settings, service))

        response = client.post("/v1/dividends/query", json={
            "symbols": ["600519.ss", "0700.HK", "AAPL"],
            "fiscal_year": 2026,
        })

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["requested_count"], 3)
        self.assertEqual(
            [item["symbol"] for item in payload["items"]],
            ["600519.SH", "00700.HK", "AAPL"],
        )
        self.assertEqual(
            [item["status"] for item in payload["items"]],
            ["available", "available", "available"],
        )

    def test_partial_failure_unavailable_and_refreshing_are_isolated(self):
        service = BatchDividendService({
            "AAPL": RuntimeError("upstream unavailable"),
            "MSFT": dividend_data("MSFT", count=0),
            "00700.HK": dividend_data("00700.HK", cache_status="refreshing"),
        })
        client = TestClient(create_app(self.settings, service))

        response = client.post("/v1/dividends/query", json={
            "symbols": ["AAPL", "MSFT", "0700.HK"],
            "fiscal_year": 2026,
        })

        self.assertEqual(response.status_code, 200)
        items = response.json()["items"]
        self.assertEqual([item["status"] for item in items], [
            "error", "unavailable", "refreshing",
        ])
        self.assertIn("upstream unavailable", items[0]["error"])
        self.assertIsNone(items[0]["data"])
        self.assertEqual(items[1]["data"]["announced_count"], 0)

    def test_cold_cache_requests_run_concurrently(self):
        service = BatchDividendService(delay=0.1)
        client = TestClient(create_app(self.settings, service))
        started = time.monotonic()

        response = client.post("/v1/dividends/query", json={
            "symbols": ["AAPL", "MSFT", "NVDA", "MU"],
            "fiscal_year": 2026,
        })
        elapsed = time.monotonic() - started

        self.assertEqual(response.status_code, 200)
        self.assertGreaterEqual(service.max_active, 2)
        self.assertLess(elapsed, 0.3)

    def test_empty_and_over_limit_requests_are_rejected(self):
        client = TestClient(create_app(self.settings, BatchDividendService()))
        empty = client.post("/v1/dividends/query", json={
            "symbols": [], "fiscal_year": 2026,
        })
        excessive = client.post("/v1/dividends/query", json={
            "symbols": [f"SYM{i}" for i in range(51)], "fiscal_year": 2026,
        })

        self.assertEqual(empty.status_code, 422)
        self.assertEqual(excessive.status_code, 422)

    def test_duplicate_symbols_share_one_service_call_and_keep_two_results(self):
        service = BatchDividendService()
        client = TestClient(create_app(self.settings, service))

        response = client.post("/v1/dividends/query", json={
            "symbols": ["0700.HK", "00700.HK"], "fiscal_year": 2026,
        })

        self.assertEqual(response.status_code, 200)
        self.assertEqual(service.calls, [("00700.HK", 2026)])
        self.assertEqual(len(response.json()["items"]), 2)

    def test_batch_data_matches_single_symbol_business_result(self):
        service = BatchDividendService({
            "AAPL": dividend_data("AAPL", cache_status="stale"),
        })
        client = TestClient(create_app(self.settings, service))

        single = client.get("/v1/dividends/AAPL?fiscal_year=2026").json()
        batch = client.post("/v1/dividends/query", json={
            "symbols": ["AAPL"], "fiscal_year": 2026,
        }).json()

        self.assertEqual(batch["items"][0]["data"], single)
        self.assertEqual(batch["items"][0]["status"], "stale")

    def test_batch_timeout_is_reported_per_symbol(self):
        settings = Settings(**{
            **self.settings.__dict__,
            "dividend_batch_timeout_seconds": 0.02,
        })
        client = TestClient(create_app(
            settings, BatchDividendService(delay=0.1)
        ))

        response = client.post("/v1/dividends/query", json={
            "symbols": ["AAPL", "MSFT"], "fiscal_year": 2026,
        })

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            [item["status"] for item in response.json()["items"]],
            ["error", "error"],
        )
        self.assertIn("batch timeout", response.json()["items"][0]["error"])


if __name__ == "__main__":
    unittest.main()
