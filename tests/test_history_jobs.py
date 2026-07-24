from __future__ import annotations

import copy
import threading
import time
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace

from fastapi.testclient import TestClient

from marketcow.api import create_app
from marketcow.config import Settings
from marketcow.history_jobs import HistoryJobManager


class MemoryJobs:
    def __init__(self):
        self.jobs = {}
        self.items = {}
        self.lock = threading.Lock()

    def upsert_history_job(self, row):
        with self.lock:
            self.jobs[row["job_id"]] = copy.deepcopy(dict(row))
            return copy.deepcopy(self.jobs[row["job_id"]])

    def upsert_history_item(self, row):
        with self.lock:
            self.items[(row["job_id"], row["item_id"])] = copy.deepcopy(dict(row))
            return copy.deepcopy(row)

    def get_history_job(self, job_id):
        with self.lock:
            return copy.deepcopy(self.jobs.get(job_id))

    def list_history_jobs(self, limit=50):
        with self.lock:
            return [copy.deepcopy(row) for row in list(self.jobs.values())[-limit:]]

    def list_history_items(self, job_id):
        with self.lock:
            return [
                copy.deepcopy(row) for (owner, _), row in self.items.items()
                if owner == job_id
            ]

    def get_instrument(self, _instrument_id):
        return None

    def latest_runs(self, _limit):
        return []


class JobService:
    def __init__(self, repository=None):
        self.metadata_repository = repository or MemoryJobs()
        self.market_bar_repository = SimpleNamespace()
        self.fail = set()
        self.delay = 0
        self.active = 0
        self.max_active = 0
        self.lock = threading.Lock()
        self.calls = []

    def refresh_quote_history(
        self, symbol, range_, interval, adjustment, provider, allow_fallback
    ):
        with self.lock:
            self.active += 1
            self.max_active = max(self.max_active, self.active)
            self.calls.append((symbol, provider, range_, interval, adjustment))
        try:
            if self.delay:
                time.sleep(self.delay)
            if symbol in self.fail:
                raise RuntimeError("token=super-secret upstream failed")
            return {
                "source": provider, "bars": [{"close": 1}, {"close": 2}], "count": 2,
            }
        finally:
            with self.lock:
                self.active -= 1

    def close(self):
        pass


def request(**overrides):
    value = {
        "symbols": ["AAPL", "MSFT"], "provider": "yahoo", "range": "1mo",
        "interval": "1d", "adjustment": "raw", "allow_fallback": False,
        "max_concurrency": 2, "max_attempts": 1,
        "retry_backoff_seconds": 0, "canonical_wait_seconds": 0,
        "idempotency_key": "history-test-0001",
    }
    value.update(overrides)
    return value


def terminal(manager, job_id, timeout=3):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = manager.detail(job_id)
        if result["status"] in {"succeeded", "partially_failed", "failed", "canceled"}:
            return result
        time.sleep(0.01)
    raise AssertionError("job did not finish")


class HistoryJobManagerTest(unittest.TestCase):
    def test_partial_failure_progress_idempotency_and_failed_retry(self):
        service = JobService()
        service.fail.add("MSFT")
        manager = HistoryJobManager(service, service.metadata_repository, max_workers=2)
        first, created = manager.create(request())
        duplicate, duplicate_created = manager.create(request())
        result = terminal(manager, first["job_id"])

        self.assertTrue(created)
        self.assertFalse(duplicate_created)
        self.assertEqual(duplicate["job_id"], first["job_id"])
        self.assertEqual(result["status"], "partially_failed")
        self.assertEqual(result["completed_symbols"], result["total_symbols"])
        self.assertEqual(result["progress_percent"], 100)
        self.assertEqual(result["canonical_completed"], 1)
        failed = next(item for item in result["items"] if item["status"] == "failed")
        self.assertNotIn("super-secret", failed["error_message"])

        service.fail.clear()
        manager.retry_failed(first["job_id"])
        retried = terminal(manager, first["job_id"])
        self.assertEqual(retried["status"], "succeeded")
        self.assertTrue(all(item["attempt"] >= 1 for item in retried["items"]))
        manager.close()

    def test_global_concurrency_is_bounded(self):
        service = JobService()
        service.delay = 0.05
        manager = HistoryJobManager(service, service.metadata_repository, max_workers=2)
        job, _ = manager.create(request(
            symbols=["AAPL", "MSFT", "GOOG", "META"], max_concurrency=4
        ))
        terminal(manager, job["job_id"])
        self.assertLessEqual(service.max_active, 2)
        manager.close()

    def test_tushare_uses_the_same_durable_lifecycle(self):
        service = JobService()
        manager = HistoryJobManager(service, service.metadata_repository, max_workers=1)
        job, _ = manager.create(request(
            symbols=["600519.SH"], provider="tushare", range="5d",
            interval="1m", idempotency_key="tushare-history-0001",
        ))
        result = terminal(manager, job["job_id"])
        self.assertEqual(result["status"], "succeeded")
        self.assertEqual(result["items"][0]["provider"], "tushare")
        self.assertEqual(service.calls[0][1], "tushare")
        manager.close()

    def test_cancel_and_restart_recovery_never_report_false_success(self):
        repository = MemoryJobs()
        service = JobService(repository)
        service.delay = 0.1
        manager = HistoryJobManager(service, repository, max_workers=1)
        job, _ = manager.create(request(symbols=["AAPL", "MSFT"], max_concurrency=1))
        time.sleep(0.02)
        manager.cancel(job["job_id"])
        result = terminal(manager, job["job_id"])
        self.assertEqual(result["status"], "canceled")
        self.assertEqual(result["progress_percent"], 100)
        manager.close()

        now = "2026-07-24T00:00:00+00:00"
        repository.upsert_history_job({
            "job_id": "interrupted", "idempotency_key": "interrupted-key",
            "status": "running", "request_json": request(),
            "created_at": now, "started_at": now, "updated_at": now,
            "finished_at": None, "error_json": None,
        })
        repository.upsert_history_item({
            "job_id": "interrupted", "item_id": "one", "symbol": "AAPL",
            "status": "running", "provider": "yahoo", "source": None, "attempt": 1,
            "rows_fetched": 0, "rows_persisted": 0, "canonical_status": "pending",
            "error_code": None, "error_message": None, "started_at": now,
            "updated_at": now, "finished_at": None,
        })
        recovered = HistoryJobManager(service, repository, max_workers=1)
        detail = recovered.detail("interrupted")
        self.assertEqual(detail["status"], "failed")
        self.assertEqual(detail["items"][0]["error_code"], "service_interrupted")
        recovered.close()


class HistoryJobApiTest(unittest.TestCase):
    def setUp(self):
        self.folder = TemporaryDirectory()
        root = Path(self.folder.name)
        self.settings = Settings(
            raw_path=root / "raw", storage_root=root, allowed_root=root,
            postgres_dsn="postgresql://u:p@localhost/test", clickhouse_password="x",
            profile="test", postgres_schema="marketcow_test",
            clickhouse_database="marketcow_test", clickhouse_spool_path=root / "spool",
        )
        self.service = JobService()
        self.client = TestClient(create_app(self.settings, self.service))

    def tearDown(self):
        self.client.close()
        self.folder.cleanup()

    def test_explicit_contract_202_json_and_browser_page(self):
        invalid = self.client.post("/v1/admin/history-jobs", json={
            "symbols": ["AAPL"], "provider": "yahoo",
        })
        created = self.client.post("/v1/admin/history-jobs", json=request())
        page = self.client.get("/v1/admin/history-jobs-ui")

        self.assertEqual(invalid.status_code, 422)
        self.assertEqual(created.status_code, 202)
        self.assertTrue(created.json()["job_id"])
        self.assertIn("History fetch jobs", page.text)
        self.assertIn("setInterval(load,2000)", page.text)
        self.assertNotIn("super-secret", page.text)


if __name__ == "__main__":
    unittest.main()
