from __future__ import annotations

import copy
import threading
import time
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace

from fastapi.testclient import TestClient
import requests

from marketcow.api import create_app
from marketcow.config import Settings
from marketcow.history_jobs import HistoryJobManager, _error_policy, _retry_delay
from marketcow.history_shards import _exchange_calendar
from marketcow.provider_routing import ProviderNotSupported
from marketcow.telemetry import Telemetry


class MemoryJobs:
    def __init__(self):
        self.jobs = {}
        self.items = {}
        self.shards = {}
        self.canonical_checks = {}
        self.lock = threading.Lock()
        self.closed = False
        self.access_after_close = 0

    def _check_open(self):
        if self.closed:
            self.access_after_close += 1
            raise RuntimeError("repository is closed")

    def upsert_history_job(self, row):
        with self.lock:
            self._check_open()
            self.jobs[row["job_id"]] = copy.deepcopy(dict(row))
            return copy.deepcopy(self.jobs[row["job_id"]])

    def get_or_create_history_job(self, row, items, shards=None):
        with self.lock:
            self._check_open()
            for existing in self.jobs.values():
                if existing["idempotency_key"] == row["idempotency_key"]:
                    return copy.deepcopy(existing), False
            self.jobs[row["job_id"]] = copy.deepcopy(dict(row))
            for item in items:
                self.items[(item["job_id"], item["item_id"])] = copy.deepcopy(item)
            for shard in shards or []:
                key = (shard["job_id"], shard["item_id"], shard["shard_key"])
                self.shards[key] = copy.deepcopy(shard)
            return copy.deepcopy(self.jobs[row["job_id"]]), True

    def upsert_history_item(self, row):
        with self.lock:
            self._check_open()
            self.items[(row["job_id"], row["item_id"])] = copy.deepcopy(dict(row))
            return copy.deepcopy(row)

    def get_history_job(self, job_id):
        with self.lock:
            self._check_open()
            return copy.deepcopy(self.jobs.get(job_id))

    def list_history_jobs(self, limit=50):
        with self.lock:
            self._check_open()
            return [copy.deepcopy(row) for row in list(self.jobs.values())[-limit:]]

    def list_recoverable_history_jobs(self):
        with self.lock:
            self._check_open()
            return [
                copy.deepcopy(row) for row in self.jobs.values()
                if row["status"] in {"queued", "running", "cancel_requested"}
            ]

    def list_history_items(self, job_id):
        with self.lock:
            self._check_open()
            return [
                copy.deepcopy(row) for (owner, _), row in self.items.items()
                if owner == job_id
            ]

    def upsert_history_shard(self, row):
        with self.lock:
            self._check_open()
            key = (row["job_id"], row["item_id"], row["shard_key"])
            self.shards[key] = copy.deepcopy(dict(row))
            return copy.deepcopy(row)

    def list_history_shards(self, job_id, item_id=""):
        with self.lock:
            self._check_open()
            rows = [
                copy.deepcopy(row)
                for (owner, item, _), row in self.shards.items()
                if owner == job_id and (not item_id or item == item_id)
            ]
            return sorted(rows, key=lambda row: (
                row["item_id"], row["shard_index"]
            ))

    def list_all_history_shards(self, limit=10000):
        with self.lock:
            self._check_open()
            return [
                copy.deepcopy(row) for row in list(self.shards.values())[:limit]
            ]

    def reconcile_history_shard(self, row, now):
        with self.lock:
            self._check_open()
            key = (row["job_id"], row["item_id"], row["shard_key"])
            current = self.shards.get(key)
            if current is None or current["status"] in {
                "succeeded", "canceled", "superseded"
            }:
                return None
            if (
                current["status"] == "running"
                and current.get("lease_expires_at")
                and current["lease_expires_at"] > now
            ):
                return None
            self.shards[key] = copy.deepcopy(dict(row))
            return copy.deepcopy(row)

    def reconcile_history_item(self, row, now):
        with self.lock:
            self._check_open()
            key = (row["job_id"], row["item_id"])
            current = self.items.get(key)
            if current is None:
                return None
            if (
                current["status"] == "running"
                and current.get("lease_expires_at")
                and current["lease_expires_at"] > now
            ):
                return None
            self.items[key] = copy.deepcopy(dict(row))
            return copy.deepcopy(row)

    def upsert_history_canonical_check(self, row):
        with self.lock:
            self._check_open()
            self.canonical_checks[row["check_id"]] = copy.deepcopy(dict(row))
            return copy.deepcopy(row)

    def list_pending_history_canonical_checks(self, limit=100):
        with self.lock:
            self._check_open()
            return [
                copy.deepcopy(row)
                for row in self.canonical_checks.values()
                if row["status"] in {"pending", "retry"}
            ][:limit]

    def list_history_canonical_checks(self, job_id, item_id=""):
        with self.lock:
            self._check_open()
            return [
                copy.deepcopy(row)
                for row in self.canonical_checks.values()
                if row["job_id"] == job_id
                and (not item_id or row["item_id"] == item_id)
            ]
    def claim_history_item(
        self, job_id, item_id, owner_id, lease_token, now, lease_expires_at
    ):
        with self.lock:
            self._check_open()
            item = self.items.get((job_id, item_id))
            if item is None:
                return None
            expired = (
                not item.get("lease_expires_at")
                or item["lease_expires_at"] <= now
            )
            if item["status"] != "queued" and not (
                item["status"] == "running" and expired
            ):
                return None
            takeover = 1 if item["status"] == "running" else 0
            item.update({
                "status": "running", "owner_id": owner_id,
                "lease_token": lease_token, "lease_expires_at": lease_expires_at,
                "heartbeat_at": now,
                "takeover_count": int(item.get("takeover_count") or 0) + takeover,
                "started_at": item.get("started_at") or now,
                "updated_at": now, "finished_at": None,
            })
            return copy.deepcopy(item)

    def renew_history_item_lease(
        self, job_id, item_id, owner_id, lease_token, now, lease_expires_at
    ):
        with self.lock:
            self._check_open()
            item = self.items.get((job_id, item_id))
            if (
                item is None or item["status"] != "running"
                or item.get("owner_id") != owner_id
                or item.get("lease_token") != lease_token
                or item.get("lease_expires_at", "") <= now
            ):
                return None
            item.update({
                "heartbeat_at": now, "lease_expires_at": lease_expires_at,
                "updated_at": now,
            })
            return copy.deepcopy(item)

    def finish_claimed_history_item(self, row, owner_id, lease_token):
        with self.lock:
            self._check_open()
            item = self.items.get((row["job_id"], row["item_id"]))
            if (
                item is None or item.get("owner_id") != owner_id
                or item.get("lease_token") != lease_token
            ):
                return None
            item.update(copy.deepcopy(row))
            item.update({
                "owner_id": None, "lease_token": None,
                "lease_expires_at": None, "heartbeat_at": None,
            })
            return copy.deepcopy(item)

    def release_history_item_lease(
        self, job_id, item_id, owner_id, lease_token, now
    ):
        with self.lock:
            self._check_open()
            item = self.items.get((job_id, item_id))
            if (
                item is None or item.get("owner_id") != owner_id
                or item.get("lease_token") != lease_token
            ):
                return None
            item.update({
                "owner_id": None, "lease_token": None,
                "lease_expires_at": None, "heartbeat_at": None,
                "updated_at": now,
            })
            return copy.deepcopy(item)

    def get_instrument(self, _instrument_id):
        return None

    def latest_runs(self, _limit):
        return []


class JobService:
    def __init__(self, repository=None):
        self.metadata_repository = repository or MemoryJobs()
        self.raw_receipts = {}
        self.market_bar_repository = SimpleNamespace(
            get_raw_ingestion_receipt=lambda ingestion_id: self.raw_receipts.get(
                ingestion_id
            ),
            list_raw_ingestion_receipts=lambda limit=10000: list(
                self.raw_receipts.values()
            )[:limit],
        )
        self.artifact_store = SimpleNamespace(
            list_artifacts=lambda _dataset="", _limit=10000: []
        )
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

    def refresh_quote_history_window(
        self, symbol, start, end, interval, adjustment, provider, allow_fallback,
        ingestion_id=None,
    ):
        return self.refresh_quote_history(
            symbol, f"{start.isoformat()}/{end.isoformat()}", interval,
            adjustment, provider, allow_fallback,
        )

    def close(self):
        pass


def request(**overrides):
    value = {
        "symbols": ["AAPL.XNAS", "MSFT.XNAS"], "provider": "yahoo", "range": "1mo",
        "interval": "1d", "adjustment": "raw", "allow_fallback": False,
        "max_concurrency": 2, "max_attempts": 1,
        "retry_backoff_seconds": 0, "retry_max_backoff_seconds": 60,
        "retry_jitter_seconds": 0, "retry_budget_seconds": 60,
        "canonical_wait_seconds": 0,
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
    def test_concurrent_idempotent_create_is_atomic_beyond_list_window(self):
        service = JobService()
        service.delay = 0.02
        manager = HistoryJobManager(service, service.metadata_repository, max_workers=2)
        barrier = threading.Barrier(3)
        results = []

        def create():
            barrier.wait()
            results.append(manager.create(request()))

        threads = [threading.Thread(target=create) for _ in range(2)]
        for thread in threads:
            thread.start()
        barrier.wait()
        for thread in threads:
            thread.join()

        self.assertEqual(len({value[0]["job_id"] for value in results}), 1)
        self.assertEqual(sorted(value[1] for value in results), [False, True])
        self.assertTrue(all(value[0]["total_symbols"] == 2 for value in results))
        terminal(manager, results[0][0]["job_id"])
        manager.close()

    def test_partial_failure_progress_idempotency_and_failed_retry(self):
        service = JobService()
        service.fail.add("MSFT.XNAS")
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
        self.assertEqual(result["total_shards"], 2)
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
            symbols=["AAPL.XNAS", "MSFT.XNAS", "GOOG.XNAS", "META.XNAS"], max_concurrency=4
        ))
        terminal(manager, job["job_id"])
        self.assertLessEqual(service.max_active, 2)
        manager.close()

    def test_heartbeat_keeps_long_running_item_owned_and_fences_rival(self):
        repository = MemoryJobs()
        service = JobService(repository)
        service.delay = 0.7
        manager = HistoryJobManager(
            service, repository, max_workers=1, lease_seconds=0.25
        )
        job, _ = manager.create(request(symbols=["AAPL.XNAS"]))
        time.sleep(0.4)
        running = manager.detail(job["job_id"])["items"][0]

        rival = repository.claim_history_item(
            job["job_id"], running["item_id"], "rival", "rival-token",
            datetime.now(timezone.utc).isoformat(timespec="microseconds"),
            "2030-01-01T00:00:00+00:00",
        )

        self.assertIsNone(rival)
        self.assertEqual(running["owner_id"], manager.owner_id)
        self.assertIsNotNone(running["heartbeat_at"])
        self.assertEqual(terminal(manager, job["job_id"])["status"], "succeeded")
        manager.close()

    def test_old_lease_token_cannot_finish_after_takeover(self):
        repository = MemoryJobs()
        now = "2026-07-24T00:00:00+00:00"
        item = {
            "job_id": "fenced", "item_id": "one", "symbol": "AAPL.XNAS",
            "status": "queued", "provider": "yahoo", "source": None, "attempt": 0,
            "rows_fetched": 0, "rows_persisted": 0, "canonical_status": "pending",
            "error_code": None, "error_message": None, "started_at": None,
            "updated_at": now, "finished_at": None,
        }
        repository.upsert_history_item(item)
        repository.claim_history_item(
            "fenced", "one", "old", "old-token", now,
            "2026-07-24T00:00:01+00:00",
        )
        repository.claim_history_item(
            "fenced", "one", "new", "new-token",
            "2026-07-24T00:00:02+00:00", "2026-07-24T00:01:00+00:00",
        )
        terminal_item = {
            **item, "status": "succeeded", "attempt": 1,
            "canonical_status": "completed",
            "updated_at": "2026-07-24T00:00:03+00:00",
            "finished_at": "2026-07-24T00:00:03+00:00",
        }

        self.assertIsNone(repository.finish_claimed_history_item(
            terminal_item, "old", "old-token"
        ))
        current = repository.list_history_items("fenced")[0]
        self.assertEqual(current["owner_id"], "new")
        self.assertEqual(current["status"], "running")

    def test_watchdog_takes_over_only_after_foreign_lease_expires(self):
        repository = MemoryJobs()
        service = JobService(repository)
        now = datetime.now(timezone.utc)
        now_text = now.isoformat(timespec="microseconds")
        expires_text = (
            now + timedelta(seconds=0.35)
        ).isoformat(timespec="microseconds")
        repository.upsert_history_job({
            "job_id": "watchdog", "idempotency_key": "watchdog-key",
            "status": "running", "request_json": request(max_attempts=3),
            "created_at": now_text, "started_at": now_text,
            "updated_at": now_text, "finished_at": None, "error_json": None,
        })
        repository.upsert_history_item({
            "job_id": "watchdog", "item_id": "one", "symbol": "AAPL.XNAS",
            "status": "running", "provider": "yahoo", "source": None, "attempt": 1,
            "rows_fetched": 0, "rows_persisted": 0, "canonical_status": "pending",
            "error_code": None, "error_message": None, "started_at": now_text,
            "updated_at": now_text, "finished_at": None, "owner_id": "foreign",
            "lease_token": "foreign-token", "lease_expires_at": expires_text,
            "heartbeat_at": now_text, "takeover_count": 0,
        })

        manager = HistoryJobManager(
            service, repository, max_workers=1, lease_seconds=0.2
        )
        time.sleep(0.15)
        self.assertEqual(service.calls, [])
        detail = terminal(manager, "watchdog")

        self.assertEqual(detail["status"], "succeeded")
        self.assertEqual(detail["items"][0]["attempt"], 2)
        self.assertEqual(detail["items"][0]["takeover_count"], 1)
        self.assertEqual(len(service.calls), 1)
        manager.close()

    def test_two_managers_execute_a_queued_item_only_once(self):
        repository = MemoryJobs()
        service = JobService(repository)
        service.delay = 0.2
        now = datetime.now(timezone.utc).isoformat(timespec="microseconds")
        repository.upsert_history_job({
            "job_id": "two-managers", "idempotency_key": "two-managers-key",
            "status": "queued", "request_json": request(symbols=["AAPL.XNAS"]),
            "created_at": now, "started_at": None, "updated_at": now,
            "finished_at": None, "error_json": None,
        })
        repository.upsert_history_item({
            "job_id": "two-managers", "item_id": "one", "symbol": "AAPL.XNAS",
            "status": "queued", "provider": "yahoo", "source": None, "attempt": 0,
            "rows_fetched": 0, "rows_persisted": 0, "canonical_status": "pending",
            "error_code": None, "error_message": None, "started_at": None,
            "updated_at": now, "finished_at": None,
        })

        first = HistoryJobManager(
            service, repository, max_workers=1, lease_seconds=0.3
        )
        second = HistoryJobManager(
            service, repository, max_workers=1, lease_seconds=0.3
        )
        detail = terminal(first, "two-managers")

        self.assertEqual(detail["status"], "succeeded")
        self.assertEqual(len(service.calls), 1)
        first.close()
        second.close()

    def test_taken_over_item_rejects_late_completion_from_old_manager(self):
        repository = MemoryJobs()
        old_service = JobService(repository)
        new_service = JobService(repository)
        old_started = threading.Event()
        release_old = threading.Event()

        def slow_old(*_args, **_kwargs):
            old_started.set()
            release_old.wait(2)
            return {
                "source": "old-owner", "bars": [{"close": 1}], "count": 1,
            }

        old_service.refresh_quote_history = slow_old
        now = datetime.now(timezone.utc).isoformat(timespec="microseconds")
        repository.upsert_history_job({
            "job_id": "late-owner", "idempotency_key": "late-owner-key",
            "status": "queued",
            "request_json": request(symbols=["AAPL.XNAS"], max_attempts=3),
            "created_at": now, "started_at": None, "updated_at": now,
            "finished_at": None, "error_json": None,
        })
        repository.upsert_history_item({
            "job_id": "late-owner", "item_id": "one", "symbol": "AAPL.XNAS",
            "status": "queued", "provider": "yahoo", "source": None, "attempt": 0,
            "rows_fetched": 0, "rows_persisted": 0, "canonical_status": "pending",
            "error_code": None, "error_message": None, "started_at": None,
            "updated_at": now, "finished_at": None,
        })
        old_manager = HistoryJobManager(
            old_service, repository, max_workers=1, lease_seconds=0.2
        )
        self.assertTrue(old_started.wait(1))
        old_manager._heartbeat_stop.set()
        heartbeat = old_manager._heartbeat
        if heartbeat is not None:
            heartbeat.join(1)

        new_manager = HistoryJobManager(
            new_service, repository, max_workers=1, lease_seconds=0.2
        )
        detail = terminal(new_manager, "late-owner")
        release_old.set()
        time.sleep(0.05)
        final = new_manager.detail("late-owner")

        self.assertEqual(detail["status"], "succeeded")
        self.assertEqual(final["items"][0]["source"], "yahoo")
        self.assertEqual(final["items"][0]["takeover_count"], 1)
        self.assertEqual(len(new_service.calls), 1)
        old_manager.close()
        new_manager.close()

    def test_tushare_uses_the_same_durable_lifecycle(self):
        service = JobService()
        manager = HistoryJobManager(service, service.metadata_repository, max_workers=1)
        job, _ = manager.create(request(
            symbols=["600519.XSHG"], provider="tushare", range="5d",
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
        job, _ = manager.create(request(symbols=["AAPL.XNAS", "MSFT.XNAS"], max_concurrency=1))
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
            "job_id": "interrupted", "item_id": "one", "symbol": "AAPL.XNAS",
            "status": "running", "provider": "yahoo", "source": None, "attempt": 1,
            "rows_fetched": 0, "rows_persisted": 0, "canonical_status": "pending",
            "error_code": None, "error_message": None, "started_at": now,
            "updated_at": now, "finished_at": None,
        })
        recovered = HistoryJobManager(service, repository, max_workers=1)
        detail = terminal(recovered, "interrupted")
        self.assertEqual(detail["status"], "failed")
        self.assertEqual(detail["items"][0]["error_code"], "service_interrupted")
        recovered.close()

    def test_restart_automatically_retries_interrupted_item_with_attempts_left(self):
        repository = MemoryJobs()
        service = JobService(repository)
        now = "2026-07-24T00:00:00+00:00"
        repository.upsert_history_job({
            "job_id": "retry-interrupted",
            "idempotency_key": "retry-interrupted-key",
            "status": "running",
            "request_json": request(max_attempts=3),
            "created_at": now, "started_at": now, "updated_at": now,
            "finished_at": None, "error_json": None,
        })
        repository.upsert_history_item({
            "job_id": "retry-interrupted", "item_id": "one", "symbol": "AAPL.XNAS",
            "status": "running", "provider": "yahoo", "source": None, "attempt": 1,
            "rows_fetched": 0, "rows_persisted": 0, "canonical_status": "pending",
            "error_code": None, "error_message": None, "started_at": now,
            "updated_at": now, "finished_at": None,
        })

        recovered = HistoryJobManager(service, repository, max_workers=1)
        detail = terminal(recovered, "retry-interrupted")

        self.assertEqual(detail["status"], "succeeded")
        self.assertEqual(detail["items"][0]["attempt"], 2)
        self.assertEqual(len(service.calls), 1)
        recovered.close()

    def test_recovery_is_not_limited_by_recent_job_page(self):
        repository = MemoryJobs()
        service = JobService(repository)
        now = "2026-07-24T00:00:00+00:00"
        repository.upsert_history_job({
            "job_id": "old-queued", "idempotency_key": "old-queued-key",
            "status": "queued", "request_json": request(symbols=["AAPL.XNAS"]),
            "created_at": now, "started_at": None, "updated_at": now,
            "finished_at": None, "error_json": None,
        })
        repository.upsert_history_item({
            "job_id": "old-queued", "item_id": "one", "symbol": "AAPL.XNAS",
            "status": "queued", "provider": "yahoo", "source": None, "attempt": 0,
            "rows_fetched": 0, "rows_persisted": 0, "canonical_status": "pending",
            "error_code": None, "error_message": None, "started_at": None,
            "updated_at": now, "finished_at": None,
        })
        for index in range(201):
            repository.upsert_history_job({
                "job_id": f"new-terminal-{index}",
                "idempotency_key": f"new-terminal-key-{index}",
                "status": "succeeded", "request_json": request(),
                "created_at": now, "started_at": now, "updated_at": now,
                "finished_at": now, "error_json": None,
            })

        self.assertNotIn(
            "old-queued",
            {job["job_id"] for job in repository.list_history_jobs(200)},
        )
        recovered = HistoryJobManager(service, repository, max_workers=1)
        detail = terminal(recovered, "old-queued")

        self.assertEqual(detail["status"], "succeeded")
        self.assertEqual(len(service.calls), 1)
        recovered.close()

    def test_legacy_job_without_shards_uses_legacy_whole_range_adapter(self):
        repository = MemoryJobs()
        service = JobService(repository)
        service.refresh_quote_history_window = lambda *_args, **_kwargs: (
            (_ for _ in ()).throw(AssertionError("window adapter must not run"))
        )
        now = "2026-07-24T00:00:00+00:00"
        legacy_request = request(symbols=["AAPL.XNAS"])
        self.assertNotIn("history_job_schema_version", legacy_request)
        repository.upsert_history_job({
            "job_id": "legacy", "idempotency_key": "legacy-history-key",
            "status": "queued", "request_json": legacy_request,
            "created_at": now, "started_at": None, "updated_at": now,
            "finished_at": None, "error_json": None,
        })
        repository.upsert_history_item({
            "job_id": "legacy", "item_id": "one", "symbol": "AAPL.XNAS",
            "status": "queued", "provider": "yahoo", "source": None, "attempt": 0,
            "rows_fetched": 0, "rows_persisted": 0, "canonical_status": "pending",
            "error_code": None, "error_message": None, "started_at": None,
            "updated_at": now, "finished_at": None,
        })

        recovered = HistoryJobManager(service, repository, max_workers=1)
        detail = terminal(recovered, "legacy")

        self.assertEqual(detail["status"], "succeeded")
        self.assertEqual(detail["total_shards"], 0)
        self.assertEqual(len(service.calls), 1)
        recovered.close()

    def test_close_waits_for_workers_and_prevents_dependency_access_after_close(self):
        repository = MemoryJobs()
        service = JobService(repository)
        service.delay = 0.08
        manager = HistoryJobManager(service, repository, max_workers=1)
        manager.create(request(symbols=["AAPL.XNAS"], max_concurrency=1))
        manager.close()
        repository.closed = True
        time.sleep(0.05)
        self.assertEqual(repository.access_after_close, 0)
        with self.assertRaisesRegex(RuntimeError, "closed"):
            manager.create(request(idempotency_key="history-after-close"))

    def test_retry_waits_for_atomic_finalize_active_transition(self):
        repository = MemoryJobs()
        service = JobService(repository)
        service.fail.add("MSFT.XNAS")
        manager = HistoryJobManager(service, repository, max_workers=2)
        job, _ = manager.create(request())
        terminal(manager, job["job_id"])
        service.fail.clear()

        original_finalize = manager._finalize
        entered = threading.Event()
        release = threading.Event()

        def blocking_finalize(job_id):
            original_finalize(job_id)
            entered.set()
            release.wait(1)

        manager._finalize = blocking_finalize
        service.fail.add("MSFT.XNAS")
        manager.retry_failed(job["job_id"])
        self.assertTrue(entered.wait(1))
        service.fail.clear()
        retry_result = []
        retry_thread = threading.Thread(
            target=lambda: retry_result.append(manager.retry_failed(job["job_id"]))
        )
        retry_thread.start()
        time.sleep(0.02)
        self.assertTrue(retry_thread.is_alive())
        release.set()
        retry_thread.join(1)
        self.assertTrue(retry_result)
        self.assertEqual(terminal(manager, job["job_id"])["status"], "succeeded")
        manager.close()

    def test_error_sanitization_matches_telemetry_policy(self):
        service = JobService()
        manager = HistoryJobManager(service, service.metadata_repository, max_workers=1)
        secrets = (
            "postgresql://user:pass@host/db "
            "https://alice:hunter2@example.test/path "
            "api_key=abc token=def authorization=Bearer-secret"
        )

        def fail(*_args, **_kwargs):
            raise RuntimeError(secrets)

        service.refresh_quote_history = fail
        job, _ = manager.create(request(symbols=["AAPL.XNAS"]))
        message = terminal(manager, job["job_id"])["items"][0]["error_message"]
        for secret in ("user", "pass", "alice", "hunter2", "abc", "def", "Bearer-secret"):
            self.assertNotIn(secret, message)
        self.assertIn("[REDACTED_DSN]", message)
        manager.close()

    def test_permanent_error_fails_without_consuming_retry_budget(self):
        service = JobService()
        manager = HistoryJobManager(service, service.metadata_repository, max_workers=1)

        def fail(*_args, **_kwargs):
            raise ValueError("unsupported interval")

        service.refresh_quote_history = fail
        job, _ = manager.create(request(symbols=["AAPL.XNAS"], max_attempts=5))
        detail = terminal(manager, job["job_id"])

        self.assertEqual(detail["items"][0]["attempt"], 1)
        self.assertEqual(detail["items"][0]["error_code"], "invalid_history_request")
        manager.close()

    def test_connection_error_retries_until_success(self):
        service = JobService()
        manager = HistoryJobManager(service, service.metadata_repository, max_workers=1)
        calls = 0

        def flaky(*_args, **_kwargs):
            nonlocal calls
            calls += 1
            if calls < 3:
                raise requests.ConnectionError("connection reset")
            return {
                "source": "yahoo", "bars": [{"close": 1}], "count": 1,
            }

        service.refresh_quote_history = flaky
        job, _ = manager.create(request(
            symbols=["AAPL.XNAS"], max_attempts=3, retry_backoff_seconds=0
        ))
        detail = terminal(manager, job["job_id"])

        self.assertEqual(detail["status"], "succeeded")
        self.assertEqual(detail["items"][0]["attempt"], 1)
        self.assertEqual(detail["items"][0]["shards"][0]["attempt"], 3)
        self.assertEqual(calls, 3)
        manager.close()

    def test_shard_checkpoint_resume_skips_already_confirmed_windows(self):
        service = JobService()
        manager = HistoryJobManager(service, service.metadata_repository, max_workers=1)
        windows = []
        failed_once = False

        def fetch_window(
            symbol, start, end, interval, adjustment, provider, allow_fallback,
            ingestion_id=None,
        ):
            nonlocal failed_once
            key = (start.isoformat(), end.isoformat())
            windows.append(key)
            if len(set(windows)) == 3 and not failed_once:
                failed_once = True
                raise ValueError("fixture permanent shard failure")
            return {
                "source": provider, "bars": [{"close": 1}], "count": 1,
            }

        service.refresh_quote_history_window = fetch_window
        job, _ = manager.create(request(
            symbols=["AAPL.XNAS"], range="3mo", interval="1m", max_attempts=1
        ))
        first = terminal(manager, job["job_id"])
        self.assertEqual(first["status"], "failed")
        succeeded_before_retry = [
            shard for shard in first["items"][0]["shards"]
            if shard["status"] == "succeeded"
        ]
        self.assertEqual(len(succeeded_before_retry), 2)

        manager.retry_failed(job["job_id"])
        final = terminal(manager, job["job_id"])

        self.assertEqual(final["status"], "succeeded")
        self.assertEqual(final["completed_shards"], final["total_shards"])
        self.assertEqual(windows.count(windows[0]), 1)
        self.assertEqual(windows.count(windows[1]), 1)
        self.assertEqual(windows.count(windows[2]), 2)
        manager.close()

    def test_suspected_truncation_is_persistently_split_and_reconciled(self):
        service = JobService()
        manager = HistoryJobManager(
            service, service.metadata_repository, max_workers=1
        )
        calls = []

        def fetch_window(
            symbol, start, end, interval, adjustment, provider, allow_fallback,
            ingestion_id=None,
        ):
            calls.append((start, end, ingestion_id))
            if (end - start) > timedelta(days=2):
                count = 3000
                step = (end - start) / count
                bars = [
                    {"bar_at": (start + step * index).isoformat()}
                    for index in range(count)
                ]
            else:
                calendar = _exchange_calendar("XSHG")
                sessions = calendar.sessions_in_range(
                    start.date().isoformat(),
                    (end - timedelta(microseconds=1)).date().isoformat(),
                )
                bars = [
                    {
                        "bar_at": (
                            calendar.session_open(session).to_pydatetime()
                            + timedelta(minutes=index)
                        ).isoformat()
                    }
                    for session in sessions
                    for index in range(242)
                ]
                count = len(bars)
            return {
                "source": provider,
                "bars": bars,
                "count": count,
            }

        service.refresh_quote_history_window = fetch_window
        job, _ = manager.create(request(
            symbols=["600519.XSHG"], provider="tushare", interval="1m",
            range="custom",
            range_start="2026-07-20T00:00:00+00:00",
            range_end="2026-07-24T00:00:00+00:00",
            max_attempts=3,
        ))
        detail = terminal(manager, job["job_id"])
        shards = detail["items"][0]["shards"]

        self.assertEqual(detail["status"], "succeeded")
        self.assertEqual([value["status"] for value in shards].count(
            "superseded"
        ), 1)
        leaves = [value for value in shards if value["status"] == "succeeded"]
        self.assertEqual(len(leaves), 2)
        self.assertEqual(detail["rows_fetched"], 4 * 242)
        self.assertEqual(detail["rows_persisted"], 4 * 242)
        self.assertEqual(detail["completed_shards"], detail["total_shards"])
        self.assertEqual(len({value["ingestion_id"] for value in leaves}), 2)
        self.assertTrue(all(
            value["cursor_json"].get("parent_shard_key") for value in leaves
        ))
        self.assertEqual(len(calls), 3)
        manager.close()

        repository = service.metadata_repository
        restarted_leaf = leaves[0]
        original_ingestion_id = restarted_leaf["ingestion_id"]
        with repository.lock:
            stored_job = repository.jobs[job["job_id"]]
            stored_job.update({"status": "running", "finished_at": None})
            stored_item = repository.items[(
                job["job_id"], detail["items"][0]["item_id"]
            )]
            stored_item.update({
                "status": "running", "owner_id": "dead-worker",
                "lease_token": "dead-token",
                "lease_expires_at": "2000-01-01T00:00:00+00:00",
                "finished_at": None,
            })
            stored_leaf = repository.shards[(
                job["job_id"], detail["items"][0]["item_id"],
                restarted_leaf["shard_key"],
            )]
            stored_leaf.update({
                "status": "queued", "rows_fetched": 0,
                "rows_persisted": 0, "finished_at": None,
            })
        restarted_service = JobService(repository)
        restarted_service.refresh_quote_history_window = fetch_window
        restarted = HistoryJobManager(
            restarted_service, repository, max_workers=1
        )
        recovered = terminal(restarted, job["job_id"])
        recovered_leaf = next(
            value for value in recovered["items"][0]["shards"]
            if value["shard_key"] == restarted_leaf["shard_key"]
        )

        self.assertEqual(recovered["status"], "succeeded")
        self.assertEqual(recovered_leaf["ingestion_id"], original_ingestion_id)
        self.assertEqual(len(calls), 4)
        self.assertEqual(recovered["superseded_shards"], 1)
        restarted.close()

    def test_unproven_minimum_shard_fails_closed(self):
        service = JobService()
        manager = HistoryJobManager(
            service, service.metadata_repository, max_workers=1
        )

        def saturated(
            _symbol, start, _end, _interval, _adjustment, provider,
            allow_fallback, ingestion_id=None,
        ):
            return {
                "source": provider,
                "bars": [
                    {"bar_at": (start + timedelta(seconds=index)).isoformat()}
                    for index in range(3000)
                ],
                "count": 3000,
            }

        service.refresh_quote_history_window = saturated
        job, _ = manager.create(request(
            symbols=["600519.XSHG"], provider="tushare", interval="1m",
            range="custom",
            range_start="2026-07-20T00:00:00+00:00",
            range_end="2026-07-21T00:00:00+00:00",
        ))
        detail = terminal(manager, job["job_id"])

        self.assertEqual(detail["status"], "failed")
        self.assertEqual(
            detail["items"][0]["error_code"],
            "upstream_coverage_unproven",
        )
        self.assertEqual(
            detail["items"][0]["shards"][0]["error_code"],
            "upstream_coverage_unproven",
        )
        manager.close()

    def test_cancel_finishes_current_shard_and_cancels_all_remaining_shards(self):
        service = JobService()
        manager = HistoryJobManager(service, service.metadata_repository, max_workers=1)
        started = threading.Event()
        release = threading.Event()
        calls = []

        def fetch_window(
            symbol, start, end, interval, adjustment, provider, allow_fallback,
            ingestion_id=None,
        ):
            calls.append((start.isoformat(), end.isoformat()))
            started.set()
            release.wait(1)
            return {
                "source": provider, "bars": [{"close": 1}], "count": 1,
            }

        service.refresh_quote_history_window = fetch_window
        job, _ = manager.create(request(
            symbols=["AAPL.XNAS"], range="3mo", interval="1m"
        ))
        self.assertTrue(started.wait(1))
        manager.cancel(job["job_id"])
        release.set()
        detail = terminal(manager, job["job_id"])
        shard_statuses = [
            shard["status"] for shard in detail["items"][0]["shards"]
        ]

        self.assertEqual(detail["status"], "canceled")
        self.assertEqual(len(calls), 1)
        self.assertEqual(shard_statuses.count("succeeded"), 1)
        self.assertEqual(
            shard_statuses.count("canceled"), len(shard_statuses) - 1
        )
        self.assertNotIn("queued", shard_statuses)
        self.assertEqual(detail["completed_shards"], detail["total_shards"])
        manager.close()

    def test_empty_shard_is_confirmed_without_synthesizing_rows(self):
        service = JobService()
        manager = HistoryJobManager(service, service.metadata_repository, max_workers=1)
        service.refresh_quote_history_window = lambda *_args, **_kwargs: {
            "source": "yahoo", "bars": [], "count": 0,
        }

        job, _ = manager.create(request(symbols=["AAPL.XNAS"], range="1d"))
        detail = terminal(manager, job["job_id"])
        shard = detail["items"][0]["shards"][0]

        self.assertEqual(detail["status"], "succeeded")
        self.assertEqual(detail["rows_fetched"], 0)
        self.assertEqual(detail["rows_persisted"], 0)
        self.assertEqual(shard["status"], "succeeded")
        self.assertEqual(
            shard["cursor_json"]["confirmed_through"], shard["range_end"]
        )
        manager.close()

    def test_error_policy_classifies_http_auth_rate_limit_and_server_errors(self):
        def http_error(status):
            response = requests.Response()
            response.status_code = status
            return requests.HTTPError(f"status {status}", response=response)

        cases = (
            (http_error(429), ("upstream_rate_limited", True)),
            (http_error(503), ("upstream_server_error", True)),
            (http_error(401), ("provider_authentication_failed", False)),
            (http_error(400), ("upstream_request_rejected", False)),
            (
                ProviderNotSupported(
                    "unsupported", provider="fixture",
                    capability="market_bar_history", market="US",
                ),
                ("provider_not_supported", False),
            ),
            (
                RuntimeError("TUSHARE_TOKEN is not configured"),
                ("provider_authentication_failed", False),
            ),
        )
        for error, expected in cases:
            with self.subTest(error=error):
                self.assertEqual(_error_policy(error), expected)

    def test_retry_delay_honors_retry_after_jitter_and_maximum(self):
        response = requests.Response()
        response.status_code = 429
        response.headers["Retry-After"] = "7"
        error = requests.HTTPError("rate limited", response=response)
        retry_request = request(
            retry_backoff_seconds=2, retry_max_backoff_seconds=8,
            retry_jitter_seconds=2,
        )

        self.assertEqual(_retry_delay(retry_request, 1, error, 0.25), 7.5)
        self.assertEqual(_retry_delay(retry_request, 3, error, 1.0), 8)

    def test_retry_budget_stops_before_sleeping_past_deadline(self):
        service = JobService()
        sleeps = []
        manager = HistoryJobManager(
            service, service.metadata_repository, max_workers=1,
            random_provider=lambda: 0, sleeper=sleeps.append,
        )

        def fail(*_args, **_kwargs):
            raise requests.ConnectionError("offline")

        service.refresh_quote_history = fail
        job, _ = manager.create(request(
            symbols=["AAPL.XNAS"], max_attempts=5, retry_backoff_seconds=2,
            retry_max_backoff_seconds=10, retry_budget_seconds=1,
        ))
        detail = terminal(manager, job["job_id"])

        self.assertEqual(detail["items"][0]["attempt"], 1)
        self.assertEqual(
            detail["items"][0]["error_code"], "retry_budget_exhausted"
        )
        self.assertEqual(sleeps, [])
        manager.close()

    def test_history_telemetry_correlates_lease_retry_shard_and_job(self):
        service = JobService()
        service.telemetry = Telemetry()
        sequence = iter((
            requests.ConnectionError("reset"),
            {"source": "yahoo", "bars": [{"close": 1}], "count": 1},
        ))

        def fetch(*_args, **_kwargs):
            outcome = next(sequence)
            if isinstance(outcome, BaseException):
                raise outcome
            return outcome

        service.refresh_quote_history_window = fetch
        manager = HistoryJobManager(
            service, service.metadata_repository, max_workers=1
        )
        job, _ = manager.create(request(
            symbols=["AAPL.XNAS"], max_attempts=2, retry_backoff_seconds=0
        ))
        terminal(manager, job["job_id"])
        manager.detail(job["job_id"])
        snapshot = service.telemetry.snapshot()
        metrics = {item["name"] for item in snapshot["metrics"]}
        rendered_logs = str(snapshot["logs"])

        self.assertIn("history_lease_total", metrics)
        self.assertIn("history_retry_total", metrics)
        self.assertIn("history_shard_latency_seconds", metrics)
        self.assertIn("history_items", metrics)
        self.assertIn(job["job_id"], rendered_logs)
        manager.close()

    def test_history_health_reports_takeover_thrashing(self):
        repository = MemoryJobs()
        service = JobService(repository)
        manager = HistoryJobManager(service, repository, max_workers=1)
        now = datetime.now(timezone.utc).isoformat(timespec="microseconds")
        repository.upsert_history_job({
            "job_id": "health", "idempotency_key": "health-key",
            "status": "running", "request_json": request(symbols=["AAPL.XNAS"]),
            "created_at": now, "started_at": now, "updated_at": now,
            "finished_at": None, "error_json": None,
        })
        repository.upsert_history_item({
            "job_id": "health", "item_id": "one", "symbol": "AAPL.XNAS",
            "status": "running", "provider": "yahoo", "source": None,
            "attempt": 1, "rows_fetched": 0, "rows_persisted": 0,
            "canonical_status": "pending", "error_code": None,
            "error_message": None, "started_at": now, "updated_at": now,
            "finished_at": None, "owner_id": "foreign",
            "lease_token": "token",
            "lease_expires_at": "2999-01-01T00:00:00+00:00",
            "heartbeat_at": now, "takeover_count": 20,
        })

        health = manager.health_snapshot()

        self.assertEqual(health["status"], "degraded")
        self.assertIn("history_takeover_rate_high", health["reasons"])
        self.assertTrue(health["watchdog_alive"])
        manager.close()


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
            "symbols": ["AAPL.XNAS"], "provider": "yahoo",
        })
        created = self.client.post("/v1/admin/history-jobs", json=request())
        page = self.client.get("/v1/admin/history-jobs-ui")

        self.assertEqual(invalid.status_code, 422)
        self.assertEqual(created.status_code, 202)
        self.assertTrue(created.json()["job_id"])
        self.assertIn("History fetch jobs", page.text)
        self.assertIn("setInterval(load,2000)", page.text)
        self.assertIn("Owner / lease", page.text)
        self.assertIn("Takeovers", page.text)
        self.assertIn("Cancel job", page.text)
        self.assertIn("encodeURIComponent(id)", page.text)
        self.assertIn("Completed data will be kept", page.text)
        self.assertNotIn("lease_token", created.text)
        self.assertNotIn("super-secret", page.text)

    def test_duplicate_symbols_rejected_and_yahoo_chart_is_normalized(self):
        duplicate = self.client.post(
            "/v1/admin/history-jobs",
            json=request(symbols=["aapl.xnas", " AAPL.XNAS "]),
        )
        normalized = self.client.post(
            "/v1/admin/history-jobs",
            json=request(
                symbols=["aapl.xnas"], provider="yahoo_chart",
                idempotency_key="history-yahoo-chart",
            ),
        )
        self.assertEqual(duplicate.status_code, 422)
        self.assertEqual(normalized.status_code, 202)
        self.assertEqual(normalized.json()["job"]["request_json"]["provider"], "yahoo")
        self.assertEqual(normalized.json()["job"]["items"][0]["symbol"], "AAPL.XNAS")

    def test_explicit_date_range_is_validated_and_frozen(self):
        created = self.client.post(
            "/v1/admin/history-jobs",
            json=request(
                symbols=["AAPL.XNAS"],
                range="custom",
                range_start="2026-06-01T00:00:00Z",
                range_end="2026-06-30T23:59:59Z",
                idempotency_key="history-explicit-window",
            ),
        )
        missing_end = self.client.post(
            "/v1/admin/history-jobs",
            json=request(
                range="custom",
                range_start="2026-06-01T00:00:00Z",
                idempotency_key="history-missing-window-end",
            ),
        )
        reversed_range = self.client.post(
            "/v1/admin/history-jobs",
            json=request(
                range="custom",
                range_start="2026-07-01T00:00:00Z",
                range_end="2026-06-01T00:00:00Z",
                idempotency_key="history-reversed-window",
            ),
        )
        self.assertEqual(created.status_code, 202)
        frozen = created.json()["job"]["request_json"]
        self.assertEqual(frozen["range"], "custom")
        self.assertEqual(frozen["range_start"], "2026-06-01T00:00:00+00:00")
        self.assertEqual(frozen["range_end"], "2026-06-30T23:59:59+00:00")
        self.assertEqual(missing_end.status_code, 422)
        self.assertEqual(reversed_range.status_code, 422)

    def test_reconcile_and_consistency_admin_endpoints(self):
        now = "2026-07-25T00:00:00+00:00"
        repository = self.service.metadata_repository
        repository.upsert_history_job({
            "job_id": "api-reconcile", "idempotency_key": "api-reconcile-key",
            "status": "failed", "request_json": request(symbols=["AAPL.XNAS"]),
            "created_at": now, "started_at": now, "updated_at": now,
            "finished_at": now, "error_json": None,
        })
        repository.upsert_history_item({
            "job_id": "api-reconcile", "item_id": "one", "symbol": "AAPL.XNAS",
            "status": "failed", "provider": "yahoo", "source": None,
            "attempt": 1, "rows_fetched": 0, "rows_persisted": 0,
            "canonical_status": "failed", "error_code": "service_interrupted",
            "error_message": "crash", "started_at": now, "updated_at": now,
            "finished_at": now,
        })
        repository.upsert_history_shard({
            "job_id": "api-reconcile", "item_id": "one",
            "shard_key": "shard", "ingestion_id": "api-ingestion",
            "shard_index": 0,
            "range_start": "2026-01-01T00:00:00+00:00",
            "range_end": "2026-01-02T00:00:00+00:00",
            "status": "failed", "attempt": 1, "rows_fetched": 0,
            "rows_persisted": 0, "cursor_json": {},
            "write_receipt_json": None, "error_code": "service_interrupted",
            "error_message": "crash", "owner_id": None, "lease_token": None,
            "lease_expires_at": None, "heartbeat_at": None,
            "created_at": now, "updated_at": now, "finished_at": now,
        })
        self.service.raw_receipts["api-ingestion"] = {
            "ingestion_id": "api-ingestion", "row_count": 2,
            "first_bar_at_ms": 1, "last_bar_at_ms": 2,
            "raw_artifact_id": "artifact",
        }

        dry_run = self.client.post(
            "/v1/admin/history-jobs/api-reconcile/reconcile",
            json={"dry_run": True},
        )
        repaired = self.client.post(
            "/v1/admin/history-jobs/api-reconcile/reconcile",
            json={"dry_run": False},
        )
        audit = self.client.get("/v1/admin/history-consistency?limit=100")
        health = self.client.get("/v1/admin/history-health")

        self.assertEqual(dry_run.status_code, 200)
        self.assertEqual(dry_run.json()["repaired_shards"], 1)
        self.assertEqual(repaired.status_code, 200)
        self.assertEqual(repaired.json()["repaired_items"], 1)
        self.assertEqual(
            repository.get_history_job("api-reconcile")["status"],
            "succeeded",
        )
        self.assertEqual(audit.status_code, 200)
        self.assertIn("findings", audit.json())
        self.assertEqual(health.status_code, 200)
        self.assertIn(health.json()["status"], {"healthy", "degraded"})


if __name__ == "__main__":
    unittest.main()
