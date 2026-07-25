from __future__ import annotations

import copy
import threading
import time
import unittest

from marketcow.csv_import_jobs import CsvImportJobManager, _safe_error_message
from marketcow.csv_import_ingestion import CsvImportCanceled


class MemoryRepository:
    def __init__(self):
        self.jobs = {}
        self.shards = {}
        self.keys = {}
        self.lock = threading.RLock()

    def get_or_create_csv_import_job(self, job, shards):
        with self.lock:
            if job["idempotency_key"] in self.keys:
                return copy.deepcopy(self.jobs[self.keys[job["idempotency_key"]]]), False
            self.jobs[job["job_id"]] = copy.deepcopy(job)
            self.keys[job["idempotency_key"]] = job["job_id"]
            for shard in shards:
                self.shards[(job["job_id"], shard["shard_index"])] = copy.deepcopy(shard)
            return copy.deepcopy(job), True

    def get_csv_import_job(self, job_id):
        with self.lock:
            return copy.deepcopy(self.jobs.get(job_id))

    def list_recoverable_csv_import_jobs(self):
        with self.lock:
            return [
                copy.deepcopy(row) for row in self.jobs.values()
                if row["status"] in {"queued", "running", "cancel_requested"}
            ]

    def list_csv_import_jobs(self, limit=50, manifest_id=""):
        with self.lock:
            rows = sorted(
                self.jobs.values(),
                key=lambda row: row["updated_at"],
                reverse=True,
            )
            if manifest_id:
                rows = [
                    row for row in rows
                    if row["manifest_id"] == manifest_id
                ]
            return copy.deepcopy(rows[:limit])

    def list_csv_import_shards(self, job_id):
        with self.lock:
            return [
                copy.deepcopy(row) for (owner, _), row in sorted(self.shards.items())
                if owner == job_id
            ]

    def update_csv_import_job(self, row):
        with self.lock:
            self.jobs[row["job_id"]] = copy.deepcopy(row)
            return copy.deepcopy(row)

    def claim_csv_import_shard(
        self, job_id, shard_index, owner_id, lease_token, now, lease_expires_at
    ):
        with self.lock:
            row = self.shards[(job_id, shard_index)]
            if row["status"] not in {"queued", "retry", "running"}:
                return None
            row.update({
                "status": "running", "attempt": row["attempt"] + 1,
                "owner_id": owner_id, "lease_token": lease_token,
                "lease_expires_at": lease_expires_at, "heartbeat_at": now,
            })
            return copy.deepcopy(row)

    def renew_csv_import_shard_lease(self, *args):
        job_id, shard_index, owner_id, token, now, expires = args
        with self.lock:
            row = self.shards[(job_id, shard_index)]
            if row.get("owner_id") != owner_id or row.get("lease_token") != token:
                return None
            row.update({"heartbeat_at": now, "lease_expires_at": expires})
            return copy.deepcopy(row)

    def finish_claimed_csv_import_shard(self, row, owner_id, token):
        with self.lock:
            current = self.shards[(row["job_id"], row["shard_index"])]
            if (
                current.get("owner_id") != owner_id
                or current.get("lease_token") != token
            ):
                return None
            saved = copy.deepcopy(row)
            for key in ("owner_id", "lease_token", "lease_expires_at", "heartbeat_at"):
                saved[key] = None
            self.shards[(row["job_id"], row["shard_index"])] = saved
            return copy.deepcopy(saved)

    def request_cancel_csv_import_job(self, job_id, now):
        with self.lock:
            row = self.jobs[job_id]
            if row["status"] not in {"queued", "running"}:
                return None
            row.update({"status": "cancel_requested", "updated_at": now})
            return copy.deepcopy(row)

    def cancel_unclaimed_csv_import_shards(self, job_id, now):
        changed = 0
        with self.lock:
            for (owner, _), row in self.shards.items():
                if owner == job_id and row["status"] in {"queued", "retry"}:
                    row.update({
                        "status": "canceled", "updated_at": now,
                        "finished_at": now,
                    })
                    changed += 1
        return changed


def wait_for(manager, job_id, status, timeout=2):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        job = manager.get(job_id)
        if job and job["status"] == status:
            return job
        time.sleep(0.01)
    raise AssertionError(f"job did not reach {status}: {manager.get(job_id)}")


def create(manager, key="import-key"):
    return manager.create(
        idempotency_key=key, manifest_id="manifest",
        request_json={"max_attempts": 2}, storage_path="/allowed/archive.csv",
        raw_artifact_id="artifact", rows_total=2,
        shards=[
            {"shard_index": 0, "row_start": 0, "row_end": 1, "ingestion_id": "a"},
            {"shard_index": 1, "row_start": 1, "row_end": 2, "ingestion_id": "b"},
        ],
    )


class CsvImportJobManagerTest(unittest.TestCase):
    def test_persisted_errors_redact_absolute_paths(self):
        message = _safe_error_message(
            "failed reading /srv/private/vendor/bars.csv at row 2"
        )
        self.assertNotIn("/srv/private", message)
        self.assertIn("<redacted-path>", message)

    def test_job_is_idempotent_and_aggregates_durable_shards(self):
        repository = MemoryRepository()
        manager = CsvImportJobManager(
            repository,
            lambda _job, shard: {
                "rows_read": shard["row_end"] - shard["row_start"],
                "rows_written": shard["row_end"] - shard["row_start"],
                "receipts": [],
            },
        )
        try:
            first, created = create(manager)
            self.assertTrue(created)
            completed = wait_for(manager, first["job_id"], "succeeded")
            second, created = create(manager)
            self.assertFalse(created)
            self.assertEqual(first["job_id"], second["job_id"])
            self.assertEqual(completed["rows_read"], 2)
            self.assertEqual(completed["rows_written"], 2)
            self.assertTrue(all(
                row["status"] == "succeeded" for row in completed["shards"]
            ))
        finally:
            manager.close()

    def test_retry_is_bounded_and_fenced_completion_is_used(self):
        repository = MemoryRepository()
        attempts = {}

        def import_shard(_job, shard):
            index = shard["shard_index"]
            attempts[index] = attempts.get(index, 0) + 1
            if index == 0 and attempts[index] == 1:
                raise ConnectionError("temporary")
            return {"rows_read": 1, "rows_written": 1, "receipts": []}

        manager = CsvImportJobManager(repository, import_shard)
        try:
            job, _ = create(manager, "retry-key")
            completed = wait_for(manager, job["job_id"], "succeeded")
            self.assertEqual(attempts[0], 2)
            self.assertEqual(completed["shards"][0]["attempt"], 2)
        finally:
            manager.close()

    def test_startup_recovers_queued_job(self):
        repository = MemoryRepository()
        first = CsvImportJobManager(repository, lambda *_: {})
        job, _ = first.create(
            idempotency_key="recover", manifest_id="manifest",
            request_json={"max_attempts": 1}, storage_path="/archive",
            raw_artifact_id="artifact", rows_total=0, shards=[],
        )
        first.close()
        repository.jobs[job["job_id"]]["status"] = "queued"
        second = CsvImportJobManager(repository, lambda *_: {})
        try:
            wait_for(second, job["job_id"], "succeeded")
        finally:
            second.close()

    def test_cancel_requested_transitions_running_job_to_canceled(self):
        repository = MemoryRepository()
        started = threading.Event()
        release = threading.Event()

        def importer(_job, _shard):
            started.set()
            release.wait(1)
            raise CsvImportCanceled("requested")

        manager = CsvImportJobManager(repository, importer, max_workers=1)
        try:
            job, _ = create(manager, "cancel-key")
            self.assertTrue(started.wait(1))
            self.assertEqual(
                manager.cancel(job["job_id"])["status"], "cancel_requested"
            )
            release.set()
            completed = wait_for(manager, job["job_id"], "canceled")
            self.assertTrue(all(
                row["status"] in {"succeeded", "canceled"}
                for row in completed["shards"]
            ))
        finally:
            release.set()
            manager.close()

    def test_progress_is_monotonic_and_visible_after_each_durable_shard(self):
        repository = MemoryRepository()
        second_started = threading.Event()
        release = threading.Event()

        def importer(_job, shard):
            if shard["shard_index"] == 1:
                second_started.set()
                release.wait(1)
            return {"rows_read": 1, "rows_written": 1, "receipts": []}

        manager = CsvImportJobManager(repository, importer, max_workers=1)
        try:
            job, _ = create(manager, "progress-key")
            self.assertTrue(second_started.wait(1))
            current = manager.get(job["job_id"])
            self.assertEqual(current["completed_shards"], 1)
            self.assertEqual(current["progress_percent"], 50)
            self.assertEqual(current["rows_written"], 1)
            release.set()
            completed = wait_for(manager, job["job_id"], "succeeded")
            self.assertEqual(completed["progress_percent"], 100)
            self.assertGreaterEqual(
                completed["rows_written"], current["rows_written"]
            )
        finally:
            release.set()
            manager.close()


if __name__ == "__main__":
    unittest.main()
