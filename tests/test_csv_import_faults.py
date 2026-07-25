from __future__ import annotations

import tempfile
import time
import tracemalloc
import unittest
from pathlib import Path

from marketcow.csv_import import dry_run_csv
from marketcow.csv_import_jobs import CsvImportJobManager
from tests.test_csv_import import request
from tests.test_csv_import_jobs import MemoryRepository, wait_for


class CsvImportFaultAndScaleTest(unittest.TestCase):
    def test_expired_running_shard_is_taken_over_after_restart(self):
        repository = MemoryRepository()
        now = "2026-07-25T00:00:00+00:00"
        job = {
            "job_id": "crashed", "idempotency_key": "crashed-key",
            "manifest_id": "manifest", "status": "running",
            "request_json": {"max_attempts": 2}, "storage_path": "/archive",
            "raw_artifact_id": "artifact", "rows_total": 1,
            "rows_read": 0, "rows_written": 0, "error_code": None,
            "error_message": None, "quality_report_json": None,
            "created_at": now, "started_at": now, "updated_at": now,
            "finished_at": None,
        }
        shard = {
            "job_id": "crashed", "shard_index": 0, "row_start": 0,
            "row_end": 1, "ingestion_id": "stable", "status": "running",
            "attempt": 1, "rows_read": 0, "rows_written": 0,
            "write_receipt_json": None, "error_code": None,
            "error_message": None, "owner_id": "dead-worker",
            "lease_token": "dead-token", "lease_expires_at": now,
            "heartbeat_at": now, "created_at": now, "updated_at": now,
            "finished_at": None,
        }
        repository.get_or_create_csv_import_job(job, [shard])
        manager = CsvImportJobManager(
            repository,
            lambda _job, _shard: {
                "rows_read": 1, "rows_written": 1, "receipts": [],
            },
        )
        try:
            completed = wait_for(manager, "crashed", "succeeded")
            self.assertEqual(completed["shards"][0]["attempt"], 2)
            self.assertNotEqual(
                completed["shards"][0].get("owner_id"), "dead-worker"
            )
        finally:
            manager.close()

    def test_failed_quality_gate_prevents_success(self):
        repository = MemoryRepository()
        manager = CsvImportJobManager(
            repository,
            lambda _job, shard: {
                "rows_read": shard["row_end"] - shard["row_start"],
                "rows_written": shard["row_end"] - shard["row_start"],
                "receipts": [],
            },
            finalize_job=lambda _job: {
                "status": "failed",
                "failures": [{"code": "canonical_coverage_incomplete"}],
            },
        )
        try:
            job, _ = manager.create(
                idempotency_key="quality-fail", manifest_id="manifest",
                request_json={"max_attempts": 1}, storage_path="/archive",
                raw_artifact_id="artifact", rows_total=1,
                shards=[{
                    "shard_index": 0, "row_start": 0, "row_end": 1,
                    "ingestion_id": "stable",
                }],
            )
            failed = wait_for(manager, job["job_id"], "failed")
            self.assertEqual(failed["error_code"], "csv_import_quality_failed")
            self.assertEqual(
                failed["quality_report_json"]["failures"][0]["code"],
                "canonical_coverage_incomplete",
            )
        finally:
            manager.close()

    def test_streaming_dry_run_memory_is_bounded_for_large_file(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "large.csv"
            with path.open("w", encoding="utf-8") as stream:
                stream.write("ticker,datetime,o,h,l,c,v\n")
                for index in range(20000):
                    minute = index % 60
                    hour = (index // 60) % 24
                    day = 1 + index // (60 * 24)
                    stream.write(
                        f"AAPL.US,2026-01-{day:02d} {hour:02d}:{minute:02d}:00,"
                        "100,101,99,100.5,10\n"
                    )
            tracemalloc.start()
            started = time.monotonic()
            result = dry_run_csv(path, request())
            _current, peak = tracemalloc.get_traced_memory()
            tracemalloc.stop()

        self.assertEqual(result["rows_valid"], 20000)
        self.assertLess(peak, 12 * 1024 * 1024)
        self.assertLess(time.monotonic() - started, 10)


if __name__ == "__main__":
    unittest.main()
