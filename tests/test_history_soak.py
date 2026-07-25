from __future__ import annotations

import threading
import time
import unittest
from datetime import datetime, timezone

from marketcow.history_jobs import HistoryJobManager
from marketcow.history_shards import freeze_history_range, plan_history_shards
from tests.test_history_jobs import JobService, request, terminal


class HistoryCapacitySoakTest(unittest.TestCase):
    def test_many_batches_and_symbols_respect_global_bound_and_clean_threads(self):
        service = JobService()
        service.delay = 0.002
        manager = HistoryJobManager(
            service, service.metadata_repository, max_workers=4,
            lease_seconds=0.5,
        )
        thread_name = manager.thread_name
        jobs = []
        for batch in range(20):
            job, _ = manager.create(request(
                symbols=[f"SYM{batch:02d}{index:02d}" for index in range(10)],
                max_concurrency=8,
                idempotency_key=f"history-soak-{batch:04d}",
            ))
            jobs.append(job["job_id"])

        results = [terminal(manager, job_id, timeout=15) for job_id in jobs]

        self.assertTrue(all(row["status"] == "succeeded" for row in results))
        self.assertEqual(sum(row["total_symbols"] for row in results), 200)
        self.assertLessEqual(service.max_active, 4)
        manager.close()
        time.sleep(0.05)
        leaked = [
            thread.name for thread in threading.enumerate()
            if thread.name.startswith(thread_name)
        ]
        self.assertEqual(leaked, [])

    def test_long_range_plan_is_bounded_and_contiguous(self):
        frozen = freeze_history_range(
            {
                "provider": "yahoo", "range": "10y", "interval": "1m",
                "adjustment": "raw",
            },
            datetime(2026, 7, 25, tzinfo=timezone.utc),
        )
        shards = plan_history_shards(frozen)

        self.assertLessEqual(len(shards), 600)
        self.assertGreater(len(shards), 500)
        self.assertEqual(shards[0]["range_start"], frozen["range_start"])
        self.assertEqual(shards[-1]["range_end"], frozen["range_end"])
        self.assertTrue(all(
            left["range_end"] == right["range_start"]
            for left, right in zip(shards, shards[1:])
        ))


if __name__ == "__main__":
    unittest.main()
