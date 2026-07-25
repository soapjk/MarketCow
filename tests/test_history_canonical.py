from __future__ import annotations

import unittest

from marketcow.history_canonical import HistoryCanonicalVerifier
from tests.test_history_jobs import MemoryJobs, request


class CanonicalIdentity:
    def __init__(self, counts):
        self.counts = iter(counts)

    def get_canonical_dataset_identity(self, *_args):
        return {"row_count": next(self.counts)}


class BytesReadRepository:
    def __init__(self, repository):
        self.repository = repository

    @staticmethod
    def _bytes(rows):
        return [{
            key: value.encode() if isinstance(value, str) else value
            for key, value in row.items()
        } for row in rows]

    def list_pending_history_canonical_checks(self, limit):
        return self._bytes(
            self.repository.list_pending_history_canonical_checks(limit)
        )

    def list_history_canonical_checks(self, job_id, item_id=None):
        return self._bytes(
            self.repository.list_history_canonical_checks(job_id, item_id)
        )

    def list_history_shards(self, job_id, item_id=None):
        return self._bytes(self.repository.list_history_shards(job_id, item_id))

    def list_history_items(self, job_id):
        return self._bytes(self.repository.list_history_items(job_id))

    def __getattr__(self, name):
        return getattr(self.repository, name)


class HistoryCanonicalVerifierTest(unittest.TestCase):
    def setUp(self):
        self.repository = MemoryJobs()
        now = "2026-07-25T00:00:00+00:00"
        self.repository.upsert_history_job({
            "job_id": "canonical", "idempotency_key": "canonical-key",
            "status": "succeeded", "request_json": request(symbols=["AAPL.XNAS"]),
            "created_at": now, "started_at": now, "updated_at": now,
            "finished_at": now, "error_json": None,
        })
        self.repository.upsert_history_item({
            "job_id": "canonical", "item_id": "one", "symbol": "AAPL.XNAS",
            "status": "succeeded", "provider": "yahoo", "source": "yahoo",
            "attempt": 1, "rows_fetched": 2, "rows_persisted": 2,
            "canonical_status": "pending", "error_code": None,
            "error_message": None, "started_at": now, "updated_at": now,
            "finished_at": now,
        })
        self.repository.upsert_history_shard({
            "job_id": "canonical", "item_id": "one", "shard_key": "shard",
            "ingestion_id": "ingestion", "shard_index": 0,
            "range_start": "2026-01-01T00:00:00+00:00",
            "range_end": "2026-01-02T00:00:00+00:00",
            "status": "succeeded", "attempt": 1, "rows_fetched": 2,
            "rows_persisted": 2, "cursor_json": {},
            "write_receipt_json": {"canonical_status": "pending"},
            "error_code": None, "error_message": None, "owner_id": None,
            "lease_token": None, "lease_expires_at": None,
            "heartbeat_at": None, "created_at": now, "updated_at": now,
            "finished_at": now,
        })

    def _check(self):
        return {
            "check_id": "ingestion", "job_id": "canonical",
            "item_id": "one", "shard_key": "shard", "symbol": "AAPL.XNAS",
            "interval": "1d", "adjustment": "raw",
            "range_start": "2026-01-01T00:00:00+00:00",
            "range_end": "2026-01-02T00:00:00+00:00",
            "expected_rows": 2,
        }

    def test_pending_check_survives_verifier_restart_and_completes(self):
        first = HistoryCanonicalVerifier(
            self.repository, CanonicalIdentity([1]), max_attempts=3
        )
        first.enqueue(self._check())
        first_result = first.run_pending()
        self.assertEqual(first_result["pending"], 1)

        restarted = HistoryCanonicalVerifier(
            self.repository, CanonicalIdentity([2]), max_attempts=3
        )
        second_result = restarted.run_pending()
        item = self.repository.list_history_items("canonical")[0]
        check = self.repository.list_history_canonical_checks("canonical")[0]

        self.assertEqual(second_result["completed"], 1)
        self.assertEqual(check["status"], "completed")
        self.assertEqual(check["attempt"], 2)
        self.assertEqual(item["canonical_status"], "completed")

    def test_check_becomes_failed_after_attempt_budget(self):
        verifier = HistoryCanonicalVerifier(
            self.repository, CanonicalIdentity([0, 0]), max_attempts=2
        )
        verifier.enqueue(self._check())

        verifier.run_pending()
        verifier.run_pending()

        check = self.repository.list_history_canonical_checks("canonical")[0]
        item = self.repository.list_history_items("canonical")[0]
        self.assertEqual(check["status"], "failed")
        self.assertEqual(item["canonical_status"], "failed")

    def test_sql_ascii_byte_rows_are_normalized_before_verification(self):
        self.repository.upsert_history_canonical_check({
            **self._check(), "status": "pending", "attempt": 0,
            "error_code": None, "error_message": None,
            "created_at": "2026-07-25T00:00:00+00:00",
            "updated_at": "2026-07-25T00:00:00+00:00",
            "finished_at": None,
        })
        verifier = HistoryCanonicalVerifier(
            BytesReadRepository(self.repository),
            CanonicalIdentity([2]),
        )

        result = verifier.run_pending()

        self.assertEqual(result["completed"], 1)
        self.assertEqual(
            self.repository.list_history_canonical_checks("canonical")[0]["status"],
            "completed",
        )


if __name__ == "__main__":
    unittest.main()
