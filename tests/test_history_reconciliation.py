from __future__ import annotations

import unittest

from marketcow.history_reconciliation import HistoryReconciler
from tests.test_history_jobs import MemoryJobs, request


class RawReceipts:
    def __init__(self, receipts):
        self.receipts = receipts

    def get_raw_ingestion_receipt(self, ingestion_id):
        return self.receipts.get(ingestion_id)


class HistoryReconciliationTest(unittest.TestCase):
    def setUp(self):
        self.repository = MemoryJobs()
        now = "2026-07-25T00:00:00+00:00"
        self.job = {
            "job_id": "reconcile", "idempotency_key": "reconcile-key",
            "status": "running", "request_json": request(symbols=["AAPL.XNAS"]),
            "created_at": now, "started_at": now, "updated_at": now,
            "finished_at": None, "error_json": None,
        }
        self.item = {
            "job_id": "reconcile", "item_id": "one", "symbol": "AAPL.XNAS",
            "status": "failed", "provider": "yahoo", "source": None,
            "attempt": 1, "rows_fetched": 0, "rows_persisted": 0,
            "canonical_status": "failed", "error_code": "service_interrupted",
            "error_message": "crash", "started_at": now, "updated_at": now,
            "finished_at": now,
        }
        self.shard = {
            "job_id": "reconcile", "item_id": "one", "shard_key": "shard",
            "ingestion_id": "ingestion", "shard_index": 0,
            "range_start": "2026-01-01T00:00:00+00:00",
            "range_end": "2026-01-02T00:00:00+00:00",
            "status": "failed", "attempt": 1, "rows_fetched": 0,
            "rows_persisted": 0, "cursor_json": {},
            "write_receipt_json": None, "error_code": "service_interrupted",
            "error_message": "crash", "owner_id": None, "lease_token": None,
            "lease_expires_at": None, "heartbeat_at": None,
            "created_at": now, "updated_at": now, "finished_at": now,
        }
        self.repository.upsert_history_job(self.job)
        self.repository.upsert_history_item(self.item)
        self.repository.upsert_history_shard(self.shard)

    def test_dry_run_reports_without_mutation(self):
        reconciler = HistoryReconciler(
            self.repository,
            RawReceipts({"ingestion": {
                "ingestion_id": "ingestion", "row_count": 2,
                "first_bar_at_ms": 1, "last_bar_at_ms": 2,
                "raw_artifact_id": "artifact",
            }}),
        )

        result = reconciler.reconcile("reconcile", dry_run=True)

        self.assertEqual(result["repaired_shards"], 1)
        self.assertEqual(
            self.repository.list_history_shards("reconcile")[0]["status"],
            "failed",
        )

    def test_reconciles_raw_receipt_to_shard_item_and_job(self):
        reconciler = HistoryReconciler(
            self.repository,
            RawReceipts({"ingestion": {
                "ingestion_id": "ingestion", "row_count": 2,
                "first_bar_at_ms": 1, "last_bar_at_ms": 2,
                "raw_artifact_id": "artifact",
            }}),
        )

        result = reconciler.reconcile("reconcile", dry_run=False)
        shard = self.repository.list_history_shards("reconcile")[0]
        item = self.repository.list_history_items("reconcile")[0]
        job = self.repository.get_history_job("reconcile")

        self.assertEqual(result["repaired_shards"], 1)
        self.assertEqual(result["repaired_items"], 1)
        self.assertEqual(shard["status"], "succeeded")
        self.assertTrue(shard["write_receipt_json"]["reconciled"])
        self.assertEqual(item["status"], "succeeded")
        self.assertEqual(item["rows_persisted"], 2)
        self.assertEqual(job["status"], "succeeded")

    def test_active_lease_is_not_overwritten(self):
        active = dict(self.shard)
        active.update({
            "status": "running", "owner_id": "live", "lease_token": "token",
            "lease_expires_at": "2999-01-01T00:00:00+00:00",
        })
        self.repository.upsert_history_shard(active)
        reconciler = HistoryReconciler(
            self.repository,
            RawReceipts({"ingestion": {
                "ingestion_id": "ingestion", "row_count": 2,
                "first_bar_at_ms": 1, "last_bar_at_ms": 2,
                "raw_artifact_id": "artifact",
            }}),
        )

        result = reconciler.reconcile("reconcile", dry_run=False)

        self.assertEqual(result["actions"][0]["action"], "lease_conflict")
        self.assertEqual(
            self.repository.list_history_shards("reconcile")[0]["owner_id"],
            "live",
        )


if __name__ == "__main__":
    unittest.main()
