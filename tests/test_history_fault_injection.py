from __future__ import annotations

import json
import unittest

from marketcow.history_consistency import HistoryConsistencyAuditor
from marketcow.history_jobs import HistoryJobManager
from marketcow.history_reconciliation import HistoryReconciler
from tests.test_history_jobs import JobService, request, terminal


class MutableArtifacts:
    def __init__(self):
        self.rows = []

    def list_artifacts(self, _dataset="", limit=10000):
        return self.rows[:limit]

    def record(self, ingestion_id):
        self.rows.append({
            "artifact_id": "artifact-" + ingestion_id[:12],
            "metadata_json": json.dumps({"ingestion_id": ingestion_id}),
        })


class CrossStoreHistoryFaultInjectionTest(unittest.TestCase):
    def test_crash_after_artifact_before_bars_is_classified_for_replay(self):
        service = JobService()
        artifacts = MutableArtifacts()
        service.artifact_store = artifacts

        def fail_after_artifact(*_args, ingestion_id=None, **_kwargs):
            artifacts.record(ingestion_id)
            raise ValueError("injected after_artifact_before_bars")

        service.refresh_quote_history_window = fail_after_artifact
        manager = HistoryJobManager(
            service, service.metadata_repository, max_workers=1
        )
        job, _ = manager.create(request(symbols=["AAPL.XNAS"], max_attempts=1))
        detail = terminal(manager, job["job_id"])
        audit = HistoryConsistencyAuditor(
            service.metadata_repository, service.market_bar_repository,
            artifacts,
        ).audit()

        self.assertEqual(detail["status"], "failed")
        self.assertEqual(audit["counts"]["market_bars_missing"], 1)
        finding = next(
            row for row in audit["findings"]
            if row["kind"] == "market_bars_missing"
        )
        self.assertEqual(finding["recommended_action"], "replay_artifact")
        manager.close()

    def test_crash_after_bars_before_checkpoint_is_reconciled(self):
        service = JobService()
        artifacts = MutableArtifacts()
        service.artifact_store = artifacts

        def fail_after_bars(*_args, ingestion_id=None, **_kwargs):
            artifacts.record(ingestion_id)
            service.raw_receipts[ingestion_id] = {
                "ingestion_id": ingestion_id, "row_count": 2,
                "first_bar_at_ms": 1, "last_bar_at_ms": 2,
                "raw_artifact_id": "artifact-" + ingestion_id[:12],
            }
            raise ValueError("injected after_bars_before_checkpoint")

        service.refresh_quote_history_window = fail_after_bars
        manager = HistoryJobManager(
            service, service.metadata_repository, max_workers=1
        )
        job, _ = manager.create(request(symbols=["AAPL.XNAS"], max_attempts=1))
        failed = terminal(manager, job["job_id"])
        repaired = HistoryReconciler(
            service.metadata_repository, service.market_bar_repository
        ).reconcile(job["job_id"], dry_run=False)
        final = manager.detail(job["job_id"])

        self.assertEqual(failed["status"], "failed")
        self.assertEqual(repaired["repaired_shards"], 1)
        self.assertEqual(repaired["repaired_items"], 1)
        self.assertEqual(final["status"], "succeeded")
        self.assertEqual(final["rows_persisted"], 2)
        manager.close()


if __name__ == "__main__":
    unittest.main()
