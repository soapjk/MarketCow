from __future__ import annotations

import json
import unittest

from marketcow.history_consistency import HistoryConsistencyAuditor
from tests.test_history_jobs import MemoryJobs


class Artifacts:
    def __init__(self, rows):
        self.rows = rows

    def list_artifacts(self, _dataset, limit):
        return self.rows[:limit]


class RawIngestions:
    def __init__(self, rows):
        self.rows = rows

    def list_raw_ingestion_receipts(self, limit):
        return self.rows[:limit]


class HistoryConsistencyAuditorTest(unittest.TestCase):
    def test_classifies_every_cross_store_inconsistency(self):
        repository = MemoryJobs()
        now = "2026-07-25T00:00:00+00:00"
        for index, ingestion_id in enumerate(
            ("healthy", "no-artifact", "no-bars", "missing")
        ):
            repository.upsert_history_shard({
                "job_id": "job", "item_id": "item",
                "shard_key": f"shard-{index}", "ingestion_id": ingestion_id,
                "shard_index": index,
                "range_start": "2026-01-01T00:00:00+00:00",
                "range_end": "2026-01-02T00:00:00+00:00",
                "status": "succeeded", "attempt": 1, "rows_fetched": 1,
                "rows_persisted": 1, "cursor_json": {},
                "write_receipt_json": {}, "error_code": None,
                "error_message": None, "owner_id": None, "lease_token": None,
                "lease_expires_at": None, "heartbeat_at": None,
                "created_at": now, "updated_at": now, "finished_at": now,
            })
        artifacts = Artifacts([
            {
                "artifact_id": "healthy-artifact",
                "metadata_json": json.dumps({"ingestion_id": "healthy"}),
            },
            {
                "artifact_id": "no-bars-artifact",
                "metadata_json": json.dumps({"ingestion_id": "no-bars"}),
            },
            {
                "artifact_id": "orphan-artifact",
                "metadata_json": json.dumps({"ingestion_id": "orphan-a"}),
            },
        ])
        raw = RawIngestions([
            {"ingestion_id": "healthy", "row_count": 1},
            {"ingestion_id": "no-artifact", "row_count": 1},
            {"ingestion_id": "orphan-b", "row_count": 2},
        ])
        auditor = HistoryConsistencyAuditor(repository, raw, artifacts)

        result = auditor.audit()

        self.assertEqual(result["counts"]["raw_artifact_missing"], 1)
        self.assertEqual(result["counts"]["market_bars_missing"], 1)
        self.assertEqual(result["counts"]["ingestion_missing"], 1)
        self.assertEqual(result["counts"]["orphan_artifact"], 1)
        self.assertEqual(result["counts"]["orphan_market_bars"], 1)
        actions = {
            row["kind"]: row["recommended_action"]
            for row in result["findings"]
        }
        self.assertEqual(actions["orphan_artifact"], "quarantine_artifact")
        self.assertEqual(actions["market_bars_missing"], "replay_artifact")


if __name__ == "__main__":
    unittest.main()
