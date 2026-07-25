from __future__ import annotations

import unittest

from marketcow.adjustment_backfill import AdjustmentBackfillService


class FakeRepository:
    def __init__(self, candidates, factors=None):
        self.candidates = candidates
        self.factors = factors or []
        self.inserted = []

    def list_adjustment_contract_candidates(self, limit):
        return self.candidates[:limit]

    def get_adjustment_factors(self, *_args):
        return self.factors

    def insert_raw_bars(self, rows, batch_id=""):
        self.inserted.extend(rows)
        return len(rows)


def row(**updates):
    value = {
        "symbol": "600519.XSHG", "market": "CN", "interval": "1m",
        "adjustment": "raw", "bar_time": "2026-07-24T01:35:00Z",
        "source": "tushare_via_stockai888", "adjustment_factor": 1,
        "content_rank": "old", "content_version": 1,
    }
    value.update(updates)
    return value


class AdjustmentBackfillTest(unittest.TestCase):
    def test_repairs_tushare_raw_and_keeps_ambiguous_adjusted_quarantined(self):
        repository = FakeRepository(
            [row(), row(adjustment="adjusted")],
            [{
                "adjustment_factor": "12.3",
                "source": "tushare_via_stockai888",
                "raw_artifact_id": "factor-a",
                "ingested_at": "2026-07-25T01:00:00Z",
            }],
        )

        plan = AdjustmentBackfillService(repository).run(apply=False)

        self.assertEqual(plan["repairable"], 1)
        self.assertEqual(plan["quarantined"], 1)
        self.assertEqual(
            plan["rows"][0]["corporate_action_factor"], "12.3"
        )
        self.assertEqual(
            plan["quarantine"][0]["reason"],
            "legacy_adjusted_is_ambiguous",
        )
        self.assertEqual(repository.inserted, [])

    def test_apply_replaces_content_identity_and_is_batch_scoped(self):
        repository = FakeRepository(
            [row()],
            [{
                "adjustment_factor": "12.3",
                "source": "tushare_via_stockai888",
                "raw_artifact_id": "factor-a",
                "ingested_at": "2026-07-25T01:00:00Z",
            }],
        )

        result = AdjustmentBackfillService(repository).run(apply=True)

        self.assertTrue(result["applied"])
        self.assertEqual(result["written"], 1)
        self.assertNotIn("content_rank", repository.inserted[0])
        self.assertEqual(
            repository.inserted[0]["ingestion_id"], result["batch_id"]
        )

    def test_crypto_is_not_applicable_and_unknown_sources_are_quarantined(self):
        repository = FakeRepository([
            row(symbol="BTC-PERP.HYPL", market="CRYPTO", source="hyperliquid"),
            row(symbol="AAPL.XNAS", market="US", source="unknown"),
        ])

        plan = AdjustmentBackfillService(repository).plan()

        self.assertEqual(plan["repairable"], 1)
        self.assertEqual(
            plan["rows"][0]["factor_applicability"], "not_applicable"
        )
        self.assertEqual(
            plan["quarantine"][0]["reason"], "factor_semantics_not_proven"
        )


if __name__ == "__main__":
    unittest.main()
