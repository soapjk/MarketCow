from __future__ import annotations

import unittest

from marketcow.bar_version import raw_content_rank


class AdjustmentBarVersionTest(unittest.TestCase):
    def test_factor_provenance_changes_raw_content_identity(self):
        row = {
            "open": "10", "high": "11", "low": "9", "close": "10",
            "raw_close": "10", "adjustment_factor": None,
            "corporate_action_factor": "12",
            "applied_adjustment_multiplier": "1",
            "adjustment_reference_date": None, "reference_factor": None,
            "factor_applicability": "applicable", "factor_source": "tushare",
            "factor_artifact_id": "factor-a",
            "factor_as_of": "2026-07-25T01:00:00Z",
            "volume": "100", "amount": "1000", "source_sequence": "1",
            "observed_at": "2026-07-25T01:00:00Z",
            "raw_artifact_id": "bar-a",
        }

        first = raw_content_rank(row)
        second = raw_content_rank({
            **row, "corporate_action_factor": "13",
        })
        provenance_change = raw_content_rank({
            **row, "factor_artifact_id": "factor-b",
        })

        self.assertNotEqual(first, second)
        self.assertNotEqual(first, provenance_change)


if __name__ == "__main__":
    unittest.main()
