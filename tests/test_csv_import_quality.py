from __future__ import annotations

import unittest

from marketcow.csv_import_quality import CsvImportQualityVerifier


class Bars:
    def __init__(
        self, canonical_rows=2, invalid_ohlc_rows=0, abnormal_price_rows=0
    ):
        self.canonical_rows = canonical_rows
        self.invalid_ohlc_rows = invalid_ohlc_rows
        self.abnormal_price_rows = abnormal_price_rows

    def get_raw_ingestion_receipt(self, ingestion_id):
        return {
            "ingestion_id": ingestion_id, "row_count": 1,
            "raw_artifact_id": "artifact", "first_bar_at_ms": 1,
            "last_bar_at_ms": 2,
        }

    def get_canonical_ingestion_coverage(self, ingestion_ids):
        return {
            "ingestion_ids": sorted(ingestion_ids), "raw_rows": 2,
            "canonical_rows": self.canonical_rows,
            "first_bar_at_ms": 1, "last_bar_at_ms": 2,
            "canonical_invalid_ohlc_rows": self.invalid_ohlc_rows,
            "canonical_abnormal_price_rows": self.abnormal_price_rows,
        }

    def get_canonical_ingestion_quality(self, ingestion_ids):
        return [{
            "ingestion_id": value, "raw_rows": 1,
            "canonical_rows": (
                0 if self.canonical_rows < 2 and index == 0 else 1
            ),
            "canonical_invalid_ohlc_rows": (
                self.invalid_ohlc_rows if index == 0 else 0
            ),
        } for index, value in enumerate(sorted(ingestion_ids))]


def job():
    return {
        "job_id": "job", "manifest_id": "manifest", "status": "succeeded",
        "raw_artifact_id": "artifact", "rows_written": 2,
        "shards": [{
            "shard_index": 0,
            "write_receipt_json": {"receipts": [
                {"instrument_id": "AAPL.XNAS", "ingestion_id": "one", "rows": 1},
                {"instrument_id": "IBM.XNYS", "ingestion_id": "two", "rows": 1},
            ]},
        }],
    }


class CsvImportQualityVerifierTest(unittest.TestCase):
    def test_passes_only_when_raw_and_canonical_are_complete(self):
        result = CsvImportQualityVerifier(Bars()).verify(job())
        self.assertEqual(result["status"], "passed")
        self.assertEqual(result["expected_rows"], 2)
        self.assertEqual(result["coverage"]["canonical_rows"], 2)
        self.assertEqual(
            {row["shard_index"] for row in result["shard_diagnostics"]},
            {0},
        )

    def test_reports_incomplete_canonical_coverage(self):
        result = CsvImportQualityVerifier(Bars(canonical_rows=1)).verify(job())
        self.assertEqual(result["status"], "failed")
        self.assertEqual(
            result["failures"][0]["code"], "canonical_coverage_incomplete"
        )

    def test_fails_invalid_canonical_ohlc_and_warns_on_anomaly(self):
        result = CsvImportQualityVerifier(Bars(
            invalid_ohlc_rows=1, abnormal_price_rows=2
        )).verify(job())
        self.assertEqual(result["status"], "failed")
        self.assertIn(
            "canonical_ohlc_invalid",
            {failure["code"] for failure in result["failures"]},
        )
        self.assertIn(
            "canonical_abnormal_price_ratio",
            {warning["code"] for warning in result["warnings"]},
        )

    def test_rejects_nonterminal_job(self):
        value = job()
        value["status"] = "running"
        with self.assertRaisesRegex(ValueError, "succeeded"):
            CsvImportQualityVerifier(Bars()).verify(value)


if __name__ == "__main__":
    unittest.main()
