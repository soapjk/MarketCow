from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from marketcow.csv_import_inference import infer_csv_semantics


class CsvImportInferenceTest(unittest.TestCase):
    def infer(self, body: str, **kwargs):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "bars.csv"
            path.write_text(body, encoding="utf-8")
            return infer_csv_semantics(
                path,
                mic=kwargs.get("mic", "XNAS"),
                columns=kwargs.get("columns", {
                    "timestamp": "DateTime", "close": "Close",
                }),
            )

    def test_new_york_session_and_dst_support_high_confidence_timezone(self):
        body = "DateTime,Open,High,Low,Close,Volume\n"
        for day in ("2025-03-07", "2025-03-10"):
            for hour in range(4, 20):
                body += f"{day} {hour:02}:00:00,1,2,1,2,3\n"
            body += f"{day} 09:30:00,1,2,1,2,3\n"
        result = self.infer(body)
        self.assertEqual(result["timezone"]["value"], "America/New_York")
        self.assertEqual(result["timezone"]["confidence"], "high")
        self.assertGreater(result["timezone"]["score"], 0.95)

    def test_explicit_utc_timestamps_are_not_relabelled_as_market_local(self):
        result = self.infer(
            "DateTime,Close\n2025-01-02T14:30:00Z,2\n"
        )
        self.assertEqual(result["timezone"]["value"], "UTC")
        self.assertEqual(result["timezone"]["confidence"], "high")

    def test_separate_adjusted_close_proves_selected_close_is_raw(self):
        result = self.infer(
            "DateTime,Close,Adj Close\n"
            "2025-01-02 09:30:00,100,99\n"
        )
        self.assertEqual(result["adjustment"]["value"], "raw")
        self.assertEqual(result["adjustment"]["confidence"], "high")

    def test_adjusted_column_is_detected_but_plain_ohlcv_stays_low_confidence(self):
        adjusted = self.infer(
            "DateTime,Adjusted Close\n2025-01-02 09:30:00,99\n",
            columns={"timestamp": "DateTime", "close": "Adjusted Close"},
        )
        self.assertIsNone(adjusted["adjustment"]["value"])
        self.assertEqual(adjusted["adjustment"]["confidence"], "none")
        self.assertIn("qfq from hfq", adjusted["adjustment"]["evidence"][0])

        ambiguous = self.infer(
            "DateTime,Close\n2025-01-02 09:30:00,100\n"
        )
        self.assertEqual(ambiguous["adjustment"]["value"], "raw")
        self.assertEqual(ambiguous["adjustment"]["confidence"], "low")
        self.assertIn("cannot prove", ambiguous["adjustment"]["evidence"][1])


if __name__ == "__main__":
    unittest.main()
