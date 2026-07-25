from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from marketcow.csv_import import (
    CsvImportContractError,
    CsvImportRequest,
    CsvRowError,
    CsvSchemaProfile,
    InstrumentMapping,
    dry_run_csv,
    iter_csv_bars,
)


def profile(**overrides):
    values = {
        "name": "vendor-us",
        "version": "1",
        "columns": {
            "symbol": "ticker",
            "timestamp": "datetime",
            "open": "o",
            "high": "h",
            "low": "l",
            "close": "c",
            "volume": "v",
        },
        "timezone_name": "America/New_York",
        "timestamp_format": "%Y-%m-%d %H:%M:%S",
    }
    values.update(overrides)
    return CsvSchemaProfile(**values)


def request(**overrides):
    values = {
        "source": "vendor_name",
        "interval": "1m",
        "adjustment": "raw",
        "profile": profile(),
        "instruments": InstrumentMapping(
            "provider:vendor_name",
            {"AAPL.US": "AAPL.XNAS", "IBM.US": "IBM.XNYS"},
        ),
    }
    values.update(overrides)
    return CsvImportRequest(**values)


class CsvImportContractTest(unittest.TestCase):
    def test_profile_is_versioned_and_rejects_ambiguous_schema(self):
        self.assertEqual(profile().identity, "vendor-us@1")
        with self.assertRaisesRegex(CsvImportContractError, "missing required"):
            profile(columns={"symbol": "ticker"})
        with self.assertRaisesRegex(CsvImportContractError, "symbol column"):
            profile(columns={
                "timestamp": "t", "open": "o", "high": "h",
                "low": "l", "close": "c",
            })
        with self.assertRaisesRegex(CsvImportContractError, "unknown canonical"):
            profile(columns={**profile().columns, "vendor_magic": "magic"})

    def test_us_mapping_is_explicit_and_missing_symbols_fail(self):
        mapping = InstrumentMapping(
            "provider:purchased_feed", {"AAPL.US": "AAPL.XNAS"}
        )
        self.assertEqual(mapping.resolve("aapl.us"), "AAPL.XNAS")
        with self.assertRaisesRegex(CsvRowError, "no provider:purchased_feed"):
            mapping.resolve("MSFT.US")
        with self.assertRaises(ValueError):
            InstrumentMapping(
                "provider:purchased_feed", {"AAPL.US": "AAPL.US"}
            )

    def test_stream_parser_applies_dst_timezone_and_ohlc_rules(self):
        body = (
            "ticker,datetime,o,h,l,c,v\n"
            "AAPL.US,2026-11-02 09:30:00,100,102,99,101,10\n"
        )
        import io

        rows = list(iter_csv_bars(io.StringIO(body), request()))
        self.assertEqual(rows[0].instrument_id, "AAPL.XNAS")
        self.assertEqual(rows[0].bar["bar_at"], "2026-11-02T14:30:00+00:00")

        invalid = body.replace("100,102,99,101", "100,100,99,101")
        with self.assertRaisesRegex(CsvRowError, "OHLC"):
            list(iter_csv_bars(io.StringIO(invalid), request()))

    def test_dry_run_is_non_writing_exact_and_bounds_error_samples(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "bars.csv"
            path.write_text(
                "ticker,datetime,o,h,l,c,v\n"
                "AAPL.US,bad-time,100,101,99,100.5,20\n"
                "AAPL.US,2026-07-24 09:31:00,101,102,100,101.5,10\n"
                "AAPL.US,2026-07-24 09:30:00,100,101,99,100.5,20\n"
                "AAPL.US,2026-07-24 09:30:00,100,101,99,100.5,20\n"
                "MSFT.US,2026-07-24 09:30:00,1,2,0.5,1.5,3\n",
                encoding="utf-8",
            )

            result = dry_run_csv(path, request(), max_error_samples=1)

        self.assertEqual(result["status"], "invalid")
        self.assertTrue(result["dry_run"])
        self.assertEqual(result["rows_total"], 5)
        self.assertEqual(result["rows_valid"], 2)
        self.assertEqual(result["rows_invalid"], 3)
        self.assertEqual(result["duplicate_rows"], 1)
        self.assertEqual(result["unordered_rows"], 1)
        self.assertEqual(result["error_counts"], {
            "duplicate_bar": 1, "instrument_mapping_missing": 1,
            "timestamp_invalid": 1,
        })
        self.assertEqual(len(result["error_samples"]), 1)
        self.assertTrue(result["errors_truncated"])
        self.assertEqual(result["instruments"]["AAPL.XNAS"]["rows"], 2)
        self.assertEqual(len(result["file"]["sha256"]), 64)

    def test_header_is_checked_before_iteration(self):
        import io

        with self.assertRaisesRegex(CsvImportContractError, "missing source"):
            list(iter_csv_bars(
                io.StringIO("ticker,datetime,o,h,l,c\n"),
                request(),
            ))


if __name__ == "__main__":
    unittest.main()
