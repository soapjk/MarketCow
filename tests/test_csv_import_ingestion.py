from __future__ import annotations

import io
import unittest
from dataclasses import replace

from marketcow.csv_import import (
    CsvImportRequest,
    CsvSchemaProfile,
    InstrumentMapping,
)
from marketcow.csv_import_ingestion import (
    CsvImportCanceled,
    CsvShardImporter,
    instrument_ingestion_id,
)
from marketcow.csv_import_manifest import CsvImportManifest


class Bars:
    def __init__(self):
        self.calls = []

    def upsert_price_bars(self, *args):
        self.calls.append(args)
        return len(args[5])


def declarations():
    profile = CsvSchemaProfile(
        name="vendor", version="1",
        columns={
            "symbol": "symbol", "timestamp": "time", "open": "open",
            "high": "high", "low": "low", "close": "close", "volume": "volume",
        },
        timezone_name="UTC",
    )
    request = CsvImportRequest(
        "vendor", "1m", "raw", profile,
        InstrumentMapping("provider:vendor", {
            "AAPL.US": "AAPL.XNAS", "IBM.US": "IBM.XNYS",
        }),
    )
    manifest = CsvImportManifest(
        manifest_id="a" * 64, contract_version="marketcow.csv-bars.v1",
        file_name="bars.csv", file_sha256="b" * 64, byte_size=1,
        source="vendor", namespace="provider:vendor",
        profile_name="vendor", profile_version="1", interval="1m",
        adjustment="raw", instrument_mappings={
            "AAPL.US": "AAPL.XNAS", "IBM.US": "IBM.XNYS",
        },
        first_bar_at=None, last_bar_at=None,
        created_at="2026-01-01T00:00:00+00:00",
        created_by="test", source_proof="fixture",
        retention_policy="retain-until-explicit-deletion",
    )
    return request, manifest


class CsvShardImporterTest(unittest.TestCase):
    def test_shard_routes_each_instrument_through_authoritative_writer(self):
        body = (
            "symbol,time,open,high,low,close,volume\n"
            "AAPL.US,2026-01-01T14:30:00Z,1,2,0.5,1.5,10\n"
            "IBM.US,2026-01-01T14:30:00Z,3,4,2,3.5,20\n"
            "AAPL.US,2026-01-01T14:31:00Z,2,3,1,2.5,30\n"
        )
        request, manifest = declarations()
        bars = Bars()
        result = CsvShardImporter(bars).import_shard(
            io.StringIO(body), request, manifest,
            {"row_start": 0, "row_end": 3, "ingestion_id": "shard"},
            raw_artifact_id="artifact", ingested_at="2026-01-02T00:00:00Z",
        )

        self.assertEqual(result["rows_read"], 3)
        self.assertEqual(result["rows_written"], 3)
        self.assertEqual([call[0] for call in bars.calls], [
            "AAPL.XNAS", "IBM.XNYS",
        ])
        self.assertEqual(len(bars.calls[0][5]), 2)
        for call in bars.calls:
            provenance = call[6]
            self.assertEqual(provenance["manifest_id"], "a" * 64)
            self.assertEqual(provenance["raw_artifact_id"], "artifact")
            self.assertEqual(len(provenance["ingestion_id"]), 64)

    def test_instrument_ingestion_identity_is_stable_and_scoped(self):
        first = instrument_ingestion_id("shard", "AAPL.XNAS")
        self.assertEqual(first, instrument_ingestion_id("shard", "AAPL.XNAS"))
        self.assertNotEqual(first, instrument_ingestion_id("shard", "IBM.XNYS"))

    def test_truncated_shard_fails_instead_of_reporting_success(self):
        request, manifest = declarations()
        with self.assertRaisesRegex(RuntimeError, "ended before"):
            CsvShardImporter(Bars()).import_shard(
                io.StringIO(
                    "symbol,time,open,high,low,close,volume\n"
                    "AAPL.US,2026-01-01T14:30:00Z,1,2,0.5,1.5,10\n"
                ),
                request, manifest,
                {"row_start": 0, "row_end": 2, "ingestion_id": "shard"},
                raw_artifact_id="artifact",
                ingested_at="2026-01-02T00:00:00Z",
            )

    def test_running_shard_checks_cooperative_cancellation(self):
        request, manifest = declarations()
        with self.assertRaises(CsvImportCanceled):
            CsvShardImporter(Bars()).import_shard(
                io.StringIO(
                    "symbol,time,open,high,low,close,volume\n"
                    "AAPL.US,2026-01-01T14:30:00Z,1,2,0.5,1.5,10\n"
                ),
                request, manifest,
                {"row_start": 0, "row_end": 1, "ingestion_id": "shard"},
                raw_artifact_id="artifact",
                ingested_at="2026-01-02T00:00:00Z",
                should_cancel=lambda: True,
            )

    def test_partial_multi_instrument_retry_reuses_stable_ingestion_ids(self):
        class PartiallyFailingBars:
            def __init__(self):
                self.rows = {}
                self.failed_once = False

            def upsert_price_bars(self, *args):
                ingestion_id = args[6]["ingestion_id"]
                self.rows[ingestion_id] = list(args[5])
                if args[0] == "IBM.XNYS" and not self.failed_once:
                    self.failed_once = True
                    raise ConnectionError("ClickHouse temporarily unavailable")
                return len(args[5])

        body = (
            "symbol,time,open,high,low,close,volume\n"
            "AAPL.US,2026-01-01T14:30:00Z,1,2,0.5,1.5,10\n"
            "IBM.US,2026-01-01T14:30:00Z,3,4,2,3.5,20\n"
        )
        request, manifest = declarations()
        bars = PartiallyFailingBars()
        importer = CsvShardImporter(bars)
        arguments = (
            request,
            manifest,
            {"row_start": 0, "row_end": 2, "ingestion_id": "stable-shard"},
        )
        with self.assertRaises(ConnectionError):
            importer.import_shard(
                io.StringIO(body), *arguments,
                raw_artifact_id="artifact",
                ingested_at="2026-01-02T00:00:00Z",
            )
        result = importer.import_shard(
            io.StringIO(body), *arguments,
            raw_artifact_id="artifact",
            ingested_at="2026-01-03T00:00:00Z",
        )
        self.assertEqual(result["rows_written"], 2)
        self.assertEqual(len(bars.rows), 2)

    def test_vendor_adjusted_bars_preserve_explicit_adjustment_semantics(self):
        request, manifest = declarations()
        request = replace(request, adjustment="adjusted")
        manifest = replace(manifest, adjustment="adjusted")
        bars = Bars()
        CsvShardImporter(bars).import_shard(
            io.StringIO(
                "symbol,time,open,high,low,close,volume\n"
                "AAPL.US,2020-08-31T13:30:00Z,125,130,123,129,1000\n"
            ),
            request,
            manifest,
            {"row_start": 0, "row_end": 1, "ingestion_id": "split-adjusted"},
            raw_artifact_id="vendor-adjusted-artifact",
            ingested_at="2026-01-02T00:00:00Z",
        )
        self.assertEqual(bars.calls[0][2], "adjusted")
        self.assertEqual(
            bars.calls[0][6]["raw_artifact_id"],
            "vendor-adjusted-artifact",
        )


if __name__ == "__main__":
    unittest.main()
