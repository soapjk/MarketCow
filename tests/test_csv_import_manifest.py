from __future__ import annotations

import tempfile
import unittest
from dataclasses import replace
from pathlib import Path

from marketcow.csv_import import (
    CsvImportRequest,
    CsvSchemaProfile,
    InstrumentMapping,
)
from marketcow.csv_import_manifest import (
    CsvImportManifest,
    archive_csv,
    plan_csv_shards,
)


def request():
    return CsvImportRequest(
        source="vendor",
        interval="1m",
        adjustment="raw",
        profile=CsvSchemaProfile(
            name="vendor-us",
            version="1",
            columns={
                "symbol": "symbol", "timestamp": "timestamp",
                "open": "open", "high": "high", "low": "low",
                "close": "close", "volume": "volume",
            },
            timezone_name="America/New_York",
        ),
        instruments=InstrumentMapping(
            "provider:vendor", {"AAPL.US": "AAPL.XNAS"}
        ),
    )


class CsvImportManifestTest(unittest.TestCase):
    def test_manifest_and_shards_are_stable_across_jobs(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "vendor.csv"
            path.write_text("symbol,timestamp,open,high,low,close,volume\n")
            first = CsvImportManifest.create(path, request())
            second = CsvImportManifest.create(path, request())

        self.assertEqual(first.manifest_id, second.manifest_id)
        self.assertEqual(first.file_sha256, second.file_sha256)
        shards = plan_csv_shards(first, 2501, 1000)
        self.assertEqual(
            [(row["row_start"], row["row_end"]) for row in shards],
            [(0, 1000), (1000, 2000), (2000, 2501)],
        )
        self.assertEqual(shards, plan_csv_shards(second, 2501, 1000))
        self.assertEqual(len({row["ingestion_id"] for row in shards}), 3)
        with self.assertRaisesRegex(ValueError, "version"):
            replace(first, contract_version="marketcow.csv-bars.v1")
        with self.assertRaisesRegex(ValueError, "adjustment"):
            replace(first, adjustment="adjusted")

    def test_file_content_or_mapping_changes_manifest_identity(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "vendor.csv"
            path.write_text("one")
            first = CsvImportManifest.create(path, request())
            path.write_text("two")
            second = CsvImportManifest.create(path, request())
        self.assertNotEqual(first.manifest_id, second.manifest_id)

    def test_manifest_records_time_range_operator_proof_and_retention(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "vendor.csv"
            path.write_text("bytes")
            declaration = request()
            manifest = CsvImportManifest.create(path, declaration, {
                "instruments": {
                    "AAPL.XNAS": {
                        "first_bar_at": "2026-01-02T14:30:00+00:00",
                        "last_bar_at": "2026-01-02T21:00:00+00:00",
                    }
                }
            })
        self.assertEqual(
            manifest.first_bar_at, "2026-01-02T14:30:00+00:00"
        )
        self.assertEqual(
            manifest.last_bar_at, "2026-01-02T21:00:00+00:00"
        )
        self.assertEqual(manifest.created_by, "local-operator")
        self.assertEqual(manifest.source_proof, "operator-declared")
        self.assertEqual(
            manifest.retention_policy, "retain-until-explicit-deletion"
        )

    def test_archive_is_atomic_hash_verified_and_deduplicated(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            source = root / "vendor.csv"
            source.write_text("immutable vendor bytes")
            manifest = CsvImportManifest.create(source, request())

            first = archive_csv(source, root / "storage", manifest)
            second = archive_csv(source, root / "storage", manifest)

            self.assertFalse(first["deduplicated"])
            self.assertTrue(second["deduplicated"])
            archived = Path(first["storage_path"])
            self.assertEqual(archived.read_text(), "immutable vendor bytes")
            self.assertFalse(any(
                path.name.endswith(".tmp")
                for path in archived.parent.iterdir()
            ))


if __name__ == "__main__":
    unittest.main()
