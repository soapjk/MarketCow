import hashlib
import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from marketcow.cold_archive import MANIFEST_VERSION, ParquetColdArchive
from marketcow.storage import Warehouse


def epoch(value):
    return int(datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp())


class ColdArchiveTest(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        self.root = Path(self.folder.name) / "data-development"
        self.database = self.root / "warehouse/market_data.duckdb"
        self.warehouse = Warehouse(self.database)
        self.archive = ParquetColdArchive(
            self.database, self.root / "cold/parquet", self.root
        )

    def tearDown(self):
        self.folder.cleanup()

    def insert(self, symbol="0700.HK", source="fixture", times=None, count=0):
        times = times or ["2026-06-30T23:59:59Z", "2026-07-01T00:00:00+00:00"]
        if count:
            start = datetime(2026, 7, 1, tzinfo=timezone.utc)
            times = [(start + timedelta(minutes=index)).isoformat() for index in range(count)]
        bars = [{
            "timestamp": epoch(value), "bar_at": value, "open": 10.0,
            "high": 11.0, "low": 9.0, "close": 10.5, "volume": 1000.0,
            "amount": 10500.0, "source_payload": {"repeated": "x" * 200},
        } for value in times]
        return self.warehouse.upsert_price_bars(
            symbol, "1m", "raw", source, "2026-07-20T00:00:00+00:00", bars,
            {"observed_at": "2026-07-20T08:00:00+08:00"},
        )

    def test_month_timezone_partition_round_trip_and_backfill(self):
        self.insert()
        june = self.archive.export_partition("HK", "1m", "fixture", 2026, 6)
        july = self.archive.export_partition("HK", "1m", "fixture", 2026, 7)
        self.assertEqual(june["row_count"], 1)
        self.assertEqual(july["row_count"], 1)
        self.assertEqual(june["watermark"]["max_timestamp"],
                         epoch("2026-06-30T23:59:59Z"))
        rows = self.archive.query(Path(july["artifact_path"]), "symbol=?", ["0700.HK"])
        self.assertEqual(rows[0]["timestamp"], epoch("2026-07-01T00:00:00Z"))
        self.assertEqual(self.archive.read_for_backfill(Path(july["artifact_path"])), rows)

    def test_repeat_export_is_idempotent_and_manifest_is_versioned(self):
        self.insert(times=["2026-07-01T00:00:00Z"])
        first = self.archive.export_partition("HK", "1m", "fixture", 2026, 7)
        second = self.archive.export_partition("HK", "1m", "fixture", 2026, 7)
        self.assertEqual(first["artifact_id"], second["artifact_id"])
        self.assertEqual(first["parquet_sha256"], second["parquet_sha256"])
        self.assertEqual(first["logical_checksum"], second["logical_checksum"])
        self.assertEqual(first["manifest_version"], MANIFEST_VERSION)
        artifacts = [path for path in Path(first["artifact_path"]).parent.iterdir()
                     if path.is_dir()]
        self.assertEqual(len(artifacts), 1)

    def test_atomic_publish_crash_windows_are_recoverable(self):
        self.insert(times=["2026-07-01T00:00:00Z"])

        def before_publish(stage):
            if stage == "after_manifest":
                raise RuntimeError("simulated crash")

        broken = ParquetColdArchive(
            self.database, self.root / "cold/crash-before", self.root,
            fault_hook=before_publish,
        )
        with self.assertRaisesRegex(RuntimeError, "simulated"):
            broken.export_partition("HK", "1m", "fixture", 2026, 7)
        self.assertEqual(list((self.root / "cold/crash-before/.staging").iterdir()), [])
        recovered = ParquetColdArchive(
            self.database, self.root / "cold/crash-before", self.root
        ).export_partition("HK", "1m", "fixture", 2026, 7)
        self.assertEqual(recovered["status"], "verified")

        def after_publish(stage):
            if stage == "after_publish":
                raise RuntimeError("published crash")

        published = ParquetColdArchive(
            self.database, self.root / "cold/crash-after", self.root,
            fault_hook=after_publish,
        )
        with self.assertRaisesRegex(RuntimeError, "published"):
            published.export_partition("HK", "1m", "fixture", 2026, 7)
        retried = ParquetColdArchive(
            self.database, self.root / "cold/crash-after", self.root
        ).export_partition("HK", "1m", "fixture", 2026, 7)
        self.assertEqual(retried["row_count"], 1)

    def test_corrupt_parquet_manifest_and_schema_evolution_are_rejected(self):
        self.insert(times=["2026-07-01T00:00:00Z"])
        result = self.archive.export_partition("HK", "1m", "fixture", 2026, 7)
        artifact = Path(result["artifact_path"])
        parquet = artifact / "data.parquet"
        original = parquet.read_bytes()
        parquet.write_bytes(original[:-8] + b"corrupt!")
        with self.assertRaisesRegex(ValueError, "checksum"):
            self.archive.query(artifact)
        parquet.write_bytes(original)
        manifest_path = artifact / "manifest.json"
        manifest = json.loads(manifest_path.read_text())
        manifest["row_count"] += 1
        manifest_path.write_text(json.dumps(manifest))
        with self.assertRaisesRegex(ValueError, "manifest checksum"):
            self.archive.verify(artifact)
        manifest["row_count"] -= 1
        manifest["schema_version"] = 999
        unsigned = {key: value for key, value in manifest.items()
                    if key != "manifest_payload_sha256"}
        manifest["manifest_payload_sha256"] = hashlib.sha256(json.dumps(
            unsigned, ensure_ascii=False, allow_nan=False, sort_keys=True,
            separators=(",", ":"), default=str,
        ).encode()).hexdigest()
        manifest_path.write_text(json.dumps(manifest))
        with self.assertRaisesRegex(ValueError, "schema version"):
            self.archive.read_for_backfill(artifact)
        manifest_path.write_text("not-json")
        with self.assertRaisesRegex(ValueError, "manifest"):
            self.archive.verify(artifact)

    def test_representative_zstd_compression_and_business_key_integrity(self):
        self.insert(count=1000)
        result = self.archive.export_partition("HK", "1m", "fixture", 2026, 7)
        self.assertEqual(result["row_count"], 1000)
        self.assertLess(result["parquet_bytes"], result["logical_json_bytes"] * 0.5)
        self.assertEqual(result["business_key"],
                         ["symbol", "interval", "adjustment", "timestamp", "source"])

    def test_development_and_allowed_root_boundaries(self):
        with self.assertRaisesRegex(ValueError, "development-only"):
            ParquetColdArchive(self.database, self.root / "cold/prod", self.root,
                               profile="production")
        with self.assertRaisesRegex(ValueError, "inside"):
            ParquetColdArchive(self.database, Path(self.folder.name) / "escape", self.root)
        outside = Path(self.folder.name) / "outside"
        outside.mkdir()
        link = self.root / "linked-cold"
        link.symlink_to(outside, target_is_directory=True)
        with self.assertRaisesRegex(ValueError, "inside"):
            ParquetColdArchive(self.database, link, self.root)


if __name__ == "__main__":
    unittest.main()
