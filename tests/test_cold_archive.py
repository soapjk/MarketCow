import hashlib
import json
import tempfile
import threading
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

import marketcow.cold_archive as cold_archive_module
from marketcow.cold_archive import MANIFEST_VERSION, ParquetColdArchive
from marketcow.storage import Warehouse


def epoch(value):
    return int(datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp())


def resign(manifest):
    unsigned = {key: value for key, value in manifest.items()
                if key != "manifest_payload_sha256"}
    manifest["manifest_payload_sha256"] = hashlib.sha256(json.dumps(
        unsigned, ensure_ascii=False, allow_nan=False, sort_keys=True,
        separators=(",", ":"), default=str,
    ).encode()).hexdigest()


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
        resign(manifest)
        manifest_path.write_text(json.dumps(manifest))
        with self.assertRaisesRegex(ValueError, "schema version"):
            self.archive.read_for_backfill(artifact)
        manifest_path.write_text("not-json")
        with self.assertRaisesRegex(ValueError, "manifest"):
            self.archive.verify(artifact)

    def test_resigned_derived_metadata_and_directory_mismatch_are_rejected(self):
        self.insert(times=["2026-07-01T00:00:00Z"])
        result = self.archive.export_partition("HK", "1m", "fixture", 2026, 7)
        artifact = Path(result["artifact_path"])
        manifest_path = artifact / "manifest.json"
        original = json.loads(manifest_path.read_text())
        cases = [
            ("watermark", lambda item: item["watermark"].update(max_timestamp=1)),
            ("partition", lambda item: item["partition"].update(year=2099)),
            ("parquet byte", lambda item: item.update(parquet_bytes=1)),
            ("logical byte", lambda item: item.update(logical_json_bytes=1)),
            ("artifact id", lambda item: item.update(artifact_id="0" * 24)),
        ]
        for label, mutate in cases:
            manifest = json.loads(json.dumps(original))
            mutate(manifest)
            resign(manifest)
            manifest_path.write_text(json.dumps(manifest))
            with self.assertRaisesRegex(ValueError, "archive", msg=label):
                self.archive.verify(artifact)
        manifest_path.write_text(json.dumps(original))
        wrong = artifact.parent / ("f" * 24)
        artifact.rename(wrong)
        with self.assertRaisesRegex(ValueError, "directory"):
            self.archive.verify(wrong)

    def test_symlink_artifact_and_file_escape_are_rejected(self):
        self.insert(times=["2026-07-01T00:00:00Z"])
        result = self.archive.export_partition("HK", "1m", "fixture", 2026, 7)
        artifact = Path(result["artifact_path"])
        outside = Path(self.folder.name) / "outside.parquet"
        outside.write_bytes((artifact / "data.parquet").read_bytes())
        (artifact / "data.parquet").unlink()
        (artifact / "data.parquet").symlink_to(outside)
        with self.assertRaisesRegex(ValueError, "symlink|escapes"):
            self.archive.verify(artifact)
        artifact_link = self.archive.archive_root / "artifact-link"
        artifact_link.symlink_to(Path(self.folder.name), target_is_directory=True)
        with self.assertRaisesRegex(ValueError, "escapes"):
            self.archive.verify(artifact_link)

    def test_concurrent_same_content_publishes_once(self):
        self.insert(times=["2026-07-01T00:00:00Z"])
        results = []
        errors = []

        def export():
            try:
                results.append(self.archive.export_partition(
                    "HK", "1m", "fixture", 2026, 7
                ))
            except Exception as error:
                errors.append(error)

        threads = [threading.Thread(target=export) for _ in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(errors, [])
        self.assertEqual(len({item["artifact_id"] for item in results}), 1)
        artifact = Path(results[0]["artifact_path"])
        self.assertEqual([path for path in artifact.parent.iterdir() if path.is_dir()],
                         [artifact])

    def test_directory_fsync_order_covers_staging_hierarchy_and_publish(self):
        self.insert(times=["2026-07-01T00:00:00Z"])
        calls = []
        real_fsync = cold_archive_module._fsync_directory

        def record(path):
            calls.append(Path(path))
            real_fsync(path)

        with patch("marketcow.cold_archive._fsync_directory", side_effect=record):
            result = self.archive.export_partition("HK", "1m", "fixture", 2026, 7)
        artifact = Path(result["artifact_path"])
        staging_index = next(index for index, path in enumerate(calls)
                             if path.parent == self.archive.staging_root)
        partition_indices = [index for index, path in enumerate(calls)
                             if path == artifact.parent]
        self.assertEqual(len(partition_indices), 2)
        self.assertLess(staging_index, partition_indices[0])
        self.assertLess(partition_indices[0], partition_indices[1])

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
