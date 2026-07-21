import json
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

from marketcow.cold_archive import ParquetColdArchive
from marketcow.retention import MAX_ARTIFACTS, MAX_HOLDS, RetentionDryRun, RetentionPolicy
from marketcow.storage import Warehouse


def epoch(value):
    return int(datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp())


def resign(manifest):
    from marketcow.cold_archive import _json_hash
    unsigned = {key: value for key, value in manifest.items()
                if key != "manifest_payload_sha256"}
    manifest["manifest_payload_sha256"] = _json_hash(unsigned)


class RetentionDryRunTest(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        self.root = Path(self.folder.name) / "data-development"
        self.database = self.root / "warehouse/market_data.duckdb"
        self.warehouse = Warehouse(self.database)
        self.archive = ParquetColdArchive(
            self.database, self.root / "cold/parquet", self.root
        )
        self.policy = RetentionPolicy(
            default_retain_days=180, safety_window_days=30,
            source_retain_days={"long_hold_source": 365},
        )
        self.planner = RetentionDryRun(self.archive, self.policy)

    def tearDown(self):
        self.folder.cleanup()

    def insert(self, value="2024-01-15T00:00:00Z", source="fixture"):
        self.warehouse.upsert_price_bars(
            "0700.HK", "1m", "raw", source, "2024-02-01T00:00:00Z",
            [{"timestamp": epoch(value), "bar_at": value, "open": 1, "high": 2,
              "low": 1, "close": 2, "volume": 3}],
        )

    def artifact(self, source="fixture"):
        result = self.archive.export_partition("HK", "1m", source, 2024, 1)
        return Path(result["artifact_path"])

    def test_threshold_month_boundary_timezone_and_stable_repeat(self):
        self.insert()
        artifact = self.artifact()
        boundary = "2024-07-30T08:00:00+08:00"  # Jan 31 end + 180 days in UTC.
        before = self.planner.dry_run([artifact], "2024-07-29T23:59:59Z")
        self.assertEqual(before["candidate_count"], 0)
        self.assertEqual(before["excluded"][0]["reason"], "inside_retention_window")
        first = self.planner.dry_run([artifact], boundary)
        second = self.planner.dry_run([artifact], "2024-07-30T00:00:00Z")
        self.assertEqual(first, second)
        self.assertEqual(first["candidate_count"], 1)
        self.assertEqual(first["mutations_performed"], 0)
        self.assertEqual(first["action"], "candidate_only_no_delete")
        candidate = first["candidates"][0]
        self.assertTrue(candidate["cold_query_equivalent"])
        self.assertEqual(candidate["estimated_reclaim_bytes"],
                         first["estimated_reclaim_bytes"])
        self.assertTrue(candidate["manifest_payload_sha256"])
        self.assertTrue(candidate["parquet_sha256"])
        self.assertEqual(candidate["policy_sha256"], first["policy_sha256"])
        self.assertEqual(candidate["input_sha256"], first["input_sha256"])

    def test_hold_and_source_specific_policy_exclude(self):
        self.insert(source="long_hold_source")
        artifact = self.artifact("long_hold_source")
        partition_id = ("market=HK/interval=1m/source=long_hold_source/"
                        "year=2024/month=01")
        held = self.planner.dry_run([artifact], "2026-01-01T00:00:00Z", [partition_id])
        self.assertEqual(held["excluded"][0]["reason"], "held")
        inside = self.planner.dry_run([artifact], "2024-10-01T00:00:00Z")
        self.assertEqual(inside["excluded"][0]["reason"], "inside_retention_window")

    def test_missing_corrupt_and_resigned_semantic_manifest_never_candidate(self):
        self.insert()
        artifact = self.artifact()
        missing = self.planner.dry_run(
            [artifact, self.root / "cold/missing"], "2026-01-01T00:00:00Z"
        )
        self.assertEqual(missing["candidate_count"], 1)
        self.assertIn("artifact_verification_failed",
                      {item["reason"] for item in missing["excluded"]})
        manifest_path = artifact / "manifest.json"
        original = json.loads(manifest_path.read_text())
        tampered = json.loads(json.dumps(original))
        tampered["watermark"]["max_timestamp"] = 1
        resign(tampered)
        manifest_path.write_text(json.dumps(tampered))
        rejected = self.planner.dry_run([artifact], "2026-01-01T00:00:00Z")
        self.assertEqual(rejected["candidate_count"], 0)
        self.assertEqual(rejected["excluded"][0]["reason"],
                         "artifact_verification_failed")
        manifest_path.unlink()
        missing_manifest = self.planner.dry_run([artifact], "2026-01-01T00:00:00Z")
        self.assertEqual(missing_manifest["candidate_count"], 0)

    def test_incomplete_online_watermark_and_unarchived_data_never_candidate(self):
        self.insert()
        artifact = self.artifact()
        self.insert("2024-01-16T00:00:00Z")
        stale = self.planner.dry_run([artifact], "2026-01-01T00:00:00Z")
        self.assertEqual(stale["candidate_count"], 0)
        self.assertEqual(stale["excluded"][0]["reason"], "online_row_count_mismatch")
        empty = self.planner.dry_run([], "2026-01-01T00:00:00Z")
        self.assertEqual(empty["candidate_count"], 0)
        self.assertEqual(empty["estimated_reclaim_bytes"], 0)

    def test_dry_run_has_no_delete_ttl_or_filesystem_removal_side_effect(self):
        self.insert()
        artifact = self.artifact()
        original_manifest = (artifact / "manifest.json").read_bytes()
        original_parquet = (artifact / "data.parquet").read_bytes()
        with patch.object(Path, "unlink", side_effect=AssertionError("unlink called")), patch(
            "shutil.rmtree", side_effect=AssertionError("rmtree called")
        ), patch("os.remove", side_effect=AssertionError("remove called")):
            result = self.planner.dry_run([artifact], "2026-01-01T00:00:00Z")
        self.assertEqual(result["candidate_count"], 1)
        self.assertEqual((artifact / "manifest.json").read_bytes(), original_manifest)
        self.assertEqual((artifact / "data.parquet").read_bytes(), original_parquet)
        with duckdb_connection(self.database) as con:
            self.assertEqual(con.execute("SELECT count(*) FROM market_price_bar").fetchone()[0], 1)

    def test_policy_validation_is_versioned_and_bounded(self):
        with self.assertRaisesRegex(ValueError, "version"):
            RetentionPolicy(version="future")
        with self.assertRaisesRegex(ValueError, "retention"):
            RetentionPolicy(default_retain_days=1)
        with self.assertRaisesRegex(ValueError, "timezone"):
            self.planner.dry_run([], "2026-01-01T00:00:00")

    def test_policy_defensively_freezes_and_normalizes_external_rules(self):
        rules = {" fixture ": 180}
        policy = RetentionPolicy(source_retain_days=rules)
        before = policy.document()
        rules[" fixture "] = 365
        rules["new"] = 90
        self.assertEqual(policy.days_for("fixture"), 180)
        self.assertEqual(policy.document(), before)
        with self.assertRaises(TypeError):
            policy.source_retain_days["fixture"] = 365
        for invalid in ({"x": True}, {"x": 30.0}, {1: 30}):
            with self.assertRaisesRegex(ValueError, "source retention"):
                RetentionPolicy(source_retain_days=invalid)
        with self.assertRaisesRegex(ValueError, "retention"):
            RetentionPolicy(default_retain_days=True)

    def test_single_artifact_read_failure_does_not_hide_healthy_candidate(self):
        self.insert(source="fixture")
        healthy = self.artifact("fixture")
        self.insert(source="second")
        failing = self.artifact("second")
        real_coverage = self.planner._coverage

        def coverage(artifact, manifest):
            if Path(artifact) == failing:
                raise PermissionError("password=secret /Volumes/T9/private")
            return real_coverage(artifact, manifest)

        with patch.object(self.planner, "_coverage", side_effect=coverage):
            report = self.planner.dry_run(
                [failing, healthy], "2026-01-01T00:00:00Z"
            )
        self.assertEqual(report["candidate_count"], 1)
        self.assertEqual(report["excluded_count"], 1)
        self.assertEqual(report["excluded"][0]["reason"], "artifact_read_failed")
        rendered = str(report)
        self.assertNotIn("secret", rendered)
        self.assertNotIn("/Volumes/T9/private", rendered)

    def test_input_limits_and_complete_no_mutation_schema(self):
        with self.assertRaisesRegex(ValueError, "artifacts exceed"):
            self.planner.dry_run(
                [Path(f"artifact-{index}") for index in range(MAX_ARTIFACTS + 1)],
                "2026-01-01T00:00:00Z",
            )
        with self.assertRaisesRegex(ValueError, "holds exceed"):
            self.planner.dry_run(
                [], "2026-01-01T00:00:00Z",
                [f"hold-{index}" for index in range(MAX_HOLDS + 1)],
            )
        report = self.planner.dry_run([], "2026-01-01T00:00:00Z")
        self.assertEqual(report["limits"], {
            "artifacts": MAX_ARTIFACTS, "holds": MAX_HOLDS,
            "excluded": MAX_ARTIFACTS,
        })
        self.assertEqual(report["action"], "candidate_only_no_delete")
        self.assertEqual(report["mutations_performed"], 0)
        self.assertTrue(report["policy_sha256"])
        self.assertTrue(report["input_sha256"])


class duckdb_connection:
    def __init__(self, path):
        import duckdb
        self.connection = duckdb.connect(str(path), read_only=True)

    def __enter__(self):
        return self.connection

    def __exit__(self, *args):
        self.connection.close()


if __name__ == "__main__":
    unittest.main()
