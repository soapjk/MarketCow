import io
import json
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from marketcow.local_backup import BackupComponent, LocalStorageBackup, _hash, _json
from marketcow.local_backfill import BACKFILL_VERSION, POSTGRES_DOMAINS, LocalStorageBackfill
from marketcow.local_benchmark import (
    OPERATIONS, BenchmarkInputs, BenchmarkPlan, LocalStorageBenchmark,
)
from marketcow.local_read_switch import SWITCH_VERSION, LocalReadSwitchDrill
from marketcow.local_restore import RESTORE_VERSION
from marketcow.production_readiness import (
    EVIDENCE_VERSION,
    READINESS_VERSION,
    REHEARSAL_GATES,
    REQUIRED_BENCHMARK_CHECKS,
    ProductionReadinessInputs,
    ProductionReadinessPackage,
    main,
)


class ProductionReadinessPackageTest(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        self.allowed = Path(self.folder.name)
        self.root = self.allowed / "readiness-development"
        self.repository = Path(__file__).resolve().parents[1]
        self.head = subprocess.check_output(
            ["git", "-C", str(self.repository), "rev-parse", "HEAD"], text=True,
        ).strip()
        self.evidence = self._write_evidence()
        self.inputs = ProductionReadinessInputs(
            root=self.root, allowed_root=self.allowed, repository_root=self.repository,
            release_commit=self.head, evidence_paths=self.evidence,
            target={
                "environment": "production", "service": "com.marketcow.production",
                "postgres": "marketcow-primary", "clickhouse": "marketcow-analytics",
                "port": 8790,
            }, profile="test",
        )

    def tearDown(self):
        self.folder.cleanup()

    def _write_evidence(self):
        evidence_root = self.allowed / "accepted-evidence"
        evidence_root.mkdir(exist_ok=True)
        backup_root = evidence_root / "SV2-021A"
        captured = "2026-07-20T00:00:00Z"
        components = [BackupComponent(
            name, f"{name}-kind", "1", {f"{name}.json": _json({"fixture": name})},
            {"captured_at": captured}, name == "clickhouse",
        ) for name in ("postgresql", "clickhouse", "duckdb", "cold_archive", "spool")]
        components.append(BackupComponent(
            "cursor_key", "sealed-secret", "1", {"cursor.key": b"c" * 48},
            {"captured_at": captured},
        ))
        backup = LocalStorageBackup(backup_root, self.allowed, b"w" * 32)
        backup_result = backup.create(components, captured)
        backup_manifest = Path(backup_result["artifact_path"]) / "manifest.json"
        backup_id = backup_result["backup_id"]
        verification = {key: "ok" for key in (
            "postgres_pit", "clickhouse_raw", "duckdb_api_contract",
            "cold_verify_query_backfill", "spool_replay_once", "canonical_boundary",
        )}
        domain_names = [domain.table for domain in POSTGRES_DOMAINS] + [
            "market_bar_raw", "market_bar_canonical", "query_contracts",
        ]
        fingerprint = {"logical": "same"}
        checkpoint = {
            "version": BACKFILL_VERSION, "run_id": "fixture-run", "phase": "complete",
            "source_fingerprint": fingerprint, "completion_fingerprint": fingerprint,
            "last_live_fingerprint": fingerprint, "snapshot_watermark": captured,
            "source_path_hash": "fixture", "targets": {"postgres": "test", "clickhouse": "test"},
            "domains": {}, "catchup_passes": 1, "errors": [],
        }
        LocalStorageBackfill._sign(checkpoint)
        switch_checkpoint = {
            "version": SWITCH_VERSION, "binding": {"fixture": "bound"},
            "phase": "rolled_back", "canonical": "duckdb", "raw": "duckdb",
            "events": [{"event": "rollback"}], "stop_reason": "fixture",
        }
        LocalReadSwitchDrill._sign(switch_checkpoint)
        plan = BenchmarkPlan(10, 20, 240, 2, 3, max_peak_memory_mb=8192)
        operations = {}
        for name in OPERATIONS:
            result = {"rows": plan.sample_raw_rows, "verification": {
                "expected_rows": plan.sample_raw_rows, "actual_rows": plan.sample_raw_rows,
                "expected_checksum": name, "actual_checksum": name,
            }}
            if name == "raw_write":
                result["bytes"] = plan.sample_raw_rows * 24
            if name == "archive":
                result.update({"bytes": plan.sample_raw_rows * 12,
                               "uncompressed_bytes": plan.sample_raw_rows * 40})
            if name in {"page_first", "page_deep"}:
                result.update({"query_plan": "ReadFromMergeTree Filter bar_time > cursor",
                               "query_sql": ("SELECT bars WHERE bar_time > "
                                             "'2026-07-15 01:59:00.000' LIMIT 101"),
                               "cursor_depth": 0 if name == "page_first" else 4600,
                               "query_after": None if name == "page_first" else 1784075940,
                               "explain_after": None if name == "page_first" else 1784075940,
                               "depth_after": None if name == "page_first" else 1784075940,
                               "cursor_predicate": "" if name == "page_first" else
                               "2026-07-15 01:59:00.000"})
            if name == "query_warm":
                result["path_kind"] = "warm_existing_session"
            if name == "query_cold":
                result["path_kind"] = "new_connection"
            if name == "merge_probe":
                result.update({"total_bytes": 1_000_000, "free_bytes": 400_000,
                               "merge_backlog": 2})
            operations[name] = lambda _run, result=result: dict(result)
        class Clock:
            value = 0.0
            def __call__(self):
                value = self.value
                self.value += .01
                return value
        benchmark = LocalStorageBenchmark(BenchmarkInputs(
            evidence_root / "benchmark-test", plan, operations, "test", self.allowed,
            {"clickhouse": "disposable", "duckdb": "local"}, Clock(),
        )).run()
        payloads = {
            "SV2-021A": json.loads(backup_manifest.read_text()),
            "SV2-021B": {"report_version": RESTORE_VERSION, "status": "complete",
                         "verification": verification, "backup_chain": [backup_id],
                         "components": [{"name": name} for name in (
                             "postgresql", "clickhouse", "duckdb", "cold_archive", "spool", "cursor_key")],
                         "canonical_boundary": "verified raw+spool only",
                         "watermark": {"latest_captured_at": captured}},
            "SV2-022A": {"version": BACKFILL_VERSION, "status": "complete", "lag": 0,
                         "run_id": "fixture-run", "mismatches": [],
                         "domains": [{"domain": name, "status": "ok"} for name in domain_names]},
            "SV2-022B": {"version": SWITCH_VERSION, "status": "rolled_back",
                         "final_backend": "duckdb", "canonical": "duckdb", "raw": "duckdb",
                         "binding": {"fixture": "bound"}, "events": [{"event": "rollback"}]},
            "SV2-023": benchmark,
        }
        commits = {"SV2-021A": "25f833f", "SV2-021B": "8b6aa74", "SV2-022A": "6b44732",
                   "SV2-022B": "8a376d9", "SV2-023": "9a685e8"}
        result = {}
        for item, payload in payloads.items():
            payload_path = backup_manifest if item == "SV2-021A" else evidence_root / item / "report.json"
            payload_path.parent.mkdir(parents=True, exist_ok=True)
            payload_path.write_bytes(_json(payload))
            record = {
                "version": EVIDENCE_VERSION, "item": item, "status": "accepted",
                "accepted_commit": commits[item],
                "evidence_uri": payload_path.resolve().relative_to(self.allowed.resolve()).as_posix(),
                "evidence_sha256": _hash(payload_path.read_bytes()),
            }
            if item == "SV2-022A":
                companion = payload_path.parent / "checkpoint.json"
                companion.write_bytes(_json(checkpoint))
                record["companions"] = {"checkpoint": {
                    "uri": companion.resolve().relative_to(self.allowed.resolve()).as_posix(),
                    "sha256": _hash(companion.read_bytes()),
                }}
            if item == "SV2-022B":
                companion = payload_path.parent / "checkpoint.json"
                companion.write_bytes(_json(switch_checkpoint))
                record["companions"] = {"checkpoint": {
                    "uri": companion.resolve().relative_to(self.allowed.resolve()).as_posix(),
                    "sha256": _hash(companion.read_bytes()),
                }}
            record["acceptance_sha256"] = _hash(_json(record))
            acceptance = evidence_root / f"{item}.acceptance.json"
            acceptance.write_bytes(_json(record))
            result[item] = acceptance
        return result

    def package(self, inputs=None):
        return ProductionReadinessPackage(inputs or self.inputs)

    def test_build_uses_verified_evidence_and_runbook_is_complete(self):
        package = self.package()
        with patch("socket.create_connection", side_effect=AssertionError("network")):
            first, second = package.build(), package.build()
        self.assertEqual(first["manifest_sha256"], second["manifest_sha256"])
        document = json.loads(package.package_path.read_text())
        self.assertEqual(document["version"], READINESS_VERSION)
        self.assertEqual(set(document["slo_checks"]), REQUIRED_BENCHMARK_CHECKS)
        self.assertEqual(document["capacity"]["measured_raw_rows"], 288_000)
        self.assertEqual(set(document["evidence"]), set(self.evidence))
        self.assertNotIn("payload", json.dumps(document["evidence"]))
        runbook = package.runbook_path.read_text()
        for stage in document["stages"]:
            self.assertIn("Preconditions:", runbook)
            self.assertIn("Success evidence:", runbook)
            self.assertIn("Stop conditions:", runbook)
            self.assertIn(stage["id"], runbook)
        for action in document["external_actions"]:
            self.assertIn(f"### {action['id']}", runbook)
            self.assertIn(f"- Destination: {action['destination']}", runbook)
        self.assertEqual(len(document["external_actions"]), 7)
        self.assertTrue(all(not item["authorized"] and not item["executed"]
                            for item in document["external_actions"]))

    def test_each_stage_verifies_package_and_stage_specific_evidence(self):
        package = self.package()
        package.build()
        for stage in REHEARSAL_GATES:
            output = io.StringIO()
            with patch("sys.stdout", output), patch(
                    "socket.create_connection", side_effect=AssertionError("network")):
                self.assertEqual(main([
                    "stage", "--stage", stage, "--target", "production", "--dry-run",
                    "--package", str(package.package_root), "--allowed-root", str(self.allowed),
                    "--repository-root", str(self.repository),
                ]), 0)
            result = json.loads(output.getvalue())
            self.assertEqual(result["status"], "ok")
            self.assertGreater(len(result["checked_evidence"]), 0)
            self.assertFalse(result["production_connection_attempted"])
            self.assertFalse(result["state_changed"])
        with patch("sys.stderr", io.StringIO()), self.assertRaises(SystemExit):
            main(["stage", "--stage", "schema", "--target", "production"])
        report = package.rehearse()
        self.assertEqual(report["status"], "passed")
        self.assertEqual([item["stage"] for item in report["results"]], list(REHEARSAL_GATES))

    def test_tampered_swapped_failed_or_fake_evidence_is_rejected_before_build(self):
        benchmark = self.allowed / "accepted-evidence/SV2-023/report.json"
        value = json.loads(benchmark.read_text())
        value["checks"]["query_p99"] = False
        benchmark.write_bytes(_json(value))
        with self.assertRaisesRegex(ValueError, "evidence checksum"):
            self.package()

        self.evidence = self._write_evidence()
        benchmark = self.allowed / "accepted-evidence/SV2-023/report.json"
        value = json.loads(benchmark.read_text())
        value["checks"] = {"caller_says_ok": True}
        benchmark.write_bytes(_json(value))
        record_path = self.evidence["SV2-023"]
        record = json.loads(record_path.read_text())
        record["evidence_sha256"] = _hash(benchmark.read_bytes())
        record.pop("acceptance_sha256")
        record["acceptance_sha256"] = _hash(_json(record))
        record_path.write_bytes(_json(record))
        with self.assertRaisesRegex(ValueError, "incomplete or failed"):
            self.package(ProductionReadinessInputs(**{**self.inputs.__dict__,
                                                       "evidence_paths": self.evidence}))

    def test_git_path_symlink_and_package_tamper_are_fail_closed(self):
        values = dict(self.inputs.__dict__)
        values["release_commit"] = "deadbeef"
        with self.assertRaisesRegex(ValueError, "Git evidence|equal local HEAD"):
            self.package(ProductionReadinessInputs(**values))
        values = dict(self.inputs.__dict__)
        values["profile"] = "production"
        with self.assertRaisesRegex(ValueError, "development/test-only"):
            self.package(ProductionReadinessInputs(**values))

        package = self.package()
        package.build()
        document = json.loads(package.package_path.read_text())
        document["capacity"]["model_online_bytes"] += 1
        package.package_path.write_bytes(_json(document))
        manifest = json.loads(package.manifest_path.read_text())
        manifest["package_sha256"] = _hash(package.package_path.read_bytes())
        manifest.pop("manifest_sha256")
        manifest["manifest_sha256"] = _hash(_json(manifest))
        package.manifest_path.write_bytes(_json(manifest))
        with self.assertRaisesRegex(ValueError, "binding mismatch"):
            package.verify()

        outside = self.allowed.parent / "outside-evidence.json"
        outside.write_text("{}")
        values = dict(self.inputs.__dict__)
        values["evidence_paths"] = {**self.evidence, "SV2-021A": outside}
        with self.assertRaisesRegex(ValueError, "escapes allowed root"):
            self.package(ProductionReadinessInputs(**values))


if __name__ == "__main__":
    unittest.main()
