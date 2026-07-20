import io
import json
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from marketcow.local_backup import MANIFEST_VERSION, _hash, _json
from marketcow.local_backfill import BACKFILL_VERSION
from marketcow.local_benchmark import BENCHMARK_VERSION
from marketcow.local_read_switch import SWITCH_VERSION
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
        payloads = {
            "SV2-021A": {"manifest_version": MANIFEST_VERSION, "mode": "full",
                         "components": ["postgres", "clickhouse", "duckdb", "cold", "spool", "cursor"]},
            "SV2-021B": {"report_version": RESTORE_VERSION, "status": "complete",
                         "verification": {"contracts": "ok"}},
            "SV2-022A": {"version": BACKFILL_VERSION, "status": "complete", "lag": 0,
                         "reconciliation": {"status": "ok"}},
            "SV2-022B": {"version": SWITCH_VERSION, "status": "rolled_back",
                         "final_backend": "duckdb"},
            "SV2-023": {
                "version": BENCHMARK_VERSION, "status": "passed",
                "checks": {name: True for name in REQUIRED_BENCHMARK_CHECKS},
                "capacity": {
                    "measured_raw_rows": 28_800, "measured_raw_bytes": 1_382_400,
                    "bytes_per_raw_row": 48.0, "model_online_bytes": 12_000_000,
                    "model_required_disk_bytes_with_30pct_free": 17_142_857,
                    "observed_clickhouse_free_ratio": .42,
                },
            },
        }
        commits = {"SV2-021A": "25f833f", "SV2-021B": "8b6aa74", "SV2-022A": "6b44732",
                   "SV2-022B": "8a376d9", "SV2-023": "9a685e8"}
        result = {}
        evidence_root = self.allowed / "accepted-evidence"
        evidence_root.mkdir(exist_ok=True)
        for item, payload in payloads.items():
            payload_path = evidence_root / f"{item}.json"
            payload_path.write_bytes(_json(payload))
            record = {
                "version": EVIDENCE_VERSION, "item": item, "status": "accepted",
                "accepted_commit": commits[item],
                "evidence_uri": payload_path.relative_to(self.allowed).as_posix(),
                "evidence_sha256": _hash(payload_path.read_bytes()),
            }
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
        self.assertEqual(document["capacity"]["measured_raw_rows"], 28_800)
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
        benchmark = self.allowed / "accepted-evidence/SV2-023.json"
        value = json.loads(benchmark.read_text())
        value["checks"]["query_p99"] = False
        benchmark.write_bytes(_json(value))
        with self.assertRaisesRegex(ValueError, "evidence checksum"):
            self.package()

        self.evidence = self._write_evidence()
        benchmark = self.allowed / "accepted-evidence/SV2-023.json"
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
