import json
import io
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from marketcow.local_backup import _hash, _json
from marketcow.production_readiness import (
    READINESS_VERSION,
    REHEARSAL_GATES,
    ProductionReadinessInputs,
    ProductionReadinessPackage,
    main,
)


class ProductionReadinessPackageTest(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        self.allowed = Path(self.folder.name)
        self.root = self.allowed / "readiness-development"
        self.inputs = ProductionReadinessInputs(
            root=self.root, allowed_root=self.allowed,
            release_commit="9a685e8af15cc9d27aada13e256a8f0ceeb46de2",
            artifacts={
                "SV2-021A": "25f833f", "SV2-021B": "8b6aa74",
                "SV2-022A": "6b44732", "SV2-022B": "8a376d9",
                "SV2-023": "9a685e8",
            },
            target={
                "environment": "production", "service": "com.marketcow.production",
                "postgres": "marketcow-primary", "clickhouse": "marketcow-analytics",
                "port": 8790,
            },
            capacity={
                "model_online_bytes": 10_000_000, "required_disk_bytes": 15_000_000,
                "free_ratio": .40, "bytes_per_raw_row": 48.0,
            },
            slo_checks={"query_p99": True, "write_throughput": True,
                        "recovery": True, "capacity": True},
            profile="test",
        )

    def tearDown(self):
        self.folder.cleanup()

    def package(self, inputs=None):
        return ProductionReadinessPackage(inputs or self.inputs)

    @staticmethod
    def probes(state_changed=False):
        return {
            name: lambda state_changed=state_changed: {
                "status": "ok", "environment": "disposable",
                "state_changed": state_changed,
            }
            for name in REHEARSAL_GATES
        }

    def test_build_is_atomic_deterministic_dry_run_only_and_complete(self):
        package = self.package()
        with patch("socket.create_connection", side_effect=AssertionError("network")):
            first = package.build()
            second = package.build()
        self.assertEqual(first["manifest_sha256"], second["manifest_sha256"])
        self.assertEqual(first["version"], READINESS_VERSION)
        document = json.loads(package.package_path.read_text())
        self.assertEqual(document["status"], "ready_for_user_review")
        self.assertFalse(document["production_connections_attempted"])
        self.assertFalse(document["state_changes_executed"])
        self.assertFalse(document["next_item_started"])
        self.assertEqual(document["schema_preflight"]["postgres_expected_version"], 4)
        self.assertEqual(document["schema_preflight"]["clickhouse_expected_version"], 4)
        self.assertIn("no database connection", document["schema_preflight"]["mode"])
        self.assertTrue(all("--dry-run" in stage["dry_run_command"]
                            for stage in document["stages"]))
        self.assertTrue(all(not stage["apply_command_included"] and
                            stage["authorization_required"]
                            for stage in document["stages"]))
        self.assertEqual(len(document["external_actions"]), 7)
        self.assertTrue(all(not action["authorized"] and not action["executed"]
                            for action in document["external_actions"]))
        rendered = package.package_path.read_text() + package.runbook_path.read_text()
        self.assertNotIn("postgresql://", rendered)
        self.assertNotIn("clickhouse://", rendered)
        for stage in document["stages"]:
            output = io.StringIO()
            with patch("sys.stdout", output):
                self.assertEqual(main([
                    "stage", "--stage", stage["id"], "--target", "production",
                    "--dry-run",
                ]), 0)
            result = json.loads(output.getvalue())
            self.assertEqual(result["status"], "dry_run_only")
            self.assertFalse(result["production_connection_attempted"])
            self.assertFalse(result["state_changed"])
        with patch("sys.stderr", io.StringIO()), self.assertRaises(SystemExit):
            main(["stage", "--stage", "schema", "--target", "production"])

    def test_disposable_rehearsal_is_bounded_and_fail_closed(self):
        package = self.package()
        package.build()
        with patch("socket.create_connection", side_effect=AssertionError("network")):
            result = package.rehearse(self.probes())
        self.assertEqual(result["status"], "passed")
        self.assertEqual([item["gate"] for item in result["results"]],
                         list(REHEARSAL_GATES))
        self.assertTrue(all(not item["state_changed"] for item in result["results"]))
        bad = self.probes()
        bad["contracts"] = lambda: {
            "status": "mismatch", "environment": "disposable", "state_changed": False,
        }
        with self.assertRaisesRegex(RuntimeError, "contracts"):
            package.rehearse(bad)
        with self.assertRaisesRegex(ValueError, "gate set"):
            package.rehearse({key: value for key, value in self.probes().items()
                              if key != "rollback"})

    def test_target_artifact_capacity_slo_and_path_boundaries_are_rejected(self):
        values = dict(self.inputs.__dict__)
        values["profile"] = "production"
        with self.assertRaisesRegex(ValueError, "development/test-only"):
            self.package(ProductionReadinessInputs(**values))
        values = dict(self.inputs.__dict__)
        values["root"] = self.allowed.parent / "escape-development"
        with self.assertRaisesRegex(ValueError, "escapes"):
            self.package(ProductionReadinessInputs(**values))
        values = dict(self.inputs.__dict__)
        values["artifacts"] = {"SV2-023": "9a685e8"}
        with self.assertRaisesRegex(ValueError, "Artifact set"):
            self.package(ProductionReadinessInputs(**values))
        values = dict(self.inputs.__dict__)
        values["capacity"] = {**self.inputs.capacity, "free_ratio": .29}
        with self.assertRaisesRegex(ValueError, "30 percent"):
            self.package(ProductionReadinessInputs(**values))
        values = dict(self.inputs.__dict__)
        values["slo_checks"] = {"query": False}
        with self.assertRaisesRegex(ValueError, "SLO"):
            self.package(ProductionReadinessInputs(**values))

    def test_tampering_symlink_and_sensitive_input_are_rejected(self):
        package = self.package()
        package.build()
        document = json.loads(package.package_path.read_text())
        document["state_changes_executed"] = True
        package.package_path.write_bytes(_json(document))
        manifest = json.loads(package.manifest_path.read_text())
        manifest["package_sha256"] = _hash(package.package_path.read_bytes())
        manifest.pop("manifest_sha256")
        manifest["manifest_sha256"] = _hash(_json(manifest))
        package.manifest_path.write_bytes(_json(manifest))
        with self.assertRaisesRegex(ValueError, "binding mismatch"):
            package.verify()

        values = dict(self.inputs.__dict__)
        values["target"] = {**self.inputs.target,
                            "postgres": "postgresql://user:plain@host/db"}
        with self.assertRaisesRegex(ValueError, "logical identifier"):
            self.package(ProductionReadinessInputs(**values))

        other = self.allowed / "other"
        other.mkdir()
        unsafe = self.root / "unsafe-development"
        unsafe.parent.mkdir(parents=True, exist_ok=True)
        unsafe.symlink_to(other, target_is_directory=True)
        values = dict(self.inputs.__dict__)
        values["root"] = unsafe
        with self.assertRaisesRegex(ValueError, "symlink"):
            self.package(ProductionReadinessInputs(**values))


if __name__ == "__main__":
    unittest.main()
