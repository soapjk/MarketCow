from __future__ import annotations

import tempfile
import unittest
import subprocess
import json
from pathlib import Path
from types import SimpleNamespace

from fastapi.testclient import TestClient

from marketcow.api import create_app
from marketcow.api_compat_gate import (
    DIFF_VERSION,
    SCHEMA_VERSION,
    capture_openapi_contract,
    capture_route_inventory,
    capture_route_matrix,
    capture_scenarios,
    load_document,
    run_gate,
    validate_coverage_inventory,
)
from marketcow.config import Settings
from marketcow.health import V2_HEALTH_SCHEMA


ROOT = Path(__file__).resolve().parents[1]
LEGACY_CONTRACT = ROOT / "docs/architecture/old-main-api-contract-v1.json"
LEGACY_SCENARIOS = ROOT / "docs/architecture/old-main-api-scenarios-v1.json"
DECLARED_DIFFERENCES = ROOT / "docs/architecture/old-main-v2-api-differences-v1.json"
DECLARED_SCENARIO_DIFFERENCES = (
    ROOT / "docs/architecture/old-main-v2-api-scenario-differences-v1.json"
)
LEGACY_ROUTE_SCENARIOS = ROOT / "docs/architecture/old-main-api-route-scenarios-v1.json"
V2_ROUTE_SCENARIOS = ROOT / "docs/architecture/v2-api-route-scenarios-v1.json"
LEGACY_ROUTE_MATRIX = ROOT / "docs/architecture/old-main-api-route-matrix-v1.json"
V2_ROUTE_MATRIX = ROOT / "docs/architecture/v2-api-route-matrix-v1.json"
COVERAGE = ROOT / "docs/architecture/old-main-v2-api-coverage-v1.json"
LEGACY_COMMIT = "701ffbde1b25ae587845ea2bd021ca8fa12b93b4"
LEGACY_CONTRACT_HASH = "2e380c45863e3acc77fe919846f3ff6b97a65c204ef69c4010d61d9091787047"
LEGACY_SCENARIO_HASH = "926219f0aba04098781587237c7a78ed1b10053f1e8107d2921a1ed3a553e091"


def v2_settings(root: Path) -> Settings:
    return Settings(
        None, root / "raw", profile="v2-test", port=8793,
        metadata_backend="postgres",
        postgres_dsn="postgresql://user:password@127.0.0.1/marketcow_test",
        postgres_schema="marketcow_test", clickhouse_enabled=True,
        clickhouse_database="marketcow_test", clickhouse_password="secret",
        storage_root=root, clickhouse_spool_path=root / "spool",
        market_bar_read_backend="clickhouse_canonical",
        raw_market_bar_read_backend="clickhouse_raw",
        runtime_architecture="postgres_clickhouse_v2",
        runtime_config_schema="marketcow.v2-runtime-config.v1",
        postgres_dsn_ref="TEST_POSTGRES_DSN",
        clickhouse_password_ref="TEST_CLICKHOUSE_PASSWORD",
        v2_allowed_root=root.parent,
    )


class FixtureRepository:
    telemetry = None

    def get_latest_quotes(self, symbols):
        return [
            {"symbol": symbol, "close": 1.0, "refresh_seen": False}
            for symbol in symbols if symbol != "MISSING"
        ]

    def __getattr__(self, _name):
        return lambda *_args, **_kwargs: []


class FixtureService:
    def __init__(self):
        repository = FixtureRepository()
        self.market_bar_repository = repository
        self.metadata_repository = repository
        self.fundamental_repository = repository
        self.v2_resources = SimpleNamespace(health_snapshot=lambda: {
            "schema": V2_HEALTH_SCHEMA,
            "components": {
                "postgresql": {"status": "healthy",
                               "logical_id": "postgresql://marketcow_test"},
                "clickhouse_main": {"status": "healthy",
                                    "logical_id": "clickhouse://marketcow_test"},
                "authoritative_wal": {"status": "healthy"},
                "canonical_scheduler": {"status": "disabled"},
                "clickhouse_scheduler": {"status": "disabled"},
                "clickhouse_pressure": {"status": "observed", "merge_queue": 0,
                                        "disk_used_ratio": 0.1},
            },
        })

    def refresh_quote(self, symbol):
        if symbol == "FAIL":
            raise RuntimeError("fixture unavailable")
        return {"symbol": symbol, "close": 1.0, "refresh_seen": True}

    def close(self):
        pass

    def __getattr__(self, _name):
        return lambda *_args, **_kwargs: []


class FaultRepository:
    telemetry = None

    def __getattr__(self, _name):
        def fail(*_args, **_kwargs):
            raise RuntimeError("bounded fixture failure")
        return fail


class FaultService(FixtureService):
    def __init__(self):
        repository = FaultRepository()
        self.market_bar_repository = repository
        self.metadata_repository = repository
        self.fundamental_repository = repository
        self.artifact_store = repository
        self.v2_resources = FixtureService().v2_resources

    def __getattr__(self, _name):
        def fail(*_args, **_kwargs):
            raise RuntimeError("bounded fixture failure")
        return fail


class OldMainApiContractTest(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory(suffix="-test")
        self.settings = v2_settings(Path(self.tempdir.name))
        self.app = create_app(self.settings, FixtureService())

    def tearDown(self):
        self.tempdir.cleanup()

    def test_full_route_and_parameter_contract_matches_exact_declared_differences(self):
        legacy = load_document(LEGACY_CONTRACT, SCHEMA_VERSION)
        declared = load_document(DECLARED_DIFFERENCES, DIFF_VERSION)
        current = capture_openapi_contract(self.app)
        report = run_gate(legacy, current, declared)
        self.assertEqual(report["status"], "ok", report)
        self.assertEqual(report["legacy_routes"], 32)
        self.assertEqual(report["v2_routes"], 38)
        self.assertGreater(report["difference_count"], 0)
        self.assertEqual(declared["legacy_sha256"], legacy["sha256"])
        self.assertEqual(declared["v2_sha256"], current["sha256"])

    def test_legacy_capture_is_frozen_to_named_git_commit_and_reproducible(self):
        legacy = load_document(LEGACY_CONTRACT, SCHEMA_VERSION)
        scenarios = load_document(LEGACY_SCENARIOS, SCHEMA_VERSION + ".scenarios")
        self.assertEqual(legacy["source_commit"], LEGACY_COMMIT)
        self.assertEqual(scenarios["source_commit"], LEGACY_COMMIT)
        self.assertEqual(legacy["sha256"], LEGACY_CONTRACT_HASH)
        self.assertEqual(scenarios["sha256"], LEGACY_SCENARIO_HASH)
        self.assertEqual(legacy["capture_tool_version"],
                         "marketcow.api-compat-capture.v2")
        subprocess.run(
            ["git", "cat-file", "-e", LEGACY_COMMIT + "^{commit}"],
            cwd=ROOT, check=True, capture_output=True,
        )
        worktrees = subprocess.check_output(
            ["git", "worktree", "list", "--porcelain"], cwd=ROOT, text=True,
        ).splitlines()
        source_root = None
        candidate = None
        for line in worktrees:
            if line.startswith("worktree "):
                candidate = Path(line.removeprefix("worktree "))
            elif line == "HEAD " + LEGACY_COMMIT:
                source_root = candidate
        self.assertIsNotNone(source_root, "frozen old-main worktree is required")
        with tempfile.TemporaryDirectory(suffix="-capture-proof") as directory:
            output = Path(directory) / "contract.json"
            matrix_output = Path(directory) / "matrix.json"
            subprocess.run([
                "uv", "run", "python", "scripts/capture_old_main_api.py",
                "--source-root", str(source_root), "--output", str(output),
                "--matrix-output", str(matrix_output),
            ], cwd=ROOT, check=True, capture_output=True)
            self.assertEqual(json.loads(output.read_text()), legacy)
            self.assertEqual(
                json.loads(matrix_output.read_text()),
                load_document(
                    LEGACY_ROUTE_MATRIX, SCHEMA_VERSION + ".route-matrix",
                ),
            )

    def test_every_public_route_has_executed_two_sided_coverage(self):
        legacy = load_document(LEGACY_CONTRACT, SCHEMA_VERSION)
        current = capture_openapi_contract(self.app)
        coverage = load_document(COVERAGE, "marketcow.old-main-v2-api-coverage.v1")
        report = validate_coverage_inventory(coverage, legacy, current)
        self.assertEqual(report["status"], "ok", report)
        self.assertEqual(report["route_count"], 38)
        legacy_routes = load_document(
            LEGACY_ROUTE_SCENARIOS,
            SCHEMA_VERSION + ".route-scenarios",
        )
        expected_v2 = load_document(
            V2_ROUTE_SCENARIOS,
            SCHEMA_VERSION + ".route-scenarios",
        )
        with TestClient(self.app, raise_server_exceptions=False) as client:
            observed_v2 = capture_route_inventory(client, current)
        self.assertEqual(set(legacy_routes["captures"]), set(legacy["routes"]))
        self.assertEqual(set(observed_v2["captures"]), set(current["routes"]))
        self.assertEqual(observed_v2, expected_v2)
        self.assertEqual(coverage["legacy_capture_sha256"],
                         legacy_routes["sha256"])
        self.assertEqual(coverage["v2_capture_sha256"], observed_v2["sha256"])
        expected_legacy_matrix = load_document(
            LEGACY_ROUTE_MATRIX, SCHEMA_VERSION + ".route-matrix",
        )
        expected_v2_matrix = load_document(
            V2_ROUTE_MATRIX, SCHEMA_VERSION + ".route-matrix",
        )
        fault_app = create_app(self.settings, FaultService())
        with TestClient(self.app, raise_server_exceptions=False) as normal_client, \
                TestClient(fault_app, raise_server_exceptions=False) as fault_client:
            observed_matrix = capture_route_matrix(
                normal_client, fault_client, current,
            )
        self.assertEqual(observed_matrix, expected_v2_matrix)
        self.assertEqual(coverage["legacy_matrix_sha256"],
                         expected_legacy_matrix["sha256"])
        self.assertEqual(coverage["v2_matrix_sha256"],
                         observed_matrix["sha256"])
        self.assertTrue(expected_legacy_matrix["captures"])
        for route in legacy["routes"]:
            self.assertIn(route + "::normal", expected_legacy_matrix["captures"])
        for route in current["routes"]:
            self.assertIn(route + "::normal", observed_matrix["captures"])

        mutated = dict(coverage)
        mutated["routes"] = coverage["routes"][:-1]
        self.assertEqual(
            validate_coverage_inventory(mutated, legacy, current)["status"],
            "mismatch",
        )
        duplicated = dict(coverage)
        duplicated["routes"] = coverage["routes"] + [coverage["routes"][0]]
        self.assertEqual(
            validate_coverage_inventory(duplicated, legacy, current)["status"],
            "mismatch",
        )

    def test_success_empty_validation_and_backend_failure_scenarios_are_exact(self):
        legacy = load_document(
            LEGACY_SCENARIOS, SCHEMA_VERSION + ".scenarios"
        )
        declared = load_document(
            DECLARED_SCENARIO_DIFFERENCES, DIFF_VERSION + ".scenarios"
        )
        with TestClient(self.app) as client:
            current = capture_scenarios(client)
        report = run_gate(legacy, current, declared)
        self.assertEqual(report["status"], "ok", report)
        self.assertEqual(set(legacy["scenarios"]), {
            "health", "history_invalid_parameter", "quotes_backend_failure",
            "quotes_batch_error", "quotes_default", "quotes_empty",
            "quotes_missing_parameter",
        })

    def test_allowlist_is_exact_path_and_cannot_hide_nested_same_name(self):
        legacy = load_document(LEGACY_CONTRACT, SCHEMA_VERSION)
        declared = load_document(DECLARED_DIFFERENCES, DIFF_VERSION)
        current = capture_openapi_contract(self.app)
        current["routes"]["GET /v1/health"]["responses"]["200"]["content"][
            "application/json"
        ] = {"schema": {"properties": {"database": {"type": "integer"}}}}
        report = run_gate(legacy, current, declared)
        self.assertEqual(report["status"], "mismatch")
        self.assertTrue(any("properties" in item["path"]
                            for item in report["mismatches"]))

    def test_documents_are_bounded_machine_readable_and_assign_every_diff_to_bg011(self):
        for path, schema in (
            (DECLARED_DIFFERENCES, DIFF_VERSION),
            (DECLARED_SCENARIO_DIFFERENCES, DIFF_VERSION + ".scenarios"),
        ):
            document = load_document(path, schema)
            self.assertLess(path.stat().st_size, 100_000)
            self.assertTrue(document["differences"])
            self.assertTrue(all(
                item["path"].startswith("$") and item["reason"] and
                item["bg011_action"] == "decide_compatibility_or_version"
                for item in document["differences"]
            ))
        scenario_text = DECLARED_SCENARIO_DIFFERENCES.read_text()
        self.assertIn("quotes refresh default", scenario_text)
        self.assertIn("batch error object", scenario_text)
        self.assertIn("health.database", scenario_text)


if __name__ == "__main__":
    unittest.main()
