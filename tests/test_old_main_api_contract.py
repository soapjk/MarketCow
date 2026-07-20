from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from fastapi.testclient import TestClient

from marketcow.api import create_app
from marketcow.api_compat_gate import (
    DIFF_VERSION,
    SCHEMA_VERSION,
    capture_openapi_contract,
    capture_scenarios,
    load_document,
    run_gate,
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
