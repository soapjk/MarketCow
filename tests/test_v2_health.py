import json
import tempfile
import threading
import unittest
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from fastapi.testclient import TestClient

from marketcow.__main__ import diagnose
from marketcow.api import create_app
from marketcow.config import Settings
from marketcow.health import V2_HEALTH_SCHEMA, V2HealthEvaluator, V2_THRESHOLDS


class Clock:
    def __init__(self):
        self.value = 0.0

    def __call__(self):
        return self.value


def snapshot(pg="healthy", main="healthy", scheduler=False, **overrides):
    components = {
        "postgresql": {"status": pg, "logical_id": "postgresql://marketcow_test"},
        "clickhouse_main": {"status": main, "logical_id": "clickhouse://marketcow_test"},
        "authoritative_wal": {
            "status": "healthy", "pending": 0, "failed": 0, "replayed": 0,
            "quarantine": 0, "oldest_pending_lag_seconds": 0,
            "disk_used_ratio": 0.1, "truncated": False,
        },
        "canonical_scheduler": ({
            "status": "healthy", "enabled": True, "thread_alive": True,
            "pending": 0, "failed": 0, "oldest_lag_seconds": 0,
            "backlog_truncated": False, "paused": False,
        } if scheduler else {"status": "disabled", "enabled": False}),
        "clickhouse_scheduler": ({
            "status": "healthy", "enabled": True,
            "logical_id": "clickhouse://marketcow_test",
        } if scheduler else {"status": "disabled", "enabled": False}),
        "clickhouse_pressure": {"status": "observed", "merge_queue": 0,
                                "disk_used_ratio": 0.1},
    }
    for key, values in overrides.items():
        components[key].update(values)
    return {"schema": V2_HEALTH_SCHEMA, "components": components}


def settings(root):
    return Settings(
        None, root / "raw", profile="v2-test", port=8793, metadata_backend="postgres",
        postgres_dsn="postgresql://user:password@127.0.0.1/marketcow_test",
        postgres_schema="marketcow_test", clickhouse_enabled=True,
        clickhouse_database="marketcow_test", clickhouse_password="secret-token",
        storage_root=root, clickhouse_spool_path=root / "spool" / "clickhouse",
        market_bar_read_backend="clickhouse_canonical",
        raw_market_bar_read_backend="clickhouse_raw",
        runtime_architecture="postgres_clickhouse_v2",
        runtime_config_schema="marketcow.v2-runtime-config.v1",
        postgres_dsn_ref="TEST_POSTGRES_DSN",
        clickhouse_password_ref="TEST_CLICKHOUSE_PASSWORD", v2_allowed_root=root.parent,
    )


class V2HealthTest(unittest.TestCase):
    def setUp(self):
        self.clock = Clock()
        self.evaluator = V2HealthEvaluator(
            self.clock, lambda: datetime(2026, 7, 21, tzinfo=timezone.utc)
        )

    def test_required_databases_fail_closed_and_scheduler_disabled_is_explicit(self):
        healthy = self.evaluator.evaluate(snapshot())
        self.assertEqual(healthy["status"], "healthy")
        self.assertEqual(healthy["components"]["canonical_scheduler"]["status"], "disabled")
        for values in ({"pg": "unavailable"}, {"main": "unavailable"}):
            failed = V2HealthEvaluator(self.clock).evaluate(snapshot(**values))
            self.assertEqual(failed["status"], "unavailable")
            self.assertFalse(failed["ready"])

    def test_threshold_window_jitter_and_recovery_hysteresis(self):
        high = snapshot(authoritative_wal={"pending": 100})
        first = self.evaluator.evaluate(high)
        self.assertEqual(first["candidate_status"], "degraded")
        self.clock.value = V2_THRESHOLDS["degrade_after_seconds"] - .001
        self.assertEqual(self.evaluator.evaluate(high)["status"], "healthy")
        self.clock.value += .001
        self.assertEqual(self.evaluator.evaluate(high)["status"], "degraded")
        jitter = V2HealthEvaluator(self.clock)
        self.clock.value += 1
        jitter.evaluate(high)
        self.clock.value += 10
        jitter.evaluate(snapshot())
        self.clock.value += 10
        result = jitter.evaluate(high)
        self.assertEqual(result["candidate_since_monotonic"], self.clock.value)
        critical = snapshot(authoritative_wal={"quarantine": 1})
        self.evaluator.evaluate(critical)
        self.clock.value += V2_THRESHOLDS["unavailable_after_seconds"]
        self.assertEqual(self.evaluator.evaluate(critical)["status"], "unavailable")
        self.clock.value += 1
        self.evaluator.evaluate(snapshot())
        self.clock.value += V2_THRESHOLDS["recover_after_seconds"]
        self.assertEqual(self.evaluator.evaluate(snapshot())["status"], "healthy")

    def test_worker_backlog_pressure_and_concurrent_schema(self):
        bad = snapshot(scheduler=True,
                       canonical_scheduler={"thread_alive": False, "pending": 1000},
                       clickhouse_pressure={"disk_used_ratio": .99, "merge_queue": 300})
        result = self.evaluator.evaluate(bad)
        self.assertEqual(result["status"], "unavailable")
        results = []
        threads = [threading.Thread(target=lambda: results.append(
            self.evaluator.evaluate(bad))) for _ in range(100)]
        for thread in threads: thread.start()
        for thread in threads: thread.join()
        self.assertTrue(all(item["schema"] == V2_HEALTH_SCHEMA for item in results))

    def test_v2_api_is_logical_only_and_readiness_503(self):
        with tempfile.TemporaryDirectory(suffix="-test") as folder:
            root = Path(folder)
            resource = SimpleNamespace(health_snapshot=lambda: snapshot(pg="unavailable"))
            service = SimpleNamespace(v2_resources=resource, close=lambda: None)
            with TestClient(create_app(settings(root), service)) as client:
                health = client.get("/v1/health")
                readiness = client.get("/v1/readiness")
            self.assertEqual(health.status_code, 200)
            self.assertEqual(readiness.status_code, 503)
            self.assertEqual(health.json()["database"],
                             "postgresql://marketcow_test+clickhouse://marketcow_test")
            rendered = health.text + readiness.text
            for forbidden in (str(root), "password", "secret-token", "duckdb"):
                self.assertNotIn(forbidden, rendered.lower())

    def test_v2_doctor_uses_factory_without_duckdb_and_redacts_failure(self):
        with tempfile.TemporaryDirectory(suffix="-test") as folder:
            config = settings(Path(folder))
            resources = MagicMock()
            resources.health_snapshot.return_value = snapshot()
            with patch("marketcow.v2_factory.create_v2_online_repositories",
                       return_value=resources), patch("importlib.import_module") as imported:
                result = diagnose(config)
            self.assertEqual(result["status"], "ready")
            imported.assert_not_called()
            resources.close.assert_called_once_with()
            rendered = json.dumps(result)
            self.assertNotIn("duckdb", rendered.lower())
            with patch("marketcow.v2_factory.create_v2_online_repositories",
                       side_effect=RuntimeError("password=secret /Volumes/T9/private")):
                failed = diagnose(config)
            self.assertEqual(failed["status"], "attention")
            self.assertNotIn("secret", json.dumps(failed))


if __name__ == "__main__":
    unittest.main()
