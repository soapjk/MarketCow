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
from marketcow.health import HEALTH_SCHEMA, HealthEvaluator, HEALTH_THRESHOLDS
from marketcow.postgres_repositories import PostgresDatabase


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
    return {"schema": HEALTH_SCHEMA, "components": components}


def settings(root):
    return Settings(
        raw_path=root / "raw", profile="test", port=8793,
        postgres_dsn="postgresql://user:password@127.0.0.1/marketcow_test",
        postgres_schema="marketcow_test",
        clickhouse_database="marketcow_test", clickhouse_password="secret-token",
        storage_root=root, clickhouse_spool_path=root / "spool" / "clickhouse",
        postgres_dsn_ref="TEST_POSTGRES_DSN",
        clickhouse_password_ref="TEST_CLICKHOUSE_PASSWORD", allowed_root=root.parent,
    )


class HealthTest(unittest.TestCase):
    def setUp(self):
        self.clock = Clock()
        self.evaluator = HealthEvaluator(
            self.clock, lambda: datetime(2026, 7, 21, tzinfo=timezone.utc)
        )

    def test_required_databases_fail_closed_and_scheduler_disabled_is_explicit(self):
        healthy = self.evaluator.evaluate(snapshot())
        self.assertEqual(healthy["status"], "healthy")
        self.assertEqual(healthy["components"]["canonical_scheduler"]["status"], "disabled")
        for values in ({"pg": "unavailable"}, {"main": "unavailable"}):
            failed = HealthEvaluator(self.clock).evaluate(snapshot(**values))
            self.assertEqual(failed["status"], "unavailable")
            self.assertFalse(failed["ready"])

    def test_threshold_window_jitter_and_recovery_hysteresis(self):
        high = snapshot(authoritative_wal={"pending": 100})
        first = self.evaluator.evaluate(high)
        self.assertEqual(first["candidate_status"], "degraded")
        self.clock.value = HEALTH_THRESHOLDS["degrade_after_seconds"] - .001
        self.assertEqual(self.evaluator.evaluate(high)["status"], "healthy")
        self.clock.value += .001
        self.assertEqual(self.evaluator.evaluate(high)["status"], "degraded")
        jitter = HealthEvaluator(self.clock)
        self.clock.value += 1
        jitter.evaluate(high)
        self.clock.value += 10
        jitter.evaluate(snapshot())
        self.clock.value += 10
        result = jitter.evaluate(high)
        self.assertEqual(result["candidate_since_monotonic"], self.clock.value)
        critical = snapshot(authoritative_wal={"quarantine": 1})
        self.evaluator.evaluate(critical)
        self.clock.value += HEALTH_THRESHOLDS["unavailable_after_seconds"]
        self.assertEqual(self.evaluator.evaluate(critical)["status"], "unavailable")
        self.clock.value += 1
        self.evaluator.evaluate(snapshot())
        self.clock.value += HEALTH_THRESHOLDS["recover_after_seconds"]
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
        self.assertTrue(all(item["schema"] == HEALTH_SCHEMA for item in results))

    def test_api_is_logical_only_and_readiness_503(self):
        with tempfile.TemporaryDirectory(suffix="-test") as folder:
            root = Path(folder)
            resource = SimpleNamespace(health_snapshot=lambda: snapshot(pg="unavailable"))
            service = SimpleNamespace(online_resources=resource, close=lambda: None)
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

    def test_doctor_uses_factory_without_duckdb_and_redacts_failure(self):
        with tempfile.TemporaryDirectory(suffix="-test") as folder:
            config = settings(Path(folder))
            resources = MagicMock()
            resources.health_snapshot.return_value = snapshot()
            with patch("marketcow.factory.create_online_repositories",
                       return_value=resources), patch("importlib.import_module") as imported:
                result = diagnose(config)
            self.assertEqual(result["status"], "ready")
            imported.assert_not_called()
            resources.close.assert_called_once_with()
            rendered = json.dumps(result)
            self.assertNotIn("duckdb", rendered.lower())
            with patch("marketcow.factory.create_online_repositories",
                       side_effect=RuntimeError("password=secret /Volumes/T9/private")):
                failed = diagnose(config)
            self.assertEqual(failed["status"], "attention")
            self.assertNotIn("secret", json.dumps(failed))

    def test_factory_snapshot_requires_successful_postgres_select(self):
        database = SimpleNamespace(
            schema="marketcow_test", health_probe=MagicMock(return_value=True)
        )
        clickhouse = SimpleNamespace(
            database="marketcow_test",
            _require_client=lambda: SimpleNamespace(ping=lambda: True),
            pressure_probe=lambda: {"status": "observed", "merge_queue": 0,
                                    "disk_used_ratio": 0.1},
        )
        spool = SimpleNamespace(
            quarantine=Path("quarantine"),
            diagnostics=lambda _limit: {"pending": 0, "failed": 0, "replayed": 0,
                                        "truncated": False, "quota": {
                                            "bytes": 0, "free_bytes": 1000,
                                        }},
            _bounded_files=lambda *_args: ([], False),
        )
        from marketcow.factory import OnlineRepositories
        resources = OnlineRepositories(
            postgres_database=database, postgres_repository=object(),
            clickhouse_database=clickhouse, market_bars=object(),
            telemetry=SimpleNamespace(snapshot=lambda: {"metrics": []}), spool=spool,
            writer=object(), canonical_builder=object(), canonical_scheduler=None,
        )
        self.assertEqual(resources.health_snapshot()["components"]["postgresql"][
            "status"], "healthy")
        database.health_probe.return_value = False
        self.assertEqual(resources.health_snapshot()["components"]["postgresql"][
            "status"], "unavailable")
        database.health_probe.side_effect = RuntimeError(
            "postgresql://user:secret@127.0.0.1/db /Volumes/T9/private"
        )
        rendered = json.dumps(resources.health_snapshot())
        self.assertIn('"status": "unavailable"', rendered)
        self.assertNotIn("secret", rendered)
        self.assertNotIn("/Volumes", rendered)

    def test_live_pressure_probe_is_required_and_fail_closed(self):
        database = SimpleNamespace(
            schema="marketcow_test", health_probe=lambda: True,
        )
        pressure = MagicMock(return_value={
            "status": "observed", "merge_queue": 0, "disk_used_ratio": 0.01,
        })
        clickhouse = SimpleNamespace(
            database="marketcow_test", pressure_probe=pressure,
            _require_client=lambda: SimpleNamespace(ping=lambda: True),
        )
        spool = SimpleNamespace(
            quarantine=Path("quarantine"),
            diagnostics=lambda _limit: {"pending": 0, "failed": 0, "replayed": 0,
                                        "truncated": False, "quota": {
                                            "bytes": 0, "free_bytes": 1000}},
            _bounded_files=lambda *_args: ([], False),
        )
        from marketcow.factory import OnlineRepositories
        resources = OnlineRepositories(
            postgres_database=database, postgres_repository=object(),
            clickhouse_database=clickhouse, market_bars=object(), telemetry=object(),
            spool=spool, writer=object(), canonical_builder=object(),
            canonical_scheduler=None,
        )
        observed = resources.health_snapshot()["components"]["clickhouse_pressure"]
        self.assertEqual(observed["status"], "observed")
        pressure.assert_called_once_with()
        pressure.side_effect = TimeoutError("host=127.0.0.1 password=secret /Volumes/T9")
        failed = resources.health_snapshot()
        self.assertEqual(failed["components"]["clickhouse_pressure"], {
            "status": "unavailable", "reason": "pressure_probe_failed",
        })
        result = HealthEvaluator(self.clock).evaluate(failed)
        self.assertEqual(result["status"], "unavailable")
        rendered = json.dumps(result)
        self.assertNotIn("secret", rendered)
        self.assertNotIn("127.0.0.1", rendered)
        self.assertNotIn("/Volumes", rendered)

    def test_postgres_probe_binds_acquisition_and_statement_timeouts(self):
        executed = []

        class Connection:
            def execute(self, statement, parameters=None):
                executed.append((statement, parameters))
                return self

            def fetchone(self):
                return {"probe_value": 1}

        class Borrow:
            def __enter__(self): return Connection()
            def __exit__(self, *_args): return None

        pool = MagicMock()
        pool.connection.return_value = Borrow()
        database = PostgresDatabase.__new__(PostgresDatabase)
        database.pool = pool
        database.connect_timeout = 1.25
        database.read_timeout = 3.5
        self.assertTrue(database.health_probe())
        pool.connection.assert_called_once_with(timeout=1.25)
        self.assertEqual(executed[0][1], ("3500ms",))
        self.assertIn("SELECT 1", executed[1][0])


if __name__ == "__main__":
    unittest.main()
