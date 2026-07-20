import threading
import unittest
from datetime import datetime, timezone
from types import SimpleNamespace

from fastapi.testclient import TestClient

from marketcow.api import create_app
from marketcow.config import Settings
from marketcow.health import HEALTH_SCHEMA, StorageHealthEvaluator, THRESHOLDS
from marketcow.telemetry import Telemetry


class MutableClock:
    def __init__(self):
        self.value = 0.0

    def __call__(self):
        return self.value


def snapshot(disk=0.1, merge=0, failed=0, quarantine=0):
    telemetry = Telemetry(clickhouse_enabled=True)
    telemetry.clickhouse_pressure(merge, disk)
    telemetry.gauge("wal_items", failed, state="failed")
    telemetry.gauge("wal_items", quarantine, state="quarantine")
    return telemetry.snapshot()


class StorageHealthTest(unittest.TestCase):
    def setUp(self):
        self.clock = MutableClock()
        self.evaluator = StorageHealthEvaluator(
            self.clock, lambda: datetime(2026, 7, 20, tzinfo=timezone.utc)
        )

    def test_disabled_missing_and_schema_are_explicit(self):
        disabled = self.evaluator.evaluate(Telemetry().snapshot())
        self.assertEqual(disabled["status"], "disabled")
        self.assertTrue(disabled["ready"])
        missing = StorageHealthEvaluator(self.clock).evaluate(
            Telemetry(clickhouse_enabled=True).snapshot()
        )
        self.assertEqual(missing["status"], "degraded")
        unavailable = StorageHealthEvaluator(self.clock).evaluate(None)
        self.assertEqual(unavailable["status"], "unavailable")
        self.assertFalse(unavailable["ready"])
        self.assertEqual(unavailable["schema"], HEALTH_SCHEMA)

    def test_degraded_threshold_is_inclusive_and_sustained(self):
        self.assertEqual(self.evaluator.evaluate(snapshot())["status"], "healthy")
        high = snapshot(disk=THRESHOLDS["disk_degraded_ratio"])
        first = self.evaluator.evaluate(high)
        self.assertEqual(first["status"], "healthy")
        self.assertEqual(first["candidate_status"], "degraded")
        self.clock.value = THRESHOLDS["degrade_after_seconds"] - 0.001
        self.assertEqual(self.evaluator.evaluate(high)["status"], "healthy")
        self.clock.value = THRESHOLDS["degrade_after_seconds"]
        self.assertEqual(self.evaluator.evaluate(high)["status"], "degraded")

    def test_critical_transition_and_hysteretic_recovery(self):
        critical = snapshot(disk=THRESHOLDS["disk_unavailable_ratio"])
        self.evaluator.evaluate(critical)
        self.clock.value = THRESHOLDS["unavailable_after_seconds"]
        self.assertEqual(self.evaluator.evaluate(critical)["status"], "unavailable")
        clean = snapshot()
        self.clock.value += 1
        pending = self.evaluator.evaluate(clean)
        self.assertEqual(pending["status"], "unavailable")
        self.clock.value += THRESHOLDS["recover_after_seconds"] - 0.001
        self.assertEqual(self.evaluator.evaluate(clean)["status"], "unavailable")
        self.clock.value += 0.001
        self.assertEqual(self.evaluator.evaluate(clean)["status"], "healthy")

    def test_jitter_resets_candidate_window(self):
        high = snapshot(merge=50)
        self.evaluator.evaluate(high)
        self.clock.value = 20
        self.evaluator.evaluate(snapshot())
        self.clock.value = 40
        result = self.evaluator.evaluate(high)
        self.assertEqual(result["status"], "healthy")
        self.assertEqual(result["candidate_since_monotonic"], 40)

    def test_reasons_are_bounded_and_do_not_expose_snapshot_errors(self):
        data = snapshot(disk=0.99, merge=300, failed=2, quarantine=20)
        data["logs"] = [{"error": "token=secret /Volumes/T9/private"}]
        result = self.evaluator.evaluate(data)
        rendered = str(result)
        self.assertLessEqual(len(result["reasons"]), 8)
        self.assertNotIn("secret", rendered)
        self.assertNotIn("/Volumes", rendered)

    def test_concurrent_reads_return_complete_schema(self):
        data = snapshot()
        results = []

        def read():
            for _ in range(100):
                results.append(self.evaluator.evaluate(data))

        threads = [threading.Thread(target=read) for _ in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(len(results), 800)
        self.assertTrue(all(item["schema"] == HEALTH_SCHEMA for item in results))
        self.assertTrue(all(item["status"] == "healthy" for item in results))

    def test_health_and_readiness_api_schema_is_backward_compatible(self):
        telemetry = Telemetry()
        repository = SimpleNamespace(telemetry=telemetry)
        service = SimpleNamespace(market_bar_repository=repository, close=lambda: None)
        settings = Settings.from_env()
        with TestClient(create_app(settings, service)) as client:
            health = client.get("/v1/health")
            ready = client.get("/v1/readiness")
        self.assertEqual(health.status_code, 200)
        self.assertEqual(health.json()["status"], "ok")
        self.assertEqual(health.json()["storage_health"]["status"], "disabled")
        self.assertEqual(ready.status_code, 200)
        self.assertEqual(ready.json()["schema"], HEALTH_SCHEMA)

        broken = SimpleNamespace(snapshot=lambda: (_ for _ in ()).throw(
            RuntimeError("password=secret /Volumes/T9/private")
        ))
        unavailable_service = SimpleNamespace(
            market_bar_repository=SimpleNamespace(telemetry=broken), close=lambda: None
        )
        with TestClient(create_app(settings, unavailable_service)) as client:
            unavailable = client.get("/v1/readiness")
        self.assertEqual(unavailable.status_code, 503)
        self.assertEqual(unavailable.json()["status"], "unavailable")
        self.assertNotIn("secret", str(unavailable.json()))


if __name__ == "__main__":
    unittest.main()
