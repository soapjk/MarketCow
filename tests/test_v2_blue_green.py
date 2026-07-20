import json
import shutil
import tempfile
import unittest
from pathlib import Path

from marketcow.offline_copy_validator import COPY_VALIDATION_VERSION
from marketcow.offline_incremental_catchup import CATCHUP_VERSION
from marketcow.v2_backup_restore import V2_RESTORE_VERSION
from marketcow.v2_benchmark import V2_BENCHMARK_VERSION
from marketcow.v2_benchmark import V2_METHODS
from marketcow.v2_blue_green import (
    BLUE_GREEN_VERSION, GOLDEN_CHECKS, STOP_CONDITIONS, BlueGreenError,
    V2BlueGreenDrill,
)


class Router:
    def __init__(self):
        self._target = "blue"
        self.history = []

    @property
    def target(self):
        return self._target

    def switch(self, target):
        if target not in {"blue", "green"}:
            raise ValueError("bad target")
        self._target = target
        self.history.append(target)


class BlueGreenTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.evidence_root = self.root / "evidence"
        self.evidence_root.mkdir()
        fingerprint = "a" * 64
        copy = {"version": COPY_VALIDATION_VERSION, "status": "verified"}
        copy["report_payload_sha256"] = self.digest(copy)
        documents = {
            "copy": copy,
            "catchup": {
                "version": CATCHUP_VERSION, "status": "complete", "lag": 0,
                "stability": [fingerprint] * 3,
                "source_high_watermark": {"source_fingerprint": fingerprint},
            },
            "restore": {
                "report_version": V2_RESTORE_VERSION, "status": "complete",
                "verification": {"repositories": "ok", "api": "ok"},
            },
            "benchmark": {
                "version": V2_BENCHMARK_VERSION, "status": "passed",
                "checks": {"all": True}, "methods": V2_METHODS,
            },
        }
        self.paths = {}
        for name, value in documents.items():
            path = self.evidence_root / f"{name}.json"
            path.write_text(json.dumps(value))
            self.paths[name] = path
        self.router = Router()
        self.gate_value = dict(STOP_CONDITIONS)
        self.golden_value = {name: {
            "status": "ok", "requests": 1,
            "blue_capture_sha256": "1" * 64, "green_capture_sha256": "2" * 64,
        } for name in GOLDEN_CHECKS}
        self.increments = 0
        self.reconciles = 0

    def tearDown(self):
        self.temp.cleanup()

    @staticmethod
    def digest(value):
        import hashlib

        raw = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()
        return hashlib.sha256(raw).hexdigest()

    def drill(self, fault=None):
        def increment():
            self.increments += 1
            return "b" * 64

        def reconcile(fingerprint):
            self.reconciles += 1
            return {**STOP_CONDITIONS, "source_fingerprint": fingerprint}

        return V2BlueGreenDrill(
            self.root / "drill", self.root, self.router, self.paths,
            lambda: dict(self.gate_value), lambda _target: dict(self.golden_value),
            increment, reconcile, observations=2, fault_hook=fault,
        )

    def test_switch_and_restart_are_whole_target_and_idempotent(self):
        drill = self.drill()
        report = drill.run()
        self.assertEqual((report["status"], report["final_target"]), ("passed", "green"))
        first_history = list(self.router.history)
        second = drill.run()
        self.assertEqual(second["attempt"], 1)
        self.assertEqual(self.router.history, first_history)
        self.assertNotIn("duckdb", json.dumps(second).lower())

    def test_each_stop_condition_rolls_back_and_requires_reconcile(self):
        for name in STOP_CONDITIONS:
            with self.subTest(name=name):
                shutil.rmtree(self.root / "drill", ignore_errors=True)
                self.router = Router()
                self.gate_value = dict(STOP_CONDITIONS)
                drill = self.drill()
                calls = 0

                def gate():
                    nonlocal calls
                    calls += 1
                    value = dict(STOP_CONDITIONS)
                    if calls >= 2:
                        value[name] = "bad"
                    return value

                drill.gate = gate
                with self.assertRaises(BlueGreenError):
                    drill.run()
                self.assertEqual(self.router.target, "blue")
                self.assertEqual(self.increments, 1)
                checkpoint = json.loads(drill.checkpoint_path.read_text())
                self.assertEqual(checkpoint["phase"], "reconcile_required")
                self.increments = 0

    def test_rollback_increment_catchup_then_regreen(self):
        drill = self.drill()
        calls = 0

        def golden(_target):
            nonlocal calls
            calls += 1
            value = dict(self.golden_value)
            if calls == 3:
                value["history_cursor"] = {**value["history_cursor"], "status": "mismatch"}
            return value

        drill.golden = golden
        with self.assertRaises(BlueGreenError):
            drill.run()
        self.assertEqual((self.router.target, self.increments), ("blue", 1))
        drill.golden = lambda _target: dict(self.golden_value)
        report = drill.run()
        self.assertEqual((report["final_target"], self.reconciles), ("green", 1))
        drill.run()
        self.assertEqual((self.increments, self.reconciles), (1, 1))

    def test_crash_after_route_recovers_whole_service(self):
        def fault(point):
            if point == "after_route_switch":
                raise RuntimeError("simulated crash")

        drill = self.drill(fault)
        with self.assertRaises(BlueGreenError):
            drill.run()
        self.assertEqual(self.router.target, "blue")
        self.assertEqual(self.increments, 1)

    def test_explicit_rollback_is_whole_and_idempotent(self):
        drill = self.drill()
        drill.run()
        first = drill.rollback()
        history = list(self.router.history)
        second = drill.rollback()
        self.assertEqual((first["final_target"], second["final_target"]), ("blue", "blue"))
        self.assertEqual(self.increments, 1)
        self.assertEqual(self.router.history, history)

    def test_evidence_tamper_and_symlink_fail_before_state_or_switch(self):
        self.paths["benchmark"].write_text(json.dumps({
            "version": V2_BENCHMARK_VERSION, "status": "passed", "checks": {"x": False},
            "methods": V2_METHODS,
        }))
        drill = self.drill()
        with self.assertRaises(BlueGreenError):
            drill.run()
        self.assertFalse(drill.state_root.exists())
        self.assertEqual(self.router.history, [])

        target = self.evidence_root / "target.json"
        target.write_text("{}")
        link = self.evidence_root / "link.json"
        link.symlink_to(target)
        self.paths["benchmark"] = link
        with self.assertRaises(BlueGreenError):
            self.drill().run()

    def test_checkpoint_tamper_and_bounded_redacted_audit(self):
        drill = self.drill()
        report = drill.run()
        self.assertEqual(report["version"], BLUE_GREEN_VERSION)
        self.assertLessEqual(len(report["audit"]), 128)
        checkpoint = json.loads(drill.checkpoint_path.read_text())
        checkpoint["attempt"] = 999
        drill.checkpoint_path.write_text(json.dumps(checkpoint))
        with self.assertRaises(BlueGreenError):
            drill.run()


if __name__ == "__main__":
    unittest.main()
