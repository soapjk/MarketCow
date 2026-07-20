from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from marketcow.local_benchmark import BenchmarkPlan
from marketcow.v2_benchmark import (
    V2_BENCHMARK_VERSION, V2_METHODS, V2_OPERATIONS, V2BenchmarkInputs,
    V2LocalBenchmark,
)


class Clock:
    def __init__(self):
        self.value = 0.0

    def __call__(self):
        self.value += .01
        return self.value


class V2BenchmarkTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.allowed = Path(self.temporary.name)
        self.root = self.allowed / "pg-ch-benchmark-test"
        self.plan = BenchmarkPlan(1, 20, 240, 2, 3, max_peak_memory_mb=8192)

    def tearDown(self):
        self.temporary.cleanup()

    def operations(self):
        rows = self.plan.sample_raw_rows
        operations = {}
        for name in V2_OPERATIONS:
            backend = "postgresql" if name == "postgres_transaction" else "clickhouse"
            if name in {"restore", "archive"}:
                backend = "local_backup"
            elif name == "spool_recovery":
                backend = "local_spool"
            result = {
                "rows": rows, "backend": backend, "target_read": True,
                "verification": {"expected_rows": rows, "actual_rows": rows,
                                 "expected_checksum": name, "actual_checksum": name},
            }
            if name == "raw_write":
                result["bytes"] = rows * 24
            if name == "archive":
                result.update(bytes=rows * 12, uncompressed_bytes=rows * 40)
            if name == "query_warm":
                result["path_kind"] = "warm_existing_session"
            if name == "query_cold":
                result["path_kind"] = "new_connection"
            if name in {"page_first", "page_deep"}:
                result.update({
                    "query_plan": "ReadFromMergeTree Filter bar_time > cursor",
                    "query_sql": "SELECT bars ORDER BY bar_time LIMIT 101",
                    "cursor_depth": 0,
                })
                if name == "page_deep":
                    result.update({
                        "query_sql": "SELECT bars WHERE bar_time > 'tail' LIMIT 101",
                        "cursor_depth": 4600, "query_after": 123,
                        "explain_after": 123, "depth_after": 123,
                        "cursor_predicate": "tail",
                    })
            if name == "merge_probe":
                result.update(total_bytes=100, free_bytes=40, merge_backlog=1)
            if name == "short_soak":
                result.update(iterations=10, mismatches=0)
            operations[name] = lambda _run, result=result: dict(result)
        return operations

    def benchmark(self, **changes):
        values = dict(
            root=self.root, plan=self.plan, operations=self.operations(),
            verifiers=self.verifiers(), methods=V2_METHODS, profile="v2-test",
            allowed_root=self.allowed,
            component_versions={"postgresql": "16", "clickhouse": "25.8"},
            clock=Clock(),
        )
        values.update(changes)
        return V2LocalBenchmark(V2BenchmarkInputs(**values))

    def verifiers(self):
        def verifier(name):
            return lambda _run, result: {
                "source": "independent_target_read",
                "expected_rows": result["rows"], "actual_rows": result["rows"],
                "expected_checksum": name, "actual_checksum": name,
            }
        return {name: verifier(name) for name in V2_OPERATIONS}

    def test_complete_pg_ch_report_and_capacity_gate(self):
        report = self.benchmark().run()
        self.assertEqual(report["version"], V2_BENCHMARK_VERSION)
        self.assertEqual(report["runtime"], "postgresql_clickhouse_online")
        self.assertTrue(all(report["checks"].values()))
        self.assertEqual(set(report["observations"]), set(V2_OPERATIONS))
        self.assertEqual(report["methods"], V2_METHODS)

    def test_forbidden_backend_missing_read_and_method_tamper_fail_closed(self):
        operations = self.operations()
        operations["query_warm"] = lambda _run: {
            "backend": "duckdb", "target_read": True, "rows": 1,
            "verification": {"expected_rows": 1, "actual_rows": 1,
                             "expected_checksum": "x", "actual_checksum": "x"},
        }
        with self.assertRaisesRegex(RuntimeError, "forbidden backend"):
            self.benchmark(operations=operations).run()
        operations = self.operations()
        value = operations["raw_write"](0)
        value["target_read"] = False
        operations["raw_write"] = lambda _run: value
        with self.assertRaisesRegex(RuntimeError, "independent target read"):
            self.benchmark(operations=operations).run()
        with self.assertRaisesRegex(ValueError, "methodology"):
            self.benchmark(methods={**V2_METHODS, "query_warm": "fake"})

    def test_pg_soak_checksum_and_bound_failures_are_terminal(self):
        verifiers = {
            name: (lambda current: lambda _run, result: {
                "source": "independent_target_read",
                "expected_rows": result["rows"], "actual_rows": result["rows"],
                "expected_checksum": current, "actual_checksum": current,
            })(name)
            for name in V2_OPERATIONS
        }
        verifiers["postgres_transaction"] = lambda _run, result: {
            "source": "independent_target_read",
            "expected_rows": result["rows"], "actual_rows": result["rows"],
            "expected_checksum": "expected", "actual_checksum": "wrong",
        }
        with self.assertRaisesRegex(RuntimeError, "target verification mismatch"):
            benchmark = self.benchmark(verifiers=verifiers)
            benchmark.run()
        terminal = json.loads(benchmark.report_path.read_text())
        self.assertEqual(terminal["version"], V2_BENCHMARK_VERSION)
        self.assertEqual(terminal["status"], "failed")
        operations = self.operations()
        short = operations["short_soak"](0)
        short.update(iterations=0, mismatches=1)
        operations["short_soak"] = lambda _run: short
        with self.assertRaisesRegex(RuntimeError, "SLO failed"):
            self.benchmark(operations=operations).run()

    def test_inventory_profile_and_offset_boundaries(self):
        operations = self.operations()
        operations.pop("short_soak")
        with self.assertRaisesRegex(ValueError, "inventory"):
            self.benchmark(operations=operations)
        with self.assertRaisesRegex(ValueError, "v2-development/test-only"):
            self.benchmark(profile="development")
        operations = self.operations()
        deep = operations["page_deep"](0)
        deep["query_sql"] += " OFFSET 4600"
        operations["page_deep"] = lambda _run: deep
        with self.assertRaisesRegex(RuntimeError, "no-OFFSET"):
            self.benchmark(operations=operations).run()

    def test_prior_pass_is_overwritten_for_every_failure_boundary(self):
        benchmark = self.benchmark()
        benchmark.run()
        verifiers = self.verifiers()
        verifiers["raw_write"] = lambda _run, result: {
            "source": "independent_target_read",
            "expected_rows": result["rows"], "actual_rows": result["rows"],
            "expected_checksum": "expected", "actual_checksum": "wrong",
        }
        with self.assertRaisesRegex(RuntimeError, "target verification mismatch"):
            self.benchmark(verifiers=verifiers).run()
        failed = json.loads(benchmark.report_path.read_text())
        self.assertEqual((failed["version"], failed["status"]),
                         (V2_BENCHMARK_VERSION, "failed"))
        self.assertNotIn("observations", failed)
        self.assertNotIn("checks", failed)

        self.benchmark().run()
        operations = self.operations()
        operations["raw_write"] = lambda _run: (_ for _ in ()).throw(
            RuntimeError("operation failed")
        )
        with self.assertRaisesRegex(RuntimeError, "operation failed"):
            self.benchmark(operations=operations).run()
        self.assertEqual(json.loads(benchmark.report_path.read_text())["status"], "failed")

        self.benchmark().run()
        operations = self.operations()
        slow = operations["merge_probe"](0)
        slow.update(total_bytes=100, free_bytes=1, merge_backlog=1000)
        operations["merge_probe"] = lambda _run: slow
        with self.assertRaisesRegex(RuntimeError, "SLO failed"):
            self.benchmark(operations=operations).run()
        self.assertEqual(json.loads(benchmark.report_path.read_text())["status"], "failed")

    def test_publication_failure_removes_prior_pass_or_writes_failed_terminal(self):
        benchmark = self.benchmark()
        benchmark.run()
        from marketcow import v2_benchmark

        original = v2_benchmark._atomic_json
        calls = {"count": 0}

        def fail_final_once(path, value):
            calls["count"] += 1
            # Base uses its own publisher; final V2 publication fails, terminal succeeds.
            if calls["count"] == 1:
                raise OSError("publication denied")
            return original(path, value)

        with patch.object(v2_benchmark, "_atomic_json", side_effect=fail_final_once):
            with self.assertRaisesRegex(RuntimeError, "publication denied"):
                self.benchmark().run()
        terminal = json.loads(benchmark.report_path.read_text())
        self.assertEqual((terminal["version"], terminal["status"]),
                         (V2_BENCHMARK_VERSION, "failed"))
        self.assertNotIn("observations", terminal)

        self.benchmark().run()
        with patch.object(v2_benchmark, "_atomic_json", side_effect=OSError("disk gone")):
            with self.assertRaisesRegex(RuntimeError, "terminal publication failed"):
                self.benchmark().run()
        self.assertFalse(benchmark.report_path.exists())


if __name__ == "__main__":
    unittest.main()
