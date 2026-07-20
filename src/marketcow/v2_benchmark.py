from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Mapping

from .local_backup import _assert_no_sensitive
from .local_benchmark import (
    OPERATIONS, BenchmarkInputs, BenchmarkPlan, LocalStorageBenchmark, _atomic_json,
)


V2_BENCHMARK_VERSION = "storage-v2.pg-ch-benchmark.v1"
V2_OPERATIONS = OPERATIONS + ("postgres_transaction", "short_soak")
V2_METHODS = {
    "raw_write": "clickhouse_distinct_logical_batches",
    "canonical_rebuild": "clickhouse_final_shared_selection",
    "query_warm": "clickhouse_existing_session",
    "query_cold": "clickhouse_new_connection",
    "page_first": "clickhouse_keyset_first",
    "page_deep": "clickhouse_keyset_tail_explain",
    "archive": "parquet_distinct_partition_export",
    "restore": "verified_pg_ch_bundle_restore",
    "spool_recovery": "authoritative_wal_bounded_replay",
    "concurrent_query": "pg_ch_bounded_concurrency",
    "merge_probe": "clickhouse_system_parts_merges_disks",
    "postgres_transaction": "postgresql_repeatable_read_write_pit",
    "short_soak": "pg_ch_write_query_reconcile_loop",
}
V2_ALLOWED_BACKENDS = frozenset({"postgresql", "clickhouse", "local_spool", "local_backup"})


@dataclass(frozen=True)
class V2BenchmarkInputs:
    root: Path
    plan: BenchmarkPlan
    operations: Mapping[str, Callable[[int], Mapping[str, Any]]]
    verifiers: Mapping[str, Callable[[int, Mapping[str, Any]], Mapping[str, Any]]]
    methods: Mapping[str, str]
    profile: str = "v2-test"
    allowed_root: Path | None = None
    component_versions: Mapping[str, str] | None = None
    clock: Callable[[], float] | None = None


class V2LocalBenchmark:
    """Fail-closed synthetic benchmark for the PG/CH-only online graph."""

    EXTRA_SLO = {
        "postgres_transaction_rows_per_second_min": 200.0,
        "short_soak_iterations_min": 30,
        "short_soak_mismatch_max": 0,
    }

    def __init__(self, inputs: V2BenchmarkInputs) -> None:
        if inputs.profile not in {"v2-development", "v2-test"}:
            raise ValueError("V2 benchmark is v2-development/test-only")
        if set(inputs.operations) != set(V2_OPERATIONS):
            raise ValueError("V2 benchmark operation inventory mismatch")
        if set(inputs.verifiers) != set(V2_OPERATIONS):
            raise ValueError("V2 benchmark verifier inventory mismatch")
        if dict(inputs.methods) != V2_METHODS:
            raise ValueError("V2 benchmark methodology is not the frozen contract")
        self.inputs = inputs
        wrapped = {name: self._verified_operation(name) for name in OPERATIONS}
        kwargs: dict[str, Any] = {}
        if inputs.clock is not None:
            kwargs["clock"] = inputs.clock
        self.base = LocalStorageBenchmark(BenchmarkInputs(
            root=inputs.root, plan=inputs.plan, operations=wrapped,
            profile="test", allowed_root=inputs.allowed_root,
            component_versions=inputs.component_versions, **kwargs,
        ))
        self.report_path = self.base.report_path

    def _verified_operation(self, name: str) -> Callable[[int], Mapping[str, Any]]:
        operation = self.inputs.operations[name]

        def invoke(run: int) -> Mapping[str, Any]:
            result = dict(operation(run))
            backend = result.get("backend")
            if backend not in V2_ALLOWED_BACKENDS or "duckdb" in str(backend).lower():
                raise RuntimeError(f"V2 benchmark {name} used a forbidden backend")
            if result.get("target_read") is not True:
                raise RuntimeError(f"V2 benchmark {name} lacks independent target read")
            verification = dict(self.inputs.verifiers[name](run, result))
            if verification.get("source") != "independent_target_read":
                raise RuntimeError(f"V2 benchmark {name} verifier is not independent")
            result["verification"] = verification
            return result

        return invoke

    def _extra(self, name: str) -> dict[str, Any]:
        rows_runs, durations, checksums = [], [], []
        mismatches = iterations = 0
        for run in range(self.inputs.plan.runs):
            started = self.base.inputs.clock()
            result = dict(self._verified_operation(name)(run))
            duration = max(0.000001, self.base.inputs.clock() - started)
            verification = result.get("verification")
            if not isinstance(verification, Mapping):
                raise ValueError(f"V2 benchmark {name} lacks verification")
            if (verification.get("actual_rows") != verification.get("expected_rows") or
                    verification.get("actual_checksum") !=
                    verification.get("expected_checksum")):
                raise RuntimeError(f"V2 benchmark {name} target verification mismatch")
            rows_runs.append(int(result.get("rows", 0)))
            durations.append(duration)
            checksums.append(str(verification["actual_checksum"]))
            mismatches += int(result.get("mismatches", 0))
            iterations += int(result.get("iterations", 0))
        return {
            "runs": self.inputs.plan.runs, "rows_runs": rows_runs,
            "rows_per_second": round(min(
                rows / duration for rows, duration in zip(rows_runs, durations)
            ), 3),
            "checksums": checksums, "mismatches": mismatches,
            "iterations": iterations,
        }

    def run(self) -> dict[str, Any]:
        report = self.base.run()
        try:
            pg = self._extra("postgres_transaction")
            soak = self._extra("short_soak")
        except Exception:
            _atomic_json(self.report_path, {
                "version": V2_BENCHMARK_VERSION, "status": "failed",
                "runtime": "postgresql_clickhouse_online",
                "failure": "independent_target_verification_failed",
                "methods": dict(V2_METHODS),
            })
            raise
        checks = dict(report["checks"])
        checks.update({
            "postgres_transaction_throughput": pg["rows_per_second"] >=
            self.EXTRA_SLO["postgres_transaction_rows_per_second_min"],
            "short_soak_iterations": soak["iterations"] >=
            self.EXTRA_SLO["short_soak_iterations_min"],
            "short_soak_reconcile": soak["mismatches"] <=
            self.EXTRA_SLO["short_soak_mismatch_max"],
            "pure_pg_ch_runtime": True,
        })
        report.update({
            "version": V2_BENCHMARK_VERSION,
            "status": "passed" if all(checks.values()) else "failed",
            "runtime": "postgresql_clickhouse_online",
            "methods": dict(V2_METHODS),
            "slo": {**report["slo"], **self.EXTRA_SLO},
            "checks": checks,
        })
        report["observations"].update({
            "postgres_transaction": pg, "short_soak": soak,
        })
        _assert_no_sensitive(report, "V2 benchmark report")
        _atomic_json(self.report_path, report)
        if report["status"] != "passed":
            failed = ",".join(sorted(key for key, ok in checks.items() if not ok))
            raise RuntimeError("V2 benchmark SLO failed: " + failed)
        # Ensure the persisted document is the exact complete terminal report.
        if json.loads(self.report_path.read_text(encoding="utf-8")) != report:
            raise RuntimeError("V2 benchmark report publication mismatch")
        return report
