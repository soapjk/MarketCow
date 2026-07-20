from __future__ import annotations

import json
import os
import platform
import re
import resource
import tempfile
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, Mapping

from .local_backup import _assert_no_sensitive, _fsync_dir, _hash, _json
from .telemetry import sanitize_text


BENCHMARK_VERSION = "storage-v2.benchmark.v1"
OPERATIONS = (
    "raw_write", "canonical_rebuild", "query_warm", "query_cold",
    "page_first", "page_deep", "archive", "restore", "spool_recovery",
    "concurrent_query", "merge_probe",
)
MAX_RUNS = 20
MAX_SAMPLE_ROWS = 5_000_000


@dataclass(frozen=True)
class BenchmarkPlan:
    symbols: int
    trading_days: int
    bars_per_day: int = 240
    sources: int = 2
    runs: int = 3
    model_symbols: int = 5500
    model_years: int = 10
    model_trading_days_per_year: int = 250
    max_threads: int = 32
    max_peak_memory_mb: float = 2048.0

    def validate(self) -> None:
        integers = {
            "symbols": (self.symbols, 1, 10000),
            "trading_days": (self.trading_days, 1, 250),
            "bars_per_day": (self.bars_per_day, 1, 1440),
            "sources": (self.sources, 1, 8),
            "runs": (self.runs, 3, MAX_RUNS),
            "model_symbols": (self.model_symbols, 1, 10000),
            "model_years": (self.model_years, 1, 50),
            "model_trading_days_per_year": (
                self.model_trading_days_per_year, 1, 366,
            ),
            "max_threads": (self.max_threads, 1, 256),
        }
        for name, (value, lower, upper) in integers.items():
            if isinstance(value, bool) or not isinstance(value, int) or not lower <= value <= upper:
                raise ValueError(f"benchmark {name} must be between {lower} and {upper}")
        if self.sample_raw_rows > MAX_SAMPLE_ROWS:
            raise ValueError("benchmark sample exceeds bounded row limit")
        if not 64 <= float(self.max_peak_memory_mb) <= 131072:
            raise ValueError("benchmark peak memory limit must be between 64 and 131072 MB")

    @property
    def sample_raw_rows(self) -> int:
        return self.symbols * self.trading_days * self.bars_per_day * self.sources

    @property
    def model_raw_rows(self) -> int:
        return (self.model_symbols * self.model_years *
                self.model_trading_days_per_year * self.bars_per_day * self.sources)


@dataclass(frozen=True)
class BenchmarkInputs:
    root: Path
    plan: BenchmarkPlan
    operations: Mapping[str, Callable[[], Mapping[str, Any]]]
    profile: str = "development"
    allowed_root: Path | None = None
    component_versions: Mapping[str, str] | None = None
    clock: Callable[[], float] = time.perf_counter


def _atomic_json(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(dir=path.parent, prefix=".benchmark-")
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(json.dumps(value, sort_keys=True, indent=2).encode())
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        _fsync_dir(path.parent)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def _percentile(values: list[float], percentile: float) -> float:
    ordered = sorted(values)
    index = max(0, min(len(ordered) - 1, int((len(ordered) - 1) * percentile + .999999)))
    return round(ordered[index], 6)


class LocalStorageBenchmark:
    """Reproducible development-only Storage V2 performance and capacity gate."""

    SLO = {
        "raw_write_rows_per_second_min": 1000.0,
        "canonical_rebuild_rows_per_second_min": 500.0,
        "query_p95_seconds_max": 5.0,
        "query_p99_seconds_max": 8.0,
        "page_deep_first_ratio_max": 5.0,
        "archive_rows_per_second_min": 500.0,
        "restore_rows_per_second_min": 500.0,
        "spool_recovery_rows_per_second_min": 500.0,
        "compression_ratio_max": 0.80,
        "clickhouse_free_ratio_min": 0.30,
        "merge_backlog_max": 100,
    }

    def __init__(self, inputs: BenchmarkInputs) -> None:
        if inputs.profile not in {"development", "test"}:
            raise ValueError("benchmark is development/test-only")
        if inputs.allowed_root is None:
            raise ValueError("benchmark requires an explicit allowed root")
        supplied = Path(inputs.root)
        if supplied.is_symlink():
            raise ValueError("benchmark root must not be a symlink")
        self.root = supplied.resolve()
        allowed = Path(inputs.allowed_root).resolve()
        try:
            self.root.relative_to(allowed)
        except ValueError as error:
            raise ValueError("benchmark root escapes allowed root") from error
        if "production" in str(self.root).lower() or not self.root.name.endswith(
            ("development", "test")
        ):
            raise ValueError("benchmark root must be development/test isolated")
        inputs.plan.validate()
        missing = sorted(set(OPERATIONS) - set(inputs.operations))
        extra = sorted(set(inputs.operations) - set(OPERATIONS))
        if missing or extra:
            raise ValueError(f"benchmark operations mismatch missing={missing} extra={extra}")
        self.inputs = inputs
        self.report_path = self.root / "storage-v2-benchmark.json"

    def _measure(self, name: str) -> Dict[str, Any]:
        durations: list[float] = []
        throughputs: list[float] = []
        checksums: list[str] = []
        last: Dict[str, Any] = {}
        for _ in range(self.inputs.plan.runs):
            started = self.inputs.clock()
            result = dict(self.inputs.operations[name]())
            elapsed = max(0.000001, self.inputs.clock() - started)
            rows = int(result.get("rows", 0))
            if rows < 0:
                raise ValueError(f"benchmark {name} returned negative rows")
            durations.append(elapsed)
            throughputs.append(rows / elapsed)
            logical = result.get("logical", {key: value for key, value in result.items()
                                              if key not in {"query_plan", "query_sql", "free_bytes",
                                                             "total_bytes"}})
            checksums.append(_hash(_json(logical)))
            last = result
        if len(set(checksums)) != 1:
            raise RuntimeError(f"benchmark {name} correctness checksum is unstable")
        plan = str(last.get("query_plan", ""))
        query_sql = str(last.get("query_sql", ""))
        if name in {"page_first", "page_deep"} and (
            not plan or not query_sql or re.search(r"\boffset\b", query_sql, re.I) or
            (name == "page_deep" and "bar_time >" not in query_sql.lower())
        ):
            raise RuntimeError(f"benchmark {name} must prove a no-OFFSET keyset plan")
        return {
            "runs": len(durations), "rows": int(last.get("rows", 0)),
            "latency_seconds": {
                "p50": _percentile(durations, .50),
                "p95": _percentile(durations, .95),
                "p99": _percentile(durations, .99),
            },
            "rows_per_second": round(min(throughputs), 3),
            "checksum": checksums[0],
            "bytes": max(0, int(last.get("bytes", 0))),
            "uncompressed_bytes": max(0, int(last.get("uncompressed_bytes", 0))),
            "query_plan": sanitize_text(plan)[:2000] if plan else None,
            "query_sql": sanitize_text(query_sql)[:2000] if query_sql else None,
            "free_bytes": max(0, int(last.get("free_bytes", 0))),
            "total_bytes": max(0, int(last.get("total_bytes", 0))),
            "merge_backlog": max(0, int(last.get("merge_backlog", 0))),
        }

    def run(self) -> Dict[str, Any]:
        threads_before = threading.active_count()
        memory_before = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        observations = {name: self._measure(name) for name in OPERATIONS}
        memory_after = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        peak_memory_mb = max(memory_before, memory_after) / (1024 * 1024 if platform.system() == "Darwin" else 1024)
        threads_peak = max(threads_before, threading.active_count())
        capacity = self._capacity(observations)
        checks = self._slo_checks(observations, capacity, peak_memory_mb, threads_peak)
        report = {
            "version": BENCHMARK_VERSION,
            "status": "passed" if all(checks.values()) else "failed",
            "plan": {
                "sample_symbols": self.inputs.plan.symbols,
                "sample_trading_days": self.inputs.plan.trading_days,
                "bars_per_day": self.inputs.plan.bars_per_day,
                "sources": self.inputs.plan.sources,
                "sample_raw_rows": self.inputs.plan.sample_raw_rows,
                "model_raw_rows": self.inputs.plan.model_raw_rows,
                "runs": self.inputs.plan.runs,
            },
            "slo": dict(self.SLO), "checks": checks,
            "observations": observations, "capacity": capacity,
            "resources": {"peak_memory_mb": round(peak_memory_mb, 3),
                          "threads_peak": threads_peak},
            "environment": {
                "system": platform.system(), "machine": platform.machine(),
                "python": platform.python_version(),
                "components": dict(self.inputs.component_versions or {}),
            },
            "limitations": (
                "local synthetic distribution; capacity is a linear planning estimate, "
                "not a production throughput promise"
            ),
        }
        _assert_no_sensitive(report, "benchmark report")
        _atomic_json(self.report_path, report)
        if report["status"] != "passed":
            failed = ",".join(sorted(key for key, ok in checks.items() if not ok))
            raise RuntimeError("Storage V2 benchmark SLO failed: " + failed)
        return report

    def _capacity(self, observations: Mapping[str, Mapping[str, Any]]) -> Dict[str, Any]:
        write = observations["raw_write"]
        stored = int(write["bytes"])
        if stored <= 0 or self.inputs.plan.sample_raw_rows <= 0:
            raise ValueError("benchmark requires measured positive storage bytes")
        bytes_per_raw_row = stored / self.inputs.plan.sample_raw_rows
        raw_bytes = bytes_per_raw_row * self.inputs.plan.model_raw_rows
        canonical_bytes = (raw_bytes / self.inputs.plan.sources) * 0.9
        online_bytes = raw_bytes + canonical_bytes
        required_disk = online_bytes / 0.70
        probe = observations["merge_probe"]
        total, free = int(probe["total_bytes"]), int(probe["free_bytes"])
        free_ratio = free / total if total else 0.0
        return {
            "bytes_per_raw_row": round(bytes_per_raw_row, 6),
            "model_online_bytes": int(online_bytes),
            "model_required_disk_bytes_with_30pct_free": int(required_disk),
            "observed_clickhouse_free_ratio": round(free_ratio, 6),
            "reserve_formula": "required_disk = modeled_online_bytes / 0.70",
        }

    def _slo_checks(self, o: Mapping[str, Mapping[str, Any]], capacity: Mapping[str, Any],
                    peak_memory_mb: float, threads_peak: int) -> Dict[str, bool]:
        query_p95 = max(o[name]["latency_seconds"]["p95"] for name in (
            "query_warm", "query_cold", "page_first", "page_deep", "concurrent_query",
        ))
        query_p99 = max(o[name]["latency_seconds"]["p99"] for name in (
            "query_warm", "query_cold", "page_first", "page_deep", "concurrent_query",
        ))
        first = max(.000001, o["page_first"]["latency_seconds"]["p95"])
        archive_uncompressed = o["archive"]["uncompressed_bytes"]
        compression = o["archive"]["bytes"] / archive_uncompressed \
            if archive_uncompressed else 1.0
        return {
            "raw_write_throughput": o["raw_write"]["rows_per_second"] >=
            self.SLO["raw_write_rows_per_second_min"],
            "canonical_rebuild_throughput": o["canonical_rebuild"]["rows_per_second"] >=
            self.SLO["canonical_rebuild_rows_per_second_min"],
            "query_p95": query_p95 <= self.SLO["query_p95_seconds_max"],
            "query_p99": query_p99 <= self.SLO["query_p99_seconds_max"],
            "keyset_page_ratio": o["page_deep"]["latency_seconds"]["p95"] / first <=
            self.SLO["page_deep_first_ratio_max"],
            "archive_throughput": o["archive"]["rows_per_second"] >=
            self.SLO["archive_rows_per_second_min"],
            "restore_throughput": o["restore"]["rows_per_second"] >=
            self.SLO["restore_rows_per_second_min"],
            "spool_recovery_throughput": o["spool_recovery"]["rows_per_second"] >=
            self.SLO["spool_recovery_rows_per_second_min"],
            "compression_ratio": compression <= self.SLO["compression_ratio_max"],
            "clickhouse_free_reserve": capacity["observed_clickhouse_free_ratio"] >=
            self.SLO["clickhouse_free_ratio_min"],
            "merge_backlog": o["merge_probe"]["merge_backlog"] <=
            self.SLO["merge_backlog_max"],
            "memory_bound": peak_memory_mb <= self.inputs.plan.max_peak_memory_mb,
            "thread_bound": threads_peak <= self.inputs.plan.max_threads,
            "no_offset": all(not re.search(r"\boffset\b", o[name]["query_sql"] or "", re.I)
                             for name in ("page_first", "page_deep")),
        }
