from __future__ import annotations

import copy
import re
import threading
from functools import wraps
from types import MethodType
from collections import deque
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Mapping


SCHEMA_VERSION = "storage-v2.telemetry.v1"
MAX_LOG_EVENTS = 200
MAX_TEXT = 1000
_SECRET = re.compile(
    r"(?i)(password|passwd|token|secret|api[_-]?key|authorization)(\s*[=:]\s*)([^\s,;]+)"
)
_DSN = re.compile(r"(?i)\b(postgresql|postgres|clickhouse|https?)://[^\s]+")
_ABSOLUTE_PATH = re.compile(r"(?<![\w.-])/(?:Volumes|Users|home|var|tmp|private)/[^\s,;]+")


METRICS: Dict[str, Dict[str, Any]] = {
    "ingest_write_latency_seconds": {
        "type": "histogram", "unit": "seconds", "labels": {
            "backend": ("duckdb", "clickhouse"), "outcome": ("ok", "error", "spooled"),
        }, "buckets": (0.005, 0.025, 0.1, 0.5, 2.0, 8.0, 30.0),
    },
    "wal_items": {"type": "gauge", "unit": "items", "labels": {
        "state": ("pending", "failed", "replayed", "quarantine"),
    }},
    "canonical_queue_items": {"type": "gauge", "unit": "items", "labels": {
        "state": ("pending", "processing", "failed"),
    }},
    "canonical_lag_seconds": {
        "type": "histogram", "unit": "seconds", "labels": {},
        "buckets": (1.0, 5.0, 30.0, 300.0, 1800.0, 7200.0),
    },
    "canonical_rebuild_total": {"type": "counter", "unit": "operations", "labels": {
        "outcome": ("ok", "error", "spooled", "truncated"),
    }},
    "contract_mismatch_total": {"type": "counter", "unit": "mismatches", "labels": {
        "contract": ("recent", "range", "canonical_page", "cross_section", "matrix",
                     "raw_range", "raw_page", "as_of"),
    }},
    "query_latency_seconds": {
        "type": "histogram", "unit": "seconds", "labels": {
            "backend": ("duckdb", "clickhouse_canonical", "clickhouse_raw"),
            "query": ("recent", "range", "page", "cross_section", "matrix", "raw", "as_of"),
            "outcome": ("ok", "empty", "error", "fallback"),
        }, "buckets": (0.001, 0.005, 0.025, 0.1, 0.5, 2.0, 8.0),
    },
    "backend_fallback_total": {"type": "counter", "unit": "operations", "labels": {
        "from_backend": ("clickhouse_canonical", "clickhouse_raw"),
        "to_backend": ("duckdb",),
        "query": ("recent", "range", "page", "cross_section", "matrix", "raw", "as_of"),
    }},
    "cache_age_seconds": {"type": "histogram", "unit": "seconds", "labels": {
        "status": ("fresh", "stale", "empty", "miss"),
    }, "buckets": (0.0, 1.0, 30.0, 300.0, 900.0, 3600.0, 86400.0)},
    "clickhouse_pressure": {"type": "gauge", "unit": "ratio_or_items", "labels": {
        "kind": ("merge_queue", "disk_used_ratio"),
    }},
}


def sanitize_text(value: Any) -> str:
    text = str(value)
    text = _SECRET.sub(lambda match: match.group(1) + match.group(2) + "[REDACTED]", text)
    text = _DSN.sub("[REDACTED_DSN]", text)
    text = _ABSOLUTE_PATH.sub("[REDACTED_PATH]", text)
    return text[:MAX_TEXT]


def _sanitize(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {
            sanitize_text(key)[:64]: _sanitize(item)
            for key, item in list(value.items())[:32]
        }
    if isinstance(value, (list, tuple)):
        return [_sanitize(item) for item in value[:32]]
    if isinstance(value, str) or isinstance(value, BaseException):
        return sanitize_text(value)
    if value is None or isinstance(value, (bool, int, float)):
        return value
    return sanitize_text(value)


_QUERY_METHODS = {
    "get_price_bars": "recent",
    "get_price_bars_range": "range",
    "get_price_bars_page": "page",
    "get_price_bars_cross_section": "cross_section",
    "get_price_bars_cross_section_page": "cross_section",
    "get_price_bars_matrix_page": "matrix",
    "get_raw_price_bars_range": "raw",
    "get_raw_price_bars_page": "raw",
    "get_price_bar_as_of": "as_of",
    "get_price_bars_as_of_page": "as_of",
    "get_latest_quotes": "recent",
}


def instrument_duckdb_market_bars(repository: Any, telemetry: "Telemetry") -> Any:
    """Attach bounded process-local telemetry without changing repository identity."""
    if getattr(repository, "_marketcow_telemetry_instrumented", False):
        return repository
    repository.telemetry = telemetry
    for name, query in _QUERY_METHODS.items():
        original = getattr(repository, name)

        @wraps(original)
        def measured_query(self: Any, *args: Any, _original: Any = original,
                           _query: str = query, **kwargs: Any) -> Any:
            started = telemetry.clock()
            try:
                result = _original(*args, **kwargs)
            except Exception:
                telemetry.safe(
                    "histogram", "query_latency_seconds",
                    max(0.0, telemetry.clock() - started),
                    backend="duckdb", query=_query, outcome="error",
                )
                raise
            rows = result[0] if isinstance(result, tuple) else result
            outcome = "empty" if rows is None or rows == [] else "ok"
            telemetry.safe(
                "histogram", "query_latency_seconds",
                max(0.0, telemetry.clock() - started),
                backend="duckdb", query=_query, outcome=outcome,
            )
            return result

        setattr(repository, name, MethodType(measured_query, repository))
    for name in ("upsert_quote", "upsert_price_bars"):
        original = getattr(repository, name)

        @wraps(original)
        def measured_write(self: Any, *args: Any, _original: Any = original,
                           **kwargs: Any) -> Any:
            started = telemetry.clock()
            try:
                result = _original(*args, **kwargs)
            except Exception:
                telemetry.safe(
                    "histogram", "ingest_write_latency_seconds",
                    max(0.0, telemetry.clock() - started),
                    backend="duckdb", outcome="error",
                )
                raise
            telemetry.safe(
                "histogram", "ingest_write_latency_seconds",
                max(0.0, telemetry.clock() - started),
                backend="duckdb", outcome="ok",
            )
            return result

        setattr(repository, name, MethodType(measured_write, repository))
    repository._marketcow_telemetry_instrumented = True
    return repository


class Telemetry:
    """Process-local, bounded telemetry. State resets on process restart."""

    def __init__(self, clock: Callable[[], float] | None = None,
                 wall_clock: Callable[[], datetime] | None = None,
                 clickhouse_enabled: bool = False) -> None:
        self.clock = clock or __import__("time").monotonic
        self.wall_clock = wall_clock or (lambda: datetime.now(timezone.utc))
        self.clickhouse_enabled = bool(clickhouse_enabled)
        self._lock = threading.RLock()
        self._values: Dict[tuple[str, tuple[tuple[str, str], ...]], Any] = {}
        self._logs: deque[Dict[str, Any]] = deque(maxlen=MAX_LOG_EVENTS)
        self._dropped = 0

    @staticmethod
    def _labels(name: str, labels: Mapping[str, str]) -> tuple[tuple[str, str], ...]:
        spec = METRICS.get(name)
        if not spec:
            raise ValueError("unknown telemetry metric")
        expected = spec["labels"]
        if set(labels) != set(expected):
            raise ValueError("telemetry labels do not match metric schema")
        normalized = []
        for key in sorted(expected):
            value = str(labels[key]).strip().lower()
            if value not in expected[key]:
                raise ValueError("telemetry label value is not allowed")
            normalized.append((key, value))
        return tuple(normalized)

    def counter(self, name: str, amount: int = 1, **labels: str) -> None:
        if METRICS.get(name, {}).get("type") != "counter" or not 0 <= amount <= 1000000:
            raise ValueError("invalid counter update")
        key = (name, self._labels(name, labels))
        with self._lock:
            self._values[key] = min(int(self._values.get(key, 0)) + amount, 2 ** 63 - 1)

    def gauge(self, name: str, value: float, **labels: str) -> None:
        if METRICS.get(name, {}).get("type") != "gauge" or not 0 <= value <= 1e15:
            raise ValueError("invalid gauge update")
        key = (name, self._labels(name, labels))
        with self._lock:
            self._values[key] = float(value)

    def histogram(self, name: str, value: float, **labels: str) -> None:
        spec = METRICS.get(name, {})
        if spec.get("type") != "histogram" or not 0 <= value <= 31536000:
            raise ValueError("invalid histogram observation")
        key = (name, self._labels(name, labels))
        with self._lock:
            state = self._values.setdefault(key, {
                "count": 0, "sum": 0.0, "buckets": [0] * len(spec["buckets"]),
            })
            state["count"] += 1
            state["sum"] += float(value)
            for index, boundary in enumerate(spec["buckets"]):
                if value <= boundary:
                    state["buckets"][index] += 1

    def safe(self, method: str, *args: Any, **kwargs: Any) -> None:
        try:
            getattr(self, method)(*args, **kwargs)
        except Exception:
            with self._lock:
                self._dropped = min(self._dropped + 1, 2 ** 63 - 1)

    def log(self, event: str, severity: str = "info", **fields: Any) -> None:
        if event not in {"ingest", "wal", "canonical", "contract", "query", "cache",
                         "clickhouse_pressure", "telemetry"}:
            raise ValueError("unknown telemetry event")
        if severity not in {"debug", "info", "warning", "error"}:
            raise ValueError("unknown telemetry severity")
        record = {"at": self.wall_clock().astimezone(timezone.utc).isoformat(),
                  "event": event, "severity": severity, "fields": _sanitize(fields)}
        with self._lock:
            self._logs.append(record)

    def clickhouse_pressure(self, merge_queue: int, disk_used_ratio: float) -> None:
        if not self.clickhouse_enabled:
            return
        self.gauge("clickhouse_pressure", merge_queue, kind="merge_queue")
        if not 0 <= disk_used_ratio <= 1:
            raise ValueError("disk used ratio must be between zero and one")
        self.gauge("clickhouse_pressure", disk_used_ratio, kind="disk_used_ratio")

    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            values = copy.deepcopy(self._values)
            logs = copy.deepcopy(list(self._logs))
            dropped = self._dropped
        metrics = []
        for (name, labels), value in sorted(values.items()):
            spec = METRICS[name]
            item = {"name": name, "type": spec["type"], "unit": spec["unit"],
                    "labels": dict(labels), "value": value}
            if spec["type"] == "histogram":
                item["buckets"] = list(spec["buckets"])
            metrics.append(item)
        return {
            "schema": SCHEMA_VERSION,
            "generated_at": self.wall_clock().astimezone(timezone.utc).isoformat(),
            "restart_semantics": "process_local_reset",
            "clickhouse": {"enabled": self.clickhouse_enabled},
            "metrics": metrics, "logs": logs, "dropped_updates": dropped,
            "limits": {"log_events": MAX_LOG_EVENTS, "text_chars": MAX_TEXT,
                       "metric_series": sum(
                           max(1, __import__("math").prod(len(values) for values in spec["labels"].values()))
                           for spec in METRICS.values()
                       )},
        }
