from __future__ import annotations

import threading
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Mapping

from .telemetry import sanitize_text


HEALTH_SCHEMA = "storage-v2.health.v1"
V2_HEALTH_SCHEMA = "marketcow.v2.health.v1"
THRESHOLDS = {
    "degrade_after_seconds": 30.0,
    "unavailable_after_seconds": 10.0,
    "recover_after_seconds": 60.0,
    "disk_degraded_ratio": 0.85,
    "disk_unavailable_ratio": 0.95,
    "merge_degraded_items": 50.0,
    "merge_unavailable_items": 200.0,
    "wal_failed_items": 1.0,
    "wal_quarantine_unavailable_items": 10.0,
}
MAX_REASONS = 8
MAX_REASON_CHARS = 240

V2_THRESHOLDS = {
    "degrade_after_seconds": 30.0,
    "unavailable_after_seconds": 10.0,
    "recover_after_seconds": 60.0,
    "wal_pending_degraded_items": 100.0,
    "wal_failed_degraded_items": 1.0,
    "wal_quarantine_unavailable_items": 1.0,
    "wal_lag_degraded_seconds": 300.0,
    "canonical_pending_degraded_items": 100.0,
    "canonical_failed_unavailable_items": 1.0,
    "canonical_lag_degraded_seconds": 300.0,
    "disk_degraded_ratio": 0.85,
    "disk_unavailable_ratio": 0.95,
    "merge_degraded_items": 50.0,
    "merge_unavailable_items": 200.0,
}


def _series(snapshot: Mapping[str, Any], name: str, **labels: str) -> Any:
    for item in snapshot.get("metrics", []):
        if item.get("name") == name and item.get("labels") == labels:
            return item.get("value")
    return None


class StorageHealthEvaluator:
    """Thread-safe local health state with bounded hysteresis and no I/O."""

    def __init__(
        self, clock: Callable[[], float] | None = None,
        wall_clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.clock = clock or __import__("time").monotonic
        self.wall_clock = wall_clock or (lambda: datetime.now(timezone.utc))
        self._lock = threading.RLock()
        self._state = "healthy"
        self._candidate: str | None = None
        self._candidate_since: float | None = None

    @staticmethod
    def _bounded_reasons(reasons: list[str]) -> list[str]:
        return [sanitize_text(reason)[:MAX_REASON_CHARS] for reason in reasons[:MAX_REASONS]]

    def _raw_state(self, snapshot: Any) -> tuple[str, list[str]]:
        if not isinstance(snapshot, Mapping) or snapshot.get("schema") != "storage-v2.telemetry.v1":
            return "unavailable", ["telemetry_snapshot_unavailable"]
        clickhouse = snapshot.get("clickhouse")
        if not isinstance(clickhouse, Mapping) or not clickhouse.get("enabled"):
            return "disabled", ["clickhouse_disabled"]
        merge = _series(snapshot, "clickhouse_pressure", kind="merge_queue")
        disk = _series(snapshot, "clickhouse_pressure", kind="disk_used_ratio")
        if merge is None or disk is None:
            return "degraded", ["clickhouse_pressure_metrics_missing"]
        failed = _series(snapshot, "wal_items", state="failed") or 0
        quarantine = _series(snapshot, "wal_items", state="quarantine") or 0
        reasons: list[str] = []
        unavailable = False
        if float(disk) >= THRESHOLDS["disk_unavailable_ratio"]:
            unavailable, reasons = True, reasons + ["clickhouse_disk_pressure_critical"]
        elif float(disk) >= THRESHOLDS["disk_degraded_ratio"]:
            reasons.append("clickhouse_disk_pressure_high")
        if float(merge) >= THRESHOLDS["merge_unavailable_items"]:
            unavailable, reasons = True, reasons + ["clickhouse_merge_queue_critical"]
        elif float(merge) >= THRESHOLDS["merge_degraded_items"]:
            reasons.append("clickhouse_merge_queue_high")
        if float(quarantine) >= THRESHOLDS["wal_quarantine_unavailable_items"]:
            unavailable, reasons = True, reasons + ["wal_quarantine_critical"]
        elif float(quarantine) > 0:
            reasons.append("wal_quarantine_present")
        if float(failed) >= THRESHOLDS["wal_failed_items"]:
            reasons.append("wal_failed_present")
        dropped = snapshot.get("dropped_updates", 0)
        if isinstance(dropped, (int, float)) and dropped > 0:
            reasons.append("telemetry_updates_dropped")
        return ("unavailable" if unavailable else "degraded" if reasons else "healthy",
                reasons)

    def evaluate(self, snapshot: Any) -> Dict[str, Any]:
        try:
            now = float(self.clock())
        except Exception:
            now = 0.0
            snapshot = None
        try:
            raw, reasons = self._raw_state(snapshot)
        except Exception:
            raw, reasons = "unavailable", ["health_evaluation_failed"]
        with self._lock:
            if raw == "disabled":
                self._state, self._candidate, self._candidate_since = raw, None, None
            elif self._state == "disabled":
                self._state, self._candidate, self._candidate_since = raw, None, None
            elif raw == "unavailable" and reasons == ["telemetry_snapshot_unavailable"]:
                self._state, self._candidate, self._candidate_since = raw, None, None
            elif raw == "degraded" and reasons == ["clickhouse_pressure_metrics_missing"]:
                self._state, self._candidate, self._candidate_since = raw, None, None
            elif raw == self._state:
                self._candidate, self._candidate_since = None, None
            else:
                if self._candidate != raw:
                    self._candidate, self._candidate_since = raw, now
                threshold = (THRESHOLDS["recover_after_seconds"] if raw == "healthy"
                             or self._state == "unavailable" and raw == "degraded"
                             else THRESHOLDS["unavailable_after_seconds"]
                             if raw == "unavailable" else THRESHOLDS["degrade_after_seconds"])
                elapsed = max(0.0, now - float(self._candidate_since))
                if elapsed >= threshold:
                    self._state, self._candidate, self._candidate_since = raw, None, None
                else:
                    reasons.append(f"transition_pending:{raw}:{threshold - elapsed:.3f}s")
            state = self._state
            candidate = self._candidate
            since = self._candidate_since
        try:
            observed = self.wall_clock().astimezone(timezone.utc).isoformat()
        except Exception:
            observed = None
        clickhouse_enabled = (
            isinstance(snapshot, Mapping)
            and isinstance(snapshot.get("clickhouse"), Mapping)
            and bool(snapshot["clickhouse"].get("enabled"))
        )
        return {
            "schema": HEALTH_SCHEMA,
            "status": state,
            "ready": state in {"disabled", "healthy", "degraded"},
            "backend": "clickhouse" if clickhouse_enabled else (
                "duckdb" if state == "disabled" else "unknown"
            ),
            "observed_at": observed,
            "candidate_status": candidate,
            "candidate_since_monotonic": since,
            "reasons": self._bounded_reasons(reasons),
            "thresholds": dict(THRESHOLDS),
            "window": {"kind": "sustained_condition", "process_local": True},
        }


class V2HealthEvaluator:
    """Deterministic PG/CH/WAL/canonical health with sustained transitions."""

    def __init__(self, clock: Callable[[], float] | None = None,
                 wall_clock: Callable[[], datetime] | None = None) -> None:
        self.clock = clock or __import__("time").monotonic
        self.wall_clock = wall_clock or (lambda: datetime.now(timezone.utc))
        self._lock = threading.RLock()
        self._state = "healthy"
        self._candidate: str | None = None
        self._candidate_since: float | None = None

    @staticmethod
    def _component(snapshot: Mapping[str, Any], name: str) -> Mapping[str, Any]:
        value = snapshot.get("components", {}).get(name, {})
        return value if isinstance(value, Mapping) else {}

    def _raw_state(self, snapshot: Any) -> tuple[str, list[str]]:
        if not isinstance(snapshot, Mapping) or snapshot.get("schema") != V2_HEALTH_SCHEMA:
            return "unavailable", ["v2_dependency_snapshot_unavailable"]
        reasons: list[str] = []
        unavailable = False
        pg = self._component(snapshot, "postgresql")
        main = self._component(snapshot, "clickhouse_main")
        if pg.get("status") != "healthy":
            unavailable, reasons = True, reasons + ["postgresql_unavailable"]
        if main.get("status") != "healthy":
            unavailable, reasons = True, reasons + ["clickhouse_main_unavailable"]
        wal = self._component(snapshot, "authoritative_wal")
        if wal.get("status") == "unavailable":
            unavailable, reasons = True, reasons + ["wal_diagnostics_unavailable"]
        else:
            pending = float(wal.get("pending", 0))
            failed = float(wal.get("failed", 0))
            quarantine = float(wal.get("quarantine", 0))
            lag = float(wal.get("oldest_pending_lag_seconds", 0))
            disk = float(wal.get("disk_used_ratio", 0))
            if quarantine >= V2_THRESHOLDS["wal_quarantine_unavailable_items"]:
                unavailable, reasons = True, reasons + ["wal_quarantine_present"]
            if failed >= V2_THRESHOLDS["wal_failed_degraded_items"]:
                reasons.append("wal_failed_present")
            if pending >= V2_THRESHOLDS["wal_pending_degraded_items"]:
                reasons.append("wal_backlog_high")
            if lag >= V2_THRESHOLDS["wal_lag_degraded_seconds"]:
                reasons.append("wal_lag_high")
            if disk >= V2_THRESHOLDS["disk_unavailable_ratio"]:
                unavailable, reasons = True, reasons + ["wal_disk_pressure_critical"]
            elif disk >= V2_THRESHOLDS["disk_degraded_ratio"]:
                reasons.append("wal_disk_pressure_high")
            if wal.get("truncated"):
                unavailable, reasons = True, reasons + ["wal_diagnostics_truncated"]
        scheduler = self._component(snapshot, "canonical_scheduler")
        if scheduler.get("status") != "disabled":
            worker = self._component(snapshot, "clickhouse_scheduler")
            if worker.get("status") != "healthy" or not scheduler.get("thread_alive"):
                unavailable, reasons = True, reasons + ["canonical_worker_unavailable"]
            if float(scheduler.get("failed", 0)) >= V2_THRESHOLDS[
                "canonical_failed_unavailable_items"
            ]:
                unavailable, reasons = True, reasons + ["canonical_failed_present"]
            if scheduler.get("backlog_truncated"):
                unavailable, reasons = True, reasons + ["canonical_backlog_truncated"]
            if float(scheduler.get("pending", 0)) >= V2_THRESHOLDS[
                "canonical_pending_degraded_items"
            ]:
                reasons.append("canonical_backlog_high")
            if float(scheduler.get("oldest_lag_seconds", 0)) >= V2_THRESHOLDS[
                "canonical_lag_degraded_seconds"
            ]:
                reasons.append("canonical_lag_high")
            if scheduler.get("paused"):
                reasons.append("canonical_worker_paused")
        pressure = self._component(snapshot, "clickhouse_pressure")
        if pressure.get("status") == "observed":
            disk = float(pressure.get("disk_used_ratio", 0))
            merge = float(pressure.get("merge_queue", 0))
            if disk >= V2_THRESHOLDS["disk_unavailable_ratio"] or merge >= V2_THRESHOLDS[
                "merge_unavailable_items"
            ]:
                unavailable, reasons = True, reasons + ["clickhouse_pressure_critical"]
            elif disk >= V2_THRESHOLDS["disk_degraded_ratio"] or merge >= V2_THRESHOLDS[
                "merge_degraded_items"
            ]:
                reasons.append("clickhouse_pressure_high")
        elif pressure.get("status") == "unavailable":
            unavailable, reasons = True, reasons + ["clickhouse_pressure_unavailable"]
        else:
            reasons.append("clickhouse_pressure_missing")
        return "unavailable" if unavailable else "degraded" if reasons else "healthy", reasons

    def evaluate(self, snapshot: Any) -> Dict[str, Any]:
        try:
            now = float(self.clock())
        except Exception:
            now, snapshot = 0.0, None
        try:
            raw, reasons = self._raw_state(snapshot)
        except Exception:
            raw, reasons = "unavailable", ["v2_health_evaluation_failed"]
        with self._lock:
            immediate = any(reason in {
                "v2_dependency_snapshot_unavailable", "postgresql_unavailable",
                "clickhouse_main_unavailable", "wal_diagnostics_unavailable",
                "canonical_worker_unavailable",
                "clickhouse_pressure_unavailable",
            } for reason in reasons)
            if immediate:
                self._state, self._candidate, self._candidate_since = raw, None, None
            elif raw == self._state:
                self._candidate = self._candidate_since = None
            else:
                if self._candidate != raw:
                    self._candidate, self._candidate_since = raw, now
                threshold = (V2_THRESHOLDS["recover_after_seconds"] if raw == "healthy"
                             or self._state == "unavailable" and raw == "degraded"
                             else V2_THRESHOLDS["unavailable_after_seconds"]
                             if raw == "unavailable"
                             else V2_THRESHOLDS["degrade_after_seconds"])
                elapsed = max(0.0, now - float(self._candidate_since))
                if elapsed >= threshold:
                    self._state, self._candidate, self._candidate_since = raw, None, None
                else:
                    reasons.append(f"transition_pending:{raw}:{threshold - elapsed:.3f}s")
            state, candidate, since = self._state, self._candidate, self._candidate_since
        try:
            observed = self.wall_clock().astimezone(timezone.utc).isoformat()
        except Exception:
            observed = None
        components = snapshot.get("components", {}) if isinstance(snapshot, Mapping) else {}
        return {
            "schema": V2_HEALTH_SCHEMA, "status": state,
            "ready": state != "unavailable", "backend": "postgresql+clickhouse",
            "observed_at": observed, "candidate_status": candidate,
            "candidate_since_monotonic": since,
            "reasons": StorageHealthEvaluator._bounded_reasons(reasons),
            "thresholds": dict(V2_THRESHOLDS),
            "window": {"kind": "sustained_condition", "process_local": True},
            "components": components,
        }
