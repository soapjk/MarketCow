from __future__ import annotations

from typing import Any, Mapping

from .telemetry import METRICS, Telemetry as BaseTelemetry, telemetry_call


TELEMETRY_SCHEMA = "marketcow.pg-ch-telemetry.v1"
ALLOWED_METRICS = frozenset({
    "ingest_write_latency_seconds", "wal_items", "canonical_queue_items",
    "canonical_lag_seconds", "canonical_rebuild_total", "query_latency_seconds",
    "cache_age_seconds", "clickhouse_pressure", "authoritative_write_total",
    "replay_total", "postgresql_query_latency_seconds",
    "backup_restore_total", "operator_total",
})
FORBIDDEN_LABEL_VALUES = frozenset({"duckdb", "fallback"})


class Telemetry(BaseTelemetry):
    """Bounded, process-local PG/CH-only telemetry; helpers are fail-open."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        kwargs["clickhouse_enabled"] = True
        super().__init__(*args, **kwargs)

    def safe_log(self, event: str, severity: str = "info", **fields: Any) -> None:
        telemetry_call(self, "log", event, severity, **fields)

    def snapshot(self) -> dict[str, Any]:
        document = super().snapshot()
        metrics = []
        for metric in document["metrics"]:
            if metric["name"] not in ALLOWED_METRICS:
                continue
            if FORBIDDEN_LABEL_VALUES & set(metric["labels"].values()):
                continue
            metrics.append(metric)
        document.update({
            "schema": TELEMETRY_SCHEMA,
            "runtime": "postgresql_clickhouse_online",
            "metrics": metrics,
            "metric_contract": {
                name: {
                    "type": METRICS[name]["type"], "unit": METRICS[name]["unit"],
                    "labels": {
                        key: [value for value in values
                              if value not in FORBIDDEN_LABEL_VALUES]
                        for key, values in METRICS[name]["labels"].items()
                    },
                    **({"buckets": list(METRICS[name]["buckets"])}
                       if "buckets" in METRICS[name] else {}),
                }
                for name in sorted(ALLOWED_METRICS)
            },
        })
        return document

    def record_authoritative(self, outcome: str) -> None:
        self.safe("counter", "authoritative_write_total", outcome=outcome)

    def record_replay(self, outcome: str, amount: int = 1) -> None:
        self.safe("counter", "replay_total", amount, outcome=outcome)

    def record_backup_restore(self, operation: str, outcome: str) -> None:
        self.safe("counter", "backup_restore_total",
                  operation=operation, outcome=outcome)

    def record_operator(self, action: str, outcome: str) -> None:
        self.safe("counter", "operator_total", action=action, outcome=outcome)


def record_operation(telemetry: Any, method: str, *args: Any, **kwargs: Any) -> None:
    telemetry_call(telemetry, method, *args, **kwargs)


def validate_snapshot(snapshot: Mapping[str, Any]) -> None:
    if snapshot.get("schema") != TELEMETRY_SCHEMA:
        raise ValueError("telemetry schema is incompatible")
    metrics = snapshot.get("metrics")
    if not isinstance(metrics, list) or len(metrics) > snapshot["limits"]["metric_series"]:
        raise ValueError("telemetry series are invalid")
    for metric in metrics:
        if metric.get("name") not in ALLOWED_METRICS:
            raise ValueError("telemetry contains a forbidden metric")
        if FORBIDDEN_LABEL_VALUES & set(metric.get("labels", {}).values()):
            raise ValueError("telemetry contains a forbidden backend label")
