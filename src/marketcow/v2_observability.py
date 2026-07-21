from __future__ import annotations

from typing import Any, Mapping

from .telemetry import METRICS, Telemetry, telemetry_call


V2_TELEMETRY_SCHEMA = "storage-v2.pg-ch-telemetry.v1"
V2_ALLOWED_METRICS = frozenset({
    "ingest_write_latency_seconds", "wal_items", "canonical_queue_items",
    "canonical_lag_seconds", "canonical_rebuild_total", "query_latency_seconds",
    "cache_age_seconds", "clickhouse_pressure", "v2_authoritative_write_total",
    "v2_replay_total", "v2_postgresql_query_latency_seconds",
    "v2_backup_restore_total", "v2_operator_total",
})
FORBIDDEN_V2_LABEL_VALUES = frozenset({"duckdb", "fallback"})


class V2Telemetry(Telemetry):
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
            if metric["name"] not in V2_ALLOWED_METRICS:
                continue
            if FORBIDDEN_V2_LABEL_VALUES & set(metric["labels"].values()):
                continue
            metrics.append(metric)
        document.update({
            "schema": V2_TELEMETRY_SCHEMA,
            "runtime": "postgresql_clickhouse_online",
            "metrics": metrics,
            "metric_contract": {
                name: {
                    "type": METRICS[name]["type"], "unit": METRICS[name]["unit"],
                    "labels": {
                        key: [value for value in values
                              if value not in FORBIDDEN_V2_LABEL_VALUES]
                        for key, values in METRICS[name]["labels"].items()
                    },
                    **({"buckets": list(METRICS[name]["buckets"])}
                       if "buckets" in METRICS[name] else {}),
                }
                for name in sorted(V2_ALLOWED_METRICS)
            },
        })
        return document

    def record_authoritative(self, outcome: str) -> None:
        self.safe("counter", "v2_authoritative_write_total", outcome=outcome)

    def record_replay(self, outcome: str, amount: int = 1) -> None:
        self.safe("counter", "v2_replay_total", amount, outcome=outcome)

    def record_backup_restore(self, operation: str, outcome: str) -> None:
        self.safe("counter", "v2_backup_restore_total",
                  operation=operation, outcome=outcome)

    def record_operator(self, action: str, outcome: str) -> None:
        self.safe("counter", "v2_operator_total", action=action, outcome=outcome)


def record_v2_operation(telemetry: Any, method: str, *args: Any, **kwargs: Any) -> None:
    telemetry_call(telemetry, method, *args, **kwargs)


def validate_v2_snapshot(snapshot: Mapping[str, Any]) -> None:
    if snapshot.get("schema") != V2_TELEMETRY_SCHEMA:
        raise ValueError("V2 telemetry schema is incompatible")
    metrics = snapshot.get("metrics")
    if not isinstance(metrics, list) or len(metrics) > snapshot["limits"]["metric_series"]:
        raise ValueError("V2 telemetry series are invalid")
    for metric in metrics:
        if metric.get("name") not in V2_ALLOWED_METRICS:
            raise ValueError("V2 telemetry contains a forbidden metric")
        if FORBIDDEN_V2_LABEL_VALUES & set(metric.get("labels", {}).values()):
            raise ValueError("V2 telemetry contains a forbidden backend label")
