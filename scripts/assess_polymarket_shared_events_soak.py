#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import math
from datetime import datetime
from pathlib import Path
from typing import Any


TRACE_MARKER = "polymarket_events_request "
PHASES = (
    "executor_queue_ms",
    "scope_bootstrap_ms",
    "stable_boundary_wait_ms",
    "sqlite_query_ms",
    "model_construction_ms",
    "json_serialization_ms",
    "response_write_ms",
    "total_ms",
)


def percentile(values: list[float], fraction: float) -> float:
    ordered = sorted(values)
    return ordered[max(0, math.ceil(len(ordered) * fraction) - 1)]


def instant(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Audit an existing shared /events soak against the work item."
    )
    parser.add_argument("--report", required=True, type=Path)
    parser.add_argument("--events-log", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()

    report_bytes = args.report.read_bytes()
    report = json.loads(report_bytes)
    started = instant(report["started_at"])
    finished = instant(report["finished_at"])
    traces: list[dict[str, Any]] = []
    for line in args.events_log.read_text(encoding="utf-8").splitlines():
        marker = line.find(TRACE_MARKER)
        if marker < 0:
            continue
        trace = json.loads(line[marker + len(TRACE_MARKER):])
        timestamp = instant(trace["timestamp"])
        if started <= timestamp <= finished:
            traces.append(trace)

    successful = [trace for trace in traces if trace.get("status") == 200]
    required_trace_fields = {
        "timestamp", "after_cursor", "limit", "market_count", "status",
        "response_bytes", "response_write_ms", "total_ms",
    }
    required_success_fields = required_trace_fields | {
        "next_cursor", "executor_queue_ms", "scope_bootstrap_ms",
        "stable_boundary_wait_ms", "sqlite_query_ms",
        "model_construction_ms", "json_serialization_ms",
    }
    phase_summary = {}
    for phase in PHASES:
        values = [float(trace[phase]) for trace in successful if phase in trace]
        phase_summary[phase] = {
            "sample_count": len(values),
            "p99_ms": percentile(values, 0.99) if values else None,
            "max_ms": max(values) if values else None,
        }
    response_bytes = [int(trace["response_bytes"]) for trace in successful]

    events = report["events"]
    samples = report["route_sample_counts"]
    failures = report["route_failure_counts"]
    client_timeouts = report["client_timeout_counts"]
    config = report["config"]
    criteria = {
        "exact_shared_v48_scope": (
            config["base_url"] == "http://127.0.0.1:8790"
            and len(config["market_ids"]) == 100
            and config["event_limit"] == 1000
            and config["timeout_seconds"] == 10.0
        ),
        "duration_at_least_one_hour": report["duration_seconds"] >= 3600,
        "events_client_timeouts_zero": events["client_timeout_count"] == 0,
        "events_no_30_second_unavailability": (
            events["maximum_unavailable_seconds"] < 30
        ),
        "events_p99_at_most_3_seconds": (
            events["latency_seconds"]["p99"] <= 3
        ),
        "all_concurrent_routes_responded": all(
            samples[name] > failures[name]
            for name in ("events", "health", "bootstrap", "snapshot")
        ) and all(value == 0 for value in client_timeouts.values()),
        "cursor_and_integrity_complete": (
            events["next_cursor_finish"] > events["after_cursor_start"]
            and events["cursor_gap_count"] == 0
            and events["duplicate_event_count"] == 0
            and report["integrity_failure_count"] == 0
        ),
        "books_complete_and_fresh": (
            report["fail_closed_frame_count"] == 0
            and report["maximum_book_age_seconds"] < 5
        ),
        "processes_survived": (
            report["environment"]["api_alive_at_finish"] is True
            and report["environment"]["collector_alive_at_finish"] is True
        ),
        "shadow_mode": config["mode"] == "shadow",
        "structured_phase_traces_complete": (
            len(successful) > 0
            and all(required_trace_fields <= set(trace) for trace in traces)
            and all(required_success_fields <= set(trace) for trace in successful)
        ),
    }
    assessment = {
        "schema_version": "marketcow.polymarket.shared-events-assessment.v1",
        "passed": all(criteria.values()),
        "criteria": criteria,
        "source_report": str(args.report.resolve()),
        "source_report_sha256": hashlib.sha256(report_bytes).hexdigest(),
        "source_events_log": str(args.events_log.resolve()),
        "source_started_at": report["started_at"],
        "source_finished_at": report["finished_at"],
        "events": events,
        "route_sample_counts": samples,
        "route_failure_counts": failures,
        "route_maximum_unavailable_seconds": (
            report["route_maximum_unavailable_seconds"]
        ),
        "client_timeout_counts": client_timeouts,
        "integrity_failure_count": report["integrity_failure_count"],
        "fail_closed_frame_count": report["fail_closed_frame_count"],
        "maximum_book_age_seconds": report["maximum_book_age_seconds"],
        "environment": report["environment"],
        "config": config,
        "trace": {
            "sample_count": len(traces),
            "successful_sample_count": len(successful),
            "server_fail_closed_count": sum(
                trace.get("status") == 503 for trace in traces
            ),
            "server_timed_out_count": sum(
                bool(trace.get("timed_out")) for trace in traces
            ),
            "response_bytes": {
                "min": min(response_bytes) if response_bytes else None,
                "p99": percentile(response_bytes, 0.99) if response_bytes else None,
                "max": max(response_bytes) if response_bytes else None,
            },
            "phases": phase_summary,
        },
        "interpretation": (
            "The source verifier applied its no-30-second condition to every "
            "concurrent route. The work item applies that condition to /events; "
            "health/bootstrap/snapshot must respond concurrently and retain "
            "fail-closed freshness semantics. The source report is preserved."
        ),
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(assessment, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(assessment, sort_keys=True))
    raise SystemExit(0 if assessment["passed"] else 1)


if __name__ == "__main__":
    main()
