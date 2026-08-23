#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
import subprocess
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx

from verify_polymarket_shared_events_soak import instant, load_v48_config


ROUTES = ("events", "health", "bootstrap", "snapshot")


def percentile(values: list[float], fraction: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    return ordered[max(0, math.ceil(len(ordered) * fraction) - 1)]


def latency_summary(values: list[float]) -> dict[str, float | int | None]:
    return {
        "count": len(values),
        "p50": percentile(values, 0.50),
        "p95": percentile(values, 0.95),
        "p99": percentile(values, 0.99),
        "max": max(values) if values else None,
    }


def server_timing_ms(value: str, phase: str) -> float | None:
    for item in value.split(","):
        fields = [field.strip() for field in item.split(";")]
        if fields and fields[0] == phase:
            for field in fields[1:]:
                if field.startswith("dur="):
                    return float(field.removeprefix("dur="))
    return None


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Ten-minute 100-market snapshot/bootstrap smoke at Tradude v55 cadence"
        )
    )
    parser.add_argument("--v55-config", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--duration-seconds", type=float, default=600)
    parser.add_argument("--api-pid", type=int)
    parser.add_argument("--collector-pid", type=int)
    arguments = parser.parse_args()
    if arguments.duration_seconds < 600:
        parser.error("snapshot acceptance smoke requires at least 600 seconds")

    config = load_v48_config(arguments.v55_config.resolve())
    if config["base_url"] != "http://127.0.0.1:8790":
        parser.error("smoke must target the shared 127.0.0.1:8790 service")
    if len(config["market_ids"]) != 100:
        parser.error("smoke requires the exact 100-market Tradude scope")
    market_params = [("market_id", item) for item in config["market_ids"]]
    timeout_seconds = 10.0
    started_wall = datetime.now(timezone.utc)
    started = time.monotonic()
    deadline = started + arguments.duration_seconds
    lock = threading.Lock()
    latencies: dict[str, list[float]] = {route: [] for route in ROUTES}
    failures: dict[str, int] = {route: 0 for route in ROUTES}
    expected_fail_closed: dict[str, int] = {route: 0 for route in ROUTES}
    timeouts: dict[str, int] = {route: 0 for route in ROUTES}
    errors: list[dict[str, Any]] = []
    maximum_book_age_seconds = 0.0
    maximum_event_loop_stall_ms = 0.0
    maximum_persistence_lag_events = 0
    maximum_persistence_queue_depth = 0
    maximum_unresolved_gaps = 0
    events_sources: set[str] = set()
    events_sqlite_query_ms: list[float] = []
    health_sqlite_query_ms: list[float] = []
    disconnect_counts: list[int] = []
    websocket_disconnected_observations = 0
    cursor_gap_count = 0
    duplicate_event_count = 0
    integrity_failure_count = 0
    successful_stale_snapshot_count = 0
    health_index_ready_with_stale_snapshot_count = 0
    event_cursor_start: int | None = None
    event_cursor_finish: int | None = None

    def record_failure(route: str, exc: Exception | None, detail: Any = None) -> None:
        with lock:
            failures[route] += 1
            if isinstance(exc, httpx.TimeoutException):
                timeouts[route] += 1
            if len(errors) < 100:
                errors.append({
                    "at": datetime.now(timezone.utc).isoformat(),
                    "route": route,
                    "exception": type(exc).__name__ if exc else None,
                    "message": str(exc)[:300] if exc else None,
                    "detail": detail,
                })

    def fetch(
        client: httpx.Client,
        route: str,
        *,
        params: list[tuple[str, str]] | None = None,
    ) -> tuple[httpx.Response | None, dict[str, Any] | None]:
        request_started = time.perf_counter()
        try:
            response = client.get(
                f"{config['base_url']}/v1/prediction-markets/polymarket/live/{route}",
                params=params,
                headers={"Accept": "application/json"},
            )
            elapsed = time.perf_counter() - request_started
            with lock:
                latencies[route].append(elapsed)
            payload = response.json()
            if response.status_code != 200:
                detail = payload.get("detail", {}) if isinstance(payload, dict) else {}
                expected_503 = (
                    response.status_code == 503
                    and (
                        (
                            route == "health"
                            and payload.get("status") == "degraded"
                            and bool(payload.get("reason_codes"))
                        )
                        or (
                            route in {"bootstrap", "snapshot"}
                            and detail.get("code") == "polymarket_state_index_lagging"
                        )
                    )
                )
                if expected_503:
                    with lock:
                        expected_fail_closed[route] += 1
                    return response, payload
                record_failure(route, None, payload)
                return response, payload
            return response, payload
        except Exception as exc:
            elapsed = time.perf_counter() - request_started
            with lock:
                latencies[route].append(elapsed)
            record_failure(route, exc)
            return None, None

    def monitor_loop() -> None:
        nonlocal maximum_book_age_seconds
        nonlocal maximum_event_loop_stall_ms
        nonlocal maximum_persistence_lag_events
        nonlocal maximum_persistence_queue_depth
        nonlocal maximum_unresolved_gaps
        nonlocal websocket_disconnected_observations
        nonlocal integrity_failure_count
        nonlocal successful_stale_snapshot_count
        nonlocal health_index_ready_with_stale_snapshot_count
        limits = httpx.Limits(max_connections=2, max_keepalive_connections=2)
        with httpx.Client(
            timeout=timeout_seconds, limits=limits, trust_env=False
        ) as client:
            while time.monotonic() < deadline:
                health_response, health = fetch(client, "health")
                bootstrap_response, bootstrap = fetch(
                    client, "bootstrap", params=market_params
                )
                snapshot_response, snapshot = fetch(
                    client, "snapshot", params=market_params
                )
                cycle_maximum_age = 0.0
                if health is not None:
                    with lock:
                        maximum_event_loop_stall_ms = max(
                            maximum_event_loop_stall_ms,
                            float(health.get("event_loop_stall_max_ms", 0)),
                        )
                        maximum_persistence_lag_events = max(
                            maximum_persistence_lag_events,
                            int(health.get("persistence_lag_events", 0)),
                        )
                        maximum_persistence_queue_depth = max(
                            maximum_persistence_queue_depth,
                            int(health.get("persistence_queue_depth", 0)),
                        )
                        maximum_unresolved_gaps = max(
                            maximum_unresolved_gaps,
                            int(health.get("unresolved_gap_count", 0)),
                        )
                        events_sources.add(str(health.get("events_read_source")))
                        health_sqlite_query_ms.append(
                            float(health.get("realtime_sqlite_query_ms", -1))
                        )
                        disconnect_counts.append(
                            int(health.get("live_stream_disconnect_count", 0))
                        )
                        if not health.get("live_stream_connected", False):
                            websocket_disconnected_observations += 1
                if bootstrap_response is not None and bootstrap_response.status_code == 200:
                    returned = {
                        str(item["identity"]["market_id"])
                        for item in bootstrap.get("markets") or []
                    }
                    if returned != set(config["market_ids"]):
                        with lock:
                            integrity_failure_count += 1
                if snapshot_response is not None and snapshot_response.status_code == 200:
                    returned = {
                        str(item["market_id"])
                        for item in snapshot.get("items") or []
                    }
                    if returned != set(config["market_ids"]):
                        with lock:
                            integrity_failure_count += 1
                    observed = datetime.now(timezone.utc)
                    unique_books: dict[str, dict[str, Any]] = snapshot.get("books") or {}
                    referenced_token_ids: set[str] = set()
                    for frame in snapshot.get("items") or []:
                        if frame.get("status") != "ready":
                            with lock:
                                integrity_failure_count += 1
                        referenced_token_ids.update(
                            str(item) for item in frame.get("token_ids", [])
                        )
                        referenced_token_ids.update(
                            str(item) for item in frame.get("relation_token_ids", [])
                        )
                    if referenced_token_ids != set(unique_books):
                        with lock:
                            integrity_failure_count += 1
                    if len(unique_books) != 200:
                        with lock:
                            integrity_failure_count += 1
                    for book in unique_books.values():
                        age = (observed - instant(book["received_at"])).total_seconds()
                        cycle_maximum_age = max(cycle_maximum_age, age)
                        if age < 0:
                            with lock:
                                integrity_failure_count += 1
                    with lock:
                        maximum_book_age_seconds = max(
                            maximum_book_age_seconds, cycle_maximum_age
                        )
                        if cycle_maximum_age >= 5:
                            successful_stale_snapshot_count += 1
                            if (
                                health_response is not None
                                and health_response.status_code == 200
                                and health
                                and health.get("status") == "index_ready"
                            ):
                                health_index_ready_with_stale_snapshot_count += 1
                sleep_for = min(1.0, max(0.0, deadline - time.monotonic()))
                if sleep_for:
                    time.sleep(sleep_for)

    monitor = threading.Thread(target=monitor_loop, name="tradude-v55-health")
    monitor.start()
    limits = httpx.Limits(max_connections=2, max_keepalive_connections=2)
    seen_ids: set[str] = set()
    cursor: int | None = None
    with httpx.Client(
        timeout=timeout_seconds, limits=limits, trust_env=False
    ) as client:
        while time.monotonic() < deadline:
            if cursor is None:
                _, initial = fetch(client, "snapshot", params=market_params)
                if initial is None:
                    time.sleep(0.5)
                    continue
                cursor = int(initial["cursor"])
                event_cursor_start = cursor
            params = list(market_params)
            params.extend((("after_cursor", str(cursor)), ("limit", "1000")))
            response, page = fetch(client, "events", params=params)
            if response is not None:
                sqlite_ms = server_timing_ms(
                    response.headers.get("server-timing", ""), "sqlite_query"
                )
                if sqlite_ms is not None:
                    events_sqlite_query_ms.append(sqlite_ms)
            if page is not None:
                items = page.get("items") or []
                cursors = [int(item["cursor"]) for item in items]
                ids = [str(item["event_id"]) for item in items]
                if cursors != sorted(set(cursors)):
                    cursor_gap_count += 1
                duplicate_event_count += len(set(ids) & seen_ids)
                seen_ids.update(ids)
                next_cursor = int(page.get("next_cursor", -1))
                expected = cursors[-1] if cursors else cursor
                if next_cursor != expected or next_cursor < cursor:
                    cursor_gap_count += 1
                cursor = next_cursor
                event_cursor_finish = cursor
            sleep_for = min(
                float(config["poll_interval_seconds"]),
                max(0.0, deadline - time.monotonic()),
            )
            if sleep_for:
                time.sleep(sleep_for)
    monitor.join(timeout=timeout_seconds + 5)
    if monitor.is_alive():
        raise RuntimeError("health/bootstrap/snapshot monitor did not terminate")

    duration = time.monotonic() - started
    disconnect_delta = (
        max(disconnect_counts) - min(disconnect_counts)
        if disconnect_counts else None
    )
    process_alive = {}
    for name, pid in (("api", arguments.api_pid), ("collector", arguments.collector_pid)):
        if pid is None:
            process_alive[name] = None
            continue
        process_alive[name] = subprocess.run(
            ["kill", "-0", str(pid)], capture_output=True, check=False
        ).returncode == 0
    route_latency = {
        route: latency_summary(values) for route, values in latencies.items()
    }
    criteria = {
        "duration_at_least_10_minutes": duration >= 600,
        "all_route_failures_zero": all(value == 0 for value in failures.values()),
        "all_10_second_timeouts_zero": all(value == 0 for value in timeouts.values()),
        "successful_books_always_younger_than_5_seconds": (
            maximum_book_age_seconds < 5 and successful_stale_snapshot_count == 0
        ),
        "health_never_ready_with_stale_readable_book": (
            health_index_ready_with_stale_snapshot_count == 0
        ),
        "events_read_from_memory_projection": events_sources == {"memory_projection"},
        "realtime_sqlite_queries_zero": (
            bool(events_sqlite_query_ms)
            and max(events_sqlite_query_ms) == 0
            and bool(health_sqlite_query_ms)
            and max(health_sqlite_query_ms) == 0
        ),
        "websocket_disconnects_zero": (
            websocket_disconnected_observations == 0 and disconnect_delta == 0
        ),
        "unresolved_gaps_zero": maximum_unresolved_gaps == 0,
        "event_cursor_advanced_without_gap_or_duplicate": (
            event_cursor_start is not None
            and event_cursor_finish is not None
            and event_cursor_finish > event_cursor_start
            and cursor_gap_count == 0
            and duplicate_event_count == 0
        ),
        "integrity_failures_zero": integrity_failure_count == 0,
        "processes_survived": all(value is not False for value in process_alive.values()),
        "real_orders_disabled": config["real_order_submission_enabled"] is False,
    }
    report = {
        "schema_version": "marketcow.polymarket.snapshot-smoke.v1",
        "passed": all(criteria.values()),
        "criteria": criteria,
        "started_at": started_wall.isoformat(),
        "finished_at": datetime.now(timezone.utc).isoformat(),
        "duration_seconds": duration,
        "git_commit": subprocess.run(
            ["git", "rev-parse", "HEAD"],
            capture_output=True,
            check=True,
            text=True,
        ).stdout.strip(),
        "config": config,
        "process_alive": process_alive,
        "route_latency_seconds": route_latency,
        "maximum_book_age_seconds": maximum_book_age_seconds,
        "event_loop_stall_max_ms": maximum_event_loop_stall_ms,
        "events_read_sources": sorted(events_sources),
        "events_sqlite_query_ms": latency_summary(events_sqlite_query_ms),
        "health_realtime_sqlite_query_ms": latency_summary(health_sqlite_query_ms),
        "maximum_persistence_lag_events": maximum_persistence_lag_events,
        "maximum_persistence_queue_depth": maximum_persistence_queue_depth,
        "websocket_disconnected_observations": websocket_disconnected_observations,
        "live_stream_disconnect_count_delta": disconnect_delta,
        "maximum_unresolved_gap_count": maximum_unresolved_gaps,
        "event_cursor_start": event_cursor_start,
        "event_cursor_finish": event_cursor_finish,
        "cursor_gap_count": cursor_gap_count,
        "duplicate_event_count": duplicate_event_count,
        "integrity_failure_count": integrity_failure_count,
        "successful_stale_snapshot_count": successful_stale_snapshot_count,
        "health_index_ready_with_stale_snapshot_count": (
            health_index_ready_with_stale_snapshot_count
        ),
        "route_failure_counts": failures,
        "expected_fail_closed_counts": expected_fail_closed,
        "route_timeout_counts": timeouts,
        "errors": errors,
    }
    arguments.output.parent.mkdir(parents=True, exist_ok=True)
    arguments.output.write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(json.dumps(report, sort_keys=True))
    raise SystemExit(0 if report["passed"] else 1)


if __name__ == "__main__":
    main()
