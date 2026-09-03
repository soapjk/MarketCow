#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import platform
import re
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx


ROUTES = {
    "events": "events",
    "health": "health",
    "bootstrap": "bootstrap",
    "snapshot": "snapshot",
    "full_sync": "full-sync",
    "rejected_candidate_probe": "full-sync",
}


def percentile(values: list[float], fraction: float) -> float:
    ordered = sorted(values)
    rank = max(0, math.ceil(len(ordered) * fraction) - 1)
    return ordered[rank]


def instant(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(
        timezone.utc
    )


def load_v48_config(path: Path) -> dict[str, Any]:
    unified = path.read_text(encoding="utf-8")
    if yaml_scalar(unified, "mode", 0) != "shadow":
        raise ValueError("acceptance config must explicitly use shadow mode")
    live_path = Path(yaml_scalar(unified, "multi_strategy_config", 0))
    multi = live_path.read_text(encoding="utf-8")
    paper_path = Path(yaml_scalar(multi, "live_paper_config", 0))
    paper = paper_path.read_text(encoding="utf-8")
    canary_path = Path(yaml_scalar(unified, "canary_config", 0))
    canary = canary_path.read_text(encoding="utf-8")
    market_block = re.search(
        r"(?m)^  market_ids:\s*\n(?P<items>(?:^    - .+\n)+)", paper,
    )
    if market_block is None:
        raise ValueError("v48 live-paper config lacks explicit market_ids")
    market_ids = list(dict.fromkeys(
        line.split("-", 1)[1].strip().strip("'\"")
        for line in market_block.group("items").splitlines()
        if "-" in line
    ))
    if len(market_ids) != 100:
        raise ValueError(f"v48 config must bind exactly 100 markets, got {len(market_ids)}")
    if yaml_scalar(canary, "mode", 0) != "disabled":
        raise ValueError("real-order canary must remain disabled")
    if int(yaml_scalar(unified, "event_page_limit", 2)) != 1000:
        raise ValueError("v48 event_page_limit must be 1000")
    if float(yaml_scalar(paper, "timeout_seconds", 2)) != 10:
        raise ValueError("v48 MarketCow timeout must be 10 seconds")
    return {
        "base_url": yaml_scalar(paper, "base_url", 2).rstrip("/"),
        "timeout_seconds": 10.0,
        "market_ids": market_ids,
        "event_limit": 1000,
        "poll_interval_seconds": float(
            yaml_scalar(unified, "poll_interval_seconds", 2)
        ),
        "configured_duration_seconds": float(
            yaml_scalar(unified, "run_duration_seconds", 2)
        ),
        "maximum_consecutive_unavailable_seconds": float(
            yaml_scalar(unified, "maximum_consecutive_seconds", 4)
        ),
        "maximum_book_age_seconds": float(
            yaml_scalar(unified, "maximum_book_age_ns", 2)
        ) / 1_000_000_000,
        "mode": "shadow",
        "config_path": str(path.resolve()),
        "config_sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
    }


def yaml_scalar(document: str, key: str, indent: int) -> str:
    match = re.search(
        rf"(?m)^{' ' * indent}{re.escape(key)}:\s*(?P<value>[^#\n]+?)\s*$",
        document,
    )
    if match is None:
        raise ValueError(f"required YAML scalar is missing: {key}")
    return match.group("value").strip().strip("'\"")


def process_alive(pid: int | None) -> bool | None:
    if pid is None:
        return None
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def main() -> None:
    parser = argparse.ArgumentParser(
        description="One-hour fail-closed soak for the shared Polymarket /events API."
    )
    parser.add_argument("--v48-config", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--duration-seconds", type=float, default=3600)
    parser.add_argument("--api-pid", type=int)
    parser.add_argument("--collector-pid", type=int)
    parser.add_argument(
        "--base-url",
        help="Loopback API override used to isolate an acceptance worktree service",
    )
    args = parser.parse_args()
    config = load_v48_config(args.v48_config.resolve())
    if args.duration_seconds < 3600:
        raise SystemExit("formal acceptance requires --duration-seconds >= 3600")
    if args.base_url:
        if not re.fullmatch(r"http://127\.0\.0\.1:\d{1,5}", args.base_url):
            raise SystemExit("--base-url must be an explicit loopback HTTP endpoint")
        config["configured_base_url"] = config["base_url"]
        config["base_url"] = args.base_url
    if not re.fullmatch(r"http://127\.0\.0\.1:\d{1,5}", config["base_url"]):
        raise SystemExit("formal acceptance must target a loopback MarketCow API")
    args.output.parent.mkdir(parents=True, exist_ok=True)

    market_params = [("market_id", item) for item in config["market_ids"]]
    latencies: dict[str, list[float]] = {name: [] for name in ROUTES}
    samples: dict[str, int] = {name: 0 for name in ROUTES}
    failures: dict[str, int] = {name: 0 for name in ROUTES}
    client_timeouts_by_route: dict[str, int] = {
        name: 0 for name in ROUTES
    }
    integrity_failures = 0
    cursor_gap_count = 0
    duplicate_event_count = 0
    fail_closed_frames = 0
    maximum_book_age_seconds = 0.0
    maximum_unresolved_gap_count = 0
    maximum_persistence_queue_depth = 0
    maximum_persistence_lag_events = 0
    disconnect_counts: list[int] = []
    realtime_sqlite_query_ms: list[float] = []
    read_sources: set[str] = set()
    disconnected_health_samples = 0
    server_timing_failures = 0
    response_body_bytes_maximum: dict[str, int] = {
        name: 0 for name in ROUTES
    }
    maximum_unavailable_seconds: dict[str, float] = {
        name: 0.0 for name in ROUTES
    }
    unavailable_since: dict[str, float | None] = {
        name: None for name in ROUTES
    }
    after_cursor = 0
    cursor_start = 0
    seen_event_ids: set[str] = set()
    errors: list[dict[str, Any]] = []
    started_wall = datetime.now(timezone.utc)
    started = time.monotonic()
    next_bootstrap = started
    next_snapshot = started
    next_health = started
    next_full_sync = started
    next_probe = started

    def request(client: httpx.Client, name: str, cursor: int) -> tuple[str, Any]:
        params = (
            market_params[:3]
            if name == "rejected_candidate_probe"
            else list(market_params)
        )
        if name == "events":
            params.extend([("after_cursor", str(cursor)), ("limit", "1000")])
        request_started = time.perf_counter()
        try:
            response = client.get(
                f"{config['base_url']}/v1/prediction-markets/polymarket/live/"
                f"{ROUTES[name]}",
                params=params,
                headers={"Accept": "application/json"},
            )
            elapsed = time.perf_counter() - request_started
            return name, (
                elapsed,
                response.status_code,
                response.json(),
                response.headers.get("server-timing", ""),
                len(response.content),
            )
        except Exception as exc:
            elapsed = time.perf_counter() - request_started
            return name, (elapsed, exc)

    def observe_availability(name: str, available: bool) -> None:
        now = time.monotonic()
        if available:
            if unavailable_since[name] is not None:
                maximum_unavailable_seconds[name] = max(
                    maximum_unavailable_seconds[name],
                    now - unavailable_since[name],
                )
                unavailable_since[name] = None
        elif unavailable_since[name] is None:
            unavailable_since[name] = now

    limits = httpx.Limits(max_connections=8, max_keepalive_connections=8)
    with (
        httpx.Client(timeout=10.0, limits=limits, trust_env=False) as client,
        ThreadPoolExecutor(max_workers=6) as executor,
    ):
        preflight_deadline = time.monotonic() + float(
            config["maximum_consecutive_unavailable_seconds"]
        )
        preflight_payload = None
        while preflight_payload is None:
            preflight_started = time.perf_counter()
            try:
                preflight = client.get(
                    f"{config['base_url']}/v1/prediction-markets/"
                    "polymarket/live/snapshot",
                    params=market_params,
                    headers={"Accept": "application/json"},
                )
                latencies["snapshot"].append(
                    time.perf_counter() - preflight_started
                )
                samples["snapshot"] += 1
                if preflight.status_code == 200:
                    observe_availability("snapshot", True)
                    preflight_payload = preflight.json()
                    break
                failures["snapshot"] += 1
                observe_availability("snapshot", False)
            except httpx.TimeoutException:
                latencies["snapshot"].append(
                    time.perf_counter() - preflight_started
                )
                samples["snapshot"] += 1
                failures["snapshot"] += 1
                client_timeouts_by_route["snapshot"] += 1
                observe_availability("snapshot", False)
            if time.monotonic() >= preflight_deadline:
                raise RuntimeError(
                    "snapshot preflight exceeded the v48 unavailability limit"
                )
            time.sleep(1)
        after_cursor = int(preflight_payload["cursor"])
        cursor_start = after_cursor
        while time.monotonic() - started < args.duration_seconds:
            now = time.monotonic()
            due = ["events"]
            if now >= next_health:
                due.append("health")
                next_health = now + 1
            if now >= next_bootstrap:
                due.append("bootstrap")
                next_bootstrap = now + 1
            if now >= next_snapshot:
                due.append("snapshot")
                next_snapshot = now + 1
            if now >= next_full_sync:
                due.append("full_sync")
                next_full_sync = now + 1
            if now >= next_probe:
                due.append("rejected_candidate_probe")
                next_probe = now + 1
            futures = [executor.submit(request, client, name, after_cursor) for name in due]
            for future in as_completed(futures):
                name, result = future.result()
                samples[name] += 1
                latencies[name].append(float(result[0]))
                if len(result) == 2:
                    failures[name] += 1
                    observe_availability(name, False)
                    if isinstance(result[1], httpx.TimeoutException):
                        client_timeouts_by_route[name] += 1
                    if len(errors) < 100:
                        errors.append({
                            "at": datetime.now(timezone.utc).isoformat(),
                            "route": name,
                            "exception": type(result[1]).__name__,
                            "message": str(result[1])[:300],
                        })
                    continue
                _, status, payload, timing_header, body_bytes = result
                response_body_bytes_maximum[name] = max(
                    response_body_bytes_maximum[name], int(body_bytes)
                )
                if status != 200:
                    failures[name] += 1
                    observe_availability(name, False)
                    if len(errors) < 100:
                        errors.append({
                            "at": datetime.now(timezone.utc).isoformat(),
                            "route": name, "status": status,
                            "detail": payload.get("detail") if isinstance(payload, dict) else None,
                        })
                    continue
                observe_availability(name, True)
                if name != "events":
                    required_timings = {
                        "executor_queue", "lock_wait", "projection_copy",
                        "frame_build", "json_serialize", "freshness_check",
                        "maximum_book_age", "freshness_headroom",
                    }
                    exposed_timings = {
                        item.split(";", 1)[0].strip()
                        for item in timing_header.split(",") if item.strip()
                    }
                    if not required_timings <= exposed_timings:
                        server_timing_failures += 1
                if name == "events":
                    if int(payload.get("after_cursor", -1)) != after_cursor:
                        integrity_failures += 1
                    items = payload.get("items") or []
                    cursors = [int(item["cursor"]) for item in items]
                    if cursors != sorted(set(cursors)):
                        cursor_gap_count += 1
                    ids = [str(item["event_id"]) for item in items]
                    duplicate_event_count += len(set(ids) & seen_event_ids)
                    seen_event_ids.update(ids)
                    next_cursor = int(payload.get("next_cursor", -1))
                    expected_cursor = cursors[-1] if cursors else after_cursor
                    if next_cursor != expected_cursor or next_cursor < after_cursor:
                        cursor_gap_count += 1
                    if payload.get("has_more") and not items:
                        integrity_failures += 1
                    after_cursor = next_cursor
                    if cursor_start == 0:
                        cursor_start = after_cursor
                elif name == "health":
                    if payload.get("status") != "index_ready":
                        integrity_failures += 1
                    maximum_unresolved_gap_count = max(
                        maximum_unresolved_gap_count,
                        int(payload.get("unresolved_gap_count", 0)),
                    )
                    maximum_persistence_queue_depth = max(
                        maximum_persistence_queue_depth,
                        int(payload.get("persistence_queue_depth", 0)),
                    )
                    maximum_persistence_lag_events = max(
                        maximum_persistence_lag_events,
                        int(payload.get("persistence_lag_events", 0)),
                    )
                    disconnect_counts.append(int(
                        payload.get("live_stream_disconnect_count", 0)
                    ))
                    realtime_sqlite_query_ms.append(float(
                        payload.get("realtime_sqlite_query_ms", -1)
                    ))
                    read_sources.add(str(payload.get("events_read_source")))
                    if not payload.get("live_stream_connected", False):
                        disconnected_health_samples += 1
                elif name == "bootstrap":
                    returned = {
                        str(item["identity"]["market_id"])
                        for item in payload.get("markets") or []
                    }
                    if returned != set(config["market_ids"]):
                        integrity_failures += 1
                elif name == "snapshot":
                    items = payload.get("items") or []
                    book_map = payload.get("books") or {}
                    returned = {str(item["market_id"]) for item in items}
                    if returned != set(config["market_ids"]):
                        integrity_failures += 1
                    observed = datetime.now(timezone.utc)
                    for frame in items:
                        if frame.get("status") != "ready":
                            fail_closed_frames += 1
                        token_ids = frame.get("token_ids") or []
                        if len(token_ids) != 2:
                            integrity_failures += 1
                        for token_id in token_ids:
                            book = book_map.get(str(token_id))
                            if not book:
                                integrity_failures += 1
                                continue
                            if not book.get("bids") or not book.get("asks"):
                                integrity_failures += 1
                            age = (observed - instant(book["received_at"])).total_seconds()
                            maximum_book_age_seconds = max(maximum_book_age_seconds, age)
                            if age < 0 or age >= config["maximum_book_age_seconds"]:
                                integrity_failures += 1
                elif name in {"full_sync", "rejected_candidate_probe"}:
                    expected_scope = (
                        config["market_ids"][:3]
                        if name == "rejected_candidate_probe"
                        else config["market_ids"]
                    )
                    boundary = (
                        payload.get("catalog_revision"), payload.get("cursor"),
                        payload.get("projection_generation"),
                        payload.get("scope_market_ids"),
                        payload.get("freshness_checked_at"),
                    )
                    for component_name in ("health", "bootstrap", "snapshot"):
                        component = payload.get(component_name) or {}
                        component_cursor = component.get(
                            "latest_cursor", component.get("cursor")
                        )
                        if (
                            component.get("catalog_revision"), component_cursor,
                            component.get("projection_generation"),
                            component.get("scope_market_ids"),
                            component.get("freshness_checked_at"),
                        ) != boundary:
                            integrity_failures += 1
                    full_snapshot = payload.get("snapshot") or {}
                    returned = {
                        str(item["market_id"])
                        for item in full_snapshot.get("items") or []
                    }
                    if returned != set(expected_scope):
                        integrity_failures += 1
                    observed = datetime.now(timezone.utc)
                    for book in (full_snapshot.get("books") or {}).values():
                        age = (
                            observed - instant(book["received_at"])
                        ).total_seconds()
                        maximum_book_age_seconds = max(maximum_book_age_seconds, age)
                        if age < 0 or age >= config["maximum_book_age_seconds"]:
                            integrity_failures += 1
                    if float(payload.get("freshness_budget_remaining_ms", -1)) < float(
                        payload.get("minimum_delivery_headroom_ms", 0)
                    ):
                        integrity_failures += 1
            sleep_for = config["poll_interval_seconds"] - (time.monotonic() - now)
            if sleep_for > 0:
                time.sleep(sleep_for)

    for name, route_started in unavailable_since.items():
        if route_started is not None:
            maximum_unavailable_seconds[name] = max(
                maximum_unavailable_seconds[name],
                time.monotonic() - route_started,
            )
    finished_wall = datetime.now(timezone.utc)
    git_commit = subprocess.run(
        ["git", "rev-parse", "HEAD"], check=False, capture_output=True, text=True,
    ).stdout.strip()
    events_p99 = percentile(latencies["events"], 0.99)
    disconnect_delta = (
        max(disconnect_counts) - min(disconnect_counts)
        if disconnect_counts else -1
    )
    criteria = {
        "duration_at_least_one_hour": time.monotonic() - started >= 3600,
        "events_client_timeouts_zero": client_timeouts_by_route["events"] == 0,
        "events_p99_at_most_3_seconds": events_p99 <= 3,
        "events_no_30_second_unavailability": (
            maximum_unavailable_seconds["events"] < 30
        ),
        "all_routes_healthy": (
            all(samples[name] > failures[name] for name in ROUTES)
            and all(value == 0 for value in client_timeouts_by_route.values())
        ),
        "cursor_and_integrity_complete": (
            cursor_gap_count == 0 and duplicate_event_count == 0
            and integrity_failures == 0 and after_cursor > cursor_start
        ),
        "books_complete_and_fresh": (
            fail_closed_frames == 0
            and maximum_book_age_seconds < config["maximum_book_age_seconds"]
        ),
        "unresolved_gaps_zero": maximum_unresolved_gap_count == 0,
        "websocket_disconnects_zero": (
            disconnect_delta == 0 and disconnected_health_samples == 0
        ),
        "realtime_sqlite_queries_zero": (
            bool(realtime_sqlite_query_ms)
            and max(realtime_sqlite_query_ms) == 0
            and read_sources == {"memory_projection"}
        ),
        "persistence_queue_and_lag_bounded": (
            maximum_persistence_queue_depth < 10_000
            and maximum_persistence_lag_events < 10_000
        ),
        "server_timing_complete": server_timing_failures == 0,
        "processes_survived": all(
            value is not False for value in (
                process_alive(args.api_pid), process_alive(args.collector_pid),
            )
        ),
        "shadow_mode": config["mode"] == "shadow",
    }
    report = {
        "schema_version": "marketcow.polymarket.shared-events-soak.v1",
        "passed": all(criteria.values()),
        "criteria": criteria,
        "environment": {
            "base_url": config["base_url"],
            "host": platform.node(),
            "platform": platform.platform(),
            "python": sys.version,
            "git_commit": git_commit,
            "api_pid": args.api_pid,
            "collector_pid": args.collector_pid,
            "api_alive_at_finish": process_alive(args.api_pid),
            "collector_alive_at_finish": process_alive(args.collector_pid),
        },
        "config": config,
        "started_at": started_wall.isoformat(),
        "finished_at": finished_wall.isoformat(),
        "duration_seconds": time.monotonic() - started,
        "events": {
            "after_cursor_start": cursor_start,
            "next_cursor_finish": after_cursor,
            "sample_count": samples["events"],
            "latency_seconds": {
                "p50": percentile(latencies["events"], 0.50),
                "p95": percentile(latencies["events"], 0.95),
                "p99": events_p99,
                "max": max(latencies["events"]),
            },
            "client_timeout_count": client_timeouts_by_route["events"],
            "maximum_unavailable_seconds": maximum_unavailable_seconds["events"],
            "cursor_gap_count": cursor_gap_count,
            "duplicate_event_count": duplicate_event_count,
        },
        "route_sample_counts": samples,
        "route_failure_counts": failures,
        "route_maximum_unavailable_seconds": maximum_unavailable_seconds,
        "client_timeout_counts": client_timeouts_by_route,
        "integrity_failure_count": integrity_failures,
        "fail_closed_frame_count": fail_closed_frames,
        "maximum_book_age_seconds": maximum_book_age_seconds,
        "maximum_unresolved_gap_count": maximum_unresolved_gap_count,
        "maximum_persistence_queue_depth": maximum_persistence_queue_depth,
        "maximum_persistence_lag_events": maximum_persistence_lag_events,
        "live_stream_disconnect_count_delta": disconnect_delta,
        "disconnected_health_samples": disconnected_health_samples,
        "realtime_sqlite_query_ms_max": max(realtime_sqlite_query_ms, default=None),
        "read_sources": sorted(read_sources),
        "server_timing_failure_count": server_timing_failures,
        "response_body_bytes_maximum": response_body_bytes_maximum,
        "errors": errors,
    }
    args.output.write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8",
    )
    print(json.dumps(report, sort_keys=True))
    raise SystemExit(0 if report["passed"] else 1)


if __name__ == "__main__":
    main()
