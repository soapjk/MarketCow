#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx
import websockets


READ_ONLY_PATHS = [
    "/v1/orders", "/v1/order", "/v1/cancel", "/v1/sign", "/v1/wallet",
    "/v1/trade", "/v1/execute", "/v1/submit",
    "/v1/prediction-markets/polymarket/orders",
    "/v1/prediction-markets/polymarket/cancel",
    "/v1/prediction-markets/polymarket/sign",
    "/v1/prediction-markets/polymarket/wallet",
    "/v1/prediction-markets/polymarket/trade",
    "/v1/prediction-markets/polymarket/submit",
]


def now() -> str:
    return datetime.now(timezone.utc).isoformat()


def validate_boundary(
    payload: dict[str, Any],
    scope_id: str,
    expected_market_count: int,
    expected_relation_count: int,
    expected_universe_generation: int | None,
) -> dict[str, Any]:
    snapshot = payload.get("snapshot") or {}
    checkpoint = payload.get("checkpoint") or {}
    health = payload.get("health") or {}
    books = snapshot.get("books") or []
    markets = snapshot.get("markets") or []
    relations = snapshot.get("negative_risk_relations") or []
    books_by_token = {str(book.get("token_id")): book for book in books}
    mismatches = []
    missing = []
    bad_revisions = []
    bad_fee_schedules = []
    tokens = []
    for market in markets:
        facts = market.get("instrument_facts") or {}
        market_id = str(market.get("market_id"))
        if re.fullmatch(r"[0-9a-f]{64}", str(facts.get("revision", ""))) is None:
            bad_revisions.append(market_id)
        fee = facts.get("fee_schedule") or {}
        if (
            re.fullmatch(r"[0-9a-f]{64}", str(fee.get("revision", ""))) is None
            or not isinstance(fee.get("maker_rate"), str)
            or not isinstance(fee.get("taker_rate"), str)
            or fee.get("calculation_status") not in {"executable", "informational_only"}
            or not fee.get("provenance")
        ):
            bad_fee_schedules.append(market_id)
        outcomes = market.get("outcomes") or []
        if len(outcomes) != 2:
            missing.append({"market_id": market_id, "reason": "outcome_count"})
        for outcome in outcomes:
            token_id = str(outcome.get("token_id"))
            tokens.append(token_id)
            book = books_by_token.get(token_id)
            if book is None:
                missing.append({"market_id": market_id, "token_id": token_id})
            elif facts.get("price_increment") != book.get("tick_size"):
                mismatches.append({
                    "market_id": market_id,
                    "token_id": token_id,
                    "facts": facts.get("price_increment"),
                    "book": book.get("tick_size"),
                })
    boundary = payload.get("boundary_cursor")
    atomic_values = {
        boundary,
        payload.get("projection_generation"),
        snapshot.get("generation"),
        (snapshot.get("watermarks") or {}).get("published_cursor"),
        (snapshot.get("watermarks") or {}).get("persisted_cursor"),
        checkpoint.get("checkpoint_cursor"),
        checkpoint.get("persisted_cursor"),
    }
    universe = payload.get("universe") or {}
    dynamic_universe_valid = expected_universe_generation is None or (
        payload.get("universe_schema_version") == "marketcow.polymarket.universe.v2"
        and payload.get("universe_id") == scope_id
        and payload.get("universe_generation") == expected_universe_generation
        and universe.get("universe_id") == scope_id
        and universe.get("generation") == expected_universe_generation
        and len(universe.get("active_markets") or []) == expected_market_count
        and universe.get("minimum_market_count") <= expected_market_count
        and universe.get("target_market_count") >= expected_market_count
        and all(
            item.get("market_id") and item.get("reason_code")
            and isinstance(item.get("retryable"), bool)
            and (item.get("retry_after") is not None) == item.get("retryable")
            for item in universe.get("excluded_markets") or []
        )
    )
    passed = (
        payload.get("schema_version") == "marketcow.polymarket.live.v3"
        and payload.get("scope_id") == scope_id
        and health.get("ready") is True
        and health.get("unresolved_gap_count") == 0
        and snapshot.get("status") == "ready"
        and len(markets) == expected_market_count
        and len(books) == expected_market_count * 2
        and len(set(tokens)) == expected_market_count * 2
        and len(relations) == expected_relation_count
        and not mismatches
        and not missing
        and not bad_revisions
        and not bad_fee_schedules
        and dynamic_universe_valid
        and len(atomic_values) == 1
        and re.fullmatch(r"[0-9a-f]{64}", str(snapshot.get("catalog_revision", ""))) is not None
        and re.fullmatch(r"[0-9a-f]{64}", str(checkpoint.get("state_sha256", ""))) is not None
    )
    return {
        "passed": passed,
        "boundary_cursor": boundary,
        "market_count": len(markets),
        "book_count": len(books),
        "token_count": len(set(tokens)),
        "relation_count": len(relations),
        "tick_mismatch_count": len(mismatches),
        "missing_count": len(missing),
        "bad_revision_count": len(bad_revisions),
        "bad_fee_schedule_count": len(bad_fee_schedules),
        "universe_generation": payload.get("universe_generation"),
        "dynamic_universe_valid": dynamic_universe_valid,
        "catalog_revision": snapshot.get("catalog_revision"),
        "checkpoint_state_sha256": checkpoint.get("state_sha256"),
        "fail_closed_reason": health.get("fail_closed_reason"),
    }


async def websocket_gate(base_url: str, scope_id: str, boundary: int) -> dict[str, Any]:
    uri = base_url.replace("http://", "ws://").replace("https://", "wss://")
    uri += f"/v1/market-data/stream?after_cursor={boundary}"
    frames = []
    async with websockets.connect(uri, open_timeout=5, close_timeout=3, proxy=None) as socket:
        for _ in range(6):
            frames.append(json.loads(await asyncio.wait_for(socket.recv(), 10)))
    subscription = frames[0]
    cursors = [int(frame["cursor"]) for frame in frames[1:]]
    return {
        "passed": (
            subscription.get("type") == "subscription"
            and subscription.get("scope_id") == scope_id
            and int(subscription.get("boundary_cursor")) >= boundary
            and cursors == list(range(boundary + 1, boundary + 1 + len(cursors)))
        ),
        "requested_boundary": boundary,
        "subscription_boundary": subscription.get("boundary_cursor"),
        "event_cursors": cursors,
    }


async def expired_cursor_gate(base_url: str, scope_id: str) -> dict[str, Any]:
    uri = base_url.replace("http://", "ws://").replace("https://", "wss://")
    uri += "/v1/market-data/stream?after_cursor=0"
    frame = None
    close = None
    try:
        async with websockets.connect(uri, open_timeout=5, close_timeout=3, proxy=None) as socket:
            frame = json.loads(await asyncio.wait_for(socket.recv(), 5))
            await socket.recv()
    except websockets.ConnectionClosed as error:
        close = {"code": error.code, "reason": error.reason}
    return {
        "passed": (
            frame is not None
            and frame.get("type") == "global_resync_required"
            and frame.get("scope_id") == scope_id
            and frame.get("reason") == "event_cursor_expired"
            and close == {"code": 1008, "reason": "full_sync_required"}
        ),
        "frame": frame,
        "close": close,
    }


def write_report(path: Path, report: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    temporary.replace(path)


def main() -> None:
    parser = argparse.ArgumentParser(description="Verify a sustained Rust Polymarket live window")
    parser.add_argument("--base-url", default="http://127.0.0.1:18872")
    parser.add_argument("--expected-scope-id", required=True)
    parser.add_argument("--expected-scope-schema", default="marketcow.polymarket.scope-discovery.v4")
    parser.add_argument("--expected-market-count", type=int, default=100)
    parser.add_argument("--expected-relation-count", type=int, default=4)
    parser.add_argument("--expected-universe-generation", type=int)
    parser.add_argument("--duration-seconds", type=int, default=300)
    parser.add_argument("--startup-timeout-seconds", type=int, default=180)
    parser.add_argument("--interval-seconds", type=float, default=1.0)
    parser.add_argument("--output", type=Path, required=True)
    arguments = parser.parse_args()
    if arguments.duration_seconds < 5 or arguments.interval_seconds <= 0:
        parser.error("duration must be >=5 seconds and interval must be positive")

    report: dict[str, Any] = {
        "schema_version": "marketcow.polymarket.live-stability-gate.v1",
        "expected_scope_id": arguments.expected_scope_id,
        "base_url": arguments.base_url,
        "duration_seconds": arguments.duration_seconds,
        "interval_seconds": arguments.interval_seconds,
        "started_at": now(),
        "window_started_at": None,
        "window_finished_at": None,
        "window_elapsed_seconds": None,
        "startup_observations": [],
        "samples": [],
        "failures": [],
        "passed": False,
    }
    startup_deadline = time.monotonic() + arguments.startup_timeout_seconds
    window_deadline = None
    window_started_monotonic = None
    with httpx.Client(timeout=15, trust_env=False) as client:
        while window_deadline is None:
            observed_at = now()
            try:
                scope_response = client.get(arguments.base_url + "/v1/prediction-markets/polymarket/live/scope")
                full_response = client.get(arguments.base_url + "/v1/prediction-markets/polymarket/live/full-sync")
                scope = scope_response.json()
                full = full_response.json()
                boundary = validate_boundary(
                    full, arguments.expected_scope_id, arguments.expected_market_count,
                    arguments.expected_relation_count, arguments.expected_universe_generation,
                ) if full_response.status_code == 200 else None
                ready = (
                    scope_response.status_code == 200
                    and scope.get("schema_version") == arguments.expected_scope_schema
                    and scope.get("active_scope_id") == arguments.expected_scope_id
                    and scope.get("ready") is True
                    and scope.get("scope_status") == "ready"
                    and scope.get("market_count") == arguments.expected_market_count
                    and scope.get("token_count") == arguments.expected_market_count * 2
                    and (
                        arguments.expected_universe_generation is None
                        or scope.get("generation") == arguments.expected_universe_generation
                    )
                    and scope.get("real_order_submission_enabled") is False
                    and full_response.status_code == 200
                    and boundary is not None
                    and boundary["passed"]
                )
                observation = {
                    "observed_at": observed_at,
                    "scope_status": scope_response.status_code,
                    "scope_ready": scope.get("ready"),
                    "full_sync_status": full_response.status_code,
                    "boundary": boundary,
                }
            except Exception as error:
                ready = False
                observation = {"observed_at": observed_at, "error": f"{type(error).__name__}: {error}"}
            report["startup_observations"].append(observation)
            write_report(arguments.output, report)
            if ready:
                report["window_started_at"] = now()
                window_started_monotonic = time.monotonic()
                window_deadline = window_started_monotonic + arguments.duration_seconds
                break
            if time.monotonic() >= startup_deadline:
                report["failures"].append({"gate": "startup", "reason": "startup_timeout"})
                report["window_finished_at"] = now()
                write_report(arguments.output, report)
                raise SystemExit(1)
            time.sleep(arguments.interval_seconds)

        assert window_started_monotonic is not None
        while time.monotonic() < window_deadline:
            observed_at = now()
            try:
                scope_response = client.get(arguments.base_url + "/v1/prediction-markets/polymarket/live/scope")
                full_response = client.get(arguments.base_url + "/v1/prediction-markets/polymarket/live/full-sync")
                scope = scope_response.json()
                boundary = validate_boundary(
                    full_response.json(), arguments.expected_scope_id,
                    arguments.expected_market_count, arguments.expected_relation_count,
                    arguments.expected_universe_generation,
                ) if full_response.status_code == 200 else None
                passed = (
                    scope_response.status_code == 200
                    and scope.get("ready") is True
                    and scope.get("scope_status") == "ready"
                    and scope.get("schema_version") == arguments.expected_scope_schema
                    and scope.get("market_count") == arguments.expected_market_count
                    and scope.get("token_count") == arguments.expected_market_count * 2
                    and (
                        arguments.expected_universe_generation is None
                        or scope.get("generation") == arguments.expected_universe_generation
                    )
                    and scope.get("real_order_submission_enabled") is False
                    and full_response.status_code == 200
                    and boundary is not None
                    and boundary["passed"]
                )
                sample = {
                    "observed_at": observed_at,
                    "passed": passed,
                    "scope_http_status": scope_response.status_code,
                    "scope_ready": scope.get("ready"),
                    "full_sync_http_status": full_response.status_code,
                    "boundary": boundary,
                }
            except Exception as error:
                sample = {"observed_at": observed_at, "passed": False, "error": f"{type(error).__name__}: {error}"}
            report["samples"].append(sample)
            if not sample["passed"]:
                report["failures"].append({"gate": "stability_sample", "sample": sample})
            write_report(arguments.output, report)
            time.sleep(min(arguments.interval_seconds, max(0.0, window_deadline - time.monotonic())))

        report["window_elapsed_seconds"] = time.monotonic() - window_started_monotonic
        cursors = [sample["boundary"]["boundary_cursor"] for sample in report["samples"] if sample.get("boundary")]
        try:
            report["websocket"] = asyncio.run(websocket_gate(arguments.base_url, arguments.expected_scope_id, cursors[-1]))
            report["expired_cursor"] = asyncio.run(expired_cursor_gate(arguments.base_url, arguments.expected_scope_id))
        except Exception as error:
            report["failures"].append({"gate": "websocket", "error": f"{type(error).__name__}: {error}"})
        report["read_only"] = {}
        for path in READ_ONLY_PATHS:
            response = client.get(arguments.base_url + path)
            report["read_only"][path] = response.status_code
        report["window_finished_at"] = now()
        report["cursor_start"] = cursors[0] if cursors else None
        report["cursor_finish"] = cursors[-1] if cursors else None
        report["passed"] = (
            report["window_elapsed_seconds"] >= arguments.duration_seconds
            and len(report["samples"]) >= 2
            and not report["failures"]
            and bool(cursors)
            and cursors[-1] > cursors[0]
            and report.get("websocket", {}).get("passed") is True
            and report.get("expired_cursor", {}).get("passed") is True
            and all(status == 404 for status in report["read_only"].values())
        )
        write_report(arguments.output, report)
    print(json.dumps({
        "passed": report["passed"],
        "samples": len(report["samples"]),
        "failures": len(report["failures"]),
        "cursor_start": report.get("cursor_start"),
        "cursor_finish": report.get("cursor_finish"),
        "output": str(arguments.output.resolve()),
    }, sort_keys=True))
    if not report["passed"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
