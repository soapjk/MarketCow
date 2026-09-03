#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx


def validate_full_sync(
    payload: dict[str, Any], market_ids: list[str], expected_scope_id: str,
) -> dict[str, Any]:
    boundary = (
        payload.get("catalog_revision"),
        payload.get("cursor"),
        payload.get("projection_generation"),
        payload.get("scope_market_ids"),
        payload.get("freshness_checked_at"),
    )
    mixed_components = []
    for component_name in ("health", "bootstrap", "snapshot"):
        component = payload.get(component_name) or {}
        component_cursor = component.get("latest_cursor", component.get("cursor"))
        observed = (
            component.get("catalog_revision"),
            component_cursor,
            component.get("projection_generation"),
            component.get("scope_market_ids"),
            component.get("freshness_checked_at"),
        )
        if observed != boundary:
            mixed_components.append(component_name)

    health = payload.get("health") or {}
    bootstrap = payload.get("bootstrap") or {}
    snapshot = payload.get("snapshot") or {}
    markets = bootstrap.get("markets") or []
    books = snapshot.get("books") or {}
    bootstrap_by_market = {str(item["identity"]["market_id"]): item for item in markets}
    tick_mismatch_token_ids = []
    revision_binding_failures = []
    seen_tokens = set()
    for market_id in market_ids:
        market = bootstrap_by_market.get(market_id)
        if market is None:
            revision_binding_failures.append(market_id)
            continue
        instrument = market["rules"]["instrument"]
        outcomes = market["identity"]["outcomes"]
        token_ids = [str(item["token_id"]) for item in outcomes]
        seen_tokens.update(token_ids)
        market_books = [books.get(token_id) for token_id in token_ids]
        if any(book is None for book in market_books):
            revision_binding_failures.append(market_id)
            continue
        for token_id, book in zip(token_ids, market_books, strict=True):
            if instrument["price_increment"] != book["tick_size"]:
                tick_mismatch_token_ids.append(token_id)
        dynamic = [row for row in instrument.get("provenance") or [] if row.get("source") == "polymarket_clob"]
        if dynamic and not any(
            row.get("boundary_cursor") is not None
            and int(row["boundary_cursor"]) <= int(payload["cursor"])
            and row.get("projection_generation") is not None
            and int(row["projection_generation"]) <= int(payload["projection_generation"])
            and set(row.get("token_ids") or []) == set(token_ids)
            and row.get("tick_version") in {book.get("tick_version") for book in market_books}
            for row in dynamic
        ):
            revision_binding_failures.append(market_id)

    expected_health = {
        "status": "index_ready",
        "market_count": 100,
        "token_count": 200,
        "book_token_count": 200,
        "book_complete_market_count": 100,
        "active_market_count": 100,
        "terminal_market_count": 0,
        "complete_market_count": 100,
        "missing_market_count": 0,
        "scope_status": "exact_ready",
        "scope_id": expected_scope_id,
        "unresolved_gap_count": 0,
        "live_stream_connected": True,
        "live_stream_disconnect_count": 0,
        "events_read_source": "memory_projection",
        "realtime_sqlite_query_ms": 0.0,
    }
    health_mismatches = {
        key: {"expected": value, "observed": health.get(key)}
        for key, value in expected_health.items()
        if health.get(key) != value
    }
    scope_exact = (
        payload.get("scope_market_ids") == market_ids
        and set(bootstrap_by_market) == set(market_ids)
        and len(markets) == 100
        and len(books) == 200
        and len(seen_tokens) == 200
        and payload.get("scope_id") == expected_scope_id
    )
    passed = (
        not any(
            (
                mixed_components,
                tick_mismatch_token_ids,
                revision_binding_failures,
                health_mismatches,
            )
        )
        and scope_exact
    )
    return {
        "passed": passed,
        "cursor": int(payload["cursor"]),
        "catalog_revision": payload["catalog_revision"],
        "projection_generation": int(payload["projection_generation"]),
        "scope_exact": scope_exact,
        "market_count": len(markets),
        "book_count": len(books),
        "complete_market_count": health.get("book_complete_market_count"),
        "gap_count": health.get("unresolved_gap_count"),
        "disconnect_count": health.get("live_stream_disconnect_count"),
        "events_read_source": health.get("events_read_source"),
        "realtime_sqlite_query_ms": health.get("realtime_sqlite_query_ms"),
        "derived_index_error": health.get("derived_index_error"),
        "tick_consistent_token_count": 200 - len(set(tick_mismatch_token_ids)),
        "tick_mismatch_token_ids": sorted(set(tick_mismatch_token_ids)),
        "mixed_revision_components": mixed_components,
        "revision_binding_failure_market_ids": sorted(set(revision_binding_failures)),
        "health_mismatches": health_mismatches,
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Verify two advancing atomic full-sync rounds on an exact scope",
    )
    parser.add_argument("--scope-manifest", required=True, type=Path)
    parser.add_argument("--expected-scope-id", required=True)
    parser.add_argument("--port", action="append", type=int, required=True)
    parser.add_argument("--rounds", type=int, default=2)
    parser.add_argument("--round-interval-seconds", type=float, default=5)
    parser.add_argument("--output", required=True, type=Path)
    arguments = parser.parse_args()
    if arguments.rounds < 2:
        parser.error("at least two verification rounds are required")
    if arguments.round_interval_seconds <= 0:
        parser.error("round interval must be positive")
    manifest_path = arguments.scope_manifest.resolve(strict=True)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("scope_id") != arguments.expected_scope_id:
        parser.error("scope manifest does not identify the exact acceptance scope")
    market_ids = [str(item) for item in manifest.get("market_ids") or []]
    if len(market_ids) != 100 or len(set(market_ids)) != 100:
        parser.error("verification requires exactly 100 unique markets")
    params = [("market_id", market_id) for market_id in market_ids]

    observations: dict[str, list[dict[str, Any]]] = {str(port): [] for port in arguments.port}
    failures: list[dict[str, Any]] = []
    started_at = datetime.now(timezone.utc)
    with httpx.Client(timeout=30, trust_env=False) as client:
        for round_index in range(arguments.rounds):
            for port in arguments.port:
                observed_at = datetime.now(timezone.utc).isoformat()
                try:
                    response = client.get(
                        f"http://127.0.0.1:{port}/v1/prediction-markets/polymarket/live/full-sync",
                        params=params,
                    )
                    response.raise_for_status()
                    evidence = validate_full_sync(
                        response.json(), market_ids, arguments.expected_scope_id,
                    )
                    evidence.update(
                        {
                            "round": round_index + 1,
                            "observed_at": observed_at,
                            "http_status": response.status_code,
                        }
                    )
                    observations[str(port)].append(evidence)
                    if not evidence["passed"]:
                        failures.append(
                            {
                                "port": port,
                                "round": round_index + 1,
                                "error": "full-sync integrity validation failed",
                            }
                        )
                except Exception as exc:
                    failures.append(
                        {
                            "port": port,
                            "round": round_index + 1,
                            "error": f"{type(exc).__name__}: {exc}",
                        }
                    )
            if round_index + 1 < arguments.rounds:
                time.sleep(arguments.round_interval_seconds)

    criteria = {}
    cursor_ranges = {}
    for port in arguments.port:
        samples = observations[str(port)]
        cursors = [sample["cursor"] for sample in samples]
        cursor_ranges[str(port)] = {
            "start": cursors[0] if cursors else None,
            "finish": cursors[-1] if cursors else None,
        }
        criteria[f"port_{port}_all_rounds_passed"] = len(samples) == arguments.rounds and all(
            sample["passed"] for sample in samples
        )
        criteria[f"port_{port}_cursor_advanced"] = len(cursors) >= 2 and cursors[-1] > cursors[0]
    report = {
        "schema": "marketcow.polymarket.exact-scope-verification.v1",
        "passed": not failures and all(criteria.values()),
        "scope_id": arguments.expected_scope_id,
        "scope_manifest": str(manifest_path),
        "market_count": len(market_ids),
        "ports": arguments.port,
        "rounds": arguments.rounds,
        "round_interval_seconds": arguments.round_interval_seconds,
        "started_at": started_at.isoformat(),
        "finished_at": datetime.now(timezone.utc).isoformat(),
        "commit": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
        "criteria": criteria,
        "cursor_ranges": cursor_ranges,
        "failures": failures,
        "observations": observations,
        "activation_evidence": {
            "endpoints": {
                str(port): {
                    "http_status": samples[-1]["http_status"],
                    "status": "index_ready",
                    "market_count": samples[-1]["market_count"],
                    "book_count": samples[-1]["book_count"],
                    "complete_market_count": samples[-1]["complete_market_count"],
                    "tick_consistent_token_count": samples[-1]["tick_consistent_token_count"],
                    "gap_count": samples[-1]["gap_count"],
                    "disconnect_count": samples[-1]["disconnect_count"],
                    "cursors": [sample["cursor"] for sample in samples],
                }
                for port in arguments.port
                if (samples := observations[str(port)])
            },
        },
    }
    arguments.output.parent.mkdir(parents=True, exist_ok=True)
    arguments.output.write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(
        json.dumps(
            {
                "passed": report["passed"],
                "criteria": criteria,
                "cursor_ranges": cursor_ranges,
                "failures": failures,
                "output": str(arguments.output.resolve()),
            },
            sort_keys=True,
        )
    )
    if not report["passed"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
