#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx


AFFECTED_MARKETS = {"1296001", "1296002", "1296004"}


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Verify atomic dynamic tick facts on an exact live scope."
    )
    parser.add_argument("--scope-manifest", required=True, type=Path)
    parser.add_argument("--port", action="append", type=int, required=True)
    parser.add_argument("--duration-seconds", type=float, default=15)
    parser.add_argument("--output", required=True, type=Path)
    arguments = parser.parse_args()
    if arguments.duration_seconds < 5:
        parser.error("duration must be at least five seconds")

    manifest = json.loads(arguments.scope_manifest.read_text(encoding="utf-8"))
    market_ids = [str(item) for item in manifest["market_ids"]]
    if manifest.get("scope_id") != arguments.scope_manifest.parent.name:
        parser.error("scope manifest path and scope_id differ")
    if len(market_ids) != 100 or len(set(market_ids)) != 100:
        parser.error("verification requires one exact 100-market scope")
    params = [("market_id", market_id) for market_id in market_ids]
    deadline = time.monotonic() + arguments.duration_seconds
    observations: dict[str, list[dict[str, Any]]] = {
        str(port): [] for port in arguments.port
    }
    failures: list[dict[str, Any]] = []

    with httpx.Client(timeout=15, trust_env=False) as client:
        while time.monotonic() < deadline:
            for port in arguments.port:
                observed_at = datetime.now(timezone.utc).isoformat()
                try:
                    response = client.get(
                        f"http://127.0.0.1:{port}/v1/prediction-markets/"
                        "polymarket/live/full-sync",
                        params=params,
                    )
                    payload = response.json()
                    if response.status_code != 200:
                        raise RuntimeError(
                            f"HTTP {response.status_code}: {payload}"
                        )
                    health = payload["health"]
                    bootstrap_by_market = {
                        item["identity"]["market_id"]: item
                        for item in payload["bootstrap"]["markets"]
                    }
                    books = payload["snapshot"]["books"]
                    tick_mismatches = []
                    empty_side_token_ids = []
                    for market_id, market in bootstrap_by_market.items():
                        instrument = market["rules"]["instrument"]
                        token_ids = [
                            item["token_id"]
                            for item in market["identity"]["outcomes"]
                        ]
                        book_ticks = {books[token_id]["tick_size"] for token_id in token_ids}
                        if book_ticks != {instrument["price_increment"]}:
                            tick_mismatches.append(market_id)
                        for token_id in token_ids:
                            book = books[token_id]
                            if not book["bids"] or not book["asks"]:
                                empty_side_token_ids.append(token_id)
                    affected = []
                    for market_id in sorted(AFFECTED_MARKETS):
                        market = bootstrap_by_market[market_id]
                        instrument = market["rules"]["instrument"]
                        token_ids = [
                            item["token_id"]
                            for item in market["identity"]["outcomes"]
                        ]
                        affected.append({
                            "market_id": market_id,
                            "instrument_revision": instrument["revision"],
                            "price_increment": instrument["price_increment"],
                            "provenance": instrument["provenance"][-1],
                            "tokens": [{
                                "token_id": token_id,
                                "tick_size": books[token_id]["tick_size"],
                                "tick_version": books[token_id]["tick_version"],
                            } for token_id in token_ids],
                        })
                    observation = {
                        "observed_at": observed_at,
                        "http_status": response.status_code,
                        "health_status": health["status"],
                        "cursor": payload["cursor"],
                        "projection_generation": payload["projection_generation"],
                        "scope_market_count": len(payload["scope_market_ids"]),
                        "book_count": len(books),
                        "complete_market_count": health["book_complete_market_count"],
                        "unresolved_gap_count": health["unresolved_gap_count"],
                        "live_stream_connected": health["live_stream_connected"],
                        "live_stream_disconnect_count": health[
                            "live_stream_disconnect_count"
                        ],
                        "events_read_source": health["events_read_source"],
                        "realtime_sqlite_query_ms": health[
                            "realtime_sqlite_query_ms"
                        ],
                        "derived_index_error": health.get("derived_index_error"),
                        "tick_mismatch_market_ids": tick_mismatches,
                        "empty_side_token_ids": empty_side_token_ids,
                        "affected": affected,
                    }
                    observations[str(port)].append(observation)
                except Exception as exc:
                    failures.append({
                        "observed_at": observed_at,
                        "port": port,
                        "error": f"{type(exc).__name__}: {exc}",
                    })
            time.sleep(min(1, max(0, deadline - time.monotonic())))

    criteria: dict[str, bool] = {}
    for port, samples in observations.items():
        criteria[f"port_{port}_sampled"] = bool(samples)
        criteria[f"port_{port}_cursor_advanced"] = (
            bool(samples) and samples[-1]["cursor"] > samples[0]["cursor"]
        )
        criteria[f"port_{port}_all_boundaries_valid"] = bool(samples) and all(
            sample["http_status"] == 200
            and sample["health_status"] == "index_ready"
            and sample["scope_market_count"] == 100
            and sample["book_count"] == 200
            and sample["complete_market_count"] == 100
            and sample["unresolved_gap_count"] == 0
            and sample["live_stream_connected"] is True
            and sample["live_stream_disconnect_count"] == 0
            and sample["events_read_source"] == "memory_projection"
            and sample["realtime_sqlite_query_ms"] == 0
            and not sample["tick_mismatch_market_ids"]
            and not sample["empty_side_token_ids"]
            for sample in samples
        )
    result = {
        "schema_version": "marketcow.polymarket.dynamic-tick-verification.v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "scope_id": manifest["scope_id"],
        "market_count": len(market_ids),
        "duration_seconds": arguments.duration_seconds,
        "criteria": criteria,
        "passed": not failures and all(criteria.values()),
        "failures": failures,
        "observations": observations,
    }
    arguments.output.parent.mkdir(parents=True, exist_ok=True)
    arguments.output.write_text(
        json.dumps(result, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps({
        "passed": result["passed"],
        "criteria": criteria,
        "failures": failures,
        "output": str(arguments.output.resolve()),
    }, sort_keys=True))
    if not result["passed"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
