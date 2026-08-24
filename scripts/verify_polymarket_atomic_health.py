#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx


def _validate_health(payload: dict[str, Any], market_ids: list[str]) -> None:
    latest_cursor = int(payload["latest_cursor"])
    persisted_cursor = int(payload["persisted_cursor"])
    lag = int(payload["persistence_lag_events"])
    if persisted_cursor > latest_cursor:
        raise ValueError(f"persisted_cursor {persisted_cursor} exceeds latest_cursor {latest_cursor}")
    if lag != latest_cursor - persisted_cursor:
        raise ValueError(f"persistence lag {lag} does not equal {latest_cursor - persisted_cursor}")
    expected = {
        "status": "index_ready",
        "market_count": 100,
        "token_count": 200,
        "book_token_count": 200,
        "book_complete_market_count": 100,
        "unresolved_gap_count": 0,
        "live_stream_connected": True,
        "live_stream_disconnect_count": 0,
        "events_read_source": "memory_projection",
        "realtime_sqlite_query_ms": 0.0,
    }
    mismatches = {
        key: {"expected": value, "observed": payload.get(key)}
        for key, value in expected.items()
        if payload.get(key) != value
    }
    if payload.get("scope_market_ids") != market_ids:
        mismatches["scope_market_ids"] = {
            "expected": market_ids,
            "observed": payload.get("scope_market_ids"),
        }
    if int(payload.get("projection_generation", 0)) < 1:
        mismatches["projection_generation"] = {
            "expected": ">=1",
            "observed": payload.get("projection_generation"),
        }
    if mismatches:
        raise ValueError(f"health contract mismatch: {mismatches}")


def _sample_port(
    port: int,
    market_ids: list[str],
    sample_count: int,
    interval_seconds: float,
    results: dict[int, dict[str, Any]],
) -> None:
    params = [("market_id", market_id) for market_id in market_ids]
    url = f"http://127.0.0.1:{port}/v1/prediction-markets/polymarket/live/health"
    samples: list[dict[str, Any]] = []
    started = time.monotonic()
    with httpx.Client(timeout=10, trust_env=False) as client:
        for index in range(sample_count):
            requested_at = datetime.now(timezone.utc)
            response = client.get(url, params=params)
            response.raise_for_status()
            payload = response.json()
            _validate_health(payload, market_ids)
            samples.append(
                {
                    "index": index,
                    "requested_at": requested_at.isoformat(),
                    "latest_cursor": payload["latest_cursor"],
                    "persisted_cursor": payload["persisted_cursor"],
                    "persistence_lag_events": payload["persistence_lag_events"],
                    "projection_generation": payload["projection_generation"],
                }
            )
            deadline = started + ((index + 1) * interval_seconds)
            remaining = deadline - time.monotonic()
            if remaining > 0 and index + 1 < sample_count:
                time.sleep(remaining)
    results[port] = {
        "sample_count": len(samples),
        "duration_seconds": time.monotonic() - started,
        "first": samples[0],
        "last": samples[-1],
        "minimum_latest_cursor": min(row["latest_cursor"] for row in samples),
        "maximum_latest_cursor": max(row["latest_cursor"] for row in samples),
        "maximum_persistence_lag_events": max(row["persistence_lag_events"] for row in samples),
        "all_persisted_lte_latest": all(row["persisted_cursor"] <= row["latest_cursor"] for row in samples),
        "all_lag_exact": all(
            row["persistence_lag_events"] == row["latest_cursor"] - row["persisted_cursor"] for row in samples
        ),
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Verify atomic MarketCow scoped-health cursor snapshots",
    )
    parser.add_argument("--scope-manifest", required=True, type=Path)
    parser.add_argument("--expected-scope-id", required=True)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--samples", type=int, default=200)
    parser.add_argument("--interval-seconds", type=float, default=0.25)
    parser.add_argument("--port", type=int, action="append", default=[])
    arguments = parser.parse_args()
    if arguments.samples < 200:
        parser.error("acceptance requires at least 200 samples per port")
    if arguments.interval_seconds <= 0:
        parser.error("sample interval must be positive")
    ports = arguments.port or [8790, 8791]
    manifest = json.loads(arguments.scope_manifest.resolve().read_bytes())
    if manifest.get("scope_id") != arguments.expected_scope_id:
        parser.error("scope manifest does not identify the exact acceptance scope")
    market_ids = [str(item) for item in manifest.get("market_ids") or []]
    if len(market_ids) != 100 or len(set(market_ids)) != 100:
        parser.error("scope manifest must contain exactly 100 unique markets")

    results: dict[int, dict[str, Any]] = {}
    failures: list[str] = []

    def run(port: int) -> None:
        try:
            _sample_port(
                port,
                market_ids,
                arguments.samples,
                arguments.interval_seconds,
                results,
            )
        except Exception as exc:  # Preserve exact evidence before failing closed.
            failures.append(f"port {port}: {type(exc).__name__}: {exc}")

    threads = [threading.Thread(target=run, args=(port,)) for port in ports]
    started_at = datetime.now(timezone.utc)
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    finished_at = datetime.now(timezone.utc)
    report = {
        "schema": "marketcow.polymarket.atomic-health-verification.v1",
        "passed": not failures and len(results) == len(ports),
        "scope_id": arguments.expected_scope_id,
        "scope_manifest": str(arguments.scope_manifest.resolve()),
        "market_count": len(market_ids),
        "ports": ports,
        "samples_per_port": arguments.samples,
        "interval_seconds": arguments.interval_seconds,
        "started_at": started_at.isoformat(),
        "finished_at": finished_at.isoformat(),
        "commit": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
        "results": {str(port): results[port] for port in sorted(results)},
        "failures": failures,
    }
    arguments.output.parent.mkdir(parents=True, exist_ok=True)
    arguments.output.write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(report, sort_keys=True))
    if not report["passed"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
