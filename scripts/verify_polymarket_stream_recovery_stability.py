#!/usr/bin/env python3
"""Verify a sustained strict MarketCow stream without relaxing fail-closed semantics."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import re
import subprocess
import time
from collections import Counter
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


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def atomic_write(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    temporary.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    temporary.replace(path)


def validate_boundary(scope: dict[str, Any], full: dict[str, Any]) -> dict[str, Any]:
    snapshot = full.get("snapshot") or {}
    checkpoint = full.get("checkpoint") or {}
    universe = full.get("universe") or {}
    markets = snapshot.get("markets") or []
    books = snapshot.get("books") or []
    by_token = {str(book.get("token_id")): book for book in books}
    tokens: list[str] = []
    tick_mismatches: list[str] = []
    bad_revisions: list[str] = []
    for market in markets:
        facts = market.get("instrument_facts") or {}
        market_id = str(market.get("market_id"))
        if re.fullmatch(r"[0-9a-f]{64}", str(facts.get("revision", ""))) is None:
            bad_revisions.append(market_id)
        for outcome in market.get("outcomes") or []:
            token = str(outcome.get("token_id"))
            tokens.append(token)
            book = by_token.get(token)
            if book is None or facts.get("price_increment") != book.get("tick_size"):
                tick_mismatches.append(token)
    boundary = full.get("boundary_cursor")
    atomic_cursors = {
        boundary,
        full.get("projection_generation"),
        snapshot.get("generation"),
        (snapshot.get("watermarks") or {}).get("published_cursor"),
        (snapshot.get("watermarks") or {}).get("persisted_cursor"),
        checkpoint.get("checkpoint_cursor"),
        checkpoint.get("persisted_cursor"),
    }
    generation = scope.get("generation")
    passed = (
        scope.get("schema_version") == "marketcow.polymarket.scope-discovery.v3"
        and scope.get("ready") is True
        and scope.get("scope_status") == "ready"
        and scope.get("market_count") == 100
        and scope.get("token_count") == 200
        and scope.get("real_order_submission_enabled") is False
        and full.get("schema_version") == "marketcow.polymarket.live.v2"
        and full.get("scope_id") == scope.get("active_scope_id")
        and full.get("universe_id") == scope.get("universe_id")
        and full.get("universe_generation") == generation
        and universe.get("generation") == generation
        and len(universe.get("active_markets") or []) == 100
        and len(markets) == 100
        and len(books) == 200
        and len(set(tokens)) == 200
        and not tick_mismatches
        and not bad_revisions
        and not snapshot.get("unresolved_gaps")
        and snapshot.get("status") == "ready"
        and len(atomic_cursors) == 1
        and re.fullmatch(r"[0-9a-f]{64}", str(snapshot.get("catalog_revision", "")))
        is not None
        and re.fullmatch(r"[0-9a-f]{64}", str(checkpoint.get("state_sha256", "")))
        is not None
    )
    return {
        "passed": passed,
        "generation": generation,
        "boundary_cursor": boundary,
        "catalog_revision": snapshot.get("catalog_revision"),
        "market_count": len(markets),
        "book_count": len(books),
        "relation_count": len(snapshot.get("negative_risk_relations") or []),
        "tick_mismatch_count": len(tick_mismatches),
        "bad_revision_count": len(bad_revisions),
        "gap_count": len(snapshot.get("unresolved_gaps") or []),
        "active_market_ids": sorted(str(item["market_id"]) for item in universe.get("active_markets") or []),
    }


async def read_boundary(client: httpx.AsyncClient, base_url: str) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    scope_response, full_response = await asyncio.gather(
        client.get(base_url + "/v1/prediction-markets/polymarket/live/scope"),
        client.get(base_url + "/v1/prediction-markets/polymarket/live/full-sync"),
    )
    if scope_response.status_code != 200 or full_response.status_code != 200:
        raise RuntimeError(f"boundary HTTP scope={scope_response.status_code} full={full_response.status_code}")
    scope, full = scope_response.json(), full_response.json()
    evidence = validate_boundary(scope, full)
    if not evidence["passed"]:
        raise RuntimeError(f"atomic boundary invalid: {evidence}")
    return scope, full, evidence


async def main_async(arguments: argparse.Namespace) -> dict[str, Any]:
    started = time.monotonic()
    deadline = started + arguments.duration_seconds
    report: dict[str, Any] = {
        "schema_version": "marketcow.polymarket.stream-recovery-stability.v1",
        "started_at": utc_now(),
        "duration_seconds": arguments.duration_seconds,
        "base_url": arguments.base_url,
        "runtime": {
            "pid": arguments.expected_pid,
            "commit": arguments.expected_commit,
            "binary_path": str(arguments.binary.resolve()),
            "binary_sha256": hashlib.sha256(arguments.binary.read_bytes()).hexdigest(),
            "launchd_label": arguments.launchd_label,
        },
        "samples": [],
        "stream": {
            "event_count": 0,
            "event_types": {},
            "source_event_types": {},
            "resync_reasons": {},
            "universe_changes": [],
            "connections": 0,
            "ordinary_unapplied_frames": 0,
            "ordinary_gap_frames": 0,
            "cursor_discontinuities": 0,
        },
        "failures": [],
        "passed": False,
    }
    atomic_write(arguments.output, report)
    if report["runtime"]["binary_sha256"] != arguments.expected_binary_sha256:
        report["failures"].append({"gate": "binary_identity", "observed": report["runtime"]["binary_sha256"]})
        return report
    try:
        os.kill(arguments.expected_pid, 0)
    except OSError as error:
        report["failures"].append({"gate": "pid_identity", "error": str(error)})
        return report
    event_types: Counter[str] = Counter()
    source_types: Counter[str] = Counter()
    resync_reasons: Counter[str] = Counter()
    last_sample_at = 0.0
    ws_url = arguments.base_url.replace("http://", "ws://") + "/v1/market-data/stream"
    async with httpx.AsyncClient(timeout=15, trust_env=False) as client:
        while time.monotonic() < deadline:
            try:
                scope, _full, boundary = await read_boundary(client, arguments.base_url)
            except Exception as error:
                report["failures"].append({"gate": "full_sync", "observed_at": utc_now(), "error": f"{type(error).__name__}: {error}"})
                atomic_write(arguments.output, report)
                await asyncio.sleep(min(5, max(0, deadline - time.monotonic())))
                continue
            report["samples"].append({"observed_at": utc_now(), **boundary})
            connection_generation = int(scope["generation"])
            connection_markets = set(boundary["active_market_ids"])
            after_cursor = int(boundary["boundary_cursor"])
            report["stream"]["connections"] += 1
            controlled_transition = False
            try:
                async with websockets.connect(
                    f"{ws_url}?after_cursor={after_cursor}", open_timeout=10,
                    close_timeout=3, proxy=None, max_queue=4096,
                ) as socket:
                    subscription = json.loads(await asyncio.wait_for(socket.recv(), 10))
                    if (
                        subscription.get("type") != "subscription"
                        or subscription.get("protocol_version") != "marketcow.market-stream.v2"
                        or int(subscription.get("boundary_cursor", -1)) < after_cursor
                    ):
                        raise RuntimeError(f"invalid subscription: {subscription}")
                    cursor = after_cursor
                    while time.monotonic() < deadline:
                        now_monotonic = time.monotonic()
                        if now_monotonic - last_sample_at >= arguments.sample_interval_seconds:
                            try:
                                _sample_scope, _sample_full, sample = await read_boundary(
                                    client, arguments.base_url
                                )
                                report["samples"].append({"observed_at": utc_now(), **sample})
                            except Exception as error:
                                report["failures"].append({
                                    "gate": "http_sample", "observed_at": utc_now(),
                                    "error": f"{type(error).__name__}: {error}",
                                })
                            last_sample_at = now_monotonic
                            atomic_write(arguments.output, report)
                        try:
                            frame = json.loads(await asyncio.wait_for(socket.recv(), 5))
                        # Python 3.9 raises asyncio.TimeoutError here (it is only an alias of the
                        # built-in TimeoutError on newer runtimes). A quiet five-second market
                        # interval is not a stream failure; keep the same verified connection and
                        # continue sampling its atomic boundary.
                        except asyncio.TimeoutError:
                            continue
                        frame_type = frame.get("type")
                        if frame_type == "event":
                            observed_cursor = int(frame.get("cursor", -1))
                            if observed_cursor != cursor + 1:
                                report["stream"]["cursor_discontinuities"] += 1
                                raise RuntimeError(f"cursor discontinuity {cursor}->{observed_cursor}")
                            cursor = observed_cursor
                            event = frame.get("event") or {}
                            if event.get("applied") is not True:
                                report["stream"]["ordinary_unapplied_frames"] += 1
                                raise RuntimeError("ready stream published applied=false")
                            if event.get("gaps") or event.get("fail_closed_reason") is not None:
                                report["stream"]["ordinary_gap_frames"] += 1
                                raise RuntimeError("ready stream published an ordinary gap frame")
                            payload = event.get("canonical_payload") or {}
                            canonical_type = str(payload.get("event_type"))
                            if canonical_type not in {"full_book", "atomic_delta"}:
                                raise RuntimeError(f"unsupported ordinary event type {canonical_type}")
                            event_types[canonical_type] += 1
                            if payload.get("source_event_type"):
                                source_types[str(payload["source_event_type"])] += 1
                            report["stream"]["event_count"] += 1
                        elif frame_type == "universe_changed":
                            new_generation = int(frame.get("new_generation", -1))
                            added = set(map(str, frame.get("added_markets") or []))
                            removed = set(map(str, frame.get("removed_markets") or []))
                            if (
                                int(frame.get("old_generation", -1)) != connection_generation
                                or new_generation != connection_generation + 1
                                or not (added or removed)
                                or frame.get("full_sync_required") is not True
                            ):
                                raise RuntimeError(f"unjustified universe control event: {frame}")
                            report["stream"]["universe_changes"].append({
                                "observed_at": utc_now(), "old_generation": connection_generation,
                                "new_generation": new_generation, "added_markets": sorted(added),
                                "removed_markets": sorted(removed), "cursor": frame.get("cursor"),
                                "previous_active_market_count": len(connection_markets),
                            })
                            controlled_transition = True
                            break
                        elif frame_type == "resync_required":
                            reason = str(frame.get("reason"))
                            resync_reasons[reason] += 1
                            raise RuntimeError(f"unexpected resync_required: {reason}")
                        else:
                            raise RuntimeError(f"unsupported stream frame: {frame_type}")
            except Exception as error:
                if controlled_transition:
                    pass
                else:
                    report["failures"].append({"gate": "stream", "observed_at": utc_now(), "error": f"{type(error).__name__}: {error}"})
            atomic_write(arguments.output, report)

        report["stream"]["event_types"] = dict(sorted(event_types.items()))
        report["stream"]["source_event_types"] = dict(sorted(source_types.items()))
        report["stream"]["resync_reasons"] = dict(sorted(resync_reasons.items()))
        report["finished_at"] = utc_now()
        report["elapsed_seconds"] = time.monotonic() - started
        report["read_only"] = {}
        for path in READ_ONLY_PATHS:
            response = await client.get(arguments.base_url + path)
            report["read_only"][path] = response.status_code
        try:
            expired_uri = ws_url + "?after_cursor=0"
            async with websockets.connect(expired_uri, open_timeout=10, close_timeout=3, proxy=None) as socket:
                first = json.loads(await asyncio.wait_for(socket.recv(), 10))
                report["expired_cursor"] = {"frame": first, "passed": first.get("type") == "resync_required" and first.get("reason") == "event_cursor_expired"}
        except websockets.ConnectionClosed as error:
            report.setdefault("expired_cursor", {})["close"] = {"code": error.code, "reason": error.reason}
        except Exception as error:
            report["expired_cursor"] = {"passed": False, "error": f"{type(error).__name__}: {error}"}
        report["passed"] = (
            report["elapsed_seconds"] >= arguments.duration_seconds
            and not report["failures"]
            and report["stream"]["event_count"] > 0
            and report["stream"]["ordinary_unapplied_frames"] == 0
            and report["stream"]["ordinary_gap_frames"] == 0
            and report["stream"]["cursor_discontinuities"] == 0
            and not report["stream"]["resync_reasons"]
            and report.get("expired_cursor", {}).get("passed") is True
            and all(value == 404 for value in report["read_only"].values())
        )
    return report


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://127.0.0.1:18872")
    parser.add_argument("--duration-seconds", type=int, default=3600)
    parser.add_argument("--sample-interval-seconds", type=float, default=5)
    parser.add_argument("--expected-pid", type=int, required=True)
    parser.add_argument("--expected-commit", required=True)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--expected-binary-sha256", required=True)
    parser.add_argument("--launchd-label", required=True)
    parser.add_argument("--output", type=Path, required=True)
    arguments = parser.parse_args()
    if arguments.duration_seconds < 60 or arguments.sample_interval_seconds <= 0:
        parser.error("duration must be >=60 seconds and sample interval positive")
    report = asyncio.run(main_async(arguments))
    report["runtime"]["process_command"] = subprocess.run(
        ["ps", "-p", str(arguments.expected_pid), "-o", "command="],
        capture_output=True, text=True, check=False,
    ).stdout.strip()
    atomic_write(arguments.output, report)
    print(json.dumps({
        "passed": report["passed"], "elapsed_seconds": report.get("elapsed_seconds"),
        "events": report["stream"]["event_count"], "failures": len(report["failures"]),
        "resync_reasons": report["stream"]["resync_reasons"],
        "universe_changes": len(report["stream"]["universe_changes"]),
        "output": str(arguments.output.resolve()),
    }, sort_keys=True))
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
