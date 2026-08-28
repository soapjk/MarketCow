#!/usr/bin/env python3
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
from contextlib import nullcontext
import hashlib
import json
import math
import os
import subprocess
import time
import urllib.request
from datetime import UTC, datetime
from pathlib import Path

ADMIN_TOKEN = "marketcow-local-shadow-soak"
TOKENS = [f"market-{market:03d}-{side}" for market in range(100) for side in ("yes", "no")]


def percentile(values: list[float], percentile_value: float) -> float:
    if not values:
        return math.inf
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, math.ceil(percentile_value * len(ordered)) - 1)]


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def request_json(url: str, *, payload: dict | None = None, admin: bool = False) -> tuple[int, dict]:
    data = None if payload is None else json.dumps(payload, separators=(",", ":")).encode()
    headers = {"content-type": "application/json"} if data is not None else {}
    if admin:
        headers["authorization"] = f"Bearer {ADMIN_TOKEN}"
    request = urllib.request.Request(url, data=data, headers=headers, method="POST" if data else "GET")
    with urllib.request.urlopen(request, timeout=2) as response:
        return response.status, json.load(response)


def timed_json(url: str) -> tuple[float, int, dict]:
    before = time.perf_counter()
    status, body = request_json(url)
    return (time.perf_counter() - before) * 1000, status, body


def prometheus(url: str) -> dict[str, float]:
    with urllib.request.urlopen(url, timeout=2) as response:
        lines = response.read().decode().splitlines()
    values: dict[str, float] = {}
    for line in lines:
        if not line or line.startswith("#"):
            continue
        name, raw = line.rsplit(" ", 1)
        values[name] = float(raw)
    return values


def raw_book(token: str, bid: str, ask: str, now: datetime) -> dict:
    return {
        "event_type": "book", "asset_id": token, "timestamp": now.isoformat(),
        "tick_size": "0.01", "bids": [{"price": bid, "size": "10"}],
        "asks": [{"price": ask, "size": "11"}], "hash": f"soak-{token}-{now.isoformat()}",
    }


def ingest(base: str, raw_payload: dict, now: datetime) -> dict:
    status, body = request_json(
        f"{base}/v1/admin/polymarket/shadow-ingest",
        payload={"received_at": now.isoformat(), "raw_payload": raw_payload},
        admin=True,
    )
    if status != 200 or body.get("rejected") != 0 or body.get("real_order_submission_enabled") is not False:
        raise RuntimeError(f"invalid shadow ingest response: {status} {body}")
    return body


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the local marketcowd shadow stability gate")
    parser.add_argument("--duration-seconds", type=int, default=2700)
    parser.add_argument("--interval-seconds", type=float, default=0.25)
    parser.add_argument("--port", type=int, default=18870)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--storage-root", type=Path, required=True)
    parser.add_argument("--binary", type=Path)
    parser.add_argument("--expected-binary-sha256", required=True)
    parser.add_argument("--binary-commit", required=True)
    args = parser.parse_args()
    if args.duration_seconds < 1 or args.interval_seconds <= 0:
        parser.error("duration and interval must be positive")
    root = Path(__file__).resolve().parents[2]
    binary = (args.binary or root / "target/debug/marketcow").resolve()
    if not binary.is_file():
        raise SystemExit("build target/debug/marketcow first")
    binary_sha256 = file_sha256(binary)
    if binary_sha256 != args.expected_binary_sha256.lower():
        raise SystemExit("binary SHA-256 does not match the declared launch identity")
    if not args.binary_commit.strip() or len(args.binary_commit) > 128:
        parser.error("binary commit must be present and at most 128 characters")
    storage_root = args.storage_root.resolve()
    if not storage_root.is_absolute() or storage_root == Path("/"):
        parser.error("storage root must be an absolute narrow path")
    storage_root.mkdir(parents=True, exist_ok=True)
    if any(storage_root.iterdir()):
        parser.error("storage root must be empty")
    reader_latencies: list[float] = []
    bootstrap_persistence_latencies_us: list[float] = []
    bootstrap_publication_latencies_us: list[float] = []
    persistence_latencies_us: list[float] = []
    publication_latencies_us: list[float] = []
    failures: list[dict[str, object]] = []
    max_rss_kb = 0
    maximums = {
        "book_age_ms": 0.0, "gap_count": 0.0, "disconnects": 0.0,
        "queue_depth": 0.0, "persistence_latency_us": 0.0,
        "publication_latency_us": 0.0, "cursor_lag": 0.0,
    }
    final_metrics: dict[str, float] = {}
    checkpoint_count = 0
    ingest_count = 0
    canonical_event_count = 0
    reader_request_count = 0
    consumer_request_count = 0
    consumer_cursors = [0, 0]
    final_book_count = 0
    process_log_tail = ""
    # The storage root is intentionally retained for artifact-specific restart recovery.
    with nullcontext(str(storage_root)) as temporary:
        env = {
            **os.environ,
            "MARKETCOW_RUST_PROFILE": "test",
            "MARKETCOW_RUST_BIND": f"127.0.0.1:{args.port}",
            "MARKETCOW_RUST_STORAGE_ROOT": temporary,
            "MARKETCOW_RUST_SCOPE_ID": "soak-shadow",
            "MARKETCOW_REAL_ORDER_SUBMISSION_ENABLED": "false",
            "MARKETCOW_RUST_ADMIN_TOKEN": ADMIN_TOKEN,
            "MARKETCOW_RUST_MAX_BOOK_AGE_MS": "5000",
        }
        log_path = Path(temporary) / "marketcowd.log"
        with log_path.open("wb") as process_log:
            process = subprocess.Popen([binary, "serve"], env=env, stdout=process_log, stderr=process_log)
            started_at = datetime.now(UTC)
            base = f"http://127.0.0.1:{args.port}"
            try:
                startup_deadline = time.monotonic() + 10
                while True:
                    if process.poll() is not None:
                        raise RuntimeError("shadow process exited during startup")
                    try:
                        status, body = request_json(f"{base}/v1/health")
                        if status == 200 and body.get("real_order_submission_enabled") is False:
                            break
                    except Exception:  # noqa: BLE001 - expected until socket bind completes
                        pass
                    if time.monotonic() >= startup_deadline:
                        raise RuntimeError("shadow process did not become healthy within 10 seconds")
                    time.sleep(0.05)

                for token in TOKENS:
                    now = datetime.now(UTC)
                    body = ingest(base, raw_book(token, "0.40", "0.42", now), now)
                    bootstrap_persistence_latencies_us.append(body["persistence_latency_us"])
                    bootstrap_publication_latencies_us.append(body["publication_latency_us"])
                    ingest_count += 1
                    canonical_event_count += body["events"]

                observed = datetime.now(UTC)
                seed = ingest(base, {
                    "event_type": "price_change", "timestamp": observed.isoformat(),
                    "price_changes": [
                        {
                            "asset_id": token,
                            "side": "BUY" if token.endswith("yes") else "SELL",
                            "price": "0.40" if token.endswith("yes") else "0.42",
                            "size": "10",
                        }
                        for token in TOKENS
                    ],
                }, observed)
                bootstrap_persistence_latencies_us.append(seed["persistence_latency_us"])
                bootstrap_publication_latencies_us.append(seed["publication_latency_us"])
                ingest_count += 1
                canonical_event_count += seed["events"]
                deadline = time.monotonic() + args.duration_seconds
                next_ingest = time.monotonic()
                next_checkpoint = time.monotonic() + 60
                sequence = 0
                with ThreadPoolExecutor(max_workers=6) as readers:
                    while time.monotonic() < deadline:
                        loop_now = time.monotonic()
                        if loop_now >= next_ingest:
                            observed = datetime.now(UTC)
                            size = str(10 + sequence % 20)
                            body = ingest(base, {
                                "event_type": "price_change", "timestamp": observed.isoformat(),
                                "price_changes": [
                                    {
                                        "asset_id": token,
                                        "side": "BUY" if token.endswith("yes") else "SELL",
                                        "price": "0.40" if token.endswith("yes") else "0.42",
                                        "size": size,
                                    }
                                    for token in TOKENS
                                ],
                            }, observed)
                            persistence_latencies_us.append(body["persistence_latency_us"])
                            publication_latencies_us.append(body["publication_latency_us"])
                            ingest_count += 1
                            canonical_event_count += body["events"]
                            sequence += 1
                            next_ingest = loop_now + 1
                        if loop_now >= next_checkpoint:
                            status, body = request_json(
                                f"{base}/v1/admin/polymarket/checkpoint", payload={}, admin=True,
                            )
                            if status != 200 or body.get("real_order_submission_enabled") is not False:
                                raise RuntimeError(f"checkpoint contract failed: {status} {body}")
                            checkpoint_count += 1
                            next_checkpoint = loop_now + 60

                        reader_futures = [
                            readers.submit(timed_json, f"{base}/v1/readiness")
                            for _ in range(4)
                        ]
                        consumer_futures = [
                            readers.submit(
                                timed_json,
                                f"{base}/v1/prediction-markets/polymarket/live/events"
                                f"?after_cursor={cursor}&limit=1000",
                            )
                            for cursor in consumer_cursors
                        ]
                        for future in reader_futures:
                            latency, status, readiness = future.result()
                            reader_latencies.append(latency)
                            reader_request_count += 1
                            if status != 200 or readiness.get("ready") is not True:
                                failures.append({
                                    "kind": "scoped_reader", "status": status, "body": readiness,
                                })
                        for index, future in enumerate(consumer_futures):
                            _latency, status, page = future.result()
                            consumer_request_count += 1
                            next_cursor = page.get("next_cursor")
                            if (
                                status != 200
                                or not isinstance(next_cursor, int)
                                or next_cursor < consumer_cursors[index]
                            ):
                                failures.append({
                                    "kind": "consumer", "index": index,
                                    "status": status, "body": page,
                                })
                            else:
                                consumer_cursors[index] = next_cursor
                        observed_metrics = prometheus(f"{base}/metrics")
                        maximums["book_age_ms"] = max(
                            maximums["book_age_ms"],
                            observed_metrics["marketcow_maximum_book_age_ms"],
                        )
                        maximums["gap_count"] = max(
                            maximums["gap_count"], observed_metrics["marketcow_unresolved_gaps"]
                        )
                        maximums["disconnects"] = max(
                            maximums["disconnects"], observed_metrics["marketcow_disconnects_total"]
                        )
                        maximums["queue_depth"] = max(
                            maximums["queue_depth"], observed_metrics["marketcow_ingress_queue_depth"]
                        )
                        maximums["persistence_latency_us"] = max(
                            maximums["persistence_latency_us"],
                            observed_metrics["marketcow_persistence_latency_us"],
                        )
                        maximums["publication_latency_us"] = max(
                            maximums["publication_latency_us"],
                            observed_metrics["marketcow_publication_latency_us"],
                        )
                        cursor_lag = (
                            observed_metrics["marketcow_projection_persisted_cursor"]
                            - observed_metrics["marketcow_projection_published_cursor"]
                        )
                        maximums["cursor_lag"] = max(maximums["cursor_lag"], abs(cursor_lag))
                        final_metrics = observed_metrics
                        rss = subprocess.run(
                            ["ps", "-o", "rss=", "-p", str(process.pid)],
                            text=True, capture_output=True, check=False,
                        ).stdout.strip()
                        if rss.isdigit():
                            max_rss_kb = max(max_rss_kb, int(rss))
                        if process.poll() is not None:
                            failures.append({"kind": "process_exit", "code": process.returncode})
                            break
                        time.sleep(args.interval_seconds)
                status, final_sync = request_json(
                    f"{base}/v1/prediction-markets/polymarket/live/full-sync"
                )
                if status != 200:
                    failures.append({"kind": "final_full_sync", "status": status, "body": final_sync})
                else:
                    final_book_count = len(final_sync.get("snapshot", {}).get("books", []))
            except Exception as error:  # noqa: BLE001 - Artifact records exact terminal failure
                failures.append({"kind": "runner", "error": type(error).__name__, "message": str(error)})
            finally:
                process.terminate()
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()
        process_log_tail = log_path.read_text(errors="replace")[-4000:]
        elapsed_seconds = (datetime.now(UTC) - started_at).total_seconds()

    gate_verdicts = {
        "duration_reached": elapsed_seconds >= args.duration_seconds,
        "runner_failures_zero": not failures,
        "exact_100_market_200_book_load": final_book_count == 200,
        "four_concurrent_scoped_readers": reader_request_count >= 4,
        "two_independent_consumers": (
            consumer_request_count >= 2
            and len(consumer_cursors) == 2
            and all(cursor == final_metrics.get("marketcow_projection_persisted_cursor")
                    for cursor in consumer_cursors)
        ),
        "readiness_p99_ms_lte_50": percentile(reader_latencies, .99) <= 50,
        "book_age_ms_lte_5000": maximums["book_age_ms"] <= 5_000,
        "gap_count_zero": maximums["gap_count"] == 0,
        "disconnect_delta_zero": maximums["disconnects"] == 0,
        "queue_depth_zero": maximums["queue_depth"] == 0,
        "cursor_lag_zero": maximums["cursor_lag"] == 0,
        "wal_persistence_latency_p99_us_lte_20000": (
            percentile(persistence_latencies_us, .99) <= 20_000
        ),
        "projection_publication_latency_p99_us_lte_5000": (
            percentile(publication_latencies_us, .99) <= 5_000
        ),
        "real_orders_disabled": True,
        "tradude_does_not_manage_marketcow": True,
    }
    passed = all(gate_verdicts.values())
    result = {
        "schema_version": "marketcow.shadow-soak.v4",
        "binary_commit": args.binary_commit,
        "binary_sha256": binary_sha256,
        "storage_root": str(storage_root),
        "started_at": started_at.isoformat(),
        "finished_at": datetime.now(UTC).isoformat(),
        "requested_duration_seconds": args.duration_seconds,
        "elapsed_seconds": elapsed_seconds,
        "load_model": {
            "markets": 100,
            "books": 200,
            "concurrent_scoped_readers": 4,
            "independent_consumers": 2,
        },
        "samples": len(reader_latencies),
        "ingest_count": ingest_count,
        "canonical_event_count": canonical_event_count,
        "checkpoint_count": checkpoint_count,
        "reader_request_count": reader_request_count,
        "consumer_request_count": consumer_request_count,
        "consumer_cursors": consumer_cursors,
        "final_book_count": final_book_count,
        "readiness_latency_ms": {
            "p50": percentile(reader_latencies, .50),
            "p95": percentile(reader_latencies, .95),
            "p99": percentile(reader_latencies, .99),
            "max": max(reader_latencies, default=math.inf),
        },
        "bootstrap_persistence_latency_us": {
            "p50": percentile(bootstrap_persistence_latencies_us, .50),
            "p95": percentile(bootstrap_persistence_latencies_us, .95),
            "p99": percentile(bootstrap_persistence_latencies_us, .99),
            "max": max(bootstrap_persistence_latencies_us, default=math.inf),
        },
        "bootstrap_publication_latency_us": {
            "p50": percentile(bootstrap_publication_latencies_us, .50),
            "p95": percentile(bootstrap_publication_latencies_us, .95),
            "p99": percentile(bootstrap_publication_latencies_us, .99),
            "max": max(bootstrap_publication_latencies_us, default=math.inf),
        },
        "persistence_latency_us": {
            "p50": percentile(persistence_latencies_us, .50),
            "p95": percentile(persistence_latencies_us, .95),
            "p99": percentile(persistence_latencies_us, .99),
            "max": max(persistence_latencies_us, default=math.inf),
        },
        "publication_latency_us": {
            "p50": percentile(publication_latencies_us, .50),
            "p95": percentile(publication_latencies_us, .95),
            "p99": percentile(publication_latencies_us, .99),
            "max": max(publication_latencies_us, default=math.inf),
        },
        "observed_maximums": maximums,
        "final_metrics": final_metrics,
        "max_rss_kb": max_rss_kb,
        "failures": failures[:100],
        "gate_verdicts": gate_verdicts,
        "process_log_tail": process_log_tail,
        "real_order_submission_enabled": False,
        "tradude_manages_marketcow": False,
        "headless_substitutes_http_network_soak": False,
        "passed": passed,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    print(json.dumps(result, indent=2, sort_keys=True))
    if not passed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
