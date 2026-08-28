from __future__ import annotations

import argparse
import json
import math
import os
import subprocess
import tempfile
import time
import urllib.request
from datetime import UTC, datetime
from pathlib import Path

ADMIN_TOKEN = "marketcow-local-shadow-soak"


def percentile(values: list[float], percentile_value: float) -> float:
    if not values:
        return math.inf
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, math.ceil(percentile_value * len(ordered)) - 1)]


def request_json(url: str, *, payload: dict | None = None, admin: bool = False) -> tuple[int, dict]:
    data = None if payload is None else json.dumps(payload, separators=(",", ":")).encode()
    headers = {"content-type": "application/json"} if data is not None else {}
    if admin:
        headers["authorization"] = f"Bearer {ADMIN_TOKEN}"
    request = urllib.request.Request(url, data=data, headers=headers, method="POST" if data else "GET")
    with urllib.request.urlopen(request, timeout=2) as response:
        return response.status, json.load(response)


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
    args = parser.parse_args()
    if args.duration_seconds < 1 or args.interval_seconds <= 0:
        parser.error("duration and interval must be positive")
    root = Path(__file__).resolve().parents[2]
    binary = root / "target/debug/marketcow"
    if not binary.is_file():
        raise SystemExit("build target/debug/marketcow first")
    latencies: list[float] = []
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
    process_log_tail = ""
    with tempfile.TemporaryDirectory(prefix="marketcow-shadow-soak-") as temporary:
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

                now = datetime.now(UTC)
                ingest(base, raw_book("binary-yes", "0.40", "0.42", now), now)
                ingest(base, raw_book("binary-no", "0.58", "0.60", now), now)
                ingest_count += 2
                deadline = time.monotonic() + args.duration_seconds
                next_ingest = time.monotonic()
                next_checkpoint = time.monotonic() + 60
                sequence = 0
                while time.monotonic() < deadline:
                    loop_now = time.monotonic()
                    if loop_now >= next_ingest:
                        observed = datetime.now(UTC)
                        size = str(10 + sequence % 20)
                        ingest(base, {
                            "event_type": "price_change", "timestamp": observed.isoformat(),
                            "price_changes": [
                                {"asset_id": "binary-yes", "side": "BUY", "price": "0.40", "size": size},
                                {"asset_id": "binary-no", "side": "SELL", "price": "0.60", "size": size},
                            ],
                        }, observed)
                        ingest_count += 1
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

                    before = time.perf_counter()
                    status, readiness = request_json(f"{base}/v1/readiness")
                    latencies.append((time.perf_counter() - before) * 1000)
                    if status != 200 or readiness.get("ready") is not True:
                        failures.append({"kind": "readiness", "status": status, "body": readiness})
                    observed_metrics = prometheus(f"{base}/metrics")
                    maximums["book_age_ms"] = max(
                        maximums["book_age_ms"], observed_metrics["marketcow_maximum_book_age_ms"]
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
                        maximums["persistence_latency_us"], observed_metrics["marketcow_persistence_latency_us"]
                    )
                    maximums["publication_latency_us"] = max(
                        maximums["publication_latency_us"], observed_metrics["marketcow_publication_latency_us"]
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

    passed = (
        elapsed_seconds >= args.duration_seconds
        and not failures
        and percentile(latencies, .99) <= 50
        and maximums["book_age_ms"] <= 5_000
        and maximums["gap_count"] == 0
        and maximums["queue_depth"] == 0
        and maximums["cursor_lag"] == 0
        and maximums["persistence_latency_us"] <= 50_000
        and maximums["publication_latency_us"] <= 5_000
    )
    result = {
        "schema_version": "marketcow.shadow-soak.v2",
        "started_at": started_at.isoformat(),
        "finished_at": datetime.now(UTC).isoformat(),
        "requested_duration_seconds": args.duration_seconds,
        "elapsed_seconds": elapsed_seconds,
        "samples": len(latencies),
        "ingest_count": ingest_count,
        "checkpoint_count": checkpoint_count,
        "readiness_latency_ms": {
            "p50": percentile(latencies, .50), "p95": percentile(latencies, .95),
            "p99": percentile(latencies, .99), "max": max(latencies, default=math.inf),
        },
        "observed_maximums": maximums,
        "final_metrics": final_metrics,
        "max_rss_kb": max_rss_kb,
        "failures": failures[:100],
        "process_log_tail": process_log_tail,
        "real_order_submission_enabled": False,
        "passed": passed,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    print(json.dumps(result, indent=2, sort_keys=True))
    if not passed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
