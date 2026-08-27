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


def percentile(values: list[float], percentile_value: float) -> float:
    if not values:
        return math.inf
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, math.ceil(percentile_value * len(ordered)) - 1)]


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
    started_at = datetime.now(UTC)
    latencies: list[float] = []
    failures: list[dict[str, object]] = []
    max_rss_kb = 0
    with tempfile.TemporaryDirectory(prefix="marketcow-shadow-soak-") as temporary:
        env = {**os.environ, "MARKETCOW_RUST_PROFILE": "test", "MARKETCOW_RUST_BIND": f"127.0.0.1:{args.port}",
               "MARKETCOW_RUST_STORAGE_ROOT": temporary, "MARKETCOW_RUST_SCOPE_ID": "soak-shadow",
               "MARKETCOW_REAL_ORDER_SUBMISSION_ENABLED": "false"}
        process = subprocess.Popen([binary, "serve"], env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        try:
            deadline = time.monotonic() + args.duration_seconds
            while time.monotonic() < deadline:
                before = time.perf_counter()
                try:
                    with urllib.request.urlopen(f"http://127.0.0.1:{args.port}/v1/health", timeout=2) as response:
                        body = json.load(response)
                    latencies.append((time.perf_counter() - before) * 1000)
                    if response.status != 200 or body.get("real_order_submission_enabled") is not False:
                        failures.append({"kind": "contract", "status": response.status})
                except Exception as error:  # noqa: BLE001 - the Artifact captures every failure
                    failures.append({"kind": "request", "error": type(error).__name__, "message": str(error)})
                rss = subprocess.run(["ps", "-o", "rss=", "-p", str(process.pid)], text=True,
                                     capture_output=True, check=False).stdout.strip()
                if rss.isdigit():
                    max_rss_kb = max(max_rss_kb, int(rss))
                if process.poll() is not None:
                    failures.append({"kind": "process_exit", "code": process.returncode})
                    break
                time.sleep(args.interval_seconds)
        finally:
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
        elapsed_seconds = (datetime.now(UTC) - started_at).total_seconds()
        result = {"schema_version": "marketcow.shadow-soak.v1", "started_at": started_at.isoformat(),
                  "finished_at": datetime.now(UTC).isoformat(), "requested_duration_seconds": args.duration_seconds,
                  "elapsed_seconds": elapsed_seconds, "samples": len(latencies),
                  "health_latency_ms": {"p50": percentile(latencies, .50), "p95": percentile(latencies, .95),
                                        "p99": percentile(latencies, .99), "max": max(latencies, default=math.inf)},
                  "max_rss_kb": max_rss_kb, "failures": failures[:100],
                  "real_order_submission_enabled": False,
                  "passed": elapsed_seconds >= args.duration_seconds and not failures and percentile(latencies, .99) <= 50}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    print(json.dumps(result, indent=2, sort_keys=True))
    if not result["passed"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
