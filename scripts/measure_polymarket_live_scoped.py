#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import statistics
import time
import tracemalloc
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from marketcow.polymarket_contracts import canonical_json
from marketcow.polymarket_live import PolymarketLiveReadStore


def percentile(values: list[float], fraction: float) -> float:
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int((len(ordered) - 1) * fraction))]


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Measure bounded, read-only Polymarket live scoped consumption."
    )
    parser.add_argument("--root", required=True, type=Path)
    parser.add_argument("--market-id", action="append", default=[])
    parser.add_argument(
        "--market-id-file",
        type=Path,
        help="Optional UTF-8 file containing one market_id per line.",
    )
    parser.add_argument("--repeat", type=int, default=20)
    parser.add_argument("--concurrency", type=int, default=1)
    parser.add_argument("--after-cursor", type=int, default=0)
    parser.add_argument("--event-limit", type=int, default=1000)
    args = parser.parse_args()
    market_ids = list(args.market_id)
    if args.market_id_file:
        market_ids.extend(
            line.strip()
            for line in args.market_id_file.read_text(encoding="utf-8").splitlines()
            if line.strip()
        )
    market_ids = list(dict.fromkeys(market_ids))
    if not 1 <= len(market_ids) <= 100:
        parser.error("provide 1–100 distinct market IDs")
    if args.repeat < 1 or args.concurrency < 1:
        parser.error("--repeat and --concurrency must be positive")

    reader = PolymarketLiveReadStore(args.root)

    def sample() -> tuple[float, int, int]:
        started = time.perf_counter()
        responses = (
            reader.bootstrap(market_ids),
            reader.snapshot(market_ids),
            reader.events_after(market_ids, args.after_cursor, args.event_limit),
            reader.checkpoint(market_ids),
            reader.gaps(market_ids, unresolved_only=True),
        )
        elapsed_ms = (time.perf_counter() - started) * 1000
        response_bytes = sum(
            len(canonical_json(response.model_dump(mode="json")))
            for response in responses
        )
        return elapsed_ms, response_bytes, responses[1].cursor

    tracemalloc.start()
    with ThreadPoolExecutor(max_workers=args.concurrency) as executor:
        samples = list(executor.map(lambda _: sample(), range(args.repeat)))
    _, peak_python_bytes = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    latencies = [item[0] for item in samples]
    response_sizes = [item[1] for item in samples]
    print(json.dumps({
        "schema_version": "marketcow.polymarket.scoped-read-measurement.v1",
        "root": str(args.root.resolve()),
        "market_count": len(market_ids),
        "repeat": args.repeat,
        "concurrency": args.concurrency,
        "after_cursor": args.after_cursor,
        "event_limit": args.event_limit,
        "latency_ms": {
            "min": min(latencies),
            "median": statistics.median(latencies),
            "p95": percentile(latencies, 0.95),
            "max": max(latencies),
        },
        "response_bytes": {
            "min": min(response_sizes),
            "max": max(response_sizes),
        },
        "peak_traced_python_bytes": peak_python_bytes,
        "latest_cursor": max(item[2] for item in samples),
    }, sort_keys=True))


if __name__ == "__main__":
    main()
