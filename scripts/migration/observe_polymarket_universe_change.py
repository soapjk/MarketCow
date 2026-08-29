#!/usr/bin/env python3
"""Capture one live MarketCow universe transition from an already-open stream."""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

from websockets.sync.client import connect

from scripts.migration.build_polymarket_rust_scope import write_atomic_json


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--uri", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--old-generation", type=int, required=True)
    parser.add_argument("--new-generation", type=int, required=True)
    parser.add_argument("--timeout-seconds", type=float, default=1_800)
    arguments = parser.parse_args()

    started = time.monotonic()
    with connect(
        arguments.uri, open_timeout=15, close_timeout=5, proxy=None
    ) as socket:
        subscription = json.loads(socket.recv(timeout=15))
        if subscription.get("type") != "subscription":
            raise RuntimeError("first frame is not a subscription")
        while True:
            remaining = arguments.timeout_seconds - (time.monotonic() - started)
            if remaining <= 0:
                raise TimeoutError("universe_changed was not observed before timeout")
            try:
                message = socket.recv(timeout=min(remaining, 30))
            except TimeoutError:
                continue
            frame = json.loads(message)
            if frame.get("type") != "universe_changed":
                continue
            if (
                frame.get("old_generation") != arguments.old_generation
                or frame.get("new_generation") != arguments.new_generation
                or frame.get("full_sync_required") is not True
                or frame.get("cursor") != frame.get("switch_boundary_cursor")
            ):
                raise RuntimeError("universe_changed contract mismatch")
            write_atomic_json(arguments.output, {
                "schema_version": "marketcow.universe-change-observation.v1",
                "verdict": "passed",
                "subscription": subscription,
                "universe_changed": frame,
                "elapsed_seconds": time.monotonic() - started,
            })
            print(json.dumps({"verdict": "passed", "output": str(arguments.output)}))
            return 0


if __name__ == "__main__":
    raise SystemExit(main())
