#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import uvicorn

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
if str(REPOSITORY_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT / "src"))

from marketcow.polymarket_live_read_api import (  # noqa: E402
    create_polymarket_live_read_app,
)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run the isolated read-only Polymarket live API",
    )
    parser.add_argument("--root", required=True, type=Path)
    parser.add_argument("--discovery-root", required=True, type=Path)
    parser.add_argument("--configured-scope", type=Path)
    parser.add_argument("--host", required=True)
    parser.add_argument("--port", required=True, type=int)
    parser.add_argument(
        "--stable-snapshot-max-book-age-seconds", required=True, type=float
    )
    parser.add_argument(
        "--consumer-maximum-book-age-seconds", required=True, type=float
    )
    parser.add_argument(
        "--minimum-delivery-headroom-seconds", required=True, type=float
    )
    parser.add_argument("--stable-read-wait-seconds", required=True, type=float)
    parser.add_argument("--stable-read-poll-seconds", required=True, type=float)
    parser.add_argument("--executor-workers", required=True, type=int)
    parser.add_argument(
        "--discovery-depth-notional",
        action="append",
        required=True,
        help="Explicit base-size depth tier (repeatable decimal string)",
    )
    parser.add_argument(
        "--discovery-maximum-book-age-ms", required=True, type=int,
    )
    parser.add_argument(
        "--discovery-maximum-full-sync-bytes",
        type=int,
        default=256 * 1024 * 1024,
    )
    parser.add_argument(
        "--live-stream-uri", default="ws://127.0.0.1:8794"
    )
    parser.add_argument("--live-stream-replay-capacity", type=int, default=10_000)
    parser.add_argument("--log-level", default="info")
    parser.add_argument("--no-access-log", action="store_true")
    arguments = parser.parse_args()
    if not arguments.root.is_absolute():
        parser.error("--root must be absolute")
    if not arguments.discovery_root.is_absolute():
        parser.error("--discovery-root must be absolute")
    if not 1 <= arguments.port <= 65535:
        parser.error("--port must be in [1, 65535]")

    app = create_polymarket_live_read_app(
        configured_scope_path=arguments.configured_scope,
        root=arguments.root,
        discovery_root=arguments.discovery_root,
        stable_snapshot_max_book_age_seconds=(
            arguments.stable_snapshot_max_book_age_seconds
        ),
        stable_read_wait_seconds=arguments.stable_read_wait_seconds,
        stable_read_poll_seconds=arguments.stable_read_poll_seconds,
        executor_workers=arguments.executor_workers,
        discovery_depth_notionals=tuple(arguments.discovery_depth_notional),
        discovery_maximum_book_age_ms=(
            arguments.discovery_maximum_book_age_ms
        ),
        discovery_maximum_full_sync_bytes=(
            arguments.discovery_maximum_full_sync_bytes
        ),
        live_stream_uri=arguments.live_stream_uri,
        live_stream_replay_capacity=arguments.live_stream_replay_capacity,
        consumer_maximum_book_age_seconds=(
            arguments.consumer_maximum_book_age_seconds
        ),
        minimum_delivery_headroom_seconds=(
            arguments.minimum_delivery_headroom_seconds
        ),
    )
    uvicorn.run(
        app,
        host=arguments.host,
        port=arguments.port,
        log_level=arguments.log_level,
        access_log=not arguments.no_access_log,
    )


if __name__ == "__main__":
    main()
