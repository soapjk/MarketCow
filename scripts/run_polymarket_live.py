#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
import logging
from pathlib import Path

from marketcow.polymarket_live import (
    ClobBooksClient,
    GammaKeysetCatalog,
    LiveStateStore,
    PolymarketLiveCollector,
    live_collector_lease,
    load_scoped_live_store,
)
from marketcow.polymarket_live_stream import PolymarketLiveStreamServer


LOGGER = logging.getLogger(__name__)


def effective_snapshot_refresh_seconds(
    requested: float | None, *, bounded_scope: bool,
) -> float | None:
    """Keep exact-scope freshness inside the strict delivery budget."""
    if requested is None or not bounded_scope:
        return requested
    return min(requested, 1.0)


def load_published_startup_data(store: LiveStateStore) -> dict[str, object]:
    """Load only an already-published, bounded runtime generation.

    Remote Gamma traversal, catalog normalization, universe selection, and fee
    policy rebuilding belong to ``prepare_polymarket_discovery.py``. A service
    start is deliberately local-only and fails immediately when that immutable
    startup boundary is absent or invalid.
    """
    if not store.catalog_path.is_file():
        raise RuntimeError(
            "Polymarket startup data is missing; run "
            "scripts/prepare_polymarket_discovery.py first"
        )
    store._load_catalog(store.catalog_path)
    if not store.catalog or store.catalog_revision is None:
        raise RuntimeError("published Polymarket startup catalog is empty")
    payload = json.loads(store.catalog_path.read_bytes())
    universe = payload.get("realtime_universe")
    if not isinstance(universe, dict):
        raise RuntimeError(
            "published Polymarket realtime universe is missing; run "
            "scripts/prepare_polymarket_discovery.py first"
        )
    selected = store._validate_realtime_universe(
        universe,
        catalog_revision=store.catalog_revision,
        available_market_ids=set(store.catalog),
    )
    if selected != set(store.catalog):
        raise RuntimeError("published Polymarket runtime generation is incomplete")
    return {
        "status": "published_startup_data_loaded",
        "catalog_revision": store.catalog_revision,
        "realtime_universe_id": universe["universe_id"],
        "market_count": len(selected),
        "token_count": len(store.token_to_market),
    }


def validate_rest_poll_startup_state(store: LiveStateStore) -> dict[str, object]:
    """Require a complete local boundary instead of re-bootstrapping remotely."""
    health = store.health()
    if (
        health.book_token_count != len(store.token_to_market)
        or health.unresolved_gap_count != 0
        or store.active_recovery_id is not None
    ):
        raise RuntimeError(
            "rest-poll startup state is incomplete; run the bounded preparation "
            "or reselect tool before starting the collector"
        )
    return {
        "status": "complete_local_rest_poll_boundary_reused",
        "catalog_revision": store.catalog_revision,
        "market_count": len(store.catalog),
        "token_count": len(store.token_to_market),
        "starting_cursor": store.cursor,
    }


async def bootstrap_books_once(collector: PolymarketLiveCollector) -> str:
    """Attempt the startup boundary once; the process supervisor owns retries."""
    lock = getattr(collector, "_snapshot_operation_lock", None)
    if lock is None:
        return await collector.bootstrap_books("startup")
    async with lock:
        return await collector.bootstrap_books("startup")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run a bounded Polymarket collector from prepared local startup data"
    )
    parser.add_argument(
        "--root", type=Path, required=True,
        help=(
            "Must equal the API storage_root/prediction-markets/polymarket-live "
            "directory so FastAPI can durable-tail collector writes"
        ),
    )
    parser.add_argument("--bootstrap-only", action="store_true")
    parser.add_argument("--shard-size", type=int, default=500)
    parser.add_argument("--max-websocket-connections", type=int, default=32)
    parser.add_argument(
        "--market-id", action="append", default=[],
        help=(
            "Hydrate only this indexed market (repeatable, at most 100) and "
            "use the same prepared local catalog."
        ),
    )
    parser.add_argument(
        "--snapshot-refresh-seconds", type=float,
        help=(
            "Periodically refresh REST books while streaming; intended for "
            "bounded scopes that require freshness during quiet markets."
        ),
    )
    parser.add_argument(
        "--rest-poll-only", action="store_true",
        help=(
            "Disable the upstream WebSocket fan-in and maintain the prepared "
            "universe only with authoritative periodic CLOB snapshots. Requires "
            "--snapshot-refresh-seconds."
        ),
    )
    parser.add_argument("--live-stream-host", default="127.0.0.1")
    parser.add_argument("--live-stream-port", type=int, default=8794)
    parser.add_argument("--live-stream-replay-capacity", type=int, default=10_000)
    arguments = parser.parse_args()
    if arguments.rest_poll_only and arguments.snapshot_refresh_seconds is None:
        parser.error("--rest-poll-only requires --snapshot-refresh-seconds")

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    with live_collector_lease(arguments.root):
        store = (
            load_scoped_live_store(arguments.root, arguments.market_id)
            if arguments.market_id else LiveStateStore(arguments.root)
        )
        collector = PolymarketLiveCollector(
            store,
            GammaKeysetCatalog(spool_root=arguments.root / "spool"),
            ClobBooksClient(
                timeout=(1.75 if arguments.market_id else 20),
                max_retries_per_batch=(1 if arguments.market_id else 5),
            ),
            shard_size=arguments.shard_size,
            max_websocket_connections=arguments.max_websocket_connections,
            snapshot_refresh_seconds=effective_snapshot_refresh_seconds(
                arguments.snapshot_refresh_seconds,
                bounded_scope=bool(arguments.market_id),
            ),
            catalog_refresh_on_lifecycle_events=False,
            publish_checkpoints=not bool(arguments.market_id),
            minimum_snapshot_refresh_age_seconds=(0.25 if arguments.market_id else 0),
            # Keep complete market/negative-risk groups together, but do not
            # let one slow 200-token CLOB request age the entire exact scope.
            # Periodic attempts are bounded and retry on their next cadence;
            # startup/reconnect recovery retains the stronger retry policy.
            max_concurrent_snapshot_refreshes=(
                16 if arguments.rest_poll_only else (4 if arguments.market_id else 1)
            ),
            periodic_snapshot_request_timeout=(
                (5.0, 5.0) if arguments.rest_poll_only
                else ((1.75, 1.75) if arguments.market_id else None)
            ),
            periodic_snapshot_max_retries=(
                0 if arguments.rest_poll_only or arguments.market_id else None
            ),
        )
        if arguments.market_id:
            print({
                "mode": "indexed_scope",
                "market_ids": arguments.market_id,
                "active_token_count": len(store.token_to_market),
                "starting_cursor": store.cursor,
            })
        else:
            evidence = load_published_startup_data(store)
            if isinstance(store, LiveStateStore) and not store._recovered:
                if arguments.rest_poll_only:
                    store.start_from_checkpoint_tail()
                else:
                    store.start_from_durable_tail()
            print(evidence)
        async def run() -> None:
            store.enable_async_persistence()
            if arguments.market_id:
                await asyncio.to_thread(
                    collector.reconcile_elapsed_markets,
                    reason="startup:elapsed_end",
                )
            stream = PolymarketLiveStreamServer(
                store,
                host=arguments.live_stream_host,
                port=arguments.live_stream_port,
                replay_capacity=arguments.live_stream_replay_capacity,
            )
            await stream.start()
            reuse_rest_poll_boundary = (
                arguments.rest_poll_only and not arguments.market_id
            )
            if reuse_rest_poll_boundary:
                print(validate_rest_poll_startup_state(store))
            collector_task = None
            if not arguments.bootstrap_only:
                collector_task = asyncio.create_task(
                    (
                        collector._refresh_snapshots_periodically()
                        if arguments.rest_poll_only
                        else collector.run()
                    ),
                    name=(
                        "polymarket-rest-snapshot-poller"
                        if arguments.rest_poll_only
                        else "polymarket-upstream-websocket"
                    ),
                )
            try:
                if not reuse_rest_poll_boundary:
                    await bootstrap_books_once(collector)
                print(store.health().model_dump(mode="json"))
                if collector_task is not None:
                    await collector_task
            finally:
                if collector_task is not None:
                    collector_task.cancel()
                    await asyncio.gather(collector_task, return_exceptions=True)
                await stream.close()
                await asyncio.to_thread(store.close_async_persistence)

        asyncio.run(run())


if __name__ == "__main__":
    main()
