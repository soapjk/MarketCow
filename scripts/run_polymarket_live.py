#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
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


async def bootstrap_books_until_ready(
    collector: PolymarketLiveCollector,
    *,
    retry_seconds: float = 1.0,
) -> str:
    """Keep the in-memory service alive across incomplete provider snapshots."""
    attempts = 0
    while True:
        try:
            reason = "startup" if attempts == 0 else f"startup_retry:{attempts}"
            lock = getattr(collector, "_snapshot_operation_lock", None)
            if lock is None:
                return await collector.bootstrap_books(reason)
            async with lock:
                return await collector.bootstrap_books(reason)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            attempts += 1
            LOGGER.warning(
                "polymarket_bootstrap_retry attempt=%d retry_seconds=%.3f "
                "error=%s detail=%s",
                attempts, retry_seconds, type(exc).__name__, str(exc)[:500],
            )
            await asyncio.sleep(retry_seconds)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Capture free official Polymarket full-market live data locally"
    )
    parser.add_argument(
        "--root", type=Path, required=True,
        help=(
            "Must equal the API storage_root/prediction-markets/polymarket-live "
            "directory so FastAPI can durable-tail collector writes"
        ),
    )
    parser.add_argument("--catalog-only", action="store_true")
    parser.add_argument("--bootstrap-only", action="store_true")
    parser.add_argument("--shard-size", type=int, default=500)
    parser.add_argument("--max-websocket-connections", type=int, default=32)
    parser.add_argument("--catalog-progress-pages", type=int, default=25)
    parser.add_argument(
        "--market-id", action="append", default=[],
        help=(
            "Hydrate only this indexed market (repeatable, at most 100) and "
            "skip the full Gamma catalog refresh."
        ),
    )
    parser.add_argument(
        "--snapshot-refresh-seconds", type=float,
        help=(
            "Periodically refresh REST books while streaming; intended for "
            "bounded scopes that require freshness during quiet markets."
        ),
    )
    parser.add_argument("--live-stream-host", default="127.0.0.1")
    parser.add_argument("--live-stream-port", type=int, default=8794)
    parser.add_argument("--live-stream-replay-capacity", type=int, default=10_000)
    arguments = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    if arguments.market_id and arguments.catalog_only:
        parser.error("--catalog-only cannot be combined with --market-id")
    with live_collector_lease(arguments.root):
        store = (
            load_scoped_live_store(arguments.root, arguments.market_id)
            if arguments.market_id else LiveStateStore(arguments.root)
        )
        collector = PolymarketLiveCollector(
            store,
            GammaKeysetCatalog(
                spool_root=arguments.root / "spool",
                progress_every_pages=arguments.catalog_progress_pages,
            ),
            ClobBooksClient(
                timeout=(1.75 if arguments.market_id else 20),
                max_retries_per_batch=(1 if arguments.market_id else 5),
            ),
            shard_size=arguments.shard_size,
            max_websocket_connections=arguments.max_websocket_connections,
            snapshot_refresh_seconds=arguments.snapshot_refresh_seconds,
            catalog_refresh_on_lifecycle_events=not bool(arguments.market_id),
            publish_checkpoints=not bool(arguments.market_id),
            minimum_snapshot_refresh_age_seconds=(0.25 if arguments.market_id else 0),
            # A bounded scope has at most 200 tokens, so /books can refresh it
            # in one request.  Parallel workers share one requests.Session and
            # serialize on the same durable publication boundary; under load
            # they create overlapping TLS requests and an unbounded commit
            # backlog that makes the resulting state older, not fresher.
            max_concurrent_snapshot_refreshes=1,
        )
        if arguments.market_id:
            print({
                "mode": "indexed_scope",
                "market_ids": arguments.market_id,
                "active_token_count": len(store.token_to_market),
                "starting_cursor": store.cursor,
            })
        else:
            evidence = collector.refresh_catalog()
            print(evidence)
        if arguments.catalog_only:
            return
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
            collector_task = (
                asyncio.create_task(
                    collector.run(), name="polymarket-upstream-websocket",
                )
                if not arguments.bootstrap_only else None
            )
            try:
                await bootstrap_books_until_ready(collector)
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
