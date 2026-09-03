#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
import logging
from pathlib import Path

from marketcow.polymarket_live import (
    ClobBooksClient,
    GammaCatalogRows,
    GammaFeeSemanticsPolicy,
    GammaKeysetCatalog,
    GammaLiveNormalizer,
    GammaRealtimeUniversePolicy,
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


def required_market_ids_from_scope(path: Path | None) -> tuple[str, ...]:
    if path is None:
        return ()
    payload = json.loads(path.read_text(encoding="utf-8"))
    market_ids = payload.get("market_ids")
    if not isinstance(market_ids, list) or any(
        not isinstance(value, str) or not value for value in market_ids
    ):
        raise ValueError("required realtime scope has invalid market_ids")
    return tuple(dict.fromkeys(market_ids))


def refresh_catalog_or_reuse_published(
    collector: PolymarketLiveCollector,
    *,
    recover_state: bool = True,
) -> dict:
    """Start from verified local state without blocking on a remote refresh.

    LiveStateStore has already integrity-checked its durable catalog. Reuse is
    permitted only when that verified catalog is non-empty. An uninitialized
    collector must fetch the authoritative catalog and still fails closed if
    that first publication cannot be completed.
    """
    store = collector.store
    catalog = getattr(store, "catalog", None)
    catalog_path = getattr(store, "catalog_path", None)
    load_catalog = getattr(store, "_load_catalog", None)
    if (
        not catalog
        and isinstance(catalog_path, Path)
        and catalog_path.is_file()
        and callable(load_catalog)
    ):
        # Loading the immutable catalog boundary is independent of the legacy
        # event-derived SQLite projection and must never trigger its replay.
        load_catalog(catalog_path)
        catalog = getattr(store, "catalog", None)
        if (
            recover_state
            and isinstance(store, LiveStateStore)
            and not store._recovered
        ):
            # The immutable catalog has already passed its complete integrity
            # checks. Replay every durable event and rebuild its derived index,
            # but do not parse the multi-gigabyte catalog a second time.
            store.recover_with_loaded_catalog()
    if catalog:
        policy = getattr(
            getattr(collector, "catalog_client", None),
            "fee_semantics_policy",
            None,
        )
        policy_fillable_fields = {
            "currency", "maker_rate", "formula", "quantum", "effective_from",
        }
        incomplete_count = (
            sum(
                bool(
                    policy_fillable_fields.intersection(
                        market.rules.fee_schedule.missing_fields
                    )
                )
                for market in catalog.values()
            )
            if policy is not None else 0
        )
        if policy is not None and incomplete_count:
            raw_path = getattr(collector.store, "raw_catalog_path", None)
            source = getattr(collector.store, "catalog_source", None) or {}
            raw_format = source.get("raw_format")
            if raw_path is None or raw_format not in {
                "canonical_jsonl", "canonical_json_array",
            }:
                raise RuntimeError(
                    "published catalog cannot be rebuilt from verified Gamma evidence"
                )
            # The verified on-disk catalog remains the rollback boundary. Do not
            # retain its full Pydantic object graph while constructing another
            # full-market generation from immutable raw evidence: that doubles
            # the working set and can force the host into swap.
            catalog.clear()
            store.token_to_market.clear()
            rows = (
                GammaCatalogRows(
                    raw_path,
                    row_count=int(source.get("market_count") or 0),
                    sha256=str(source.get("raw_payload_sha256") or ""),
                )
                if raw_format == "canonical_jsonl"
                else json.loads(raw_path.read_text(encoding="utf-8"))
            )
            markets = GammaLiveNormalizer.normalize(
                rows,
                collector.store.now_provider(),
                fee_semantics_policy=policy,
            )
            update = collector.store.replace_catalog(markets, rows)
            remaining = sum(
                not market.rules.fee_schedule.complete for market in markets
            )
            LOGGER.info(
                "polymarket_catalog_fee_policy_rebuilt market_count=%d "
                "previous_incomplete_count=%d remaining_incomplete_count=%d",
                len(markets), incomplete_count, remaining,
            )
            return {
                "status": "published_catalog_fee_policy_rebuilt",
                "market_count": len(markets),
                "previous_incomplete_count": incomplete_count,
                "remaining_incomplete_count": remaining,
                **update,
            }
        LOGGER.info(
            "polymarket_catalog_startup_reusing_published market_count=%d",
            len(catalog),
        )
        return {
            "status": "published_catalog_reused",
            "market_count": len(catalog),
        }
    return collector.refresh_catalog()


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
        "--realtime-market-limit", type=int,
        help=(
            "Keep the complete Gamma catalog, but bootstrap and subscribe only "
            "this many ranked discovery markets"
        ),
    )
    parser.add_argument(
        "--required-realtime-scope", type=Path,
        help="Prefer eligible incumbent market_ids from this Rust scope manifest",
    )
    parser.add_argument(
        "--fee-semantics-policy",
        type=Path,
        help=(
            "Absolute, versioned protocol policy used to complete Gamma fee facts; "
            "without it missing provider fields remain fail-closed"
        ),
    )
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
    if arguments.market_id and arguments.realtime_market_limit is not None:
        parser.error("--realtime-market-limit cannot be combined with --market-id")
    if (
        arguments.required_realtime_scope is not None
        and arguments.realtime_market_limit is None
    ):
        parser.error(
            "--required-realtime-scope requires --realtime-market-limit"
        )
    if arguments.fee_semantics_policy is not None and not (
        arguments.fee_semantics_policy.is_absolute()
    ):
        parser.error("--fee-semantics-policy must be absolute")
    fee_semantics_policy = (
        GammaFeeSemanticsPolicy.from_path(arguments.fee_semantics_policy)
        if arguments.fee_semantics_policy is not None else None
    )
    realtime_universe_policy = (
        GammaRealtimeUniversePolicy(
            arguments.realtime_market_limit,
            required_market_ids=required_market_ids_from_scope(
                arguments.required_realtime_scope
            ),
        )
        if arguments.realtime_market_limit is not None else None
    )
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
                fee_semantics_policy=fee_semantics_policy,
            ),
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
            catalog_refresh_on_lifecycle_events=not bool(arguments.market_id),
            publish_checkpoints=not bool(arguments.market_id),
            minimum_snapshot_refresh_age_seconds=(0.25 if arguments.market_id else 0),
            # Keep complete market/negative-risk groups together, but do not
            # let one slow 200-token CLOB request age the entire exact scope.
            # Periodic attempts are bounded and retry on their next cadence;
            # startup/reconnect recovery retains the stronger retry policy.
            max_concurrent_snapshot_refreshes=(4 if arguments.market_id else 1),
            periodic_snapshot_request_timeout=(
                (1.75, 1.75) if arguments.market_id else None
            ),
            periodic_snapshot_max_retries=(0 if arguments.market_id else None),
            realtime_universe_policy=realtime_universe_policy,
        )
        if arguments.market_id:
            print({
                "mode": "indexed_scope",
                "market_ids": arguments.market_id,
                "active_token_count": len(store.token_to_market),
                "starting_cursor": store.cursor,
            })
        else:
            # Establish the bounded, book-backed generation before replaying
            # mutable state.  A legacy manifest may map hundreds of thousands
            # of Gamma directory entries; recovering that generation first is
            # both wasteful and exposes stale state to concurrently-started
            # readers.
            evidence = refresh_catalog_or_reuse_published(
                collector, recover_state=False,
            )
            realtime_evidence = collector.configure_published_realtime_universe()
            if realtime_evidence is not None:
                evidence = {**evidence, **realtime_evidence}
            if isinstance(store, LiveStateStore) and not store._recovered:
                store.recover_with_loaded_catalog()
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
