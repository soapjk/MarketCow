#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
from pathlib import Path

from marketcow.polymarket_live import (
    ClobBooksClient,
    GammaKeysetCatalog,
    LiveStateStore,
    PolymarketLiveCollector,
)


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
    arguments = parser.parse_args()

    store = LiveStateStore(arguments.root)
    collector = PolymarketLiveCollector(
        store, GammaKeysetCatalog(), ClobBooksClient(),
        shard_size=arguments.shard_size,
    )
    evidence = collector.refresh_catalog()
    print(evidence)
    if arguments.catalog_only:
        return
    asyncio.run(collector.bootstrap_books())
    print(store.health().model_dump(mode="json"))
    if not arguments.bootstrap_only:
        asyncio.run(collector.run())


if __name__ == "__main__":
    main()
