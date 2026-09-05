#!/usr/bin/env python3
"""Rebuild only the bounded realtime universe from a prepared Gamma catalog."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from marketcow.polymarket_contracts import canonical_json
from marketcow.polymarket_live import (
    ClobBooksClient,
    GammaCatalogRows,
    GammaKeysetCatalog,
    GammaNormalizedCatalog,
    GammaRealtimeUniversePolicy,
    LiveStateStore,
    PolymarketLiveCollector,
    live_collector_lease,
)
from marketcow.polymarket_sources import _atomic_write


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Re-select and bootstrap a bounded universe from an existing, "
            "checksum-bound local Gamma catalog without traversing Gamma"
        )
    )
    parser.add_argument("--root", required=True, type=Path)
    parser.add_argument("--realtime-market-limit", required=True, type=int)
    arguments = parser.parse_args()
    if not arguments.root.is_absolute():
        parser.error("--root must be absolute")
    if arguments.realtime_market_limit <= 0:
        parser.error("--realtime-market-limit must be positive")

    root = arguments.root.resolve()
    manifest = json.loads((root / "catalog.json").read_bytes())
    normalized_source = manifest["normalized_catalog"]
    catalog_source = manifest["catalog_source"]
    normalized = GammaNormalizedCatalog(
        Path(normalized_source["path"]),
        market_count=int(normalized_source["market_count"]),
        revision=str(manifest["catalog_revision"]),
        sha256=str(normalized_source["sha256"]),
    )
    normalized.validate()
    raw = GammaCatalogRows(
        Path(catalog_source["raw_path"]),
        row_count=int(catalog_source["market_count"]),
        sha256=str(catalog_source["raw_payload_sha256"]),
    )

    with live_collector_lease(root):
        store = LiveStateStore(root)
        # Recover the currently published generation before replacing its
        # token map. Otherwise the first post-publication recovery would try
        # to replay the old checkpoint against the new universe.
        store.recover()
        collector = PolymarketLiveCollector(
            store,
            GammaKeysetCatalog(),
            ClobBooksClient(),
            realtime_universe_policy=GammaRealtimeUniversePolicy(
                arguments.realtime_market_limit
            ),
            catalog_refresh_on_lifecycle_events=False,
        )
        universe = collector._select_realtime_universe(
            normalized,
            raw,
            catalog_revision=normalized.revision,
        )
        # The resident store intentionally contains only the previously
        # published bounded generation. Load the newly selected rows from the
        # checksum-verified normalized catalog before the publication guard
        # validates exact market coverage.
        store.catalog.update(normalized.load_market_ids(universe["market_ids"]))
        update = store.replace_realtime_universe(universe)
        selection_report = universe["selection_report"]
        report_sha256 = selection_report["report_sha256"]
        report_path = root / "selection-reports" / f"{report_sha256}.json"
        _atomic_write(report_path, canonical_json(selection_report))
        _atomic_write(
            root / "selection-report.json",
            canonical_json({
                "path": str(report_path.resolve()),
                "report_sha256": report_sha256,
            }),
        )
        recovery = collector.publish_catalog_selection_books(
            reason="prepared_catalog_reselection_boundary"
        )
    print(json.dumps({
        "complete": True,
        "catalog_revision": normalized.revision,
        "universe": universe,
        "update": update,
        "recovery": recovery,
        "selection_report_path": str(report_path),
    }, sort_keys=True))


if __name__ == "__main__":
    main()
