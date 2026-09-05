#!/usr/bin/env python3
"""Explicitly build the immutable startup boundary for Polymarket discovery."""

from __future__ import annotations

import argparse
import json
import logging
import os
from pathlib import Path
from datetime import datetime, timezone

from marketcow.polymarket_live import (
    ClobBooksClient,
    GammaFeeSemanticsPolicy,
    GammaKeysetCatalog,
    GammaNormalizedCatalog,
    GammaRealtimeUniversePolicy,
    LiveStateStore,
    PolymarketLiveCollector,
    live_collector_lease,
)
from marketcow.polymarket_contracts import canonical_json
from marketcow.polymarket_sources import _atomic_write


def progress_writer(path: Path):
    def write(payload: dict) -> None:
        document = {
            **payload,
            "pid": os.getpid(),
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        _atomic_write(path, canonical_json(document))
    return write


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


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Traverse authoritative Gamma, verify CLOB coverage, and atomically "
            "publish a bounded Polymarket discovery startup generation"
        ),
    )
    parser.add_argument("--root", required=True, type=Path)
    parser.add_argument("--realtime-market-limit", required=True, type=int)
    parser.add_argument("--required-realtime-scope", type=Path)
    parser.add_argument("--fee-semantics-policy", required=True, type=Path)
    parser.add_argument("--catalog-progress-pages", type=int, default=25)
    parser.add_argument("--shard-size", type=int, default=500)
    parser.add_argument("--progress-path", type=Path)
    parser.add_argument("--catalog-spool-root", type=Path)
    parser.add_argument("--retain-verified-spool", action="store_true")
    parser.add_argument("--normalized-catalog-path", type=Path)
    parser.add_argument("--normalized-catalog-revision")
    parser.add_argument("--normalized-catalog-sha256")
    parser.add_argument("--normalized-market-count", type=int)
    arguments = parser.parse_args()
    if not arguments.root.is_absolute():
        parser.error("--root must be absolute")
    if arguments.realtime_market_limit <= 0:
        parser.error("--realtime-market-limit must be positive")
    if not arguments.fee_semantics_policy.is_absolute():
        parser.error("--fee-semantics-policy must be absolute")
    normalized_arguments = (
        arguments.normalized_catalog_path,
        arguments.normalized_catalog_revision,
        arguments.normalized_catalog_sha256,
        arguments.normalized_market_count,
    )
    if any(value is not None for value in normalized_arguments) and not all(
        value is not None for value in normalized_arguments
    ):
        parser.error("all normalized catalog reuse arguments are required together")
    if (
        arguments.normalized_catalog_path is not None
        and not arguments.normalized_catalog_path.is_absolute()
    ):
        parser.error("--normalized-catalog-path must be absolute")
    catalog_spool_root = arguments.catalog_spool_root or arguments.root / "spool"
    if not catalog_spool_root.is_absolute():
        parser.error("--catalog-spool-root must be absolute")

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    progress_path = (
        arguments.progress_path
        if arguments.progress_path is not None
        else arguments.root / "prepare-progress.json"
    )
    if not progress_path.is_absolute():
        parser.error("--progress-path must be absolute")
    report_progress = progress_writer(progress_path)
    report_progress({"phase": "starting", "complete": False})
    policy = GammaRealtimeUniversePolicy(
        arguments.realtime_market_limit,
        required_market_ids=required_market_ids_from_scope(
            arguments.required_realtime_scope
        ),
    )
    try:
        with live_collector_lease(arguments.root):
            store = LiveStateStore(arguments.root)
            collector = PolymarketLiveCollector(
                store,
                GammaKeysetCatalog(
                    spool_root=catalog_spool_root,
                    progress_every_pages=arguments.catalog_progress_pages,
                    progress=lambda item: report_progress({
                        **item, "phase": "gamma_catalog",
                    }),
                    fee_semantics_policy=GammaFeeSemanticsPolicy.from_path(
                        arguments.fee_semantics_policy
                    ),
                    retain_verified_spool=arguments.retain_verified_spool,
                ),
                ClobBooksClient(),
                shard_size=arguments.shard_size,
                realtime_universe_policy=policy,
                catalog_refresh_on_lifecycle_events=False,
                preparation_progress=lambda item: report_progress({
                    **item,
                    "stage_complete": item["complete"],
                    "complete": False,
                }),
            )
            normalized_catalog = (
                GammaNormalizedCatalog(
                    arguments.normalized_catalog_path,
                    market_count=arguments.normalized_market_count,
                    revision=arguments.normalized_catalog_revision,
                    sha256=arguments.normalized_catalog_sha256,
                )
                if arguments.normalized_catalog_path is not None else None
            )
            result = collector.refresh_catalog(
                normalized_catalog=normalized_catalog,
            )
            recovery = collector.publish_catalog_selection_books()
            manifest = json.loads(store.catalog_path.read_text(encoding="utf-8"))
            selection_report = manifest["realtime_universe"]["selection_report"]
            report_sha256 = selection_report["report_sha256"]
            report_path = (
                arguments.root / "selection-reports" / f"{report_sha256}.json"
            )
            _atomic_write(report_path, canonical_json(selection_report))
            _atomic_write(
                arguments.root / "selection-report.json",
                canonical_json({
                    "path": str(report_path.resolve()),
                    "report_sha256": report_sha256,
                }),
            )
            health = store.health().model_dump(mode="json")
            complete = {
                "phase": "complete",
                "complete": True,
                "catalog": result,
                "recovery": recovery,
                "health": health,
                "selection_report_path": str(report_path.resolve()),
                "selection_report_sha256": report_sha256,
            }
            report_progress(complete)
    except BaseException as exc:
        report_progress({
            "phase": "failed",
            "complete": False,
            "error_type": type(exc).__name__,
            "error": str(exc),
        })
        raise
    print(json.dumps(complete, sort_keys=True))


if __name__ == "__main__":
    main()
