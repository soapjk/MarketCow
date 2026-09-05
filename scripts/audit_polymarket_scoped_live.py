#!/usr/bin/env python3
"""Audit a frozen scoped live.v2 selection without weakening readiness gates."""

from __future__ import annotations

import argparse
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

from marketcow.polymarket_contracts import canonical_json, content_sha256
from marketcow.polymarket_live import PolymarketLiveReadStore
from marketcow.polymarket_sources import _atomic_write


def audit_scoped_live(
    *, root: Path, selection_path: Path, output_path: Path,
) -> dict[str, object]:
    selection = json.loads(selection_path.resolve(strict=True).read_bytes())
    market_ids = selection.get("market_ids")
    if not isinstance(market_ids, list) or len(market_ids) != 100:
        raise ValueError("scoped live audit requires the frozen 100-market selection")

    reader = PolymarketLiveReadStore(root.resolve(), stable_read_wait_seconds=0)
    bootstrap = reader.bootstrap(market_ids, _bind_live_books=False)
    snapshot = reader.snapshot(market_ids, _wait_for_stable_boundary=False)
    markets = {item.identity.market_id: item for item in bootstrap.markets}
    frames = {item.market_id: item for item in snapshot.items}
    results = []
    reason_counts: Counter[str] = Counter()
    for market_id in market_ids:
        market = markets[market_id]
        frame = frames[market_id]
        reasons = set(frame.reason_codes)
        instrument = market.rules.instrument
        fee = market.rules.fee_schedule
        token_ids = [outcome.token_id for outcome in market.identity.outcomes]
        books = [snapshot.books.get(token_id) for token_id in token_ids]
        if any(book is None for book in books):
            reasons.add("missing_book")
        if any(
            book is None or not book.bids or not book.asks
            for book in books
        ):
            reasons.add("incomplete_book")
        if any(
            book is not None
            and instrument.price_increment is not None
            and book.tick_size != instrument.price_increment
            for book in books
        ):
            reasons.add("inconsistent_tick")
        if not instrument.complete:
            reasons.add("instrument_incomplete")
        if not fee.complete:
            reasons.add("fee_schedule_incomplete")
        reason_codes = sorted(reasons)
        reason_counts.update(reason_codes)
        results.append({
            "market_id": market_id,
            "status": "ready" if not reason_codes else "rejected",
            "reason_codes": reason_codes,
            "token_ids": token_ids,
            "frame_cursor": frame.cursor,
            "book_received_at": [
                book.received_at.isoformat() if book is not None else None
                for book in books
            ],
            "instrument_complete": instrument.complete,
            "fee_schedule_complete": fee.complete,
            "fee_calculation_status": fee.calculation_status,
        })

    ready_count = sum(item["status"] == "ready" for item in results)
    report: dict[str, object] = {
        "schema_version": "marketcow.polymarket.scoped-live-audit.v1",
        "observed_at": datetime.now(timezone.utc).isoformat(),
        "root": str(root.resolve()),
        "selection_path": str(selection_path.resolve()),
        "selection_evidence_sha256": selection.get("selection_evidence_sha256"),
        "catalog_revision": bootstrap.catalog_revision,
        "scope_id": reader.scope_id,
        "boundary_cursor": snapshot.cursor,
        "market_count": len(results),
        "ready_market_count": ready_count,
        "rejected_market_count": len(results) - ready_count,
        "reason_counts": dict(sorted(reason_counts.items())),
        "markets": results,
    }
    report["evidence_sha256"] = content_sha256(report)
    _atomic_write(output_path.resolve(), canonical_json(report))
    return report


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--selection", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    arguments = parser.parse_args()
    for name in ("root", "selection", "output"):
        if not getattr(arguments, name).is_absolute():
            parser.error(f"--{name} must be absolute")
    report = audit_scoped_live(
        root=arguments.root,
        selection_path=arguments.selection,
        output_path=arguments.output,
    )
    print(json.dumps({
        key: report[key]
        for key in (
            "catalog_revision", "scope_id", "boundary_cursor", "market_count",
            "ready_market_count", "rejected_market_count", "reason_counts",
            "evidence_sha256",
        )
    }, sort_keys=True))


if __name__ == "__main__":
    main()
