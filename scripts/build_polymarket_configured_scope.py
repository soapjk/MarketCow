#!/usr/bin/env python3
"""Build an exact configured scope from an immutable catalog and selection.

This tool performs indexed catalog reads only.  It never traverses Gamma and
never starts a collector.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from marketcow.polymarket_configured_scope import PolymarketConfiguredScope
from marketcow.polymarket_contracts import canonical_json, content_sha256
from marketcow.polymarket_live import PolymarketLiveReadStore
from marketcow.polymarket_sources import _atomic_write


SELECTION_SCHEMA = "tradude.prediction_market.scope_selection.v2"


def digest(path: Path) -> str:
    result = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            result.update(chunk)
    return result.hexdigest()


def build(
    *,
    storage_root: Path,
    source_root: Path,
    selection_path: Path,
    selection_sha256: str,
    output: Path,
    report_path: Path,
) -> dict[str, object]:
    storage_root = storage_root.resolve(strict=True)
    source_root = source_root.resolve(strict=True)
    selection_path = selection_path.resolve(strict=True)
    output = output.resolve()
    report_path = report_path.resolve()
    if not source_root.is_relative_to(storage_root):
        raise ValueError("source root escapes isolated storage")
    if not output.is_relative_to(storage_root) or not report_path.is_relative_to(
        storage_root
    ):
        raise ValueError("scope output escapes isolated storage")
    if digest(selection_path) != selection_sha256:
        raise ValueError("selection SHA-256 mismatch")
    selection = json.loads(selection_path.read_bytes())
    market_ids = selection.get("market_ids")
    if (
        selection.get("schema") != SELECTION_SCHEMA
        or not isinstance(market_ids, list)
        or not 1 <= len(market_ids) <= 250
        or len(set(market_ids)) != len(market_ids)
        or any(not isinstance(item, str) or not item for item in market_ids)
    ):
        raise ValueError("selection must contain 1..250 unique market IDs")

    manifest = json.loads((source_root / "catalog.json").read_bytes())
    revision = str(manifest.get("catalog_revision") or "")
    if selection.get("catalog_revision") != revision:
        raise ValueError("selection catalog revision mismatch")
    reader = PolymarketLiveReadStore(source_root)
    markets = []
    active_tokens: set[str] = set()
    for offset in range(0, len(market_ids), 100):
        bootstrap = reader.bootstrap(
            market_ids[offset : offset + 100], _bind_live_books=False
        )
        if bootstrap.catalog_revision != revision:
            raise ValueError("indexed catalog revision mismatch")
        markets.extend(bootstrap.markets)
        active_tokens.update(bootstrap.active_token_ids)
    if [market.identity.market_id for market in markets] != market_ids:
        raise ValueError("indexed catalog order or identity mismatch")

    configured_markets = []
    selected = set(market_ids)
    relation_dependencies: set[str] = set()
    for market in markets:
        configured_markets.append(
            {
                "market_id": market.identity.market_id,
                "condition_id": market.identity.condition_id,
                "token_ids": [item.token_id for item in market.identity.outcomes],
                "end_at": market.end_at.isoformat().replace("+00:00", "Z") if market.end_at else None,
            }
        )
        for relation in market.relations:
            relation_dependencies.update(
                pair.market_id
                for pair in relation.outcome_pairs
                if pair.market_id not in selected
            )
    identity = {
        "catalog_revision": revision,
        "configured_markets": configured_markets,
        "mode": "shadow",
    }
    scope = PolymarketConfiguredScope.model_validate(
        {
            **identity,
            "schema_version": "marketcow.polymarket.scope-discovery.v1",
            "active_scope_id": content_sha256(identity),
            "configured_market_count": len(configured_markets),
        }
    )
    _atomic_write(output, canonical_json(scope.model_dump(mode="json")))
    report = {
        "schema_version": "marketcow.polymarket.configured-scope-build.v1",
        "complete": True,
        "source_root": str(source_root),
        "selection_path": str(selection_path),
        "selection_sha256": selection_sha256,
        "selection_evidence_sha256": selection.get("selection_evidence_sha256"),
        "catalog_revision": revision,
        "configured_market_count": len(configured_markets),
        "active_token_count": len(active_tokens),
        "active_scope_id": scope.active_scope_id,
        "configured_scope_path": str(output),
        "configured_scope_sha256": digest(output),
        "relation_dependency_market_count": len(relation_dependencies),
        "relation_dependency_market_ids_sha256": content_sha256(
            sorted(relation_dependencies)
        ),
    }
    _atomic_write(report_path, canonical_json(report))
    return report


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--storage-root", required=True, type=Path)
    parser.add_argument("--source-root", required=True, type=Path)
    parser.add_argument("--selection", required=True, type=Path)
    parser.add_argument("--selection-sha256", required=True)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--report", required=True, type=Path)
    args = parser.parse_args()
    os.umask(0o077)
    print(json.dumps(build(
        storage_root=args.storage_root,
        source_root=args.source_root,
        selection_path=args.selection,
        selection_sha256=args.selection_sha256,
        output=args.output,
        report_path=args.report,
    ), sort_keys=True))


if __name__ == "__main__":
    main()
