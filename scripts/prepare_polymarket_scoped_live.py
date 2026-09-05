#!/usr/bin/env python3
"""Promote one verified catalog into an isolated bounded live.v2 root."""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import subprocess
from pathlib import Path
from typing import Any

from marketcow.api import PolymarketConfiguredScope
from marketcow.polymarket_contracts import canonical_json, content_sha256
from marketcow.polymarket_live import PolymarketLiveReadStore
from marketcow.polymarket_scopes import write_scope_runtime
from marketcow.polymarket_sources import _atomic_write


SELECTION_SCHEMA = "tradude.prediction_market.scope_selection.v2"


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _verified_local_path(root: Path, raw: object, directory: str) -> Path:
    path = Path(str(raw or "")).resolve(strict=True)
    if not path.is_relative_to((root / directory).resolve()):
        raise ValueError(f"catalog {directory} path escapes source root")
    return path


def _clone_verified(source: Path, destination: Path, expected_sha256: str) -> None:
    if file_sha256(source) != expected_sha256:
        raise ValueError(f"source artifact hash mismatch: {source.name}")
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists():
        if file_sha256(destination) != expected_sha256:
            raise ValueError(f"target artifact differs: {destination}")
        return
    temporary = destination.with_name(f".{destination.name}.preparing")
    temporary.unlink(missing_ok=True)
    try:
        # APFS clone-on-write keeps a 2.6 GB catalog promotion bounded without
        # sharing an inode with the immutable source generation.
        cloned = subprocess.run(
            ["/bin/cp", "-c", str(source), str(temporary)],
            check=False,
            capture_output=True,
        )
        if cloned.returncode:
            shutil.copyfile(source, temporary)
        if file_sha256(temporary) != expected_sha256:
            raise ValueError(f"cloned artifact hash mismatch: {source.name}")
        temporary.replace(destination)
    finally:
        temporary.unlink(missing_ok=True)


def prepare_scoped_live(
    *,
    source_root: Path,
    target_root: Path,
    selection_path: Path,
    selection_sha256: str,
    configured_scope_path: Path,
) -> dict[str, Any]:
    source_root = source_root.resolve(strict=True)
    target_root = target_root.resolve()
    selection_path = selection_path.resolve(strict=True)
    configured_scope_path = configured_scope_path.resolve()
    if target_root == source_root or target_root.is_relative_to(source_root):
        raise ValueError("scoped live target must be independent of discovery root")
    if file_sha256(selection_path) != selection_sha256:
        raise ValueError("frozen selection SHA-256 mismatch")
    selection = json.loads(selection_path.read_bytes())
    market_ids = selection.get("market_ids")
    if (
        selection.get("schema") != SELECTION_SCHEMA
        or not isinstance(market_ids, list)
        or len(market_ids) != 100
        or len(set(market_ids)) != 100
        or any(not isinstance(value, str) or not value for value in market_ids)
    ):
        raise ValueError("frozen selection must contain exactly 100 unique markets")

    source_manifest_path = source_root / "catalog.json"
    source_manifest = json.loads(source_manifest_path.read_bytes())
    catalog_revision = str(source_manifest.get("catalog_revision") or "")
    if selection.get("catalog_revision") != catalog_revision:
        raise ValueError("frozen selection catalog revision mismatch")
    normalized = source_manifest.get("normalized_catalog")
    catalog_index = source_manifest.get("catalog_index")
    candidate = source_manifest.get("candidate_snapshot")
    catalog_source = source_manifest.get("catalog_source")
    if not all(
        isinstance(value, dict)
        for value in (normalized, catalog_index, candidate, catalog_source)
    ):
        raise ValueError("source catalog publication is incomplete")

    reader = PolymarketLiveReadStore(source_root)
    bootstrap = reader.bootstrap(market_ids, _bind_live_books=False)
    if bootstrap.catalog_revision != catalog_revision:
        raise ValueError("verified bootstrap catalog revision mismatch")

    bindings = (
        (normalized, "catalogs"),
        (catalog_index, "catalog-indexes"),
        (candidate, "candidate-snapshots"),
        ({
            "path": catalog_source.get("raw_path"),
            "sha256": catalog_source.get("raw_payload_sha256"),
        }, "raw/gamma-catalog"),
    )
    promoted: list[tuple[dict[str, Any], Path]] = []
    for metadata, directory in bindings:
        source = _verified_local_path(source_root, metadata.get("path"), directory)
        expected = str(metadata.get("sha256") or "")
        if len(expected) != 64:
            raise ValueError(f"catalog {directory} SHA-256 is invalid")
        destination = target_root / directory / source.name
        _clone_verified(source, destination, expected)
        promoted.append((metadata, destination))

    target_manifest = {
        **source_manifest,
        "normalized_catalog": {**normalized, "path": str(promoted[0][1])},
        "catalog_index": {**catalog_index, "path": str(promoted[1][1])},
        "candidate_snapshot": {**candidate, "path": str(promoted[2][1])},
        "catalog_source": {
            **catalog_source,
            "raw_path": str(promoted[3][1]),
        },
    }
    # A bounded writer receives its exact market list from the frozen scope;
    # retaining the broad discovery universe here would be misleading.
    target_manifest.pop("realtime_universe", None)
    _atomic_write(target_root / "catalog.json", canonical_json(target_manifest))

    target_bootstrap = PolymarketLiveReadStore(target_root).bootstrap(
        market_ids, _bind_live_books=False,
    )
    configured_markets = []
    missing_relation_market_ids: set[str] = set()
    selected = set(market_ids)
    for market in target_bootstrap.markets:
        if market.end_at is None:
            raise ValueError(f"configured market end_at is missing: {market.identity.market_id}")
        configured_markets.append({
            "market_id": market.identity.market_id,
            "condition_id": market.identity.condition_id,
            "token_ids": [item.token_id for item in market.identity.outcomes],
            "end_at": market.end_at.isoformat().replace("+00:00", "Z"),
        })
        for relation in market.relations:
            if relation.relation_type == "standard_negative_risk":
                missing_relation_market_ids.update(
                    pair.market_id for pair in relation.outcome_pairs
                    if pair.market_id not in selected
                )
    scope_body = {
        "schema_version": "marketcow.polymarket.scope-discovery.v1",
        "mode": "shadow",
        "catalog_revision": catalog_revision,
        "configured_market_count": len(configured_markets),
        "configured_markets": configured_markets,
    }
    scope_body["active_scope_id"] = content_sha256({
        "catalog_revision": catalog_revision,
        "configured_markets": configured_markets,
        "mode": "shadow",
    })
    scope = PolymarketConfiguredScope.model_validate(scope_body).model_dump(mode="json")
    _atomic_write(configured_scope_path, canonical_json(scope))
    scope_manifest_sha256 = file_sha256(configured_scope_path)
    write_scope_runtime(
        target_root,
        scope_id=scope["active_scope_id"],
        manifest_sha256=scope_manifest_sha256,
    )

    report = {
        "schema_version": "marketcow.polymarket.scoped-live-preparation.v1",
        "source_root": str(source_root),
        "target_root": str(target_root),
        "selection_path": str(selection_path),
        "selection_sha256": selection_sha256,
        "selection_evidence_sha256": selection.get("selection_evidence_sha256"),
        "catalog_revision": catalog_revision,
        "market_count": len(configured_markets),
        "token_count": len(target_bootstrap.active_token_ids),
        "active_scope_id": scope["active_scope_id"],
        "missing_relation_market_count": len(missing_relation_market_ids),
        "missing_relation_market_ids_sha256": content_sha256(
            sorted(missing_relation_market_ids)
        ),
        "configured_scope_path": str(configured_scope_path),
        "configured_scope_sha256": scope_manifest_sha256,
    }
    _atomic_write(target_root / "scoped-live-preparation.json", canonical_json(report))
    return report


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Promote a verified catalog and frozen 100-market selection into scoped live.v2",
    )
    parser.add_argument("--source-root", required=True, type=Path)
    parser.add_argument("--target-root", required=True, type=Path)
    parser.add_argument("--selection", required=True, type=Path)
    parser.add_argument("--selection-sha256", required=True)
    parser.add_argument("--configured-scope-output", required=True, type=Path)
    arguments = parser.parse_args()
    for name in ("source_root", "target_root", "selection", "configured_scope_output"):
        if not getattr(arguments, name).is_absolute():
            parser.error(f"--{name.replace('_', '-')} must be absolute")
    report = prepare_scoped_live(
        source_root=arguments.source_root,
        target_root=arguments.target_root,
        selection_path=arguments.selection,
        selection_sha256=arguments.selection_sha256,
        configured_scope_path=arguments.configured_scope_output,
    )
    print(json.dumps(report, sort_keys=True))


if __name__ == "__main__":
    main()
