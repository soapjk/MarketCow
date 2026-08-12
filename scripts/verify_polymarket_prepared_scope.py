#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
from pathlib import Path

from marketcow.polymarket_live import PolymarketLiveReadStore


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def verify(root: Path, manifest_path: Path) -> dict[str, object]:
    root = root.resolve()
    manifest_path = manifest_path.resolve()
    manifest_body = manifest_path.read_bytes()
    manifest_sha256 = hashlib.sha256(manifest_body).hexdigest()
    manifest = json.loads(manifest_body)
    market_ids = [str(item) for item in manifest["market_ids"]]
    if len(market_ids) != 100 or len(set(market_ids)) != 100:
        raise RuntimeError("scope manifest must contain exactly 100 unique markets")

    binding = json.loads((root / "scope-binding.json").read_text(encoding="utf-8"))
    boundary_path = root / "prepared-boundary.json"
    boundary = json.loads(boundary_path.read_text(encoding="utf-8"))
    if binding.get("manifest_sha256") != manifest_sha256:
        raise RuntimeError("prepared scope manifest checksum mismatch")
    if binding.get("scope_id") != manifest.get("scope_id"):
        raise RuntimeError("prepared scope ID mismatch")
    if binding.get("catalog_revision") != manifest.get("catalog_revision"):
        raise RuntimeError("prepared scope catalog revision mismatch")
    if boundary.get("manifest_sha256") != manifest_sha256:
        raise RuntimeError("prepared boundary manifest checksum mismatch")
    if boundary.get("scope_binding_sha256") != sha256(root / "scope-binding.json"):
        raise RuntimeError("prepared boundary scope binding checksum mismatch")

    reader = PolymarketLiveReadStore(root)
    bootstrap = reader.bootstrap(market_ids)
    expected_tokens = {
        outcome.token_id
        for market in bootstrap.markets
        for outcome in market.identity.outcomes
    }
    if bootstrap.catalog_revision != manifest["catalog_revision"]:
        raise RuntimeError("bootstrap catalog revision mismatch")
    if {market.identity.market_id for market in bootstrap.markets} != set(market_ids):
        raise RuntimeError("bootstrap does not exactly cover the scope manifest")
    if len(expected_tokens) != 200:
        raise RuntimeError("scope catalog does not contain exactly 200 unique tokens")

    state_manifest = json.loads(
        (root / "state-index.json").read_text(encoding="utf-8")
    )
    state_path = Path(state_manifest["path"]).resolve()
    if not state_path.is_relative_to(root / "indexes"):
        raise RuntimeError("prepared state index escapes its root")
    if state_manifest.get("catalog_revision") != manifest["catalog_revision"]:
        raise RuntimeError("state index catalog revision mismatch")
    if boundary.get("state_index_manifest_sha256") != sha256(
        root / "state-index.json"
    ):
        raise RuntimeError("prepared boundary state manifest checksum mismatch")
    if boundary.get("state_index_sqlite_sha256") != sha256(state_path):
        raise RuntimeError("prepared boundary state database checksum mismatch")
    if boundary.get("events_sha256") != sha256(root / "events.jsonl"):
        raise RuntimeError("prepared boundary event log checksum mismatch")

    with sqlite3.connect(f"file:{state_path}?mode=ro&immutable=1", uri=True) as db:
        rows = db.execute(
            "SELECT market_id, token_id FROM books ORDER BY market_id, token_id"
        ).fetchall()
        unresolved = db.execute(
            "SELECT COUNT(*) FROM gaps WHERE resolved=0"
        ).fetchone()[0]
        active_recovery_id = db.execute(
            "SELECT value FROM metadata WHERE key='active_recovery_id'"
        ).fetchone()

    actual_tokens = {str(token_id) for _, token_id in rows}
    book_counts = {market_id: 0 for market_id in market_ids}
    extra_market_ids = set()
    for market_id, _ in rows:
        if market_id in book_counts:
            book_counts[market_id] += 1
        else:
            extra_market_ids.add(str(market_id))
    invalid_counts = {
        market_id: count for market_id, count in book_counts.items() if count != 2
    }
    missing_tokens = sorted(expected_tokens - actual_tokens)
    extra_tokens = sorted(actual_tokens - expected_tokens)
    recovery = str(active_recovery_id[0]) if active_recovery_id else ""
    if invalid_counts or extra_market_ids or missing_tokens or extra_tokens:
        raise RuntimeError("prepared state does not exactly cover the scope manifest")
    if unresolved or recovery:
        raise RuntimeError("prepared state has unresolved gaps or active recovery")

    return {
        "schema_version": "marketcow.polymarket.prepared-scope-verification.v1",
        "root": str(root),
        "scope_id": manifest["scope_id"],
        "manifest_path": str(manifest_path),
        "manifest_sha256": manifest_sha256,
        "binding_sha256": sha256(root / "scope-binding.json"),
        "boundary_path": str(boundary_path),
        "boundary_sha256": sha256(boundary_path),
        "prepared_at": boundary["prepared_at"],
        "catalog_revision": manifest["catalog_revision"],
        "state_index_path": str(state_path),
        "latest_cursor": int(state_manifest["latest_cursor"]),
        "market_count": len(book_counts),
        "book_token_count": len(rows),
        "expected_token_count": len(expected_tokens),
        "markets_with_exactly_two_books": sum(
            count == 2 for count in book_counts.values()
        ),
        "unresolved_gap_count": int(unresolved),
        "active_recovery_id": recovery or None,
        "missing_market_ids": [],
        "extra_market_ids": [],
        "missing_token_ids": [],
        "extra_token_ids": [],
        "exact_scope": True,
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Verify an exact-100 immutable Polymarket prepared scope"
    )
    parser.add_argument("--root", required=True, type=Path)
    parser.add_argument("--manifest", required=True, type=Path)
    arguments = parser.parse_args()
    print(json.dumps(verify(arguments.root, arguments.manifest), sort_keys=True))


if __name__ == "__main__":
    main()
