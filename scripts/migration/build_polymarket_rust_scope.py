#!/usr/bin/env python3
"""Build a hash-pinned Rust Polymarket live scope from an exact market manifest."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sqlite3
import tempfile
from pathlib import Path
from typing import Any


SCHEMA_VERSION = "marketcow.polymarket.rust-live-scope.v1"


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _required_string(value: Any, name: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(f"{name} must be a non-empty string")
    return value


def _decimal_ids(value: Any, name: str) -> list[str]:
    if not isinstance(value, list) or not value:
        raise ValueError(f"{name} must be a non-empty array")
    identifiers = [_required_string(item, f"{name}[]") for item in value]
    if any(not item.isascii() or not item.isdecimal() for item in identifiers):
        raise ValueError(f"{name} must contain decimal identifiers")
    if len(set(identifiers)) != len(identifiers):
        raise ValueError(f"{name} must not contain duplicates")
    return identifiers


def build_scope(
    manifest_path: Path,
    catalog_index_path: Path,
    *,
    expected_market_count: int,
    expected_token_count: int,
    expected_manifest_sha256: str | None = None,
) -> dict[str, Any]:
    manifest_bytes = manifest_path.read_bytes()
    manifest_sha256 = hashlib.sha256(manifest_bytes).hexdigest()
    if expected_manifest_sha256 and manifest_sha256 != expected_manifest_sha256.lower():
        raise ValueError("exact-scope manifest SHA-256 mismatch")
    manifest = json.loads(manifest_bytes)
    if not isinstance(manifest, dict):
        raise ValueError("exact-scope manifest must be a JSON object")
    scope_id = _required_string(manifest.get("scope_id"), "scope_id")
    market_ids = _decimal_ids(manifest.get("market_ids"), "market_ids")
    if len(market_ids) != expected_market_count:
        raise ValueError(
            f"exact-scope market count mismatch: {len(market_ids)} != {expected_market_count}"
        )

    uri = f"{catalog_index_path.resolve().as_uri()}?mode=ro&immutable=1"
    with sqlite3.connect(uri, uri=True) as connection:
        metadata = dict(connection.execute("SELECT key, value FROM metadata"))
        placeholders = ",".join("?" for _ in market_ids)
        rows = connection.execute(
            f"SELECT market_id, token_id FROM tokens WHERE market_id IN ({placeholders}) "
            "ORDER BY market_id, token_id",
            market_ids,
        ).fetchall()

    by_market: dict[str, list[str]] = {market_id: [] for market_id in market_ids}
    for market_id, token_id in rows:
        if market_id in by_market:
            by_market[market_id].append(token_id)
    invalid = {
        market_id: tokens
        for market_id, tokens in by_market.items()
        if len(tokens) != 2 or len(set(tokens)) != 2
    }
    if invalid:
        raise ValueError(
            "each exact-scope market must resolve to exactly two unique CLOB tokens: "
            + ",".join(sorted(invalid))
        )
    token_ids = sorted(token for tokens in by_market.values() for token in tokens)
    if len(token_ids) != expected_token_count or len(set(token_ids)) != len(token_ids):
        raise ValueError(
            f"exact-scope token count mismatch: {len(token_ids)} != {expected_token_count}"
        )
    if any(not token.isascii() or not token.isdecimal() for token in token_ids):
        raise ValueError("catalog index returned a non-decimal token identifier")

    catalog_revision = _required_string(metadata.get("catalog_revision"), "catalog_revision")
    manifest_revision = manifest.get("catalog_revision")
    if manifest_revision is not None and manifest_revision != catalog_revision:
        raise ValueError("manifest/catalog index revision mismatch")
    catalog_index_sha256 = sha256_file(catalog_index_path)
    return {
        "schema_version": SCHEMA_VERSION,
        "scope_id": scope_id,
        "market_count": len(market_ids),
        "token_count": len(token_ids),
        "market_ids": sorted(market_ids),
        "token_ids": token_ids,
        "source": {
            "manifest_path": str(manifest_path.resolve()),
            "manifest_sha256": manifest_sha256,
            "catalog_index_path": str(catalog_index_path.resolve()),
            "catalog_index_sha256": catalog_index_sha256,
            "catalog_revision": catalog_revision,
        },
    }


def write_atomic_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode() + b"\n"
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as output:
            output.write(encoded)
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        temporary.unlink(missing_ok=True)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--catalog-index", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--expected-market-count", type=int, default=100)
    parser.add_argument("--expected-token-count", type=int, default=200)
    parser.add_argument("--expected-manifest-sha256")
    arguments = parser.parse_args()
    payload = build_scope(
        arguments.manifest,
        arguments.catalog_index,
        expected_market_count=arguments.expected_market_count,
        expected_token_count=arguments.expected_token_count,
        expected_manifest_sha256=arguments.expected_manifest_sha256,
    )
    write_atomic_json(arguments.output, payload)
    print(
        json.dumps(
            {
                "output": str(arguments.output.resolve()),
                "output_sha256": sha256_file(arguments.output),
                "scope_id": payload["scope_id"],
                "market_count": payload["market_count"],
                "token_count": payload["token_count"],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
