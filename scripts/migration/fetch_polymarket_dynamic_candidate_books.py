#!/usr/bin/env python3
"""Fetch an auditable, isolation-tolerant CLOB book snapshot for ranked candidates."""

from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Protocol

import requests

from scripts.migration.build_polymarket_rust_scope import write_atomic_json


SCHEMA_VERSION = "marketcow.polymarket.candidate-books.v1"
DEFAULT_ENDPOINT = "https://clob.polymarket.com/books"
MAXIMUM_RESPONSE_BYTES = 64 * 1024 * 1024


class Poster(Protocol):
    def post(self, url: str, *, json: Any, timeout: float) -> Any: ...


def _decimal(value: Any, name: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{name} must be an exact decimal string")
    try:
        parsed = Decimal(value)
    except InvalidOperation as error:
        raise ValueError(f"{name} must be an exact decimal string") from error
    if not parsed.is_finite() or parsed <= 0:
        raise ValueError(f"{name} must be a positive exact decimal string")
    return value


def _validate_partial_book(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError("CLOB book must be an object")
    token_id = value.get("asset_id")
    if not isinstance(token_id, str) or not token_id.isascii() or not token_id.isdecimal():
        raise ValueError("CLOB book asset_id must be a decimal string")
    timestamp = value.get("timestamp")
    if not isinstance(timestamp, str) or not timestamp.isdecimal():
        raise ValueError(f"book {token_id} timestamp must be an epoch-millisecond string")
    _decimal(value.get("tick_size"), f"book {token_id} tick_size")
    for side in ("bids", "asks"):
        levels = value.get(side)
        if not isinstance(levels, list):
            raise ValueError(f"book {token_id} {side} must be an array")
        for index, level in enumerate(levels):
            if not isinstance(level, dict):
                raise ValueError(f"book {token_id} {side}[{index}] must be an object")
            _decimal(level.get("price"), f"book {token_id} {side}[{index}].price")
            _decimal(level.get("size"), f"book {token_id} {side}[{index}].size")
    return {**value, "event_type": "book"}


def resolve_candidate_tokens(
    candidate_manifest_path: Path, catalog_index_path: Path
) -> tuple[list[str], list[str], str]:
    manifest_bytes = candidate_manifest_path.read_bytes()
    manifest = json.loads(manifest_bytes)
    market_ids = manifest.get("market_ids") if isinstance(manifest, dict) else None
    if (
        not isinstance(market_ids, list)
        or not market_ids
        or len(set(market_ids)) != len(market_ids)
        or any(not isinstance(value, str) or not value.isascii() or not value.isdecimal()
               for value in market_ids)
    ):
        raise ValueError("candidate manifest must contain unique decimal market_ids")
    uri = f"{catalog_index_path.resolve(strict=True).as_uri()}?mode=ro&immutable=1"
    with sqlite3.connect(uri, uri=True) as connection:
        placeholders = ",".join("?" for _ in market_ids)
        rows = connection.execute(
            f"SELECT market_id, token_id FROM tokens WHERE market_id IN ({placeholders}) "
            "ORDER BY market_id, token_id",
            market_ids,
        ).fetchall()
    resolved_markets = {str(row[0]) for row in rows}
    unresolved_markets = sorted(set(market_ids) - resolved_markets)
    token_ids = sorted({str(row[1]) for row in rows})
    if any(not token.isascii() or not token.isdecimal() for token in token_ids):
        raise ValueError("catalog index returned a non-decimal token identifier")
    return token_ids, unresolved_markets, hashlib.sha256(manifest_bytes).hexdigest()


def fetch_candidate_books(
    token_ids: list[str],
    *,
    endpoint: str = DEFAULT_ENDPOINT,
    timeout_seconds: float = 60,
    batch_size: int = 250,
    poster: Poster = requests,
) -> tuple[list[dict[str, Any]], list[str]]:
    if (
        not token_ids
        or len(set(token_ids)) != len(token_ids)
        or any(not token.isascii() or not token.isdecimal() for token in token_ids)
        or timeout_seconds <= 0
        or batch_size <= 0
        or batch_size > 500
        or not endpoint.startswith("https://")
    ):
        raise ValueError("candidate book fetch configuration is invalid")
    books: dict[str, dict[str, Any]] = {}
    requested = set(token_ids)
    for offset in range(0, len(token_ids), batch_size):
        batch = token_ids[offset : offset + batch_size]
        response = poster.post(
            endpoint,
            json=[{"token_id": token_id} for token_id in batch],
            timeout=timeout_seconds,
        )
        response.raise_for_status()
        if len(response.content) > MAXIMUM_RESPONSE_BYTES:
            raise ValueError("CLOB book response exceeds the bounded payload size")
        payload = response.json(parse_float=str, parse_int=str)
        if not isinstance(payload, list):
            raise ValueError("CLOB book response must be an array")
        for raw_book in payload:
            book = _validate_partial_book(raw_book)
            token_id = book["asset_id"]
            if token_id not in requested:
                raise ValueError(f"unexpected CLOB book token {token_id}")
            if token_id in books:
                raise ValueError(f"duplicate CLOB book for token {token_id}")
            books[token_id] = book
    return [books[token] for token in sorted(books)], sorted(requested - books.keys())


def build_candidate_book_snapshot(
    candidate_manifest_path: Path,
    catalog_index_path: Path,
    *,
    endpoint: str = DEFAULT_ENDPOINT,
    timeout_seconds: float = 60,
    batch_size: int = 250,
    poster: Poster = requests,
    observed_at: datetime | None = None,
) -> dict[str, Any]:
    token_ids, unresolved_markets, manifest_sha256 = resolve_candidate_tokens(
        candidate_manifest_path, catalog_index_path
    )
    books, missing_tokens = fetch_candidate_books(
        token_ids,
        endpoint=endpoint,
        timeout_seconds=timeout_seconds,
        batch_size=batch_size,
        poster=poster,
    )
    now = observed_at or datetime.now(timezone.utc)
    if now.tzinfo is None:
        raise ValueError("observed_at must be timezone-aware")
    return {
        "schema_version": SCHEMA_VERSION,
        "observed_at": now.isoformat().replace("+00:00", "Z"),
        "endpoint": endpoint,
        "candidate_manifest_sha256": manifest_sha256,
        "catalog_index_path": str(catalog_index_path.resolve(strict=True)),
        "requested_token_count": len(token_ids),
        "book_count": len(books),
        "missing_token_ids": missing_tokens,
        "unresolved_market_ids": unresolved_markets,
        "books": books,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--candidates", type=Path, required=True)
    parser.add_argument("--catalog-index", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--endpoint", default=DEFAULT_ENDPOINT)
    parser.add_argument("--timeout-seconds", type=float, default=60)
    parser.add_argument("--batch-size", type=int, default=250)
    arguments = parser.parse_args()
    result = build_candidate_book_snapshot(
        arguments.candidates,
        arguments.catalog_index,
        endpoint=arguments.endpoint,
        timeout_seconds=arguments.timeout_seconds,
        batch_size=arguments.batch_size,
    )
    write_atomic_json(arguments.output, result)
    encoded = arguments.output.read_bytes()
    print(json.dumps({
        "output": str(arguments.output.resolve()),
        "sha256": hashlib.sha256(encoded).hexdigest(),
        "requested_token_count": result["requested_token_count"],
        "book_count": result["book_count"],
        "missing_token_count": len(result["missing_token_ids"]),
        "unresolved_market_count": len(result["unresolved_market_ids"]),
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
