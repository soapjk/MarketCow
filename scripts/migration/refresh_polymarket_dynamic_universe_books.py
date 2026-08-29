#!/usr/bin/env python3
"""Refresh one unpublished dynamic-universe candidate with current complete CLOB books."""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Protocol

import requests

from scripts.migration.build_polymarket_rust_scope import write_atomic_json


DEFAULT_ENDPOINT = "https://clob.polymarket.com/books"
MAXIMUM_RESPONSE_BYTES = 64 * 1024 * 1024


class Poster(Protocol):
    def post(self, url: str, *, json: Any, timeout: float) -> Any: ...


def _positive_decimal(value: Any, name: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{name} must be an exact decimal string")
    try:
        parsed = Decimal(value)
    except InvalidOperation as error:
        raise ValueError(f"{name} must be an exact decimal string") from error
    if not parsed.is_finite() or parsed <= 0:
        raise ValueError(f"{name} must be a positive exact decimal string")
    return value


def _validate_book(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError("CLOB book must be an object")
    token_id = value.get("asset_id")
    if not isinstance(token_id, str) or not token_id.isascii() or not token_id.isdecimal():
        raise ValueError("CLOB book asset_id must be a decimal string")
    if not isinstance(value.get("timestamp"), str) or not value["timestamp"].isdecimal():
        raise ValueError(f"book {token_id} timestamp must be an epoch-millisecond string")
    _positive_decimal(value.get("tick_size"), f"book {token_id} tick_size")
    for side in ("bids", "asks"):
        levels = value.get(side)
        if not isinstance(levels, list) or not levels:
            raise ValueError(f"book {token_id} must have a non-empty {side} side")
        for index, level in enumerate(levels):
            if not isinstance(level, dict):
                raise ValueError(f"book {token_id} {side}[{index}] must be an object")
            _positive_decimal(level.get("price"), f"book {token_id} {side}[{index}].price")
            _positive_decimal(level.get("size"), f"book {token_id} {side}[{index}].size")
    return {**value, "event_type": "book"}


def fetch_books(
    token_ids: list[str],
    *,
    endpoint: str = DEFAULT_ENDPOINT,
    timeout_seconds: float = 60,
    batch_size: int = 250,
    poster: Poster = requests,
) -> list[dict[str, Any]]:
    if (
        not token_ids
        or len(set(token_ids)) != len(token_ids)
        or any(not token.isascii() or not token.isdecimal() for token in token_ids)
        or timeout_seconds <= 0
        or batch_size <= 0
        or batch_size > 500
        or not endpoint.startswith("https://")
    ):
        raise ValueError("book refresh configuration is invalid")
    books: dict[str, dict[str, Any]] = {}
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
            book = _validate_book(raw_book)
            token_id = book["asset_id"]
            if token_id in books:
                raise ValueError(f"duplicate CLOB book for token {token_id}")
            books[token_id] = book
    missing = sorted(set(token_ids) - books.keys())
    unexpected = sorted(books.keys() - set(token_ids))
    if missing or unexpected:
        raise ValueError(f"CLOB book identity mismatch: missing={missing}, unexpected={unexpected}")
    return [books[token_id] for token_id in token_ids]


def refresh_candidate(
    candidate: dict[str, Any],
    *,
    endpoint: str = DEFAULT_ENDPOINT,
    timeout_seconds: float = 60,
    batch_size: int = 250,
    poster: Poster = requests,
    observed_at: datetime | None = None,
) -> dict[str, Any]:
    if candidate.get("schema_version") != "marketcow.polymarket.rust-live-scope.v4":
        raise ValueError("candidate must use the dynamic universe v4 scope schema")
    token_ids = candidate.get("token_ids")
    markets = candidate.get("catalog_frame", {}).get("markets")
    if not isinstance(token_ids, list) or not isinstance(markets, list):
        raise ValueError("candidate token and market facts are required")
    books = fetch_books(
        token_ids,
        endpoint=endpoint,
        timeout_seconds=timeout_seconds,
        batch_size=batch_size,
        poster=poster,
    )
    by_token = {book["asset_id"]: book for book in books}
    for market in markets:
        outcome_ids = [outcome.get("token_id") for outcome in market.get("outcomes", [])]
        if len(outcome_ids) != 2 or any(token_id not in by_token for token_id in outcome_ids):
            raise ValueError(f"market {market.get('market_id')} lacks two complete outcome books")
        if len({by_token[token_id]["tick_size"] for token_id in outcome_ids}) != 1:
            raise ValueError(f"market {market.get('market_id')} outcome book ticks disagree")
    now = observed_at or datetime.now(timezone.utc)
    if now.tzinfo is None:
        raise ValueError("observed_at must be timezone-aware")
    snapshot_bytes = json.dumps(
        {"books": books}, sort_keys=True, separators=(",", ":")
    ).encode() + b"\n"
    refreshed = json.loads(json.dumps(candidate))
    refreshed["initial_book_frames"] = books
    refreshed["source"]["book_snapshot_sha256"] = hashlib.sha256(snapshot_bytes).hexdigest()
    refreshed["source"]["book_snapshot_observed_at"] = now.isoformat().replace("+00:00", "Z")
    refreshed["universe"]["validated_at"] = now.isoformat().replace("+00:00", "Z")
    return refreshed


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--endpoint", default=DEFAULT_ENDPOINT)
    parser.add_argument("--timeout-seconds", type=float, default=60)
    parser.add_argument("--batch-size", type=int, default=250)
    arguments = parser.parse_args()
    candidate = json.loads(arguments.candidate.read_bytes())
    refreshed = refresh_candidate(
        candidate,
        endpoint=arguments.endpoint,
        timeout_seconds=arguments.timeout_seconds,
        batch_size=arguments.batch_size,
    )
    write_atomic_json(arguments.output, refreshed)
    encoded = arguments.output.read_bytes()
    print(json.dumps({
        "output": str(arguments.output.resolve()),
        "sha256": hashlib.sha256(encoded).hexdigest(),
        "generation": refreshed["universe"]["generation"],
        "market_count": refreshed["market_count"],
        "token_count": refreshed["token_count"],
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
