#!/usr/bin/env python3
"""Build one atomically publishable generation from a Tradude-owned exact selection."""

from __future__ import annotations

import argparse
import hashlib
import json
import tempfile
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

from scripts.migration.build_polymarket_rust_scope import build_scope, sha256_file, write_atomic_json


SCHEMA_VERSION = "marketcow.polymarket.rust-live-scope.v4"
UNIVERSE_SCHEMA_VERSION = "marketcow.polymarket.universe.v2"


def _reason(error: Exception) -> tuple[str, bool]:
    message = str(error)
    if "expires before" in message or "non-active" in message:
        return "market_expired", False
    if "exactly two" in message or "token" in message:
        return "token_missing", True
    if "fee" in message:
        return "fee_facts_unavailable", True
    if "catalog index did not resolve" in message or "identity mismatch" in message:
        return "market_not_found", True
    return "instrument_facts_invalid", True


def _book_frames(path: Path) -> dict[str, dict[str, Any]]:
    payload = json.loads(path.read_bytes())
    values = payload.get("books") if isinstance(payload, dict) else payload
    if isinstance(values, dict):
        frames = list(values.values())
    elif isinstance(values, list):
        frames = values
    else:
        raise ValueError("book snapshot must contain an object or array named books")
    result: dict[str, dict[str, Any]] = {}
    for frame in frames:
        if not isinstance(frame, dict):
            raise ValueError("book snapshot entries must be objects")
        token = frame.get("asset_id", frame.get("token_id"))
        if not isinstance(token, str) or not token.isascii() or not token.isdecimal():
            raise ValueError("book snapshot token identifiers must be decimal strings")
        if token in result:
            raise ValueError("book snapshot contains duplicate token identifiers")
        result[token] = frame
    return result


def _valid_two_sided_book(frame: dict[str, Any]) -> bool:
    if frame.get("event_type") != "book":
        return False
    bids, asks = frame.get("bids"), frame.get("asks")
    if not isinstance(bids, list) or not bids or not isinstance(asks, list) or not asks:
        return False
    reported_tick = frame.get("tick_size")
    if not isinstance(reported_tick, str):
        return False
    try:
        parsed = Decimal(reported_tick)
        return parsed.is_finite() and parsed > 0
    except InvalidOperation:
        return False


def _previous_identity_map(
    identities: list[dict[str, Any]] | None,
) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for identity in identities or []:
        if not isinstance(identity, dict) or set(identity) != {
            "market_id", "condition_id", "token_ids", "end_at"
        }:
            raise ValueError("previous market identity is invalid")
        market_id = identity["market_id"]
        token_ids = identity["token_ids"]
        if (
            not isinstance(market_id, str)
            or not market_id.isascii()
            or not market_id.isdecimal()
            or not isinstance(identity["condition_id"], str)
            or not identity["condition_id"]
            or not isinstance(token_ids, list)
            or len(token_ids) != 2
            or any(
                not isinstance(token, str) or not token.isascii() or not token.isdecimal()
                for token in token_ids
            )
            or len(set(token_ids)) != 2
            or not isinstance(identity["end_at"], str)
        ):
            raise ValueError("previous market identity is invalid")
        canonical_end = datetime.fromisoformat(identity["end_at"].replace("Z", "+00:00"))
        if canonical_end.tzinfo is None or market_id in result:
            raise ValueError("previous market identity is invalid")
        result[market_id] = {
            "market_id": market_id,
            "condition_id": identity["condition_id"],
            "token_ids": list(token_ids),
            "end_at": identity["end_at"],
        }
    return result


def build_dynamic_universe(
    candidate_manifest_path: Path,
    catalog_index_path: Path,
    catalog_path: Path,
    registry_path: Path,
    book_snapshot_path: Path,
    *,
    universe_id: str,
    generation: int,
    target_market_count: int,
    minimum_market_count: int,
    previous_market_ids: list[str] | None = None,
    previous_market_identities: list[dict[str, Any]] | None = None,
    validated_at: datetime | None = None,
    retry_seconds: int = 60,
) -> dict[str, Any]:
    if (
        len(universe_id) != 64
        or any(value not in "0123456789abcdefABCDEF" for value in universe_id)
        or generation <= 0
        or minimum_market_count <= 0
        or minimum_market_count != target_market_count
        or target_market_count > 250
        or retry_seconds <= 0
    ):
        raise ValueError("dynamic universe configuration is invalid")
    now = validated_at or datetime.now(timezone.utc)
    if now.tzinfo is None:
        raise ValueError("validated_at must be timezone-aware")
    manifest_bytes = candidate_manifest_path.read_bytes()
    manifest = json.loads(manifest_bytes)
    if (
        not isinstance(manifest, dict)
        or manifest.get("schema") != "tradude.prediction_market.scope_selection.v2"
    ):
        raise ValueError("candidate manifest must be a Tradude scope selection v2")
    candidates = manifest.get("market_ids")
    if not isinstance(candidates, list) or not candidates:
        raise ValueError("selection manifest must contain explicit market_ids")
    if any(not isinstance(value, str) or not value.isascii() or not value.isdecimal() for value in candidates):
        raise ValueError("candidate market identifiers must be decimal strings")
    if len(set(candidates)) != len(candidates):
        raise ValueError("candidate market identifiers must be unique")
    if len(candidates) != target_market_count:
        raise ValueError("explicit selection count must equal target market count")
    previous_identity_by_id = _previous_identity_map(previous_market_identities)
    previous_order = list(previous_market_ids or previous_identity_by_id)
    if (
        len(previous_order) != len(set(previous_order))
        or any(
            not isinstance(value, str) or not value.isascii() or not value.isdecimal()
            for value in previous_order
        )
        or (previous_identity_by_id and set(previous_order) != set(previous_identity_by_id))
    ):
        raise ValueError("previous market ids and identities disagree")
    books = _book_frames(book_snapshot_path)
    active: list[dict[str, Any]] = []
    active_books: list[dict[str, Any]] = []
    known_markets: dict[str, dict[str, Any]] = {}
    excluded: list[dict[str, Any]] = []
    retry_after = now + timedelta(seconds=retry_seconds)

    with tempfile.TemporaryDirectory(prefix="marketcow-universe-") as directory:
        root = Path(directory)
        for rank, market_id in enumerate(candidates):
            single = root / f"candidate-{rank}.json"
            single.write_text(json.dumps({
                "scope_id": universe_id,
                "market_ids": [market_id],
                "negative_risk_relation_ids": [],
            }, separators=(",", ":")))
            try:
                built = build_scope(
                    single,
                    catalog_index_path,
                    catalog_path,
                    registry_path,
                    expected_market_count=1,
                    expected_token_count=2,
                    validated_at=now,
                )
                market = built["catalog_frame"]["markets"][0]
                known_markets[market_id] = market
                token_ids = [outcome["token_id"] for outcome in market["outcomes"]]
                frames = [books.get(token) for token in token_ids]
                if any(frame is None for frame in frames):
                    reason, retryable = "book_missing", True
                elif not all(_valid_two_sided_book(frame) for frame in frames if frame):
                    reason, retryable = "one_sided_book", True
                elif len({Decimal(frame["tick_size"]) for frame in frames if frame}) != 1:
                    # The runtime can atomically reconcile a catalog tick to a newer
                    # authoritative book tick, but the two outcome tokens must agree.
                    reason, retryable = "instrument_facts_invalid", True
                else:
                    active.append(market)
                    active_books.extend(frame for frame in frames if frame)
                    continue
            except Exception as error:  # isolate one candidate; global checks remain below
                reason, retryable = _reason(error)
            excluded.append({
                "market_id": market_id,
                "reason_code": reason,
                "retryable": retryable,
                "retry_after": retry_after.isoformat().replace("+00:00", "Z") if retryable else None,
                "observed_at": now.isoformat().replace("+00:00", "Z"),
            })

    if len(active) != target_market_count:
        counts: dict[str, int] = {}
        for item in excluded:
            counts[item["reason_code"]] = counts.get(item["reason_code"], 0) + 1
        raise ValueError(
            "Tradude selection is not atomically ready: "
            f"{len(active)} != {target_market_count}; exclusions={counts}"
        )
    active_ids = [market["market_id"] for market in active]
    previous = set(previous_order)
    current = set(active_ids)
    removed = previous - current
    if not removed.issubset(set(known_markets) | set(previous_identity_by_id)):
        raise ValueError("removed market identity is unavailable; refusing to forget token identity")
    token_ids = sorted(
        outcome["token_id"] for market in active for outcome in market["outcomes"]
    )
    registry_bytes = registry_path.read_bytes()
    registry = json.loads(registry_bytes)
    active_by_id = {market["market_id"]: market for market in active}
    relations: list[dict[str, Any]] = []
    for relation in registry.get("negative_risk_groups", []):
        members = relation.get("members") or []
        member_ids = {str(member.get("market_id")) for member in members}
        if not relation.get("complete") or not member_ids or not member_ids.issubset(current):
            continue
        relation_id = relation.get("relation_id")
        if not isinstance(relation_id, str) or not relation_id:
            raise ValueError("eligible negative-risk relation lacks identity")
        yes_tokens: list[str] = []
        for member in members:
            market_id = str(member["market_id"])
            active_by_id[market_id]["negative_risk_group"] = relation_id
            outcome_id = member.get("yes_outcome_id")
            if not isinstance(outcome_id, str) or outcome_id.count(":") != 2:
                raise ValueError("eligible negative-risk relation has invalid yes outcome")
            yes_tokens.append(outcome_id.rsplit(":", 1)[-1])
        relations.append({
            "group_id": relation_id,
            "member_market_ids": sorted(member_ids),
            "yes_token_ids": sorted(yes_tokens),
            "revision": relation.get("relation_revision"),
            "complete": True,
            "valid_to": None,
        })
    catalog_revision = hashlib.sha256(json.dumps(
        {"generation": generation, "markets": active, "relations": relations},
        sort_keys=True, separators=(",", ":")
    ).encode()).hexdigest()
    return {
        "schema_version": SCHEMA_VERSION,
        "scope_id": universe_id.lower(),
        "market_count": len(active),
        "token_count": len(token_ids),
        "market_ids": active_ids,
        "token_ids": token_ids,
        "catalog_revision": catalog_revision,
        "catalog_frame": {
            "event_type": "catalog_revision",
            "catalog_revision": catalog_revision,
            "markets": active,
            "negative_risk_relations": relations,
        },
        "universe": {
            "schema_version": UNIVERSE_SCHEMA_VERSION,
            "universe_id": universe_id.lower(),
            "generation": generation,
            "target_market_count": target_market_count,
            "minimum_market_count": minimum_market_count,
            "filters": {
                "require_two_sided_books": True,
                "require_complete_instrument_facts": True,
            },
            "active_markets": [{
                "market_id": market["market_id"],
                "condition_id": market["condition_id"],
                "token_ids": [outcome["token_id"] for outcome in market["outcomes"]],
                "end_at": market["instrument_facts"]["end_at"],
            } for market in active],
            "added_markets": sorted(current - previous),
            "removed_markets": sorted(removed),
            "added_market_identities": [{
                "market_id": market["market_id"],
                "condition_id": market["condition_id"],
                "token_ids": [outcome["token_id"] for outcome in market["outcomes"]],
                "end_at": market["instrument_facts"]["end_at"],
            } for market in active if market["market_id"] in current - previous],
            "removed_market_identities": [
                previous_identity_by_id.get(market_id) or {
                    "market_id": known_markets[market_id]["market_id"],
                    "condition_id": known_markets[market_id]["condition_id"],
                    "token_ids": [
                        outcome["token_id"] for outcome in known_markets[market_id]["outcomes"]
                    ],
                    "end_at": known_markets[market_id]["instrument_facts"]["end_at"],
                }
                for market_id in sorted(removed)
            ],
            "excluded_markets": excluded,
            "validated_at": now.isoformat().replace("+00:00", "Z"),
        },
        "initial_book_frames": active_books,
        "source": {
            "manifest_sha256": hashlib.sha256(manifest_bytes).hexdigest(),
            "catalog_index_sha256": sha256_file(catalog_index_path),
            "catalog_sha256": sha256_file(catalog_path),
            "registry_sha256": hashlib.sha256(registry_bytes).hexdigest(),
            "book_snapshot_sha256": sha256_file(book_snapshot_path),
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--candidates", type=Path, required=True)
    parser.add_argument("--catalog-index", type=Path, required=True)
    parser.add_argument("--catalog", type=Path, required=True)
    parser.add_argument("--registry", type=Path, required=True)
    parser.add_argument("--books", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--universe-id", required=True)
    parser.add_argument("--generation", type=int, required=True)
    parser.add_argument("--target-market-count", type=int, required=True)
    parser.add_argument("--minimum-market-count", type=int, required=True)
    parser.add_argument("--previous-market-id", action="append", default=[])
    parser.add_argument("--previous-market-identities", type=Path)
    arguments = parser.parse_args()
    result = build_dynamic_universe(
        arguments.candidates, arguments.catalog_index, arguments.catalog, arguments.registry,
        arguments.books, universe_id=arguments.universe_id, generation=arguments.generation,
        target_market_count=arguments.target_market_count,
        minimum_market_count=arguments.minimum_market_count,
        previous_market_ids=arguments.previous_market_id or None,
        previous_market_identities=(
            json.loads(arguments.previous_market_identities.read_bytes())
            if arguments.previous_market_identities
            else None
        ),
    )
    write_atomic_json(arguments.output, result)
    print(json.dumps({"output": str(arguments.output.resolve()), "sha256": sha256_file(arguments.output),
                      "universe_id": result["universe"]["universe_id"],
                      "generation": result["universe"]["generation"]}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
