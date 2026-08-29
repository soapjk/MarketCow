#!/usr/bin/env python3
"""Build a hash-pinned Rust Polymarket live scope from an exact market manifest."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sqlite3
import tempfile
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any


SCHEMA_VERSION = "marketcow.polymarket.rust-live-scope.v3"


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


def _decimal_string(value: Any, name: str, *, positive: bool = False) -> str:
    encoded = _required_string(value, name)
    try:
        parsed = Decimal(encoded)
    except InvalidOperation as error:
        raise ValueError(f"{name} must be an exact decimal string") from error
    if not parsed.is_finite() or parsed < 0 or (positive and parsed == 0):
        qualifier = "positive" if positive else "non-negative"
        raise ValueError(f"{name} must be a {qualifier} exact decimal string")
    return encoded


def _timestamp(value: Any, name: str) -> tuple[str, datetime]:
    encoded = _required_string(value, name)
    try:
        parsed = datetime.fromisoformat(encoded.replace("Z", "+00:00"))
    except ValueError as error:
        raise ValueError(f"{name} must be an ISO-8601 timestamp") from error
    if parsed.tzinfo is None:
        raise ValueError(f"{name} must include a timezone")
    return encoded, parsed


def _fee_schedule(value: Any, market_id: str) -> dict[str, Any]:
    if not isinstance(value, dict) or value.get("complete") is not True:
        raise ValueError(f"{market_id}.fee_schedule must be explicitly complete")
    if value.get("missing_fields") not in (None, []):
        raise ValueError(f"{market_id}.fee_schedule contains missing fields")
    schedule_id = _required_string(value.get("schedule_id"), f"{market_id}.fee.schedule_id")
    if len(schedule_id) != 64 or any(character not in "0123456789abcdefABCDEF" for character in schedule_id):
        raise ValueError(f"{market_id}.fee.schedule_id must be a SHA-256 hex digest")
    effective_from, effective_from_at = _timestamp(
        value.get("effective_from"), f"{market_id}.fee.effective_from"
    )
    effective_to_value = value.get("effective_to")
    effective_to = None
    if effective_to_value is not None:
        effective_to, effective_to_at = _timestamp(
            effective_to_value, f"{market_id}.fee.effective_to"
        )
        if effective_to_at <= effective_from_at:
            raise ValueError(f"{market_id}.fee effective interval is invalid")
    calculation_status = _required_string(
        value.get("calculation_status"), f"{market_id}.fee.calculation_status"
    )
    if calculation_status not in {"executable", "informational_only"}:
        raise ValueError(f"{market_id}.fee.calculation_status is unsupported")
    rounding_mode = _required_string(
        value.get("rounding_mode"), f"{market_id}.fee.rounding_mode"
    )
    tie_semantics = _required_string(
        value.get("tie_semantics"), f"{market_id}.fee.tie_semantics"
    )
    if calculation_status == "executable" and (
        rounding_mode.upper() == "UNSPECIFIED" or tie_semantics.lower() == "unspecified"
    ):
        raise ValueError(f"{market_id}.fee executable schedule lacks deterministic rounding")
    provenance_value = value.get("provenance")
    if not isinstance(provenance_value, list) or not provenance_value:
        raise ValueError(f"{market_id}.fee.provenance must be non-empty")
    provenance: list[dict[str, Any]] = []
    observed_values: list[datetime] = []
    for index, item in enumerate(provenance_value):
        if not isinstance(item, dict):
            raise ValueError(f"{market_id}.fee.provenance[{index}] must be an object")
        payload_sha256 = _required_string(
            item.get("payload_sha256"), f"{market_id}.fee.provenance[{index}].payload_sha256"
        )
        if len(payload_sha256) != 64 or any(
            character not in "0123456789abcdefABCDEF" for character in payload_sha256
        ):
            raise ValueError(
                f"{market_id}.fee.provenance[{index}].payload_sha256 must be a SHA-256 hex digest"
            )
        observed_at, observed = _timestamp(
            item.get("observed_at"), f"{market_id}.fee.provenance[{index}].observed_at"
        )
        observed_values.append(observed)
        field_paths = item.get("field_paths")
        if not isinstance(field_paths, list) or not field_paths:
            raise ValueError(f"{market_id}.fee.provenance[{index}].field_paths must be non-empty")
        provenance.append(
            {
                "source": _required_string(
                    item.get("source"), f"{market_id}.fee.provenance[{index}].source"
                ),
                "source_url": _required_string(
                    item.get("source_url"), f"{market_id}.fee.provenance[{index}].source_url"
                ),
                "revision": _required_string(
                    item.get("revision"), f"{market_id}.fee.provenance[{index}].revision"
                ),
                "payload_sha256": payload_sha256.lower(),
                "observed_at": observed_at,
                "field_paths": [
                    _required_string(path, f"{market_id}.fee.provenance[{index}].field_paths[]")
                    for path in field_paths
                ],
            }
        )
    schedule = {
        "schedule_id": schedule_id.lower(),
        "schedule_version": _required_string(
            value.get("schedule_version"), f"{market_id}.fee.schedule_version"
        ),
        "currency": _required_string(value.get("currency"), f"{market_id}.fee.currency"),
        "maker_rate": _decimal_string(value.get("maker_rate"), f"{market_id}.fee.maker_rate"),
        "taker_rate": _decimal_string(value.get("taker_rate"), f"{market_id}.fee.taker_rate"),
        "formula_id": "polymarket_probability_fee.v1",
        "formula": _required_string(value.get("formula"), f"{market_id}.fee.formula"),
        "exponent": _decimal_string(value.get("exponent"), f"{market_id}.fee.exponent"),
        "quantum": _decimal_string(
            value.get("quantum"), f"{market_id}.fee.quantum", positive=True
        ),
        "rounding_mode": rounding_mode,
        "tie_semantics": tie_semantics,
        "calculation_status": calculation_status,
        "effective_from": effective_from,
        "effective_to": effective_to,
        "observed_at": max(observed_values).isoformat().replace("+00:00", "Z"),
        "provenance": provenance,
    }
    revision_material = json.dumps(schedule, sort_keys=True, separators=(",", ":")).encode()
    schedule["revision"] = hashlib.sha256(revision_material).hexdigest()
    return schedule


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
    catalog_path: Path,
    registry_path: Path,
    *,
    expected_market_count: int,
    expected_token_count: int,
    expected_manifest_sha256: str | None = None,
    validated_at: datetime | None = None,
    minimum_scope_lifetime_seconds: int = 0,
) -> dict[str, Any]:
    if minimum_scope_lifetime_seconds < 0:
        raise ValueError("minimum scope lifetime must not be negative")
    observed_at = validated_at or datetime.now(timezone.utc)
    if observed_at.tzinfo is None:
        raise ValueError("validated_at must be timezone-aware")
    required_valid_until = observed_at + timedelta(seconds=minimum_scope_lifetime_seconds)
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
        catalog_rows = connection.execute(
            f"SELECT market_id, byte_offset, byte_length FROM markets "
            f"WHERE market_id IN ({placeholders}) ORDER BY market_id",
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
    catalog_sha256 = sha256_file(catalog_path)
    market_records: list[dict[str, Any]] = []
    with catalog_path.open("rb") as catalog:
        for market_id, byte_offset, byte_length in catalog_rows:
            catalog.seek(byte_offset)
            raw = catalog.read(byte_length)
            row = json.loads(raw)
            identity = row.get("identity") or {}
            rules = row.get("rules") or {}
            instrument = rules.get("instrument") or {}
            outcomes = identity.get("outcomes")
            if identity.get("market_id") != market_id or not isinstance(outcomes, list):
                raise ValueError(f"catalog identity mismatch for market {market_id}")
            if len(outcomes) != 2:
                raise ValueError(f"catalog market {market_id} must have two outcomes")
            required_facts = {
                key: _required_string(instrument.get(key), f"{market_id}.instrument.{key}")
                for key in (
                    "price_increment",
                    "size_increment",
                    "minimum_order_size",
                    "settlement_currency",
                    "revision",
                )
            }
            fee_schedule = _fee_schedule(rules.get("fee_schedule"), market_id)
            _, fee_effective_from = _timestamp(
                fee_schedule["effective_from"], f"{market_id}.fee.effective_from"
            )
            fee_effective_to = None
            if fee_schedule["effective_to"] is not None:
                _, fee_effective_to = _timestamp(
                    fee_schedule["effective_to"], f"{market_id}.fee.effective_to"
                )
            if fee_effective_from > observed_at or (
                fee_effective_to is not None and fee_effective_to <= observed_at
            ):
                raise ValueError(f"{market_id}.fee schedule is not effective at validation time")
            start_at = _required_string(row.get("start_at"), f"{market_id}.start_at")
            end_at = _required_string(row.get("end_at"), f"{market_id}.end_at")
            try:
                end_datetime = datetime.fromisoformat(end_at.replace("Z", "+00:00"))
            except ValueError as error:
                raise ValueError(f"{market_id}.end_at must be an ISO-8601 timestamp") from error
            if end_datetime.tzinfo is None or end_datetime <= required_valid_until:
                raise ValueError(
                    f"exact live scope market {market_id} expires before required validity boundary"
                )
            lifecycle = _required_string(row.get("lifecycle_state"), f"{market_id}.lifecycle_state")
            if lifecycle != "active":
                raise ValueError(f"exact live scope contains non-active market {market_id}")
            market_records.append(
                {
                    "market_id": market_id,
                    "condition_id": _required_string(
                        identity.get("condition_id"), f"{market_id}.condition_id"
                    ),
                    "outcomes": [
                        {
                            "token_id": _required_string(value.get("token_id"), "token_id"),
                            "outcome": _required_string(value.get("outcome"), "outcome"),
                            "instrument_id": _required_string(
                                value.get("instrument_id"), "instrument_id"
                            ),
                        }
                        for value in outcomes
                    ],
                    "negative_risk_group": None,
                    "lifecycle_state": lifecycle,
                    "resolution": row.get("resolution"),
                    "metadata_revision": _required_string(
                        row.get("metadata_revision"), f"{market_id}.metadata_revision"
                    ),
                    "observed_at": _required_string(
                        row.get("observed_at"), f"{market_id}.observed_at"
                    ),
                    "terminal_at": None,
                    "instrument_facts": {
                        **required_facts,
                        "start_at": start_at,
                        "end_at": end_at,
                        "fee_schedule": fee_schedule,
                    },
                }
            )
    if len(market_records) != len(market_ids):
        raise ValueError("catalog index did not resolve every exact-scope market")

    registry_bytes = registry_path.read_bytes()
    registry = json.loads(registry_bytes)
    relation_ids = manifest.get("negative_risk_relation_ids") or []
    relation_by_id = {
        relation.get("relation_id"): relation
        for relation in registry.get("negative_risk_groups", [])
    }
    market_record_by_id = {record["market_id"]: record for record in market_records}
    relations: list[dict[str, Any]] = []
    for relation_id in relation_ids:
        relation = relation_by_id.get(relation_id)
        if not relation or not relation.get("complete"):
            raise ValueError(f"missing or incomplete negative-risk relation {relation_id}")
        members = relation.get("members") or []
        member_ids = {str(member.get("market_id")) for member in members}
        if not member_ids or not member_ids.issubset(market_record_by_id):
            raise ValueError(f"negative-risk relation {relation_id} crosses scope boundary")
        yes_tokens: set[str] = set()
        for member in members:
            market_id = str(member["market_id"])
            market_record_by_id[market_id]["negative_risk_group"] = relation_id
            instrument_id = _required_string(member.get("yes_outcome_id"), "yes_outcome_id")
            yes_tokens.add(instrument_id.rsplit(":", 1)[-1])
        relations.append(
            {
                "group_id": relation_id,
                "member_market_ids": sorted(member_ids),
                "yes_token_ids": sorted(yes_tokens),
                "revision": _required_string(
                    relation.get("relation_revision"), "relation_revision"
                ),
                "complete": True,
                "valid_to": None,
            }
        )

    return {
        "schema_version": SCHEMA_VERSION,
        "scope_id": scope_id,
        "market_count": len(market_ids),
        "token_count": len(token_ids),
        "market_ids": sorted(market_ids),
        "token_ids": token_ids,
        "catalog_revision": catalog_revision,
        "catalog_frame": {
            "event_type": "catalog_revision",
            "catalog_revision": catalog_revision,
            "markets": market_records,
            "negative_risk_relations": relations,
        },
        "source": {
            "manifest_path": str(manifest_path.resolve()),
            "manifest_sha256": manifest_sha256,
            "catalog_index_path": str(catalog_index_path.resolve()),
            "catalog_index_sha256": catalog_index_sha256,
            "catalog_path": str(catalog_path.resolve()),
            "catalog_sha256": catalog_sha256,
            "registry_path": str(registry_path.resolve()),
            "registry_sha256": hashlib.sha256(registry_bytes).hexdigest(),
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
    parser.add_argument("--catalog", type=Path, required=True)
    parser.add_argument("--registry", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--expected-market-count", type=int, default=100)
    parser.add_argument("--expected-token-count", type=int, default=200)
    parser.add_argument("--expected-manifest-sha256")
    parser.add_argument("--minimum-scope-lifetime-seconds", type=int, default=0)
    arguments = parser.parse_args()
    payload = build_scope(
        arguments.manifest,
        arguments.catalog_index,
        arguments.catalog,
        arguments.registry,
        expected_market_count=arguments.expected_market_count,
        expected_token_count=arguments.expected_token_count,
        expected_manifest_sha256=arguments.expected_manifest_sha256,
        minimum_scope_lifetime_seconds=arguments.minimum_scope_lifetime_seconds,
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
