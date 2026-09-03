#!/usr/bin/env python3
"""Replace expired Polymarket scope members without weakening exact-scope readiness."""

from __future__ import annotations

import argparse
import hashlib
import json
import time
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

try:
    from scripts.migration.build_polymarket_rust_scope import (
        build_scope,
        sha256_file,
        write_atomic_json,
    )
except ModuleNotFoundError:  # Direct execution places this script's directory on sys.path.
    from build_polymarket_rust_scope import build_scope, sha256_file, write_atomic_json


def scope_content_id(manifest: dict[str, Any]) -> str:
    content = dict(manifest)
    content.pop("scope_id", None)
    encoded = json.dumps(
        content,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _market_ids(relation: dict[str, Any]) -> set[str]:
    return {
        str(member.get("market_id"))
        for member in relation.get("members") or []
        if member.get("market_id") is not None
    }


def build_refreshed_manifest(
    current: dict[str, Any],
    candidates: dict[str, Any],
    registry: dict[str, Any],
    *,
    generated_at_ns: int,
    minimum_scope_lifetime_seconds: int,
    expected_market_count: int,
) -> tuple[dict[str, Any], dict[str, Any]]:
    if minimum_scope_lifetime_seconds <= 0:
        raise ValueError("minimum scope lifetime must be positive")
    if current.get("schema") != "tradude.prediction_market.scope_manifest.v1":
        raise ValueError("current scope manifest schema is invalid")
    if current.get("scope_id") != scope_content_id(current):
        raise ValueError("current scope_id does not match canonical manifest contents")
    if candidates.get("schema") != "tradude.prediction_market.scope_candidates.v1":
        raise ValueError("candidate snapshot schema is invalid")
    catalog_revision = current.get("catalog_revision")
    if candidates.get("catalog_revision") != catalog_revision:
        raise ValueError("candidate/catalog revision mismatch")
    if registry.get("catalog_revision") != catalog_revision:
        raise ValueError("relation registry/catalog revision mismatch")
    current_ids = [str(value) for value in current.get("market_ids") or []]
    if len(current_ids) != expected_market_count or len(set(current_ids)) != len(current_ids):
        raise ValueError("current scope market count is not exact")

    required_valid_until_ns = generated_at_ns + minimum_scope_lifetime_seconds * 1_000_000_000
    eligible: dict[str, dict[str, Any]] = {}
    for row in candidates.get("markets") or []:
        if not isinstance(row, dict):
            continue
        market_id = row.get("market_id")
        end_at_ns = row.get("end_at_ns")
        if not all(
            (
                isinstance(market_id, str) and market_id.isdecimal(),
                row.get("active") is True,
                row.get("accepting_orders") is True,
                row.get("closed") is False,
                row.get("binary_yes_no") is True,
                row.get("rules_complete") is True,
                isinstance(end_at_ns, int) and end_at_ns > required_valid_until_ns,
                isinstance(row.get("yes_outcome_id"), str),
                isinstance(row.get("no_outcome_id"), str),
                row.get("yes_outcome_id") != row.get("no_outcome_id"),
            )
        ):
            continue
        try:
            liquidity = Decimal(str(row.get("liquidity") or row.get("liquidity_num") or "0"))
        except InvalidOperation:
            continue
        normalized = dict(row)
        normalized["liquidity_num"] = str(liquidity)
        eligible[market_id] = normalized

    relations = {
        str(relation.get("relation_id")): relation
        for relation in registry.get("negative_risk_groups") or []
        if isinstance(relation, dict) and relation.get("relation_id")
    }
    all_relation_members = set().union(*(_market_ids(value) for value in relations.values()))
    surviving_relation_ids: list[str] = []
    surviving_relation_members: set[str] = set()
    dropped_relation_ids: list[str] = []
    for relation_id in current.get("negative_risk_relation_ids") or []:
        relation = relations.get(str(relation_id))
        members = _market_ids(relation or {})
        if (
            relation
            and relation.get("complete") is True
            and members
            and members.issubset(current_ids)
            and members.issubset(eligible)
        ):
            surviving_relation_ids.append(str(relation_id))
            surviving_relation_members.update(members)
        else:
            dropped_relation_ids.append(str(relation_id))

    retained = [
        market_id
        for market_id in current_ids
        if market_id in eligible
        and (market_id not in all_relation_members or market_id in surviving_relation_members)
    ]
    replacements = sorted(
        (
            row
            for market_id, row in eligible.items()
            if market_id not in retained and market_id not in all_relation_members
        ),
        key=lambda row: (
            -Decimal(row["liquidity_num"]),
            -int(row["end_at_ns"]),
            str(row["market_id"]),
        ),
    )
    selected = retained + [
        str(row["market_id"])
        for row in replacements[: expected_market_count - len(retained)]
    ]
    if len(selected) != expected_market_count or len(set(selected)) != len(selected):
        raise ValueError("eligible candidate pool cannot refill the exact scope")

    manifest = {
        "schema": "tradude.prediction_market.scope_manifest.v1",
        "catalog_revision": catalog_revision,
        "candidate_snapshot_id": candidates.get("snapshot_id"),
        "generated_at_ns": generated_at_ns,
        "market_ids": selected,
        "market_limit": expected_market_count,
        "negative_risk_relation_ids": surviving_relation_ids,
        "logical_relation_ids": [],
        "registry_id": registry.get("registry_id"),
        "selection_config_id": current.get("selection_config_id"),
        "replaces_scope_id": current.get("scope_id"),
    }
    manifest["scope_id"] = scope_content_id(manifest)
    evidence = {
        "schema_version": "marketcow.polymarket.scope-refresh.v1",
        "previous_scope_id": current.get("scope_id"),
        "scope_id": manifest["scope_id"],
        "generated_at_ns": generated_at_ns,
        "required_valid_until_ns": required_valid_until_ns,
        "minimum_scope_lifetime_seconds": minimum_scope_lifetime_seconds,
        "retained_market_count": len(retained),
        "replacement_market_count": expected_market_count - len(retained),
        "removed_market_ids": sorted(set(current_ids) - set(selected)),
        "replacement_market_ids": selected[len(retained) :],
        "surviving_negative_risk_relation_ids": surviving_relation_ids,
        "dropped_negative_risk_relation_ids": dropped_relation_ids,
    }
    return manifest, evidence


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--current-manifest", type=Path, required=True)
    parser.add_argument("--candidate-snapshot", type=Path, required=True)
    parser.add_argument("--catalog-index", type=Path, required=True)
    parser.add_argument("--catalog", type=Path, required=True)
    parser.add_argument("--registry", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--expected-market-count", type=int, default=100)
    parser.add_argument("--minimum-scope-lifetime-seconds", type=int, default=86_400)
    parser.add_argument("--generated-at-ns", type=int)
    arguments = parser.parse_args()
    generated_at_ns = arguments.generated_at_ns or time.time_ns()
    current = json.loads(arguments.current_manifest.read_text(encoding="utf-8"))
    candidates = json.loads(arguments.candidate_snapshot.read_text(encoding="utf-8"))
    registry = json.loads(arguments.registry.read_text(encoding="utf-8"))
    manifest, evidence = build_refreshed_manifest(
        current,
        candidates,
        registry,
        generated_at_ns=generated_at_ns,
        minimum_scope_lifetime_seconds=arguments.minimum_scope_lifetime_seconds,
        expected_market_count=arguments.expected_market_count,
    )
    scope_root = arguments.output_root.resolve() / "scopes" / manifest["scope_id"]
    manifest_path = scope_root / "manifest.json"
    write_atomic_json(manifest_path, manifest)
    rust_scope = build_scope(
        manifest_path,
        arguments.catalog_index,
        arguments.catalog,
        arguments.registry,
        expected_market_count=arguments.expected_market_count,
        expected_token_count=arguments.expected_market_count * 2,
        minimum_scope_lifetime_seconds=arguments.minimum_scope_lifetime_seconds,
    )
    registry_path = arguments.output_root.resolve() / "registry" / f"{manifest['scope_id']}.json"
    write_atomic_json(registry_path, rust_scope)
    evidence.update(
        {
            "manifest_path": str(manifest_path),
            "manifest_sha256": sha256_file(manifest_path),
            "rust_scope_path": str(registry_path),
            "rust_scope_sha256": sha256_file(registry_path),
            "market_count": rust_scope["market_count"],
            "token_count": rust_scope["token_count"],
        }
    )
    report_path = scope_root / "refresh-report.json"
    write_atomic_json(report_path, evidence)
    print(json.dumps({**evidence, "report_path": str(report_path)}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
