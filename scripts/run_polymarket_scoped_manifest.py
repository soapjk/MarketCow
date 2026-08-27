#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import time
from pathlib import Path
from typing import Any

from marketcow.polymarket_scopes import write_scope_runtime


MINIMUM_SAFE_REFRESH_SECONDS = 1.0
MAXIMUM_SAFE_REFRESH_SECONDS = 2.0
DEFAULT_MINIMUM_SCOPE_LIFETIME_SECONDS = 86_400


def validate_snapshot_refresh_seconds(value: float) -> float:
    if not MINIMUM_SAFE_REFRESH_SECONDS <= value <= MAXIMUM_SAFE_REFRESH_SECONDS:
        raise ValueError(
            "snapshot refresh must be between 1 and 2 seconds for an "
            "exact-100 scoped collector"
        )
    return value


def validate_scope_lifetime(
    manifest: dict[str, Any],
    candidate_snapshot: dict[str, Any],
    minimum_scope_lifetime_seconds: int,
    *,
    now_ns: int | None = None,
) -> dict[str, Any]:
    """Prove that every selected market outlives the requested run window."""
    if minimum_scope_lifetime_seconds <= 0:
        raise ValueError("minimum scope lifetime must be positive")
    if candidate_snapshot.get("catalog_revision") != manifest.get("catalog_revision"):
        raise ValueError("candidate snapshot catalog revision mismatch")
    if candidate_snapshot.get("snapshot_id") != manifest.get("candidate_snapshot_id"):
        raise ValueError("candidate snapshot ID mismatch")

    market_ids = {str(item) for item in manifest.get("market_ids") or []}
    candidate_by_id = {
        str(item.get("market_id")): item
        for item in candidate_snapshot.get("markets") or []
        if isinstance(item, dict) and item.get("market_id") is not None
    }
    missing = sorted(market_ids - candidate_by_id.keys())
    if missing:
        raise ValueError(
            "candidate snapshot does not cover scope markets: " + ",".join(missing)
        )

    observed_now_ns = time.time_ns() if now_ns is None else now_ns
    required_valid_until_ns = observed_now_ns + (
        minimum_scope_lifetime_seconds * 1_000_000_000
    )
    expiring = sorted(
        (
            market_id,
            candidate_by_id[market_id].get("end_at_ns"),
        )
        for market_id in market_ids
        if not isinstance(candidate_by_id[market_id].get("end_at_ns"), int)
        or candidate_by_id[market_id]["end_at_ns"] <= required_valid_until_ns
    )
    if expiring:
        details = ",".join(
            f"{market_id}:{end_at_ns}" for market_id, end_at_ns in expiring
        )
        raise ValueError(
            "scope does not satisfy minimum lifetime; expiring markets=" + details
        )

    earliest_end_at_ns = min(
        candidate_by_id[market_id]["end_at_ns"] for market_id in market_ids
    )
    return {
        "candidate_snapshot_id": candidate_snapshot["snapshot_id"],
        "earliest_end_at_ns": earliest_end_at_ns,
        "minimum_scope_lifetime_seconds": minimum_scope_lifetime_seconds,
        "required_valid_until_ns": required_valid_until_ns,
        "validated_at_ns": observed_now_ns,
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run the Polymarket collector from a checksum-bound scope manifest"
    )
    parser.add_argument("--root", required=True, type=Path)
    parser.add_argument("--manifest", required=True, type=Path)
    parser.add_argument("--manifest-sha256", required=True)
    parser.add_argument("--candidate-snapshot", required=True, type=Path)
    parser.add_argument("--candidate-sha256", required=True)
    parser.add_argument("--catalog-revision", required=True)
    parser.add_argument("--scope-id", required=True)
    parser.add_argument(
        "--minimum-scope-lifetime-seconds",
        type=int,
        default=DEFAULT_MINIMUM_SCOPE_LIFETIME_SECONDS,
    )
    parser.add_argument("--snapshot-refresh-seconds", type=float, default=2.0)
    parser.add_argument("--bootstrap-only", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    arguments = parser.parse_args()

    try:
        validate_snapshot_refresh_seconds(arguments.snapshot_refresh_seconds)
    except ValueError as exc:
        parser.error(f"--snapshot-refresh-seconds {exc}")

    manifest_body = arguments.manifest.resolve().read_bytes()
    observed_sha256 = hashlib.sha256(manifest_body).hexdigest()
    if observed_sha256 != arguments.manifest_sha256:
        parser.error("scope manifest checksum mismatch")
    manifest = json.loads(manifest_body)
    market_ids = [str(item) for item in manifest.get("market_ids") or []]
    if len(market_ids) != 100 or len(set(market_ids)) != 100:
        parser.error("scope manifest must contain exactly 100 unique markets")
    if manifest.get("catalog_revision") != arguments.catalog_revision:
        parser.error("scope manifest catalog revision mismatch")
    if manifest.get("scope_id") != arguments.scope_id:
        parser.error("scope manifest ID mismatch")

    candidate_body = arguments.candidate_snapshot.resolve().read_bytes()
    candidate_sha256 = hashlib.sha256(candidate_body).hexdigest()
    if candidate_sha256 != arguments.candidate_sha256:
        parser.error("candidate snapshot checksum mismatch")
    candidate_snapshot = json.loads(candidate_body)
    try:
        scope_lifetime = validate_scope_lifetime(
            manifest,
            candidate_snapshot,
            arguments.minimum_scope_lifetime_seconds,
        )
    except ValueError as exc:
        parser.error(str(exc))

    runner = Path(__file__).with_name("run_polymarket_live.py").resolve()
    command = [
        sys.executable,
        str(runner),
        "--root",
        str(arguments.root.resolve()),
    ]
    for market_id in market_ids:
        command.extend(("--market-id", market_id))
    command.extend((
        "--snapshot-refresh-seconds",
        str(arguments.snapshot_refresh_seconds),
    ))
    if arguments.bootstrap_only:
        command.append("--bootstrap-only")
    if arguments.dry_run:
        print(json.dumps({
            "catalog_revision": arguments.catalog_revision,
            "candidate_sha256": candidate_sha256,
            "command": command,
            "manifest_sha256": observed_sha256,
            "market_count": len(market_ids),
            "scope_id": arguments.scope_id,
            "scope_lifetime": scope_lifetime,
        }, sort_keys=True))
        return
    print(json.dumps({
        "event": "scope_lifetime_validated",
        "catalog_revision": arguments.catalog_revision,
        "candidate_sha256": candidate_sha256,
        "manifest_sha256": observed_sha256,
        "market_count": len(market_ids),
        "scope_id": arguments.scope_id,
        "scope_lifetime": scope_lifetime,
    }, sort_keys=True), flush=True)
    write_scope_runtime(
        arguments.root,
        scope_id=arguments.scope_id,
        manifest_sha256=observed_sha256,
    )
    os.execv(sys.executable, command)


if __name__ == "__main__":
    main()
