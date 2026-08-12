#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from pathlib import Path


MINIMUM_SAFE_REFRESH_SECONDS = 1.0
MAXIMUM_SAFE_REFRESH_SECONDS = 2.0


def validate_snapshot_refresh_seconds(value: float) -> float:
    if not MINIMUM_SAFE_REFRESH_SECONDS <= value <= MAXIMUM_SAFE_REFRESH_SECONDS:
        raise ValueError(
            "snapshot refresh must be between 1 and 2 seconds for an "
            "exact-100 scoped collector"
        )
    return value


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run the Polymarket collector from a checksum-bound scope manifest"
    )
    parser.add_argument("--root", required=True, type=Path)
    parser.add_argument("--manifest", required=True, type=Path)
    parser.add_argument("--manifest-sha256", required=True)
    parser.add_argument("--catalog-revision", required=True)
    parser.add_argument("--scope-id", required=True)
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
            "command": command,
            "manifest_sha256": observed_sha256,
            "market_count": len(market_ids),
            "scope_id": arguments.scope_id,
        }, sort_keys=True))
        return
    os.execv(sys.executable, command)


if __name__ == "__main__":
    main()
