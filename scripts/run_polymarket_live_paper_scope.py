#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
from pathlib import Path


def load_market_ids(path: Path) -> list[str]:
    document = path.read_text(encoding="utf-8")
    if not re.search(r"(?m)^schema:\s*tradude\.prediction_market\.live_paper\.v1\s*$", document):
        raise ValueError("scope config schema is not live-paper v1")
    block = re.search(
        r"(?m)^  market_ids:\s*\n(?P<items>(?:^    - .+\n)+)", document,
    )
    if block is None:
        raise ValueError("scope config lacks explicit market_ids")
    market_ids = [
        line.split("-", 1)[1].strip().strip("'\"")
        for line in block.group("items").splitlines()
    ]
    if len(market_ids) != 100 or len(set(market_ids)) != 100:
        raise ValueError("scope config must contain exactly 100 unique markets")
    if any(not market_id.isdecimal() for market_id in market_ids):
        raise ValueError("scope market IDs must be decimal strings")
    return market_ids


def load_scope_manifest_market_ids(path: Path) -> list[str]:
    document = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(document, dict):
        raise ValueError("scope manifest must be a JSON object")
    if document.get("schema") != "tradude.prediction_market.scope_manifest.v1":
        raise ValueError("scope manifest schema is not scope_manifest v1")
    scope_id = document.get("scope_id")
    if not isinstance(scope_id, str) or len(scope_id) != 64:
        raise ValueError("scope manifest lacks a valid scope_id")
    canonical = dict(document)
    canonical.pop("scope_id", None)
    calculated_scope_id = hashlib.sha256(
        json.dumps(
            canonical, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
        ).encode("utf-8"),
    ).hexdigest()
    if calculated_scope_id != scope_id:
        raise ValueError("scope manifest scope_id does not match its contents")
    market_ids = document.get("market_ids")
    if not isinstance(market_ids, list):
        raise ValueError("scope manifest lacks explicit market_ids")
    if len(market_ids) != 100 or len(set(market_ids)) != 100:
        raise ValueError("scope manifest must contain exactly 100 unique markets")
    if any(not isinstance(value, str) or not value.isdecimal() for value in market_ids):
        raise ValueError("scope market IDs must be decimal strings")
    return market_ids


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run the bounded Polymarket collector from a live-paper scope",
    )
    parser.add_argument("--root", required=True, type=Path)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--config", type=Path)
    source.add_argument("--scope-manifest", type=Path)
    parser.add_argument("--snapshot-refresh-seconds", type=float, default=2.0)
    arguments = parser.parse_args()
    if not 1 <= arguments.snapshot_refresh_seconds <= 2:
        parser.error("--snapshot-refresh-seconds must be between 1 and 2")
    try:
        market_ids = (
            load_market_ids(arguments.config.resolve(strict=True))
            if arguments.config is not None
            else load_scope_manifest_market_ids(
                arguments.scope_manifest.resolve(strict=True),
            )
        )
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        parser.error(str(exc))
    runner = Path(__file__).with_name("run_polymarket_live.py").resolve()
    command = [
        sys.executable, str(runner), "--root", str(arguments.root.resolve()),
        "--snapshot-refresh-seconds", str(arguments.snapshot_refresh_seconds),
    ]
    for market_id in market_ids:
        command.extend(("--market-id", market_id))
    os.execv(sys.executable, command)


if __name__ == "__main__":
    main()
