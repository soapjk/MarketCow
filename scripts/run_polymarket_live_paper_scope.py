#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
import sys
import time
from pathlib import Path
from typing import Any


REQUIRED_TRADUDE_COMMIT = "1d509944134cf3e181782ff2acbe2d907ccdde1b"
MAXIMUM_CAPITAL_LOCK_DURATION_NS = 30 * 86_400 * 1_000_000_000
MINIMUM_RUNTIME_LIFETIME_SECONDS = 3_600


def load_market_ids(path: Path) -> list[str]:
    document = path.read_text(encoding="utf-8")
    if not re.search(r"(?m)^schema:\s*tradude\.prediction_market\.live_paper\.v1\s*$", document):
        raise ValueError("scope config schema is not live-paper v1")
    block = re.search(
        r"(?m)^  market_ids:\s*\n(?P<items>(?:^    - .+\n)+)",
        document,
    )
    if block is None:
        raise ValueError("scope config lacks explicit market_ids")
    market_ids = [line.split("-", 1)[1].strip().strip("'\"") for line in block.group("items").splitlines()]
    if len(market_ids) != 100 or len(set(market_ids)) != 100:
        raise ValueError("scope config must contain exactly 100 unique markets")
    if any(not market_id.isdecimal() for market_id in market_ids):
        raise ValueError("scope market IDs must be decimal strings")
    return market_ids


def _read_json_object(path: Path, label: str) -> dict[str, Any]:
    document = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(document, dict):
        raise ValueError(f"{label} must be a JSON object")
    return document


def load_scope_manifest(path: Path) -> dict[str, Any]:
    document = _read_json_object(path, "scope manifest")
    if document.get("schema") != "tradude.prediction_market.scope_manifest.v1":
        raise ValueError("scope manifest schema is not scope_manifest v1")
    scope_id = document.get("scope_id")
    if not isinstance(scope_id, str) or len(scope_id) != 64:
        raise ValueError("scope manifest lacks a valid scope_id")
    canonical = dict(document)
    canonical.pop("scope_id", None)
    calculated_scope_id = hashlib.sha256(
        json.dumps(
            canonical,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
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
    return document


def load_scope_manifest_market_ids(path: Path) -> list[str]:
    return list(load_scope_manifest(path)["market_ids"])


def validate_scope_selection(
    manifest_path: Path,
    selection_report_path: Path,
    candidate_snapshot_path: Path,
    *,
    now_ns: int | None = None,
) -> dict[str, Any]:
    manifest = load_scope_manifest(manifest_path)
    report = _read_json_object(selection_report_path, "selection report")
    candidate = _read_json_object(candidate_snapshot_path, "candidate snapshot")
    if report.get("schema") != "tradude.prediction_market.clob_complete_scope.v2":
        raise ValueError("selection report schema is not clob_complete_scope v2")
    selection = report.get("selection")
    if not isinstance(selection, dict) or selection.get("manifest") != manifest:
        raise ValueError("selection report is not exactly bound to the scope manifest")
    if candidate.get("schema") != "tradude.prediction_market.scope_candidates.v1":
        raise ValueError("candidate snapshot schema is not scope_candidates v1")
    if candidate.get("snapshot_id") != manifest.get("candidate_snapshot_id"):
        raise ValueError("candidate snapshot ID differs from scope manifest")
    if candidate.get("catalog_revision") != manifest.get("catalog_revision"):
        raise ValueError("candidate catalog revision differs from scope manifest")

    candidate_markets = candidate.get("markets")
    if not isinstance(candidate_markets, list):
        raise ValueError("candidate snapshot lacks markets")
    by_market_id = {
        item.get("market_id"): item
        for item in candidate_markets
        if isinstance(item, dict) and isinstance(item.get("market_id"), str)
    }
    market_ids = list(manifest["market_ids"])
    missing_markets = sorted(set(market_ids) - set(by_market_id))
    if missing_markets:
        raise ValueError("candidate snapshot lacks selected markets")
    generated_at_ns = manifest.get("generated_at_ns")
    if isinstance(generated_at_ns, bool) or not isinstance(generated_at_ns, int):
        raise ValueError("scope manifest generated_at_ns must be an integer")
    observed_now_ns = time.time_ns() if now_ns is None else now_ns
    required_valid_until_ns = observed_now_ns + MINIMUM_RUNTIME_LIFETIME_SECONDS * 1_000_000_000
    durations: dict[str, int] = {}
    token_ids: set[str] = set()
    for market_id in market_ids:
        market = by_market_id[market_id]
        if any(
            market.get(field) is not expected
            for field, expected in (
                ("accepting_orders", True),
                ("active", True),
                ("binary_yes_no", True),
                ("closed", False),
                ("rules_complete", True),
            )
        ):
            raise ValueError(f"selected market {market_id} is not runtime eligible")
        end_at_ns = market.get("end_at_ns")
        if isinstance(end_at_ns, bool) or not isinstance(end_at_ns, int):
            raise ValueError(f"selected market {market_id} lacks integer end_at_ns")
        duration = end_at_ns - generated_at_ns
        if not 0 < duration <= MAXIMUM_CAPITAL_LOCK_DURATION_NS:
            raise ValueError(f"selected market {market_id} exceeds 30-day capital lock")
        if end_at_ns <= required_valid_until_ns:
            raise ValueError(f"selected market {market_id} cannot cover one-hour runtime")
        durations[market_id] = duration
        for field in ("yes_outcome_id", "no_outcome_id"):
            outcome_id = market.get(field)
            if not isinstance(outcome_id, str) or outcome_id.count(":") != 2:
                raise ValueError(f"selected market {market_id} has invalid outcome IDs")
            token_ids.add(outcome_id.rsplit(":", 1)[1])
    if len(token_ids) != 200:
        raise ValueError("selected scope does not contain exactly 200 unique tokens")

    attempts = report.get("attempts")
    if not isinstance(attempts, list) or not attempts:
        raise ValueError("selection report lacks CLOB coverage attempts")
    final_attempt = attempts[-1]
    if not isinstance(final_attempt, dict) or final_attempt.get("complete") is not True:
        raise ValueError("selection report final CLOB coverage is incomplete")
    for field in ("requested_token_ids", "received_token_ids", "two_sided_token_ids"):
        values = final_attempt.get(field)
        if not isinstance(values, list) or set(values) != token_ids or len(values) != 200:
            raise ValueError(f"selection report final {field} differs from exact scope")
    if final_attempt.get("missing_token_ids") != []:
        raise ValueError("selection report final coverage has missing tokens")
    if final_attempt.get("non_two_sided_token_ids") != []:
        raise ValueError("selection report final coverage has incomplete books")
    return {
        "scope_id": manifest["scope_id"],
        "market_ids": market_ids,
        "token_count": len(token_ids),
        "maximum_capital_lock_duration_ns": max(durations.values()),
        "required_valid_until_ns": required_valid_until_ns,
        "final_clob_checked_at_ns": final_attempt.get("checked_at_ns"),
    }


def verify_tradude_commit(worktree: Path) -> str:
    head = subprocess.check_output(
        ["git", "-C", str(worktree), "rev-parse", "HEAD"],
        text=True,
    ).strip()
    completed = subprocess.run(
        [
            "git",
            "-C",
            str(worktree),
            "merge-base",
            "--is-ancestor",
            REQUIRED_TRADUDE_COMMIT,
            head,
        ],
        check=False,
    )
    if completed.returncode != 0:
        raise ValueError(f"Tradude runtime does not contain required commit {REQUIRED_TRADUDE_COMMIT}")
    return head


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run the bounded Polymarket collector from a live-paper scope",
    )
    parser.add_argument("--root", required=True, type=Path)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--config", type=Path)
    source.add_argument("--scope-manifest", type=Path)
    parser.add_argument("--selection-report", type=Path)
    parser.add_argument("--candidate-snapshot", type=Path)
    parser.add_argument("--tradude-worktree", type=Path)
    parser.add_argument("--snapshot-refresh-seconds", type=float, default=2.0)
    arguments = parser.parse_args()
    if not 1 <= arguments.snapshot_refresh_seconds <= 2:
        parser.error("--snapshot-refresh-seconds must be between 1 and 2")
    try:
        if arguments.config is not None:
            if any(
                value is not None
                for value in (
                    arguments.selection_report,
                    arguments.candidate_snapshot,
                    arguments.tradude_worktree,
                )
            ):
                raise ValueError("selection provenance is only valid with --scope-manifest")
            market_ids = load_market_ids(arguments.config.resolve(strict=True))
        else:
            if any(
                value is None
                for value in (
                    arguments.selection_report,
                    arguments.candidate_snapshot,
                    arguments.tradude_worktree,
                )
            ):
                raise ValueError(
                    "--scope-manifest requires --selection-report, --candidate-snapshot and --tradude-worktree"
                )
            manifest_path = arguments.scope_manifest.resolve(strict=True)
            report_path = arguments.selection_report.resolve(strict=True)
            candidate_path = arguments.candidate_snapshot.resolve(strict=True)
            tradude_worktree = arguments.tradude_worktree.resolve(strict=True)
            provenance = validate_scope_selection(
                manifest_path,
                report_path,
                candidate_path,
            )
            provenance.update(
                {
                    "event": "scope_selection_validated",
                    "manifest_path": str(manifest_path),
                    "manifest_sha256": hashlib.sha256(manifest_path.read_bytes()).hexdigest(),
                    "selection_report_path": str(report_path),
                    "selection_report_sha256": hashlib.sha256(report_path.read_bytes()).hexdigest(),
                    "candidate_snapshot_path": str(candidate_path),
                    "candidate_snapshot_sha256": hashlib.sha256(candidate_path.read_bytes()).hexdigest(),
                    "tradude_head": verify_tradude_commit(tradude_worktree),
                    "required_tradude_commit": REQUIRED_TRADUDE_COMMIT,
                }
            )
            print(json.dumps(provenance, sort_keys=True), flush=True)
            market_ids = provenance["market_ids"]
    except (OSError, subprocess.SubprocessError, ValueError, json.JSONDecodeError) as exc:
        parser.error(str(exc))
    runner = Path(__file__).with_name("run_polymarket_live.py").resolve()
    command = [
        sys.executable,
        str(runner),
        "--root",
        str(arguments.root.resolve()),
        "--snapshot-refresh-seconds",
        str(arguments.snapshot_refresh_seconds),
    ]
    for market_id in market_ids:
        command.extend(("--market-id", market_id))
    os.execv(sys.executable, command)


if __name__ == "__main__":
    main()
