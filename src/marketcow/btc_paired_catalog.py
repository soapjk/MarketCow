"""Publish a bounded catalog of independently verified BTC-hour datasets."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path

from .btc_hourly_dataset import canonical, timestamp
from .universe_live_probe import strict_json


def _read(path: Path, maximum: int) -> bytes:
    with path.open("rb") as stream:
        raw = stream.read(maximum + 1)
    if len(raw) > maximum:
        raise ValueError("paired_catalog_input_capacity")
    return raw


def _digest(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _verify_manifest(path: Path, maximum_manifest_bytes: int,
                     maximum_part_bytes: int) -> dict:
    if not path.is_absolute() or path.name != "manifest.json":
        raise ValueError("absolute_manifest_required")
    raw = _read(path, maximum_manifest_bytes)
    value = strict_json(raw)
    if value.get("schema_version") != "marketcow.btc-hour.paired-dataset.v1":
        raise ValueError("paired_manifest_schema")
    payload = {key: item for key, item in value.items()
               if key not in {"dataset_id", "manifest_payload_sha256"}}
    identity = _digest(canonical(payload))
    if value.get("dataset_id") != identity or value.get("manifest_payload_sha256") != identity:
        raise ValueError("paired_manifest_identity")

    market = value.get("market")
    outcomes = market.get("outcomes") if isinstance(market, dict) else None
    if (not isinstance(outcomes, list) or len(outcomes) != 2
            or [row.get("outcome") for row in outcomes] != ["Up", "Down"]
            or len({row.get("token_id") for row in outcomes}) != 2
            or value.get("settlement", {}).get("status") != "verified_final"):
        raise ValueError("paired_market_binding")
    start, end = timestamp(market["start_utc"]), timestamp(market["end_utc"])
    if (end - start).total_seconds() != 3600:
        raise ValueError("paired_hour_window")
    expected = ["1", "0"] if value.get("binance_result", {}).get("result") is True else ["0", "1"]
    if [row.get("payout") for row in outcomes] != expected:
        raise ValueError("paired_result_disagreement")

    parts = value.get("parts")
    if not isinstance(parts, list) or not parts:
        raise ValueError("paired_parts_missing")
    part_names, total = set(), 0
    for part in parts:
        if (not isinstance(part, dict) or set(part) != {"file", "bytes", "sha256"}
                or not isinstance(part["file"], str) or Path(part["file"]).name != part["file"]
                or part["file"] in part_names or type(part["bytes"]) is not int
                or not 0 <= part["bytes"] <= maximum_part_bytes):
            raise ValueError("paired_part_schema")
        part_names.add(part["file"])
        part_path = path.parent / part["file"]
        if part_path.stat().st_size != part["bytes"]:
            raise ValueError("paired_part_integrity")
        part_raw = _read(part_path, part["bytes"])
        if len(part_raw) != part["bytes"] or _digest(part_raw) != part["sha256"]:
            raise ValueError("paired_part_integrity")
        total += len(part_raw)
    evidence = strict_json(_read(path.parent / "market-evidence.json", maximum_part_bytes))
    if (evidence.get("market_id") != market.get("market_id")
            or evidence.get("condition_id") != market.get("condition_id")
            or _digest(_read(path.parent / "market-evidence.json", maximum_part_bytes))
            != value.get("sources", {}).get("market_evidence_sha256")):
        raise ValueError("paired_evidence_binding")
    observed = evidence.get("observed_at")
    observed_at = timestamp(observed)
    return {
        "dataset_id": identity,
        "manifest_path": str(path),
        "manifest_sha256": _digest(raw),
        "market_id": market["market_id"],
        "condition_id": market["condition_id"],
        "start_utc": market["start_utc"],
        "end_utc": market["end_utc"],
        "rule_observed_at": observed,
        "rule_observed_before_window": observed_at <= start,
        "result": "Up" if value["binance_result"]["result"] else "Down",
        "token_ids": [row["token_id"] for row in outcomes],
        "part_count": len(parts),
        "part_bytes": total,
        "limitations": value.get("limitations", []),
    }


def build_catalog(manifests: list[Path], output: Path, *, maximum_datasets: int = 256,
                  maximum_manifest_bytes: int = 1 << 20,
                  maximum_part_bytes: int = 16 << 20) -> dict:
    if (not output.is_absolute() or output.exists() or not manifests
            or len(manifests) > maximum_datasets):
        raise ValueError("paired_catalog_arguments")
    if len(set(manifests)) != len(manifests):
        raise ValueError("paired_catalog_duplicate_manifest")
    rows = [_verify_manifest(path, maximum_manifest_bytes, maximum_part_bytes)
            for path in manifests]
    rows.sort(key=lambda row: (row["start_utc"], row["market_id"]))
    for key in ("dataset_id", "market_id", "condition_id", "start_utc"):
        if len({row[key] for row in rows}) != len(rows):
            raise ValueError(f"paired_catalog_duplicate_{key}")
    payload = {
        "schema_version": "marketcow.btc-hour.paired-catalog.v1",
        "selection_basis": "explicit_verified_dataset_manifests_not_exhaustive_history",
        "complete_for_listed_inputs": True,
        "historical_market_coverage_complete": False,
        "dataset_count": len(rows),
        "window_start_utc": rows[0]["start_utc"],
        "window_end_utc": rows[-1]["end_utc"],
        "rule_observed_before_window_count": sum(row["rule_observed_before_window"] for row in rows),
        "datasets": rows,
    }
    payload_raw = canonical(payload)
    result = {**payload, "catalog_id": _digest(payload_raw)}
    raw = canonical(result)
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("xb") as stream:
        stream.write(raw)
        stream.flush()
        os.fsync(stream.fileno())
    descriptor = os.open(output.parent, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", action="append", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--maximum-datasets", type=int, default=256)
    args = parser.parse_args()
    result = build_catalog(args.manifest, args.output, maximum_datasets=args.maximum_datasets)
    print(json.dumps({"catalog_id": result["catalog_id"],
                      "dataset_count": result["dataset_count"]}, sort_keys=True))


if __name__ == "__main__":
    main()
