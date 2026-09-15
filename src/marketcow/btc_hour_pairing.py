"""Build one immutable BTC-hour rule/bar/finality evidence bundle.

The bundle proves source identity and outcome agreement.  It deliberately does
not claim that standard CTF collateral is redeemable as an application pUSD
balance; that adapter/account mapping needs its own receipt.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from datetime import timedelta
from decimal import Decimal
from pathlib import Path

from .btc_hourly_dataset import canonical, timestamp
from .btc_polymarket_binding import bind_hour, review_binance_hour
from .universe_live_probe import strict_json


def _read(path: Path, maximum: int) -> bytes:
    with path.open("rb") as stream:
        raw = stream.read(maximum + 1)
    if len(raw) > maximum:
        raise ValueError("pairing_input_capacity")
    return raw


def _sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _facts(path: Path, maximum_bytes: int, maximum_rows: int) -> tuple[bytes, list[dict]]:
    raw = _read(path, maximum_bytes)
    lines = raw.splitlines()
    if len(lines) > maximum_rows:
        raise ValueError("pairing_fact_count_capacity")
    return raw, [strict_json(line) for line in lines]


def _verify_part(root: Path, manifest: dict, name: str) -> Path:
    matches = [row for row in manifest.get("parts", []) if row.get("file") == name]
    if len(matches) != 1:
        raise ValueError("dataset_part_identity")
    path = root / name
    raw = _read(path, int(matches[0]["bytes"]))
    if len(raw) != matches[0]["bytes"] or _sha(raw) != matches[0]["sha256"]:
        raise ValueError("dataset_part_integrity")
    return path


def _aggregate(rows: list[dict]) -> dict:
    first, last = rows[0]["payload"], rows[-1]["payload"]
    return {
        "open": first["open"],
        "high": str(max(Decimal(row["payload"]["high"]) for row in rows)),
        "low": str(min(Decimal(row["payload"]["low"]) for row in rows)),
        "close": last["close"],
        "volume": str(sum(Decimal(row["payload"]["volume"]) for row in rows)),
        "quote_volume": str(sum(Decimal(row["payload"]["quote_volume"]) for row in rows)),
        "taker_base_volume": str(sum(Decimal(row["payload"]["taker_base_volume"]) for row in rows)),
        "taker_quote_volume": str(sum(Decimal(row["payload"]["taker_quote_volume"]) for row in rows)),
        "trades": sum(row["payload"]["trades"] for row in rows),
    }


def build_pairing(evidence_root: Path, archive_root: Path, output: Path, *,
                  start_utc: str, maximum_input_bytes: int = 16 * 1024 * 1024) -> dict:
    if not all(path.is_absolute() for path in (evidence_root, archive_root, output)):
        raise ValueError("absolute_paths_required")
    if output.exists():
        raise FileExistsError(output)
    start = timestamp(start_utc)
    if start.minute or start.second or start.microsecond:
        raise ValueError("hour_boundary_required")
    end = start + timedelta(hours=1)

    evidence_raw = _read(evidence_root / "evidence.json", maximum_input_bytes)
    evidence = strict_json(evidence_raw)
    review = review_binance_hour(evidence, start_utc)
    binding = bind_hour(evidence, review)
    finality_raw = _read(evidence_root / "verified-finality.json", maximum_input_bytes)
    finality = strict_json(finality_raw)
    if (finality.get("schema_version") != "marketcow.polymarket.ctf-finality-quorum.v1"
            or finality.get("status") != "verified_final"
            or finality.get("settlement_import_allowed") is not True
            or finality.get("condition_id") != binding["condition_id"]
            or finality.get("token_ids") != [binding["up_token"], binding["down_token"]]
            or len(finality.get("payout_numerators", [])) != 2
            or int(finality.get("payout_denominator", "0")) <= 0):
        raise ValueError("finality_binding")

    selected: dict[str, tuple[bytes, dict, Path]] = {}
    for interval, expected in (("1m", 60), ("1h", 1)):
        root = archive_root / interval
        manifest_raw = _read(root / "manifest.json", maximum_input_bytes)
        manifest = strict_json(manifest_raw)
        if (manifest.get("status") != "complete" or manifest.get("coverage_complete") is not True
                or manifest.get("configuration", {}).get("interval") != interval):
            raise ValueError("binance_dataset_incomplete")
        facts_path = _verify_part(root, manifest, "facts.jsonl")
        _, rows = _facts(facts_path, maximum_input_bytes, 1440)
        window = [row for row in rows if row.get("interval") == interval
                  and int(row.get("payload", {}).get("open_us", -1)) >= int(start.timestamp() * 1_000_000)
                  and int(row["payload"].get("close_us", -1)) < int(end.timestamp() * 1_000_000)]
        if len(window) != expected or any(row.get("type") != "bar_final" for row in window):
            raise ValueError("binance_hour_coverage")
        selected[interval] = (b"\n".join(canonical(row) for row in window) + b"\n", manifest, manifest_raw)

    minutes = [strict_json(line) for line in selected["1m"][0].splitlines()]
    hour = strict_json(selected["1h"][0].strip())
    aggregate = _aggregate(minutes)
    for key, value in aggregate.items():
        observed = hour["payload"][key]
        if key == "trades":
            equal = observed == value
        else:
            equal = Decimal(observed) == Decimal(value)
        if not equal:
            raise ValueError("minute_hour_mismatch")
    close_gte_open = Decimal(hour["payload"]["close"]) >= Decimal(hour["payload"]["open"])
    expected_payouts = ["1", "0"] if close_gte_open else ["0", "1"]
    numerators = finality["payout_numerators"]
    denominator = Decimal(finality["payout_denominator"])
    normalized = [str(Decimal(value) / denominator) for value in numerators]
    if normalized != expected_payouts:
        raise ValueError("binance_finality_disagreement")

    output.mkdir(parents=True)
    parts: list[dict] = []
    values = {
        "market-evidence.json": evidence_raw,
        "verified-finality.json": finality_raw,
        "binance-1m-hour.jsonl": selected["1m"][0],
        "binance-1h-hour.jsonl": selected["1h"][0],
    }
    observations = {_sha(_read(path, maximum_input_bytes)): path
                    for path in sorted(evidence_root.glob("finality-observation*.json"))[:3]}
    for receipt in finality["provider_receipts"]:
        source = observations.get(receipt.get("observation_sha256"))
        if source is None:
            raise ValueError("finality_observation_original_missing")
        provider = receipt.get("provider_id")
        if not isinstance(provider, str) or not provider.isascii() or not provider.replace("-", "").isalnum():
            raise ValueError("finality_provider_identity")
        values[f"finality-observation-{provider}.json"] = _read(source, maximum_input_bytes)
    for name, raw in values.items():
        path = output / name
        with path.open("xb") as stream:
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
        parts.append({"file": name, "bytes": len(raw), "sha256": _sha(raw)})

    payload = {
        "schema_version": "marketcow.btc-hour.paired-dataset.v1",
        "market": {"market_id": binding["market_id"], "condition_id": binding["condition_id"],
                   "start_utc": binding["start_utc"], "end_utc": binding["end_utc"],
                   "source_instrument": binding["source_instrument"],
                   "outcomes": [{"outcome": "Up", "token_id": binding["up_token"], "payout": normalized[0]},
                                {"outcome": "Down", "token_id": binding["down_token"], "payout": normalized[1]}]},
        "binance_result": {"comparison": "final_close_gte_open", "result": close_gte_open,
                           "hour": hour["payload"], "minute_count": 60,
                           "minute_aggregation_matches_hour": True},
        "settlement": {"status": "verified_final", "policy": finality["finality_policy"],
                       "collateral": finality["collateral"], "ctf_address": finality["ctf_address"],
                       "provider_receipts": finality["provider_receipts"],
                       "adapter_redemption_verified": False,
                       "application_pusd_mapping_verified": False},
        "sources": {"market_evidence_sha256": _sha(evidence_raw),
                    "finality_sha256": _sha(finality_raw),
                    "binance_1m_dataset_id": selected["1m"][1]["dataset_id"],
                    "binance_1h_dataset_id": selected["1h"][1]["dataset_id"],
                    "binance_archive_root": str(archive_root)},
        "parts": sorted(parts, key=lambda row: row["file"]),
        "coverage": {"rule": True, "binance_final_1m": True, "binance_final_1h": True,
                     "standard_ctf_finality": True, "historical_first_received": False,
                     "historical_l2": False, "adapter_redemption": False, "application_pusd_mapping": False},
        "limitations": ["historical_first_received_unknown", "historical_l2_absent",
                        "adapter_redemption_outside_receipt", "application_pusd_mapping_unverified"],
    }
    payload_raw = canonical(payload)
    manifest = {**payload, "dataset_id": _sha(payload_raw), "manifest_payload_sha256": _sha(payload_raw)}
    manifest_raw = canonical(manifest)
    with (output / "manifest.json").open("xb") as stream:
        stream.write(manifest_raw)
        stream.flush()
        os.fsync(stream.fileno())
    directory_fd = os.open(output, os.O_RDONLY)
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)
    return manifest


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--evidence-root", type=Path, required=True)
    parser.add_argument("--archive-root", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--start-utc", required=True)
    args = parser.parse_args()
    result = build_pairing(args.evidence_root, args.archive_root, args.output, start_utc=args.start_utc)
    print(json.dumps({"dataset_id": result["dataset_id"], "market_id": result["market"]["market_id"],
                      "result": result["binance_result"]["result"]}, sort_keys=True))


if __name__ == "__main__":
    main()
