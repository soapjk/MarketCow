"""Extract exact Binance BTC-hour identities from one frozen Gamma catalog."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
from datetime import timedelta
from pathlib import Path

from .btc_hourly_dataset import canonical, timestamp
from .btc_polymarket_binding import BINANCE_HOUR_RULE
from .universe_live_probe import strict_json


def _capture_relation(observed, start, end):
    if observed <= start:
        return "before_window"
    if observed < end:
        return "during_window"
    return "after_window"


def build_rule_catalog(source: Path, output: Path, *, source_sha256: str,
                       source_observed_at: str, maximum_source_bytes: int,
                       maximum_line_bytes: int = 2 << 20,
                       maximum_records: int = 4096) -> dict:
    if (not source.is_absolute() or not output.is_absolute() or output.exists()
            or not re.fullmatch(r"[0-9a-f]{64}", source_sha256)
            or min(maximum_source_bytes, maximum_line_bytes, maximum_records) <= 0):
        raise ValueError("rule_catalog_arguments")
    observed = timestamp(source_observed_at)
    digest, offset, rows = hashlib.sha256(), 0, []
    with source.open("rb") as stream:
        while True:
            line = stream.readline(maximum_line_bytes + 1)
            if not line:
                break
            if len(line) > maximum_line_bytes or offset + len(line) > maximum_source_bytes:
                raise ValueError("rule_catalog_source_capacity")
            digest.update(line)
            if b'Bitcoin Up or Down -' in line and b'Binance' in line:
                value = strict_json(line)
                start, end = timestamp(value.get("eventStartTime")), timestamp(value.get("endDate"))
                if (value.get("description") == BINANCE_HOUR_RULE
                        and value.get("resolutionSource") == "https://www.binance.com/en/trade/BTC_USDT"
                        and end - start == timedelta(hours=1)
                        and not (start.minute or start.second or start.microsecond)):
                    outcomes = strict_json(value.get("outcomes", "").encode())
                    tokens = strict_json(value.get("clobTokenIds", "").encode())
                    condition, market = value.get("conditionId"), value.get("id")
                    if (outcomes != ["Up", "Down"] or not isinstance(tokens, list) or len(tokens) != 2
                            or len(set(tokens)) != 2 or not all(isinstance(token, str) and token.isdecimal()
                                                                 for token in tokens)
                            or not isinstance(market, str) or not market.isdecimal()
                            or not isinstance(condition, str)
                            or not re.fullmatch(r"0x[0-9a-fA-F]{64}", condition)):
                        raise ValueError("rule_catalog_identity")
                    if len(rows) >= maximum_records:
                        raise ValueError("rule_catalog_record_capacity")
                    rows.append({
                        "market_id": market, "condition_id": condition,
                        "outcomes": [{"outcome": outcome, "token_id": token}
                                     for outcome, token in zip(outcomes, tokens, strict=True)],
                        "question": value.get("question"),
                        "start_utc": start.isoformat().replace("+00:00", "Z"),
                        "end_utc": end.isoformat().replace("+00:00", "Z"),
                        "created_at": value.get("createdAt"), "updated_at": value.get("updatedAt"),
                        "active_at_capture": value.get("active"),
                        "closed_at_capture": value.get("closed"),
                        "accepting_orders_at_capture": value.get("acceptingOrders"),
                        "source_instrument": "BINANCE_SPOT:BTCUSDT",
                        "comparison": "final_close_gte_open",
                        "rule_template": "exact_source_template_v1",
                        "capture_relation": _capture_relation(observed, start, end),
                        "raw_sha256": hashlib.sha256(line).hexdigest(),
                        "raw_locator": {"path": str(source), "offset": offset, "length": len(line)},
                    })
            offset += len(line)
    if digest.hexdigest() != source_sha256:
        raise ValueError("rule_catalog_source_hash")
    rows.sort(key=lambda row: (row["start_utc"], row["market_id"]))
    for key in ("market_id", "condition_id", "start_utc"):
        if len({row[key] for row in rows}) != len(rows):
            raise ValueError(f"rule_catalog_duplicate_{key}")
    payload = {
        "schema_version": "marketcow.btc-hour.rule-catalog.v1",
        "source": {"name": "polymarket_gamma", "path": str(source),
                   "sha256": source_sha256, "bytes": offset,
                   "observed_at": source_observed_at},
        "predicate": "exact_binance_btcusdt_finalized_1h_template_v1",
        "complete_for_source_and_predicate": True,
        "historical_coverage_complete": False,
        "limitations": ["single_frozen_gamma_catalog", "not_historical_change_log",
                        "capture_flags_are_not_current", "raw_rules_may_be_observed_after_window_start"],
        "record_count": len(rows),
        "before_window_count": sum(row["capture_relation"] == "before_window" for row in rows),
        "during_window_count": sum(row["capture_relation"] == "during_window" for row in rows),
        "after_window_count": sum(row["capture_relation"] == "after_window" for row in rows),
        "records": rows,
    }
    payload_raw = canonical(payload)
    result = {**payload, "catalog_id": hashlib.sha256(payload_raw).hexdigest()}
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("xb") as target:
        target.write(canonical(result))
        target.flush()
        os.fsync(target.fileno())
    descriptor = os.open(output.parent, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--source-sha256", required=True)
    parser.add_argument("--source-observed-at", required=True)
    parser.add_argument("--maximum-source-bytes", type=int, required=True)
    parser.add_argument("--maximum-line-bytes", type=int, default=2 << 20)
    parser.add_argument("--maximum-records", type=int, default=4096)
    args = parser.parse_args()
    result = build_rule_catalog(**vars(args))
    print(json.dumps({"catalog_id": result["catalog_id"],
                      "record_count": result["record_count"]}, sort_keys=True))


if __name__ == "__main__":
    main()
