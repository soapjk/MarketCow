"""Project a bounded Rust BTC research archive into versioned local L2 states."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from decimal import Decimal, InvalidOperation
from pathlib import Path


def _canonical(value) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()


def _sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _file_sha(path: Path) -> tuple[int, str]:
    digest, size = hashlib.sha256(), 0
    with path.open("rb") as source:
        while chunk := source.read(1024 * 1024):
            size += len(chunk)
            digest.update(chunk)
    return size, digest.hexdigest()


def _decimal(value, name: str) -> Decimal:
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"invalid_{name}") from exc
    if not result.is_finite():
        raise ValueError(f"invalid_{name}")
    return result


def _levels(values, side: str) -> dict[Decimal, Decimal]:
    result = {}
    for row in values or []:
        price, size = _decimal(row["price"], "price"), _decimal(row["size"], "size")
        if not Decimal(0) <= price <= Decimal(1) or size <= 0 or price in result:
            raise ValueError(f"invalid_{side}_level")
        result[price] = size
    return result


def _render(levels: dict[Decimal, Decimal], reverse: bool) -> list[dict]:
    return [{"price": str(price), "size": str(levels[price])} for price in sorted(levels, reverse=reverse)]


def project_archive(capture: Path, output: Path) -> dict:
    if not capture.is_absolute() or not output.is_absolute() or output.exists():
        raise ValueError("absolute_new_output_required")
    config_raw = (capture / "config.json").read_bytes()
    report_raw = (capture / "report.json").read_bytes()
    archive = capture / "frames.jsonl"
    report, config = json.loads(report_raw), json.loads(config_raw)
    if report.get("status") != "complete" or report.get("config_sha256") != _sha(config_raw):
        raise ValueError("incomplete_or_unbound_capture")
    archive_bytes, archive_sha = _file_sha(archive)
    if report.get("frames_sha256") != archive_sha or report.get("bytes") != archive_bytes:
        raise ValueError("archive_integrity")
    token_map = {}
    for market in config["markets"]:
        for index, token in enumerate(market["token_ids"]):
            if token in token_map:
                raise ValueError("duplicate_token")
            token_map[token] = (market, "Up" if index == 0 else "Down")
    output.mkdir(mode=0o700, parents=True)
    states, epochs, emitted, rejected = {}, {}, 0, 0
    digest, output_bytes = hashlib.sha256(), 0
    maximum_line_bytes = min(int(config.get("maximum_batch_bytes", 8 * 1024 * 1024)), 8 * 1024 * 1024) + 1
    maximum_output_bytes = min(int(config.get("maximum_total_bytes", archive_bytes)) * 2, 1024 * 1024 * 1024)
    with (output / "l2.jsonl").open("xb") as target:
        with archive.open("rb") as source:
            ordinal = 0
            while line := source.readline(maximum_line_bytes + 1):
                ordinal += 1
                if len(line) > maximum_line_bytes or not line.endswith(b"\n"):
                    raise ValueError("frame_line_budget")
                envelope = json.loads(line)
                if envelope.get("schema_version") != "marketcow.btc-hour.rust-research-frame.v1":
                    raise ValueError("frame_schema")
                payload = envelope["raw_payload"]
                event_type = payload.get("event_type")
                affected = []
                changes_by_token = {}
                if event_type == "source_gap":
                    token = str(payload.get("asset_id"))
                    if token in token_map:
                        states.pop(token, None)
                        epochs[token] = epochs.get(token, 0) + 1
                    continue
                if event_type == "book":
                    token = str(payload.get("asset_id"))
                    if token not in token_map:
                        rejected += 1
                        continue
                    epochs[token] = epochs.get(token, 0) + 1
                    states[token] = {
                        "bids": _levels(payload.get("bids"), "bid"),
                        "asks": _levels(payload.get("asks"), "ask"),
                        "sequence": 1,
                    }
                    affected = [token]
                elif event_type == "price_change":
                    grouped = {}
                    for change in payload.get("price_changes", []):
                        grouped.setdefault(str(change.get("asset_id")), []).append(change)
                    for token, changes in grouped.items():
                        if token not in token_map or token not in states:
                            rejected += 1
                            continue
                        candidate = {
                            "bids": dict(states[token]["bids"]),
                            "asks": dict(states[token]["asks"]),
                            "sequence": states[token]["sequence"] + 1,
                        }
                        for change in changes:
                            side = str(change.get("side", "")).upper()
                            levels = (
                                candidate["bids"]
                                if side in {"BUY", "BID"}
                                else candidate["asks"]
                                if side in {"SELL", "ASK"}
                                else None
                            )
                            if levels is None:
                                raise ValueError("invalid_side")
                            price, size = _decimal(change["price"], "price"), _decimal(change["size"], "size")
                            if not Decimal(0) <= price <= Decimal(1) or size < 0:
                                raise ValueError("invalid_change")
                            if size == 0:
                                levels.pop(price, None)
                            else:
                                levels[price] = size
                        states[token] = candidate
                        affected.append(token)
                        changes_by_token[token] = [
                            {
                                "side": str(change["side"]).upper(),
                                "price": str(_decimal(change["price"], "price")),
                                "size": str(_decimal(change["size"], "size")),
                            }
                            for change in changes
                        ]
                for token in affected:
                    state = states[token]
                    market, outcome = token_map[token]
                    book = {"bids": _render(state["bids"], True), "asks": _render(state["asks"], False)}
                    if (
                        book["bids"]
                        and book["asks"]
                        and Decimal(book["bids"][0]["price"]) >= Decimal(book["asks"][0]["price"])
                    ):
                        raise ValueError("crossed_book")
                    row = {
                        "schema_version": "marketcow.btc-hour.research-l2.v1",
                        "capture_config_sha256": _sha(config_raw),
                        "capture_frames_sha256": archive_sha,
                        "market_id": market["market_id"],
                        "condition_id": market["condition_id"],
                        "token_id": token,
                        "outcome": outcome,
                        "book_epoch": epochs[token],
                        "local_sequence": state["sequence"],
                        "received_at": envelope["received_at"],
                        "source_timestamp": payload.get("timestamp"),
                        "input_ordinal": ordinal,
                        "input_line_sha256": _sha(line.rstrip(b"\n")),
                        "raw_wire_bytes_preserved": False,
                        "state_checksum": _sha(_canonical(book)),
                    }
                    if event_type == "book":
                        row["kind"] = "snapshot"
                        row["book"] = book
                    else:
                        row["kind"] = "delta"
                        row["changes"] = changes_by_token[token]
                    raw = _canonical(row) + b"\n"
                    output_bytes += len(raw)
                    if output_bytes > maximum_output_bytes:
                        raise ValueError("l2_output_budget")
                    target.write(raw)
                    digest.update(raw)
                    emitted += 1
        target.flush()
        os.fsync(target.fileno())
    complete = []
    for market in config["markets"]:
        if all(token in states for token in market["token_ids"]):
            complete.append(market["market_id"])
    result = {
        "schema_version": "marketcow.btc-hour.research-l2-report.v1",
        "capture_config_sha256": _sha(config_raw),
        "capture_report_sha256": _sha(report_raw),
        "capture_frames_sha256": archive_sha,
        "l2_rows": emitted,
        "l2_bytes": output_bytes,
        "maximum_l2_bytes": maximum_output_bytes,
        "l2_sha256": digest.hexdigest(),
        "complete_two_sided_market_ids": sorted(complete),
        "missing_or_gapped_token_ids": sorted(set(token_map) - set(states)),
        "rejected_unapplied_changes": rejected,
        "sequence_semantics": "capture_order_within_book_epoch_not_venue_sequence",
        "raw_wire_bytes_preserved": False,
    }
    raw = _canonical(result)
    fd = os.open(output / "report.json", os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "wb") as target:
        target.write(raw)
        target.flush()
        os.fsync(target.fileno())
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--capture", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    print(json.dumps(project_archive(args.capture, args.output), sort_keys=True, separators=(",", ":")))


if __name__ == "__main__":
    main()
