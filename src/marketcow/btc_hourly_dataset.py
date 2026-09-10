"""Bounded, offline Binance archive import for BTC hourly research.

This is a historical dataset builder, not the realtime persistence path.
It never assigns historical first-receipt times or Polymarket settlement labels.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import os
import re
import zipfile
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path


UTC = timezone.utc
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
INTERVAL_US = {"1m": 60_000_000, "1h": 3_600_000_000}


def canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode()


def timestamp(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("timezone_required")
    return parsed.astimezone(UTC)


def iso(value: datetime) -> str:
    if value.tzinfo is None:
        raise ValueError("timezone_required")
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def decimal_value(raw: str) -> Decimal:
    try:
        result = Decimal(raw)
    except InvalidOperation as exc:
        raise ValueError("invalid_decimal") from exc
    if not result.is_finite() or result < 0:
        raise ValueError("invalid_decimal")
    return result


def visible_asof(fact: dict, asof: datetime, mode: str) -> bool:
    """Event-time research is explicitly NOT historical observed availability."""
    if asof.tzinfo is None:
        raise ValueError("timezone_required")
    if mode == "observed":
        value = fact.get("first_received_at")
    elif mode == "event_time_research":
        value = fact["event_at"]
    else:
        raise ValueError("invalid_asof_mode")
    return value is not None and timestamp(value) <= asof


def import_archive(
    archive: Path, checksum: Path, output: Path, *, interval: str,
    day: str, captured_at: datetime, maximum_zip_bytes: int,
    maximum_uncompressed_bytes: int, maximum_records: int,
) -> dict:
    if captured_at.tzinfo is None:
        raise ValueError("timezone_required")
    if interval not in INTERVAL_US or min(maximum_zip_bytes, maximum_uncompressed_bytes, maximum_records) <= 0:
        raise ValueError("invalid_budget_or_interval")
    start = datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=UTC)
    if start < datetime(2025, 1, 1, tzinfo=UTC):
        raise ValueError("only_explicit_microsecond_archive_supported")
    end = start + timedelta(days=1)
    if end > captured_at:
        raise ValueError("future_or_incomplete_day")
    expected_name = f"BTCUSDT-{interval}-{day}.zip"
    if archive.name != expected_name:
        raise ValueError("archive_identity_mismatch")
    if checksum.stat().st_size > 4096:
        raise ValueError("checksum_size_exceeded")
    match = re.fullmatch(r"([0-9a-fA-F]{64})\s+\*?([^\r\n]+)\s*", checksum.read_text().strip())
    if match is None or match[2] != expected_name:
        raise ValueError("checksum_identity_mismatch")
    # Hold the same descriptor throughout hashing and parsing.
    with archive.open("rb") as source:
        if os.fstat(source.fileno()).st_size > maximum_zip_bytes:
            raise ValueError("zip_size_exceeded")
        digest = hashlib.file_digest(source, "sha256").hexdigest()
        if digest != match[1].lower():
            raise ValueError("checksum_mismatch")
        source.seek(0)
        with zipfile.ZipFile(source) as zipped:
            entries = zipped.infolist()
            if len(entries) != 1 or entries[0].filename != expected_name.removesuffix(".zip") + ".csv":
                raise ValueError("zip_member_identity_mismatch")
            member = entries[0]
            if member.file_size > maximum_uncompressed_bytes:
                raise ValueError("uncompressed_size_exceeded")
            output.mkdir(parents=True, exist_ok=False)
            # An interrupted/failed directory is retained without manifest.json.
            raw_sha = hashlib.sha256()
            facts_sha = hashlib.sha256()
            records = 0
            previous_open = None
            gaps: list[dict] = []
            step = INTERVAL_US[interval]
            start_us = (start - EPOCH) // timedelta(microseconds=1)
            end_us = (end - EPOCH) // timedelta(microseconds=1)
            expected_open = start_us
            offset = 0
            with zipped.open(member) as csv_source, (output / "raw.csv").open("xb") as raw_out, (output / "facts.jsonl").open("xb") as facts_out:
                while True:
                    line = csv_source.readline(min(65537, maximum_uncompressed_bytes - offset + 1))
                    if not line:
                        break
                    if len(line) > 65536 or offset + len(line) > maximum_uncompressed_bytes:
                        raise ValueError("csv_byte_budget_exceeded")
                    if records >= maximum_records:
                        raise ValueError("record_budget_exceeded")
                    row = next(csv.reader(io.StringIO(line.decode("utf-8", errors="strict")), strict=True))
                    if len(row) != 12:
                        raise ValueError("kline_schema_mismatch")
                    opened, closed, trades = int(row[0]), int(row[6]), int(row[8])
                    if not start_us <= opened < end_us or opened % step or closed != opened + step - 1:
                        raise ValueError("kline_window_mismatch")
                    if previous_open is not None and opened <= previous_open:
                        raise ValueError("duplicate_or_out_of_order")
                    values = [decimal_value(row[index]) for index in (1, 2, 3, 4, 5, 7, 9, 10)]
                    opening, high, low, closing = values[:4]
                    if not 0 < low <= min(opening, closing) <= max(opening, closing) <= high or trades < 0:
                        raise ValueError("invalid_ohlcv")
                    if opened != expected_open:
                        gaps.append({"start_us": expected_open, "end_us_exclusive": opened})
                    fact = {
                        "schema_version": "marketcow.btc-hourly.fact.v1", "type": "bar_final",
                        "source": "binance_spot_public_archive", "source_version": "spot_klines_us_2025",
                        "instrument_id": "BTCUSDT.BINANCE", "product": "spot", "interval": interval,
                        "event_at": iso(EPOCH + timedelta(microseconds=closed)),
                        "source_published_at": None, "first_received_at": None,
                        "received_monotonic_ns": None, "ingested_at": iso(captured_at),
                        "capture_id": digest, "connection_epoch": None, "source_sequence": None,
                        "local_sequence": records + 1, "raw_sha256": hashlib.sha256(line).hexdigest(),
                        "raw_locator": {"file": "raw.csv", "offset": offset, "length": len(line)},
                        "quality": "historical_archive", "missing_reasons": ["historical_first_received_unknown"],
                        "supersedes": None,
                        "payload": {"open_us": opened, "close_us": closed, "final": True,
                                    "open": row[1], "high": row[2], "low": row[3], "close": row[4],
                                    "volume": row[5], "quote_volume": row[7], "trades": trades,
                                    "taker_base_volume": row[9], "taker_quote_volume": row[10]},
                    }
                    encoded = canonical(fact) + b"\n"
                    raw_out.write(line)
                    raw_sha.update(line)
                    facts_out.write(encoded)
                    facts_sha.update(encoded)
                    offset += len(line)
                    records += 1
                    previous_open, expected_open = opened, opened + step
                if expected_open != end_us:
                    gaps.append({"start_us": expected_open, "end_us_exclusive": end_us})
                for stream in (raw_out, facts_out):
                    stream.flush()
                    os.fsync(stream.fileno())
    manifest = {
        "schema_version": "marketcow.btc-hourly.dataset.v1", "status": "complete",
        "dataset_id": digest, "capture_at": iso(captured_at),
        "source_url": f"https://data.binance.vision/data/spot/daily/klines/BTCUSDT/{interval}/{expected_name}",
        "archive_sha256": digest, "archive_bytes": archive.stat().st_size,
        "code_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "configuration": {"interval": interval, "day": day, "maximum_zip_bytes": maximum_zip_bytes,
                          "maximum_uncompressed_bytes": maximum_uncompressed_bytes, "maximum_records": maximum_records},
        "records": records, "expected_records": (end_us - start_us) // step,
        "coverage_complete": not gaps, "gaps": gaps, "durable_sequence": records,
        "parts": [{"file": "raw.csv", "sha256": raw_sha.hexdigest(), "bytes": offset},
                  {"file": "facts.jsonl", "sha256": facts_sha.hexdigest(), "bytes": (output / "facts.jsonl").stat().st_size}],
        "limitations": ["historical_first_received_unknown", "not_polymarket_settlement", "not_orderbook_data"],
    }
    manifest["configuration_sha256"] = hashlib.sha256(canonical(manifest["configuration"])).hexdigest()
    manifest["manifest_payload_sha256"] = hashlib.sha256(canonical(manifest)).hexdigest()
    with (output / "manifest.pending").open("xb") as target:
        target.write(canonical(manifest))
        target.flush()
        os.fsync(target.fileno())
    os.rename(output / "manifest.pending", output / "manifest.json")
    descriptor = os.open(output, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    return manifest


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("archive", "checksum", "output"):
        parser.add_argument(f"--{name}", type=Path, required=True)
    parser.add_argument("--interval", choices=INTERVAL_US, required=True)
    parser.add_argument("--day", required=True)
    parser.add_argument("--captured-at", type=timestamp, required=True)
    for name in ("maximum-zip-bytes", "maximum-uncompressed-bytes", "maximum-records"):
        parser.add_argument(f"--{name}", type=int, required=True)
    print(json.dumps(import_archive(**vars(parser.parse_args())), sort_keys=True))


if __name__ == "__main__":
    main()
