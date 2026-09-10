"""Immutable bounded export of a fixed durable fact-log prefix; no network."""
import argparse
import hashlib
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

from .btc_fact_log import read_page
from .btc_hourly_dataset import canonical, timestamp


def export(root, output, *, through, start, end, source, mode, maximum_records, maximum_bytes):
    start_at, end_at = timestamp(start), timestamp(end)
    if start_at >= end_at or mode not in ("observed", "event_time_research"):
        raise ValueError("invalid_time_range_or_mode")
    if type(through) is not int or through < 0 or maximum_records <= 0 or maximum_bytes <= 0 or through > maximum_records:
        raise ValueError("export_budget")
    output.mkdir(parents=True, exist_ok=False)
    config = dict(through=through, start=start, end=end, source=source, mode=mode,
                  maximum_records=maximum_records, maximum_bytes=maximum_bytes)
    report = dict(schema_version="marketcow.btc-research.export.v1", config=config,
                  scanned=0, selected=0, missing_time=0, other_source=0, status="incomplete",
                  coverage_complete=False, error=None)
    scanned_hash, selected_hash = hashlib.sha256(), hashlib.sha256()
    scanned_bytes = selected_bytes = 0
    try:
        # Even the empty prefix must name an existing readable database.
        if through == 0:
            read_page(root, after=0, through=0, limit=1, maximum_bytes=maximum_bytes)
        with (output / "facts.jsonl").open("xb") as stream:
            while report["scanned"] < through:
                remaining = maximum_bytes-scanned_bytes
                if remaining <= 0:
                    raise ValueError("scan_byte_budget")
                page = read_page(root, after=report["scanned"], through=through,
                                 limit=min(100, through-report["scanned"]),
                                 maximum_bytes=min(8*1024*1024, remaining))
                for row in page:
                    raw = canonical(row)+b"\n"
                    scanned_hash.update(raw)
                    scanned_bytes += len(raw)
                    if scanned_bytes > maximum_bytes:
                        raise ValueError("scan_byte_budget")
                    report["scanned"] += 1
                    data = row["fact"]
                    if data.get("source") != source:
                        report["other_source"] += 1
                        continue
                    at = data.get("first_received_at") if mode == "observed" else data.get("event_at")
                    if at is None and mode == "event_time_research" and type(data.get("exchange_at_ms")) is int:
                        at = (datetime(1970, 1, 1, tzinfo=timezone.utc)+timedelta(milliseconds=data["exchange_at_ms"])).isoformat()
                    if at is None:
                        report["missing_time"] += 1
                        continue
                    if start_at <= timestamp(at) < end_at:
                        stream.write(raw)
                        selected_hash.update(raw)
                        selected_bytes += len(raw)
                        report["selected"] += 1
            stream.flush()
            os.fsync(stream.fileno())
        report["status"] = "export_complete"
    except Exception as exc:
        report["error"] = type(exc).__name__+":"+str(exc)
    report.update(scanned_canonical_sha256=scanned_hash.hexdigest(), scanned_bytes=scanned_bytes,
                  facts_sha256=selected_hash.hexdigest(), facts_bytes=selected_bytes,
                  limitation="fixed observed prefix; missing timestamps excluded; no market coverage or finality claim")
    report["dataset_id"] = hashlib.sha256(canonical({"config": config, "prefix": scanned_hash.hexdigest()})).hexdigest()
    report["code_sha256"] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    with (output / "manifest.json").open("xb") as stream:
        stream.write(canonical(report))
        stream.flush()
        os.fsync(stream.fileno())
    fd = os.open(output, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    for name in ("through", "maximum-records", "maximum-bytes"):
        parser.add_argument("--"+name, type=int, required=True)
    for name in ("start", "end", "source"):
        parser.add_argument("--"+name, required=True)
    parser.add_argument("--mode", choices=("observed", "event_time_research"), required=True)
    report = export(**vars(parser.parse_args()))
    print(canonical(report).decode())
    if report["status"] != "export_complete":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
