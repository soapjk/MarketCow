"""Explicit read-only BTC-hour capture entry point; no scope activation."""
import argparse
import asyncio
import hashlib
import os
from datetime import datetime, timezone
from pathlib import Path

from .btc_fact_log import FactLog
from .btc_hourly_dataset import canonical
from .btc_polymarket_binding import bind_hour
from .btc_polymarket_capture import capture
from .universe_live_probe import strict_json


FIELDS = {"schema_version", "endpoint", "scope_id", "reviewed_markets", "seconds",
          "maximum_frames", "maximum_total_bytes", "maximum_frame_bytes", "maximum_fullsync_bytes",
          "maximum_pending", "maximum_pending_bytes", "maximum_disk_bytes"}


def read_config(path):
    with path.open("rb") as source:
        raw = source.read(2*1024*1024+1)
    if len(raw) > 2*1024*1024:
        raise ValueError("configuration_budget")
    config = strict_json(raw)
    if set(config) != FIELDS or config["schema_version"] != "marketcow.btc-hour.capture-config.v1":
        raise ValueError("configuration_schema")
    for key in FIELDS:
        if key.startswith("maximum_") or key == "seconds":
            if type(config[key]) is not int or config[key] <= 0:
                raise ValueError("configuration_budget")
    if config["seconds"] > 1800 or not isinstance(config["scope_id"], str) or not config["scope_id"]:
        raise ValueError("configuration_scope_or_duration")
    markets = config["reviewed_markets"]
    if not isinstance(markets, list) or not 1 <= len(markets) <= 3:
        raise ValueError("configuration_market_budget")
    bindings = [bind_hour(m["evidence"], m["review"]) for m in markets]
    if len({b["market_id"] for b in bindings}) != len(bindings):
        raise ValueError("duplicate_reviewed_market")
    return config, bindings, hashlib.sha256(raw).hexdigest()


def run(config_path, output, *, capture_function=capture, registry=None):
    config, bindings, digest = read_config(config_path)
    # A new capture keeps its configuration/report immutable and avoids conflating runs.
    output.mkdir(parents=True, exist_ok=False)
    report = {"schema_version": "marketcow.btc-hour.capture-report.v1",
              "config_sha256": digest, "bindings": bindings, "capture": None,
              "error": None, "persistence_error": None, "status": "failed"}
    log = None
    try:
        if registry is not None:
            registry.register(bindings)
            report["lifecycle_plan"] = registry.plan(datetime.now(timezone.utc), maximum_subscriptions=3)
        log = FactLog(output / "facts", maximum_pending=config["maximum_pending"],
                      maximum_pending_bytes=config["maximum_pending_bytes"],
                      maximum_disk_bytes=config["maximum_disk_bytes"], maximum_subscribers=4)
        kwargs = {k: config[k] for k in ("seconds", "maximum_frames", "maximum_total_bytes",
                                       "maximum_frame_bytes", "maximum_fullsync_bytes")}
        report["capture"] = asyncio.run(capture_function(config["endpoint"], config["scope_id"],
                                                        bindings, log.publish, **kwargs))
        if not report["capture"]["ready_observed"]:
            raise ValueError("ready_not_observed")
        report["status"] = "capture_complete"
    except Exception as exc:
        # Transport exception strings can include URLs/configuration; do not echo them.
        report["error"] = type(exc).__name__
    finally:
        if log is not None:
            try:
                log.close(10)
            except Exception as exc:
                report["persistence_error"] = type(exc).__name__
                report["status"] = "failed"
            report["log"] = log.status()
        report["code_sha256"] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
        with (output / "report.json").open("xb") as stream:
            stream.write(canonical(report))
            stream.flush()
            os.fsync(stream.fileno())
        directory = os.open(output, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--lifecycle-db", type=Path)
    parser.add_argument("--maximum-lifecycle-markets", type=int)
    parser.add_argument("--maximum-lifecycle-payload-bytes", type=int)
    args = parser.parse_args()
    registry = None
    options = (args.lifecycle_db, args.maximum_lifecycle_markets, args.maximum_lifecycle_payload_bytes)
    if any(v is not None for v in options):
        if any(v is None for v in options):
            parser.error("lifecycle database requires explicit market and payload budgets")
        from .btc_lifecycle import HourRegistry
        registry = HourRegistry(args.lifecycle_db, maximum_markets=args.maximum_lifecycle_markets,
                                maximum_bytes=args.maximum_lifecycle_payload_bytes)
    report = run(args.config, args.output, registry=registry)
    print(canonical({"status": report["status"], "error": report["error"],
                     "persistence_error": report["persistence_error"]}).decode())
    if report["status"] != "capture_complete":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
