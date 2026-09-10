"""Execute one explicit Discovery-parent/Live-child BTC-hour rotation.

This entry never chooses markets and never retries a management mutation.  It
persists the exact reviewed bindings, raw post-activation observation and a
stage receipt so an uncertain response can be reconciled by a separate action.
"""
import argparse
import asyncio
import hashlib
import os
from pathlib import Path

from .btc_fact_log import FactLog
from .btc_hourly_dataset import canonical
from .btc_polymarket_binding import bind_hour
from .btc_rotation import HotHttpOperations, rotate_discovery_and_live
from .universe_live_probe import strict_json


FIELDS = {
    "schema_version", "management_endpoint", "discovery_endpoint", "live_endpoint",
    "caller", "bearer_file", "bearer_sha256", "discovery_request", "live_request",
    "reviewed_markets", "control_timeout_seconds", "maximum_control_response_bytes",
    "discovery_capture", "live_capture", "persistence",
}
DISCOVERY_BUDGET = {"maximum_bytes", "timeout"}
LIVE_BUDGET = {
    "seconds", "maximum_frames", "maximum_total_bytes", "maximum_frame_bytes",
    "maximum_fullsync_bytes",
}
PERSISTENCE_BUDGET = {"maximum_pending", "maximum_pending_bytes", "maximum_disk_bytes"}


def _read(path, maximum):
    with path.open("rb") as stream:
        raw = stream.read(maximum + 1)
    if len(raw) > maximum:
        raise ValueError("rotation_input_capacity")
    return raw


def read_config(path):
    raw = _read(path, 4 * 1024 * 1024)
    value = strict_json(raw)
    if set(value) != FIELDS or value["schema_version"] != "marketcow.btc-hour.rotation-config.v1":
        raise ValueError("rotation_config_schema")
    if (not isinstance(value["caller"], str) or not value["caller"].isascii()
            or not value["caller"] or len(value["caller"]) > 128):
        raise ValueError("rotation_caller_identity")
    bearer_path = Path(value["bearer_file"])
    if not bearer_path.is_absolute():
        raise ValueError("rotation_bearer_path")
    bearer_raw = _read(bearer_path, 8192)
    if hashlib.sha256(bearer_raw).hexdigest() != value["bearer_sha256"]:
        raise ValueError("rotation_bearer_identity")
    try:
        bearer = bearer_raw.decode("ascii").strip()
    except UnicodeDecodeError as exc:
        raise ValueError("rotation_bearer_encoding") from exc
    if not bearer or any(char in bearer for char in "\r\n"):
        raise ValueError("rotation_bearer_value")
    if (type(value["control_timeout_seconds"]) is not int
            or not 1 <= value["control_timeout_seconds"] <= 300
            or type(value["maximum_control_response_bytes"]) is not int
            or not 1 <= value["maximum_control_response_bytes"] <= 1024 * 1024):
        raise ValueError("rotation_control_budget")
    for section, fields in (("discovery_capture", DISCOVERY_BUDGET),
                            ("live_capture", LIVE_BUDGET),
                            ("persistence", PERSISTENCE_BUDGET)):
        budget = value[section]
        if (not isinstance(budget, dict) or set(budget) != fields
                or any(type(item) is not int or item <= 0 for item in budget.values())):
            raise ValueError("rotation_" + section + "_budget")
    if value["live_capture"]["seconds"] > 1800:
        raise ValueError("rotation_live_capture_budget")
    reviewed = value["reviewed_markets"]
    if not isinstance(reviewed, list) or not 1 <= len(reviewed) <= 3:
        raise ValueError("rotation_reviewed_market_budget")
    bindings = [bind_hour(row["evidence"], row["review"]) for row in reviewed]
    if len({row["market_id"] for row in bindings}) != len(bindings):
        raise ValueError("rotation_duplicate_binding")
    return value, bindings, bearer, hashlib.sha256(raw).hexdigest()


def run(config_path, output, *, operations_factory=HotHttpOperations,
        rotation=rotate_discovery_and_live):
    config, bindings, bearer, config_sha = read_config(config_path)
    output.mkdir(parents=True, exist_ok=False)
    report = {
        "schema_version": "marketcow.btc-hour.rotation-run.v1",
        "config_sha256": config_sha, "status": "failed", "error_type": None,
        "bindings": bindings, "rotation": None, "fact_log": None,
    }
    log = operations = None
    try:
        persistence = config["persistence"]
        log = FactLog(output / "facts", maximum_subscribers=2, **persistence)
        operations = operations_factory(
            config["management_endpoint"], bearer,
            timeout=config["control_timeout_seconds"],
            maximum_bytes=config["maximum_control_response_bytes"],
        )
        report["rotation"] = asyncio.run(rotation(
            operations, config["caller"], config["discovery_request"],
            config["live_request"], bindings, config["discovery_endpoint"],
            config["live_endpoint"], log.publish, output / "attempt",
            discovery_budget=config["discovery_capture"],
            capture_budgets=config["live_capture"],
        ))
        report["status"] = "capture_complete"
    except Exception as exc:
        report["error_type"] = type(exc).__name__
    finally:
        if operations is not None:
            operations.close()
        if log is not None:
            try:
                log.close(10)
            except Exception as exc:
                report["status"] = "failed"
                report["persistence_error_type"] = type(exc).__name__
            report["fact_log"] = log.status()
        target = output / "report.json"
        with target.open("xb") as stream:
            stream.write(canonical(report)); stream.flush(); os.fsync(stream.fileno())
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
    args = parser.parse_args()
    report = run(args.config, args.output)
    print(canonical({"status": report["status"], "error_type": report["error_type"]}).decode())
    if report["status"] != "capture_complete":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
