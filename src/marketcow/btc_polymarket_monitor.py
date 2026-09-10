"""Persistent reviewed BTC-hour capture with fresh-scope reconnects.

The monitor reads the authoritative Live identity before every session.  It
never resumes a cursor across sessions or mutates a scope.  Disconnects and hot
scope changes therefore lead to a new full-sync/ready boundary.
"""
import argparse
import asyncio
import hashlib
import signal
from datetime import datetime, timezone
from pathlib import Path

from .btc_fact_log import FactLog
from .btc_lifecycle import HourRegistry
from .btc_polymarket_capture import capture
from .btc_rotation import HotHttpOperations
from .universe_live_probe import strict_json


FIELDS = {
    "schema_version", "management_endpoint", "live_endpoint", "caller",
    "bearer_file", "bearer_sha256", "control_timeout_seconds",
    "maximum_control_response_bytes", "lifecycle_db", "maximum_lifecycle_markets",
    "maximum_lifecycle_payload_bytes", "capture", "reconnect_seconds",
    "maximum_consecutive_failures", "maximum_pending", "maximum_pending_bytes",
    "maximum_disk_bytes",
}
CAPTURE_FIELDS = {
    "seconds", "maximum_frames", "maximum_total_bytes", "maximum_frame_bytes",
    "maximum_fullsync_bytes",
}


def _read(path, maximum):
    with path.open("rb") as stream:
        raw = stream.read(maximum + 1)
    if len(raw) > maximum:
        raise ValueError("polymarket_monitor_input_capacity")
    return raw


def read_config(path):
    raw = _read(path, 1048576)
    value = strict_json(raw)
    if set(value) != FIELDS or value["schema_version"] != "marketcow.btc-hour.polymarket-monitor.v1":
        raise ValueError("polymarket_monitor_schema")
    capture_budget = value["capture"]
    integer_fields = (
        "control_timeout_seconds", "maximum_control_response_bytes",
        "maximum_lifecycle_markets", "maximum_lifecycle_payload_bytes",
        "reconnect_seconds", "maximum_consecutive_failures", "maximum_pending",
        "maximum_pending_bytes", "maximum_disk_bytes",
    )
    if (not isinstance(capture_budget, dict) or set(capture_budget) != CAPTURE_FIELDS
            or any(type(value[key]) is not int or value[key] <= 0 for key in integer_fields)
            or any(type(item) is not int or item <= 0 for item in capture_budget.values())
            or capture_budget["seconds"] > 1800 or value["control_timeout_seconds"] > 300
            or value["maximum_control_response_bytes"] > 1048576
            or not 1 <= value["maximum_consecutive_failures"] <= 100):
        raise ValueError("polymarket_monitor_budget")
    for key in ("bearer_file", "lifecycle_db"):
        if not Path(value[key]).is_absolute():
            raise ValueError("polymarket_monitor_absolute_path")
    secret = _read(Path(value["bearer_file"]), 8192)
    if hashlib.sha256(secret).hexdigest() != value["bearer_sha256"]:
        raise ValueError("polymarket_monitor_bearer_identity")
    try:
        bearer = secret.decode("ascii").strip()
    except UnicodeDecodeError as exc:
        raise ValueError("polymarket_monitor_bearer_encoding") from exc
    if not bearer or any(char in bearer for char in "\r\n"):
        raise ValueError("polymarket_monitor_bearer_value")
    return value, bearer


def current_bindings(registry, now):
    plan = registry.plan(now, maximum_subscriptions=3)
    return [registry.binding(market_id) for market_id in plan["subscribe_proposal"]]


async def monitor(config, operations, registry, publish, stop, *, capture_function=capture,
                  now_provider=lambda: datetime.now(timezone.utc)):
    failures = sessions = 0
    while not stop.is_set():
        try:
            bindings = await asyncio.to_thread(current_bindings, registry, now_provider())
            if not bindings:
                publish({"schema_version": "marketcow.btc-hour.polymarket-monitor-event.v1",
                         "event_type": "no_current_reviewed_hour", "session": sessions})
            else:
                status = await asyncio.to_thread(operations.status, "live")
                actual = status.get("actual")
                scope = actual.get("scope_id") if isinstance(actual, dict) else None
                if (status.get("schema_version") != "marketcow.hot-scope-status.v1"
                        or status.get("pool") != "live" or not isinstance(scope, str) or not scope):
                    raise ValueError("invalid_live_status_identity")
                capture_task = asyncio.create_task(capture_function(
                    config["live_endpoint"], scope, bindings, publish, **config["capture"]))
                stop_task = asyncio.create_task(stop.wait())
                done, _ = await asyncio.wait((capture_task, stop_task),
                                             return_when=asyncio.FIRST_COMPLETED)
                if stop_task in done and capture_task not in done:
                    capture_task.cancel()
                    await asyncio.gather(capture_task, return_exceptions=True)
                    return
                stop_task.cancel()
                await asyncio.gather(stop_task, return_exceptions=True)
                result = await capture_task
                if result.get("ready_observed") is not True or result.get("scope_id") != scope:
                    raise ValueError("capture_missing_fresh_baseline")
                publish({"schema_version": "marketcow.btc-hour.polymarket-monitor-event.v1",
                         "event_type": "capture_session_complete", "session": sessions,
                         "scope_id": scope, "stream_instance_id": result.get("stream_instance_id"),
                         "last_cursor": result.get("last_cursor")})
            failures = 0
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            failures += 1
            publish({"schema_version": "marketcow.btc-hour.polymarket-monitor-event.v1",
                     "event_type": "capture_session_failed", "session": sessions,
                     "consecutive_failures": failures, "error_type": type(exc).__name__})
            if failures >= config["maximum_consecutive_failures"]:
                raise RuntimeError("polymarket_monitor_failure_budget") from exc
        sessions += 1
        try:
            await asyncio.wait_for(stop.wait(), config["reconnect_seconds"])
        except TimeoutError:
            pass


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    config, bearer = read_config(args.config)
    registry = HourRegistry(Path(config["lifecycle_db"]),
                            maximum_markets=config["maximum_lifecycle_markets"],
                            maximum_bytes=config["maximum_lifecycle_payload_bytes"])
    log = FactLog(args.output, maximum_pending=config["maximum_pending"],
                  maximum_pending_bytes=config["maximum_pending_bytes"],
                  maximum_disk_bytes=config["maximum_disk_bytes"], maximum_subscribers=4)
    operations = HotHttpOperations(
        config["management_endpoint"], bearer,
        timeout=config["control_timeout_seconds"],
        maximum_bytes=config["maximum_control_response_bytes"],
    )

    async def run():
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        installed = []
        try:
            for signum in (signal.SIGTERM, signal.SIGINT):
                loop.add_signal_handler(signum, stop.set)
                installed.append(signum)
            await monitor(config, operations, registry, log.publish, stop)
        finally:
            for signum in installed:
                loop.remove_signal_handler(signum)

    try:
        asyncio.run(run())
    finally:
        operations.close()
        log.close(10)


if __name__ == "__main__":
    main()
