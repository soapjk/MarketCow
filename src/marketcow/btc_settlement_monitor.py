"""Bounded two-provider CTF finality polling for reviewed BTC hourly markets."""
import hashlib
import os
import signal
import subprocess
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

from .btc_finality_quorum import verify_quorum
from .btc_hourly_dataset import canonical
from .btc_lifecycle import HourRegistry
from .universe_live_probe import strict_json


def read(path, maximum):
    with path.open("rb") as stream:
        raw = stream.read(maximum + 1)
    if len(raw) > maximum:
        raise ValueError("settlement_input_capacity")
    return raw


def file_sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def providers(path):
    value = strict_json(read(path, 65536))
    if (not isinstance(value, list) or not 2 <= len(value) <= 3
            or len({row.get("provider_id") for row in value}) != len(value)):
        raise ValueError("two_independent_provider_profiles_required")
    endpoint_identities = set()
    for row in value:
        if set(row) != {"provider_id", "config_path", "config_sha256"}:
            raise ValueError("provider_manifest_schema")
        config = Path(row["config_path"])
        config_raw = read(config, 16384)
        if (not row["provider_id"].isascii() or not config.is_absolute()
                or hashlib.sha256(config_raw).hexdigest() != row["config_sha256"]):
            raise ValueError("provider_profile_identity")
        config_value = strict_json(config_raw)
        endpoint = config_value.get("rpc_endpoint")
        if not isinstance(endpoint, str) or not endpoint.startswith("https://"):
            raise ValueError("provider_endpoint_identity")
        endpoint_identity = hashlib.sha256(endpoint.encode()).hexdigest()
        if endpoint_identity in endpoint_identities:
            raise ValueError("independent_provider_endpoints_required")
        endpoint_identities.add(endpoint_identity)
        row["config_path"] = config
    return value


def execute(binary, config, condition, tokens, output, timeout):
    command = [str(binary), str(config), condition,
               "0x" + format(int(tokens[0]), "064x"), "0x" + format(int(tokens[1]), "064x"), str(output)]
    with subprocess.Popen(command, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                          stderr=subprocess.DEVNULL) as child:
        try:
            return child.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            child.kill(); child.wait()
            raise TimeoutError("settlement_provider_deadline") from None


def sweep_once(registry, provider_rows, binary, output, *, maximum_markets, seconds,
               runner=execute, verifier=verify_quorum):
    if (not 1 <= maximum_markets <= 24 or not 1 <= seconds <= 600
            or output.exists()):
        raise ValueError("settlement_sweep_budget_or_output")
    pending = registry.plan(datetime.now(timezone.utc), maximum_subscriptions=3)["settlement_pending"]
    pending = pending[:maximum_markets]
    output.mkdir(parents=True, mode=0o700)
    report = {"schema_version": "marketcow.btc-hour.settlement-monitor-round.v1",
              "started_at": datetime.now(timezone.utc).isoformat(), "pending_before": pending,
              "markets": [], "complete": False}
    deadline = time.monotonic() + seconds
    try:
        for market_id in pending:
            binding = registry.binding(market_id)
            observations, row = [], {"market_id": market_id, "provider_receipts": [], "status": "pending"}
            for index, provider in enumerate(provider_rows):
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError("settlement_round_deadline")
                target = output / f"{market_id}-{index}.json"
                code = runner(binary, provider["config_path"], binding["condition_id"],
                              [binding["up_token"], binding["down_token"]], target, min(65, remaining))
                raw = read(target, 1048576) if target.exists() else b""
                receipt = {"provider_id": provider["provider_id"], "exit_code": code,
                           "raw_bytes": len(raw), "raw_sha256": hashlib.sha256(raw).hexdigest()}
                row["provider_receipts"].append(receipt)
                if code != 0:
                    raise RuntimeError("settlement_provider_failed")
                value = strict_json(raw)
                if value.get("status") == "unresolved":
                    row["status"] = "unresolved"
                observations.append({"provider_id": provider["provider_id"], "path": target})
            if row["status"] != "unresolved":
                final = verifier(observations, condition_id=binding["condition_id"],
                                 token_ids=[binding["up_token"], binding["down_token"]])
                final_raw = canonical(final)
                final_path = output / f"{market_id}-verified.json"
                with final_path.open("xb") as stream:
                    stream.write(final_raw); stream.flush(); os.fsync(stream.fileno())
                row.update(status="verified_final", final_sha256=registry.observe_final_settlement(market_id, final_raw))
            report["markets"].append(row)
        report["complete"] = True
    except Exception as exc:
        report["error_type"] = type(exc).__name__
    finally:
        report["finished_at"] = datetime.now(timezone.utc).isoformat()
        with (output / "report.json").open("xb") as stream:
            stream.write(canonical(report)); stream.flush(); os.fsync(stream.fileno())
    return report


def main():
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--registry", type=Path, required=True)
    parser.add_argument("--providers", type=Path, required=True)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--binary-sha256", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--maximum-registry-markets", type=int, required=True)
    parser.add_argument("--maximum-registry-bytes", type=int, required=True)
    parser.add_argument("--maximum-markets-per-round", type=int, required=True)
    parser.add_argument("--seconds-per-round", type=int, required=True)
    parser.add_argument("--interval-seconds", type=int, required=True)
    parser.add_argument("--maximum-consecutive-failures", type=int, required=True)
    args = parser.parse_args()
    if (not args.binary.is_absolute() or file_sha256(args.binary) != args.binary_sha256
            or args.interval_seconds < 60 or not 1 <= args.maximum_consecutive_failures <= 10):
        parser.error("invalid binary or explicit monitor budget")
    rows = providers(args.providers)
    registry = HourRegistry(args.registry, maximum_markets=args.maximum_registry_markets,
                            maximum_bytes=args.maximum_registry_bytes)
    args.output.mkdir(parents=True, exist_ok=True)
    stop = threading.Event()
    for signum in (signal.SIGTERM, signal.SIGINT):
        signal.signal(signum, lambda *_: stop.set())
    failures = round_index = 0
    while not stop.is_set():
        target = args.output / f"round-{time.time_ns()}-{round_index}"
        result = sweep_once(registry, rows, args.binary, target,
            maximum_markets=args.maximum_markets_per_round, seconds=args.seconds_per_round)
        print(canonical({"round": round_index, "complete": result["complete"],
                         "markets": len(result["markets"]), "error_type": result.get("error_type")}).decode(), flush=True)
        failures = 0 if result["complete"] else failures + 1
        if failures >= args.maximum_consecutive_failures:
            raise SystemExit("settlement failure budget exhausted")
        round_index += 1
        stop.wait(args.interval_seconds)


if __name__ == "__main__":
    main()
