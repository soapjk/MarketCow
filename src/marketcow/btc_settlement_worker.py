"""Finite pending-settlement sweep using the existing pinned Rust reader.

Operator-provided RPC profiles stay in their files, not output or command text.
No default RPC endpoint, no account writes, no finality promotion.
"""
import hashlib
import os
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path

from .btc_hourly_dataset import canonical
from .universe_live_probe import strict_json


def read_bounded(path, maximum):
    with path.open("rb") as stream:
        raw = stream.read(maximum + 1)
    if len(raw) > maximum:
        raise ValueError("settlement_file_capacity")
    return raw


def verify_profile(path, binding):
    raw = read_bounded(path, 16384)
    value = strict_json(raw)
    expected_tokens = ["0x" + format(int(binding[k]), "064x") for k in ("up_token", "down_token")]
    if (value.get("condition_id") != binding["condition_id"]
            or value.get("token_ids_hex") != expected_tokens
            or any(len(t) != 66 for t in expected_tokens)):
        raise ValueError("settlement_profile_identity")
    for key in ("rpc_endpoint", "chain_id", "contract", "code_sha256", "finality_policy", "collateral"):
        if not isinstance(value.get(key), str) or not value[key]:
            raise ValueError("settlement_profile_missing")
    return hashlib.sha256(raw).hexdigest()


def execute_reader(binary, profile, output, timeout):
    # Arguments are fixed; neither source data nor profile contents become shell code.
    with subprocess.Popen([str(binary), str(profile), str(output)],
                          stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                          stderr=subprocess.DEVNULL) as child:
        try:
            return child.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            child.kill()
            child.wait()
            raise TimeoutError("settlement_reader_deadline") from None


def sweep(registry, profiles, binary, binary_sha256, output, *, market_ids,
          seconds, maximum_markets, runner=execute_reader):
    if (not 0 < seconds <= 300 or type(maximum_markets) is not int or not 1 <= maximum_markets <= 10
            or not 0 < len(market_ids) <= maximum_markets or len(set(market_ids)) != len(market_ids)):
        raise ValueError("settlement_sweep_budget")
    binary = Path(binary)
    if not binary.is_absolute() or not binary.is_file():
        raise ValueError("explicit_rust_binary_required")
    with binary.open("rb") as stream:
        digest = hashlib.file_digest(stream, "sha256").hexdigest()
    if digest != binary_sha256:
        raise ValueError("settlement_binary_hash")
    # Verify every identity/config before launching any child or touching its RPC.
    checked = []
    now = datetime.now(timezone.utc)
    from .btc_polymarket_binding import timestamp
    for market in market_ids:
        binding = registry.binding(market)
        if timestamp(binding["end_utc"]) > now:
            raise ValueError("hour_not_ended")
        path = Path(profiles[market])
        if not path.is_absolute():
            raise ValueError("absolute_profile_required")
        checked.append((market, path, verify_profile(path, binding)))
    output.mkdir(parents=True, exist_ok=False)
    report = dict(schema_version="marketcow.btc-hour.settlement-sweep.v1",
                  binary_sha256=digest, observations=[], complete=False,
                  settlement_import_allowed=False)
    deadline = time.monotonic() + seconds
    try:
        for index, (market, profile, config_sha) in enumerate(checked):
            remaining = deadline-time.monotonic()
            if remaining <= 0:
                raise TimeoutError("settlement_sweep_deadline")
            path = output / f"observation-{index}.json"
            # Recheck mutable operator config immediately before execution.
            if verify_profile(profile, registry.binding(market)) != config_sha:
                raise ValueError("settlement_profile_changed")
            code = runner(binary, profile, path, min(65, remaining))
            raw = read_bounded(path, 1048576) if path.exists() else b""
            row = dict(market_id=market, config_sha256=config_sha, exit_code=code,
                       bytes=len(raw), raw_sha256=hashlib.sha256(raw).hexdigest(),
                       filename=path.name, stored=False)
            report["observations"].append(row)
            if code != 0:
                raise RuntimeError("settlement_reader_failed")
            row["stored_sha256"] = registry.observe_settlement(market, raw)
            row["stored"] = True
        report["complete"] = True
    except Exception as exc:
        report["error_type"] = type(exc).__name__
    finally:
        with (output / "report.json").open("xb") as stream:
            stream.write(canonical(report))
            stream.flush()
            os.fsync(stream.fileno())
    return report


def main():
    import argparse
    from .btc_lifecycle import HourRegistry
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("registry", "profiles", "binary", "output"):
        parser.add_argument("--" + name, type=Path, required=True)
    parser.add_argument("--binary-sha256", required=True)
    parser.add_argument("--market-ids", nargs="+", required=True)
    for name in ("seconds", "maximum-markets", "maximum-registry-markets", "maximum-registry-bytes"):
        parser.add_argument("--" + name, type=int, required=True)
    args = parser.parse_args()
    profiles = strict_json(read_bounded(args.profiles, 16384))
    registry = HourRegistry(args.registry, maximum_markets=args.maximum_registry_markets,
                            maximum_bytes=args.maximum_registry_bytes)
    report = sweep(registry, profiles, args.binary, args.binary_sha256, args.output,
                   market_ids=args.market_ids, seconds=args.seconds, maximum_markets=args.maximum_markets)
    print(canonical({"complete": report["complete"], "observations": len(report["observations"]),
                     "error_type": report.get("error_type"), "settlement_import_allowed": False}).decode())
    if not report["complete"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
