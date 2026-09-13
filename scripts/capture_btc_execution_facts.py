"""Capture bounded BTC-hour CLOB execution facts through the existing U1 proxy."""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile
import time
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

MARKET_CAP = 262_144
DOC_TOTAL_CAP = 2_097_152
DOC_URLS = (
    "https://docs.polymarket.com/api-reference/markets/get-clob-market-info",
    "https://docs.polymarket.com/trading/fees",
    "https://docs.polymarket.com/v2-migration",
)


def utc_now() -> datetime:
    return datetime.now(UTC)


def iso_utc(value: datetime) -> str:
    return value.isoformat(timespec="microseconds").replace("+00:00", "Z")


def sha256(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def atomic_write(path: Path, body: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        raise FileExistsError(path)
    descriptor, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(descriptor, "wb") as output:
            output.write(body)
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    except BaseException:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass
        raise


def atomic_json(path: Path, value: Any) -> None:
    atomic_write(path, json.dumps(value, sort_keys=True, separators=(",", ":")).encode() + b"\n")


def ssh_get(host: str, url: str, cap: int) -> tuple[bytes, str]:
    command = (
        "HTTPS_PROXY=http://127.0.0.1:17890 "
        "HTTP_PROXY=http://127.0.0.1:17890 "
        "curl --silent --show-error --fail --proto '=https' --max-time 15 "
        f"--max-filesize {cap} --retry 0 --url {json.dumps(url)}"
    )
    result = subprocess.run(
        ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=5", host, command],
        capture_output=True,
        check=False,
        timeout=20,
    )
    received_at = iso_utc(utc_now())
    if result.returncode != 0:
        message = result.stderr.decode(errors="replace")[-4096:]
        raise RuntimeError(f"upstream_get_failed:{result.returncode}:{message}")
    if len(result.stdout) > cap:
        raise RuntimeError(f"response_size_exceeded:{len(result.stdout)}:{cap}")
    return result.stdout, received_at


def decimal(value: Any, name: str, *, allow_zero: bool) -> str:
    parsed = Decimal(str(value))
    if parsed < 0 or (not allow_zero and parsed == 0):
        raise ValueError(f"{name}_invalid")
    return format(parsed.normalize(), "f")


def project(market: dict[str, Any], raw: bytes, received_at: str, url: str) -> dict[str, Any]:
    source = json.loads(raw)
    condition = market["condition_id"]
    tokens = market["token_ids"]
    expected = [{"o": "Up", "t": tokens[0]}, {"o": "Down", "t": tokens[1]}]
    if source.get("c") != condition:
        raise ValueError("condition_identity_mismatch")
    actual = [{"o": item.get("o"), "t": item.get("t")} for item in source.get("t", [])]
    if actual != expected:
        raise ValueError("ordered_token_identity_mismatch")
    rate = decimal(source.get("fd", {}).get("r"), "fee_rate", allow_zero=True)
    exponent = source.get("fd", {}).get("e")
    if not isinstance(exponent, int) or isinstance(exponent, bool) or exponent <= 0:
        raise ValueError("fee_exponent_invalid")
    if source.get("fd", {}).get("to") is not True:
        raise ValueError("unsupported_fee_curve")
    digest = sha256(raw)
    return {
        "schema_version": "marketcow.polymarket.market-execution-facts.v1",
        "market_id": market["market_id"],
        "condition_id": condition,
        "outcomes": [
            {"outcome": "Up", "token_id": tokens[0]},
            {"outcome": "Down", "token_id": tokens[1]},
        ],
        "source": "polymarket_clob_v2",
        "source_url": url,
        "observed_at": received_at,
        "raw_complete": True,
        "raw_bytes": len(raw),
        "raw_sha256": digest,
        "raw_base64": base64.b64encode(raw).decode(),
        "instrument": {
            "price_increment": decimal(source.get("mts"), "price_increment", allow_zero=False),
            "price_unit": "probability",
            "minimum_order_size": decimal(source.get("mos"), "minimum_order_size", allow_zero=False),
            "size_unit": "shares",
            "size_increment": None,
            "version_sha256": digest,
            "complete": False,
            "missing_fields": ["size_increment"],
        },
        "fee_schedule": {
            "model": "clob_v2_dynamic",
            "currency": "USDC",
            "maker_rate": "0",
            "taker_rate": rate,
            "formula": "fee = shares * rate * (price * (1 - price)) ^ exponent",
            "exponent": exponent,
            "taker_only": True,
            "quantum": "0.00001",
            "rounding_decimal_places": 5,
            "rounding_mode": None,
            "effective_at": None,
            "maker_base_fee_bps": decimal(source.get("mbf"), "maker_base_fee", allow_zero=True),
            "taker_base_fee_bps": decimal(source.get("tbf"), "taker_base_fee", allow_zero=True),
            "applicable_parameter_source": "fd",
            "base_fee_fields_applicability": "preserved_source_fields_not_used_for_clob_v2_dynamic_fee_calculation",
            "applicability_basis": "CLOB V2 determines fees at match time and directs clients to fd.r/fd.e/fd.to",
            "version_sha256": digest,
            "complete": False,
            "missing_fields": ["rounding_mode", "effective_at"],
        },
        "paper_assumption": {
            "schema_version": "marketcow.polymarket.paper-execution-assumption.v1",
            "fee_currency_conversion": "1 pUSD = 1 USDC face value",
            "fee_rounding_mode": "ROUND_UP",
            "size_increment": "0.000001",
            "purpose": "conservative_simulation_only",
            "not_source_facts": ["fee_currency_conversion", "fee_rounding_mode", "size_increment"],
        },
        "execution_eligible": False,
        "paper_simulation_eligible_with_explicit_assumption": True,
    }


def load_package(path: Path, now: datetime) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    manifest_raw = (path / "manifest.json").read_bytes()
    stream_raw = (path / "stream-config.json").read_bytes()
    manifest = json.loads(manifest_raw)
    stream = json.loads(stream_raw)
    if manifest.get("schema_version") != "marketcow.btc-hour.research-package.v1":
        raise ValueError("package_schema_mismatch")
    audit = datetime.fromisoformat(manifest["audit_clock"].replace("Z", "+00:00"))
    if now.replace(minute=0, second=0, microsecond=0) != audit.replace(minute=0, second=0, microsecond=0):
        raise ValueError("package_utc_hour_expired")
    if sha256(stream_raw) != manifest["stream_config_sha256"]:
        raise ValueError("stream_config_hash_mismatch")
    markets = stream.get("markets")
    if not isinstance(markets, list) or not 1 <= len(markets) <= 3:
        raise ValueError("market_count_out_of_bounds")
    return {
        "path": str(path.resolve()),
        "manifest_sha256": sha256(manifest_raw),
        "stream_config_sha256": sha256(stream_raw),
        "audit_clock": manifest["audit_clock"],
    }, markets


def run(args: argparse.Namespace) -> None:
    started = utc_now()
    package, markets = load_package(args.package, started)
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    captures: list[dict[str, Any]] = []
    try:
        for index, market in enumerate(markets):
            if index:
                time.sleep(1.1)
            market_id = market["market_id"]
            url = f"https://clob.polymarket.com/clob-markets/{market['condition_id']}"
            raw, received_at = ssh_get(args.ssh_host, url, MARKET_CAP)
            raw_path = output / "markets" / market_id / "raw.json"
            atomic_write(raw_path, raw)
            projection = project(market, raw, received_at, url)
            projection_path = output / "markets" / market_id / "projection.json"
            atomic_json(projection_path, projection)
            captures.append({
                "market_id": market_id,
                "condition_id": market["condition_id"],
                "received_at": received_at,
                "source_url": url,
                "raw_path": str(raw_path),
                "raw_bytes": len(raw),
                "raw_sha256": sha256(raw),
                "projection_path": str(projection_path),
                "projection_sha256": sha256(projection_path.read_bytes()),
            })

        documents: list[dict[str, Any]] = []
        total = 0
        for index, url in enumerate(DOC_URLS):
            if captures or index:
                time.sleep(1.1)
            raw, received_at = ssh_get(args.ssh_host, url, DOC_TOTAL_CAP - total)
            total += len(raw)
            if total > DOC_TOTAL_CAP:
                raise RuntimeError("documentation_total_size_exceeded")
            path = output / "documentation" / f"{index + 1}.html"
            atomic_write(path, raw)
            documents.append({
                "url": url,
                "received_at": received_at,
                "path": str(path),
                "bytes": len(raw),
                "sha256": sha256(raw),
            })

        manifest = {
            "schema_version": "marketcow.polymarket.btc-execution-capture.v1",
            "status": "complete",
            "started_at": iso_utc(started),
            "completed_at": iso_utc(utc_now()),
            "package": package,
            "limits": {
                "maximum_markets": 3,
                "maximum_market_response_bytes": MARKET_CAP,
                "request_timeout_seconds": 15,
                "minimum_interval_seconds": 1.0,
                "retries": 0,
                "redirects": 0,
                "maximum_documentation_bytes": DOC_TOTAL_CAP,
            },
            "markets": captures,
            "documentation": documents,
        }
        atomic_json(output / "manifest.json", manifest)
    except BaseException as error:
        atomic_json(output / "rejection.json", {
            "schema_version": "marketcow.polymarket.btc-execution-capture-rejection.v1",
            "failed_at": iso_utc(utc_now()),
            "error_type": type(error).__name__,
            "error": str(error),
            "completed_markets": captures,
        })
        raise


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--package", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--ssh-host", default="czx@u1")
    run(parser.parse_args())


if __name__ == "__main__":
    main()
