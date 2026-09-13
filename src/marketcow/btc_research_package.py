"""Build a bounded, reviewed three-hour BTC Polymarket research package.

This performs discovery and rule review only.  It does not start the Rust
stream, install a scope, activate a selection, or touch an account.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path

from .btc_discovery import discover


SCHEMA = "marketcow.btc-hour.rust-research-stream-config.v1"
RESEARCH_ENDPOINT = "http://192.168.124.3:8793"


def _canonical(value) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()


def _write_new(path: Path, raw: bytes) -> None:
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(fd, "wb") as output:
            output.write(raw)
            output.flush()
            os.fsync(output.fileno())
    except BaseException:
        path.unlink(missing_ok=True)
        raise


async def build_package(endpoint: str, output: Path, *, now: datetime) -> dict:
    if not output.is_absolute() or output.exists():
        raise ValueError("new_absolute_output_required")
    if endpoint != RESEARCH_ENDPOINT:
        raise ValueError("research_endpoint_must_be_formal_rust_live_api")
    output.mkdir(mode=0o700, parents=True)
    published = []
    try:
        rows = await discover(endpoint, now, published.append, maximum_bytes=2 * 1024 * 1024, seconds=60)
        if len(rows) != 3:
            raise ValueError("three_reviewed_hours_required")
        markets = []
        for row in rows:
            market_id = row["binding"]["market_id"]
            market_root = output / market_id
            market_root.mkdir(mode=0o700)
            evidence_raw = _canonical(row["evidence"])
            review_raw = _canonical(row["review"])
            binding_raw = _canonical(row["binding"])
            evidence_path = market_root / "evidence.json"
            review_path = market_root / "review.json"
            binding_path = market_root / "binding.json"
            _write_new(evidence_path, evidence_raw)
            _write_new(review_path, review_raw)
            _write_new(binding_path, binding_raw)
            markets.append(
                {
                    "market_id": market_id,
                    "condition_id": row["binding"]["condition_id"],
                    "token_ids": [row["binding"]["up_token"], row["binding"]["down_token"]],
                    "rule_evidence_path": str(evidence_path),
                    "rule_evidence_sha256": hashlib.sha256(evidence_raw).hexdigest(),
                    "rule_review_path": str(review_path),
                    "rule_review_sha256": hashlib.sha256(review_raw).hexdigest(),
                    "binding_path": str(binding_path),
                    "binding_sha256": hashlib.sha256(binding_raw).hexdigest(),
                }
            )
        config = {
            "schema_version": SCHEMA,
            "markets": markets,
            "seconds": 1800,
            "maximum_batches": 250_000,
            "maximum_frames": 250_000,
            "maximum_total_bytes": 512 * 1024 * 1024,
            "maximum_batch_bytes": 8 * 1024 * 1024,
            "maximum_pending_batches": 8,
            "maximum_pending_bytes": 64 * 1024 * 1024,
        }
        config_raw = _canonical(config)
        _write_new(output / "stream-config.json", config_raw)
        manifest = {
            "schema_version": "marketcow.btc-hour.research-package.v1",
            "created_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "audit_clock": now.astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
            "endpoint": endpoint,
            "market_count": 3,
            "token_count": 6,
            "stream_config_path": str(output / "stream-config.json"),
            "stream_config_sha256": hashlib.sha256(config_raw).hexdigest(),
            "observation_seconds": 1800,
            "activation_performed": False,
            "subscription_started": False,
        }
        manifest_raw = _canonical(manifest)
        _write_new(output / "manifest.json", manifest_raw)
        directory = os.open(output, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
        return manifest
    except BaseException:
        # Preserve any fetched evidence for diagnosis; absence of manifest means
        # the package is incomplete and cannot be started.
        raise


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--endpoint", required=True)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    result = asyncio.run(build_package(args.endpoint, args.output, now=datetime.now(timezone.utc)))
    print(json.dumps(result, sort_keys=True, separators=(",", ":")))


if __name__ == "__main__":
    main()
