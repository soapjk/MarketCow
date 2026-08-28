#!/usr/bin/env python3
"""Read-only WAL verification plus bounded daemon restart against an isolated storage copy."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import tempfile
import time
from urllib.error import HTTPError, URLError
from urllib.request import urlopen


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def request_json(url: str) -> tuple[int, dict]:
    try:
        with urlopen(url, timeout=1) as response:  # noqa: S310 - loopback URL is constructed here
            return response.status, json.load(response)
    except HTTPError as error:
        return error.code, json.load(error)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--storage-root", type=Path, required=True)
    parser.add_argument("--expected-binary-sha256", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    binary = args.binary.resolve(strict=True)
    storage_root = args.storage_root.resolve(strict=True)
    if not storage_root.is_dir() or storage_root == Path("/"):
        raise SystemExit("storage root must be a narrow existing directory")
    actual_binary_sha256 = sha256(binary)
    if actual_binary_sha256 != args.expected_binary_sha256.lower():
        raise SystemExit("binary SHA-256 does not match soak launch identity")

    manifest = json.loads(
        (storage_root / "polymarket" / "checkpoint-manifest.json").read_text()
    )
    wal = subprocess.run(
        [str(binary), "wal", "verify", str(storage_root / "polymarket" / "wal")],
        check=True,
        capture_output=True,
        text=True,
    )
    wal_result = json.loads(wal.stdout)
    with tempfile.TemporaryDirectory(prefix="marketcow-soak-recovery-") as temporary:
        recovery_root = Path(temporary) / "storage"
        shutil.copytree(storage_root, recovery_root)
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", 0))
            port = probe.getsockname()[1]
        environment = {
            **os.environ,
            "MARKETCOW_RUST_PROFILE": "test",
            "MARKETCOW_RUST_BIND": f"127.0.0.1:{port}",
            "MARKETCOW_RUST_STORAGE_ROOT": str(recovery_root),
            "MARKETCOW_RUST_SCOPE_ID": manifest["scope_id"],
            "MARKETCOW_RUST_SHADOW": "true",
            "MARKETCOW_RUST_MAX_BOOK_AGE_MS": "86400000",
            "MARKETCOW_REAL_ORDER_SUBMISSION_ENABLED": "false",
        }
        process = subprocess.Popen(
            [str(binary), "serve"],
            env=environment,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True,
        )
        status = None
        response = None
        started = time.monotonic()
        try:
            while time.monotonic() - started < 10:
                if process.poll() is not None:
                    break
                try:
                    status, response = request_json(f"http://127.0.0.1:{port}/v1/readiness")
                    if status == 200:
                        break
                except (URLError, ConnectionError, TimeoutError):
                    pass
                time.sleep(0.05)
            if status != 200:
                raise RuntimeError(f"recovered daemon did not become ready: status={status}")
            snapshot_status, snapshot = request_json(
                f"http://127.0.0.1:{port}/v1/prediction-markets/polymarket/live/full-sync"
            )
            if snapshot_status != 200:
                raise RuntimeError(f"full-sync failed: status={snapshot_status}")
        finally:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
        stderr_tail = (process.stderr.read() if process.stderr else "")[-4000:]

    passed = (
        wal_result.get("status") == "ok"
        and wal_result.get("last_cursor") == manifest["wal_last_cursor"]
        and response is not None
        and response.get("ready") is True
        and snapshot["boundary_cursor"] == manifest["wal_last_cursor"]
        and snapshot["snapshot"]["watermarks"]["published_cursor"]
        == snapshot["snapshot"]["watermarks"]["persisted_cursor"]
        and snapshot["health"]["unresolved_gap_count"] == 0
    )
    result = {
        "schema_version": "marketcow.soak-recovery-check.v1",
        "passed": passed,
        "source_storage_root": str(storage_root),
        "recovery_used_isolated_copy": True,
        "binary_sha256": actual_binary_sha256,
        "scope_id": manifest["scope_id"],
        "manifest_cursor": manifest["wal_last_cursor"],
        "wal": wal_result,
        "readiness": response,
        "full_sync": {
            "boundary_cursor": snapshot["boundary_cursor"],
            "published_cursor": snapshot["snapshot"]["watermarks"]["published_cursor"],
            "persisted_cursor": snapshot["snapshot"]["watermarks"]["persisted_cursor"],
            "book_count": snapshot["health"]["book_count"],
            "unresolved_gap_count": snapshot["health"]["unresolved_gap_count"],
        },
        "process_exit_code": process.returncode,
        "process_stderr_tail": stderr_tail,
        "real_order_submission_enabled": False,
        "tradude_manages_marketcow": False,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
