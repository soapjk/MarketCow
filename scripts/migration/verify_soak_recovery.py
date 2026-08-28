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

    checks = {
        "checkpoint_manifest_loaded": False,
        "wal_verified": False,
        "wal_cursor_matches_manifest": False,
        "recovered_daemon_ready": False,
        "full_sync_available": False,
        "full_sync_cursor_matches_manifest": False,
        "published_cursor_equals_persisted_cursor": False,
        "unresolved_gap_count_zero": False,
        "real_orders_disabled": False,
        "tradude_does_not_manage_marketcow": True,
    }
    manifest: dict = {}
    wal_result: dict = {}
    response: dict = {}
    snapshot: dict = {}
    failure: str | None = None
    process_exit_code: int | None = None
    stderr_tail = ""
    try:
        manifest = json.loads(
            (storage_root / "polymarket" / "checkpoint-manifest.json").read_text()
        )
        checks["checkpoint_manifest_loaded"] = (
            isinstance(manifest.get("scope_id"), str)
            and isinstance(manifest.get("wal_last_cursor"), int)
        )
        if not checks["checkpoint_manifest_loaded"]:
            raise RuntimeError("checkpoint manifest contract is invalid")
        wal = subprocess.run(
            [str(binary), "wal", "verify", str(storage_root / "polymarket" / "wal")],
            check=False,
            capture_output=True,
            text=True,
        )
        if wal.returncode == 0:
            wal_result = json.loads(wal.stdout)
        else:
            wal_result = {"status": "error", "stderr": wal.stderr[-4000:]}
        checks["wal_verified"] = wal_result.get("status") == "ok"
        checks["wal_cursor_matches_manifest"] = (
            wal_result.get("last_cursor") == manifest["wal_last_cursor"]
        )
        if not checks["wal_verified"]:
            raise RuntimeError("WAL verification failed")

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
            started = time.monotonic()
            try:
                while time.monotonic() - started < 10:
                    if process.poll() is not None:
                        break
                    try:
                        status, response = request_json(
                            f"http://127.0.0.1:{port}/v1/readiness"
                        )
                        if status == 200:
                            break
                    except (URLError, ConnectionError, TimeoutError):
                        pass
                    time.sleep(0.05)
                checks["recovered_daemon_ready"] = (
                    status == 200 and response.get("ready") is True
                )
                checks["real_orders_disabled"] = (
                    response.get("real_order_submission_enabled") is False
                )
                if not checks["recovered_daemon_ready"]:
                    raise RuntimeError(
                        f"recovered daemon did not become ready: status={status}"
                    )
                snapshot_status, snapshot = request_json(
                    f"http://127.0.0.1:{port}"
                    "/v1/prediction-markets/polymarket/live/full-sync"
                )
                checks["full_sync_available"] = snapshot_status == 200
                if not checks["full_sync_available"]:
                    raise RuntimeError(f"full-sync failed: status={snapshot_status}")
                checks["full_sync_cursor_matches_manifest"] = (
                    snapshot.get("boundary_cursor") == manifest["wal_last_cursor"]
                )
                watermarks = snapshot.get("snapshot", {}).get("watermarks", {})
                checks["published_cursor_equals_persisted_cursor"] = (
                    watermarks.get("published_cursor")
                    == watermarks.get("persisted_cursor")
                )
                checks["unresolved_gap_count_zero"] = (
                    snapshot.get("health", {}).get("unresolved_gap_count") == 0
                )
            finally:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)
                process_exit_code = process.returncode
                stderr_tail = (
                    process.stderr.read() if process.stderr else ""
                )[-4000:]
    except Exception as error:  # Preserve every partial gate and terminal diagnostic.
        failure = f"{type(error).__name__}: {error}"

    passed = all(checks.values()) and failure is None
    watermarks = snapshot.get("snapshot", {}).get("watermarks", {})
    health = snapshot.get("health", {})
    result = {
        "schema_version": "marketcow.soak-recovery-check.v2",
        "passed": passed,
        "checks": checks,
        "failure": failure,
        "source_storage_root": str(storage_root),
        "recovery_used_isolated_copy": True,
        "binary_sha256": actual_binary_sha256,
        "scope_id": manifest.get("scope_id"),
        "manifest_cursor": manifest.get("wal_last_cursor"),
        "wal": wal_result,
        "readiness": response,
        "full_sync": {
            "boundary_cursor": snapshot.get("boundary_cursor"),
            "published_cursor": watermarks.get("published_cursor"),
            "persisted_cursor": watermarks.get("persisted_cursor"),
            "book_count": health.get("book_count"),
            "unresolved_gap_count": health.get("unresolved_gap_count"),
        },
        "process_exit_code": process_exit_code,
        "process_stderr_tail": stderr_tail,
        "real_order_submission_enabled": False,
        "tradude_manages_marketcow": False,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    temporary_output = args.output.with_suffix(args.output.suffix + ".tmp")
    temporary_output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    temporary_output.replace(args.output)
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
