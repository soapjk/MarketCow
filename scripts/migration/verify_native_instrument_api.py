#!/usr/bin/env python3
"""Verify the Rust Instrument HTTP/MCP boundary against a real PostgreSQL process."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import socket
import subprocess
import tempfile
import time
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import httpx

from marketcow.market_data_contracts import canonical_hash
from marketcow.mcp_server import MarketCowClient, McpServer, create_tools


def free_port() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return int(probe.getsockname()[1])


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def get_json(url: str) -> tuple[int, dict]:
    try:
        with urlopen(url, timeout=2) as response:  # noqa: S310 - loopback URL only
            return response.status, json.load(response)
    except HTTPError as error:
        return error.code, json.load(error)


def post_json(url: str, payload: dict) -> tuple[int, dict]:
    body = json.dumps(payload, separators=(",", ":")).encode()
    request = Request(
        url,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urlopen(request, timeout=2) as response:  # noqa: S310 - loopback URL only
            return response.status, json.load(response)
    except HTTPError as error:
        return error.code, json.load(error)


def put_json(url: str, payload: dict, bearer_token: str = "") -> tuple[int, dict]:
    body = json.dumps(payload, separators=(",", ":")).encode()
    headers = {"Content-Type": "application/json"}
    if bearer_token:
        headers["Authorization"] = f"Bearer {bearer_token}"
    request = Request(url, data=body, headers=headers, method="PUT")
    try:
        with urlopen(request, timeout=2) as response:  # noqa: S310 - loopback URL only
            return response.status, json.load(response)
    except HTTPError as error:
        return error.code, json.load(error)


def wait_for_health(process: subprocess.Popen[str], base_url: str) -> dict:
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline and process.poll() is None:
        try:
            status, payload = get_json(f"{base_url}/v1/health")
            if status == 200:
                return payload
        except (URLError, ConnectionError, TimeoutError):
            pass
        time.sleep(0.05)
    raise RuntimeError(f"marketcowd did not become healthy; exit={process.poll()}")


def stop_process(process: subprocess.Popen[str]) -> int:
    process.terminate()
    try:
        return process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()
        return process.wait(timeout=5)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--expected-binary-sha256", required=True)
    parser.add_argument("--source-commit", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    binary = args.binary.resolve(strict=True)
    binary_sha256 = file_sha256(binary)
    if binary_sha256 != args.expected_binary_sha256.lower():
        raise SystemExit("binary SHA-256 does not match expected identity")
    source_commit = args.source_commit.lower()
    if len(source_commit) != 40 or any(c not in "0123456789abcdef" for c in source_commit):
        raise SystemExit("source commit must be a full 40-character Git SHA")

    instrument_input = {
        "schema_version": 1,
        "instrument_id": "AAPL.XNAS",
        "instrument_type": "equity",
        "asset_class": "equity",
        "symbol": "AAPL",
        "market": "US",
        "mic": "XNAS",
        "currency": "USD",
        "price_precision": 4,
        "size_precision": 8,
        "tick_size": "0.0100",
        "size_increment": "0.00000001",
        "lot_size": "1",
        "ts_event": "2026-08-28T00:00:00Z",
        "ts_init": "2026-08-28T00:00:01Z",
        "provider_symbols": {"longport": "AAPL.US"},
        "broker_symbols": {"ibkr": "AAPL"},
    }
    expected: dict = {}
    checks: dict[str, bool] = {}
    failure = ""
    process_exit_codes: list[int] = []
    process: subprocess.Popen[str] | None = None
    postgres_started = False
    log_tail = ""
    expected_mcp_call: dict | None = None
    actual_mcp_call: dict | None = None

    with tempfile.TemporaryDirectory(prefix="marketcow-native-instrument-") as temporary:
        root = Path(temporary)
        pg_data = root / "postgres"
        storage = root / "storage"
        storage.mkdir(mode=0o700)
        pg_port = free_port()
        rust_port = free_port()
        pg_log = root / "postgres.log"
        process_log = root / "marketcowd.log"
        dsn = f"postgresql://marketcow_test@127.0.0.1:{pg_port}/marketcow_test"
        base_url = f"http://127.0.0.1:{rust_port}"
        environment = {
            "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
            "LANG": "C.UTF-8",
            "MARKETCOW_RUST_PROFILE": "test",
            "MARKETCOW_RUST_BIND": f"127.0.0.1:{rust_port}",
            "MARKETCOW_RUST_STORAGE_ROOT": str(storage),
            "MARKETCOW_RUST_SCOPE_ID": "native-instrument-differential",
            "MARKETCOW_RUST_SHADOW": "true",
            "MARKETCOW_REAL_ORDER_SUBMISSION_ENABLED": "false",
            "MARKETCOW_POSTGRES_DSN": dsn,
            "MARKETCOW_BINARY_COMMIT": source_commit,
            "MARKETCOW_RUST_ADMIN_TOKEN": "local-integration-admin-token",
        }
        try:
            subprocess.run(
                ["initdb", "-D", str(pg_data), "--auth=trust", "--username=marketcow_test"],
                check=True,
                stdout=subprocess.DEVNULL,
            )
            subprocess.run(
                [
                    "pg_ctl", "-D", str(pg_data), "-l", str(pg_log),
                    "-o", f"-h 127.0.0.1 -p {pg_port} -k {root}", "start",
                ],
                check=True,
                stdout=subprocess.DEVNULL,
            )
            postgres_started = True
            subprocess.run(
                ["createdb", "-h", "127.0.0.1", "-p", str(pg_port),
                 "-U", "marketcow_test", "marketcow_test"],
                check=True,
            )
            with process_log.open("w", encoding="utf-8") as log:
                process = subprocess.Popen(
                    [str(binary), "serve"],
                    env=environment,
                    stdout=log,
                    stderr=log,
                    text=True,
                )
                health = wait_for_health(process, base_url)
                checks["instrument_persistence_healthy"] = (
                    health.get("components", {}).get("instrument_persistence") == "healthy"
                )
                checks["control_plane_persistence_healthy"] = (
                    health.get("components", {}).get("control_plane_persistence")
                    == "healthy"
                )
                checks["config_revision_is_sha256"] = (
                    isinstance(health.get("config_revision"), str)
                    and health["config_revision"].startswith("sha256:")
                    and len(health["config_revision"]) == 71
                )
                checks["real_orders_disabled"] = (
                    health.get("real_order_submission_enabled") is False
                )
                checks["two_native_tools_reported"] = (
                    health.get("mcp", {}).get("native_tools") == 2
                )

                admin_url = f"{base_url}/v1/admin/instruments/AAPL.XNAS"
                unauthorized_status, unauthorized = put_json(
                    admin_url, instrument_input
                )
                checks["admin_put_requires_bearer"] = (
                    unauthorized_status == 401
                    and unauthorized.get("detail", {}).get("code")
                    == "authentication_required"
                )
                admin_status, expected = put_json(
                    admin_url, instrument_input, "local-integration-admin-token"
                )
                checks["admin_put_200"] = admin_status == 200
                checks["admin_put_exact_contract_and_hash"] = (
                    all(expected.get(key) == value for key, value in instrument_input.items())
                    and expected.get("content_hash") == canonical_hash(instrument_input)
                    and str(expected.get("updated_at", "")).endswith("Z")
                )

                status, actual = get_json(f"{base_url}/v1/instruments/AAPL.XNAS")
                checks["http_get_200"] = status == 200
                checks["http_exact_record"] = actual == expected
                status, missing = get_json(f"{base_url}/v1/instruments/MSFT.XNAS")
                checks["http_missing_404_machine_detail"] = (
                    status == 404
                    and missing == {"detail": {
                        "code": "instrument_not_found",
                        "instrument_id": "MSFT.XNAS",
                    }}
                )
                resolve_url = (
                    f"{base_url}/v1/instruments:resolve"
                    "?namespace=provider%3Alongport&external_symbol=AAPL.US"
                )
                status, resolved = get_json(resolve_url)
                checks["http_mapping_resolve_exact_record"] = (
                    status == 200 and resolved == expected
                )
                missing_resolve_url = (
                    f"{base_url}/v1/instruments:resolve"
                    "?namespace=provider%3Alongport&external_symbol=MSFT.US"
                )
                status, unresolved = get_json(missing_resolve_url)
                checks["http_mapping_missing_404_machine_detail"] = (
                    status == 404
                    and unresolved == {"detail": {
                        "code": "instrument_mapping_not_found",
                        "namespace": "provider:longport",
                        "external_symbol": "MSFT.US",
                    }}
                )

                list_request = {
                    "jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {},
                }
                list_status, listed = post_json(f"{base_url}/mcp", list_request)
                python_client = MarketCowClient(
                    transport=httpx.MockTransport(
                        lambda _request: httpx.Response(200, json=expected)
                    )
                )
                python_mcp = McpServer(python_client)
                python_definition = create_tools(python_client)["get_instrument"].definition()
                checks["mcp_list_http_200"] = list_status == 200
                checks["mcp_get_definition_matches_python"] = (
                    listed.get("result", {}).get("tools", [None, None])[-1]
                    == python_definition
                )
                call_request = {
                    "jsonrpc": "2.0",
                    "id": 2,
                    "method": "tools/call",
                    "params": {
                        "name": "get_instrument",
                        "arguments": {"instrument_id": "AAPL.XNAS"},
                    },
                }
                expected_mcp_call = python_mcp.handle_message(call_request)
                call_status, actual_mcp_call = post_json(f"{base_url}/mcp", call_request)
                checks["mcp_call_http_200"] = call_status == 200
                checks["mcp_result_matches_python"] = actual_mcp_call == expected_mcp_call
                python_client.close()

                process_exit_codes.append(stop_process(process))
                process = subprocess.Popen(
                    [str(binary), "serve"],
                    env=environment,
                    stdout=log,
                    stderr=log,
                    text=True,
                )
                restart_health = wait_for_health(process, base_url)
                status, restarted = get_json(f"{base_url}/v1/instruments/AAPL.XNAS")
                resolve_status, restarted_resolve = get_json(resolve_url)
                checks["restart_health_200"] = restart_health.get("status") == "healthy"
                checks["restart_reads_same_postgres_record"] = status == 200 and restarted == expected
                checks["restart_resolves_same_mapping"] = (
                    resolve_status == 200 and restarted_resolve == expected
                )
                process_exit_codes.append(stop_process(process))
                process = None

            migration_count = subprocess.run(
                [
                    "psql", dsn, "-At", "-v", "ON_ERROR_STOP=1", "-c",
                    "SELECT COUNT(*) FROM marketcow_rust_migration "
                    "WHERE version IN ('rust-provider-job-v1',"
                    "'rust-artifact-manifest-v1','rust-instrument-master-v1',"
                    "'rust-control-plane-v1')",
                ],
                check=True,
                capture_output=True,
                text=True,
            ).stdout.strip()
            checks["all_four_rust_migrations_recorded_once"] = migration_count == "4"
            config_count = subprocess.run(
                [
                    "psql", dsn, "-At", "-v", "ON_ERROR_STOP=1", "-c",
                    "SELECT COUNT(*) FROM runtime_config_version "
                    "WHERE config_id='marketcowd-runtime'",
                ],
                check=True,
                capture_output=True,
                text=True,
            ).stdout.strip()
            checks["restart_does_not_duplicate_runtime_config"] = config_count == "1"
        except Exception as error:  # preserve a bounded diagnostic in the result
            failure = f"{type(error).__name__}: {error}"
        finally:
            if process is not None and process.poll() is None:
                process_exit_codes.append(stop_process(process))
            if process_log.exists():
                log_tail = process_log.read_text(encoding="utf-8", errors="replace")[-4000:]
            if postgres_started:
                subprocess.run(
                    ["pg_ctl", "-D", str(pg_data), "stop"],
                    check=False,
                    stdout=subprocess.DEVNULL,
                )

    passed = bool(checks) and all(checks.values()) and not failure
    result = {
        "schema_version": "marketcow.native-instrument-differential.v1",
        "passed": passed,
        "binary_path": str(binary),
        "binary_sha256": binary_sha256,
        "source_commit": source_commit,
        "checks": checks,
        "failure": failure or None,
        "process_exit_codes": process_exit_codes,
        "process_log_tail": log_tail,
        "mcp_expected_on_failure": (
            expected_mcp_call if checks.get("mcp_result_matches_python") is False else None
        ),
        "mcp_actual_on_failure": (
            actual_mcp_call if checks.get("mcp_result_matches_python") is False else None
        ),
        "real_order_submission_enabled": False,
        "tradude_manages_marketcow": False,
        "headless_substitutes_http_network_soak": False,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
