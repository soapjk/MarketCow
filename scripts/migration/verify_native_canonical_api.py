#!/usr/bin/env python3
"""Verify Rust canonical-bar HTTP/MCP reads against real PostgreSQL and ClickHouse."""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import socket
import subprocess
import tempfile
import time
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


def free_port() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return int(probe.getsockname()[1])


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def json_request(
    url: str,
    *,
    method: str = "GET",
    payload: dict | None = None,
    bearer: str = "",
) -> tuple[int, dict]:
    data = None if payload is None else json.dumps(payload, separators=(",", ":")).encode()
    headers = {}
    if payload is not None:
        headers["Content-Type"] = "application/json"
    if bearer:
        headers["Authorization"] = f"Bearer {bearer}"
    request = Request(url, data=data, headers=headers, method=method)
    try:
        with urlopen(request, timeout=3) as response:  # noqa: S310 - loopback only
            return response.status, json.load(response)
    except HTTPError as error:
        return error.code, json.load(error)


def clickhouse_query(base_url: str, sql: str) -> str:
    credentials = base64.b64encode(
        b"marketcow_test:marketcow_test_password"
    ).decode()
    request = Request(
        f"{base_url}/?database=marketcow_test",
        data=sql.encode(),
        headers={
            "Authorization": f"Basic {credentials}",
            "Content-Type": "text/plain",
        },
        method="POST",
    )
    with urlopen(request, timeout=5) as response:  # noqa: S310 - loopback only
        return response.read().decode().strip()


def wait_for_http(process: subprocess.Popen[bytes], base_url: str) -> dict:
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline and process.poll() is None:
        try:
            status, payload = json_request(f"{base_url}/v1/health")
            if status == 200:
                return payload
        except (URLError, ConnectionError, TimeoutError):
            pass
        time.sleep(0.1)
    raise RuntimeError(f"marketcowd failed health check; exit={process.poll()}")


def stop_process(process: subprocess.Popen[bytes]) -> int:
    process.terminate()
    try:
        return process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        process.kill()
        return process.wait(timeout=5)


def insert_bar(clickhouse_url: str, minute: int, version: int) -> None:
    minute_text = f"{minute:02d}"
    sql = f"""
INSERT INTO market_bar_canonical
(symbol,market,interval,adjustment,bar_time,open,high,low,close,
 raw_close,adjustment_factor,factor_applicability,corporate_action_factor,
 applied_adjustment_multiplier,adjustment_reference_date,reference_factor,
 factor_source,factor_artifact_id,factor_as_of,volume,amount,selected_source,
 source_count,quality_status,input_fingerprint,version,observed_at,ingested_at,
 raw_artifact_id,updated_at)
SELECT 'AAPL','US','1m','raw',
 toDateTime64('2026-08-28 00:{minute_text}:00',3,'UTC'),
 10.125,10.5,10.0,10.25,NULL,NULL,'applicable',
 toDecimal128('12.345678901234567890',18),toDecimal128('1',18),NULL,NULL,
 'fixture','factor-artifact',
 toDateTime64('2026-08-28 00:{minute_text}:01',3,'UTC'),
 100.125,NULL,'fixture',1,'single_source','fingerprint-{version}',{version},
 toDateTime64('2026-08-28 00:{minute_text}:01',3,'UTC'),
 toDateTime64('2026-08-28 00:{minute_text}:02',3,'UTC'),
 'raw-artifact-{version}',
 toDateTime64('2026-08-28 00:{minute_text}:02',3,'UTC')
"""
    clickhouse_query(clickhouse_url, sql)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--expected-binary-sha256", required=True)
    parser.add_argument("--source-commit", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--clickhouse-image",
        default="clickhouse/clickhouse-server:25.8-alpine",
    )
    args = parser.parse_args()

    binary = args.binary.resolve(strict=True)
    actual_binary_sha256 = sha256_file(binary)
    source_commit = args.source_commit.lower()
    if actual_binary_sha256 != args.expected_binary_sha256.lower():
        raise SystemExit("binary SHA-256 does not match expected identity")
    if len(source_commit) != 40 or any(c not in "0123456789abcdef" for c in source_commit):
        raise SystemExit("source commit must be a full 40-character Git SHA")

    output = args.output.resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    checks: dict[str, bool] = {}
    failure = ""
    process_exit_codes: list[int] = []
    process: subprocess.Popen[bytes] | None = None
    postgres_started = False
    clickhouse_started = False
    log_tail = ""
    container_name = f"marketcow-canonical-api-{os.getpid()}"

    with tempfile.TemporaryDirectory(prefix="marketcow-native-canonical-") as temporary:
        root = Path(temporary)
        storage = root / "storage"
        storage.mkdir(mode=0o700)
        pg_data = root / "postgres"
        pg_log = root / "postgres.log"
        process_log = root / "marketcowd.log"
        pg_port = free_port()
        clickhouse_port = free_port()
        rust_port = free_port()
        dsn = f"postgresql://marketcow_test@127.0.0.1:{pg_port}/marketcow_test"
        clickhouse_url = f"http://127.0.0.1:{clickhouse_port}"
        base_url = f"http://127.0.0.1:{rust_port}"
        environment = {
            "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
            "LANG": "C.UTF-8",
            "MARKETCOW_RUST_PROFILE": "test",
            "MARKETCOW_RUST_BIND": f"127.0.0.1:{rust_port}",
            "MARKETCOW_RUST_STORAGE_ROOT": str(storage),
            "MARKETCOW_RUST_SCOPE_ID": "native-canonical-differential",
            "MARKETCOW_RUST_SHADOW": "true",
            "MARKETCOW_REAL_ORDER_SUBMISSION_ENABLED": "false",
            "MARKETCOW_POSTGRES_DSN": dsn,
            "MARKETCOW_CLICKHOUSE_HOST": "127.0.0.1",
            "MARKETCOW_CLICKHOUSE_PORT": str(clickhouse_port),
            "MARKETCOW_CLICKHOUSE_DATABASE": "marketcow_test",
            "MARKETCOW_CLICKHOUSE_USERNAME": "marketcow_test",
            "MARKETCOW_CLICKHOUSE_PASSWORD": "marketcow_test_password",
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
                [
                    "createdb", "-h", "127.0.0.1", "-p", str(pg_port),
                    "-U", "marketcow_test", "marketcow_test",
                ],
                check=True,
            )
            subprocess.run(
                [
                    "docker", "run", "--detach", "--rm", "--name", container_name,
                    "-p", f"127.0.0.1:{clickhouse_port}:8123",
                    "-e", "CLICKHOUSE_DB=marketcow_test",
                    "-e", "CLICKHOUSE_USER=marketcow_test",
                    "-e", "CLICKHOUSE_PASSWORD=marketcow_test_password",
                    "-e", "CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1",
                    args.clickhouse_image,
                ],
                check=True,
                stdout=subprocess.DEVNULL,
            )
            clickhouse_started = True
            deadline = time.monotonic() + 20
            while True:
                try:
                    if clickhouse_query(clickhouse_url, "SELECT 1") == "1":
                        break
                except (URLError, ConnectionError, TimeoutError):
                    if time.monotonic() >= deadline:
                        raise
                    time.sleep(0.2)

            log = process_log.open("ab")
            process = subprocess.Popen(
                [str(binary), "serve"],
                env=environment,
                stdout=log,
                stderr=log,
            )
            health = wait_for_http(process, base_url)
            checks["postgres_and_clickhouse_healthy"] = (
                health.get("components", {}).get("instrument_persistence") == "healthy"
                and health.get("components", {}).get("market_data_persistence") == "healthy"
            )
            checks["four_native_tools_reported"] = (
                health.get("mcp", {}).get("native_tools") == 4
            )
            checks["real_orders_disabled"] = (
                health.get("real_order_submission_enabled") is False
            )
            migration_status, migration = json_request(
                f"{base_url}/v1/admin/migration",
                bearer="local-integration-admin-token",
            )
            checks["tradude_does_not_manage_marketcow"] = (
                migration_status == 200
                and migration.get("tradude_may_manage_marketcow") is False
                and migration.get("cutover_allowed") is False
                and migration.get("real_order_submission_enabled") is False
            )

            instrument = {
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
            status, _ = json_request(
                f"{base_url}/v1/admin/instruments/AAPL.XNAS",
                method="PUT",
                payload=instrument,
                bearer="local-integration-admin-token",
            )
            checks["instrument_registered"] = status == 200
            insert_bar(clickhouse_url, 0, 1)
            insert_bar(clickhouse_url, 1, 2)

            query = {
                "start": "2026-08-28T00:00:00Z",
                "end": "2026-08-28T00:03:00Z",
                "interval": "1-MINUTE",
                "adjustment": "raw",
                "page_size": "1",
            }
            canonical_url = (
                f"{base_url}/v1/canonical-bars/aapl.xnas?{urlencode(query)}"
            )
            status, first = json_request(canonical_url)
            cursor = first.get("next_cursor", "")
            checks["first_page_exact_decimal_and_manifest"] = (
                status == 200
                and first.get("manifest", {}).get("row_count") == 2
                and first.get("count") == 1
                and first.get("bars", [{}])[0].get("instrument_id") == "AAPL.XNAS"
                and first.get("bars", [{}])[0].get("open") == "10.125"
                and first.get("bars", [{}])[0].get("corporate_action_factor")
                == "12.345678901234567890"
                and first.get("truncated") is True
                and isinstance(cursor, str)
                and bool(cursor)
            )
            status, second = json_request(f"{canonical_url}&cursor={cursor}")
            checks["signed_cursor_second_page"] = (
                status == 200
                and second.get("count") == 1
                and second.get("bars", [{}])[0].get("row_version") == "2"
                and second.get("truncated") is False
            )
            tampered = ("A" if not cursor.startswith("A") else "B") + cursor[1:]
            status, rejected = json_request(f"{canonical_url}&cursor={tampered}")
            checks["tampered_cursor_rejected"] = (
                status == 400
                and rejected.get("detail", {}).get("code") == "invalid_canonical_query"
            )
            oversized_query = dict(query, page_size="1001")
            status, _ = json_request(
                f"{base_url}/v1/canonical-bars/AAPL.XNAS?{urlencode(oversized_query)}"
            )
            checks["http_page_limit_matches_contract"] = status == 400

            full_query = dict(query, page_size="2")
            full_url = (
                f"{base_url}/v1/canonical-bars/AAPL.XNAS?{urlencode(full_query)}"
            )
            status, http_full = json_request(full_url)
            mcp_request = {
                "jsonrpc": "2.0",
                "id": 9,
                "method": "tools/call",
                "params": {
                    "name": "get_canonical_bars",
                    "arguments": {
                        "instrument_id": "AAPL.XNAS",
                        "start": query["start"],
                        "end": query["end"],
                        "interval": query["interval"],
                        "adjustment": query["adjustment"],
                        "page_size": 2,
                    },
                },
            }
            mcp_status, mcp = json_request(
                f"{base_url}/mcp", method="POST", payload=mcp_request
            )
            checks["http_and_mcp_are_identical"] = (
                status == 200
                and mcp_status == 200
                and mcp.get("result", {}).get("isError") is False
                and mcp.get("result", {}).get("structuredContent") == http_full
            )

            list_status, listed = json_request(
                f"{base_url}/mcp",
                method="POST",
                payload={
                    "jsonrpc": "2.0", "id": 10, "method": "tools/list", "params": {},
                },
            )
            fixture = json.loads(
                (
                    Path(__file__).resolve().parents[2]
                    / "tests/fixtures/mcp-get-canonical-bars-tool-v1.json"
                ).read_text(encoding="utf-8")
            )
            checks["mcp_tool_matches_golden"] = (
                list_status == 200
                and listed.get("result", {}).get("tools", [])[-1] == fixture
            )

            insert_bar(clickhouse_url, 2, 3)
            status, invalidated = json_request(f"{canonical_url}&cursor={cursor}")
            checks["cursor_is_snapshot_bound"] = (
                status == 400
                and invalidated.get("detail", {}).get("code")
                == "invalid_canonical_query"
            )

            before_restart_status, before_restart = json_request(
                f"{base_url}/v1/canonical-bars/AAPL.XNAS?"
                + urlencode(dict(query, page_size="3"))
            )
            process_exit_codes.append(stop_process(process))
            process = subprocess.Popen(
                [str(binary), "serve"],
                env=environment,
                stdout=log,
                stderr=log,
            )
            restart_health = wait_for_http(process, base_url)
            after_restart_status, after_restart = json_request(
                f"{base_url}/v1/canonical-bars/AAPL.XNAS?"
                + urlencode(dict(query, page_size="3"))
            )
            checks["restart_recovers_exact_clickhouse_snapshot"] = (
                restart_health.get("status") == "healthy"
                and before_restart_status == 200
                and after_restart_status == 200
                and before_restart == after_restart
                and after_restart.get("manifest", {}).get("row_count") == 3
            )
            checks["safe_forward_migrations_recorded_once"] = (
                clickhouse_query(
                    clickhouse_url,
                    "SELECT count() FROM marketcow_rust_clickhouse_migration FINAL "
                    "WHERE version IN ('rust-quote-read-v1',"
                    "'rust-canonical-adjustment-read-v1')",
                )
                == "2"
            )
            process_exit_codes.append(stop_process(process))
            process = None
            log.close()
        except Exception as error:  # Preserve partial gates and diagnostics.
            failure = f"{type(error).__name__}: {error}"
        finally:
            if process is not None:
                process_exit_codes.append(stop_process(process))
            if process_log.exists():
                log_tail = "\n".join(
                    process_log.read_text(encoding="utf-8", errors="replace").splitlines()[-80:]
                )
            if clickhouse_started:
                subprocess.run(
                    ["docker", "stop", container_name],
                    check=False,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )
            if postgres_started:
                subprocess.run(
                    ["pg_ctl", "-D", str(pg_data), "stop"],
                    check=False,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )

    passed = bool(checks) and all(checks.values()) and not failure
    image_id = subprocess.run(
        ["docker", "image", "inspect", "--format", "{{.Id}}", args.clickhouse_image],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    result = {
        "schema_version": "marketcow.native-canonical-api.v1",
        "source_commit": source_commit,
        "binary": str(binary),
        "binary_sha256": actual_binary_sha256,
        "passed": passed,
        "checks": checks,
        "process_exit_codes": process_exit_codes,
        "clickhouse": {"image": args.clickhouse_image, "image_id": image_id},
        "failure": failure or None,
        "process_log_tail": log_tail,
        "safety": {
            "real_order_submission_enabled": False,
            "tradude_manages_marketcow": False,
            "headless_substitutes_http_network_soak": False,
        },
    }
    temporary_output = output.with_suffix(output.suffix + ".tmp")
    temporary_output.write_text(
        json.dumps(result, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    temporary_output.replace(output)
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
