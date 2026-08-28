#!/usr/bin/env python3
"""Cross-process golden verification for Rust-owned MCP and a loopback Python proxy."""

from __future__ import annotations

import argparse
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import socket
import subprocess
import tempfile
import threading
import time
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import httpx

from marketcow.mcp_server import MarketCowClient, McpServer


MAX_BODY_BYTES = 1_048_576


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


def post_json(url: str, payload: dict) -> tuple[int, dict]:
    body = json.dumps(payload, separators=(",", ":")).encode()
    request = Request(
        url,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urlopen(request, timeout=2) as response:  # noqa: S310 - caller constructs loopback URL
            return response.status, json.load(response)
    except HTTPError as error:
        return error.code, json.load(error)


def get_json(url: str) -> tuple[int, dict]:
    try:
        with urlopen(url, timeout=1) as response:  # noqa: S310 - caller constructs loopback URL
            return response.status, json.load(response)
    except HTTPError as error:
        return error.code, json.load(error)


def build_legacy_server() -> tuple[ThreadingHTTPServer, McpServer]:
    def upstream(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/v1/quotes/query":
            payload = json.loads(request.content)
            return httpx.Response(
                200,
                json={
                    "count": len(payload["symbols"]),
                    "items": [
                        {
                            "symbol": symbol,
                            "last": "213.880000",
                            "currency": "USD",
                            "observed_at": "2026-08-28T00:00:00Z",
                            "source": "fixture://mcp-proxy",
                        }
                        for symbol in payload["symbols"]
                    ],
                },
            )
        return httpx.Response(200, json={"status": "ok"})

    client = MarketCowClient(transport=httpx.MockTransport(upstream))
    mcp = McpServer(client)

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:  # noqa: N802 - stdlib handler API
            if self.path != "/mcp":
                self.send_error(404)
                return
            length = int(self.headers.get("content-length", "0"))
            if length <= 0 or length > MAX_BODY_BYTES:
                self.send_error(413)
                return
            result = mcp.handle_json(self.rfile.read(length))
            if result is None:
                self.send_response(202)
                self.end_headers()
                return
            body = json.dumps(result, ensure_ascii=False, separators=(",", ":")).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, _format: str, *_args: object) -> None:
            return

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    return server, mcp


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--expected-binary-sha256", required=True)
    parser.add_argument("--source-commit", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    binary = args.binary.resolve(strict=True)
    actual_binary_sha256 = file_sha256(binary)
    if actual_binary_sha256 != args.expected_binary_sha256.lower():
        raise SystemExit("binary SHA-256 does not match expected identity")
    if len(args.source_commit) != 40 or any(
        character not in "0123456789abcdef" for character in args.source_commit.lower()
    ):
        raise SystemExit("source commit must be a full 40-character Git SHA")

    legacy_server, python_mcp = build_legacy_server()
    legacy_thread = threading.Thread(target=legacy_server.serve_forever, daemon=True)
    legacy_thread.start()
    rust_port = free_port()
    process: subprocess.Popen[str] | None = None
    stderr_tail = ""
    checks: dict[str, bool] = {}
    failure = ""
    with tempfile.TemporaryDirectory(prefix="marketcow-mcp-proxy-") as temporary:
        storage = Path(temporary) / "storage"
        storage.mkdir(mode=0o700)
        environment = {
            "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
            "LANG": "C.UTF-8",
            "MARKETCOW_RUST_PROFILE": "test",
            "MARKETCOW_RUST_BIND": f"127.0.0.1:{rust_port}",
            "MARKETCOW_RUST_STORAGE_ROOT": str(storage),
            "MARKETCOW_RUST_SCOPE_ID": "mcp-proxy-differential",
            "MARKETCOW_RUST_SHADOW": "true",
            "MARKETCOW_REAL_ORDER_SUBMISSION_ENABLED": "false",
            "MARKETCOW_LEGACY_MCP_URL": (
                f"http://127.0.0.1:{legacy_server.server_port}/mcp"
            ),
        }
        process = subprocess.Popen(
            [str(binary), "serve"],
            env=environment,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True,
        )
        try:
            health = None
            status = None
            deadline = time.monotonic() + 10
            while time.monotonic() < deadline and process.poll() is None:
                try:
                    status, health = get_json(f"http://127.0.0.1:{rust_port}/v1/health")
                    if status == 200:
                        break
                except (URLError, ConnectionError, TimeoutError):
                    pass
                time.sleep(0.05)
            checks["rust_health_available"] = health is not None and status == 200
            checks["real_orders_disabled"] = bool(
                health and health.get("real_order_submission_enabled") is False
            )
            checks["legacy_proxy_reported_without_url"] = bool(
                health
                and health.get("mcp", {}).get("legacy_proxy_configured") is True
                and "legacy_mcp_url" not in json.dumps(health)
            )

            list_request = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "tools/list",
                "params": {},
            }
            expected_list = python_mcp.handle_message(list_request)
            list_status, actual_list = post_json(
                f"http://127.0.0.1:{rust_port}/mcp", list_request
            )
            checks["tools_list_http_200"] = list_status == 200
            checks["all_14_tool_definitions_match"] = actual_list == expected_list

            call_request = {
                "jsonrpc": "2.0",
                "id": 2,
                "method": "tools/call",
                "params": {
                    "name": "get_quotes",
                    "arguments": {"symbols": ["AAPL.XNAS"]},
                },
            }
            expected_call = python_mcp.handle_message(call_request)
            call_status, actual_call = post_json(
                f"http://127.0.0.1:{rust_port}/mcp", call_request
            )
            checks["proxied_call_http_200"] = call_status == 200
            checks["proxied_tool_result_matches"] = actual_call == expected_call
        except Exception as error:  # preserve a bounded diagnostic in the result
            failure = f"{type(error).__name__}: {error}"
        finally:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
            stderr_tail = (process.stderr.read() if process.stderr else "")[-4000:]
            legacy_server.shutdown()
            legacy_server.server_close()
            python_mcp.client.close()

    passed = bool(checks) and all(checks.values()) and not failure
    result = {
        "schema_version": "marketcow.mcp-staged-proxy-differential.v1",
        "passed": passed,
        "binary_path": str(binary),
        "binary_sha256": actual_binary_sha256,
        "source_commit": args.source_commit.lower(),
        "checks": checks,
        "failure": failure or None,
        "process_exit_code": process.returncode if process else None,
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
