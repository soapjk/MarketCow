from __future__ import annotations

import json
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path

import httpx
from fastapi.testclient import TestClient

from marketcow.api import create_app
from marketcow.config import Settings
from marketcow.mcp_server import (
    LATEST_PROTOCOL_VERSION,
    MarketCowClient,
    McpServer,
)
from tests.test_market_data_api import Service


class McpServerTest(unittest.TestCase):
    def setUp(self) -> None:
        self.requests: list[httpx.Request] = []

        def handler(request: httpx.Request) -> httpx.Response:
            self.requests.append(request)
            if request.url.path == "/v1/instruments/search":
                return httpx.Response(200, json={
                    "count": 1,
                    "items": [{"instrument_id": "AAPL.XNAS", "symbol": "AAPL"}],
                })
            if request.url.path == "/v1/quotes/query":
                return httpx.Response(200, json={
                    "count": 1, "items": [{"symbol": "AAPL.XNAS", "last": "213.88"}],
                })
            if request.url.path == "/v1/tushare/cb_basic":
                return httpx.Response(200, json={"data": {
                    "fields": [
                        "ts_code", "bond_short_name", "bond_full_name", "stk_code",
                        "stk_short_name", "issue_price", "par", "first_conv_price",
                        "conv_price", "newest_rating", "issue_size", "remain_size",
                        "list_date",
                    ],
                    "items": [[
                        "113052.SH", "兴业转债", "兴业银行可转换公司债券",
                        "601166.SH", "兴业银行", 100, 100, 25.51, 23.51, "AAA",
                        500, 499.5, "20210114",
                    ]],
                }})
            if request.url.path == "/v1/tushare/cb_issue":
                return httpx.Response(200, json={"data": {
                    "fields": [
                        "ts_code", "ann_date", "res_ann_date", "issue_price",
                        "issue_size", "onl_date",
                    ],
                    "items": [["113052.SH", "20201223", "20210104", 100, 50_000_000_000,
                               "20201228"]],
                }})
            if request.url.path == "/v1/tushare/stock_basic":
                return httpx.Response(200, json={"data": {
                    "fields": ["ts_code", "name", "fullname", "list_status"],
                    "items": [["601166.SH", "兴业银行", "兴业银行股份有限公司", "L"]],
                }})
            if request.url.path.startswith("/v1/fundamentals/"):
                return httpx.Response(404, json={"detail": "fundamental data not found"})
            return httpx.Response(200, json={"status": "ok"})

        self.client = MarketCowClient(
            transport=httpx.MockTransport(handler),
            bearer_token="local-secret",
        )
        self.server = McpServer(self.client)

    def tearDown(self) -> None:
        self.client.close()

    def request(
        self, method: str, params: dict | None = None, request_id: int = 1
    ) -> dict:
        message = {"jsonrpc": "2.0", "id": request_id, "method": method}
        if params is not None:
            message["params"] = params
        response = self.server.handle_message(message)
        self.assertIsInstance(response, dict)
        return response

    def test_initialize_negotiates_supported_and_latest_fallback(self) -> None:
        supported = self.request("initialize", {"protocolVersion": "2025-06-18"})
        self.assertEqual(supported["result"]["protocolVersion"], "2025-06-18")
        latest = self.request("initialize", {"protocolVersion": "unknown"}, 2)
        self.assertEqual(latest["result"]["protocolVersion"], LATEST_PROTOCOL_VERSION)
        self.assertIn("tools", latest["result"]["capabilities"])

    def test_lists_only_read_only_tools(self) -> None:
        response = self.request("tools/list")
        tools = response["result"]["tools"]
        golden = json.loads(
            (Path(__file__).parent / "fixtures/mcp-service-health-tool-v1.json")
            .read_text(encoding="utf-8")
        )
        self.assertEqual(
            next(tool for tool in tools if tool["name"] == "service_health"),
            golden,
        )
        names = {tool["name"] for tool in tools}
        self.assertIn("get_quotes", names)
        quote_golden = json.loads(
            (Path(__file__).parent / "fixtures/mcp-get-quotes-tool-v1.json")
            .read_text(encoding="utf-8")
        )
        self.assertEqual(
            next(tool for tool in tools if tool["name"] == "get_quotes"),
            quote_golden,
        )
        self.assertIn("get_canonical_bars", names)
        self.assertIn("get_financial_statements", names)
        self.assertIn("get_fund_dividend_history", names)
        self.assertTrue(all(tool["annotations"]["readOnlyHint"] for tool in tools))
        self.assertTrue(all(not tool["annotations"]["destructiveHint"] for tool in tools))
        open_world = {
            tool["name"] for tool in tools if tool["annotations"]["openWorldHint"]
        }
        self.assertEqual(open_world, {
            "get_fund_dividend_history",
            "search_convertible_bonds",
            "get_convertible_bond",
            "get_convertible_bond_market",
        })

    def test_fund_dividend_history_uses_inclusive_date_range(self) -> None:
        response = self.request("tools/call", {
            "name": "get_fund_dividend_history",
            "arguments": {
                "symbol": "563020.XSHG",
                "from": "2025-08-06",
                "to": "2026-08-06",
                "refresh": False,
            },
        })
        self.assertFalse(response["result"]["isError"])
        request = self.requests[0]
        self.assertEqual(request.url.path, "/v1/funds/563020.XSHG/dividends")
        self.assertEqual(request.url.params["from"], "2025-08-06")
        self.assertEqual(request.url.params["to"], "2026-08-06")
        self.assertEqual(request.url.params["refresh"], "false")

    def test_tool_call_returns_text_and_structured_content(self) -> None:
        response = self.request("tools/call", {
            "name": "search_instruments",
            "arguments": {"query": "Apple", "limit": 5},
        })
        result = response["result"]
        self.assertFalse(result["isError"])
        self.assertEqual(result["structuredContent"]["items"][0]["instrument_id"], "AAPL.XNAS")
        self.assertEqual(json.loads(result["content"][0]["text"]), result["structuredContent"])
        self.assertEqual(self.requests[0].url.params["q"], "Apple")
        self.assertEqual(self.requests[0].headers["Authorization"], "Bearer local-secret")

    def test_rejects_invalid_base_url(self) -> None:
        with self.assertRaisesRegex(ValueError, "http"):
            MarketCowClient(base_url="file:///tmp/marketcow.sock")

    def test_quote_tool_forces_cached_read(self) -> None:
        response = self.request("tools/call", {
            "name": "get_quotes",
            "arguments": {"symbols": ["AAPL.XNAS"]},
        })
        self.assertFalse(response["result"]["isError"])
        body = json.loads(self.requests[0].content)
        self.assertEqual(body["symbols"], ["AAPL.XNAS"])
        self.assertFalse(body["refresh"])
        self.assertIsNone(body["provider"])
        self.assertFalse(body["allow_fallback"])

    def test_convertible_bond_tool_loads_provider_catalog_not_bundled_rows(self) -> None:
        response = self.request("tools/call", {
            "name": "search_convertible_bonds",
            "arguments": {"query": "113052.XSHG"},
        })
        content = response["result"]["structuredContent"]
        self.assertEqual(content["catalog_size"], 1)
        self.assertEqual(content["items"][0]["bond_id"], "113052.XSHG")
        self.assertEqual(content["items"][0]["issuer"], "兴业银行股份有限公司")
        paths = [request.url.path for request in self.requests]
        self.assertEqual(paths, [
            "/v1/tushare/cb_basic",
            "/v1/tushare/cb_issue",
            "/v1/tushare/stock_basic",
        ])

    def test_invalid_tool_input_is_visible_to_agent(self) -> None:
        response = self.request("tools/call", {
            "name": "get_market_bars",
            "arguments": {"symbols": ["AAPL"], "interval": "2d"},
        })
        result = response["result"]
        self.assertTrue(result["isError"])
        self.assertEqual(result["structuredContent"]["error"], "invalid_tool_input")
        self.assertEqual(self.requests, [])

    def test_rejects_arguments_outside_declared_schema(self) -> None:
        response = self.request("tools/call", {
            "name": "service_health",
            "arguments": {"refresh": True},
        })
        result = response["result"]
        self.assertTrue(result["isError"])
        self.assertIn("unexpected argument", result["structuredContent"]["detail"])
        self.assertEqual(self.requests, [])

    def test_rejects_missing_required_argument(self) -> None:
        response = self.request("tools/call", {
            "name": "get_dividends",
            "arguments": {"symbol": "AAPL"},
        })
        result = response["result"]
        self.assertTrue(result["isError"])
        self.assertIn("fiscal_year", result["structuredContent"]["detail"])
        self.assertEqual(self.requests, [])

    def test_api_errors_are_tool_errors_without_token_disclosure(self) -> None:
        response = self.request("tools/call", {
            "name": "get_fundamental",
            "arguments": {"symbol": "AAPL"},
        })
        result = response["result"]
        self.assertTrue(result["isError"])
        serialized = json.dumps(result)
        self.assertIn("fundamental data not found", serialized)
        self.assertNotIn("local-secret", serialized)

    def test_batch_and_notifications(self) -> None:
        response = self.server.handle_message([
            {"jsonrpc": "2.0", "method": "notifications/initialized"},
            {"jsonrpc": "2.0", "id": 7, "method": "ping"},
        ])
        self.assertEqual(response, [{"jsonrpc": "2.0", "id": 7, "result": {}}])
        self.assertTrue(self.server.initialized)

    def test_unknown_tool_is_protocol_error(self) -> None:
        response = self.request("tools/call", {
            "name": "delete_everything", "arguments": {},
        })
        self.assertEqual(response["error"]["code"], -32602)


class McpHttpEndpointTest(unittest.TestCase):
    def setUp(self) -> None:
        self.folder = tempfile.TemporaryDirectory()
        root = Path(self.folder.name) / "runtime-test"
        self.settings = Settings(
            raw_path=root / "raw",
            storage_root=root,
            allowed_root=root.parent,
            postgres_dsn="postgresql://u:p@127.0.0.1/marketcow_test",
            clickhouse_password="secret",
            profile="test",
            port=8793,
            postgres_schema="marketcow_test",
            clickhouse_database="marketcow_test",
            clickhouse_spool_path=root / "spool",
        )

        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json={"status": "ok", "via": request.url.path})

        self.market_client = MarketCowClient(transport=httpx.MockTransport(handler))
        self.mcp = McpServer(self.market_client)

    def tearDown(self) -> None:
        self.market_client.close()
        self.folder.cleanup()

    def post(self, client: TestClient, payload: object, **headers: str):
        return client.post(
            "/mcp",
            content=json.dumps(payload),
            headers={"Content-Type": "application/json", **headers},
        )

    def test_service_exposes_mcp_by_default(self) -> None:
        with TestClient(create_app(
            self.settings, Service(), mcp_server=self.mcp
        )) as client:
            initialized = self.post(client, {
                "jsonrpc": "2.0", "id": 1, "method": "initialize",
                "params": {
                    "protocolVersion": "2025-11-25",
                    "capabilities": {},
                    "clientInfo": {"name": "test", "version": "1"},
                },
            })
            tools = self.post(client, {
                "jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {},
            }, **{"MCP-Protocol-Version": "2025-11-25"})
            health = client.get("/v1/health")
        self.assertEqual(initialized.status_code, 200)
        self.assertEqual(initialized.json()["result"]["protocolVersion"], "2025-11-25")
        self.assertEqual(len(tools.json()["result"]["tools"]), 14)
        self.assertEqual(health.json()["mcp"], {
            "enabled": True, "endpoint": "/mcp",
        })

    def test_notification_returns_202(self) -> None:
        with TestClient(create_app(
            self.settings, Service(), mcp_server=self.mcp
        )) as client:
            response = self.post(client, {
                "jsonrpc": "2.0", "method": "notifications/initialized",
            })
        self.assertEqual(response.status_code, 202)
        self.assertEqual(response.content, b"")

    def test_origin_invalid_protocol_and_large_body_fail_closed(self) -> None:
        with TestClient(create_app(
            self.settings, Service(), mcp_server=self.mcp
        )) as client:
            origin = self.post(
                client, {"jsonrpc": "2.0", "id": 1, "method": "ping"},
                Origin="https://attacker.example",
            )
            protocol = self.post(
                client, {"jsonrpc": "2.0", "id": 2, "method": "ping"},
                **{"MCP-Protocol-Version": "unknown"},
            )
            large = client.post(
                "/mcp",
                content=b" " * (1024 * 1024 + 1),
                headers={"Content-Type": "application/json"},
            )
        self.assertEqual(origin.status_code, 403)
        self.assertEqual(protocol.status_code, 400)
        self.assertEqual(large.status_code, 413)

    def test_mcp_can_be_disabled(self) -> None:
        disabled = replace(self.settings, mcp_enabled=False)
        with TestClient(create_app(
            disabled, Service(), mcp_server=self.mcp
        )) as client:
            response = self.post(client, {
                "jsonrpc": "2.0", "id": 1, "method": "ping",
            })
            health = client.get("/v1/health")
        self.assertEqual(response.status_code, 404)
        self.assertFalse(health.json()["mcp"]["enabled"])


if __name__ == "__main__":
    unittest.main()
