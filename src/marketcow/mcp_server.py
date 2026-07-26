from __future__ import annotations

import json
import os
import sys
from dataclasses import dataclass
from typing import Any, Callable, Mapping, Sequence
from urllib.parse import quote, urlsplit

import httpx

from . import __version__


LATEST_PROTOCOL_VERSION = "2025-11-25"
SUPPORTED_PROTOCOL_VERSIONS = frozenset({
    "2024-11-05",
    "2025-03-26",
    "2025-06-18",
    LATEST_PROTOCOL_VERSION,
})
JSONRPC_VERSION = "2.0"


class ToolInputError(ValueError):
    pass


class MarketCowApiError(RuntimeError):
    def __init__(self, status_code: int, detail: Any) -> None:
        self.status_code = status_code
        self.detail = detail
        super().__init__(f"MarketCow API returned HTTP {status_code}")


def _object_schema(
    properties: dict[str, Any],
    required: Sequence[str] = (),
) -> dict[str, Any]:
    return {
        "type": "object",
        "properties": properties,
        "required": list(required),
        "additionalProperties": False,
    }


def _string(description: str, **constraints: Any) -> dict[str, Any]:
    return {"type": "string", "description": description, **constraints}


def _integer(
    description: str, minimum: int, maximum: int, default: int | None = None
) -> dict[str, Any]:
    schema: dict[str, Any] = {
        "type": "integer",
        "description": description,
        "minimum": minimum,
        "maximum": maximum,
    }
    if default is not None:
        schema["default"] = default
    return schema


@dataclass(frozen=True)
class Tool:
    name: str
    description: str
    input_schema: dict[str, Any]
    handler: Callable[[Mapping[str, Any]], dict[str, Any]]

    def definition(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "description": self.description,
            "inputSchema": self.input_schema,
            "annotations": {
                "readOnlyHint": True,
                "destructiveHint": False,
                "idempotentHint": True,
                "openWorldHint": False,
            },
        }


class MarketCowClient:
    """Bounded, read-only adapter over MarketCow's public HTTP contract."""

    def __init__(
        self,
        base_url: str = "http://127.0.0.1:8790",
        timeout_seconds: float = 20.0,
        bearer_token: str = "",
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        parsed = urlsplit(base_url)
        if (
            parsed.scheme not in {"http", "https"}
            or not parsed.hostname
            or parsed.username is not None
            or parsed.password is not None
            or parsed.query
            or parsed.fragment
        ):
            raise ValueError("MARKETCOW_MCP_BASE_URL must be an http(s) URL")
        headers = {"Accept": "application/json", "User-Agent": f"marketcow-mcp/{__version__}"}
        if bearer_token:
            headers["Authorization"] = f"Bearer {bearer_token}"
        self._client = httpx.Client(
            base_url=base_url.rstrip("/"),
            headers=headers,
            timeout=timeout_seconds,
            transport=transport,
            trust_env=False,
        )

    @classmethod
    def from_env(cls) -> MarketCowClient:
        raw_timeout = os.environ.get("MARKETCOW_MCP_TIMEOUT_SECONDS", "20")
        try:
            timeout = float(raw_timeout)
        except ValueError as exc:
            raise ValueError("MARKETCOW_MCP_TIMEOUT_SECONDS must be a number") from exc
        if not 0 < timeout <= 120:
            raise ValueError("MARKETCOW_MCP_TIMEOUT_SECONDS must be between 0 and 120")
        return cls(
            base_url=os.environ.get(
                "MARKETCOW_MCP_BASE_URL", "http://127.0.0.1:8790"
            ),
            timeout_seconds=timeout,
            bearer_token=os.environ.get("MARKETCOW_MCP_BEARER_TOKEN", ""),
        )

    def close(self) -> None:
        self._client.close()

    def request(
        self,
        method: str,
        path: str,
        *,
        params: Mapping[str, Any] | None = None,
        json_body: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        try:
            response = self._client.request(
                method, path, params=params, json=json_body
            )
        except httpx.TimeoutException as exc:
            raise MarketCowApiError(504, "MarketCow API request timed out") from exc
        except httpx.HTTPError as exc:
            raise MarketCowApiError(503, "MarketCow API is unavailable") from exc
        try:
            payload = response.json()
        except ValueError:
            payload = {"detail": "MarketCow API returned a non-JSON response"}
        if response.is_error:
            detail = payload.get("detail", payload) if isinstance(payload, dict) else payload
            raise MarketCowApiError(response.status_code, detail)
        if not isinstance(payload, dict):
            raise MarketCowApiError(502, "MarketCow API returned an invalid response")
        return payload


def _required_string(arguments: Mapping[str, Any], name: str) -> str:
    value = arguments.get(name)
    if not isinstance(value, str) or not value.strip():
        raise ToolInputError(f"{name} must be a non-empty string")
    return value.strip()


def _path_segment(arguments: Mapping[str, Any], name: str) -> str:
    return quote(_required_string(arguments, name), safe="")


def _optional_string(arguments: Mapping[str, Any], name: str, default: str = "") -> str:
    value = arguments.get(name, default)
    if not isinstance(value, str):
        raise ToolInputError(f"{name} must be a string")
    return value.strip()


def _bounded_int(
    arguments: Mapping[str, Any], name: str, default: int, minimum: int, maximum: int
) -> int:
    value = arguments.get(name, default)
    if isinstance(value, bool) or not isinstance(value, int):
        raise ToolInputError(f"{name} must be an integer")
    if not minimum <= value <= maximum:
        raise ToolInputError(f"{name} must be between {minimum} and {maximum}")
    return value


def _symbols(arguments: Mapping[str, Any], maximum: int = 20) -> list[str]:
    values = arguments.get("symbols")
    if not isinstance(values, list) or not values:
        raise ToolInputError("symbols must be a non-empty array")
    if len(values) > maximum:
        raise ToolInputError(f"symbols accepts at most {maximum} items")
    normalized = []
    for value in values:
        if not isinstance(value, str) or not value.strip():
            raise ToolInputError("every symbol must be a non-empty string")
        normalized.append(value.strip())
    return normalized


def create_tools(client: MarketCowClient) -> dict[str, Tool]:
    def service_health(_: Mapping[str, Any]) -> dict[str, Any]:
        return client.request("GET", "/v1/health")

    def search_instruments(arguments: Mapping[str, Any]) -> dict[str, Any]:
        return client.request("GET", "/v1/instruments/search", params={
            "q": _required_string(arguments, "query"),
            "limit": _bounded_int(arguments, "limit", 12, 1, 30),
        })

    def get_instrument(arguments: Mapping[str, Any]) -> dict[str, Any]:
        instrument_id = _path_segment(arguments, "instrument_id")
        return client.request("GET", f"/v1/instruments/{instrument_id}")

    def get_quotes(arguments: Mapping[str, Any]) -> dict[str, Any]:
        return client.request("POST", "/v1/quotes/query", json_body={
            "symbols": _symbols(arguments),
            "refresh": False,
            "provider": None,
            "allow_fallback": False,
        })

    def get_market_bars(arguments: Mapping[str, Any]) -> dict[str, Any]:
        interval = _optional_string(arguments, "interval", "1d")
        if interval not in {"1m", "5m", "15m", "30m", "1h", "1d"}:
            raise ToolInputError("interval must be one of 1m, 5m, 15m, 30m, 1h, 1d")
        adjustment = _optional_string(arguments, "adjustment", "qfq")
        if adjustment not in {"raw", "qfq", "hfq"}:
            raise ToolInputError("adjustment must be raw, qfq or hfq")
        return client.request("POST", "/v1/market-bars/query", json_body={
            "symbols": _symbols(arguments),
            "range": _optional_string(arguments, "range", "1y"),
            "interval": interval,
            "adjustment": adjustment,
            "refresh": False,
            "provider": None,
            "allow_fallback": False,
            "limit": _bounded_int(arguments, "limit", 500, 1, 1000),
        })

    def get_canonical_bars(arguments: Mapping[str, Any]) -> dict[str, Any]:
        interval = _optional_string(arguments, "interval", "1-DAY")
        supported = {
            "1-MINUTE", "5-MINUTE", "15-MINUTE",
            "30-MINUTE", "1-HOUR", "1-DAY",
        }
        if interval not in supported:
            raise ToolInputError(f"interval must be one of {', '.join(sorted(supported))}")
        adjustment = _optional_string(arguments, "adjustment", "qfq")
        if adjustment not in {"raw", "qfq", "hfq"}:
            raise ToolInputError("adjustment must be raw, qfq or hfq")
        params: dict[str, Any] = {
            "start": _required_string(arguments, "start"),
            "end": _required_string(arguments, "end"),
            "interval": interval,
            "adjustment": adjustment,
            "page_size": _bounded_int(arguments, "page_size", 500, 1, 1000),
        }
        cursor = _optional_string(arguments, "cursor")
        if cursor:
            params["cursor"] = cursor
        instrument_id = _path_segment(arguments, "instrument_id")
        return client.request(
            "GET", f"/v1/canonical-bars/{instrument_id}", params=params
        )

    def get_fundamental(arguments: Mapping[str, Any]) -> dict[str, Any]:
        symbol = _path_segment(arguments, "symbol")
        params = {
            key: value for key, value in {
                "report_period": _optional_string(arguments, "report_period"),
                "as_of": _optional_string(arguments, "as_of"),
            }.items() if value
        }
        return client.request("GET", f"/v1/fundamentals/{symbol}", params=params)

    def get_financial_statements(arguments: Mapping[str, Any]) -> dict[str, Any]:
        statement = _optional_string(arguments, "statement")
        if statement not in {"", "income", "balance", "cashflow"}:
            raise ToolInputError("statement must be income, balance, cashflow or empty")
        symbol = _path_segment(arguments, "symbol")
        params: dict[str, Any] = {
            "statement": statement,
            "limit_periods": _bounded_int(arguments, "limit_periods", 8, 1, 40),
        }
        as_of = _optional_string(arguments, "as_of")
        if as_of:
            params["as_of"] = as_of
        return client.request(
            "GET", f"/v1/financials/{symbol}/statements", params=params
        )

    def get_dividends(arguments: Mapping[str, Any]) -> dict[str, Any]:
        symbol = _path_segment(arguments, "symbol")
        return client.request("GET", f"/v1/dividends/{symbol}", params={
            "fiscal_year": _bounded_int(arguments, "fiscal_year", 2025, 1991, 2100),
        })

    def get_exposure_facts(arguments: Mapping[str, Any]) -> dict[str, Any]:
        symbol = _path_segment(arguments, "symbol")
        return client.request(
            "GET", f"/v1/exposure-facts/{symbol}", params={"refresh": False}
        )

    common_symbol = _string(
        "MarketCow symbol or canonical instrument identifier, depending on the endpoint."
    )
    tools = [
        Tool(
            "service_health",
            "Check whether the local MarketCow API and its data stores are healthy.",
            _object_schema({}),
            service_health,
        ),
        Tool(
            "search_instruments",
            "Search the instrument master before requesting data for an unfamiliar ticker.",
            _object_schema({
                "query": _string("Ticker, company name, alias, or instrument identifier."),
                "limit": _integer("Maximum matches.", 1, 30, 12),
            }, ("query",)),
            search_instruments,
        ),
        Tool(
            "get_instrument",
            "Get canonical identity, venue, currency, precision, and provider mappings.",
            _object_schema({
                "instrument_id": _string("Canonical ID such as AAPL.XNAS or 600519.XSHG.")
            }, ("instrument_id",)),
            get_instrument,
        ),
        Tool(
            "get_quotes",
            "Read cached quotes for up to 20 symbols without calling upstream providers.",
            _object_schema({
                "symbols": {
                    "type": "array", "items": common_symbol,
                    "minItems": 1, "maxItems": 20,
                },
            }, ("symbols",)),
            get_quotes,
        ),
        Tool(
            "get_market_bars",
            "Read cached OHLCV bars for up to 20 symbols. Use canonical bars for reproducible pagination.",
            _object_schema({
                "symbols": {
                    "type": "array", "items": common_symbol,
                    "minItems": 1, "maxItems": 20,
                },
                "range": _string("Relative history range understood by MarketCow, for example 1y."),
                "interval": {
                    **_string("Bar interval."), "enum": ["1m", "5m", "15m", "30m", "1h", "1d"],
                    "default": "1d",
                },
                "adjustment": {
                    **_string("Price adjustment series."), "enum": ["raw", "qfq", "hfq"],
                    "default": "qfq",
                },
                "limit": _integer("Maximum bars per symbol.", 1, 1000, 500),
            }, ("symbols",)),
            get_market_bars,
        ),
        Tool(
            "get_canonical_bars",
            "Read deterministic canonical OHLCV bars for an exact UTC window, with provenance and a continuation cursor.",
            _object_schema({
                "instrument_id": _string("Canonical ID such as AAPL.XNAS."),
                "start": _string("Inclusive timezone-aware ISO-8601 window start."),
                "end": _string("Inclusive timezone-aware ISO-8601 window end."),
                "interval": {
                    **_string("Canonical contract interval."),
                    "enum": sorted({
                        "1-MINUTE", "5-MINUTE", "15-MINUTE",
                        "30-MINUTE", "1-HOUR", "1-DAY",
                    }),
                    "default": "1-DAY",
                },
                "adjustment": {
                    **_string("Price adjustment series."), "enum": ["raw", "qfq", "hfq"],
                    "default": "qfq",
                },
                "page_size": _integer("Maximum rows in this page.", 1, 1000, 500),
                "cursor": _string("Continuation cursor from the previous page."),
            }, ("instrument_id", "start", "end")),
            get_canonical_bars,
        ),
        Tool(
            "get_fundamental",
            "Get one company's cached point-in-time fundamental and valuation record.",
            _object_schema({
                "symbol": common_symbol,
                "report_period": _string("Optional report period accepted by MarketCow."),
                "as_of": _string("Optional point-in-time ISO-8601 cutoff."),
            }, ("symbol",)),
            get_fundamental,
        ),
        Tool(
            "get_financial_statements",
            "Get cached income, balance sheet, and/or cash-flow statement rows.",
            _object_schema({
                "symbol": common_symbol,
                "statement": {
                    **_string("Optional statement type."),
                    "enum": ["", "income", "balance", "cashflow"],
                },
                "limit_periods": _integer("Maximum reporting periods.", 1, 40, 8),
                "as_of": _string("Optional point-in-time ISO-8601 cutoff."),
            }, ("symbol",)),
            get_financial_statements,
        ),
        Tool(
            "get_dividends",
            "Get cached dividend evidence and assessment for a symbol and fiscal year.",
            _object_schema({
                "symbol": common_symbol,
                "fiscal_year": _integer("Fiscal year.", 1991, 2100),
            }, ("symbol", "fiscal_year")),
            get_dividends,
        ),
        Tool(
            "get_exposure_facts",
            "Get auditable issuer or fund exposure facts without inferred themes or factors.",
            _object_schema({"symbol": common_symbol}, ("symbol",)),
            get_exposure_facts,
        ),
    ]
    return {tool.name: tool for tool in tools}


class McpServer:
    def __init__(self, client: MarketCowClient) -> None:
        self.client = client
        self.tools = create_tools(client)
        self.initialized = False

    @staticmethod
    def _response(request_id: Any, result: dict[str, Any]) -> dict[str, Any]:
        return {"jsonrpc": JSONRPC_VERSION, "id": request_id, "result": result}

    @staticmethod
    def _error(
        request_id: Any, code: int, message: str, data: Any = None
    ) -> dict[str, Any]:
        error: dict[str, Any] = {"code": code, "message": message}
        if data is not None:
            error["data"] = data
        return {"jsonrpc": JSONRPC_VERSION, "id": request_id, "error": error}

    @staticmethod
    def _tool_result(payload: dict[str, Any]) -> dict[str, Any]:
        return {
            "content": [{
                "type": "text",
                "text": json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
            }],
            "structuredContent": payload,
            "isError": False,
        }

    @staticmethod
    def _tool_error(message: str, detail: Any = None) -> dict[str, Any]:
        payload: dict[str, Any] = {"error": message}
        if detail is not None:
            payload["detail"] = detail
        return {
            "content": [{
                "type": "text",
                "text": json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
            }],
            "structuredContent": payload,
            "isError": True,
        }

    def handle_request(self, message: Any) -> dict[str, Any] | None:
        if not isinstance(message, dict):
            return self._error(None, -32600, "Invalid Request")
        request_id = message.get("id")
        is_notification = "id" not in message
        if message.get("jsonrpc") != JSONRPC_VERSION or not isinstance(
            message.get("method"), str
        ):
            return None if is_notification else self._error(
                request_id, -32600, "Invalid Request"
            )
        method = message["method"]
        params = message.get("params", {})
        if not isinstance(params, dict):
            return None if is_notification else self._error(
                request_id, -32602, "Invalid params"
            )
        if is_notification:
            if method == "notifications/initialized":
                self.initialized = True
            return None
        if method == "initialize":
            requested = params.get("protocolVersion")
            protocol = (
                requested if requested in SUPPORTED_PROTOCOL_VERSIONS
                else LATEST_PROTOCOL_VERSION
            )
            return self._response(request_id, {
                "protocolVersion": protocol,
                "capabilities": {"tools": {"listChanged": False}},
                "serverInfo": {
                    "name": "marketcow",
                    "title": "MarketCow Financial Data",
                    "version": __version__,
                },
                "instructions": (
                    "All tools are read-only and use cached MarketCow data. "
                    "Search instruments first when venue identity is ambiguous. "
                    "Treat provider timestamps, provenance, and quality fields as part "
                    "of the analytical evidence."
                ),
            })
        if method == "ping":
            return self._response(request_id, {})
        if method == "tools/list":
            return self._response(request_id, {
                "tools": [tool.definition() for tool in self.tools.values()]
            })
        if method == "tools/call":
            name = params.get("name")
            arguments = params.get("arguments", {})
            if not isinstance(name, str) or not isinstance(arguments, dict):
                return self._error(request_id, -32602, "Invalid tool call parameters")
            tool = self.tools.get(name)
            if tool is None:
                return self._error(request_id, -32602, f"Unknown tool: {name}")
            try:
                allowed = set(tool.input_schema["properties"])
                unexpected = sorted(set(arguments) - allowed)
                if unexpected:
                    raise ToolInputError(
                        f"unexpected argument(s): {', '.join(unexpected)}"
                    )
                missing = [
                    field for field in tool.input_schema.get("required", [])
                    if field not in arguments
                ]
                if missing:
                    raise ToolInputError(
                        f"missing required argument(s): {', '.join(missing)}"
                    )
                result = tool.handler(arguments)
                return self._response(request_id, self._tool_result(result))
            except ToolInputError as exc:
                return self._response(
                    request_id, self._tool_error("invalid_tool_input", str(exc))
                )
            except MarketCowApiError as exc:
                return self._response(request_id, self._tool_error(
                    "marketcow_api_error",
                    {"status_code": exc.status_code, "detail": exc.detail},
                ))
            except Exception:
                return self._response(
                    request_id, self._tool_error("unexpected_server_error")
                )
        return self._error(request_id, -32601, "Method not found")

    def handle_message(self, message: Any) -> dict[str, Any] | list[dict[str, Any]] | None:
        if isinstance(message, list):
            if not message:
                return self._error(None, -32600, "Invalid Request")
            if len(message) > 100:
                return self._error(None, -32600, "Batch exceeds 100 messages")
            responses = [
                response for item in message
                if (response := self.handle_request(item)) is not None
            ]
            return responses or None
        return self.handle_request(message)

    def handle_json(
        self, raw: str | bytes
    ) -> dict[str, Any] | list[dict[str, Any]] | None:
        try:
            message = json.loads(raw)
        except (json.JSONDecodeError, UnicodeDecodeError):
            return self._error(None, -32700, "Parse error")
        return self.handle_message(message)

    def run_stdio(self) -> None:
        for line in sys.stdin:
            response = self.handle_json(line)
            if response is None:
                continue
            sys.stdout.write(json.dumps(
                response, ensure_ascii=False, separators=(",", ":")
            ))
            sys.stdout.write("\n")
            sys.stdout.flush()


def main() -> int:
    client: MarketCowClient | None = None
    try:
        client = MarketCowClient.from_env()
        McpServer(client).run_stdio()
        return 0
    except (ValueError, OSError) as exc:
        print(f"marketcow-mcp: {exc}", file=sys.stderr)
        return 2
    finally:
        if client is not None:
            client.close()


if __name__ == "__main__":
    raise SystemExit(main())
