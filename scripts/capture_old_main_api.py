#!/usr/bin/env python3
"""Reproduce the frozen old-main API capture without contacting a running service."""
from __future__ import annotations

import argparse
import hashlib
import importlib
import json
import subprocess
import sys
import tempfile
from pathlib import Path
from urllib.parse import urlencode

from fastapi.testclient import TestClient


EXPECTED_COMMIT = "701ffbde1b25ae587845ea2bd021ca8fa12b93b4"
TOOL_VERSION = "marketcow.api-compat-capture.v2"
PARAMETERS = {
    "symbols": "AAPL", "symbol": "AAPL", "q": "AAPL",
    "bar_at": "2026-01-02T00:00:00Z", "bar_ats": "2026-01-02T00:00:00Z",
    "as_of": "2026-01-02T00:00:00Z", "start": "2026-01-01T00:00:00Z",
    "end": "2026-01-02T00:00:00Z", "from": "2026-01-01",
    "to": "2026-01-02", "report_period": "20241231", "api_name": "daily",
}


def canonical(value):
    if isinstance(value, dict):
        return {str(key): canonical(item) for key, item in sorted(value.items())}
    if isinstance(value, list):
        return [canonical(item) for item in value]
    return value


def digest(value):
    data = json.dumps(canonical(value), ensure_ascii=False, allow_nan=False,
                      sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(data).hexdigest()


def shape(value):
    if isinstance(value, dict):
        return {str(key): shape(item) for key, item in sorted(value.items())}
    if isinstance(value, list):
        return {"type": "array", "items": shape(value[0]) if value else None}
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "integer"
    if isinstance(value, float):
        return "number"
    return "string"


def semantics(value):
    if not isinstance(value, dict):
        return {"count": None, "empty_collections": [], "has_detail": False,
                "structured_error_fields": [], "has_partial_errors": False}
    count = value.get("count")
    error_fields = sorted({"detail", "error", "code", "message"}.intersection(value))
    errors = value.get("errors")
    return {
        "count": count if isinstance(count, int) and not isinstance(count, bool) else None,
        "empty_collections": [str(key) for key, item in sorted(value.items())
                              if isinstance(item, list) and not item],
        "has_detail": "detail" in value,
        "structured_error_fields": error_fields,
        "has_structured_error": bool(error_fields),
        "has_partial_errors": isinstance(errors, (list, dict)) and bool(errors),
    }


def request_for(route, detail):
    method, template = route.split(" ", 1)
    path, query = template, {}
    for parameter in detail["parameters"]:
        name = parameter["name"]
        value = PARAMETERS.get(name, "1")
        if parameter["in"] == "path":
            path = path.replace("{" + name + "}", value)
        elif parameter["in"] == "query" and parameter["required"]:
            query[name] = value
    if route == "GET /v1/quotes":
        query["symbols"] = "AAPL"
    body = None
    if route == "POST /v1/tushare/realtime-quote":
        body = {"ts_code": "AAPL"}
    elif route == "POST /v1/tushare/{api_name}":
        body = {"params": {}, "fields": ""}
    return method, path, query, body


class Repository:
    def get_latest_quotes(self, symbols):
        return [{"symbol": value, "close": 1.0, "refresh_seen": False}
                for value in symbols]

    def query_fundamentals(self, *args, **kwargs):
        if kwargs.get("limit") == 1:
            return [{"symbol": kwargs.get("symbol", "000001")}]
        return []

    def __getattr__(self, name):
        if name.startswith(("get_", "query_", "latest_", "provider_", "tdx_")):
            return lambda *_args, **_kwargs: []
        return lambda *_args, **_kwargs: {}


class Service:
    def __init__(self):
        repository = Repository()
        self.market_bar_repository = repository
        self.metadata_repository = repository
        self.fundamental_repository = repository
        self.artifact_store = repository
        self.warehouse = repository

    def refresh_quote(self, symbol):
        return {"symbol": symbol, "close": 1.0, "refresh_seen": True}

    def get_quote(self, symbol, force_refresh=False):
        if symbol in {"FAIL", "MISSING"}:
            raise RuntimeError("bounded fixture failure")
        return {"symbol": symbol, "close": 1.0,
                "refresh_seen": bool(force_refresh)}

    def refresh_quote_history(self, symbol, _range, interval, adjustment):
        if interval == "bad":
            raise RuntimeError("bounded fixture failure")
        return {"symbol": symbol, "interval": interval, "adjustment": adjustment,
                "bars": [], "count": 0}

    def tushare_realtime_quote(self, _symbol):
        return []

    def calendar_snapshot(self, *_args):
        return {}

    def search_instruments(self, *_args):
        return []

    def close(self):
        pass

    def __getattr__(self, name):
        if name.startswith(("get_", "query_", "search_")):
            return lambda *_args, **_kwargs: []
        return lambda *_args, **_kwargs: {}


class FaultRepository:
    def __getattr__(self, _name):
        def fail(*_args, **_kwargs):
            raise RuntimeError("bounded fixture failure")
        return fail


class FaultService(Service):
    def __init__(self):
        repository = FaultRepository()
        self.market_bar_repository = repository
        self.metadata_repository = repository
        self.fundamental_repository = repository
        self.artifact_store = repository
        self.warehouse = repository

    def __getattr__(self, _name):
        def fail(*_args, **_kwargs):
            raise RuntimeError("bounded fixture failure")
        return fail

    def get_quote(self, *_args, **_kwargs):
        raise RuntimeError("bounded fixture failure")

    refresh_quote_history = get_quote
    calendar_snapshot = get_quote
    search_instruments = get_quote
    tushare_realtime_quote = get_quote


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-root", type=Path, required=True)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--scenario-output", type=Path)
    parser.add_argument("--matrix-output", type=Path)
    args = parser.parse_args()
    root = args.source_root.resolve()
    commit = subprocess.check_output(
        ["git", "-C", str(root), "rev-parse", "HEAD"], text=True
    ).strip()
    if commit != EXPECTED_COMMIT:
        raise SystemExit(f"old-main source commit mismatch: {commit}")
    sys.path.insert(0, str(root / "src"))
    api = importlib.import_module("marketcow.api")
    config = importlib.import_module("marketcow.config")
    with tempfile.TemporaryDirectory(suffix="-old-main-capture") as directory:
        base = Path(directory)
        settings = config.Settings(base / "warehouse.duckdb", base / "raw",
                                   port=8792, profile="development")
        app = api.create_app(settings, Service())
        fault_app = api.create_app(settings, FaultService())
        schema = app.openapi()
    routes = {}
    for path, operations in sorted(schema["paths"].items()):
        for method, operation in sorted(operations.items()):
            if method.upper() not in {"GET", "POST", "PUT", "PATCH", "DELETE"}:
                continue
            parameters = [{
                "name": item.get("name"), "in": item.get("in"),
                "required": bool(item.get("required", False)),
                "schema": canonical(item.get("schema", {})),
            } for item in operation.get("parameters", [])]
            parameters.sort(key=lambda item: (item["in"], item["name"]))
            routes[f"{method.upper()} {path}"] = {
                "parameters": parameters,
                "request_body": canonical(operation.get("requestBody")),
                "responses": {str(status): canonical({"content": detail.get("content", {})})
                              for status, detail in sorted(operation["responses"].items())},
            }
    result = {
        "schema": "marketcow.old-main-api-contract.v1",
        "capture_tool_version": TOOL_VERSION,
        "source_commit": commit,
        "generation_input": "isolated-fastapi-openapi:legacy-fixture-v1",
        "routes": routes,
    }
    result["sha256"] = digest(result)
    rendered = json.dumps(result, indent=2, ensure_ascii=False, sort_keys=True) + "\n"
    if args.output:
        args.output.write_text(rendered, encoding="utf-8")
    else:
        print(rendered, end="")
    if args.scenario_output:
        requests = {
            "health": "/v1/health",
            "quotes_default": "/v1/quotes?symbols=AAPL",
            "quotes_empty": "/v1/quotes?symbols=MISSING&refresh=false",
            "quotes_batch_error": "/v1/quotes?symbols=AAPL,FAIL&refresh=true",
            "quotes_backend_failure": "/v1/quotes?symbols=FAIL&refresh=true",
            "quotes_missing_parameter": "/v1/quotes?symbols=",
            "history_invalid_parameter": "/v1/quotes/AAPL/history?interval=bad",
        }
        scenario_values = {}
        with TestClient(app, raise_server_exceptions=False) as client:
            for name, url in requests.items():
                response = client.get(url)
                value = response.json()
                semantic = {}
                if name == "health":
                    semantic["database"] = "filesystem_path"
                elif name == "quotes_default":
                    items = value.get("items", [])
                    semantic["refresh_seen"] = (
                        items[0].get("refresh_seen") if items else None
                    )
                scenario_values[name] = {
                    "status": response.status_code, "shape": shape(value),
                    "semantic": semantic,
                }
        scenarios = {
            "schema": "marketcow.old-main-api-contract.v1.scenarios",
            "capture_tool_version": TOOL_VERSION, "source_commit": commit,
            "generation_input": "isolated-deterministic-fixture:legacy-fixture-v1",
            "scenarios": scenario_values,
        }
        scenarios["sha256"] = digest(scenarios)
        args.scenario_output.write_text(
            json.dumps(scenarios, indent=2, ensure_ascii=False, sort_keys=True) + "\n",
            encoding="utf-8",
        )
    if args.matrix_output:
        captures = {}
        with TestClient(app, raise_server_exceptions=False) as normal_client, \
                TestClient(fault_app, raise_server_exceptions=False) as fault_client:
            for route, detail in sorted(routes.items()):
                method, path, query, body = request_for(route, detail)
                base = {"method": method, "path": path, "query": query, "json": body}
                variants = [("normal", normal_client, base)]
                if method == "GET":
                    empty = {**base, "query": dict(query)}
                    empty["query"]["refresh"] = "false"
                    if any(item["name"] == "symbols" for item in detail["parameters"]):
                        empty["query"]["symbols"] = "MISSING"
                    if any(item["name"] == "q" for item in detail["parameters"]):
                        empty["query"]["q"] = ""
                    variants.append(("empty", normal_client, empty))
                if detail["parameters"] or detail["request_body"]:
                    invalid = {**base, "query": dict(query), "json": body}
                    required = next((item["name"] for item in detail["parameters"]
                                     if item["in"] == "query" and item["required"]), None)
                    if required:
                        invalid["query"].pop(required, None)
                    elif any(item["name"] == "limit" for item in detail["parameters"]):
                        invalid["query"]["limit"] = "0"
                    elif detail["request_body"]:
                        invalid["json"] = {}
                    else:
                        invalid["query"]["__invalid"] = "true"
                    variants.append(("validation_error", normal_client, invalid))
                if route == "GET /v1/quotes":
                    variants.append(("partial_failure", fault_client, base))
                elif route != "GET /v1/health":
                    variants.append(("backend_failure", fault_client, base))
                for kind, client, request in variants:
                    url = request["path"]
                    if request["query"]:
                        url += "?" + urlencode(request["query"])
                    response = client.request(request["method"], url,
                                              json=request["json"])
                    try:
                        value = response.json()
                    except ValueError:
                        value = {"non_json": True}
                    captures[f"{route}::{kind}"] = {
                        "route": route, "kind": kind, "request": request,
                        "status": response.status_code, "shape": shape(value),
                        "semantics": semantics(value),
                    }
        matrix = {
            "schema": "marketcow.old-main-api-contract.v1.route-matrix",
            "capture_tool_version": TOOL_VERSION, "source_commit": commit,
            "generation_input": "isolated-deterministic-fixture:route-matrix-v1",
            "captures": captures,
        }
        matrix["sha256"] = digest(matrix)
        args.matrix_output.write_text(
            json.dumps(matrix, indent=2, ensure_ascii=False, sort_keys=True) + "\n",
            encoding="utf-8",
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
