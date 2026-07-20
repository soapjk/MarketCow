from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import urlencode


SCHEMA_VERSION = "marketcow.old-main-api-contract.v1"
DIFF_VERSION = "marketcow.old-main-v2-api-differences.v1"
CAPTURE_TOOL_VERSION = "marketcow.api-compat-capture.v2"
LEGACY_SOURCE_COMMIT = "701ffbde1b25ae587845ea2bd021ca8fa12b93b4"
MAX_DIFFERENCES = 1000
STRUCTURED_ERROR_FIELDS = frozenset({"detail", "error", "code", "message"})
PARAMETER_FIXTURES = {
    "symbols": "AAPL", "symbol": "AAPL", "q": "AAPL",
    "bar_at": "2026-01-02T00:00:00Z",
    "bar_ats": "2026-01-02T00:00:00Z",
    "as_of": "2026-01-02T00:00:00Z",
    "start": "2026-01-01T00:00:00Z", "end": "2026-01-02T00:00:00Z",
    "from": "2026-01-01", "to": "2026-01-02",
    "report_period": "20241231", "api_name": "daily",
}


def _canonical(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _canonical(item) for key, item in sorted(value.items())}
    if isinstance(value, list):
        return [_canonical(item) for item in value]
    return value


def sha256_json(value: Any) -> str:
    encoded = json.dumps(
        _canonical(value), ensure_ascii=False, allow_nan=False,
        sort_keys=True, separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def capture_openapi_contract(
    app: Any, *, source_commit: str = "working-tree",
    generation_input: str = "isolated-fastapi-openapi",
) -> dict[str, Any]:
    """Capture public route syntax without importing a backend implementation."""
    schema = app.openapi()
    routes: dict[str, Any] = {}
    for path, operations in sorted(schema.get("paths", {}).items()):
        for method, operation in sorted(operations.items()):
            if method.upper() not in {"GET", "POST", "PUT", "PATCH", "DELETE"}:
                continue
            parameters = []
            for parameter in operation.get("parameters", []):
                item = {
                    "name": parameter.get("name"), "in": parameter.get("in"),
                    "required": bool(parameter.get("required", False)),
                    "schema": _canonical(parameter.get("schema", {})),
                }
                parameters.append(item)
            parameters.sort(key=lambda item: (item["in"], item["name"]))
            responses = {
                str(status): _canonical({
                    "content": detail.get("content", {}),
                })
                for status, detail in sorted(operation.get("responses", {}).items())
            }
            routes[f"{method.upper()} {path}"] = {
                "parameters": parameters,
                "request_body": _canonical(operation.get("requestBody")),
                "responses": responses,
            }
    result = {
        "schema": SCHEMA_VERSION,
        "capture_tool_version": CAPTURE_TOOL_VERSION,
        "source_commit": source_commit,
        "generation_input": generation_input,
        "routes": routes,
    }
    result["sha256"] = sha256_json(result)
    return result


def response_shape(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): response_shape(item) for key, item in sorted(value.items())}
    if isinstance(value, list):
        return {"type": "array", "items": response_shape(value[0]) if value else None}
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "integer"
    if isinstance(value, float):
        return "number"
    return "string"


def response_semantics(value: Any) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        return {"count": None, "empty_collections": [],
                "structured_error_fields": [], "has_partial_errors": False}
    empty = [str(key) for key, item in sorted(value.items())
             if isinstance(item, list) and not item]
    count = value.get("count")
    error_fields = sorted(STRUCTURED_ERROR_FIELDS.intersection(value))
    errors = value.get("errors")
    return {
        "count": count if isinstance(count, int) and not isinstance(count, bool) else None,
        "empty_collections": empty,
        "has_detail": "detail" in value,
        "structured_error_fields": error_fields,
        "has_structured_error": bool(error_fields),
        "has_partial_errors": isinstance(errors, (list, dict)) and bool(errors),
    }


def route_request(route: str, parameters: list[Mapping[str, Any]]) -> dict[str, Any]:
    """Produce the stable isolated-fixture request used by both captures."""
    method, template = route.split(" ", 1)
    path = template
    query: dict[str, str] = {}
    for parameter in parameters:
        name = str(parameter.get("name"))
        location = parameter.get("in")
        value = PARAMETER_FIXTURES.get(name, "1")
        if location == "path":
            path = path.replace("{" + name + "}", value)
        elif location == "query" and parameter.get("required"):
            query[name] = value
    if route == "GET /v1/quotes":
        query["symbols"] = "AAPL"
    body = None
    if method == "POST" and template == "/v1/tushare/realtime-quote":
        body = {"ts_code": "AAPL"}
    elif method == "POST" and template == "/v1/tushare/{api_name}":
        body = {"params": {}, "fields": ""}
    return {"method": method, "path": path, "query": query, "json": body}


def capture_route_inventory(client: Any, contract: Mapping[str, Any]) -> dict[str, Any]:
    captures = {}
    for route, detail in sorted(contract.get("routes", {}).items()):
        request = route_request(route, detail.get("parameters", []))
        url = request["path"]
        if request["query"]:
            url += "?" + urlencode(request["query"])
        response = client.request(request["method"], url, json=request["json"])
        try:
            body = response.json()
        except ValueError:
            body = {"non_json": True}
        captures[route] = {
            "request": request, "status": response.status_code,
            "shape": response_shape(body),
        }
    result = {"schema": SCHEMA_VERSION + ".route-scenarios", "captures": captures}
    result["sha256"] = sha256_json(result)
    return result


def capture_route_matrix(
    normal_client: Any, fault_client: Any, contract: Mapping[str, Any],
) -> dict[str, Any]:
    """Execute every applicable route scenario, preserving exact status and shape."""
    captures = {}
    for route, detail in sorted(contract.get("routes", {}).items()):
        base = route_request(route, detail.get("parameters", []))
        variants: list[tuple[str, Any, dict[str, Any]]] = [
            ("normal", normal_client, base),
        ]
        method, _path = route.split(" ", 1)
        if method == "GET":
            empty = {**base, "query": dict(base["query"])}
            empty["query"].update({"refresh": "false"})
            if any(item.get("name") == "symbols" for item in detail["parameters"]):
                empty["query"]["symbols"] = "MISSING"
            if any(item.get("name") == "q" for item in detail["parameters"]):
                empty["query"]["q"] = ""
            variants.append(("empty", normal_client, empty))
        if detail.get("parameters") or detail.get("request_body"):
            invalid = {**base, "query": dict(base["query"]), "json": base["json"]}
            required_query = next((
                item["name"] for item in detail["parameters"]
                if item.get("in") == "query" and item.get("required")
            ), None)
            if required_query:
                invalid["query"].pop(required_query, None)
            elif any(item.get("name") == "limit" for item in detail["parameters"]):
                invalid["query"]["limit"] = "0"
            elif detail.get("request_body"):
                invalid["json"] = {}
            else:
                invalid["query"]["__invalid"] = "true"
            variants.append(("validation_error", normal_client, invalid))
        if route == "GET /v1/quotes":
            variants.append(("partial_failure", fault_client, base))
        elif route not in {"GET /v1/health", "GET /v1/readiness"}:
            variants.append(("backend_failure", fault_client, base))
        for kind, client, request in variants:
            url = request["path"]
            if request["query"]:
                url += "?" + urlencode(request["query"])
            response = client.request(request["method"], url, json=request["json"])
            try:
                body = response.json()
            except ValueError:
                body = {"non_json": True}
            captures[f"{route}::{kind}"] = {
                "route": route, "kind": kind, "request": request,
                "status": response.status_code, "shape": response_shape(body),
                "semantics": response_semantics(body),
            }
    result = {"schema": SCHEMA_VERSION + ".route-matrix", "captures": captures}
    result["sha256"] = sha256_json(result)
    return result


def capture_scenarios(
    client: Any, *, source_commit: str = "working-tree",
    generation_input: str = "isolated-deterministic-fixture",
) -> dict[str, Any]:
    requests = {
        "health": ("/v1/health", {}),
        "quotes_default": ("/v1/quotes?symbols=AAPL", {}),
        "quotes_empty": ("/v1/quotes?symbols=MISSING&refresh=false", {}),
        "quotes_batch_error": ("/v1/quotes?symbols=AAPL,FAIL&refresh=true", {}),
        "quotes_backend_failure": ("/v1/quotes?symbols=FAIL&refresh=true", {}),
        "quotes_missing_parameter": ("/v1/quotes?symbols=", {}),
        "history_invalid_parameter": ("/v1/quotes/AAPL/history?interval=bad", {}),
    }
    scenarios = {}
    for name, (path, kwargs) in requests.items():
        response = client.get(path, **kwargs)
        body = response.json()
        semantic = {}
        if name == "health":
            database = body.get("database")
            semantic["database"] = (
                "postgresql_clickhouse_logical" if isinstance(database, str) and
                database.startswith("postgresql://") else "filesystem_path"
            )
        elif name == "quotes_default":
            items = body.get("items", [])
            semantic["refresh_seen"] = items[0].get("refresh_seen") if items else None
        scenarios[name] = {
            "status": response.status_code,
            "shape": response_shape(body), "semantic": semantic,
        }
    result = {
        "schema": SCHEMA_VERSION + ".scenarios",
        "capture_tool_version": CAPTURE_TOOL_VERSION,
        "source_commit": source_commit,
        "generation_input": generation_input,
        "scenarios": scenarios,
    }
    result["sha256"] = sha256_json(result)
    return result


def load_document(path: Path, expected_schema: str) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict) or value.get("schema") != expected_schema:
        raise ValueError("API contract document schema is invalid")
    expected = value.get("sha256")
    unsigned = {key: item for key, item in value.items() if key != "sha256"}
    if expected != sha256_json(unsigned):
        raise ValueError("API contract document checksum mismatch")
    return value


def validate_coverage_inventory(
    inventory: Mapping[str, Any], legacy: Mapping[str, Any], v2: Mapping[str, Any],
    legacy_matrix: Mapping[str, Any], v2_matrix: Mapping[str, Any],
) -> dict[str, Any]:
    """Fail closed unless every public route has an explicit two-sided scenario plan."""
    entries = inventory.get("routes")
    if not isinstance(entries, list):
        raise ValueError("API coverage inventory routes are invalid")
    legacy_routes = set(legacy.get("routes", {}))
    v2_routes = set(v2.get("routes", {}))
    expected = legacy_routes | v2_routes
    seen: set[str] = set()
    scenario_ids: set[str] = set()
    errors: list[str] = []
    if inventory.get("legacy_matrix_sha256") != legacy_matrix.get("sha256"):
        errors.append("legacy-matrix-checksum")
    if inventory.get("v2_matrix_sha256") != v2_matrix.get("sha256"):
        errors.append("v2-matrix-checksum")
    matrices = {"legacy": legacy_matrix.get("captures", {}),
                "v2": v2_matrix.get("captures", {})}
    for entry in entries:
        route = entry.get("route") if isinstance(entry, Mapping) else None
        if not isinstance(route, str) or route in seen:
            errors.append(f"duplicate-or-invalid-route:{route}")
            continue
        seen.add(route)
        scenarios = entry.get("scenarios")
        if not isinstance(scenarios, list) or not scenarios:
            errors.append(f"missing-scenarios:{route}")
            continue
        for scenario in scenarios:
            if not isinstance(scenario, Mapping):
                errors.append(f"invalid-scenario:{route}")
                continue
            scenario_id = scenario.get("id")
            if not isinstance(scenario_id, str) or scenario_id in scenario_ids:
                errors.append(f"duplicate-or-invalid-scenario:{scenario_id}")
            else:
                scenario_ids.add(scenario_id)
            kind = scenario.get("kind")
            if kind not in {
                "normal", "empty", "validation_error", "backend_failure",
                "partial_failure",
            }:
                errors.append(f"invalid-scenario-kind:{route}:{scenario_id}")
            if scenario.get("capture_key") != f"{route}::{kind}":
                errors.append(f"invalid-capture-key:{route}:{scenario_id}")
            for side, present in (("legacy", route in legacy_routes),
                                  ("v2", route in v2_routes)):
                state = scenario.get(side)
                if present and state not in {"executed", "not_applicable"}:
                    errors.append(f"unexecuted:{side}:{route}:{scenario_id}")
                if present and state == "not_applicable" and not scenario.get(
                    side + "_reason"
                ):
                    errors.append(
                        f"missing-side-not-applicable-reason:{side}:{route}:{scenario_id}"
                    )
                if not present and state != "route_absent":
                    errors.append(f"missing-route-absence:{side}:{route}:{scenario_id}")
                if state == "executed":
                    capture = matrices[side].get(f"{route}::{kind}")
                    if not isinstance(capture, Mapping):
                        errors.append(f"missing-capture:{side}:{route}:{kind}")
                        continue
                    if capture.get("route") != route or capture.get("kind") != kind:
                        errors.append(f"capture-binding:{side}:{route}:{kind}")
                    if scenario.get(side + "_capture_sha256") != sha256_json(capture):
                        errors.append(f"capture-checksum:{side}:{route}:{kind}")
                    status = capture.get("status")
                    semantics = capture.get("semantics", {})
                    valid = False
                    if kind == "normal":
                        valid = isinstance(status, int) and 200 <= status < 300
                    elif kind == "validation_error":
                        valid = (isinstance(status, int) and 400 <= status < 500
                                 and semantics.get("has_detail") is True)
                    elif kind == "backend_failure":
                        valid = (isinstance(status, int) and 500 <= status < 600
                                 and semantics.get("has_structured_error") is True)
                    elif kind == "partial_failure":
                        valid = (isinstance(status, int) and 200 <= status < 300
                                 and semantics.get("has_partial_errors") is True)
                    elif kind == "empty":
                        valid = (isinstance(status, int) and 200 <= status < 300
                                 and (semantics.get("count") == 0 or bool(
                                     semantics.get("empty_collections")
                                 )))
                    if not valid:
                        errors.append(f"semantic-mismatch:{side}:{route}:{kind}")
        applicability = entry.get("applicability")
        if not isinstance(applicability, Mapping):
            errors.append(f"missing-applicability:{route}")
            continue
        for kind in (
            "normal", "empty", "validation_error", "backend_failure",
            "partial_failure",
        ):
            value = applicability.get(kind)
            if not isinstance(value, Mapping) or value.get("status") not in {
                "covered", "not_applicable",
            }:
                errors.append(f"missing-applicability:{route}:{kind}")
            elif value.get("status") == "not_applicable" and not value.get("reason"):
                errors.append(f"missing-not-applicable-reason:{route}:{kind}")
            elif value.get("status") == "covered" and not any(
                item.get("kind") == kind for item in scenarios
                if isinstance(item, Mapping)
            ):
                errors.append(f"covered-without-scenario:{route}:{kind}")
    errors.extend(f"unmapped-route:{route}" for route in sorted(expected - seen))
    errors.extend(f"unknown-route:{route}" for route in sorted(seen - expected))
    return {
        "status": "ok" if not errors else "mismatch",
        "route_count": len(seen), "scenario_count": len(scenario_ids),
        "errors": errors[:100], "truncated": len(errors) > 100,
    }


def contract_differences(legacy: Any, v2: Any) -> list[dict[str, Any]]:
    differences: list[dict[str, Any]] = []

    def walk(left: Any, right: Any, path: str) -> None:
        if len(differences) >= MAX_DIFFERENCES:
            return
        if isinstance(left, dict) and isinstance(right, dict):
            for key in sorted(set(left) | set(right)):
                child = f"{path}.{key}"
                if key not in left:
                    differences.append({"path": child, "kind": "v2_added",
                                        "legacy": None, "v2": right[key]})
                elif key not in right:
                    differences.append({"path": child, "kind": "v2_missing",
                                        "legacy": left[key], "v2": None})
                else:
                    walk(left[key], right[key], child)
            return
        if isinstance(left, list) and isinstance(right, list):
            if len(left) != len(right):
                differences.append({"path": path + ".length", "kind": "changed",
                                    "legacy": len(left), "v2": len(right)})
            for index, (old, new) in enumerate(zip(left, right)):
                walk(old, new, f"{path}[{index}]")
            return
        if left != right:
            differences.append({"path": path, "kind": "changed",
                                "legacy": left, "v2": right})

    walk(_canonical(legacy), _canonical(v2), "$")
    return differences


def run_gate(legacy: Mapping[str, Any], v2: Mapping[str, Any],
             decision_document: Mapping[str, Any]) -> dict[str, Any]:
    excluded = {"sha256", "capture_tool_version", "source_commit",
                "generation_input"}
    observed = contract_differences(
        {key: value for key, value in legacy.items() if key not in excluded},
        {key: value for key, value in v2.items() if key not in excluded},
    )
    declared = decision_document.get("differences")
    if not isinstance(declared, list):
        raise ValueError("API difference document is invalid")
    observed_by_path = {item["path"]: item for item in observed}
    declared_by_path = {item.get("path"): item for item in declared}
    if len(declared_by_path) != len(declared) or None in declared_by_path:
        raise ValueError("API difference paths must be unique and explicit")
    mismatches = []
    for path in sorted(set(observed_by_path) | set(declared_by_path)):
        actual = observed_by_path.get(path)
        expected = declared_by_path.get(path)
        if actual is None or expected is None:
            mismatches.append({"path": path, "observed": actual, "declared": expected})
            continue
        for key in ("kind", "legacy", "v2"):
            if expected.get(key) != actual.get(key):
                mismatches.append({"path": path, "observed": actual,
                                   "declared": expected})
                break
        if not expected.get("bg011_action") or not expected.get("reason"):
            mismatches.append({"path": path, "observed": actual,
                               "declared": "missing BG-011 disposition"})
    return {
        "status": "ok" if not mismatches else "mismatch",
        "legacy_routes": len(legacy.get("routes", {})),
        "v2_routes": len(v2.get("routes", {})),
        "difference_count": len(observed),
        "mismatches": mismatches[:100],
        "truncated": len(mismatches) > 100 or len(observed) >= MAX_DIFFERENCES,
    }
