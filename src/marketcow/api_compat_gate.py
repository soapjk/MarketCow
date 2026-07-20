from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Mapping


SCHEMA_VERSION = "marketcow.old-main-api-contract.v1"
DIFF_VERSION = "marketcow.old-main-v2-api-differences.v1"
MAX_DIFFERENCES = 1000


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


def capture_openapi_contract(app: Any) -> dict[str, Any]:
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
    result = {"schema": SCHEMA_VERSION, "routes": routes}
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


def capture_scenarios(client: Any) -> dict[str, Any]:
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
    result = {"schema": SCHEMA_VERSION + ".scenarios", "scenarios": scenarios}
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
    observed = contract_differences(
        {key: value for key, value in legacy.items() if key != "sha256"},
        {key: value for key, value in v2.items() if key != "sha256"},
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
