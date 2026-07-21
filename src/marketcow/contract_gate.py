from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
import re
from typing import Any, Mapping, Sequence

from .telemetry import telemetry_call


CONTRACT_MATRIX = {
    "recent": ("bars", "cache", "ordering", "provenance"),
    "range": ("bars", "truncated", "cache", "ordering", "provenance"),
    "canonical_page": ("bars", "cursor", "truncated", "cache", "provenance"),
    "exact_cross_section_page": ("bars", "cursor", "truncated", "cache"),
    "matrix": ("bars", "cursor", "truncated", "cache"),
    "raw_range": ("bars", "truncated", "cache", "provenance"),
    "raw_page": ("bars", "cursor", "truncated", "cache", "provenance"),
    "single_as_of": ("bars", "effective_time", "cache"),
    "cross_section_as_of": ("bars", "cursor", "effective_time", "cache"),
}

# Backend diagnostics describe how a result was obtained, not its data contract.  The
# paths are deliberately exact: identically named fields inside bars remain data.
ROUTING_DIAGNOSTIC_PATHS = frozenset({
    "$.backend", "$.attempted_backend", "$.fallback", "$.error",
    "$.diagnostics.backend", "$.diagnostics.attempted_backend",
    "$.diagnostics.fallback", "$.diagnostics.error",
})
# Legacy provider JSON is excluded only where a query result actually carries a bar.
# $[].source_payload is a bare row list, $[][].source_payload is a (rows, truncated)
# repository tuple, and $.bars[].source_payload is an API response.
LEGACY_PAYLOAD_PATHS = frozenset({
    "$[].source_payload", "$[][].source_payload", "$.bars[].source_payload",
})
MAX_MISMATCHES = 50
MAX_VALUE_TEXT = 500


@dataclass(frozen=True)
class ContractMismatch:
    path: str
    expected: Any
    actual: Any


def _utc_iso(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat()


def normalize_contract_value(value: Any) -> Any:
    """Normalize backend representation differences without changing semantics."""
    if isinstance(value, datetime):
        return _utc_iso(value)
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        value = float(value)
    if isinstance(value, float):
        if value == 0:
            return 0.0
        return float(format(value, ".15g"))
    if isinstance(value, str) and "T" in value:
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            pass
        else:
            if parsed.tzinfo is not None:
                return _utc_iso(parsed)
    if isinstance(value, Mapping):
        return {
            str(key): normalize_contract_value(item)
            for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))
        }
    if isinstance(value, (list, tuple)):
        return [normalize_contract_value(item) for item in value]
    return value


def _safe(value: Any) -> Any:
    if isinstance(value, str) and len(value) > MAX_VALUE_TEXT:
        return value[:MAX_VALUE_TEXT] + "..."
    if isinstance(value, Mapping):
        return {str(key): _safe(item) for key, item in list(value.items())[:50]}
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [_safe(item) for item in value[:50]]
    return value


def compare_contract(
    expected: Any, actual: Any, allowed_paths: Sequence[str] = (),
    telemetry: Any = None, contract: str = "range",
) -> dict[str, Any]:
    """Return a bounded, deterministic, data-only mismatch report."""
    ignored = ROUTING_DIAGNOSTIC_PATHS | frozenset(allowed_paths)

    def canonical_path(path: str) -> str:
        return re.sub(r"\[\d+\]", "[]", path)

    def path_is_ignored(path: str) -> bool:
        path = canonical_path(path)
        return any(
            path == prefix or path.startswith(prefix + ".") or path.startswith(prefix + "[")
            for prefix in ignored
        )

    def without_allowed(value: Any, path: str = "$") -> Any:
        if isinstance(value, Mapping):
            return {
                key: without_allowed(item, f"{path}.{key}")
                for key, item in value.items()
                if not path_is_ignored(f"{path}.{key}")
            }
        if isinstance(value, list):
            return [
                without_allowed(item, f"{path}[{index}]")
                for index, item in enumerate(value)
                if not path_is_ignored(f"{path}[{index}]")
            ]
        return value

    left = without_allowed(normalize_contract_value(expected))
    right = without_allowed(normalize_contract_value(actual))
    mismatches: list[ContractMismatch] = []

    def walk(a: Any, b: Any, path: str) -> None:
        if len(mismatches) >= MAX_MISMATCHES:
            return
        if isinstance(a, dict) and isinstance(b, dict):
            for key in sorted(set(a) | set(b)):
                if key not in a or key not in b:
                    mismatches.append(ContractMismatch(f"{path}.{key}", a.get(key), b.get(key)))
                else:
                    walk(a[key], b[key], f"{path}.{key}")
            return
        if isinstance(a, list) and isinstance(b, list):
            if len(a) != len(b):
                mismatches.append(ContractMismatch(f"{path}.length", len(a), len(b)))
            for index, (left_item, right_item) in enumerate(zip(a, b)):
                walk(left_item, right_item, f"{path}[{index}]")
            return
        if a != b:
            mismatches.append(ContractMismatch(path, a, b))

    walk(left, right, "$")
    report = {
        "status": "ok" if not mismatches else "mismatch",
        "mismatch_count": len(mismatches),
        "truncated": len(mismatches) >= MAX_MISMATCHES,
        "mismatches": [
            {"path": item.path, "expected": _safe(item.expected), "actual": _safe(item.actual)}
            for item in mismatches
        ],
    }
    if mismatches and telemetry is not None:
        telemetry_call(
            telemetry, "safe",
            "counter", "contract_mismatch_total", min(len(mismatches), MAX_MISMATCHES),
            contract=contract,
        )
    return report


def assert_contract_equal(
    expected: Any, actual: Any, label: str = "contract",
    allowed_paths: Sequence[str] = (),
) -> None:
    report = compare_contract(expected, actual, allowed_paths)
    if report["status"] != "ok":
        raise AssertionError(f"{label} mismatch: {report}")
