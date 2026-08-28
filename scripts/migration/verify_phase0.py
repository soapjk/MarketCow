from __future__ import annotations

import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
PLAN = Path("/Volumes/T9/projects/marketcow-workitem-collaboration-7424ef29-33e7-418f-8df0-83afc6caf0/artifacts/objective-marketcow-rust-python-migration-technical-plan.md")
EXPECTED = "62d1a0418eedf6dbf00c37dabce827e0cf1b4d3c8fc43536ffe3e0421d3742d4"


def verify_b16f_exact_scope(manifest: dict[str, object]) -> tuple[bool, list[str]]:
    errors: list[str] = []
    scope = manifest.get("b16f_exact_scope")
    if not isinstance(scope, dict):
        return False, ["b16f_exact_scope: missing or invalid"]

    root_value = scope.get("root")
    if not isinstance(root_value, str):
        return False, ["b16f_exact_scope.root: missing or invalid"]
    root = Path(root_value)
    if not root.is_absolute():
        errors.append("b16f_exact_scope.root: must be absolute")

    files = scope.get("files")
    if not isinstance(files, list) or not files:
        return False, errors + ["b16f_exact_scope.files: missing or empty"]

    for entry in files:
        if not isinstance(entry, dict):
            errors.append("b16f_exact_scope.files: invalid entry")
            continue
        relative_value = entry.get("path")
        if not isinstance(relative_value, str):
            errors.append("b16f_exact_scope.files: path missing or invalid")
            continue
        relative = Path(relative_value)
        if relative.is_absolute() or ".." in relative.parts:
            errors.append(f"b16f_exact_scope.files: unsafe path {relative_value!r}")
            continue
        candidate = root / relative
        if not candidate.is_file():
            errors.append(f"b16f_exact_scope.files: missing {candidate}")
            continue
        expected_bytes = entry.get("bytes")
        if not isinstance(expected_bytes, int) or candidate.stat().st_size != expected_bytes:
            errors.append(f"b16f_exact_scope.files: byte count mismatch for {candidate}")
        expected_sha256 = entry.get("sha256")
        actual_sha256 = hashlib.sha256(candidate.read_bytes()).hexdigest()
        if not isinstance(expected_sha256, str) or actual_sha256 != expected_sha256:
            errors.append(f"b16f_exact_scope.files: SHA-256 mismatch for {candidate}")

    return not errors, errors


def main() -> None:
    actual = hashlib.sha256(PLAN.read_bytes()).hexdigest()
    manifest = json.loads((ROOT / "artifacts/rust-python-migration/phase0/replay-corpus/manifest.json").read_text())
    b16f_verified, b16f_errors = verify_b16f_exact_scope(manifest)
    missing = [value for value in manifest["fixtures"] if not (ROOT / value).is_file()]
    required = [
        "docs/architecture/migration/phase0-decisions.md",
        "docs/architecture/migration/domain-ownership.yaml",
        "docs/architecture/migration/frozen-contracts.md",
        "artifacts/rust-python-migration/phase0/baseline.json",
    ]
    missing += [value for value in required if not (ROOT / value).is_file()]
    local_contract_checks_passed = actual == EXPECTED and not missing and b16f_verified
    result = {
        "schema_version": "marketcow.phase0-verification.v1",
        "passed": local_contract_checks_passed,
        "local_contract_checks_passed": local_contract_checks_passed,
        "plan_sha256": actual,
        "missing": missing,
        "b16f_exact_scope_available": manifest["b16f_exact_scope"]["availability"]
        == "present-as-external-local-read-only-corpus",
        "b16f_exact_scope_verified": b16f_verified,
        "b16f_exact_scope_errors": b16f_errors,
        "raw_r2_available": manifest["r2_cursor_range"]["availability"] != "not-present-in-repository",
        "raw_r2_is_acceptance_blocker": True,
        "phase0_complete": False,
    }
    print(json.dumps(result, indent=2, sort_keys=True))
    if not result["passed"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
