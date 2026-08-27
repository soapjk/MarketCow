from __future__ import annotations

import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
PLAN = Path("/Volumes/T9/projects/marketcow-workitem-collaboration-7424ef29-33e7-418f-8df0-83afc6caf0/artifacts/objective-marketcow-rust-python-migration-technical-plan.md")
EXPECTED = "62d1a0418eedf6dbf00c37dabce827e0cf1b4d3c8fc43536ffe3e0421d3742d4"


def main() -> None:
    actual = hashlib.sha256(PLAN.read_bytes()).hexdigest()
    manifest = json.loads((ROOT / "artifacts/rust-python-migration/phase0/replay-corpus/manifest.json").read_text())
    missing = [value for value in manifest["fixtures"] if not (ROOT / value).is_file()]
    required = [
        "docs/architecture/migration/phase0-decisions.md",
        "docs/architecture/migration/domain-ownership.yaml",
        "docs/architecture/migration/frozen-contracts.md",
        "artifacts/rust-python-migration/phase0/baseline.json",
    ]
    missing += [value for value in required if not (ROOT / value).is_file()]
    result = {
        "schema_version": "marketcow.phase0-verification.v1",
        "passed": actual == EXPECTED and not missing,
        "plan_sha256": actual,
        "missing": missing,
        "raw_r2_available": manifest["r2_cursor_range"]["availability"] != "not-present-in-repository",
        "raw_r2_is_acceptance_blocker": True,
    }
    print(json.dumps(result, indent=2, sort_keys=True))
    if not result["passed"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()

