from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
from datetime import UTC, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def run(command: list[str]) -> dict[str, object]:
    started = datetime.now(UTC)
    result = subprocess.run(command, cwd=ROOT, text=True, capture_output=True, check=False)
    output = result.stdout + result.stderr
    return {
        "command": command,
        "started_at": started.isoformat(),
        "finished_at": datetime.now(UTC).isoformat(),
        "exit_code": result.returncode,
        "output_sha256": hashlib.sha256(output.encode()).hexdigest(),
        "output_tail": output[-4000:],
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    commands = [
        ["cargo", "fmt", "--all", "--", "--check"],
        ["cargo", "test", "--workspace"],
        ["cargo", "clippy", "--workspace", "--all-targets", "--", "-D", "warnings"],
        ["python3", "-m", "pytest", "-q", "tests/test_rust_python_worker.py", "tests/test_rust_migration_architecture.py"],
        ["python3", "scripts/migration/verify_phase0.py"],
    ]
    checks = [run(command) for command in commands]
    revision = subprocess.run(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True, capture_output=True, check=True).stdout.strip()
    corpus = json.loads((ROOT / "artifacts/rust-python-migration/phase0/replay-corpus/manifest.json").read_text())
    result = {
        "schema_version": "marketcow.local-migration-verification.v1",
        "generated_at": datetime.now(UTC).isoformat(),
        "commit": revision,
        "checks": checks,
        "local_checks_passed": all(check["exit_code"] == 0 for check in checks),
        "raw_r2_available": corpus["r2_cursor_range"]["availability"] != "not-present-in-repository",
        "overall_work_item_complete": False,
        "incomplete_reason": "Raw r2 corpus, full public API/storage cutover, production shadow, 24-hour and 7-day gates are not available or executed.",
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    print(json.dumps({"output": str(args.output), "local_checks_passed": result["local_checks_passed"],
                      "overall_work_item_complete": False}, sort_keys=True))
    if not result["local_checks_passed"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()

