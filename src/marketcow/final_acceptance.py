from __future__ import annotations

import json
import os
import subprocess
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from .local_backup import _assert_no_sensitive, _fsync_dir, _hash, _json


FINAL_ACCEPTANCE_VERSION = "storage-v2.final-acceptance.v1"
ACCEPTED_ARTIFACTS = {
    "SV2-012": "ed0e55f", "SV2-013": "ccda668", "SV2-014A": "8f33237",
    "SV2-014B": "34efd83", "SV2-015": "de2c70f", "SV2-016": "9766d7b",
    "SV2-017": "6d9fc6e", "SV2-018": "52ecd0c", "SV2-019A": "a0ce718",
    "SV2-019B": "7d8c646", "SV2-020A": "0a34202", "SV2-020B": "134c032",
    "SV2-021A": "25f833f", "SV2-021B": "8b6aa74", "SV2-022A": "6b44732",
    "SV2-022B": "8a376d9", "SV2-023": "9a685e8", "SV2-024": "1ce0fe5",
}
MAX_OUTPUT = 4000


@dataclass(frozen=True)
class FinalAcceptanceInputs:
    root: Path
    allowed_root: Path
    repository_root: Path
    readiness_package: Path
    commands: Mapping[str, Sequence[str]]
    profile: str = "development"


def default_commands() -> dict[str, tuple[str, ...]]:
    python = ("uv", "run", "python", "-m", "unittest")
    return {
        "default_suite": (*python, "discover", "-s", "tests", "-q"),
        "final_contract": (*python, "tests.test_storage_v2_contract_gate", "-q"),
        "postgres": (*python, "tests.test_postgres_repositories", "-q"),
        "clickhouse": (*python, "tests.test_clickhouse_repositories", "-q"),
        "restore_backfill_switch_benchmark": (
            *python, "tests.test_local_restore.LocalStorageRestoreIntegrationTest",
            "tests.test_local_backfill.LocalStorageBackfillIntegrationTest",
            "tests.test_local_read_switch.LocalReadSwitchComposedIntegrationTest",
            "tests.test_storage_v2_benchmark.StorageV2BenchmarkIntegrationTest", "-q",
        ),
        "ruff": ("uv", "run", "ruff", "check", "src", "tests"),
        "diff_check": ("git", "diff", "--check"),
    }


class LocalFinalAcceptance:
    def __init__(self, inputs: FinalAcceptanceInputs,
                 runner: Callable[..., Any] = subprocess.run) -> None:
        if inputs.profile not in {"development", "test"}:
            raise ValueError("final acceptance is development/test-only")
        self.allowed_root = Path(inputs.allowed_root).resolve()
        self.root = Path(inputs.root).resolve()
        self.repository = Path(inputs.repository_root).resolve()
        self.readiness = Path(inputs.readiness_package).resolve()
        for path, label in ((self.root, "root"), (self.readiness, "readiness package")):
            try:
                path.relative_to(self.allowed_root)
            except ValueError as error:
                raise ValueError(f"final acceptance {label} escapes allowed root") from error
        if not self.root.name.endswith(("development", "test")):
            raise ValueError("final acceptance root must be isolated")
        if not self.readiness.is_dir() or self.readiness.is_symlink():
            raise ValueError("final acceptance readiness package is missing or unsafe")
        self.inputs, self.runner = inputs, runner
        self.report_path = self.root / "storage-v2-final-acceptance.json"

    def _git(self, *args: str) -> str:
        result = subprocess.run(["git", "-C", str(self.repository), *args],
                                capture_output=True, text=True, check=False)
        if result.returncode:
            raise ValueError("final acceptance Git audit failed")
        return result.stdout.strip()

    def preflight(self) -> dict[str, Any]:
        head = self._git("rev-parse", "HEAD")
        commits = {}
        for item, commit in ACCEPTED_ARTIFACTS.items():
            full = self._git("rev-parse", f"{commit}^{{commit}}")
            self._git("merge-base", "--is-ancestor", full, head)
            commits[item] = full
        document = (self.repository / "docs/development-handoff-storage-v2.md").read_text()
        for item, commit in ACCEPTED_ARTIFACTS.items():
            section = document.split(f"### `{item}`", 1)[1].split("### `", 1)[0]
            if "**状态**：`已验收`" not in section or commit not in section:
                raise ValueError(f"final acceptance document is stale for {item}")
        external = document.split("### `SV2-EXT`", 1)[1]
        if "`需用户授权`" not in external or "不得因前置项完成而自动开始" not in external:
            raise ValueError("final acceptance external authorization boundary is missing")
        manifest = json.loads((self.readiness / "manifest.json").read_text())
        package = (self.readiness / "package.json").read_bytes()
        runbook = (self.readiness / "RUNBOOK.md").read_bytes()
        unsigned = dict(manifest)
        signature = unsigned.pop("manifest_sha256", None)
        if (signature != _hash(_json(unsigned)) or manifest.get("package_sha256") != _hash(package)
                or manifest.get("runbook_sha256") != _hash(runbook)
                or manifest.get("release_commit") != commits["SV2-024"]):
            raise ValueError("final acceptance readiness binding failed")
        readiness = json.loads(package)
        if (readiness.get("production_connections_attempted") is not False or
                readiness.get("state_changes_executed") is not False or
                any(action.get("authorized") or action.get("executed")
                    for action in readiness.get("external_actions", []))):
            raise ValueError("final acceptance readiness authorization boundary failed")
        return {"head": head, "artifacts": commits,
                "readiness_manifest_sha256": signature,
                "external_actions": "unauthorized_unexecuted"}

    def run(self) -> dict[str, Any]:
        preflight = self.preflight()
        checks = []
        for name, command in self.inputs.commands.items():
            started = time.monotonic()
            result = self.runner(command, cwd=self.repository, capture_output=True,
                                 text=True, check=False, timeout=900, env=os.environ.copy())
            output = ((result.stdout or "") + (result.stderr or ""))[-MAX_OUTPUT:]
            checks.append({"name": name, "returncode": result.returncode,
                           "duration_seconds": round(time.monotonic() - started, 6),
                           "output_sha256": _hash(output.encode())})
            if result.returncode:
                raise RuntimeError(f"final acceptance check failed: {name}")
        report = {
            "version": FINAL_ACCEPTANCE_VERSION, "status": "passed",
            "head": preflight["head"], "accepted_artifacts": preflight["artifacts"],
            "readiness_manifest_sha256": preflight["readiness_manifest_sha256"],
            "checks": checks, "limitations": [
                "local synthetic/disposable evidence only",
                "SV2-EXT production and remote actions remain separately authorized",
            ],
            "production_connections_attempted": False, "remote_writes_executed": False,
            "external_actions": "unauthorized_unexecuted",
        }
        _assert_no_sensitive(report, "final acceptance report")
        self.root.mkdir(parents=True, exist_ok=True)
        descriptor, temporary = tempfile.mkstemp(dir=self.root, prefix=".final-acceptance-")
        try:
            with os.fdopen(descriptor, "wb") as handle:
                handle.write(_json(report)); handle.flush(); os.fsync(handle.fileno())
            os.replace(temporary, self.report_path); _fsync_dir(self.root)
        finally:
            if os.path.exists(temporary):
                os.unlink(temporary)
        return report
