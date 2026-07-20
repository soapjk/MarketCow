"""BG-020 fail-closed local acceptance gate for the pure PG/CH V2 graph."""
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
from .telemetry import sanitize_text

V2_FINAL_VERSION = "storage-v2.pg-ch-final-acceptance.v1"
MAX_OUTPUT = 4096
ACCEPTED_BG_ARTIFACTS = {
    "BG-001": ("c937960", "6b365fe"), "BG-002": ("3fceadc", "30888a9"),
    "BG-003": ("4320b6e", "e162286"), "BG-004": ("ad82092",),
    "BG-005": ("c06e4e8", "2811842"), "BG-006": ("8f1180c",),
    "BG-007": ("7fb9cf8", "5058efd"), "BG-008": ("cddcc58",),
    "BG-009": ("bba6f7e", "6ae6ae1"),
    "BG-010": ("ebf1da0", "f9a46e2", "9a5656a", "b1acd3c"),
    "BG-011": ("0a85013",), "BG-012": ("94b4675", "d236f5f"),
    "BG-013": ("67dbbb6",), "BG-014": ("2a7f75c", "38791a0"),
    "BG-015": ("90279c9", "9b1bc1c"), "BG-016": ("f52e4bf",),
    "BG-017": ("d592d77",), "BG-018": ("f2144fa", "5808848"),
    "BG-019": ("8b84b04",),
}
REQUIRED_COMMANDS = (
    "default_suite", "old_main_api", "online_dependency", "v2_runtime",
    "authoritative_market", "offline_migration", "copy_authorization",
    "backup_restore", "benchmark", "blue_green", "postgres", "clickhouse_contract",
    "ruff", "diff_check", "clean_worktree",
)


def default_commands() -> dict[str, tuple[str, ...]]:
    unit = ("uv", "run", "python", "-m", "unittest")
    return {
        "default_suite": (*unit, "discover", "-s", "tests", "-q"),
        "old_main_api": (*unit, "tests.test_old_main_api_contract", "-q"),
        "online_dependency": (*unit, "tests.test_online_dependency_policy", "-q"),
        "v2_runtime": (*unit, "tests.test_v2_factory", "tests.test_v2_service", "tests.test_v2_health", "-q"),
        "authoritative_market": (*unit, "tests.test_clickhouse_writer", "tests.test_clickhouse_scheduler", "tests.test_clickhouse_direct_repository", "-q"),
        "offline_migration": (*unit, "tests.test_offline_full_import", "tests.test_offline_incremental_catchup", "-q"),
        "copy_authorization": (*unit, "tests.test_offline_copy_validator", "-q"),
        "backup_restore": (*unit, "tests.test_v2_backup_restore", "-q"),
        "benchmark": (*unit, "tests.test_v2_benchmark", "-q"),
        "blue_green": (*unit, "tests.test_v2_blue_green", "-q"),
        "postgres": (*unit, "tests.test_postgres_repositories", "-q"),
        "clickhouse_contract": (*unit, "tests.test_clickhouse_repositories", "tests.test_storage_v2_contract_gate", "-q"),
        "ruff": ("uv", "run", "ruff", "check", "src", "tests"),
        "diff_check": ("git", "diff", "--check"),
        "clean_worktree": ("git", "status", "--porcelain"),
    }


@dataclass(frozen=True)
class V2FinalAcceptanceInputs:
    root: Path
    allowed_root: Path
    repository_root: Path
    commands: Mapping[str, Sequence[str]]
    profile: str = "v2-test"


class V2FinalAcceptance:
    def __init__(self, inputs: V2FinalAcceptanceInputs,
                 runner: Callable[..., Any] = subprocess.run) -> None:
        if inputs.profile not in {"v2-test", "v2-development"}:
            raise ValueError("V2 final acceptance is development/test-only")
        if tuple(inputs.commands) != REQUIRED_COMMANDS:
            raise ValueError("V2 final acceptance command matrix mismatch")
        self.inputs, self.runner = inputs, runner
        self.root = Path(inputs.root).resolve()
        self.allowed_root = Path(inputs.allowed_root).resolve(strict=True)
        self.repository = Path(inputs.repository_root).resolve(strict=True)
        if not self.root.is_relative_to(self.allowed_root):
            raise ValueError("V2 final acceptance root escapes allowed root")
        if not self.root.name.endswith(("development", "test")):
            raise ValueError("V2 final acceptance root must be isolated")
        self.report_path = self.root / "storage-v2-pg-ch-final-acceptance.json"
        self.manifest_path = self.root / "manifest.json"

    def _git(self, *args: str) -> str:
        result = subprocess.run(("git", "-C", str(self.repository), *args),
                                capture_output=True, text=True, check=False)
        if result.returncode:
            raise ValueError("V2 final acceptance Git audit failed")
        return result.stdout.strip()

    def preflight(self) -> dict[str, Any]:
        head = self._git("rev-parse", "HEAD")
        resolved: dict[str, list[str]] = {}
        document = (self.repository / "docs/development-handoff-storage-v2.md").read_text()
        for item, commits in ACCEPTED_BG_ARTIFACTS.items():
            section = document.split(f"### `{item}`", 1)[1].split("### `", 1)[0]
            if "**状态**：`已验收" not in section:
                raise ValueError("handoff acceptance state is stale: " + item)
            resolved[item] = []
            for commit in commits:
                full = self._git("rev-parse", f"{commit}^{{commit}}")
                self._git("merge-base", "--is-ancestor", full, head)
                if commit not in section:
                    raise ValueError("handoff Artifact binding is stale: " + item)
                resolved[item].append(full)
        bg020 = document.split("### `BG-020`", 1)[1].split("### `", 1)[0]
        if "本地实现完成，待独立验收" not in bg020:
            raise ValueError("BG-020 handoff state is stale")
        external = document.split("### `BG-EXT`", 1)[1]
        if "未授权、未开始" not in external or "不属于本地完成定义" not in document:
            raise ValueError("BG-EXT authorization boundary is missing")
        if any(marker in document for marker in ("`返修中`", "`暂不通过`", "`BLOCKED`")):
            raise ValueError("unresolved revision or block remains")
        policy = json.loads((self.repository / "docs/architecture/storage-v2-online-dependency-policy.json").read_text())
        if (policy.get("temporary_reachability_exceptions") != []
                or "duckdb" not in policy.get("forbidden_external_imports", [])
                or not {"marketcow.storage", "marketcow.duckdb_repositories"}.issubset(
                    policy.get("forbidden_internal_imports", []))):
            raise ValueError("online DuckDB dependency policy is not closed")
        return {"head": head, "accepted_artifacts": resolved,
                "artifact_chain_sha256": _hash(_json(resolved)),
                "online_dependency_policy_sha256": _hash(_json(policy))}

    def _publish(self, report: Mapping[str, Any]) -> None:
        self.root.mkdir(parents=True, exist_ok=True)
        payload = _json(report)
        descriptor, temporary = tempfile.mkstemp(dir=self.root, prefix=".acceptance-")
        try:
            with os.fdopen(descriptor, "wb") as handle:
                handle.write(payload); handle.flush(); os.fsync(handle.fileno())
            os.replace(temporary, self.report_path); _fsync_dir(self.root)
            manifest = {"version": V2_FINAL_VERSION, "report_sha256": _hash(payload),
                        "report_bytes": len(payload)}
            manifest["manifest_sha256"] = _hash(_json(manifest))
            descriptor, temporary = tempfile.mkstemp(dir=self.root, prefix=".manifest-")
            with os.fdopen(descriptor, "wb") as handle:
                handle.write(_json(manifest)); handle.flush(); os.fsync(handle.fileno())
            os.replace(temporary, self.manifest_path); _fsync_dir(self.root)
            if self.report_path.read_bytes() != payload:
                raise RuntimeError("final report persistence mismatch")
            current = json.loads(self.manifest_path.read_text())
            unsigned = dict(current); signature = unsigned.pop("manifest_sha256", None)
            if signature != _hash(_json(unsigned)) or current["report_sha256"] != _hash(payload):
                raise RuntimeError("final manifest persistence mismatch")
        finally:
            if os.path.exists(temporary):
                os.unlink(temporary)

    def run(self) -> dict[str, Any]:
        try:
            preflight = self.preflight()
            checks = []
            for name, command in self.inputs.commands.items():
                started = time.monotonic()
                result = self.runner(command, cwd=self.repository, capture_output=True,
                                     text=True, check=False, timeout=1200, env=os.environ.copy())
                output = ((result.stdout or "") + (result.stderr or ""))[-MAX_OUTPUT:]
                if name == "clean_worktree" and output.strip():
                    raise RuntimeError("final acceptance worktree is not clean")
                checks.append({"name": name, "returncode": result.returncode,
                               "duration_seconds": round(time.monotonic() - started, 6),
                               "output_sha256": _hash(output.encode())})
                if result.returncode:
                    raise RuntimeError("final acceptance check failed: " + name)
            report = {"version": V2_FINAL_VERSION, "status": "passed", **preflight,
                      "checks": checks,
                      "scope": "T9/local synthetic/disposable PostgreSQL+ClickHouse V2 only",
                      "limitations": [
                          "production data copy/connect/migrate unauthorized and unexecuted",
                          "real consumer, launchd, 8790/8791 switch unauthorized and unexecuted",
                          "push, PR, deploy, upload, publish and remote writes unauthorized and unexecuted",
                          "BG-EXT not started"],
                      "production_connections_attempted": False,
                      "remote_writes_executed": False,
                      "external_actions": "unauthorized_unexecuted"}
            _assert_no_sensitive(report, "V2 final acceptance report")
            self._publish(report)
            return report
        except Exception as error:
            failed = {"version": V2_FINAL_VERSION, "status": "failed",
                      "failure": "acceptance_gate_failed", "reason": sanitize_text(error),
                      "external_actions": "unauthorized_unexecuted"}
            try:
                _assert_no_sensitive(failed, "V2 final failed terminal")
                self._publish(failed)
            except Exception as publication_error:
                self.report_path.unlink(missing_ok=True); self.manifest_path.unlink(missing_ok=True)
                raise RuntimeError("V2 final acceptance failed; terminal publication failed") from publication_error
            raise RuntimeError("V2 final acceptance failed: " + sanitize_text(error)) from error
