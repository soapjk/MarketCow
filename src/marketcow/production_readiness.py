from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, Mapping

from .local_backup import _assert_no_sensitive, _fsync_dir, _hash, _json


READINESS_VERSION = "storage-v2.production-readiness.v1"
REQUIRED_ARTIFACTS = (
    "SV2-021A", "SV2-021B", "SV2-022A", "SV2-022B", "SV2-023",
)
REHEARSAL_GATES = (
    "configuration", "schema", "backup_restore", "backfill", "contracts",
    "capacity", "rollback",
)
MAX_DOCUMENT_BYTES = 1_000_000
_IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
_COMMIT = re.compile(r"^[0-9a-f]{7,40}$")


@dataclass(frozen=True)
class ProductionReadinessInputs:
    root: Path
    allowed_root: Path
    release_commit: str
    artifacts: Mapping[str, str]
    target: Mapping[str, Any]
    capacity: Mapping[str, Any]
    slo_checks: Mapping[str, bool]
    profile: str = "development"


def _atomic_write(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(dir=path.parent, prefix=".readiness-")
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        _fsync_dir(path.parent)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


class ProductionReadinessPackage:
    """Build and rehearse a local-only production change proposal."""

    def __init__(self, inputs: ProductionReadinessInputs) -> None:
        if inputs.profile not in {"development", "test"}:
            raise ValueError("production readiness preparation is development/test-only")
        supplied = Path(inputs.root)
        if supplied.is_symlink():
            raise ValueError("readiness root must not be a symlink")
        self.root = supplied.resolve()
        self.allowed_root = Path(inputs.allowed_root).resolve()
        try:
            self.root.relative_to(self.allowed_root)
        except ValueError as error:
            raise ValueError("readiness root escapes allowed root") from error
        if not self.root.name.endswith(("development", "test")):
            raise ValueError("readiness root must be development/test isolated")
        if not _COMMIT.fullmatch(inputs.release_commit):
            raise ValueError("readiness release commit is invalid")
        if set(inputs.artifacts) != set(REQUIRED_ARTIFACTS):
            raise ValueError("readiness Artifact set is incomplete")
        if any(not _COMMIT.fullmatch(str(value)) for value in inputs.artifacts.values()):
            raise ValueError("readiness Artifact identifier is invalid")
        self._validate_target(inputs.target)
        self._validate_capacity(inputs.capacity)
        if not inputs.slo_checks or not all(
            isinstance(value, bool) and value for value in inputs.slo_checks.values()
        ):
            raise ValueError("readiness requires all local SLO checks to pass")
        _assert_no_sensitive({
            "target": dict(inputs.target), "capacity": dict(inputs.capacity),
        }, "production readiness input")
        self.inputs = inputs
        self.package_root = self.root / "storage-v2-production-readiness"
        self.manifest_path = self.package_root / "manifest.json"
        self.package_path = self.package_root / "package.json"
        self.runbook_path = self.package_root / "RUNBOOK.md"
        self.rehearsal_path = self.package_root / "rehearsal.json"

    @staticmethod
    def _validate_target(target: Mapping[str, Any]) -> None:
        required = {"environment", "service", "postgres", "clickhouse", "port"}
        if set(target) != required or target.get("environment") != "production":
            raise ValueError("readiness requires an explicit production logical target")
        for key in ("service", "postgres", "clickhouse"):
            if not _IDENTIFIER.fullmatch(str(target[key])):
                raise ValueError(f"readiness target {key} must be a logical identifier")
        port = target["port"]
        if isinstance(port, bool) or not isinstance(port, int) or not 1 <= port <= 65535:
            raise ValueError("readiness target port is invalid")

    @staticmethod
    def _validate_capacity(capacity: Mapping[str, Any]) -> None:
        required = {
            "model_online_bytes", "required_disk_bytes", "free_ratio",
            "bytes_per_raw_row",
        }
        if set(capacity) != required:
            raise ValueError("readiness capacity evidence is incomplete")
        if any(float(capacity[key]) <= 0 for key in required):
            raise ValueError("readiness capacity evidence must be positive")
        if float(capacity["free_ratio"]) < .30:
            raise ValueError("readiness capacity reserve is below 30 percent")

    @staticmethod
    def _stages() -> list[Dict[str, Any]]:
        definitions = (
            ("configuration", "configuration and credential-reference audit", "restore prior config"),
            ("backup", "create and verify an approved production backup point", "retain backup; no data mutation"),
            ("schema", "validate PostgreSQL and ClickHouse migrations", "stop before migration apply"),
            ("backfill", "validate bounded backfill and zero-lag reconciliation", "stop writers; resume from checkpoint"),
            ("read_switch", "validate staged canonical then raw read switches", "restore DuckDB read backends"),
            ("observation", "observe SLO, queue, lag and consumer compatibility", "restore DuckDB and prior service config"),
        )
        return [{
            "id": name, "proposal": proposal,
            "dry_run_command": (
                "uv run python -m marketcow.production_readiness stage "
                f"--stage {name} --target production --dry-run"
            ),
            "preconditions": ["previous stage accepted", "local evidence bound", "user authorization recorded"],
            "success_evidence": ["bounded check status ok", "audit record checksum verified"],
            "stop_conditions": ["lag above zero", "contract mismatch", "readiness unavailable", "capacity reserve below 30 percent"],
            "rollback": rollback, "apply_command_included": False,
            "authorization_required": True,
        } for name, proposal, rollback in definitions]

    @staticmethod
    def _external_actions(target: Mapping[str, Any]) -> list[Dict[str, Any]]:
        definitions = (
            ("source_publish", "source code and local commits", True, "configured repository", "repository access policy", "git push or PR"),
            ("production_backup", "database and local storage records", False, target["service"], "operator-controlled retention", "create backup point"),
            ("schema_migration", "schema metadata", False, f"{target['postgres']}+{target['clickhouse']}", "database retention policy", "apply migrations"),
            ("data_backfill", "market data and provenance", False, target["clickhouse"], "database retention policy", "run bounded backfill"),
            ("read_switch", "configuration and diagnostics", False, target["service"], "local service logs", "switch read backends"),
            ("service_update", "service configuration", False, target["service"], "host service configuration", "update service manager"),
            ("consumer_validation", "health and market-data responses", False, target["service"], "consumer log policy", "validate consumers"),
        )
        visibility = "requires explicit user confirmation"
        return [{
            "id": name, "destination": destination, "data": data,
            "source_code_included": source_code, "visibility": visibility,
            "retention": retention, "proposed_action": action,
            "authorized": False, "executed": False,
        } for name, data, source_code, destination, retention, action in definitions]

    def _document(self) -> Dict[str, Any]:
        from .clickhouse_repositories import CLICKHOUSE_MIGRATIONS
        from .postgres_migrations import POSTGRES_MIGRATIONS

        target = dict(self.inputs.target)
        return {
            "version": READINESS_VERSION,
            "status": "ready_for_user_review",
            "release_commit": self.inputs.release_commit,
            "artifacts": dict(sorted(self.inputs.artifacts.items())),
            "target": target,
            "configuration_matrix": {
                "current": {"metadata": "duckdb", "canonical_reads": "duckdb", "raw_reads": "duckdb"},
                "proposed_stages": [
                    {"stage": "metadata", "backend": "postgresql"},
                    {"stage": "canonical_reads", "backend": "clickhouse_canonical"},
                    {"stage": "raw_reads", "backend": "clickhouse_raw"},
                ],
                "service_manager": "launchd", "listen_port": target["port"],
                "credential_delivery": "out-of-band references only; no values stored",
            },
            "capacity": dict(self.inputs.capacity),
            "slo_checks": dict(sorted(self.inputs.slo_checks.items())),
            "schema_preflight": {
                "postgres_expected_version": max(item[0] for item in POSTGRES_MIGRATIONS),
                "clickhouse_expected_version": max(item[0] for item in CLICKHOUSE_MIGRATIONS),
                "mode": "metadata-only dry-run; no database connection",
            },
            "observation_window": {
                "minimum_minutes": 30, "minimum_requests": 1000,
                "continue_only_if": "all stop conditions remain false",
            },
            "stages": self._stages(),
            "external_actions": self._external_actions(target),
            "responsibility": {
                "agent": "prepare and locally verify this package only",
                "user": "approve each external action separately",
                "operator": "execute only approved runbook stages and retain audit evidence",
            },
            "production_connections_attempted": False,
            "state_changes_executed": False,
            "next_item_started": False,
        }

    @staticmethod
    def _render_runbook(document: Mapping[str, Any]) -> str:
        lines = [
            "# Storage V2 production readiness proposal", "",
            f"Version: `{document['version']}`", "",
            "This package is a dry-run proposal. It contains no production apply command.", "",
        ]
        for stage in document["stages"]:
            lines.extend([
                f"## {stage['id']}", "", stage["proposal"], "",
                f"Dry-run: `{stage['dry_run_command']}`", "",
                f"Rollback: {stage['rollback']}", "",
            ])
        lines.extend([
            "## Authorization boundary", "",
            "Every external action remains unauthorized and unexecuted until the user approves that exact action.", "",
        ])
        return "\n".join(lines)

    def build(self) -> Dict[str, Any]:
        document = self._document()
        runbook = self._render_runbook(document).encode()
        payload = _json(document)
        if len(payload) > MAX_DOCUMENT_BYTES or len(runbook) > MAX_DOCUMENT_BYTES:
            raise ValueError("readiness document exceeds bounded size")
        _assert_no_sensitive(document, "production readiness package")
        staging = self.root / ".storage-v2-production-readiness.staging"
        if staging.exists():
            shutil.rmtree(staging)
        staging.mkdir(parents=True)
        _atomic_write(staging / "package.json", payload)
        _atomic_write(staging / "RUNBOOK.md", runbook)
        manifest = {
            "version": READINESS_VERSION,
            "package_sha256": _hash(payload), "runbook_sha256": _hash(runbook),
            "release_commit": self.inputs.release_commit,
        }
        manifest["manifest_sha256"] = _hash(_json(manifest))
        _atomic_write(staging / "manifest.json", _json(manifest))
        if self.package_root.exists():
            existing = self.verify(self.package_root)
            if existing["manifest_sha256"] == manifest["manifest_sha256"]:
                shutil.rmtree(staging)
                return existing
            raise ValueError("readiness package already exists with different binding")
        os.replace(staging, self.package_root)
        _fsync_dir(self.root)
        return self.verify(self.package_root)

    def verify(self, root: Path | None = None) -> Dict[str, Any]:
        root = (Path(root) if root is not None else self.package_root)
        if root.is_symlink() or not root.is_dir() or root.resolve() != self.package_root:
            raise ValueError("readiness package path is invalid")
        for path in root.iterdir():
            if path.is_symlink() or path.name not in {"manifest.json", "package.json", "RUNBOOK.md", "rehearsal.json"}:
                raise ValueError("readiness package contains an unsafe path")
        manifest = json.loads((root / "manifest.json").read_text())
        unsigned = dict(manifest)
        checksum = unsigned.pop("manifest_sha256", None)
        if checksum != _hash(_json(unsigned)):
            raise ValueError("readiness manifest checksum mismatch")
        payload = (root / "package.json").read_bytes()
        runbook = (root / "RUNBOOK.md").read_bytes()
        if manifest.get("version") != READINESS_VERSION or manifest.get(
            "package_sha256"
        ) != _hash(payload) or manifest.get("runbook_sha256") != _hash(runbook):
            raise ValueError("readiness package checksum or version mismatch")
        document = json.loads(payload)
        _assert_no_sensitive(document, "production readiness package")
        _assert_no_sensitive(runbook.decode(), "production readiness runbook")
        _assert_no_sensitive(manifest, "production readiness manifest")
        if document != self._document():
            raise ValueError("readiness package binding mismatch")
        if any(not stage["dry_run_command"].find("--dry-run") >= 0 or
               stage["apply_command_included"] for stage in document["stages"]):
            raise ValueError("readiness runbook is not dry-run-only")
        return {**manifest, "status": "verified", "artifact_path": str(root)}

    def rehearse(
        self, probes: Mapping[str, Callable[[], Mapping[str, Any]]],
    ) -> Dict[str, Any]:
        self.verify()
        if set(probes) != set(REHEARSAL_GATES):
            raise ValueError("readiness rehearsal gate set mismatch")
        results = []
        for name in REHEARSAL_GATES:
            result = dict(probes[name]())
            if result.get("status") != "ok" or result.get("environment") not in {
                "development", "test", "disposable",
            } or result.get("state_changed") is not False:
                raise RuntimeError(f"readiness rehearsal failed at {name}")
            results.append({"gate": name, "status": "ok", "state_changed": False})
        report = {
            "version": READINESS_VERSION, "status": "passed", "results": results,
            "production_connections_attempted": False, "state_changes_executed": False,
        }
        _assert_no_sensitive(report, "production readiness rehearsal")
        _atomic_write(self.rehearsal_path, _json(report))
        return report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Storage V2 readiness dry-run stage")
    subparsers = parser.add_subparsers(dest="command", required=True)
    stage = subparsers.add_parser("stage")
    stage.add_argument("--stage", choices=[item["id"] for item in
                                           ProductionReadinessPackage._stages()], required=True)
    stage.add_argument("--target", choices=["production"], required=True)
    stage.add_argument("--dry-run", action="store_true")
    arguments = parser.parse_args(argv)
    if arguments.command != "stage" or not arguments.dry_run:
        parser.error("readiness stage execution is prohibited; --dry-run is required")
    print(json.dumps({
        "version": READINESS_VERSION, "status": "dry_run_only",
        "stage": arguments.stage, "target": arguments.target,
        "production_connection_attempted": False, "state_changed": False,
        "authorization_required": True,
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
