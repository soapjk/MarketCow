from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Mapping

from .local_backup import (
    MANIFEST_VERSION, REQUIRED_COMPONENTS, _assert_no_sensitive, _fsync_dir, _hash, _json,
)
from .local_backfill import BACKFILL_VERSION, POSTGRES_DOMAINS, LocalStorageBackfill
from .local_benchmark import BENCHMARK_VERSION, OPERATIONS, LocalStorageBenchmark
from .local_read_switch import SWITCH_VERSION, LocalReadSwitchDrill
from .local_restore import RESTORE_VERSION


READINESS_VERSION = "storage-v2.production-readiness.v1"
EVIDENCE_VERSION = "storage-v2.accepted-evidence.v1"
REQUIRED_ARTIFACTS = ("SV2-021A", "SV2-021B", "SV2-022A", "SV2-022B", "SV2-023")
REHEARSAL_GATES = ("configuration", "backup", "schema", "backfill", "read_switch", "observation")
REQUIRED_BENCHMARK_CHECKS = {
    "raw_write_throughput", "canonical_rebuild_throughput", "query_p95", "query_p99",
    "keyset_page_ratio", "archive_throughput", "restore_throughput",
    "spool_recovery_throughput", "compression_ratio", "clickhouse_free_reserve",
    "merge_backlog", "memory_bound", "thread_bound", "no_offset",
}
MAX_DOCUMENT_BYTES = 1_000_000
_IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
_COMMIT = re.compile(r"^[0-9a-f]{7,40}$")


@dataclass(frozen=True)
class ProductionReadinessInputs:
    root: Path
    allowed_root: Path
    repository_root: Path
    release_commit: str
    evidence_paths: Mapping[str, Path]
    target: Mapping[str, Any]
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


def _contained_file(path: Path, allowed_root: Path, label: str) -> Path:
    supplied = Path(path)
    if supplied.is_symlink() or not supplied.is_file():
        raise ValueError(f"{label} is missing or unsafe")
    allowed_lexical = Path(allowed_root).absolute()
    allowed = allowed_root.resolve()
    try:
        lexical_relative = supplied.absolute().relative_to(allowed_lexical)
    except ValueError as error:
        raise ValueError(f"{label} escapes allowed root") from error
    current = allowed_lexical
    for part in lexical_relative.parts:
        current = current / part
        if current.is_symlink():
            raise ValueError(f"{label} contains a symlink")
    resolved = supplied.resolve()
    try:
        resolved.relative_to(allowed)
    except ValueError as error:
        raise ValueError(f"{label} escapes allowed root") from error
    if supplied.stat().st_size > MAX_DOCUMENT_BYTES:
        raise ValueError(f"{label} exceeds bounded size")
    return resolved


def _git(repository: Path, *args: str) -> str:
    result = subprocess.run(
        ["git", "-C", str(repository), *args], capture_output=True, text=True, check=False,
    )
    if result.returncode:
        raise ValueError("readiness Git evidence is invalid")
    return result.stdout.strip()


class ProductionReadinessPackage:
    """Build a local-only proposal from verified, accepted local evidence."""

    def __init__(self, inputs: ProductionReadinessInputs) -> None:
        if inputs.profile not in {"development", "test"}:
            raise ValueError("production readiness preparation is development/test-only")
        supplied = Path(inputs.root)
        if supplied.is_symlink():
            raise ValueError("readiness root must not be a symlink")
        self.root = supplied.resolve()
        self.allowed_root_supplied = Path(inputs.allowed_root).absolute()
        self.allowed_root = Path(inputs.allowed_root).resolve()
        try:
            self.root.relative_to(self.allowed_root)
        except ValueError as error:
            raise ValueError("readiness root escapes allowed root") from error
        if not self.root.name.endswith(("development", "test")):
            raise ValueError("readiness root must be development/test isolated")
        self.repository_root = Path(inputs.repository_root).resolve()
        if not (self.repository_root / ".git").exists():
            raise ValueError("readiness repository root is invalid")
        if not _COMMIT.fullmatch(inputs.release_commit):
            raise ValueError("readiness release commit is invalid")
        if set(inputs.evidence_paths) != set(REQUIRED_ARTIFACTS):
            raise ValueError("readiness evidence set is incomplete")
        self._validate_target(inputs.target)
        self.inputs = inputs
        self._evidence = self._verify_evidence(inputs.evidence_paths, inputs.release_commit)
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
    def _validate_payload(item: str, value: Mapping[str, Any], payload_path: Path,
                          acceptance: Mapping[str, Any]) -> Dict[str, Any]:
        if item == "SV2-021A":
            unsigned = dict(value)
            signature = unsigned.pop("manifest_payload_sha256", None)
            identity = dict(unsigned)
            backup_id = identity.pop("backup_id", None)
            components = value.get("components", [])
            names = {component.get("name") for component in components}
            files_checked = 0
            files_ok = True
            for component in components:
                for entry in component.get("files", []):
                    relative = entry.get("path", "")
                    file_path = payload_path.parent / relative
                    if (not relative.startswith("components/") or file_path.is_symlink() or
                            not file_path.is_file() or
                            len(file_path.read_bytes()) != entry.get("bytes") or
                            _hash(file_path.read_bytes()) != entry.get("sha256")):
                        files_ok = False
                    files_checked += 1
            ok = (value.get("manifest_version") == MANIFEST_VERSION and
                  signature == _hash(_json(unsigned)) and backup_id == payload_path.parent.name and
                  backup_id == _hash(_json(identity))[:24] and names == REQUIRED_COMPONENTS and
                  len(components) == len(REQUIRED_COMPONENTS) and files_checked >= len(REQUIRED_COMPONENTS)
                  and files_ok)
            facts = {"backup_id": backup_id, "components": sorted(names),
                     "files_checked": files_checked}
        elif item == "SV2-021B":
            verification = value.get("verification", {})
            required = {"postgres_pit", "clickhouse_raw", "duckdb_api_contract",
                        "cold_verify_query_backfill", "spool_replay_once", "canonical_boundary"}
            ok = (value.get("report_version") == RESTORE_VERSION and
                  value.get("status") == "complete" and isinstance(verification, Mapping) and
                  set(verification) == required and all(result == "ok" for result in verification.values())
                  and {item.get("name") for item in value.get("components", [])} == REQUIRED_COMPONENTS
                  and len(value.get("backup_chain", [])) >= 1 and
                  value.get("canonical_boundary") and value.get("watermark"))
            facts = {"backup_chain": list(value.get("backup_chain", [])),
                     "verification": dict(verification)}
        elif item == "SV2-022A":
            companion = ProductionReadinessPackage._companion(
                acceptance, "checkpoint", payload_path, payload_path.parents[2]
            )
            checkpoint = json.loads(companion.read_text())
            LocalStorageBackfill._validate_checkpoint(checkpoint)
            domains = value.get("domains", [])
            expected_domains = {domain.table for domain in POSTGRES_DOMAINS} | {
                "market_bar_raw", "market_bar_canonical", "query_contracts",
            }
            ok = (value.get("version") == BACKFILL_VERSION and value.get("status") == "complete" and
                  value.get("lag") == 0 and not value.get("mismatches") and
                  {domain.get("domain") for domain in domains} == expected_domains and
                  all(domain.get("status") == "ok" for domain in domains) and
                  checkpoint.get("phase") == "complete" and
                  checkpoint.get("run_id") == value.get("run_id") and
                  checkpoint.get("completion_fingerprint") == checkpoint.get("last_live_fingerprint"))
            facts = {"run_id": value.get("run_id"), "lag": value.get("lag"),
                     "domains_checked": len(domains), "checkpoint_sha256": _hash(companion.read_bytes())}
        elif item == "SV2-022B":
            companion = ProductionReadinessPackage._companion(
                acceptance, "checkpoint", payload_path, payload_path.parents[2]
            )
            checkpoint = json.loads(companion.read_text())
            LocalReadSwitchDrill._validate_signed(checkpoint, "read switch checkpoint")
            ok = (value.get("version") == SWITCH_VERSION and value.get("status") == "rolled_back"
                  and value.get("final_backend") == "duckdb" and value.get("canonical") == "duckdb"
                  and value.get("raw") == "duckdb" and checkpoint.get("phase") == "rolled_back"
                  and checkpoint.get("binding") == value.get("binding") and
                  any(event.get("name") == "rollback" for event in value.get("events", [])))
            facts = {"final_backend": value.get("final_backend"),
                     "phase": checkpoint.get("phase"), "events": len(value.get("events", [])),
                     "checkpoint_sha256": _hash(companion.read_bytes())}
        else:
            checks = value.get("checks", {})
            capacity = value.get("capacity", {})
            plan = value.get("plan", {})
            observations = value.get("observations", {})
            measured_rows = int(capacity.get("measured_raw_rows", 0))
            measured_bytes = int(capacity.get("measured_raw_bytes", 0))
            sources = int(plan.get("sources", 0))
            model_rows = int(plan.get("model_raw_rows", 0))
            derived_bpr = measured_bytes / measured_rows if measured_rows else 0
            derived_online = int(derived_bpr * model_rows * (1 + (.9 / sources))) if sources else 0
            expected_required = int(derived_online / .70) if derived_online else 0
            verification_ok = all(
                observation.get("verification", {}).get("expected_rows") ==
                observation.get("verification", {}).get("actual_rows") and
                observation.get("verification", {}).get("expected_checksum") ==
                observation.get("verification", {}).get("actual_checksum")
                for observation in observations.values()
            )
            ok = (
                value.get("version") == BENCHMARK_VERSION and value.get("status") == "passed"
                and isinstance(checks, Mapping) and set(checks) == REQUIRED_BENCHMARK_CHECKS
                and all(check is True for check in checks.values())
                and set(observations) == set(OPERATIONS) and value.get("slo") == LocalStorageBenchmark.SLO
                and measured_rows > 0 and measured_bytes > 0 and sources > 0 and model_rows > 0
                and abs(float(capacity.get("bytes_per_raw_row", 0)) - derived_bpr) < 1e-5
                and abs(int(capacity.get("model_online_bytes", 0)) - derived_online) <= 1
                and abs(int(capacity.get("model_required_disk_bytes_with_30pct_free", 0)) -
                        expected_required) <= 2
                and float(capacity.get("observed_clickhouse_free_ratio", 0)) >= .30
                and verification_ok
            )
            facts = {"checks": len(checks), "operations": len(observations),
                     "measured_raw_rows": measured_rows, "measured_raw_bytes": measured_bytes,
                     "required_disk_bytes": expected_required}
        if not ok:
            raise ValueError(f"{item} evidence payload is incomplete or failed")
        _assert_no_sensitive(value, f"{item} readiness evidence")
        return facts

    @staticmethod
    def _companion(acceptance: Mapping[str, Any], name: str, payload_path: Path,
                   allowed_root: Path) -> Path:
        companions = acceptance.get("companions", {})
        descriptor = companions.get(name, {}) if isinstance(companions, Mapping) else {}
        uri, checksum = descriptor.get("uri"), descriptor.get("sha256")
        if not isinstance(uri, str) or not isinstance(checksum, str):
            raise ValueError(f"readiness {name} companion is missing")
        path = _contained_file(allowed_root / uri, allowed_root, f"readiness {name} companion")
        if _hash(path.read_bytes()) != checksum or path.parent != payload_path.parent:
            raise ValueError(f"readiness {name} companion binding mismatch")
        return path

    def _verify_evidence(self, paths: Mapping[str, Path], release_commit: str) -> Dict[str, Any]:
        head = _git(self.repository_root, "rev-parse", "HEAD")
        release = _git(self.repository_root, "rev-parse", f"{release_commit}^{{commit}}")
        if release != head:
            raise ValueError("readiness release commit must equal local HEAD")
        result: Dict[str, Any] = {}
        for item in REQUIRED_ARTIFACTS:
            acceptance_path = _contained_file(Path(paths[item]), self.allowed_root_supplied,
                                              f"{item} acceptance")
            acceptance_bytes = acceptance_path.read_bytes()
            acceptance = json.loads(acceptance_bytes)
            unsigned = dict(acceptance)
            checksum = unsigned.pop("acceptance_sha256", None)
            if checksum != _hash(_json(unsigned)):
                raise ValueError(f"{item} acceptance checksum mismatch")
            if (acceptance.get("version") != EVIDENCE_VERSION or acceptance.get("item") != item or
                    acceptance.get("status") != "accepted"):
                raise ValueError(f"{item} acceptance record is invalid")
            commit = str(acceptance.get("accepted_commit", ""))
            if not _COMMIT.fullmatch(commit):
                raise ValueError(f"{item} accepted commit is invalid")
            _git(self.repository_root, "merge-base", "--is-ancestor", commit, release)
            uri = str(acceptance.get("evidence_uri", ""))
            payload_path = _contained_file(self.allowed_root_supplied / uri,
                                           self.allowed_root_supplied, f"{item} payload")
            if payload_path.relative_to(self.allowed_root).as_posix() != uri:
                raise ValueError(f"{item} evidence URI is not canonical")
            payload_bytes = payload_path.read_bytes()
            if acceptance.get("evidence_sha256") != _hash(payload_bytes):
                raise ValueError(f"{item} evidence checksum mismatch")
            payload = json.loads(payload_bytes)
            facts = self._validate_payload(item, payload, payload_path, acceptance)
            result[item] = {
                "accepted_commit": _git(self.repository_root, "rev-parse", f"{commit}^{{commit}}"),
                "acceptance_uri": acceptance_path.relative_to(self.allowed_root).as_posix(),
                "acceptance_sha256": _hash(acceptance_bytes),
                "evidence_uri": uri, "evidence_sha256": _hash(payload_bytes),
                "evidence_version": payload.get("version", payload.get("report_version",
                                                payload.get("manifest_version"))),
                "status": payload.get("status", "verified"), "facts": facts,
                "payload": payload,
            }
        return result

    @staticmethod
    def _stages() -> list[Dict[str, Any]]:
        definitions = (
            ("configuration", "configuration and evidence-chain audit", "restore prior config"),
            ("backup", "verify approved backup and restore evidence", "retain backup; no data mutation"),
            ("schema", "validate migration versions from local source", "stop before migration apply"),
            ("backfill", "verify completed zero-lag reconciliation evidence", "resume from durable checkpoint"),
            ("read_switch", "verify disposable read-switch and rollback evidence", "restore DuckDB read backends"),
            ("observation", "verify complete benchmark capacity and SLO evidence", "restore prior service config"),
        )
        return [{
            "id": name, "proposal": proposal,
            "dry_run_command": (
                "uv run python -m marketcow.production_readiness stage "
                f"--stage {name} --target production --dry-run "
                "--package <LOCAL_PACKAGE> --allowed-root <LOCAL_ALLOWED_ROOT> "
                "--repository-root <LOCAL_REPOSITORY>"
            ),
            "preconditions": ["readiness package checksum verified", "accepted evidence ancestry verified",
                              "user authorization remains required"],
            "success_evidence": ["stage-specific local evidence hashes checked",
                                 "no production connection or state change"],
            "stop_conditions": ["missing or changed evidence", "lag above zero", "contract mismatch",
                                "readiness unavailable", "capacity reserve below 30 percent"],
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
        return [{"id": name, "destination": destination, "data": data,
                 "source_code_included": source_code,
                 "visibility": "requires explicit user confirmation", "retention": retention,
                 "proposed_action": action, "authorized": False, "executed": False}
                for name, data, source_code, destination, retention, action in definitions]

    def _public_evidence(self) -> Dict[str, Any]:
        return {item: {key: value for key, value in evidence.items() if key != "payload"}
                for item, evidence in self._evidence.items()}

    def _document(self) -> Dict[str, Any]:
        from .clickhouse_repositories import CLICKHOUSE_MIGRATIONS
        from .postgres_migrations import POSTGRES_MIGRATIONS

        benchmark = self._evidence["SV2-023"]["payload"]
        capacity = benchmark["capacity"]
        target = dict(self.inputs.target)
        return {
            "version": READINESS_VERSION, "status": "ready_for_user_review",
            "release_commit": _git(self.repository_root, "rev-parse", "HEAD"),
            "evidence": self._public_evidence(), "target": target,
            "capacity": {
                "measured_raw_rows": capacity["measured_raw_rows"],
                "measured_raw_bytes": capacity["measured_raw_bytes"],
                "bytes_per_raw_row": capacity["bytes_per_raw_row"],
                "model_online_bytes": capacity["model_online_bytes"],
                "required_disk_bytes": capacity["model_required_disk_bytes_with_30pct_free"],
                "free_ratio": capacity["observed_clickhouse_free_ratio"],
            },
            "slo_checks": dict(sorted(benchmark["checks"].items())),
            "configuration_matrix": {
                "current": {"metadata": "duckdb", "canonical_reads": "duckdb", "raw_reads": "duckdb"},
                "proposed_stages": [{"stage": "metadata", "backend": "postgresql"},
                                    {"stage": "canonical_reads", "backend": "clickhouse_canonical"},
                                    {"stage": "raw_reads", "backend": "clickhouse_raw"}],
                "service_manager": "launchd", "listen_port": target["port"],
                "credential_delivery": "out-of-band references only; no values stored",
            },
            "schema_preflight": {
                "postgres_expected_version": max(item[0] for item in POSTGRES_MIGRATIONS),
                "clickhouse_expected_version": max(item[0] for item in CLICKHOUSE_MIGRATIONS),
                "mode": "metadata-only dry-run; no database connection",
            },
            "observation_window": {"minimum_minutes": 30, "minimum_requests": 1000,
                                   "continue_only_if": "all stop conditions remain false"},
            "stages": self._stages(), "external_actions": self._external_actions(target),
            "responsibility": {"agent": "prepare and locally verify this package only",
                               "user": "approve each external action separately",
                               "operator": "execute only approved runbook stages"},
            "production_connections_attempted": False, "state_changes_executed": False,
            "next_item_started": False,
        }

    @staticmethod
    def _render_runbook(document: Mapping[str, Any]) -> str:
        lines = ["# Storage V2 production readiness proposal", "",
                 f"Version: `{document['version']}`", "",
                 "Dry-run proposal only; no production apply command is included.", ""]
        for stage in document["stages"]:
            lines.extend([f"## {stage['id']}", "", stage["proposal"], "",
                          f"Dry-run: `{stage['dry_run_command']}`", "", "Preconditions:", ""])
            lines.extend(f"- {value}" for value in stage["preconditions"])
            lines.extend(["", "Success evidence:", ""])
            lines.extend(f"- {value}" for value in stage["success_evidence"])
            lines.extend(["", "Stop conditions:", ""])
            lines.extend(f"- {value}" for value in stage["stop_conditions"])
            lines.extend(["", f"Rollback: {stage['rollback']}", ""])
        lines.extend(["## External actions and authorization", ""])
        for action in document["external_actions"]:
            lines.extend([f"### {action['id']}", "",
                          f"- Destination: {action['destination']}",
                          f"- Data: {action['data']}",
                          f"- Source code included: {str(action['source_code_included']).lower()}",
                          f"- Visibility/access: {action['visibility']}",
                          f"- Retention: {action['retention']}",
                          f"- Proposed action: {action['proposed_action']}",
                          "- Authorized: false", "- Executed: false", ""])
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
        manifest = {"version": READINESS_VERSION, "package_sha256": _hash(payload),
                    "runbook_sha256": _hash(runbook), "release_commit": document["release_commit"]}
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
        root = Path(root) if root is not None else self.package_root
        if root.is_symlink() or not root.is_dir() or root.resolve() != self.package_root:
            raise ValueError("readiness package path is invalid")
        names = {"manifest.json", "package.json", "RUNBOOK.md", "rehearsal.json"}
        if any(path.is_symlink() or path.name not in names for path in root.iterdir()):
            raise ValueError("readiness package contains an unsafe path")
        manifest = json.loads((root / "manifest.json").read_text())
        unsigned = dict(manifest)
        checksum = unsigned.pop("manifest_sha256", None)
        if checksum != _hash(_json(unsigned)):
            raise ValueError("readiness manifest checksum mismatch")
        payload, runbook = (root / "package.json").read_bytes(), (root / "RUNBOOK.md").read_bytes()
        if (manifest.get("version") != READINESS_VERSION or
                manifest.get("package_sha256") != _hash(payload) or
                manifest.get("runbook_sha256") != _hash(runbook)):
            raise ValueError("readiness package checksum or version mismatch")
        document = json.loads(payload)
        if document != self._document():
            raise ValueError("readiness package or evidence binding mismatch")
        _assert_no_sensitive(document, "production readiness package")
        if any(stage["apply_command_included"] or "--dry-run" not in stage["dry_run_command"]
               for stage in document["stages"]):
            raise ValueError("readiness runbook is not dry-run-only")
        return {**manifest, "status": "verified", "artifact_path": str(root)}

    def check_stage(self, stage: str) -> Dict[str, Any]:
        self.verify()
        if stage not in REHEARSAL_GATES:
            raise ValueError("unknown readiness stage")
        selected = {
            "configuration": REQUIRED_ARTIFACTS, "backup": ("SV2-021A", "SV2-021B"),
            "schema": REQUIRED_ARTIFACTS, "backfill": ("SV2-022A",),
            "read_switch": ("SV2-022B",), "observation": ("SV2-023",),
        }[stage]
        checked = [{"item": item, "evidence_sha256": self._evidence[item]["evidence_sha256"],
                    "accepted_commit": self._evidence[item]["accepted_commit"],
                    "facts": self._evidence[item]["facts"]} for item in selected]
        document = self._document()
        if stage == "configuration":
            from .config import Settings
            defaults = Settings.__dataclass_fields__
            facts = {
                "default_canonical_backend": defaults["market_bar_read_backend"].default,
                "default_raw_backend": defaults["raw_market_bar_read_backend"].default,
                "default_clickhouse_enabled": defaults["clickhouse_enabled"].default,
                "production_logical_port": document["target"]["port"],
                "credential_delivery": document["configuration_matrix"]["credential_delivery"],
            }
            if facts != {"default_canonical_backend": "duckdb", "default_raw_backend": "duckdb",
                         "default_clickhouse_enabled": False, "production_logical_port": 8790,
                         "credential_delivery": "out-of-band references only; no values stored"}:
                raise ValueError("readiness configuration static audit failed")
        elif stage == "schema":
            from .clickhouse_repositories import CLICKHOUSE_MIGRATIONS
            from .postgres_migrations import POSTGRES_MIGRATIONS
            facts = {"postgres_source_version": max(row[0] for row in POSTGRES_MIGRATIONS),
                     "clickhouse_source_version": max(row[0] for row in CLICKHOUSE_MIGRATIONS)}
            if (facts["postgres_source_version"] != document["schema_preflight"]["postgres_expected_version"] or
                    facts["clickhouse_source_version"] !=
                    document["schema_preflight"]["clickhouse_expected_version"]):
                raise ValueError("readiness schema static audit failed")
        elif stage == "backup":
            facts = {"backup": self._evidence["SV2-021A"]["facts"],
                     "restore": self._evidence["SV2-021B"]["facts"]}
            if facts["backup"]["backup_id"] not in facts["restore"]["backup_chain"]:
                raise ValueError("readiness backup/restore binding failed")
        elif stage == "backfill":
            facts = dict(self._evidence["SV2-022A"]["facts"])
            if facts["lag"] != 0 or facts["domains_checked"] != len(POSTGRES_DOMAINS) + 3:
                raise ValueError("readiness backfill audit failed")
        elif stage == "read_switch":
            facts = dict(self._evidence["SV2-022B"]["facts"])
            if facts["final_backend"] != "duckdb" or facts["phase"] != "rolled_back":
                raise ValueError("readiness rollback audit failed")
        else:
            facts = dict(self._evidence["SV2-023"]["facts"])
            if facts["checks"] != len(REQUIRED_BENCHMARK_CHECKS) or facts["operations"] != len(OPERATIONS):
                raise ValueError("readiness observation audit failed")
        return {"version": READINESS_VERSION, "status": "ok", "stage": stage,
                "checked_evidence": checked, "checked_facts": facts,
                "production_connection_attempted": False,
                "state_changed": False, "authorization_required": True}

    def rehearse(self) -> Dict[str, Any]:
        results = [self.check_stage(stage) for stage in REHEARSAL_GATES]
        report = {"version": READINESS_VERSION, "status": "passed", "results": results,
                  "production_connections_attempted": False, "state_changes_executed": False}
        _assert_no_sensitive(report, "production readiness rehearsal")
        _atomic_write(self.rehearsal_path, _json(report))
        return report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Storage V2 readiness dry-run stage")
    subparsers = parser.add_subparsers(dest="command", required=True)
    stage = subparsers.add_parser("stage")
    stage.add_argument("--stage", choices=REHEARSAL_GATES, required=True)
    stage.add_argument("--target", choices=["production"], required=True)
    stage.add_argument("--dry-run", action="store_true")
    stage.add_argument("--package", type=Path, required=True)
    stage.add_argument("--allowed-root", type=Path, required=True)
    stage.add_argument("--repository-root", type=Path, required=True)
    arguments = parser.parse_args(argv)
    if not arguments.dry_run:
        parser.error("readiness stage execution is prohibited; --dry-run is required")
    supplied_package = arguments.package
    if supplied_package.is_symlink() or not supplied_package.is_dir():
        raise ValueError("readiness package path is invalid")
    package_root = supplied_package.resolve()
    try:
        package_root.relative_to(arguments.allowed_root.resolve())
    except ValueError as error:
        raise ValueError("readiness package escapes allowed root") from error
    root = package_root.parent
    package_file = _contained_file(package_root / "package.json", arguments.allowed_root.resolve(),
                                   "readiness package")
    document = json.loads(package_file.read_text())
    evidence_paths = {item: arguments.allowed_root / value["acceptance_uri"]
                      for item, value in document["evidence"].items()}
    package = ProductionReadinessPackage(ProductionReadinessInputs(
        root=root, allowed_root=arguments.allowed_root, repository_root=arguments.repository_root,
        release_commit=document["release_commit"], evidence_paths=evidence_paths,
        target=document["target"], profile="test",
    ))
    if package.package_root.resolve() != package_root:
        raise ValueError("readiness package path does not match its logical root")
    print(json.dumps(package.check_stage(arguments.stage), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
