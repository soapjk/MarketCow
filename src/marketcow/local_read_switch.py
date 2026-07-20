from __future__ import annotations

import fcntl
import json
import os
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, Mapping

from .local_backup import _assert_no_sensitive, _fsync_dir, _hash, _json
from .local_backfill import LocalStorageBackfill
from .telemetry import sanitize_text


SWITCH_VERSION = "storage-v2.read-switch.v1"
MAX_AUDIT_EVENTS = 100
MAX_SAMPLES = 50
BACKENDS = {"duckdb", "clickhouse_canonical", "clickhouse_raw"}


@dataclass(frozen=True)
class ReadSwitchInputs:
    root: Path
    repository: Any
    backfill_checkpoint: Path
    backfill_report: Path
    restore_report: Path
    backup_artifact_id: str
    restore_artifact_id: str
    profile: str = "development"
    allowed_root: Path | None = None
    gate: Callable[[], Mapping[str, Any]] | None = None
    golden: Callable[[str], Mapping[str, Any]] | None = None
    incremental_write: Callable[[], Any] | None = None


def _atomic_json(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(dir=path.parent, prefix=".switch-")
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(json.dumps(value, sort_keys=True, indent=2).encode())
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        _fsync_dir(path.parent)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


class LocalReadSwitchDrill:
    """Durable development-only ClickHouse read switch and rollback drill."""

    def __init__(
        self, inputs: ReadSwitchInputs, clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if inputs.profile not in {"development", "test"}:
            raise ValueError("read switch is development/test-only")
        if inputs.allowed_root is None:
            raise ValueError("read switch requires an explicit allowed root")
        supplied = Path(inputs.root)
        if supplied.is_symlink():
            raise ValueError("read switch root must not be a symlink")
        self.root = supplied.resolve()
        allowed = Path(inputs.allowed_root).resolve()
        self.allowed_root = allowed
        try:
            self.root.relative_to(allowed)
        except ValueError as error:
            raise ValueError("read switch root escapes allowed root") from error
        identifiers = (
            self.root.name, str(inputs.backup_artifact_id), str(inputs.restore_artifact_id),
        )
        if any("production" in item.lower() for item in identifiers):
            raise ValueError("read switch rejects production identifiers")
        if not self.root.name.endswith(("development", "test")):
            raise ValueError("read switch root must be development/test isolated")
        self.inputs = inputs
        self.clock = clock
        self.state_root = self.root / ".storage-v2-read-switch"
        self.checkpoint_path = self.state_root / "checkpoint.json"
        self.config_path = self.state_root / "read-backend.json"
        self.report_path = self.state_root / "report.json"
        self._binding_hash: str | None = None
        self._recover_effective_config()

    @staticmethod
    def _sign(value: Dict[str, Any]) -> None:
        value.pop("checksum", None)
        value["checksum"] = _hash(_json(value))

    @classmethod
    def _validate_signed(cls, value: Mapping[str, Any], label: str) -> None:
        unsigned = dict(value)
        checksum = unsigned.pop("checksum", None)
        if checksum != _hash(_json(unsigned)):
            raise ValueError(f"{label} checksum mismatch")

    def _recover_effective_config(self) -> None:
        if not self.config_path.exists():
            return
        if self.config_path.is_symlink():
            raise ValueError("read backend config must not be symlinked")
        config = json.loads(self.config_path.read_text())
        self._validate_signed(config, "read backend config")
        if config.get("version") != SWITCH_VERSION:
            raise ValueError("read backend config version mismatch")
        configured_binding = config.get("binding_hash")
        if configured_binding is not None:
            try:
                backfill, report, restore = self._read_preflight()
                current_binding = _hash(_json(self._binding(backfill, report, restore)))
            except Exception:
                self._apply_repository("duckdb", "duckdb")
                raise
            if configured_binding != current_binding:
                self._apply_repository("duckdb", "duckdb")
                raise ValueError("read backend config binding mismatch")
            self._binding_hash = current_binding
        self._apply_repository(config["canonical"], config["raw"])

    def _apply_repository(self, canonical: str, raw: str) -> None:
        if canonical not in {"duckdb", "clickhouse_canonical"}:
            raise ValueError("invalid canonical read backend")
        if raw not in {"duckdb", "clickhouse_raw"}:
            raise ValueError("invalid raw read backend")
        self.inputs.repository.canonical_reads_enabled = canonical == "clickhouse_canonical"
        self.inputs.repository.raw_reads_enabled = raw == "clickhouse_raw"

    def _persist_backend(self, canonical: str, raw: str, reason: str) -> None:
        config = {
            "version": SWITCH_VERSION, "canonical": canonical, "raw": raw,
            "reason": sanitize_text(reason)[:240],
            "binding_hash": self._binding_hash,
        }
        self._sign(config)
        _atomic_json(self.config_path, config)
        self._apply_repository(canonical, raw)

    def _binding(self, backfill: Mapping[str, Any], report: Mapping[str, Any],
                 restore: Mapping[str, Any]) -> Dict[str, Any]:
        return {
            "backfill_run_id": backfill["run_id"],
            "completion_fingerprint": backfill["completion_fingerprint"],
            "targets": backfill["targets"],
            "source_path_hash": backfill["source_path_hash"],
            "backfill_report_hash": _hash(_json(report)),
            "restore_report_hash": _hash(_json(restore)),
            "backup_artifact_id": self.inputs.backup_artifact_id,
            "restore_artifact_id": self.inputs.restore_artifact_id,
        }

    def _read_preflight(self):
        paths = (
            self.inputs.backfill_checkpoint, self.inputs.backfill_report,
            self.inputs.restore_report,
        )
        documents = []
        for path in paths:
            path = Path(path)
            if path.is_symlink() or not path.is_file():
                raise ValueError("switch preflight Artifact is missing or symlinked")
            resolved = path.resolve()
            try:
                resolved.relative_to(self.allowed_root)
            except ValueError as error:
                raise ValueError("switch preflight Artifact escapes allowed root") from error
            documents.append(json.loads(resolved.read_text()))
        backfill, report, restore = documents
        LocalStorageBackfill._validate_checkpoint(backfill)
        if (backfill.get("phase") != "complete" or
                not backfill.get("completion_fingerprint") or
                backfill["completion_fingerprint"] != backfill.get("last_live_fingerprint")):
            raise ValueError("backfill is not complete and stable")
        if report.get("status") != "complete" or report.get("lag") != 0:
            raise ValueError("backfill report does not prove zero lag")
        if restore.get("status") != "complete" or not restore.get("verification"):
            raise ValueError("verified backup/restore drill is required")
        return backfill, report, restore

    def _check_gate(self) -> Dict[str, Any]:
        if self.inputs.gate is None:
            raise ValueError("read switch requires a target-bound gate")
        result = dict(self.inputs.gate())
        required = {
            "lag": 0, "reconcile": "ok", "contract": "ok", "spool_pending": 0,
            "canonical_queue": 0,
        }
        failures = [key for key, expected in required.items() if result.get(key) != expected]
        if result.get("readiness") == "unavailable":
            failures.append("readiness")
        if failures:
            raise RuntimeError("switch stop condition: " + ",".join(sorted(set(failures))))
        return {key: result.get(key) for key in (*required, "readiness")}

    def _golden(self, backend: str) -> Dict[str, Any]:
        if backend not in BACKENDS or self.inputs.golden is None:
            raise ValueError("read switch requires a target-bound golden gate")
        result = dict(self.inputs.golden(backend))
        if result.get("status") != "ok":
            raise RuntimeError(f"golden contract mismatch for {backend}")
        samples = int(result.get("samples", 0))
        if not 1 <= samples <= MAX_SAMPLES:
            raise RuntimeError("golden observation sample count is out of bounds")
        return {
            "backend": backend, "status": "ok", "samples": samples,
            "fallbacks": min(MAX_SAMPLES, max(0, int(result.get("fallbacks", 0)))),
        }

    def _load_or_initialize(self, binding: Mapping[str, Any]) -> Dict[str, Any]:
        self._binding_hash = _hash(_json(binding))
        if self.config_path.exists():
            config = json.loads(self.config_path.read_text())
            self._validate_signed(config, "read backend config")
            configured_binding = config.get("binding_hash")
            if configured_binding not in {None, self._binding_hash}:
                self._persist_backend("duckdb", "duckdb", "binding_mismatch")
                raise ValueError("read backend config binding mismatch")
        if self.checkpoint_path.exists():
            checkpoint = json.loads(self.checkpoint_path.read_text())
            self._validate_signed(checkpoint, "read switch checkpoint")
            if checkpoint.get("version") != SWITCH_VERSION or checkpoint.get("binding") != binding:
                raise ValueError("read switch checkpoint binding mismatch")
            return checkpoint
        checkpoint: Dict[str, Any] = {
            "version": SWITCH_VERSION, "binding": dict(binding), "phase": "preflight",
            "canonical": "duckdb", "raw": "duckdb", "events": [], "stop_reason": None,
        }
        self._save(checkpoint)
        self._persist_backend("duckdb", "duckdb", "preflight")
        return checkpoint

    def _save(self, checkpoint: Dict[str, Any]) -> None:
        self._sign(checkpoint)
        _atomic_json(self.checkpoint_path, checkpoint)

    def _event(self, checkpoint: Dict[str, Any], name: str, detail: Mapping[str, Any]) -> None:
        checkpoint["events"] = (checkpoint.get("events", []) + [{
            "name": name, "detail": dict(detail),
        }])[-MAX_AUDIT_EVENTS:]

    def _stage(self, checkpoint: Dict[str, Any], name: str, canonical: str, raw: str,
               fault_hook: Any = None) -> None:
        checkpoint["phase"] = f"applying:{name}"
        self._save(checkpoint)
        if fault_hook:
            fault_hook("before_apply", name)
        self._persist_backend(canonical, raw, name)
        if fault_hook:
            fault_hook("after_apply", name)
        checkpoint.update({"phase": name, "canonical": canonical, "raw": raw})
        self._event(checkpoint, name, {"canonical": canonical, "raw": raw})
        self._save(checkpoint)
        if fault_hook:
            fault_hook("after_checkpoint", name)

    def _rollback_locked(self, checkpoint: Dict[str, Any], reason: str) -> Dict[str, Any]:
        started = self.clock()
        self._persist_backend("duckdb", "duckdb", reason)
        checkpoint.update({
            "phase": "rolled_back", "canonical": "duckdb", "raw": "duckdb",
            "stop_reason": sanitize_text(reason)[:240],
        })
        sample = self._golden("duckdb")
        self._event(checkpoint, "rollback", sample)
        checkpoint["rollback_seconds"] = max(0.0, self.clock() - started)
        self._save(checkpoint)
        return sample

    def run(self, observation_samples: int = 3, fault_hook: Any = None) -> Dict[str, Any]:
        if not 1 <= observation_samples <= MAX_SAMPLES:
            raise ValueError("observation_samples must be between 1 and 50")
        backfill, report, restore = self._read_preflight()
        binding = self._binding(backfill, report, restore)
        self.state_root.mkdir(parents=True, exist_ok=True)
        with (self.state_root / "switch.lock").open("a+") as lock:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
            checkpoint = self._load_or_initialize(binding)
            try:
                if checkpoint["phase"].startswith("applying:"):
                    self._rollback_locked(checkpoint, "crash_recovery")
                self._check_gate()
                baseline = self._golden("duckdb")
                self._event(checkpoint, "preflight", baseline)
                self._stage(
                    checkpoint, "canonical_enabled", "clickhouse_canonical", "duckdb",
                    fault_hook,
                )
                canonical_samples = []
                for _ in range(observation_samples):
                    canonical_samples.append(self._golden("clickhouse_canonical"))
                    self._check_gate()
                if self.inputs.incremental_write is not None:
                    self.inputs.incremental_write()
                    self._check_gate()
                    canonical_samples.append(self._golden("clickhouse_canonical"))
                self._event(checkpoint, "canonical_observation", {
                    "samples": len(canonical_samples),
                    "fallbacks": sum(item["fallbacks"] for item in canonical_samples),
                })
                self._stage(
                    checkpoint, "raw_enabled", "clickhouse_canonical", "clickhouse_raw",
                    fault_hook,
                )
                raw_samples = []
                for _ in range(observation_samples):
                    raw_samples.append(self._golden("clickhouse_raw"))
                    self._check_gate()
                self._event(checkpoint, "raw_observation", {
                    "samples": len(raw_samples),
                    "fallbacks": sum(item["fallbacks"] for item in raw_samples),
                })
                checkpoint["phase"] = "switched"
                self._save(checkpoint)
                result = self._report(checkpoint, "clickhouse")
                _atomic_json(self.report_path, result)
                return result
            except Exception as error:
                try:
                    self._rollback_locked(checkpoint, str(error))
                finally:
                    fcntl.flock(lock.fileno(), fcntl.LOCK_UN)
                raise
            finally:
                try:
                    fcntl.flock(lock.fileno(), fcntl.LOCK_UN)
                except OSError:
                    pass

    def rollback(self, reason: str = "explicit_rollback") -> Dict[str, Any]:
        backfill, report, restore = self._read_preflight()
        binding = self._binding(backfill, report, restore)
        self.state_root.mkdir(parents=True, exist_ok=True)
        with (self.state_root / "switch.lock").open("a+") as lock:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
            checkpoint = self._load_or_initialize(binding)
            self._rollback_locked(checkpoint, reason)
            result = self._report(checkpoint, "duckdb")
            _atomic_json(self.report_path, result)
            return result

    def _report(self, checkpoint: Mapping[str, Any], backend: str) -> Dict[str, Any]:
        report = {
            "version": SWITCH_VERSION, "status": checkpoint["phase"],
            "binding": checkpoint["binding"], "final_backend": backend,
            "canonical": checkpoint["canonical"], "raw": checkpoint["raw"],
            "stop_reason": checkpoint.get("stop_reason"),
            "rollback_seconds": checkpoint.get("rollback_seconds"),
            "events": list(checkpoint.get("events", []))[-MAX_AUDIT_EVENTS:],
            "runbook": "stop on lag, mismatch, backlog or unavailable; persist DuckDB rollback",
        }
        _assert_no_sensitive(report, "read switch report")
        return report
