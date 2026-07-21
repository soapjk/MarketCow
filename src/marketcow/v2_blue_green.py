"""BG-019 local-only whole-service blue/green cutover rehearsal."""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
from pathlib import Path
from typing import Any, Callable, Mapping, Protocol

from .local_backup import _assert_no_sensitive
from .local_benchmark import _atomic_json
from .offline_copy_validator import COPY_VALIDATION_VERSION
from .offline_incremental_catchup import CATCHUP_VERSION
from .telemetry import sanitize_text
from .v2_backup_restore import V2_RESTORE_VERSION
from .v2_benchmark import V2_BENCHMARK_VERSION, V2_METHODS


BLUE_GREEN_VERSION = "storage-v2.pg-ch-blue-green.v1"
MAX_EVIDENCE_BYTES = 4 * 1024 * 1024
MAX_AUDIT_EVENTS = 128
GOLDEN_CHECKS = (
    "shared_routes", "defaults", "error_shapes", "quotes_cache_only",
    "quotes_partial_failure", "history_cursor", "cache_continuity",
    "health", "readiness",
)
STOP_CONDITIONS = {
    "lag": 0,
    "reconcile": "ok",
    "contract": "ok",
    "spool_pending": 0,
    "wal_failed": 0,
    "quarantine": 0,
    "canonical_queue": 0,
    "postgresql": "healthy",
    "clickhouse": "healthy",
    "readiness": "ready",
}


class ConsumerRouter(Protocol):
    @property
    def target(self) -> str: ...

    def switch(self, target: str) -> None: ...


class BlueGreenError(RuntimeError):
    pass


def _digest(value: Any) -> str:
    raw = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()
    return hashlib.sha256(raw).hexdigest()


def _snapshot(path: Path, root: Path) -> tuple[dict[str, Any], str]:
    resolved_root = root.resolve(strict=True)
    if path.is_symlink():
        raise BlueGreenError("evidence symlink is forbidden")
    resolved = path.resolve(strict=True)
    if not resolved.is_relative_to(resolved_root):
        raise BlueGreenError("evidence escapes allowed root")
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    fd = os.open(resolved, flags)
    try:
        before = os.fstat(fd)
        if before.st_size > MAX_EVIDENCE_BYTES:
            raise BlueGreenError("evidence exceeds byte bound")
        raw = os.read(fd, MAX_EVIDENCE_BYTES + 1)
        after = os.fstat(fd)
        current = os.stat(resolved, follow_symlinks=False)
        identity = (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
        if identity != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns):
            raise BlueGreenError("evidence changed while reading")
        if (after.st_dev, after.st_ino) != (current.st_dev, current.st_ino):
            raise BlueGreenError("evidence identity changed")
        if len(raw) != before.st_size:
            raise BlueGreenError("evidence read is incomplete")
        value = json.loads(raw)
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise BlueGreenError("evidence cannot be verified") from error
    finally:
        os.close(fd)
    if not isinstance(value, dict):
        raise BlueGreenError("evidence must be an object")
    return value, hashlib.sha256(raw).hexdigest()


class V2BlueGreenDrill:
    """Switch one external consumer target; never switches repositories in-process."""

    def __init__(
        self,
        root: Path,
        allowed_root: Path,
        router: ConsumerRouter,
        evidence: Mapping[str, Path],
        gate: Callable[[], Mapping[str, Any]],
        golden: Callable[[str], Mapping[str, Any]],
        source_increment: Callable[[], str],
        catchup_reconcile: Callable[[str], Mapping[str, Any]],
        *,
        profile: str = "v2-test",
        observations: int = 3,
        fault_hook: Callable[[str], None] | None = None,
    ) -> None:
        if profile not in {"v2-test", "v2-development"}:
            raise ValueError("blue/green drill is development/test-only")
        if not 1 <= observations <= 20:
            raise ValueError("observation count outside supported bound")
        self.root = root
        self.allowed_root = allowed_root
        self.router = router
        self.evidence = dict(evidence)
        self.gate = gate
        self.golden = golden
        self.source_increment = source_increment
        self.catchup_reconcile = catchup_reconcile
        self.observations = observations
        self.fault_hook = fault_hook or (lambda _point: None)
        self.state_root = root / ".v2-blue-green"
        self.checkpoint_path = self.state_root / "checkpoint.json"
        self.report_path = self.state_root / "report.json"
        self.lock_path = self.state_root / "drill.lock"

    def _evidence_binding(self) -> dict[str, str]:
        if set(self.evidence) != {"copy", "catchup", "restore", "benchmark"}:
            raise BlueGreenError("evidence inventory mismatch")
        values: dict[str, dict[str, Any]] = {}
        hashes: dict[str, str] = {}
        for name, path in sorted(self.evidence.items()):
            values[name], hashes[name] = _snapshot(path, self.allowed_root)
        copy = values["copy"]
        catchup = values["catchup"]
        restore = values["restore"]
        benchmark = values["benchmark"]
        copy_unsigned = dict(copy)
        copy_checksum = copy_unsigned.pop("report_payload_sha256", None)
        if (copy.get("version") != COPY_VALIDATION_VERSION or copy.get("status") != "verified"
                or copy_checksum != _digest(copy_unsigned)):
            raise BlueGreenError("BG-015 evidence is not accepted")
        if (catchup.get("version") != CATCHUP_VERSION or catchup.get("status") != "complete"
                or catchup.get("lag") != 0 or len(set(catchup.get("stability", []))) != 1
                or len(catchup.get("stability", [])) != 3):
            raise BlueGreenError("BG-014 evidence is not stable at lag zero")
        restore_checks = restore.get("verification", {})
        if (restore.get("report_version") != V2_RESTORE_VERSION or restore.get("status") != "complete"
                or not restore_checks or any(value != "ok" for value in restore_checks.values())):
            raise BlueGreenError("BG-016 restore evidence is incomplete")
        if (benchmark.get("version") != V2_BENCHMARK_VERSION
                or benchmark.get("status") != "passed"
                or benchmark.get("methods") != V2_METHODS
                or not benchmark.get("checks") or not all(benchmark["checks"].values())):
            raise BlueGreenError("BG-018 benchmark evidence is not passed")
        watermark = catchup.get("source_high_watermark", {}).get("source_fingerprint")
        if not watermark:
            raise BlueGreenError("catch-up watermark is missing")
        return {**hashes, "source_fingerprint": str(watermark)}

    @staticmethod
    def _check_gate(value: Mapping[str, Any]) -> None:
        for name, expected in STOP_CONDITIONS.items():
            if value.get(name) != expected:
                raise BlueGreenError("stop condition: " + name)

    @staticmethod
    def _check_golden(value: Mapping[str, Any]) -> None:
        if set(value) != set(GOLDEN_CHECKS):
            raise BlueGreenError("API golden mismatch")
        for name in GOLDEN_CHECKS:
            item = value[name]
            if (not isinstance(item, Mapping) or item.get("status") != "ok"
                    or int(item.get("requests", 0)) < 1
                    or not isinstance(item.get("blue_capture_sha256"), str)
                    or len(item["blue_capture_sha256"]) != 64
                    or not isinstance(item.get("green_capture_sha256"), str)
                    or len(item["green_capture_sha256"]) != 64):
                raise BlueGreenError("API golden mismatch: " + name)

    def _save(self, checkpoint: dict[str, Any]) -> None:
        unsigned = dict(checkpoint)
        unsigned.pop("checksum", None)
        checkpoint["checksum"] = _digest(unsigned)
        _atomic_json(self.checkpoint_path, checkpoint)

    def _load(self, binding: Mapping[str, str]) -> dict[str, Any]:
        if not self.checkpoint_path.exists():
            checkpoint = {
                "version": BLUE_GREEN_VERSION, "binding": dict(binding), "phase": "blue",
                "attempt": 0, "audit": [], "increment_fingerprint": "",
            }
            self._save(checkpoint)
            return checkpoint
        checkpoint = json.loads(self.checkpoint_path.read_text(encoding="utf-8"))
        unsigned = dict(checkpoint)
        checksum = unsigned.pop("checksum", None)
        if (checkpoint.get("version") != BLUE_GREEN_VERSION or checksum != _digest(unsigned)
                or checkpoint.get("binding") != dict(binding)):
            raise BlueGreenError("checkpoint binding is invalid")
        return checkpoint

    def _audit(self, checkpoint: dict[str, Any], event: str, result: str) -> None:
        checkpoint["audit"] = (checkpoint.get("audit", []) + [{
            "sequence": len(checkpoint.get("audit", [])) + 1,
            "event": event, "result": result,
        }])[-MAX_AUDIT_EVENTS:]
        self._save(checkpoint)

    def _rollback(self, checkpoint: dict[str, Any], reason: str) -> None:
        self.router.switch("blue")
        checkpoint["phase"] = "reconcile_required"
        checkpoint["stop_reason"] = reason
        if not checkpoint.get("increment_fingerprint"):
            checkpoint["increment_fingerprint"] = str(self.source_increment())
        self._audit(checkpoint, "whole_service_rollback", reason)

    def _report(self, checkpoint: Mapping[str, Any], status: str) -> dict[str, Any]:
        report = {
            "version": BLUE_GREEN_VERSION, "status": status,
            "final_target": self.router.target, "attempt": checkpoint["attempt"],
            "binding": checkpoint["binding"], "audit": checkpoint.get("audit", []),
            "stop_reason": checkpoint.get("stop_reason", ""),
            "external_actions": "unauthorized_unexecuted",
        }
        _assert_no_sensitive(report, "blue/green report")
        _atomic_json(self.report_path, report)
        return report

    def run(self) -> dict[str, Any]:
        try:
            binding = self._evidence_binding()  # no resources before evidence acceptance
        except Exception as error:
            if self.router.target == "green":
                self.router.switch("blue")
            if self.state_root.is_dir():
                terminal = {
                    "version": BLUE_GREEN_VERSION, "status": "failed",
                    "final_target": self.router.target,
                    "failure": "evidence_preflight_failed",
                    "external_actions": "unauthorized_unexecuted",
                }
                _atomic_json(self.report_path, terminal)
            raise BlueGreenError("blue/green evidence preflight failed") from error
        self.state_root.mkdir(parents=True, exist_ok=True)
        with self.lock_path.open("a+") as lock:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
            checkpoint = self._load(binding)
            try:
                if checkpoint["phase"] == "green":
                    self._check_gate(self.gate())
                    self._check_golden(self.golden("green"))
                    return self._report(checkpoint, "passed")
                if checkpoint["phase"] in {"switching", "observing"}:
                    self._rollback(checkpoint, "crash_recovery")
                if checkpoint["phase"] == "reconcile_required":
                    result = self.catchup_reconcile(checkpoint["increment_fingerprint"])
                    self._check_gate(result)
                    if result.get("source_fingerprint") != checkpoint["increment_fingerprint"]:
                        raise BlueGreenError("post-rollback reconcile binding mismatch")
                    checkpoint["phase"] = "blue"
                    checkpoint["stop_reason"] = ""
                    self._audit(checkpoint, "post_rollback_reconcile", "ok")
                self._check_gate(self.gate())
                self._check_golden(self.golden("blue"))
                checkpoint["attempt"] += 1
                checkpoint["phase"] = "switching"
                self._save(checkpoint)
                self.fault_hook("before_route_switch")
                self.router.switch("green")
                self.fault_hook("after_route_switch")
                checkpoint["phase"] = "observing"
                self._audit(checkpoint, "consumer_target", "green")
                self.fault_hook("after_switch_checkpoint")
                for _ in range(self.observations):
                    self._check_gate(self.gate())
                    self._check_golden(self.golden("green"))
                checkpoint["phase"] = "green"
                checkpoint["stop_reason"] = ""
                self._audit(checkpoint, "observation_window", "ok")
                return self._report(checkpoint, "passed")
            except Exception as error:
                reason = sanitize_text(error)
                if self.router.target != "blue" or checkpoint.get("phase") != "blue":
                    self._rollback(checkpoint, reason)
                self._report(checkpoint, "rolled_back")
                raise BlueGreenError("blue/green drill stopped: " + reason) from error
            finally:
                fcntl.flock(lock.fileno(), fcntl.LOCK_UN)

    def rollback(self) -> dict[str, Any]:
        """Idempotently return the entire consumer target to blue."""
        binding = self._evidence_binding()
        self.state_root.mkdir(parents=True, exist_ok=True)
        with self.lock_path.open("a+") as lock:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
            checkpoint = self._load(binding)
            try:
                if checkpoint["phase"] != "reconcile_required":
                    self._rollback(checkpoint, "operator_requested")
                elif self.router.target != "blue":
                    self.router.switch("blue")
                return self._report(checkpoint, "rolled_back")
            finally:
                fcntl.flock(lock.fileno(), fcntl.LOCK_UN)
