"""BG-014 bounded synthetic incremental catch-up over verified BG-012 streams."""

from __future__ import annotations

import fcntl
import json
from typing import Any, Callable

from .local_backfill import POSTGRES_DOMAINS
from .offline_duckdb_import import ALLOWED_TABLES, OfflineDuckDBImporter
from .offline_full_import import (
    FULL_IMPORT_VERSION,
    MAX_ERRORS,
    FullImportTargets,
    OfflineFullImport,
    _atomic,
    _digest,
)


CATCHUP_VERSION = "storage-v2.offline-incremental-catchup.v1"
MAX_PASSES = 10


class OfflineIncrementalCatchup:
    """Idempotently converge a changing synthetic copy onto a BG-013 target."""

    def __init__(
        self,
        source: OfflineDuckDBImporter,
        targets: FullImportTargets,
        fault_hook: Callable[[str, str], None] | None = None,
    ) -> None:
        self.full = OfflineFullImport(source, targets, fault_hook)
        self.source = source
        self.targets = targets
        self.state = self.full.root / ".offline-incremental-catchup"
        self.checkpoint_path = self.state / "checkpoint.json"
        self.report_path = self.state / "report.json"
        self.lock_path = self.state / "catchup.lock"

    @staticmethod
    def _validate_signed(document: dict[str, Any], version: str) -> None:
        unsigned = dict(document)
        checksum = unsigned.pop("checksum", None)
        if checksum != _digest(unsigned) or document.get("version") != version:
            raise ValueError("incremental evidence is invalid")

    def _full_checkpoint(self) -> dict[str, Any]:
        if not self.full.checkpoint_path.is_file():
            raise ValueError("accepted full-import checkpoint is required")
        document = json.loads(self.full.checkpoint_path.read_text())
        self._validate_signed(document, FULL_IMPORT_VERSION)
        if document.get("phase") != "complete" or document.get("targets") != self.full._target_ids():
            raise ValueError("full-import checkpoint is incomplete or target-mismatched")
        return document

    def _save(self, checkpoint: dict[str, Any]) -> None:
        checkpoint.pop("checksum", None)
        checkpoint["checksum"] = _digest(checkpoint)
        _atomic(self.checkpoint_path, checkpoint)

    def _load(self, full: dict[str, Any]) -> dict[str, Any]:
        if self.checkpoint_path.exists():
            checkpoint = json.loads(self.checkpoint_path.read_text())
            self._validate_signed(checkpoint, CATCHUP_VERSION)
            if (
                checkpoint.get("full_run_id") != full["run_id"]
                or checkpoint.get("full_checkpoint_checksum") != full["checksum"]
                or checkpoint.get("targets") != self.full._target_ids()
            ):
                raise ValueError("incremental checkpoint binding mismatch")
            return checkpoint
        binding = {
            "version": CATCHUP_VERSION,
            "full_run_id": full["run_id"],
            "full_checkpoint_checksum": full["checksum"],
            "targets": self.full._target_ids(),
        }
        checkpoint = {
            **binding,
            "catchup_run_id": _digest(binding),
            "run_id": full["run_id"],
            "phase": "catchup",
            "passes": 0,
            "active_fingerprint": "",
            "domains": {},
            "stability": [],
            "errors": [],
        }
        self._save(checkpoint)
        return checkpoint

    def _stage_snapshot(self, fingerprint: str) -> None:
        self.full.stage = self.state / "snapshots" / fingerprint
        for table in ALLOWED_TABLES:
            self.full._stage_table(table, fingerprint)

    def _watermark(self, fingerprint: str) -> dict[str, Any]:
        tables = []
        for table in ALLOWED_TABLES:
            evidence = self.full._verify_stage(self.full.stage / f"{table}.ndjson", table, fingerprint)
            tables.append({"table": table, **evidence})
        return {"source_fingerprint": fingerprint, "tables": tables, "table_count": len(tables)}

    def _apply(self, checkpoint: dict[str, Any], fingerprint: str) -> None:
        if checkpoint["active_fingerprint"] != fingerprint:
            checkpoint["active_fingerprint"] = fingerprint
            checkpoint["domains"] = {}
            self._save(checkpoint)
        original = self.full.checkpoint_path
        self.full.checkpoint_path = self.checkpoint_path
        try:
            for domain in POSTGRES_DOMAINS:
                self.full._import_pg_domain(checkpoint, domain, fingerprint)
            self.full._import_market(checkpoint, fingerprint)
        finally:
            self.full.checkpoint_path = original

    def _record_control_checkpoint(self, checkpoint: dict[str, Any], fingerprint: str) -> None:
        with self.targets.postgres.connection() as connection:
            connection.execute(
                "INSERT INTO migration_checkpoint "
                "(run_id,domain,shard,revision,status,source_watermark,target_watermark,cursor_json,evidence_json,updated_at) "
                "VALUES (%s,'incremental-catchup','',%s,'completed',%s,%s,%s,%s,CURRENT_TIMESTAMP) "
                "ON CONFLICT (run_id,domain,shard) DO UPDATE SET "
                "revision=EXCLUDED.revision,status='completed',source_watermark=EXCLUDED.source_watermark,"
                "target_watermark=EXCLUDED.target_watermark,cursor_json=EXCLUDED.cursor_json,"
                "evidence_json=EXCLUDED.evidence_json,updated_at=CURRENT_TIMESTAMP",
                (
                    checkpoint["catchup_run_id"], checkpoint["passes"], fingerprint, fingerprint,
                    json.dumps({"passes": checkpoint["passes"]}),
                    json.dumps({"stability": checkpoint["stability"]}),
                ),
            )

    @staticmethod
    def _completion_report(checkpoint: dict[str, Any]) -> dict[str, Any]:
        completion = checkpoint.get("completion")
        if not isinstance(completion, dict):
            raise ValueError("incremental completion evidence is missing")
        return {
            "version": CATCHUP_VERSION,
            "status": "complete",
            "catchup_run_id": checkpoint["catchup_run_id"],
            "full_run_id": checkpoint["full_run_id"],
            "source_high_watermark": checkpoint["source_high_watermark"],
            "stability": checkpoint["stability"],
            "passes": checkpoint["passes"],
            "lag": 0,
            "domains": completion["domains"],
            "spool_pending": 0,
            "checkpoint_checksum": checkpoint["checksum"],
        }

    def run(self, max_passes: int = 3) -> dict[str, Any]:
        if not 1 <= max_passes <= MAX_PASSES:
            raise ValueError("max_passes outside supported bound")
        full = self._full_checkpoint()
        self.state.mkdir(parents=True, exist_ok=True)
        checkpoint: dict[str, Any] | None = None
        with self.lock_path.open("a+") as lock:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
            try:
                checkpoint = self._load(full)
                if checkpoint["phase"] == "complete":
                    expected = self._completion_report(checkpoint)
                    if not self.report_path.exists():
                        _atomic(self.report_path, expected)
                        return expected
                    report = json.loads(self.report_path.read_text())
                    if report != expected:
                        raise ValueError("incremental report binding mismatch")
                    return report
                for _ in range(max_passes):
                    before = self.source.inspect()["source_fingerprint"]
                    self._stage_snapshot(before)
                    checkpoint["source_high_watermark"] = self._watermark(before)
                    self._apply(checkpoint, before)
                    pre_reconcile = self.source.inspect()["source_fingerprint"]
                    checkpoint["passes"] += 1
                    checkpoint["stability"] = [before, pre_reconcile]
                    self._save(checkpoint)
                    if before != pre_reconcile:
                        continue
                    reconciliation = self.full._reconcile(checkpoint, before)
                    after = self.source.inspect()["source_fingerprint"]
                    checkpoint["stability"] = [before, pre_reconcile, after]
                    self._save(checkpoint)
                    if before != after or reconciliation["status"] != "ok":
                        continue
                    checkpoint["phase"] = "complete"
                    checkpoint["completion"] = {"domains": reconciliation["domains"]}
                    self._record_control_checkpoint(checkpoint, after)
                    self._save(checkpoint)
                    report = self._completion_report(checkpoint)
                    _atomic(self.report_path, report)
                    return report
                report = {
                    "version": CATCHUP_VERSION,
                    "status": "incomplete",
                    "catchup_run_id": checkpoint["catchup_run_id"],
                    "passes": checkpoint["passes"],
                    "lag": 1,
                    "stability": checkpoint["stability"],
                    "reason": "source_changed_or_reconciliation_failed",
                }
                _atomic(self.report_path, report)
                return report
            except Exception as error:
                if checkpoint is not None:
                    checkpoint["errors"] = (checkpoint["errors"] + [type(error).__name__])[-MAX_ERRORS:]
                    self._save(checkpoint)
                raise
            finally:
                fcntl.flock(lock.fileno(), fcntl.LOCK_UN)
