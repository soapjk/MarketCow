from __future__ import annotations

import fcntl
import json
import os
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterator, List

from .clickhouse_writer import LocalClickHouseSpool


KINDS = {
    "wal-pending": "pending", "wal-replayed": "replayed",
    "raw-intents": "intents", "raw-processing": "processing_intents",
    "quarantine": "quarantine",
}
SCHEDULER_KINDS = {
    "scheduler-pending": "pending", "scheduler-processing": "processing",
    "scheduler-failed": "failed",
}


class SpoolOperator:
    """Explicit, local-only and bounded operations over a development spool."""

    def __init__(self, spool: LocalClickHouseSpool) -> None:
        self.spool = spool
        self.scheduler_root = spool.root / "canonical-scheduler"
        self.audit_log = spool.root / "operator-audit.jsonl"

    @contextmanager
    def mutation(self, action: str) -> Iterator[None]:
        lock_path = self.spool.root / ".operator.lock"
        with lock_path.open("a+") as lock:
            try:
                fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise RuntimeError("another spool operation is active") from None
            try:
                yield
                self._trace(action, "ok")
            except Exception as error:
                self._trace(action, "error", str(error))
                raise
            finally:
                fcntl.flock(lock.fileno(), fcntl.LOCK_UN)

    def _trace(self, action: str, status: str, error: str = "") -> None:
        record = json.dumps({
            "at": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
            "action": action, "status": status, "error": error[:1000],
        }, sort_keys=True) + "\n"
        descriptor = os.open(self.audit_log, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
        try:
            os.write(descriptor, record.encode("utf-8"))
            os.fsync(descriptor)
        finally:
            os.close(descriptor)

    def _folder(self, kind: str) -> Path:
        if kind in KINDS:
            folder = getattr(self.spool, KINDS[kind])
        elif kind in SCHEDULER_KINDS:
            folder = self.scheduler_root / SCHEDULER_KINDS[kind]
        else:
            raise ValueError("unknown spool item kind")
        resolved = folder.resolve()
        if resolved != self.spool.root and self.spool.root not in resolved.parents:
            raise ValueError("spool item folder escapes the allowed spool root")
        return resolved

    def list_items(self, kind: str, limit: int = 100) -> Dict[str, Any]:
        if not 1 <= limit <= 1000:
            raise ValueError("operator limit must be between 1 and 1000")
        folder = self._folder(kind)
        if not folder.exists():
            return {"status": "ok", "kind": kind, "items": [], "truncated": False}
        paths, truncated = self.spool._bounded_files(folder, limit)
        items = []
        for path in paths:
            try:
                payload = self.spool.read(path, require_checksum=True)
                item = {"name": path.name, "status": "ok", "bytes": path.stat().st_size,
                        "checksum": payload.get("_checksum")}
                for key in ("dataset", "batch_id", "intent_id", "task_id", "attempts",
                            "created_at", "replayed_at", "last_error"):
                    if key in payload:
                        item[key] = payload[key]
            except Exception as error:
                item = {"name": path.name, "status": "corrupt",
                        "error": str(error)[:1000]}
            items.append(item)
        return {"status": "attention" if any(item["status"] != "ok" for item in items)
                else "ok", "kind": kind, "items": items, "truncated": truncated}

    def audit(self, limit: int = 1000) -> Dict[str, Any]:
        if not 1 <= limit <= 10000:
            raise ValueError("audit limit must be between 1 and 10000")
        corrupt: List[Dict[str, str]] = []
        wal_ids = set()
        intent_refs = set()
        checked = 0
        for kind in KINDS | SCHEDULER_KINDS:
            folder = self._folder(kind)
            if not folder.exists():
                continue
            paths, _ = self.spool._bounded_files(folder, max(1, limit - checked))
            for path in paths:
                checked += 1
                try:
                    payload = self.spool.read(path, require_checksum=True)
                except Exception as error:
                    corrupt.append({"kind": kind, "name": path.name,
                                    "error": str(error)[:1000]})
                    continue
                if kind == "wal-pending":
                    wal_ids.add(str(payload.get("batch_id")))
                if kind in {"raw-intents", "raw-processing"}:
                    intent_refs.update(str(value) for value in payload.get("pending", []))
                if checked >= limit:
                    break
            if checked >= limit:
                break
        missing_wal = sorted(intent_refs - wal_ids)[:limit]
        orphan_wal = sorted(wal_ids - intent_refs)[:limit]
        return {"status": "ok" if not corrupt and not missing_wal else "attention",
                "checked": checked, "truncated": checked >= limit,
                "corrupt": corrupt[:limit], "missing_wal_references": missing_wal,
                "orphan_wal": orphan_wal, "quota": self.spool.usage(limit)}

    def quarantine_corrupt(self, limit: int = 100) -> Dict[str, Any]:
        if not 1 <= limit <= 1000:
            raise ValueError("operator limit must be between 1 and 1000")
        moved = errors = 0
        with self.mutation("quarantine-corrupt"):
            for kind in ("wal-pending", "raw-intents", "raw-processing",
                         "scheduler-pending", "scheduler-processing", "scheduler-failed"):
                folder = self._folder(kind)
                if not folder.exists():
                    continue
                paths, _ = self.spool._bounded_files(folder, max(1, limit - moved - errors))
                for path in paths:
                    try:
                        self.spool.read(path, require_checksum=True)
                    except Exception as error:
                        try:
                            self.spool.quarantine_item(path, str(error))
                            moved += 1
                        except Exception:
                            errors += 1
                    if moved + errors >= limit:
                        break
                if moved + errors >= limit:
                    break
        return {"status": "ok" if not errors else "partial", "moved": moved,
                "errors": errors, "limit": limit}

    def retry_scheduler_failed(self, limit: int = 100) -> Dict[str, Any]:
        if not 1 <= limit <= 1000:
            raise ValueError("operator limit must be between 1 and 1000")
        source, destination = self._folder("scheduler-failed"), self._folder("scheduler-pending")
        destination.mkdir(parents=True, exist_ok=True)
        retried = corrupt = 0
        with self.mutation("retry-scheduler-failed"):
            if source.exists():
                paths, _ = self.spool._bounded_files(source, limit)
                for path in paths:
                    try:
                        payload = self.spool.read(path, require_checksum=True)
                        payload.update({"attempts": 0, "next_attempt_epoch": 0,
                                        "last_error": ""})
                        self.spool._atomic_json(destination / path.name, payload)
                        path.unlink()
                        retried += 1
                    except Exception as error:
                        try:
                            self.spool.quarantine_item(path, str(error))
                        except Exception:
                            pass
                        corrupt += 1
        return {"status": "ok" if not corrupt else "partial", "retried": retried,
                "quarantined": corrupt, "limit": limit}

    def cleanup_replayed(self, retention_seconds: int, limit: int = 100,
                         now: datetime | None = None) -> Dict[str, Any]:
        if not 0 <= retention_seconds <= 31536000:
            raise ValueError("retention must be between 0 and 31536000 seconds")
        if not 1 <= limit <= 1000:
            raise ValueError("operator limit must be between 1 and 1000")
        point = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
        removed = skipped = corrupt = 0
        with self.mutation("cleanup-replayed"):
            paths, _ = self.spool._bounded_files(self.spool.replayed, limit)
            for path in paths:
                try:
                    payload = self.spool.read(path, require_checksum=True)
                    replayed_at = datetime.fromisoformat(
                        str(payload["replayed_at"]).replace("Z", "+00:00")
                    ).astimezone(timezone.utc)
                except Exception:
                    corrupt += 1
                    continue
                if (point - replayed_at).total_seconds() >= retention_seconds:
                    path.unlink()
                    removed += 1
                else:
                    skipped += 1
        return {"status": "ok" if not corrupt else "partial", "removed": removed,
                "skipped": skipped, "corrupt": corrupt, "limit": limit}
