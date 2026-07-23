from __future__ import annotations

import fcntl
import json
import os
from contextlib import contextmanager
from datetime import datetime, timezone
from functools import wraps
from pathlib import Path
from typing import Any, Dict, Iterator, List

from .clickhouse_writer import LocalClickHouseSpool
from .telemetry import sanitize_text, telemetry_call


MAX_AUDIT_BYTES = 256 * 1024
MAX_AUDIT_EVENTS = 500


def _serialized_read(action: str):
    def decorate(function):
        @wraps(function)
        def wrapped(self, *args, **kwargs):
            lock_path = self.spool.root / ".operator.lock"
            with lock_path.open("a+") as lock:
                try:
                    fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError:
                    self._metric(action, "blocked")
                    raise RuntimeError("another spool operation is active") from None
                try:
                    return function(self, *args, **kwargs)
                finally:
                    fcntl.flock(lock.fileno(), fcntl.LOCK_UN)
        return wrapped
    return decorate


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
        self.telemetry = getattr(spool, "telemetry", None)

    def _metric(self, action: str, outcome: str) -> None:
        telemetry_call(self.telemetry, "record_operator", action, outcome)

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
                normalized = {
                    "quarantine-corrupt": "quarantine",
                    "retry-scheduler-failed": "retry",
                    "cleanup-replayed": "cleanup",
                }.get(action, action)
                self._metric(normalized, "ok")
            except Exception as error:
                self._trace(action, "error", str(error))
                self._metric("audit", "error")
                raise
            finally:
                fcntl.flock(lock.fileno(), fcntl.LOCK_UN)

    def _trace(self, action: str, status: str, error: str = "") -> None:
        record = json.dumps({
            "at": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
            "action": action, "status": status, "error": sanitize_text(error),
        }, sort_keys=True) + "\n"
        descriptor = os.open(
            self.audit_log,
            os.O_WRONLY | os.O_CREAT | os.O_APPEND | getattr(os, "O_NOFOLLOW", 0),
            0o600,
        )
        try:
            os.write(descriptor, record.encode("utf-8"))
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        if self.audit_log.stat().st_size > MAX_AUDIT_BYTES:
            lines = self.audit_log.read_bytes().splitlines()[-MAX_AUDIT_EVENTS:]
            selected = []
            used = 0
            for line in reversed(lines):
                size = len(line) + 1
                if used + size > MAX_AUDIT_BYTES:
                    break
                selected.append(line)
                used += size
            bounded = b"\n".join(reversed(selected))
            if selected:
                bounded += b"\n"
            temporary = self.audit_log.with_suffix(".tmp")
            descriptor = os.open(
                temporary,
                os.O_WRONLY | os.O_CREAT | os.O_TRUNC | getattr(os, "O_NOFOLLOW", 0),
                0o600,
            )
            try:
                os.write(descriptor, bounded)
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
            os.replace(temporary, self.audit_log)
            directory = os.open(self.audit_log.parent, os.O_RDONLY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)

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

    @_serialized_read("list")
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
                        "error": sanitize_text(error)}
            items.append(item)
        result = {"status": "attention" if any(item["status"] != "ok" for item in items)
                  else "ok", "kind": kind, "items": items, "truncated": truncated}
        self._metric("list", "partial" if result["status"] != "ok" else "ok")
        return result

    def audit(self, limit: int = 1000) -> Dict[str, Any]:
        if not 1 <= limit <= 10000:
            raise ValueError("audit limit must be between 1 and 10000")
        corrupt: List[Dict[str, str]] = []
        wal_ids, replayed_ids, intent_refs = set(), set(), set()
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
                                    "error": sanitize_text(error)})
                    continue
                if kind == "wal-pending":
                    wal_ids.add(str(payload.get("batch_id")))
                if kind == "wal-replayed" and payload.get("dataset") == "raw":
                    replayed_ids.add(str(payload.get("batch_id")))
                if kind in {"raw-intents", "raw-processing"}:
                    intent_refs.update(str(value) for value in payload.get("pending", []))
                if checked >= limit:
                    break
            if checked >= limit:
                break
        missing_wal = sorted(intent_refs - wal_ids - replayed_ids)[:limit]
        orphan_wal = sorted(wal_ids - intent_refs)[:limit]
        result = {"status": "ok" if not corrupt and not missing_wal else "attention",
                  "checked": checked, "truncated": checked >= limit,
                  "corrupt": corrupt[:limit], "missing_wal_references": missing_wal,
                  "orphan_wal": orphan_wal, "quota": self.spool.usage(limit)}
        self._metric("audit", "partial" if result["status"] != "ok" else "ok")
        return result

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
