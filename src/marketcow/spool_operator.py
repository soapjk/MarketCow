from __future__ import annotations

import fcntl
import hashlib
import json
import os
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterator, List

from .clickhouse_writer import LocalClickHouseSpool, normalize_bar, stable_batch_id


KINDS = {
    "wal-pending": "pending", "wal-replayed": "replayed",
    "raw-intents": "intents", "raw-processing": "processing_intents",
    "quarantine": "quarantine",
}
SCHEDULER_KINDS = {
    "scheduler-pending": "pending", "scheduler-processing": "processing",
    "scheduler-failed": "failed",
}
LEGACY_KINDS = tuple(KINDS)[:-1] + tuple(SCHEDULER_KINDS)


def _exact_keys(payload: Dict[str, Any], required: set[str], optional: set[str]) -> None:
    keys = set(payload)
    if not required <= keys or keys - required - optional:
        raise ValueError("legacy item fields do not match its schema")


def _bounded_text(value: Any, name: str, maximum: int = 4000) -> str:
    if not isinstance(value, str) or not value or len(value) > maximum:
        raise ValueError(f"legacy {name} is invalid")
    return value


def _legacy_wal(kind: str, path: Path, payload: Dict[str, Any]) -> None:
    required = {"dataset", "batch_id", "rows", "attempts", "created_at",
                "last_attempt_at", "last_error"}
    optional = {"intent_id"}
    if kind == "wal-replayed":
        required.add("replayed_at")
    _exact_keys(payload, required, optional)
    dataset = payload.get("dataset")
    rows = payload.get("rows")
    if (dataset not in {"raw", "canonical"} or not isinstance(rows, list)
            or not 1 <= len(rows) <= 50000):
        raise ValueError("legacy WAL dataset or rows are invalid")
    normalized = [normalize_bar(dataset, row) for row in rows]
    batch_id = _bounded_text(payload.get("batch_id"), "batch_id", 64)
    if batch_id != path.stem or batch_id != stable_batch_id(dataset, normalized):
        raise ValueError("legacy WAL stable batch identity is invalid")
    if not isinstance(payload.get("attempts"), int) or not 0 <= payload["attempts"] <= 100000:
        raise ValueError("legacy WAL attempts are invalid")
    for field in ("created_at", "last_attempt_at"):
        _bounded_text(payload.get(field), field, 64)
        datetime.fromisoformat(payload[field].replace("Z", "+00:00"))
    if kind == "wal-replayed":
        _bounded_text(payload.get("replayed_at"), "replayed_at", 64)
        datetime.fromisoformat(payload["replayed_at"].replace("Z", "+00:00"))
    if not isinstance(payload.get("last_error"), str) or len(payload["last_error"]) > 4000:
        raise ValueError("legacy WAL error is invalid")
    if "intent_id" in payload:
        _bounded_text(payload["intent_id"], "intent_id", 64)


def _legacy_raw_intent(path: Path, payload: Dict[str, Any]) -> None:
    _exact_keys(payload, {"intent_id", "rows", "pending", "callback_attempts",
                          "last_callback_error"}, set())
    rows, pending = payload.get("rows"), payload.get("pending")
    if (not isinstance(rows, list) or not 1 <= len(rows) <= 100000
            or not isinstance(pending, list)):
        raise ValueError("legacy raw intent rows or pending list are invalid")
    normalized = [normalize_bar("raw", row) for row in rows]
    intent_id = _bounded_text(payload.get("intent_id"), "intent_id", 64)
    if intent_id != path.stem or intent_id != stable_batch_id("raw", normalized):
        raise ValueError("legacy raw intent stable identity is invalid")
    if len(pending) > 100 or len(set(pending)) != len(pending):
        raise ValueError("legacy raw intent pending list is invalid")
    if any(not isinstance(value, str) or len(value) != 64
           or any(character not in "0123456789abcdef" for character in value)
           for value in pending):
        raise ValueError("legacy raw intent pending batch id is invalid")
    attempts = payload.get("callback_attempts")
    if not isinstance(attempts, int) or not 0 <= attempts <= 100000:
        raise ValueError("legacy raw intent callback attempts are invalid")
    error = payload.get("last_callback_error")
    if not isinstance(error, str) or len(error) > 4000:
        raise ValueError("legacy raw intent error is invalid")


def _legacy_scheduler(path: Path, payload: Dict[str, Any]) -> None:
    _exact_keys(payload, {"task_id", "symbol", "interval", "adjustment", "start", "end",
                          "attempts", "created_at_epoch", "next_attempt_epoch", "last_error"},
                set())
    group = {}
    for field in ("symbol", "interval", "adjustment", "start", "end"):
        group[field] = _bounded_text(payload.get(field), field, 128)
    parsed_times = []
    for field in ("start", "end"):
        value = datetime.fromisoformat(group[field].replace("Z", "+00:00"))
        if value.tzinfo is None:
            raise ValueError("legacy scheduler time must include timezone")
        parsed_times.append(value.astimezone(timezone.utc))
    if parsed_times[0] > parsed_times[1]:
        raise ValueError("legacy scheduler range is invalid")
    encoded = json.dumps(group, sort_keys=True, separators=(",", ":")).encode()
    task_id = hashlib.sha256(encoded).hexdigest()
    if payload.get("task_id") != task_id or path.stem != task_id:
        raise ValueError("legacy scheduler stable identity is invalid")
    attempts = payload.get("attempts")
    if not isinstance(attempts, int) or not 0 <= attempts <= 100:
        raise ValueError("legacy scheduler attempts are invalid")
    for field in ("created_at_epoch", "next_attempt_epoch"):
        value = payload.get(field)
        if not isinstance(value, (int, float)) or value < 0:
            raise ValueError(f"legacy scheduler {field} is invalid")
    error = payload.get("last_error")
    if not isinstance(error, str) or len(error) > 4000:
        raise ValueError("legacy scheduler error is invalid")


def validate_legacy_item(kind: str, path: Path, payload: Dict[str, Any]) -> None:
    if "_checksum" in payload:
        raise ValueError("item is not legacy")
    if kind in {"wal-pending", "wal-replayed"}:
        _legacy_wal(kind, path, payload)
    elif kind in {"raw-intents", "raw-processing"}:
        _legacy_raw_intent(path, payload)
    elif kind in SCHEDULER_KINDS:
        _legacy_scheduler(path, payload)
    else:
        raise ValueError("legacy migration is not allowed for this kind")


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

    def migrate_legacy(self, limit: int = 100, already_locked: bool = False,
                       kinds: tuple[str, ...] = LEGACY_KINDS) -> Dict[str, Any]:
        if not 1 <= limit <= 1000:
            raise ValueError("operator limit must be between 1 and 1000")
        migrated = invalid = errors = checked = 0

        def perform() -> None:
            nonlocal migrated, invalid, errors, checked
            for kind in kinds:
                folder = self._folder(kind)
                if not folder.exists():
                    continue
                paths, _ = self.spool._bounded_files(folder, max(1, limit - checked))
                for path in paths:
                    if checked >= limit:
                        return
                    checked += 1
                    try:
                        payload = self.spool.read(path)
                        if payload.get("_checksum"):
                            continue
                        validate_legacy_item(kind, path, payload)
                        self.spool._atomic_json(path, payload)
                        migrated += 1
                    except (ValueError, KeyError, TypeError, json.JSONDecodeError) as error:
                        invalid += 1
                        try:
                            self.spool.quarantine_item(path, f"legacy migration rejected: {error}")
                        except OSError:
                            errors += 1
                    except OSError:
                        errors += 1

        if already_locked:
            perform()
            try:
                self._trace("migrate-legacy", "ok" if not errors else "partial")
            except OSError:
                errors += 1
        else:
            with self.mutation("migrate-legacy"):
                perform()
        return {"status": "ok" if not invalid and not errors else "attention",
                "checked": checked, "migrated": migrated, "invalid": invalid,
                "errors": errors, "limit": limit, "truncated": checked >= limit}

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
