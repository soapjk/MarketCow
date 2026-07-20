from __future__ import annotations

import fcntl
import hashlib
import json
import os
import shutil
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional

from .telemetry import sanitize_text, telemetry_call, telemetry_elapsed

from .clickhouse_repositories import ClickHouseMarketBarRepository
from .bar_version import raw_content_rank, raw_content_version


DATASET_COLUMNS = {
    "raw": ClickHouseMarketBarRepository.RAW_COLUMNS,
    "canonical": ClickHouseMarketBarRepository.CANONICAL_COLUMNS,
}
DATETIME_COLUMNS = {"bar_time", "observed_at", "ingested_at", "updated_at"}
FLOAT_COLUMNS = {
    "open", "high", "low", "close", "raw_close", "adjustment_factor", "volume", "amount"
}
INTEGER_COLUMNS = {"source_count", "version"}
OPTIONAL_COLUMNS = {
    "amount", "raw_close", "adjustment_factor", "source_sequence", "raw_artifact_id"
}


def _utc_iso(value: Any) -> str:
    if isinstance(value, datetime):
        parsed = value
    else:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).isoformat(timespec="milliseconds")


def normalize_bar(dataset: str, row: Dict[str, Any]) -> Dict[str, Any]:
    if dataset not in DATASET_COLUMNS:
        raise ValueError("dataset must be raw or canonical")
    normalized: Dict[str, Any] = {}
    for column in DATASET_COLUMNS[dataset]:
        if dataset == "raw" and column in {"content_rank", "content_version"}:
            continue
        value = row.get(column)
        if value is None and column not in OPTIONAL_COLUMNS:
            raise ValueError(f"{dataset} bar requires {column}")
        if value is not None and column in DATETIME_COLUMNS:
            value = _utc_iso(value)
        elif value is not None and column in FLOAT_COLUMNS:
            value = float(value)
        elif value is not None and column in INTEGER_COLUMNS:
            value = int(value)
        normalized[column] = value
    if dataset == "raw":
        normalized["content_rank"] = raw_content_rank(normalized)
        normalized["content_version"] = raw_content_version(
            normalized["ingested_at"], normalized["content_rank"]
        )
    return normalized


def stable_batch_id(dataset: str, rows: List[Dict[str, Any]]) -> str:
    encoded = json.dumps(
        {"dataset": dataset, "rows": rows}, ensure_ascii=False,
        allow_nan=False, sort_keys=True, separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _bounded_error(error: BaseException) -> str:
    return sanitize_text(f"{type(error).__name__}: {error}")[:1000]


class AuthoritativeWriteError(RuntimeError):
    """Fail-closed writer error carrying a bounded machine-readable outcome."""

    def __init__(self, outcome: Dict[str, Any]) -> None:
        self.outcome = outcome
        super().__init__(f"ClickHouse authoritative write {outcome['status']}")


class LocalClickHouseSpool:
    """Atomic development-only filesystem WAL for failed ClickHouse batches."""

    def __init__(self, root: Path, allowed_root: Path, quota_bytes: int = 1073741824,
                 quota_warning_ratio: float = 0.8) -> None:
        self.root = root.resolve()
        self.allowed_root = allowed_root.resolve()
        if self.root != self.allowed_root and self.allowed_root not in self.root.parents:
            raise ValueError("ClickHouse spool must stay within its allowed development root")
        if not 1048576 <= quota_bytes <= 1099511627776:
            raise ValueError("spool quota must be between 1 MiB and 1 TiB")
        if not 0.5 <= quota_warning_ratio < 1:
            raise ValueError("spool quota warning ratio must be between 0.5 and 1")
        self.quota_bytes = quota_bytes
        self.quota_warning_ratio = quota_warning_ratio
        self.telemetry: Any = None
        self.pending = self.root / "pending"
        self.replayed = self.root / "replayed"
        self.intents = self.root / "intents"
        self.processing_intents = self.root / "processing-intents"
        self.quarantine = self.root / "quarantine"
        self.pending.mkdir(parents=True, exist_ok=True)
        self.replayed.mkdir(parents=True, exist_ok=True)
        self.intents.mkdir(parents=True, exist_ok=True)
        self.processing_intents.mkdir(parents=True, exist_ok=True)
        self.quarantine.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def _now() -> str:
        return datetime.now(timezone.utc).isoformat(timespec="milliseconds")

    @staticmethod
    def _payload_checksum(payload: Dict[str, Any]) -> str:
        clean = {key: value for key, value in payload.items() if key != "_checksum"}
        encoded = json.dumps(clean, ensure_ascii=False, allow_nan=False,
                             sort_keys=True, separators=(",", ":")).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()

    def usage(self, limit: int = 10000) -> Dict[str, Any]:
        total = files = 0
        truncated = False
        for folder, _, names in os.walk(self.root, followlinks=False):
            for name in names:
                path = Path(folder) / name
                if path.is_symlink():
                    continue
                files += 1
                if files > limit:
                    truncated = True
                    break
                try:
                    total += path.stat().st_size
                except OSError:
                    continue
            if truncated:
                break
        free = shutil.disk_usage(self.root).free
        return {"bytes": total, "files": min(files, limit), "truncated": truncated,
                "quota_bytes": self.quota_bytes,
                "warning": total >= self.quota_bytes * self.quota_warning_ratio,
                "free_bytes": free}

    def _atomic_json(self, path: Path, payload: Dict[str, Any]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = {key: value for key, value in payload.items() if key != "_checksum"}
        payload["_checksum"] = self._payload_checksum(payload)
        encoded = json.dumps(payload, ensure_ascii=False, allow_nan=False,
                             sort_keys=True).encode("utf-8")
        usage = self.usage()
        previous = path.stat().st_size if path.exists() else 0
        if usage["truncated"] or usage["bytes"] - previous + len(encoded) > self.quota_bytes:
            raise OSError("ClickHouse spool quota exceeded")
        if usage["free_bytes"] < len(encoded) + 4096:
            raise OSError("insufficient disk space for ClickHouse spool write")
        descriptor, temporary = tempfile.mkstemp(prefix=".write-", dir=path.parent)
        try:
            with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
                handle.write(encoded.decode("utf-8"))
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, path)
            directory = os.open(path.parent, os.O_RDONLY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
        finally:
            if os.path.exists(temporary):
                os.unlink(temporary)

    def enqueue(
        self, dataset: str, batch_id: str, rows: List[Dict[str, Any]], error: str,
        increment_attempt: bool = True,
    ) -> Path:
        path = self.pending / f"{batch_id}.json"
        existing = self.read(path, require_checksum=True) if path.exists() else {}
        now = self._now()
        payload = {
            "dataset": dataset, "batch_id": batch_id, "rows": rows,
            "attempts": int(existing.get("attempts", 0)) + int(increment_attempt),
            "created_at": existing.get("created_at", now), "last_attempt_at": now,
            "last_error": error[:4000],
        }
        self._atomic_json(path, payload)
        return path

    @classmethod
    def read(cls, path: Path, require_checksum: bool = False) -> Dict[str, Any]:
        with path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
        if not isinstance(payload, dict):
            raise ValueError("spool item must be a JSON object")
        checksum = payload.get("_checksum")
        if checksum is None:
            if require_checksum:
                raise ValueError("spool item checksum is missing")
            return payload
        if checksum != cls._payload_checksum(payload):
            raise ValueError("spool item checksum mismatch")
        return payload

    def quarantine_item(self, path: Path, reason: str) -> Path:
        destination = self.quarantine / (self._now().replace(":", "-") + "-" + path.name)
        os.replace(path, destination)
        self._atomic_json(destination.with_suffix(".meta.json"), {
            "original_path": str(path.relative_to(self.root)),
            "quarantined_at": self._now(), "reason": reason[:4000],
        })
        return destination

    def mark_replayed(self, path: Path, payload: Dict[str, Any]) -> None:
        payload = {**payload, "replayed_at": self._now(), "last_error": ""}
        destination = self.replayed / path.name
        self._atomic_json(destination, payload)
        path.unlink()

    def remove_intent(self, intent_id: str) -> None:
        path = self.intents / f"{intent_id}.json"
        if path.exists():
            path.unlink()
            directory = os.open(path.parent, os.O_RDONLY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)

    def save_intent(self, intent_id: str, rows: List[Dict[str, Any]], pending: List[str]) -> None:
        self._atomic_json(self.intents / f"{intent_id}.json", {
            "intent_id": intent_id, "rows": rows, "pending": pending,
            "callback_attempts": 0, "last_callback_error": "",
        })

    def complete_chunk(self, intent_id: str, batch_id: str) -> None:
        path = self.intents / f"{intent_id}.json"
        if not path.exists():
            return
        intent = self.read(path, require_checksum=True)
        intent["pending"] = [value for value in intent["pending"] if value != batch_id]
        self._atomic_json(path, intent)
    def ready_intents(self, limit: int) -> List[Dict[str, Any]]:
        # A crash while claimed is recoverable; rebuild itself is idempotent.
        processing, _ = self._bounded_files(self.processing_intents, limit)
        for path in processing:
            os.replace(path, self.intents / path.name)
        paths, _ = self._bounded_files(self.intents, limit)
        ready = []
        for path in paths:
            intent = self.read(path, require_checksum=True)
            remaining = [batch_id for batch_id in intent["pending"]
                         if (self.pending / f"{batch_id}.json").exists()]
            if remaining != intent["pending"]:
                intent["pending"] = remaining
                self._atomic_json(path, intent)
            if not remaining:
                claimed = self.processing_intents / path.name
                os.replace(path, claimed)
                ready.append(self.read(claimed, require_checksum=True))
        return ready

    def callback_result(self, intent: Dict[str, Any], error: str = "") -> None:
        path = self.processing_intents / f"{intent['intent_id']}.json"
        if error:
            intent["callback_attempts"] = int(intent.get("callback_attempts", 0)) + 1
            intent["last_callback_error"] = error[:4000]
            self._atomic_json(self.intents / path.name, intent)
            if path.exists():
                path.unlink()
        elif path.exists():
            path.unlink()

    @staticmethod
    def _bounded_files(folder: Path, limit: int) -> tuple[List[Path], bool]:
        files = []
        with os.scandir(folder) as entries:
            for entry in entries:
                if entry.is_file(follow_symlinks=False) and entry.name.endswith(".json"):
                    if len(files) >= limit:
                        return sorted(files), True
                    files.append(Path(entry.path))
        return sorted(files), False

    def diagnostics(self, limit: int = 1000) -> Dict[str, Any]:
        if not 1 <= limit <= 10000:
            raise ValueError("diagnostic limit must be between 1 and 10000")
        pending, pending_truncated = self._bounded_files(self.pending, limit)
        replayed, replayed_truncated = self._bounded_files(self.replayed, limit)
        failed = 0
        oldest = None
        for path in pending:
            payload = self.read(path)
            failed += bool(payload.get("last_error"))
            created = payload.get("created_at")
            if created and (oldest is None or created < oldest):
                oldest = created
        lag = 0.0
        if oldest:
            lag = max(0.0, (datetime.now(timezone.utc) - datetime.fromisoformat(oldest)).total_seconds())
        return {
            "pending": len(pending), "failed": failed, "replayed": len(replayed),
            "oldest_pending_lag_seconds": round(lag, 3),
            "truncated": pending_truncated or replayed_truncated,
            "scan_limit": limit,
            "quota": self.usage(limit),
        }


class ReliableClickHouseWriter:
    def __init__(
        self, repository: ClickHouseMarketBarRepository,
        spool: LocalClickHouseSpool, batch_size: int = 5000,
    ) -> None:
        if not 1000 <= batch_size <= 50000:
            raise ValueError("batch size must be between 1000 and 50000")
        self.repository = repository
        self.spool = spool
        self.batch_size = batch_size
        self.on_raw_replayed: Optional[Callable[[List[Dict[str, Any]]], None]] = None
        self.last_replay_callback: Dict[str, Any] = {"status": "not_run"}

    @staticmethod
    def _chunks(rows: List[Dict[str, Any]], size: int) -> Iterable[List[Dict[str, Any]]]:
        for start in range(0, len(rows), size):
            yield rows[start:start + size]

    def _insert(self, dataset: str, rows: List[Dict[str, Any]], batch_id: str) -> int:
        method = (self.repository.insert_raw_bars if dataset == "raw"
                  else self.repository.insert_canonical_bars)
        return method(rows, batch_id=batch_id)

    def write(self, dataset: str, rows: List[Dict[str, Any]]) -> Dict[str, Any]:
        if dataset not in DATASET_COLUMNS:
            raise ValueError("dataset must be raw or canonical")
        outcome: Dict[str, Any] = {
            "status": "success", "acknowledged": True, "verified": True,
            "durability": "committed", "retryable": False,
            "terminal": False, "rows": len(rows), "written": 0,
            "spooled": 0, "batches": 0, "intent_id": "",
            "pending_batches": [], "error": "",
        }
        started = telemetry_elapsed(self.spool.telemetry)
        normalized_rows = [normalize_bar(dataset, row) for row in rows]
        intent_id = stable_batch_id(dataset, normalized_rows)
        outcome["intent_id"] = intent_id
        chunks = []
        for chunk in self._chunks(normalized_rows, self.batch_size):
            batch_id = stable_batch_id(dataset, chunk)
            chunks.append((batch_id, chunk))
            outcome["batches"] += 1
        staged: List[tuple[Path, bool]] = []
        try:
            # WAL every micro-batch before the first ClickHouse mutation. A crash can
            # therefore never leave an acknowledged or untracked partial logical write.
            for batch_id, chunk in chunks:
                target = self.spool.pending / f"{batch_id}.json"
                existed = target.exists()
                path = self.spool.enqueue(
                    dataset, batch_id, chunk, "awaiting ClickHouse acknowledgement",
                    increment_attempt=False,
                )
                staged.append((path, existed))
            if dataset == "raw" and chunks:
                self.spool.save_intent(
                    intent_id, normalized_rows, [batch_id for batch_id, _ in chunks]
                )
        except Exception as error:
            # No ClickHouse call has occurred yet. Remove only WAL created by this
            # attempt; pre-existing durable items belong to an earlier retry.
            for path, existed in staged:
                if not existed and path.exists():
                    path.unlink()
            if staged:
                try:
                    directory = os.open(self.spool.pending, os.O_RDONLY)
                    try:
                        os.fsync(directory)
                    finally:
                        os.close(directory)
                except OSError:
                    pass
            outcome.update({
                "status": "terminal_failure", "acknowledged": False,
                "verified": False, "durability": "not_durable",
                "retryable": False, "terminal": True,
                "pending_batches": [path.stem for path, existed in staged if existed],
                "error": _bounded_error(error),
            })
            raise AuthoritativeWriteError(outcome) from None

        failed_batches = []
        for batch_id, chunk in chunks:
            path = self.spool.pending / f"{batch_id}.json"
            try:
                written = self._insert(dataset, chunk, batch_id)
                if written != len(chunk):
                    raise RuntimeError("ClickHouse acknowledgement row count mismatch")
            except Exception as error:
                try:
                    self.spool.enqueue(dataset, batch_id, chunk, _bounded_error(error))
                except Exception as wal_error:
                    outcome.update({
                        "status": "terminal_failure", "acknowledged": False,
                        "verified": False, "durability": "partial_unrecoverable",
                        "retryable": False, "terminal": True,
                        "pending_batches": [batch_id],
                        "error": _bounded_error(wal_error),
                    })
                    raise AuthoritativeWriteError(outcome) from None
                failed_batches.append(batch_id)
                outcome["spooled"] += len(chunk)
            else:
                outcome["written"] += written
                try:
                    payload = self.spool.read(path, require_checksum=True)
                    if dataset == "raw":
                        payload["intent_id"] = intent_id
                    self.spool.mark_replayed(path, payload)
                    if dataset == "raw":
                        self.spool.complete_chunk(intent_id, batch_id)
                except Exception as error:
                    # The target may already contain the batch. Preserve/recreate its
                    # stable WAL so retry remains safe and withholds caller acknowledgement.
                    try:
                        self.spool.enqueue(
                            dataset, batch_id, chunk, _bounded_error(error)
                        )
                    except Exception as wal_error:
                        outcome.update({
                            "status": "terminal_failure", "acknowledged": False,
                            "verified": False, "durability": "partial_unrecoverable",
                            "retryable": False, "terminal": True,
                            "pending_batches": [batch_id],
                            "error": _bounded_error(wal_error),
                        })
                        raise AuthoritativeWriteError(outcome) from None
                    failed_batches.append(batch_id)
                    outcome["spooled"] += len(chunk)
        if dataset == "raw" and chunks:
            try:
                for batch_id in failed_batches:
                    path = self.spool.pending / f"{batch_id}.json"
                    payload = self.spool.read(path, require_checksum=True)
                    self.spool._atomic_json(path, {**payload, "intent_id": intent_id})
                if not failed_batches:
                    self.spool.remove_intent(intent_id)
            except Exception as error:
                outcome.update({
                    "status": "terminal_failure", "acknowledged": False,
                    "verified": False, "durability": "partial_unrecoverable",
                    "retryable": False, "terminal": True,
                    "pending_batches": failed_batches[:1000],
                    "error": _bounded_error(error),
                })
                raise AuthoritativeWriteError(outcome) from None
        if failed_batches:
            outcome.update({
                "status": "durable_pending", "acknowledged": False,
                "verified": False, "durability": "wal_fsynced",
                "retryable": True, "pending_batches": failed_batches,
                "error": "ClickHouse batch unavailable; durable replay required",
            })
        if self.spool.telemetry:
            elapsed = telemetry_elapsed(self.spool.telemetry, started)
            if elapsed is not None:
                telemetry_call(
                    self.spool.telemetry, "safe",
                    "histogram", "ingest_write_latency_seconds",
                    elapsed,
                    backend="clickhouse", outcome=outcome["status"],
                )
            try:
                diagnostics = self.spool.diagnostics()
                for state in ("pending", "failed", "replayed"):
                    telemetry_call(
                        self.spool.telemetry, "safe",
                        "gauge", "wal_items", diagnostics[state], state=state
                    )
            except Exception:
                pass
        return outcome

    def replay(self, limit: int = 100) -> Dict[str, Any]:
        if not 1 <= limit <= 1000:
            raise ValueError("replay limit must be between 1 and 1000")
        outcome = {"attempted": 0, "replayed": 0, "failed": 0, "quarantined": 0,
                   "callback_attempted": 0, "callback_ok": 0,
                   "callback_failed": 0, "remaining": 0,
                   "truncated": False, "lock_busy": False,
                   "legacy_migrated": 0, "legacy_invalid": 0, "legacy_errors": 0,
                   "legacy_blocked": 0}
        operator_path = self.spool.root / ".operator.lock"
        with operator_path.open("a+") as operator_lock:
            try:
                fcntl.flock(operator_lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                outcome["lock_busy"] = True
                outcome["remaining"] = 1
                outcome["truncated"] = True
                return outcome
            from .spool_operator import SpoolOperator
            migration = SpoolOperator(self.spool).migrate_legacy(
                limit, already_locked=True,
                kinds=("wal-pending", "raw-intents", "raw-processing"),
            )
            outcome["legacy_migrated"] = migration["migrated"]
            outcome["legacy_invalid"] = migration["invalid"]
            outcome["legacy_errors"] = migration["errors"]
            outcome["legacy_blocked"] = migration["remaining"]
            outcome["quarantined"] += migration["quarantined"]
            lock_path = self.spool.root / ".replay.lock"
            with lock_path.open("a+") as lock:
                fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
                try:
                    paths, wal_truncated = self.spool._bounded_files(self.spool.pending, limit)
                    for path in paths:
                        try:
                            payload = self.spool.read(path)
                            if not payload.get("_checksum"):
                                outcome["legacy_blocked"] = max(
                                    outcome["legacy_blocked"], 1
                                )
                                continue
                        except Exception as error:
                            try:
                                self.spool.quarantine_item(path, _bounded_error(error))
                            except Exception:
                                outcome["legacy_blocked"] += 1
                            else:
                                outcome["quarantined"] += 1
                            continue
                        outcome["attempted"] += 1
                        try:
                            written = self._insert(
                                payload["dataset"], payload["rows"], payload["batch_id"]
                            )
                            if written != len(payload["rows"]):
                                raise RuntimeError(
                                    "ClickHouse acknowledgement row count mismatch"
                                )
                        except Exception as error:
                            self.spool.enqueue(payload["dataset"], payload["batch_id"],
                                               payload["rows"], _bounded_error(error))
                            outcome["failed"] += 1
                        else:
                            self.spool.mark_replayed(path, payload)
                            outcome["replayed"] += 1
                            if payload["dataset"] == "raw" and payload.get("intent_id"):
                                self.spool.complete_chunk(payload["intent_id"], payload["batch_id"])
                    budget = limit - outcome["attempted"]
                    if self.on_raw_replayed and budget and not migration["remaining"]:
                        for intent in self.spool.ready_intents(budget):
                            outcome["callback_attempted"] += 1
                            try:
                                self.on_raw_replayed(intent["rows"])
                            except Exception as error:
                                bounded = _bounded_error(error)
                                self.spool.callback_result(intent, bounded)
                                outcome["callback_failed"] += 1
                                self.last_replay_callback = {
                                    "status": "error", "error": bounded,
                                }
                            else:
                                self.spool.callback_result(intent)
                                outcome["callback_ok"] += 1
                                self.last_replay_callback = {
                                    "status": "ok", "rows": len(intent["rows"]),
                                }
                    pending, pending_more = self.spool._bounded_files(
                        self.spool.pending, 10000
                    )
                    intents, intents_more = self.spool._bounded_files(
                        self.spool.intents, 10000
                    )
                    processing, processing_more = self.spool._bounded_files(
                        self.spool.processing_intents, 10000
                    )
                    outcome["remaining"] = len(pending) + len(intents) + len(processing)
                    outcome["truncated"] = bool(
                        outcome["remaining"] or wal_truncated or pending_more or intents_more
                        or processing_more or migration["truncated"]
                    )
                finally:
                    fcntl.flock(lock.fileno(), fcntl.LOCK_UN)
            fcntl.flock(operator_lock.fileno(), fcntl.LOCK_UN)
        if self.spool.telemetry:
            try:
                diagnostics = self.spool.diagnostics()
                for state in ("pending", "failed", "replayed"):
                    telemetry_call(
                        self.spool.telemetry, "safe",
                        "gauge", "wal_items", diagnostics[state], state=state
                    )
                quarantine, _ = self.spool._bounded_files(self.spool.quarantine, 10000)
                telemetry_call(
                    self.spool.telemetry, "safe",
                    "gauge", "wal_items", len(quarantine), state="quarantine"
                )
            except Exception:
                pass
        return outcome
