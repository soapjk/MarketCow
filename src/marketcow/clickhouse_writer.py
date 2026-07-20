from __future__ import annotations

import hashlib
import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional

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


class LocalClickHouseSpool:
    """Atomic development-only filesystem WAL for failed ClickHouse batches."""

    def __init__(self, root: Path, allowed_root: Path) -> None:
        self.root = root.resolve()
        self.allowed_root = allowed_root.resolve()
        if self.root != self.allowed_root and self.allowed_root not in self.root.parents:
            raise ValueError("ClickHouse spool must stay within its allowed development root")
        self.pending = self.root / "pending"
        self.replayed = self.root / "replayed"
        self.intents = self.root / "intents"
        self.pending.mkdir(parents=True, exist_ok=True)
        self.replayed.mkdir(parents=True, exist_ok=True)
        self.intents.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def _now() -> str:
        return datetime.now(timezone.utc).isoformat(timespec="milliseconds")

    @staticmethod
    def _atomic_json(path: Path, payload: Dict[str, Any]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        descriptor, temporary = tempfile.mkstemp(prefix=".write-", dir=path.parent)
        try:
            with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
                json.dump(payload, handle, ensure_ascii=False, allow_nan=False, sort_keys=True)
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
        self, dataset: str, batch_id: str, rows: List[Dict[str, Any]], error: str
    ) -> Path:
        path = self.pending / f"{batch_id}.json"
        existing = self.read(path) if path.exists() else {}
        now = self._now()
        payload = {
            "dataset": dataset, "batch_id": batch_id, "rows": rows,
            "attempts": int(existing.get("attempts", 0)) + 1,
            "created_at": existing.get("created_at", now), "last_attempt_at": now,
            "last_error": error[:4000],
        }
        self._atomic_json(path, payload)
        return path

    @staticmethod
    def read(path: Path) -> Dict[str, Any]:
        with path.open("r", encoding="utf-8") as handle:
            return json.load(handle)

    def mark_replayed(self, path: Path, payload: Dict[str, Any]) -> None:
        payload = {**payload, "replayed_at": self._now(), "last_error": ""}
        destination = self.replayed / path.name
        self._atomic_json(destination, payload)
        path.unlink()

    def save_intent(self, intent_id: str, rows: List[Dict[str, Any]], pending: List[str]) -> None:
        self._atomic_json(self.intents / f"{intent_id}.json", {
            "intent_id": intent_id, "rows": rows, "pending": pending,
            "callback_attempts": 0, "last_callback_error": "",
        })

    def complete_chunk(self, intent_id: str, batch_id: str) -> Optional[Dict[str, Any]]:
        path = self.intents / f"{intent_id}.json"
        if not path.exists():
            return None
        intent = self.read(path)
        intent["pending"] = [value for value in intent["pending"] if value != batch_id]
        self._atomic_json(path, intent)
        return intent if not intent["pending"] else None

    def callback_result(self, intent: Dict[str, Any], error: str = "") -> None:
        path = self.intents / f"{intent['intent_id']}.json"
        if error:
            intent["callback_attempts"] = int(intent.get("callback_attempts", 0)) + 1
            intent["last_callback_error"] = error[:4000]
            self._atomic_json(path, intent)
        elif path.exists():
            path.unlink()

    @staticmethod
    def _bounded_files(folder: Path, limit: int) -> tuple[List[Path], bool]:
        files = []
        with os.scandir(folder) as entries:
            for entry in entries:
                if entry.is_file() and entry.name.endswith(".json"):
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

    def write(self, dataset: str, rows: List[Dict[str, Any]]) -> Dict[str, int]:
        if dataset not in DATASET_COLUMNS:
            raise ValueError("dataset must be raw or canonical")
        outcome = {"rows": len(rows), "written": 0, "spooled": 0, "batches": 0}
        normalized_rows = [normalize_bar(dataset, row) for row in rows]
        failed_batches = []
        intent_id = stable_batch_id(dataset, normalized_rows)
        for chunk in self._chunks(normalized_rows, self.batch_size):
            batch_id = stable_batch_id(dataset, chunk)
            outcome["batches"] += 1
            try:
                outcome["written"] += self._insert(dataset, chunk, batch_id)
            except Exception as error:
                self.spool.enqueue(dataset, batch_id, chunk, str(error))
                failed_batches.append(batch_id)
                outcome["spooled"] += len(chunk)
        if dataset == "raw" and failed_batches:
            self.spool.save_intent(intent_id, normalized_rows, failed_batches)
            for batch_id in failed_batches:
                path = self.spool.pending / f"{batch_id}.json"
                payload = self.spool.read(path)
                self.spool._atomic_json(path, {**payload, "intent_id": intent_id})
        return outcome

    def replay(self, limit: int = 100) -> Dict[str, int]:
        if not 1 <= limit <= 1000:
            raise ValueError("replay limit must be between 1 and 1000")
        paths, _ = self.spool._bounded_files(self.spool.pending, limit)
        outcome = {"attempted": 0, "replayed": 0, "failed": 0}
        for path in paths:
            payload = self.spool.read(path)
            outcome["attempted"] += 1
            try:
                self._insert(payload["dataset"], payload["rows"], payload["batch_id"])
            except Exception as error:
                self.spool.enqueue(
                    payload["dataset"], payload["batch_id"], payload["rows"], str(error)
                )
                outcome["failed"] += 1
            else:
                self.spool.mark_replayed(path, payload)
                outcome["replayed"] += 1
                intent = (self.spool.complete_chunk(payload["intent_id"], payload["batch_id"])
                          if payload["dataset"] == "raw" and payload.get("intent_id") else None)
                if intent and self.on_raw_replayed:
                    try:
                        self.on_raw_replayed(intent["rows"])
                    except Exception as error:
                        self.spool.callback_result(intent, str(error))
                        self.last_replay_callback = {"status": "error", "error": str(error)[:4000]}
                    else:
                        self.spool.callback_result(intent)
                        self.last_replay_callback = {"status": "ok", "rows": len(intent["rows"])}
        return outcome
