from __future__ import annotations

import fcntl
import hashlib
import json
import os
import threading
from pathlib import Path
from typing import Any, Callable, Dict, List


class BackgroundCanonicalScheduler:
    """Single-process, durable and bounded development canonical scheduler."""

    def __init__(
        self, builder: Any, spool: Any, canonical_limit: int = 50000,
        queue_cap: int = 1000, scan_limit: int = 100, poll_seconds: float = 1.0,
        backoff_base_seconds: float = 1.0, backoff_max_seconds: float = 60.0,
        max_attempts: int = 10, clock: Callable[[], float] | None = None,
        start_paused: bool = False,
    ) -> None:
        if not 1 <= queue_cap <= 10000 or not 1 <= scan_limit <= 1000:
            raise ValueError("scheduler queue/scan limits are out of bounds")
        if not 0.05 <= poll_seconds <= 60:
            raise ValueError("scheduler poll seconds must be between 0.05 and 60")
        if not 0.01 <= backoff_base_seconds <= backoff_max_seconds <= 3600:
            raise ValueError("scheduler backoff bounds are invalid")
        if not 1 <= max_attempts <= 100:
            raise ValueError("scheduler max attempts must be between 1 and 100")
        self.builder = builder
        self.spool = spool
        self.canonical_limit = canonical_limit
        self.queue_cap = queue_cap
        self.scan_limit = scan_limit
        self.poll_seconds = poll_seconds
        self.backoff_base_seconds = backoff_base_seconds
        self.backoff_max_seconds = backoff_max_seconds
        self.max_attempts = max_attempts
        self.clock = clock or __import__("time").time
        self._queue_lock = threading.Lock()
        self._run_lock = threading.Lock()
        self.root = spool.root / "canonical-scheduler"
        self.pending = self.root / "pending"
        self.processing = self.root / "processing"
        self.failed = self.root / "failed"
        for path in (self.pending, self.processing, self.failed):
            path.mkdir(parents=True, exist_ok=True)
        self._lease = (self.root / ".lease").open("a+")
        try:
            fcntl.flock(self._lease.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            self._lease.close()
            raise RuntimeError("canonical scheduler lease is already held") from None
        try:
            from .spool_operator import SpoolOperator
            migration = SpoolOperator(spool).migrate_legacy(
                1000, kinds=("scheduler-pending", "scheduler-processing", "scheduler-failed")
            )
        except Exception:
            fcntl.flock(self._lease.fileno(), fcntl.LOCK_UN)
            self._lease.close()
            raise
        if not migration["remaining"] and not migration["errors"]:
            self._recover_processing()
        self._stop = threading.Event()
        self._wake = threading.Event()
        self._paused = threading.Event()
        if start_paused:
            self._paused.set()
        self._state_lock = threading.Lock()
        self._legacy_migration = migration
        self._last: Dict[str, Any] = {"status": "starting"}
        self._thread = threading.Thread(
            target=self._run, name="marketcow-canonical-scheduler", daemon=False
        )
        self._thread.start()

    @staticmethod
    def _groups(rows: List[Dict[str, Any]]) -> List[Dict[str, str]]:
        groups: Dict[tuple[str, str, str], List[str]] = {}
        for row in rows:
            key = (str(row["symbol"]), str(row["interval"]), str(row["adjustment"]))
            groups.setdefault(key, []).append(str(row["bar_time"]))
        return [
            {"symbol": key[0], "interval": key[1], "adjustment": key[2],
             "start": min(times), "end": max(times)}
            for key, times in sorted(groups.items())
        ]

    @staticmethod
    def _task_id(group: Dict[str, str]) -> str:
        encoded = json.dumps(group, sort_keys=True, separators=(",", ":")).encode()
        return hashlib.sha256(encoded).hexdigest()

    @staticmethod
    def _files(folder: Path, limit: int) -> List[Path]:
        paths = []
        with os.scandir(folder) as entries:
            for entry in entries:
                if entry.is_file(follow_symlinks=False) and entry.name.endswith(".json"):
                    paths.append(Path(entry.path))
                    if len(paths) >= limit:
                        break
        return sorted(paths, key=lambda path: path.name)

    def _recover_processing(self) -> None:
        for path in self._files(self.processing, self.scan_limit):
            destination = self.pending / path.name
            if destination.exists():
                path.unlink()
            else:
                os.replace(path, destination)

    def enqueue_rows(self, rows: List[Dict[str, Any]]) -> Dict[str, int | str]:
        groups = self._groups(rows)
        accepted = duplicate = full = 0
        with self._queue_lock:
            for group in groups:
                task_id = self._task_id(group)
                destinations = [folder / f"{task_id}.json"
                                for folder in (self.pending, self.processing)]
                if any(path.exists() for path in destinations):
                    duplicate += 1
                    continue
                backlog = len(self._files(self.pending, self.queue_cap + 1)) + len(
                    self._files(self.processing, self.queue_cap + 1)
                )
                if backlog >= self.queue_cap:
                    full += 1
                    continue
                now = float(self.clock())
                self.spool._atomic_json(self.pending / f"{task_id}.json", {
                    "task_id": task_id, **group, "attempts": 0,
                    "created_at_epoch": now, "next_attempt_epoch": now,
                    "last_error": "",
                })
                accepted += 1
        self._wake.set()
        return {"status": "ok" if not full else "full", "accepted": accepted,
                "duplicate": duplicate, "full": full}

    def enqueue_replayed_rows(self, rows: List[Dict[str, Any]]) -> None:
        """Keep the upstream raw replay intent durable until enqueue is durable."""
        outcome = self.enqueue_rows(rows)
        if outcome["full"]:
            raise RuntimeError("canonical scheduler queue is full")

    def _run_one(self, path: Path) -> None:
        claimed = self.processing / path.name
        try:
            os.replace(path, claimed)
        except FileNotFoundError:
            return
        task = self.spool.read(claimed, require_checksum=True)
        try:
            result = self.builder.rebuild(
                task["symbol"], task["interval"], task["adjustment"],
                task["start"], task["end"], self.canonical_limit,
            )
            if result.get("status") != "ok" or result.get("spooled"):
                raise RuntimeError("canonical rebuild did not complete")
        except Exception as error:
            task["attempts"] = int(task.get("attempts", 0)) + 1
            task["last_error"] = str(error)[:4000]
            delay = min(
                self.backoff_max_seconds,
                self.backoff_base_seconds * (2 ** (task["attempts"] - 1)),
            )
            task["next_attempt_epoch"] = float(self.clock()) + delay
            destination = (self.failed if task["attempts"] >= self.max_attempts
                           else self.pending) / claimed.name
            self.spool._atomic_json(destination, task)
            claimed.unlink(missing_ok=True)
            with self._state_lock:
                self._last = {"status": "failed", "task_id": task["task_id"],
                              "attempts": task["attempts"], "error": task["last_error"]}
            if self.spool.telemetry:
                self.spool.telemetry.safe(
                    "counter", "canonical_rebuild_total", outcome="error"
                )
        else:
            claimed.unlink(missing_ok=True)
            with self._state_lock:
                self._last = {"status": "ok", "task_id": task["task_id"],
                              "attempts": int(task.get("attempts", 0)) + 1}
            if self.spool.telemetry:
                self.spool.telemetry.safe(
                    "counter", "canonical_rebuild_total", outcome="ok"
                )

    def run_once(self) -> int:
        if self._paused.is_set() or self._stop.is_set():
            return 0
        if not self._run_lock.acquire(blocking=False):
            return 0
        try:
            with (self.spool.root / ".operator.lock").open("a+") as operator_lock:
                try:
                    fcntl.flock(operator_lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError:
                    return 0
                try:
                    from .spool_operator import SpoolOperator
                    migration = SpoolOperator(self.spool).migrate_legacy(
                        self.scan_limit, already_locked=True,
                        kinds=("scheduler-pending", "scheduler-processing", "scheduler-failed"),
                    )
                    with self._state_lock:
                        self._legacy_migration = migration
                    now = float(self.clock())
                    if not migration["remaining"] and not migration["errors"]:
                        self._recover_processing()
                    ready = []
                    for path in self._files(self.pending, self.scan_limit):
                        try:
                            task = self.spool.read(path, require_checksum=True)
                        except Exception as error:
                            with self._state_lock:
                                self._last = {"status": "invalid", "error": str(error)[:4000]}
                            continue
                        if float(task.get("next_attempt_epoch", 0)) <= now:
                            ready.append(path)
                    for path in ready:
                        if self._stop.is_set():
                            break
                        self._run_one(path)
                    return len(ready)
                finally:
                    fcntl.flock(operator_lock.fileno(), fcntl.LOCK_UN)
        finally:
            self._run_lock.release()

    def _run(self) -> None:
        with self._state_lock:
            self._last = {"status": "idle"}
        while not self._stop.is_set():
            self.run_once()
            self._wake.wait(self.poll_seconds)
            self._wake.clear()

    def pause(self) -> None:
        self._paused.set()

    def resume(self) -> None:
        self._paused.clear()
        self._wake.set()

    def diagnostics(self) -> Dict[str, Any]:
        pending = self._files(self.pending, self.queue_cap + 1)
        failed = self._files(self.failed, self.queue_cap + 1)
        oldest = 0.0
        invalid = 0
        if pending:
            created = []
            for path in pending[:self.scan_limit]:
                try:
                    payload = self.spool.read(path, require_checksum=True)
                    created.append(float(payload.get("created_at_epoch", self.clock())))
                except (OSError, ValueError, TypeError, json.JSONDecodeError):
                    invalid += 1
            oldest = max(0.0, float(self.clock()) - min(created)) if created else 0.0
        with self._state_lock:
            last = dict(self._last)
        if self.spool.telemetry:
            self.spool.telemetry.safe("gauge", "canonical_queue_items", len(pending),
                                      state="pending")
            self.spool.telemetry.safe("gauge", "canonical_queue_items", len(failed),
                                      state="failed")
            self.spool.telemetry.safe("histogram", "canonical_lag_seconds", oldest)
        return {
            "enabled": True, "paused": self._paused.is_set(),
            "thread_alive": self._thread.is_alive(), "pending": min(len(pending), self.queue_cap),
            "failed": min(len(failed), self.queue_cap), "backlog_truncated": (
                len(pending) > self.queue_cap or len(failed) > self.queue_cap
            ), "oldest_lag_seconds": round(oldest, 3), "last": last,
            "invalid": invalid,
            "legacy_migration": dict(self._legacy_migration),
        }

    def close(self, timeout: float = 35.0) -> None:
        self._stop.set()
        self._wake.set()
        self._thread.join(timeout)
        if self._thread.is_alive():
            raise RuntimeError("canonical scheduler did not stop")
        fcntl.flock(self._lease.fileno(), fcntl.LOCK_UN)
        self._lease.close()
