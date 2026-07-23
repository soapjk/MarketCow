from __future__ import annotations

import queue
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class QuotePersistenceSnapshot:
    queued: int
    completed: int
    failed: int
    rejected: int
    pending: int


class AsyncQuotePersistence:
    """Bounded single-worker queue kept off the real-time response path."""

    def __init__(self, *, capacity: int = 256, thread_factory: Callable[..., Any] = threading.Thread):
        if capacity < 1:
            raise ValueError("quote persistence capacity must be positive")
        self._queue: queue.Queue[Callable[[], None] | None] = queue.Queue(capacity)
        self._lock = threading.Lock()
        self._accepting = True
        self._stop_sent = False
        self._queued = 0
        self._completed = 0
        self._failed = 0
        self._rejected = 0
        self._thread = thread_factory(
            target=self._run, name="marketcow-quote-persistence", daemon=True
        )
        self._thread.start()

    def submit(self, operation: Callable[[], None]) -> bool:
        with self._lock:
            if not self._accepting:
                self._rejected += 1
                return False
            try:
                self._queue.put_nowait(operation)
            except queue.Full:
                self._rejected += 1
                return False
            self._queued += 1
            return True

    def snapshot(self) -> QuotePersistenceSnapshot:
        with self._lock:
            return QuotePersistenceSnapshot(
                queued=self._queued,
                completed=self._completed,
                failed=self._failed,
                rejected=self._rejected,
                pending=self._queue.unfinished_tasks,
            )

    def close(self, timeout: float = 5.0) -> bool:
        with self._lock:
            if self._stop_sent:
                return not self._thread.is_alive()
            self._accepting = False
        deadline = time.monotonic() + max(0.0, timeout)
        while self._queue.unfinished_tasks and time.monotonic() < deadline:
            time.sleep(0.005)
        if self._queue.unfinished_tasks:
            return False
        self._queue.put_nowait(None)
        with self._lock:
            self._stop_sent = True
        self._thread.join(max(0.0, deadline - time.monotonic()))
        return not self._thread.is_alive()

    def _run(self) -> None:
        while True:
            operation = self._queue.get()
            try:
                if operation is None:
                    return
                operation()
            except Exception:
                with self._lock:
                    self._failed += 1
            else:
                if operation is not None:
                    with self._lock:
                        self._completed += 1
            finally:
                self._queue.task_done()
