from __future__ import annotations

import hashlib
import json
import re
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any, Dict


TERMINAL_JOB = {"succeeded", "partially_failed", "failed", "canceled"}
TERMINAL_ITEM = {"succeeded", "failed", "canceled"}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _safe_error(exc: BaseException) -> tuple[str, str]:
    text = str(exc)
    text = re.sub(
        r"(?i)(password|token|secret|authorization)\s*[=:]\s*\S+",
        r"\1=[redacted]", text,
    )
    return type(exc).__name__.lower(), text[:1000]


def _clean(value: Any) -> Any:
    if isinstance(value, bytes):
        return value.decode("utf-8")
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(_clean(key)): _clean(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_clean(item) for item in value]
    return value


class HistoryJobManager:
    """Durable bounded coordinator for explicit market-history requests."""

    def __init__(self, service: Any, repository: Any, max_workers: int = 4) -> None:
        self.service = service
        self.repository = repository
        self.max_workers = max(1, min(int(max_workers), 16))
        self.executor = ThreadPoolExecutor(
            max_workers=self.max_workers, thread_name_prefix="history-job"
        )
        self._slots = threading.BoundedSemaphore(self.max_workers)
        self._guard = threading.Lock()
        self._active: set[str] = set()
        self._recover()

    @staticmethod
    def identity(request: Dict[str, Any]) -> str:
        payload = json.dumps(request, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(payload.encode()).hexdigest()

    def _recover(self) -> None:
        for job in map(_clean, self.repository.list_history_jobs(200)):
            job_id = str(job["job_id"])
            items = list(map(_clean, self.repository.list_history_items(job_id)))
            if str(job["status"]) in TERMINAL_JOB:
                unfinished = [
                    item for item in items if item["status"] not in TERMINAL_ITEM
                ]
                for raw in unfinished:
                    item = dict(raw)
                    item.update({
                        "status": "canceled" if job["status"] == "canceled" else "failed",
                        "canonical_status": "failed",
                        "error_code": "service_interrupted",
                        "error_message": "terminal job contained unfinished item",
                        "updated_at": _now(), "finished_at": _now(),
                    })
                    self.repository.upsert_history_item(item)
                if unfinished:
                    self._finalize(job_id)
                continue
            interrupted = False
            for raw in items:
                item = dict(raw)
                if item["status"] == "running":
                    interrupted = True
                    item.update({
                        "status": "failed", "canonical_status": "failed",
                        "error_code": "service_interrupted",
                        "error_message": "service stopped while item was running",
                        "updated_at": _now(), "finished_at": _now(),
                    })
                    self.repository.upsert_history_item(item)
            if interrupted:
                self._finalize(job_id)
            else:
                self._dispatch(job_id)

    def create(self, request: Dict[str, Any]) -> tuple[Dict[str, Any], bool]:
        key = str(request["idempotency_key"])
        for existing in map(_clean, self.repository.list_history_jobs(200)):
            if str(existing["idempotency_key"]) == key:
                return self.detail(str(existing["job_id"])), False
        now = _now()
        job_id = uuid.uuid4().hex
        job = {
            "job_id": job_id, "idempotency_key": key, "status": "queued",
            "request_json": request, "created_at": now, "started_at": None,
            "updated_at": now, "finished_at": None, "error_json": None,
        }
        self.repository.upsert_history_job(job)
        for index, symbol in enumerate(request["symbols"]):
            item_id = hashlib.sha256(
                f"{job_id}|{symbol}".encode()
            ).hexdigest()[:24]
            self.repository.upsert_history_item({
                "job_id": job_id, "item_id": item_id, "symbol": symbol,
                "status": "queued", "provider": request["provider"], "source": None,
                "attempt": 0, "rows_fetched": 0, "rows_persisted": 0,
                "canonical_status": "pending", "error_code": None,
                "error_message": None, "started_at": None, "updated_at": now,
                "finished_at": None, "_index": index,
            })
        # Let independent PostgreSQL transactions for every item become visible
        # before the coordinator reads the batch through another pooled connection.
        threading.Timer(0.05, self._dispatch, args=(job_id,)).start()
        return self.detail(job_id), True

    def _dispatch(self, job_id: str) -> None:
        with self._guard:
            if job_id in self._active:
                return
            self._active.add(job_id)
        threading.Thread(
            target=self._run_job, args=(job_id,), daemon=True,
            name=f"history-batch-{job_id[:8]}",
        ).start()

    def _run_job(self, job_id: str) -> None:
        try:
            job = dict(_clean(self.repository.get_history_job(job_id)))
            request = dict(job["request_json"])
            now = _now()
            job.update({"status": "running", "started_at": job.get("started_at") or now,
                        "updated_at": now})
            self.repository.upsert_history_job(job)
            items = [
                dict(_clean(item))
                for item in self.repository.list_history_items(job_id)
            ]
            queued = [item for item in items if item["status"] == "queued"]
            concurrency = min(int(request["max_concurrency"]), self.max_workers)
            with ThreadPoolExecutor(max_workers=concurrency) as pool:
                futures = {
                    pool.submit(self._run_item_bounded, job_id, item, request): item
                    for item in queued
                }
                for future in as_completed(futures):
                    future.result()
            self._finalize(job_id)
        finally:
            with self._guard:
                self._active.discard(job_id)

    def _run_item_bounded(
        self, job_id: str, item: Dict[str, Any], request: Dict[str, Any]
    ) -> None:
        with self._slots:
            self._run_item(job_id, item, request)

    def _run_item(
        self, job_id: str, item: Dict[str, Any], request: Dict[str, Any]
    ) -> None:
        job = _clean(self.repository.get_history_job(job_id))
        if job and job["status"] == "cancel_requested":
            self._cancel_item(item)
            return
        item.update({
            "status": "running", "attempt": int(item["attempt"]) + 1,
            "started_at": item.get("started_at") or _now(), "updated_at": _now(),
            "error_code": None, "error_message": None,
        })
        self.repository.upsert_history_item(item)
        attempts = int(request["max_attempts"])
        while True:
            try:
                result = self.service.refresh_quote_history(
                    item["symbol"], request["range"], request["interval"],
                    request["adjustment"], provider=request["provider"],
                    allow_fallback=bool(request["allow_fallback"]),
                )
                fetched = len(result.get("bars") or [])
                persisted = int(result.get("count") or 0)
                canonical_status = self._canonical_status(
                    item["symbol"], request, result
                )
                current = _clean(self.repository.get_history_job(job_id))
                if current and current["status"] == "cancel_requested":
                    item.update({
                        "status": "canceled",
                        "canonical_status": canonical_status,
                    })
                else:
                    item.update({
                        "status": "succeeded",
                        "canonical_status": canonical_status,
                    })
                item.update({
                    "source": result.get("source"), "rows_fetched": fetched,
                    "rows_persisted": persisted, "updated_at": _now(),
                    "finished_at": _now(),
                })
                self.repository.upsert_history_item(item)
                return
            except Exception as exc:
                if int(item["attempt"]) >= attempts:
                    code, message = _safe_error(exc)
                    item.update({
                        "status": "failed", "canonical_status": "failed",
                        "error_code": code, "error_message": message,
                        "updated_at": _now(), "finished_at": _now(),
                    })
                    self.repository.upsert_history_item(item)
                    return
                time.sleep(
                    float(request["retry_backoff_seconds"])
                    * (2 ** (int(item["attempt"]) - 1))
                )
                item["attempt"] = int(item["attempt"]) + 1
                item["updated_at"] = _now()
                self.repository.upsert_history_item(item)

    def _canonical_status(
        self, symbol: str, request: Dict[str, Any], result: Dict[str, Any]
    ) -> str:
        bars = result.get("bars") or []
        repository = getattr(self.service, "market_bar_repository", None)
        identity_method = getattr(repository, "get_canonical_dataset_identity", None)
        if not bars or not callable(identity_method):
            return "completed"
        positions = []
        for bar in bars:
            position = bar.get("bar_at")
            if not position and bar.get("timestamp") is not None:
                position = datetime.fromtimestamp(
                    int(bar["timestamp"]), timezone.utc
                ).isoformat()
            positions.append(str(position or ""))
        if not all(positions):
            return "failed"
        deadline = time.monotonic() + float(request["canonical_wait_seconds"])
        while True:
            try:
                identity = identity_method(
                    symbol, request["interval"], request["adjustment"],
                    min(positions), max(positions),
                )
                if int(identity.get("row_count") or 0) >= len(set(positions)):
                    return "completed"
            except Exception:
                return "failed"
            if time.monotonic() >= deadline:
                return "pending"
            time.sleep(0.1)

    def _cancel_item(self, item: Dict[str, Any]) -> None:
        item.update({
            "status": "canceled", "canonical_status": "pending",
            "updated_at": _now(), "finished_at": _now(),
        })
        self.repository.upsert_history_item(item)

    def _finalize(self, job_id: str) -> None:
        job = dict(_clean(self.repository.get_history_job(job_id)))
        items = [
            dict(_clean(value)) for value in self.repository.list_history_items(job_id)
        ]
        succeeded = sum(item["status"] == "succeeded" for item in items)
        failed = sum(item["status"] == "failed" for item in items)
        canceled = sum(item["status"] == "canceled" for item in items)
        if canceled and not succeeded and not failed:
            status = "canceled"
        elif failed and succeeded:
            status = "partially_failed"
        elif failed:
            status = "failed"
        elif succeeded == len(items):
            status = "succeeded"
        else:
            status = "canceled"
        job.update({"status": status, "updated_at": _now(), "finished_at": _now()})
        self.repository.upsert_history_job(job)

    def cancel(self, job_id: str) -> Dict[str, Any]:
        job = _clean(self.repository.get_history_job(job_id))
        if not job:
            raise KeyError(job_id)
        job = dict(job)
        if job["status"] not in TERMINAL_JOB:
            job.update({"status": "cancel_requested", "updated_at": _now()})
            self.repository.upsert_history_job(job)
            for raw in map(_clean, self.repository.list_history_items(job_id)):
                if raw["status"] == "queued":
                    self._cancel_item(dict(raw))
        return self.detail(job_id)

    def retry_failed(self, job_id: str) -> Dict[str, Any]:
        job = _clean(self.repository.get_history_job(job_id))
        if not job:
            raise KeyError(job_id)
        changed = False
        for raw in map(_clean, self.repository.list_history_items(job_id)):
            item = dict(raw)
            if item["status"] == "failed":
                changed = True
                item.update({
                    "status": "queued", "canonical_status": "pending",
                    "error_code": None, "error_message": None,
                    "updated_at": _now(), "finished_at": None,
                })
                self.repository.upsert_history_item(item)
        if not changed:
            raise ValueError("job has no failed items")
        job = dict(job)
        job.update({"status": "queued", "updated_at": _now(), "finished_at": None})
        self.repository.upsert_history_job(job)
        self._dispatch(job_id)
        return self.detail(job_id)

    def detail(self, job_id: str) -> Dict[str, Any]:
        job = _clean(self.repository.get_history_job(job_id))
        if not job:
            raise KeyError(job_id)
        items = [
            dict(_clean(value)) for value in self.repository.list_history_items(job_id)
        ]
        counts = {
            key: sum(item["status"] == key for item in items)
            for key in ("queued", "running", "succeeded", "failed", "canceled")
        }
        completed = counts["succeeded"] + counts["failed"] + counts["canceled"]
        total = len(items)
        result = {
            **dict(job), "items": items, "total_symbols": total, **{
                f"{key}_symbols": value for key, value in counts.items()
            },
            "completed_symbols": completed,
            "progress_percent": round(100 * completed / total, 2) if total else 100.0,
            "rows_fetched": sum(int(item["rows_fetched"]) for item in items),
            "rows_persisted": sum(int(item["rows_persisted"]) for item in items),
            "canonical_pending": sum(
                item["canonical_status"] == "pending" for item in items
            ),
            "canonical_completed": sum(
                item["canonical_status"] == "completed" for item in items
            ),
            "canonical_failed": sum(
                item["canonical_status"] == "failed" for item in items
            ),
        }
        return result

    def list(self, limit: int = 50) -> list[Dict[str, Any]]:
        return [
            self.detail(str(job["job_id"]))
            for job in map(_clean, self.repository.list_history_jobs(limit))
        ]

    def close(self) -> None:
        self.executor.shutdown(wait=False, cancel_futures=True)
