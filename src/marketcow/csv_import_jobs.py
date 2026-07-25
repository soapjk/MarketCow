from __future__ import annotations

import threading
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from typing import Any, Callable


TERMINAL_CSV_IMPORT_JOBS = {"succeeded", "failed", "canceled"}
TERMINAL_CSV_IMPORT_SHARDS = {"succeeded", "failed", "canceled"}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds")


class CsvImportJobManager:
    """Durable, fenced coordinator for local CSV import shards."""

    def __init__(
        self,
        repository: Any,
        import_shard: Callable[[dict[str, Any], dict[str, Any]], dict[str, Any]],
        *,
        max_workers: int = 2,
        lease_seconds: float = 30,
    ):
        self.repository = repository
        self.import_shard = import_shard
        self.max_workers = max(1, min(int(max_workers), 16))
        self.lease_seconds = max(1.0, float(lease_seconds))
        self.owner_id = uuid.uuid4().hex
        self._executor = ThreadPoolExecutor(
            max_workers=self.max_workers, thread_name_prefix="csv-import"
        )
        self._guard = threading.Lock()
        self._active: set[str] = set()
        self._closed = False
        for job in repository.list_recoverable_csv_import_jobs():
            self._schedule(str(job["job_id"]))

    def create(
        self,
        *,
        idempotency_key: str,
        manifest_id: str,
        request_json: dict[str, Any],
        storage_path: str,
        raw_artifact_id: str,
        rows_total: int,
        shards: list[dict[str, Any]],
    ) -> tuple[dict[str, Any], bool]:
        if not idempotency_key.strip():
            raise ValueError("idempotency_key is required")
        now = _now()
        job_id = uuid.uuid4().hex
        job = {
            "job_id": job_id, "idempotency_key": idempotency_key.strip(),
            "manifest_id": manifest_id, "status": "queued",
            "request_json": dict(request_json), "storage_path": storage_path,
            "raw_artifact_id": raw_artifact_id, "rows_total": rows_total,
            "rows_read": 0, "rows_written": 0, "error_code": None,
            "error_message": None, "created_at": now, "started_at": None,
            "updated_at": now, "finished_at": None,
        }
        durable_shards = [{
            **shard, "job_id": job_id, "status": "queued", "attempt": 0,
            "rows_read": 0, "rows_written": 0, "write_receipt_json": None,
            "error_code": None, "error_message": None, "created_at": now,
            "updated_at": now, "finished_at": None,
        } for shard in shards]
        saved, created = self.repository.get_or_create_csv_import_job(
            job, durable_shards
        )
        if saved["manifest_id"] != manifest_id:
            raise ValueError("idempotency key conflicts with another CSV manifest")
        if created or saved["status"] not in TERMINAL_CSV_IMPORT_JOBS:
            self._schedule(str(saved["job_id"]))
        return saved, created

    def get(self, job_id: str) -> dict[str, Any] | None:
        job = self.repository.get_csv_import_job(job_id)
        if job is None:
            return None
        shards = self.repository.list_csv_import_shards(job_id)
        return {**job, "shards": shards}

    def cancel(self, job_id: str) -> dict[str, Any] | None:
        return self.repository.request_cancel_csv_import_job(job_id, _now())

    def _schedule(self, job_id: str) -> None:
        with self._guard:
            if self._closed or job_id in self._active:
                return
            self._active.add(job_id)
        self._executor.submit(self._run_and_release, job_id)

    def _run_and_release(self, job_id: str) -> None:
        try:
            self._run_job(job_id)
        finally:
            with self._guard:
                self._active.discard(job_id)

    def _run_job(self, job_id: str) -> None:
        job = self.repository.get_csv_import_job(job_id)
        if job is None or job["status"] in TERMINAL_CSV_IMPORT_JOBS:
            return
        now = _now()
        self.repository.update_csv_import_job({
            **job, "status": "running", "started_at": job.get("started_at") or now,
            "updated_at": now, "finished_at": None,
        })
        while True:
            current = self.repository.get_csv_import_job(job_id)
            if current is None or current["status"] == "cancel_requested":
                self.repository.cancel_unclaimed_csv_import_shards(job_id, _now())
            shards = self.repository.list_csv_import_shards(job_id)
            pending = [
                shard for shard in shards
                if shard["status"] not in TERMINAL_CSV_IMPORT_SHARDS
            ]
            if not pending or (
                current is not None and current["status"] == "cancel_requested"
            ):
                break
            with ThreadPoolExecutor(max_workers=self.max_workers) as pool:
                futures = [
                    pool.submit(self._run_shard, job_id, shard)
                    for shard in pending
                ]
                for future in as_completed(futures):
                    future.result()
        self._finalize(job_id)

    def _run_shard(self, job_id: str, shard: dict[str, Any]) -> None:
        job = self.repository.get_csv_import_job(job_id)
        if job is None:
            return
        if job["status"] == "cancel_requested":
            self.repository.cancel_unclaimed_csv_import_shards(job_id, _now())
            return
        token = uuid.uuid4().hex
        now = _now()
        expires = (
            datetime.now(timezone.utc) + timedelta(seconds=self.lease_seconds)
        ).isoformat(timespec="microseconds")
        claimed = self.repository.claim_csv_import_shard(
            job_id, int(shard["shard_index"]), self.owner_id, token, now, expires
        )
        if claimed is None:
            return
        stop = threading.Event()
        heartbeat = threading.Thread(
            target=self._heartbeat,
            args=(job_id, int(shard["shard_index"]), token, stop),
            daemon=True,
        )
        heartbeat.start()
        try:
            receipt = self.import_shard(job, claimed)
            finished = _now()
            self.repository.finish_claimed_csv_import_shard({
                **claimed, "status": "succeeded",
                "rows_read": int(receipt["rows_read"]),
                "rows_written": int(receipt["rows_written"]),
                "write_receipt_json": receipt, "error_code": None,
                "error_message": None, "updated_at": finished,
                "finished_at": finished,
            }, self.owner_id, token)
        except Exception as exc:
            finished = _now()
            max_attempts = int(job["request_json"].get("max_attempts", 3))
            status = "retry" if int(claimed["attempt"]) < max_attempts else "failed"
            self.repository.finish_claimed_csv_import_shard({
                **claimed, "status": status, "rows_read": 0,
                "rows_written": 0, "write_receipt_json": None,
                "error_code": type(exc).__name__.lower(),
                "error_message": str(exc)[:500], "updated_at": finished,
                "finished_at": finished if status == "failed" else None,
            }, self.owner_id, token)
        finally:
            stop.set()
            heartbeat.join(timeout=max(1.0, self.lease_seconds))

    def _heartbeat(
        self, job_id: str, shard_index: int, token: str, stop: threading.Event
    ) -> None:
        interval = max(0.5, self.lease_seconds / 3)
        while not stop.wait(interval):
            now = _now()
            expires = (
                datetime.now(timezone.utc) + timedelta(seconds=self.lease_seconds)
            ).isoformat(timespec="microseconds")
            renewed = self.repository.renew_csv_import_shard_lease(
                job_id, shard_index, self.owner_id, token, now, expires
            )
            if renewed is None:
                return

    def _finalize(self, job_id: str) -> None:
        job = self.repository.get_csv_import_job(job_id)
        if job is None:
            return
        if job["status"] == "cancel_requested":
            self.repository.cancel_unclaimed_csv_import_shards(job_id, _now())
        shards = self.repository.list_csv_import_shards(job_id)
        rows_read = sum(int(row["rows_read"]) for row in shards)
        rows_written = sum(int(row["rows_written"]) for row in shards)
        statuses = {str(row["status"]) for row in shards}
        if statuses <= {"succeeded"}:
            status, code = "succeeded", None
        elif job["status"] == "cancel_requested" and not statuses.intersection(
            {"queued", "running", "retry"}
        ):
            status, code = "canceled", None
        elif statuses.intersection({"queued", "running", "retry"}):
            status, code = "queued", None
        else:
            status, code = "failed", "csv_import_shard_failed"
        now = _now()
        self.repository.update_csv_import_job({
            **job, "status": status, "rows_read": rows_read,
            "rows_written": rows_written, "error_code": code,
            "error_message": None, "updated_at": now,
            "finished_at": now if status in TERMINAL_CSV_IMPORT_JOBS else None,
        })

    def close(self) -> None:
        with self._guard:
            self._closed = True
        self._executor.shutdown(wait=True)
