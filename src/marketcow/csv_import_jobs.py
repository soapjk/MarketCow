from __future__ import annotations

import inspect
import re
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from typing import Any, Callable


TERMINAL_CSV_IMPORT_JOBS = {"succeeded", "failed", "canceled"}
TERMINAL_CSV_IMPORT_SHARDS = {"succeeded", "failed", "canceled"}
_ABSOLUTE_PATH = re.compile(r"(?:[A-Za-z]:[\\/]|/)[^\s\"']+")


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds")


def _safe_error_message(value: Any) -> str:
    return _ABSOLUTE_PATH.sub("<redacted-path>", str(value))[:500]


class CsvImportJobManager:
    """Durable, fenced coordinator for local CSV import shards."""

    def __init__(
        self,
        repository: Any,
        import_shard: Callable[..., dict[str, Any]],
        finalize_job: Callable[[dict[str, Any]], dict[str, Any]] | None = None,
        *,
        max_workers: int = 2,
        lease_seconds: float = 30,
        progress_interval_seconds: float = 1,
    ):
        self.repository = repository
        self.import_shard = import_shard
        try:
            parameters = tuple(
                inspect.signature(import_shard).parameters.values()
            )
        except (TypeError, ValueError):
            parameters = ()
        self._importer_accepts_progress = (
            any(value.kind == inspect.Parameter.VAR_POSITIONAL for value in parameters)
            or sum(
                value.kind in {
                    inspect.Parameter.POSITIONAL_ONLY,
                    inspect.Parameter.POSITIONAL_OR_KEYWORD,
                }
                for value in parameters
            ) >= 3
        )
        self.finalize_job = finalize_job
        self.max_workers = max(1, min(int(max_workers), 16))
        self.lease_seconds = max(1.0, float(lease_seconds))
        self.progress_interval_seconds = max(
            0.1, float(progress_interval_seconds)
        )
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
            "error_message": None, "quality_report_json": None,
            "created_at": now, "started_at": None,
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
        rows_read = sum(int(row.get("rows_read") or 0) for row in shards)
        rows_written = sum(int(row.get("rows_written") or 0) for row in shards)
        completed = sum(
            1 for row in shards if row["status"] in TERMINAL_CSV_IMPORT_SHARDS
        )
        all_shards_succeeded = bool(shards) and all(
            row["status"] == "succeeded" for row in shards
        )
        status = str(job["status"])
        phase = (
            "completed" if status == "succeeded"
            else "failed" if status == "failed"
            else "canceled" if status == "canceled"
            else "canceling" if status == "cancel_requested"
            else "verifying" if all_shards_succeeded
            else "importing" if status == "running"
            else "queued"
        )
        updated_at = max(
            [str(job["updated_at"])]
            + [str(row["updated_at"]) for row in shards if row.get("updated_at")]
        )
        heartbeat_at = max(
            (
                str(row["heartbeat_at"])
                for row in shards if row.get("heartbeat_at")
            ),
            default=None,
        )
        if status == "succeeded":
            progress_percent = 100
        elif int(job.get("rows_total") or 0) > 0:
            progress_percent = min(
                99,
                round(
                    100
                    * min(rows_written, int(job["rows_total"]))
                    / int(job["rows_total"]),
                    2,
                ),
            )
        else:
            progress_percent = 0
        return {
            **job,
            "rows_read": max(int(job.get("rows_read") or 0), rows_read),
            "rows_written": max(int(job.get("rows_written") or 0), rows_written),
            "completed_shards": completed,
            "total_shards": len(shards),
            "progress_percent": progress_percent,
            "phase": phase,
            "updated_at": updated_at,
            "heartbeat_at": heartbeat_at,
            "shards": shards,
        }

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
            last_checkpoint_at = 0.0

            def checkpoint(rows_read: int, rows_written: int) -> None:
                nonlocal last_checkpoint_at
                observed = time.monotonic()
                shard_rows = int(claimed["row_end"]) - int(claimed["row_start"])
                if (
                    rows_read < shard_rows
                    and observed - last_checkpoint_at
                    < self.progress_interval_seconds
                ):
                    return
                saved = self.repository.checkpoint_csv_import_shard(
                    job_id,
                    int(shard["shard_index"]),
                    self.owner_id,
                    token,
                    int(rows_read),
                    int(rows_written),
                    _now(),
                )
                if saved is None:
                    raise RuntimeError("CSV import shard lease was lost")
                last_checkpoint_at = observed

            receipt = (
                self.import_shard(job, claimed, checkpoint)
                if self._importer_accepts_progress
                else self.import_shard(job, claimed)
            )
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
            current = next(
                (
                    row for row in self.repository.list_csv_import_shards(job_id)
                    if int(row["shard_index"]) == int(shard["shard_index"])
                ),
                claimed,
            )
            canceled = type(exc).__name__ == "CsvImportCanceled"
            max_attempts = int(job["request_json"].get("max_attempts", 3))
            status = (
                "canceled" if canceled
                else "retry" if int(claimed["attempt"]) < max_attempts
                else "failed"
            )
            self.repository.finish_claimed_csv_import_shard({
                **current, "status": status,
                "rows_read": int(current.get("rows_read") or 0),
                "rows_written": int(current.get("rows_written") or 0),
                "write_receipt_json": None,
                "error_code": None if canceled else type(exc).__name__.lower(),
                "error_message": _safe_error_message(exc), "updated_at": finished,
                "finished_at": (
                    finished if status in {"failed", "canceled"} else None
                ),
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
        quality_report = None
        if statuses <= {"succeeded"}:
            candidate = {
                **job, "status": "succeeded", "rows_read": rows_read,
                "rows_written": rows_written, "shards": shards,
            }
            try:
                quality_report = (
                    self.finalize_job(candidate)
                    if self.finalize_job is not None else None
                )
                if quality_report is not None and quality_report.get(
                    "status"
                ) != "passed":
                    raise RuntimeError("CSV import quality gate failed")
                status, code = "succeeded", None
            except Exception as exc:
                status, code = "failed", "csv_import_quality_failed"
                job = {**job, "error_message": _safe_error_message(exc)}
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
            "error_message": job.get("error_message"), "updated_at": now,
            "quality_report_json": quality_report,
            "finished_at": now if status in TERMINAL_CSV_IMPORT_JOBS else None,
        })

    def close(self) -> None:
        with self._guard:
            self._closed = True
        self._executor.shutdown(wait=True)
