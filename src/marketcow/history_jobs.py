from __future__ import annotations

import hashlib
import json
import random
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, Optional

import requests

from .provider_routing import ProviderNotSupported, ProviderRoutingError
from .history_shards import (
    freeze_history_range,
    history_ingestion_identity,
    plan_history_shards,
)
from .history_canonical import HistoryCanonicalVerifier
from .telemetry import sanitize_text, telemetry_call


TERMINAL_JOB = {"succeeded", "partially_failed", "failed", "canceled"}
TERMINAL_ITEM = {"succeeded", "failed", "canceled"}
HISTORY_HEALTH_THRESHOLDS = {
    "queued_degraded": 100,
    "expired_lease_degraded": 1,
    "expired_lease_unavailable": 10,
    "canonical_pending_degraded": 100,
    "takeover_degraded": 20,
}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _safe_error(exc: BaseException) -> tuple[str, str]:
    return type(exc).__name__.lower(), sanitize_text(exc)


def _error_chain(exc: BaseException) -> list[BaseException]:
    values, seen = [], set()
    current: Optional[BaseException] = exc
    while current is not None and id(current) not in seen:
        values.append(current)
        seen.add(id(current))
        current = current.__cause__ or current.__context__
    return values


def _error_policy(exc: BaseException) -> tuple[str, bool]:
    chain = _error_chain(exc)
    for value in chain:
        if type(value).__name__ == "ClickHouseRepositoryError":
            return "storage_unavailable", True
        if (
            type(value).__module__.startswith("psycopg")
            and type(value).__name__ in {
                "OperationalError", "InterfaceError", "ConnectionTimeout",
            }
        ):
            return "storage_connection_error", True
        if isinstance(value, requests.Timeout):
            return "upstream_timeout", True
        if isinstance(value, requests.ConnectionError):
            return "upstream_connection_error", True
        if isinstance(value, requests.HTTPError):
            status = getattr(value.response, "status_code", None)
            if status == 429:
                return "upstream_rate_limited", True
            if status is not None and 500 <= int(status) <= 599:
                return "upstream_server_error", True
            if status in {401, 403}:
                return "provider_authentication_failed", False
            return "upstream_request_rejected", False
    if isinstance(exc, ProviderNotSupported):
        return exc.code, False
    if isinstance(exc, ProviderRoutingError):
        return exc.code, False
    if isinstance(exc, ValueError):
        return "invalid_history_request", False
    message = str(exc).lower()
    if any(value in message for value in (
        "token is not configured", "not configured", "authentication",
        "unauthorized", "forbidden",
    )):
        return "provider_authentication_failed", False
    if any(value in message for value in ("rate limit", "too many requests", "频率")):
        return "upstream_rate_limited", True
    return "history_fetch_failed", False


def _retry_after_seconds(exc: BaseException) -> Optional[float]:
    for value in _error_chain(exc):
        if not isinstance(value, requests.HTTPError) or value.response is None:
            continue
        raw = value.response.headers.get("Retry-After")
        if raw is None:
            continue
        try:
            return max(0.0, float(raw))
        except ValueError:
            return None
    return None


def _retry_delay(
    request: Dict[str, Any], attempt: int, exc: BaseException,
    random_value: float,
) -> float:
    base = float(request["retry_backoff_seconds"]) * (2 ** (attempt - 1))
    retry_after = _retry_after_seconds(exc) or 0.0
    jitter = max(0.0, min(1.0, random_value)) * float(
        request.get("retry_jitter_seconds", 0)
    )
    return min(
        float(request.get("retry_max_backoff_seconds", 600)),
        max(base, retry_after) + jitter,
    )


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

    def __init__(
        self, service: Any, repository: Any, max_workers: int = 4,
        lease_seconds: float = 30,
        random_provider: Callable[[], float] = random.random,
        monotonic: Callable[[], float] = time.monotonic,
        sleeper: Callable[[float], None] = time.sleep,
        clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
    ) -> None:
        self.service = service
        self.repository = repository
        self.max_workers = max(1, min(int(max_workers), 16))
        self.lease_seconds = max(0.2, float(lease_seconds))
        self.owner_id = uuid.uuid4().hex
        self.thread_name = f"history-{self.owner_id[:8]}"
        self._random = random_provider
        self._monotonic = monotonic
        self._sleep = sleeper
        self._clock = clock
        self.telemetry = getattr(service, "telemetry", None)
        self.executor = ThreadPoolExecutor(
            max_workers=self.max_workers,
            thread_name_prefix=f"{self.thread_name}-item",
        )
        self._coordinator = ThreadPoolExecutor(
            max_workers=self.max_workers,
            thread_name_prefix=f"{self.thread_name}-batch",
        )
        self._slots = threading.BoundedSemaphore(self.max_workers)
        self._guard = threading.Lock()
        self._active: set[str] = set()
        self._leases: dict[tuple[str, str], str] = {}
        self._lease_guard = threading.Lock()
        self._heartbeat_stop = threading.Event()
        self._heartbeat: Optional[threading.Thread] = None
        self._watchdog_stop = threading.Event()
        self._watchdog = threading.Thread(
            target=self._watchdog_loop,
            name=f"{self.thread_name}-watchdog",
            daemon=True,
        )
        self._canonical_verifier = (
            HistoryCanonicalVerifier(repository, service.market_bar_repository)
            if all(hasattr(repository, name) for name in (
                "upsert_history_canonical_check",
                "list_pending_history_canonical_checks",
                "list_history_canonical_checks",
            )) and getattr(service, "market_bar_repository", None) is not None
            else None
        )
        self._closed = False
        self._recover()
        self._watchdog.start()

    def _lease_deadline(self) -> str:
        return (
            datetime.now(timezone.utc) + timedelta(seconds=self.lease_seconds)
        ).isoformat(timespec="microseconds")

    @staticmethod
    def _lease_expired(item: Dict[str, Any]) -> bool:
        value = item.get("lease_expires_at")
        if not value:
            return True
        try:
            expires_at = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except ValueError:
            return True
        return expires_at <= datetime.now(timezone.utc)

    def _heartbeat_loop(self) -> None:
        interval = max(0.1, min(1.0, self.lease_seconds / 3))
        while not self._heartbeat_stop.wait(interval):
            with self._lease_guard:
                leases = list(self._leases.items())
                if not leases:
                    self._heartbeat = None
                    return
            for (job_id, item_id), token in leases:
                now = _now()
                try:
                    renewed = self.repository.renew_history_item_lease(
                        job_id, item_id, self.owner_id, token, now,
                        self._lease_deadline(),
                    )
                except Exception:
                    continue
                if renewed is None:
                    with self._lease_guard:
                        if self._leases.get((job_id, item_id)) == token:
                            self._leases.pop((job_id, item_id), None)
                    telemetry_call(
                        self.telemetry, "safe", "counter",
                        "history_lease_total", outcome="lost",
                    )
                else:
                    telemetry_call(
                        self.telemetry, "safe", "counter",
                        "history_lease_total", outcome="renewed",
                    )

    def _register_lease(self, item: Dict[str, Any], token: str) -> None:
        with self._lease_guard:
            self._leases[(item["job_id"], item["item_id"])] = token
            if self._heartbeat is None or not self._heartbeat.is_alive():
                self._heartbeat = threading.Thread(
                    target=self._heartbeat_loop,
                    name=f"{self.thread_name}-heartbeat",
                    daemon=False,
                )
                self._heartbeat.start()

    def _watchdog_loop(self) -> None:
        interval = max(0.2, min(5.0, self.lease_seconds / 2))
        while not self._watchdog_stop.wait(interval):
            try:
                jobs = list(map(
                    _clean, self.repository.list_recoverable_history_jobs()
                ))
                for job in jobs:
                    items = list(map(
                        _clean,
                        self.repository.list_history_items(str(job["job_id"])),
                    ))
                    if any(
                        item["status"] == "queued"
                        or (
                            item["status"] == "running"
                            and self._lease_expired(item)
                        )
                        for item in items
                    ):
                        self._dispatch(str(job["job_id"]))
                if self._canonical_verifier is not None:
                    self._canonical_verifier.run_pending(100)
            except Exception:
                continue

    def _finish_claim(
        self, item: Dict[str, Any], token: str
    ) -> Optional[Dict[str, Any]]:
        try:
            return self.repository.finish_claimed_history_item(
                item, self.owner_id, token
            )
        finally:
            with self._lease_guard:
                if self._leases.get((item["job_id"], item["item_id"])) == token:
                    self._leases.pop((item["job_id"], item["item_id"]), None)

    @staticmethod
    def identity(request: Dict[str, Any]) -> str:
        payload = json.dumps(request, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(payload.encode()).hexdigest()

    def _recover(self) -> None:
        jobs = self.repository.list_recoverable_history_jobs()
        for job in map(_clean, jobs):
            job_id = str(job["job_id"])
            items = list(map(_clean, self.repository.list_history_items(job_id)))
            redispatch = False
            for raw in items:
                item = dict(raw)
                if job["status"] == "cancel_requested" and item["status"] == "queued":
                    self._cancel_item(item)
                    continue
                if item["status"] == "running":
                    if not self._lease_expired(item):
                        continue
                    redispatch = True
                elif item["status"] == "queued":
                    redispatch = True
            if redispatch:
                self._dispatch(job_id)
            else:
                self._finalize(job_id)

    def create(self, request: Dict[str, Any]) -> tuple[Dict[str, Any], bool]:
        with self._guard:
            if self._closed:
                raise RuntimeError("history job manager is closed")
        request = freeze_history_range(request, self._clock())
        request["history_job_schema_version"] = 2
        request["shard_plan"] = plan_history_shards(request)
        now = _now()
        job_id = uuid.uuid4().hex
        job = {
            "job_id": job_id, "idempotency_key": str(request["idempotency_key"]),
            "status": "queued",
            "request_json": request, "created_at": now, "started_at": None,
            "updated_at": now, "finished_at": None, "error_json": None,
        }
        items = []
        shards = []
        for index, symbol in enumerate(request["symbols"]):
            item_id = hashlib.sha256(
                f"{job_id}|{symbol}".encode()
            ).hexdigest()[:24]
            items.append({
                "job_id": job_id, "item_id": item_id, "symbol": symbol,
                "status": "queued", "provider": request["provider"], "source": None,
                "attempt": 0, "rows_fetched": 0, "rows_persisted": 0,
                "canonical_status": "pending", "error_code": None,
                "error_message": None, "started_at": None, "updated_at": now,
                "finished_at": None, "_index": index,
            })
            for planned in request["shard_plan"]:
                shards.append({
                    **planned, "job_id": job_id, "item_id": item_id,
                    "ingestion_id": history_ingestion_identity(
                        symbol, request, planned
                    ),
                    "status": "queued", "attempt": 0, "rows_fetched": 0,
                    "rows_persisted": 0, "cursor_json": {},
                    "write_receipt_json": None, "error_code": None,
                    "error_message": None, "owner_id": None,
                    "lease_token": None, "lease_expires_at": None,
                    "heartbeat_at": None, "created_at": now, "updated_at": now,
                    "finished_at": None,
                })
        saved, created = self.repository.get_or_create_history_job(
            job, items, shards
        )
        saved = _clean(saved)
        if not created:
            return self.detail(str(saved["job_id"])), False
        self._dispatch(job_id)
        return self.detail(job_id), True

    def _dispatch(self, job_id: str) -> None:
        with self._guard:
            if self._closed or job_id in self._active:
                return
            self._active.add(job_id)
            self._coordinator.submit(self._run_job, job_id)

    def _run_job(self, job_id: str) -> None:
        try:
            job = dict(_clean(self.repository.get_history_job(job_id)))
            request = dict(job["request_json"])
            now = _now()
            job.update({
                "status": (
                    "cancel_requested"
                    if job["status"] == "cancel_requested" else "running"
                ),
                "started_at": job.get("started_at") or now,
                "updated_at": now,
            })
            self.repository.upsert_history_job(job)
            items = [
                dict(_clean(item))
                for item in self.repository.list_history_items(job_id)
            ]
            queued = [
                item for item in items
                if item["status"] == "queued"
                or (
                    item["status"] == "running"
                    and self._lease_expired(item)
                )
            ]
            concurrency = min(int(request["max_concurrency"]), self.max_workers)
            job_slots = threading.BoundedSemaphore(concurrency)
            futures = {}
            for item in queued:
                with self._guard:
                    if self._closed:
                        self._cancel_item(item)
                        continue
                    future = self.executor.submit(
                        self._run_item_bounded, job_id, item, request, job_slots
                    )
                futures[future] = item
            for future in as_completed(futures):
                future.result()
        finally:
            with self._guard:
                self._finalize(job_id)
                self._active.discard(job_id)

    def _run_item_bounded(
        self, job_id: str, item: Dict[str, Any], request: Dict[str, Any],
        job_slots: threading.BoundedSemaphore,
    ) -> None:
        with job_slots:
            with self._slots:
                self._run_item(job_id, item, request)

    def _run_item(
        self, job_id: str, item: Dict[str, Any], request: Dict[str, Any]
    ) -> None:
        was_interrupted = item["status"] == "running"
        now = _now()
        lease_token = uuid.uuid4().hex
        claimed = self.repository.claim_history_item(
            job_id, item["item_id"], self.owner_id, lease_token, now,
            self._lease_deadline(),
        )
        if claimed is None:
            return
        item = dict(_clean(claimed))
        self._register_lease(item, lease_token)
        telemetry_call(
            self.telemetry, "safe", "counter", "history_lease_total",
            outcome="taken_over" if was_interrupted else "claimed",
        )
        telemetry_call(
            self.telemetry, "safe", "log", "history",
            action="item_claimed", job_id=job_id, item_id=item["item_id"],
            owner_id=self.owner_id, takeover=was_interrupted,
        )
        job = _clean(self.repository.get_history_job(job_id))
        if job and job["status"] == "cancel_requested":
            item.update({
                "status": "canceled", "canonical_status": "pending",
                "updated_at": _now(), "finished_at": _now(),
            })
            self._finish_claim(item, lease_token)
            return
        attempts = int(request["max_attempts"])
        if was_interrupted and int(item["attempt"]) >= attempts:
            item.update({
                "status": "failed", "canonical_status": "failed",
                "error_code": "service_interrupted",
                "error_message": "lease expired after retry budget was exhausted",
                "updated_at": _now(), "finished_at": _now(),
            })
            self._finish_claim(item, lease_token)
            return
        item.update({
            "attempt": int(item["attempt"]) + 1,
            "started_at": item.get("started_at") or now, "updated_at": now,
            "error_code": None, "error_message": None,
        })
        self.repository.upsert_history_item(item)
        shards = list(map(
            _clean, self.repository.list_history_shards(job_id, item["item_id"])
        ))
        if shards:
            self._run_item_shards(
                job_id, item, request, shards, lease_token
            )
            return
        retry_deadline = (
            self._monotonic() + float(request.get("retry_budget_seconds", 86400))
        )
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
                self._finish_claim(item, lease_token)
                return
            except Exception as exc:
                code, retryable = _error_policy(exc)
                if not retryable or int(item["attempt"]) >= attempts:
                    _, message = _safe_error(exc)
                    item.update({
                        "status": "failed", "canonical_status": "failed",
                        "error_code": code, "error_message": message,
                        "updated_at": _now(), "finished_at": _now(),
                    })
                    self._finish_claim(item, lease_token)
                    return
                delay = _retry_delay(
                    request, int(item["attempt"]), exc, self._random()
                )
                if self._monotonic() + delay > retry_deadline:
                    _, message = _safe_error(exc)
                    item.update({
                        "status": "failed", "canonical_status": "failed",
                        "error_code": "retry_budget_exhausted",
                        "error_message": message, "updated_at": _now(),
                        "finished_at": _now(),
                    })
                    self._finish_claim(item, lease_token)
                    return
                self._sleep(delay)
                item["attempt"] = int(item["attempt"]) + 1
                item["updated_at"] = _now()
                self.repository.upsert_history_item(item)

    def _run_item_shards(
        self, job_id: str, item: Dict[str, Any], request: Dict[str, Any],
        shards: list[Dict[str, Any]], lease_token: str,
    ) -> None:
        total_fetched = 0
        total_persisted = 0
        canonical_statuses = []
        source = item.get("source")
        for raw in shards:
            shard = dict(raw)
            if shard["status"] == "succeeded":
                total_fetched += int(shard["rows_fetched"])
                total_persisted += int(shard["rows_persisted"])
                receipt = dict(shard.get("write_receipt_json") or {})
                canonical_statuses.append(
                    str(receipt.get("canonical_status") or "completed")
                )
                source = receipt.get("source") or source
                continue
            job = _clean(self.repository.get_history_job(job_id))
            if job and job["status"] == "cancel_requested":
                self._cancel_item_shards(job_id, item["item_id"])
                item.update({
                    "status": "canceled", "canonical_status": "pending",
                    "rows_fetched": total_fetched,
                    "rows_persisted": total_persisted,
                    "updated_at": _now(), "finished_at": _now(),
                })
                self._finish_claim(item, lease_token)
                return
            retry_deadline = (
                self._monotonic()
                + float(request.get("retry_budget_seconds", 86400))
            )
            while True:
                shard.update({
                    "status": "running",
                    "attempt": int(shard["attempt"]) + 1,
                    "error_code": None, "error_message": None,
                    "updated_at": _now(), "finished_at": None,
                    "owner_id": self.owner_id, "lease_token": lease_token,
                })
                self.repository.upsert_history_shard(shard)
                try:
                    shard_started = self._monotonic()
                    start = datetime.fromisoformat(
                        str(shard["range_start"]).replace("Z", "+00:00")
                    )
                    end = datetime.fromisoformat(
                        str(shard["range_end"]).replace("Z", "+00:00")
                    )
                    result = self.service.refresh_quote_history_window(
                        item["symbol"], start, end, request["interval"],
                        request["adjustment"], provider=request["provider"],
                        allow_fallback=bool(request["allow_fallback"]),
                        ingestion_id=shard.get("ingestion_id"),
                    )
                    fetched = len(result.get("bars") or [])
                    persisted = int(result.get("count") or 0)
                    canonical = self._canonical_status(
                        item["symbol"],
                        {**request, "canonical_wait_seconds": 0},
                        result,
                    )
                    shard.update({
                        "status": "succeeded", "rows_fetched": fetched,
                        "rows_persisted": persisted,
                        "cursor_json": {
                            "confirmed_through": shard["range_end"]
                        },
                        "write_receipt_json": {
                            "ingestion_id": shard.get("ingestion_id"),
                            "source": result.get("source"),
                            "raw_artifact_id": result.get("raw_artifact_id"),
                            "observed_at": result.get("observed_at"),
                            "canonical_status": canonical,
                        },
                        "owner_id": None, "lease_token": None,
                        "lease_expires_at": None, "heartbeat_at": None,
                        "updated_at": _now(), "finished_at": _now(),
                    })
                    self.repository.upsert_history_shard(shard)
                    telemetry_call(
                        self.telemetry, "safe", "histogram",
                        "history_shard_latency_seconds",
                        self._monotonic() - shard_started,
                        outcome="succeeded",
                    )
                    if (
                        canonical != "completed"
                        and self._canonical_verifier is not None
                    ):
                        self._canonical_verifier.enqueue({
                            "check_id": str(
                                shard.get("ingestion_id")
                                or f"{job_id}:{item['item_id']}:{shard['shard_key']}"
                            ),
                            "job_id": job_id, "item_id": item["item_id"],
                            "shard_key": shard["shard_key"],
                            "symbol": item["symbol"],
                            "interval": request["interval"],
                            "adjustment": request["adjustment"],
                            "range_start": shard["range_start"],
                            "range_end": shard["range_end"],
                            "expected_rows": len({
                                str(
                                    bar.get("bar_at")
                                    or bar.get("timestamp")
                                    or ""
                                )
                                for bar in (result.get("bars") or [])
                            }),
                        })
                    total_fetched += fetched
                    total_persisted += persisted
                    canonical_statuses.append(canonical)
                    source = result.get("source") or source
                    break
                except Exception as exc:
                    code, retryable = _error_policy(exc)
                    if (
                        not retryable
                        or int(shard["attempt"]) >= int(request["max_attempts"])
                    ):
                        _, message = _safe_error(exc)
                        shard.update({
                            "status": "failed", "error_code": code,
                            "error_message": message, "owner_id": None,
                            "lease_token": None, "lease_expires_at": None,
                            "heartbeat_at": None, "updated_at": _now(),
                            "finished_at": _now(),
                        })
                        self.repository.upsert_history_shard(shard)
                        telemetry_call(
                            self.telemetry, "safe", "counter",
                            "history_retry_total",
                            reason=self._retry_reason(code),
                            outcome="terminal",
                        )
                        telemetry_call(
                            self.telemetry, "safe", "histogram",
                            "history_shard_latency_seconds",
                            self._monotonic() - shard_started,
                            outcome="failed",
                        )
                        item.update({
                            "status": "failed", "canonical_status": "failed",
                            "source": source, "rows_fetched": total_fetched,
                            "rows_persisted": total_persisted,
                            "error_code": code, "error_message": message,
                            "updated_at": _now(), "finished_at": _now(),
                        })
                        self._finish_claim(item, lease_token)
                        return
                    delay = _retry_delay(
                        request, int(shard["attempt"]), exc, self._random()
                    )
                    if self._monotonic() + delay > retry_deadline:
                        _, message = _safe_error(exc)
                        shard.update({
                            "status": "failed",
                            "error_code": "retry_budget_exhausted",
                            "error_message": message, "owner_id": None,
                            "lease_token": None, "lease_expires_at": None,
                            "heartbeat_at": None, "updated_at": _now(),
                            "finished_at": _now(),
                        })
                        self.repository.upsert_history_shard(shard)
                        telemetry_call(
                            self.telemetry, "safe", "counter",
                            "history_retry_total",
                            reason=self._retry_reason(code),
                            outcome="exhausted",
                        )
                        item.update({
                            "status": "failed", "canonical_status": "failed",
                            "source": source, "rows_fetched": total_fetched,
                            "rows_persisted": total_persisted,
                            "error_code": "retry_budget_exhausted",
                            "error_message": message, "updated_at": _now(),
                            "finished_at": _now(),
                        })
                        self._finish_claim(item, lease_token)
                        return
                    telemetry_call(
                        self.telemetry, "safe", "counter",
                        "history_retry_total", reason=self._retry_reason(code),
                        outcome="scheduled",
                    )
                    self._sleep(delay)
        canonical = (
            "failed" if "failed" in canonical_statuses
            else "pending" if "pending" in canonical_statuses
            else "completed"
        )
        current = _clean(self.repository.get_history_job(job_id))
        item.update({
            "status": (
                "canceled"
                if current and current["status"] == "cancel_requested"
                else "succeeded"
            ),
            "source": source, "rows_fetched": total_fetched,
            "rows_persisted": total_persisted,
            "canonical_status": canonical, "error_code": None,
            "error_message": None, "updated_at": _now(), "finished_at": _now(),
        })
        self._finish_claim(item, lease_token)

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

    @staticmethod
    def _retry_reason(code: str) -> str:
        if "connection" in code:
            return "connection"
        if "timeout" in code:
            return "timeout"
        if "rate" in code:
            return "rate_limit"
        if "server" in code:
            return "server"
        if "storage" in code:
            return "storage"
        return "other"

    def _cancel_item(self, item: Dict[str, Any]) -> None:
        self._cancel_item_shards(item["job_id"], item["item_id"])
        item.update({
            "status": "canceled", "canonical_status": "pending",
            "updated_at": _now(), "finished_at": _now(),
        })
        self.repository.upsert_history_item(item)

    def _cancel_item_shards(self, job_id: str, item_id: str) -> None:
        for raw in map(
            _clean, self.repository.list_history_shards(job_id, item_id)
        ):
            if raw["status"] in TERMINAL_ITEM:
                continue
            shard = dict(raw)
            shard.update({
                "status": "canceled", "owner_id": None, "lease_token": None,
                "lease_expires_at": None, "heartbeat_at": None,
                "updated_at": _now(), "finished_at": _now(),
            })
            self.repository.upsert_history_shard(shard)

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
        elif any(item["status"] in {"queued", "running"} for item in items):
            status = (
                "cancel_requested"
                if job["status"] == "cancel_requested" else "running"
            )
        else:
            status = "canceled"
        job.update({
            "status": status, "updated_at": _now(),
            "finished_at": _now() if status in TERMINAL_JOB else None,
        })
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
        with self._guard:
            if self._closed:
                raise RuntimeError("history job manager is closed")
            job = _clean(self.repository.get_history_job(job_id))
            if not job:
                raise KeyError(job_id)
            if job_id in self._active:
                raise ValueError("job is still finalizing")
            changed = False
            for raw in map(_clean, self.repository.list_history_items(job_id)):
                item = dict(raw)
                if item["status"] == "failed":
                    changed = True
                    for shard_raw in map(
                        _clean,
                        self.repository.list_history_shards(
                            job_id, item["item_id"]
                        ),
                    ):
                        if shard_raw["status"] != "failed":
                            continue
                        shard = dict(shard_raw)
                        shard.update({
                            "status": "queued", "error_code": None,
                            "error_message": None, "owner_id": None,
                            "lease_token": None, "lease_expires_at": None,
                            "heartbeat_at": None, "updated_at": _now(),
                            "finished_at": None,
                        })
                        self.repository.upsert_history_shard(shard)
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
            self._active.add(job_id)
            self._coordinator.submit(self._run_job, job_id)
        return self.detail(job_id)

    def detail(self, job_id: str) -> Dict[str, Any]:
        job = _clean(self.repository.get_history_job(job_id))
        if not job:
            raise KeyError(job_id)
        stored_items = [
            dict(_clean(value)) for value in self.repository.list_history_items(job_id)
        ]
        items = []
        shards = list(map(
            _clean, self.repository.list_history_shards(job_id)
        ))
        shards_by_item: dict[str, list[Dict[str, Any]]] = {}
        for shard in shards:
            clean_shard = {
                key: value for key, value in dict(shard).items()
                if key != "lease_token"
            }
            shards_by_item.setdefault(str(shard["item_id"]), []).append(clean_shard)
        for stored in stored_items:
            item = {
                key: value for key, value in stored.items()
                if key != "lease_token"
            }
            item["lease_active"] = (
                item["status"] == "running" and not self._lease_expired(item)
            )
            item["shards"] = shards_by_item.get(str(item["item_id"]), [])
            items.append(item)
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
            "total_shards": len(shards),
            "completed_shards": sum(
                shard["status"] in TERMINAL_ITEM for shard in shards
            ),
        }
        for state in ("queued", "running", "succeeded", "failed", "canceled"):
            telemetry_call(
                self.telemetry, "safe", "gauge", "history_items",
                counts[state], state=state,
            )
        return result

    def list(self, limit: int = 50) -> list[Dict[str, Any]]:
        return [
            self.detail(str(job["job_id"]))
            for job in map(_clean, self.repository.list_history_jobs(limit))
        ]

    def health_snapshot(self) -> Dict[str, Any]:
        jobs = list(map(
            _clean, self.repository.list_recoverable_history_jobs()
        ))
        items = [
            item
            for job in jobs
            for item in map(
                _clean,
                self.repository.list_history_items(str(job["job_id"])),
            )
        ]
        queued = sum(item["status"] == "queued" for item in items)
        running = sum(item["status"] == "running" for item in items)
        expired = sum(
            item["status"] == "running" and self._lease_expired(item)
            for item in items
        )
        takeovers = sum(int(item.get("takeover_count") or 0) for item in items)
        canonical_pending = (
            len(self.repository.list_pending_history_canonical_checks(1001))
            if hasattr(
                self.repository, "list_pending_history_canonical_checks"
            )
            else 0
        )
        reasons = []
        unavailable = False
        if not self._watchdog.is_alive() and not self._closed:
            unavailable = True
            reasons.append("history_watchdog_unavailable")
        if expired >= HISTORY_HEALTH_THRESHOLDS[
            "expired_lease_unavailable"
        ]:
            unavailable = True
            reasons.append("history_expired_leases_critical")
        elif expired >= HISTORY_HEALTH_THRESHOLDS[
            "expired_lease_degraded"
        ]:
            reasons.append("history_expired_leases_present")
        if queued >= HISTORY_HEALTH_THRESHOLDS["queued_degraded"]:
            reasons.append("history_queue_backlog_high")
        if canonical_pending >= HISTORY_HEALTH_THRESHOLDS[
            "canonical_pending_degraded"
        ]:
            reasons.append("history_canonical_backlog_high")
        if takeovers >= HISTORY_HEALTH_THRESHOLDS["takeover_degraded"]:
            reasons.append("history_takeover_rate_high")
        return {
            "schema": "marketcow.history-health.v1",
            "status": (
                "unavailable" if unavailable
                else "degraded" if reasons else "healthy"
            ),
            "ready": not unavailable,
            "reasons": reasons,
            "counts": {
                "recoverable_jobs": len(jobs), "queued_items": queued,
                "running_items": running, "expired_leases": expired,
                "takeovers": takeovers,
                "canonical_pending": canonical_pending,
            },
            "thresholds": dict(HISTORY_HEALTH_THRESHOLDS),
            "watchdog_alive": self._watchdog.is_alive(),
        }

    def close(self) -> None:
        with self._guard:
            self._closed = True
        self._watchdog_stop.set()
        self._watchdog.join(timeout=max(1.0, self.lease_seconds))
        self._coordinator.shutdown(wait=True, cancel_futures=True)
        self.executor.shutdown(wait=True, cancel_futures=True)
        self._heartbeat_stop.set()
        heartbeat = self._heartbeat
        if heartbeat is not None:
            heartbeat.join(timeout=max(1.0, self.lease_seconds))
