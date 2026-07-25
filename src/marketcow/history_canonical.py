from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict

from .telemetry import sanitize_text


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds")


def _clean(value: Any) -> Any:
    """Normalize text returned as bytes by legacy SQL_ASCII PostgreSQL."""
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="strict")
    if isinstance(value, dict):
        return {str(_clean(key)): _clean(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_clean(item) for item in value]
    return value


class HistoryCanonicalVerifier:
    def __init__(
        self, metadata_repository: Any, market_bar_repository: Any,
        max_attempts: int = 3,
    ) -> None:
        self.metadata_repository = metadata_repository
        self.market_bar_repository = market_bar_repository
        self.max_attempts = max(1, int(max_attempts))

    def enqueue(self, row: Dict[str, Any]) -> Dict[str, Any]:
        now = _now()
        check = {
            **row, "status": "pending", "attempt": 0,
            "error_code": None, "error_message": None,
            "created_at": row.get("created_at") or now,
            "updated_at": now, "finished_at": None,
        }
        return self.metadata_repository.upsert_history_canonical_check(check)

    def run_pending(self, limit: int = 100) -> Dict[str, int]:
        checks = self.metadata_repository.list_pending_history_canonical_checks(
            limit
        )
        completed = failed = pending = 0
        for raw in checks:
            check = dict(_clean(raw))
            now = _now()
            check["attempt"] = int(check["attempt"]) + 1
            try:
                identity = (
                    self.market_bar_repository.get_canonical_dataset_identity(
                        check["symbol"], check["interval"],
                        check["adjustment"], check["range_start"],
                        check["range_end"],
                    )
                )
                if int(identity.get("row_count") or 0) >= int(
                    check["expected_rows"]
                ):
                    check.update({
                        "status": "completed", "error_code": None,
                        "error_message": None, "updated_at": now,
                        "finished_at": now,
                    })
                    completed += 1
                else:
                    check.update({
                        "status": (
                            "failed"
                            if check["attempt"] >= self.max_attempts
                            else "retry"
                        ),
                        "error_code": "canonical_rows_pending",
                        "error_message": (
                            f"expected {check['expected_rows']} canonical rows; "
                            f"observed {identity.get('row_count') or 0}"
                        ),
                        "updated_at": now,
                        "finished_at": (
                            now if check["attempt"] >= self.max_attempts else None
                        ),
                    })
                    if check["status"] == "failed":
                        failed += 1
                    else:
                        pending += 1
            except Exception as exc:
                check.update({
                    "status": (
                        "failed"
                        if check["attempt"] >= self.max_attempts else "retry"
                    ),
                    "error_code": "canonical_check_failed",
                    "error_message": sanitize_text(exc),
                    "updated_at": now,
                    "finished_at": (
                        now if check["attempt"] >= self.max_attempts else None
                    ),
                })
                if check["status"] == "failed":
                    failed += 1
                else:
                    pending += 1
            self.metadata_repository.upsert_history_canonical_check(check)
            self._refresh_item(check["job_id"], check["item_id"])
        return {
            "processed": len(checks), "completed": completed,
            "failed": failed, "pending": pending,
        }

    def _refresh_item(self, job_id: str, item_id: str) -> None:
        checks = list(map(
            _clean,
            self.metadata_repository.list_history_canonical_checks(job_id, item_id),
        ))
        if not checks:
            return
        statuses = {str(check["status"]) for check in checks}
        canonical_status = (
            "failed" if "failed" in statuses
            else "completed" if statuses == {"completed"}
            else "pending"
        )
        checks_by_shard = {
            str(check["shard_key"]): str(check["status"]) for check in checks
        }
        for raw_shard in map(
            _clean,
            self.metadata_repository.list_history_shards(job_id, item_id),
        ):
            check_status = checks_by_shard.get(str(raw_shard["shard_key"]))
            if check_status is None:
                continue
            shard = dict(raw_shard)
            receipt = dict(shard.get("write_receipt_json") or {})
            receipt["canonical_status"] = (
                "completed" if check_status == "completed"
                else "failed" if check_status == "failed"
                else "pending"
            )
            shard["write_receipt_json"] = receipt
            shard["updated_at"] = _now()
            self.metadata_repository.upsert_history_shard(shard)
        items = list(map(
            _clean, self.metadata_repository.list_history_items(job_id)
        ))
        item = next(
            (dict(value) for value in items if value["item_id"] == item_id),
            None,
        )
        if item is None:
            return
        item["canonical_status"] = canonical_status
        item["updated_at"] = _now()
        self.metadata_repository.upsert_history_item(item)
