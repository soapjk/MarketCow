from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict

from .telemetry import telemetry_call

def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds")


class HistoryReconciler:
    """Repair durable checkpoints from ClickHouse ingestion receipts."""

    def __init__(
        self, metadata_repository: Any, market_bar_repository: Any,
        telemetry: Any = None,
    ) -> None:
        self.metadata_repository = metadata_repository
        self.market_bar_repository = market_bar_repository
        self.telemetry = telemetry

    def reconcile(self, job_id: str, *, dry_run: bool = True) -> Dict[str, Any]:
        job = self.metadata_repository.get_history_job(job_id)
        if not job:
            raise KeyError(job_id)
        items = self.metadata_repository.list_history_items(job_id)
        shards = self.metadata_repository.list_history_shards(job_id)
        now = _now()
        actions = []
        repaired_keys = set()
        for raw in shards:
            shard = dict(raw)
            if shard["status"] in {"succeeded", "canceled"}:
                continue
            ingestion_id = str(shard.get("ingestion_id") or "")
            if not ingestion_id:
                actions.append({
                    "item_id": shard["item_id"],
                    "shard_key": shard["shard_key"],
                    "action": "unverifiable",
                    "reason": "ingestion_identity_missing",
                })
                continue
            receipt = self.market_bar_repository.get_raw_ingestion_receipt(
                ingestion_id
            )
            if receipt is None:
                actions.append({
                    "item_id": shard["item_id"],
                    "shard_key": shard["shard_key"],
                    "action": "no_change",
                    "reason": "raw_ingestion_not_found",
                })
                continue
            repaired = {
                **shard, "status": "succeeded",
                "rows_fetched": int(receipt["row_count"]),
                "rows_persisted": int(receipt["row_count"]),
                "cursor_json": {"confirmed_through": shard["range_end"]},
                "write_receipt_json": {
                    **dict(shard.get("write_receipt_json") or {}),
                    **receipt, "reconciled": True,
                },
                "updated_at": now, "finished_at": now,
            }
            applied = True
            if not dry_run:
                applied = self.metadata_repository.reconcile_history_shard(
                    repaired, now
                ) is not None
            actions.append({
                "item_id": shard["item_id"],
                "shard_key": shard["shard_key"],
                "action": "mark_succeeded" if applied else "lease_conflict",
                "reason": "raw_ingestion_found",
                "row_count": receipt["row_count"],
            })
            if applied:
                repaired_keys.add((shard["item_id"], shard["shard_key"]))

        repaired_items = 0
        if not dry_run and repaired_keys:
            current_shards = self.metadata_repository.list_history_shards(job_id)
            by_item: dict[str, list[Dict[str, Any]]] = {}
            for shard in current_shards:
                by_item.setdefault(str(shard["item_id"]), []).append(dict(shard))
            for raw_item in items:
                item = dict(raw_item)
                owned = by_item.get(str(item["item_id"]), [])
                if not owned or any(
                    shard["status"] != "succeeded" for shard in owned
                ):
                    continue
                rows = sum(int(shard["rows_persisted"]) for shard in owned)
                sources = [
                    dict(shard.get("write_receipt_json") or {}).get("source")
                    for shard in owned
                ]
                item.update({
                    "status": "succeeded",
                    "source": next((value for value in sources if value), None),
                    "rows_fetched": sum(
                        int(shard["rows_fetched"]) for shard in owned
                    ),
                    "rows_persisted": rows,
                    "canonical_status": "pending",
                    "updated_at": now, "finished_at": now,
                })
                if self.metadata_repository.reconcile_history_item(
                    item, now
                ) is not None:
                    repaired_items += 1
            current_items = self.metadata_repository.list_history_items(job_id)
            statuses = [str(item["status"]) for item in current_items]
            if statuses and all(
                status in {"succeeded", "failed", "canceled"}
                for status in statuses
            ):
                succeeded = statuses.count("succeeded")
                failed = statuses.count("failed")
                canceled = statuses.count("canceled")
                status = (
                    "partially_failed" if succeeded and failed
                    else "failed" if failed
                    else "canceled" if canceled and not succeeded
                    else "succeeded"
                )
                saved_job = dict(job)
                saved_job.update({
                    "status": status, "updated_at": now, "finished_at": now,
                })
                self.metadata_repository.upsert_history_job(saved_job)
        result = {
            "job_id": job_id, "dry_run": dry_run, "actions": actions,
            "repaired_shards": sum(
                action["action"] == "mark_succeeded" for action in actions
            ),
            "repaired_items": repaired_items,
        }
        outcome = (
            "dry_run" if dry_run
            else "repaired" if result["repaired_shards"]
            else "conflict" if any(
                action["action"] == "lease_conflict" for action in actions
            )
            else "no_change"
        )
        telemetry_call(
            self.telemetry, "safe", "counter",
            "history_reconcile_total", outcome=outcome,
        )
        telemetry_call(
            self.telemetry, "safe", "log", "history",
            action="reconcile", job_id=job_id, outcome=outcome,
            repaired_shards=result["repaired_shards"],
            repaired_items=result["repaired_items"],
        )
        return result
