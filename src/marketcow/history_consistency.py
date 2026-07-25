from __future__ import annotations

import json
from typing import Any, Dict


def _metadata(row: Dict[str, Any]) -> Dict[str, Any]:
    value = row.get("metadata_json") or row.get("metadata") or {}
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except (TypeError, ValueError):
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return dict(value) if isinstance(value, dict) else {}


class HistoryConsistencyAuditor:
    """Classify cross-store history inconsistencies without destructive repair."""

    def __init__(
        self, metadata_repository: Any, market_bar_repository: Any,
        artifact_store: Any,
    ) -> None:
        self.metadata_repository = metadata_repository
        self.market_bar_repository = market_bar_repository
        self.artifact_store = artifact_store

    def audit(self, limit: int = 10000) -> Dict[str, Any]:
        shards = self.metadata_repository.list_all_history_shards(limit)
        artifacts = self.artifact_store.list_artifacts("", limit)
        raw_receipts = self.market_bar_repository.list_raw_ingestion_receipts(
            limit
        )
        shards_by_ingestion = {
            str(row.get("ingestion_id")): dict(row)
            for row in shards if row.get("ingestion_id")
        }
        artifacts_by_ingestion: dict[str, list[Dict[str, Any]]] = {}
        for row in artifacts:
            ingestion_id = str(_metadata(dict(row)).get("ingestion_id") or "")
            if ingestion_id:
                artifacts_by_ingestion.setdefault(ingestion_id, []).append(
                    dict(row)
                )
        receipts_by_ingestion = {
            str(row["ingestion_id"]): dict(row) for row in raw_receipts
        }
        findings = []
        for ingestion_id, shard in shards_by_ingestion.items():
            has_artifact = ingestion_id in artifacts_by_ingestion
            has_bars = ingestion_id in receipts_by_ingestion
            if has_artifact and has_bars:
                continue
            if not has_artifact and has_bars:
                kind, action = "raw_artifact_missing", "refetch_or_restore_artifact"
            elif has_artifact and not has_bars:
                kind, action = "market_bars_missing", "replay_artifact"
            else:
                kind, action = "ingestion_missing", "refetch_shard"
            findings.append({
                "kind": kind, "recommended_action": action,
                "ingestion_id": ingestion_id, "job_id": shard["job_id"],
                "item_id": shard["item_id"], "shard_key": shard["shard_key"],
            })
        for ingestion_id, rows in artifacts_by_ingestion.items():
            if ingestion_id not in shards_by_ingestion:
                findings.append({
                    "kind": "orphan_artifact",
                    "recommended_action": "quarantine_artifact",
                    "ingestion_id": ingestion_id,
                    "artifact_ids": [row.get("artifact_id") for row in rows],
                })
        for ingestion_id, receipt in receipts_by_ingestion.items():
            if ingestion_id not in shards_by_ingestion:
                findings.append({
                    "kind": "orphan_market_bars",
                    "recommended_action": "quarantine_ingestion",
                    "ingestion_id": ingestion_id,
                    "row_count": receipt["row_count"],
                })
        findings.sort(key=lambda row: (
            str(row["kind"]), str(row["ingestion_id"])
        ))
        return {
            "findings": findings,
            "counts": {
                kind: sum(row["kind"] == kind for row in findings)
                for kind in {
                    "raw_artifact_missing", "market_bars_missing",
                    "ingestion_missing", "orphan_artifact",
                    "orphan_market_bars",
                }
            },
            "scanned": {
                "shards": len(shards), "artifacts": len(artifacts),
                "raw_ingestions": len(raw_receipts),
            },
            "truncated": any(
                len(rows) >= limit
                for rows in (shards, artifacts, raw_receipts)
            ),
        }
