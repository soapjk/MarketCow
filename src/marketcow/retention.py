from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import MappingProxyType
from typing import Any, Dict, Iterable, Mapping

import duckdb

from .cold_archive import BUSINESS_KEY, DATA_COLUMNS, ParquetColdArchive, _json_hash


POLICY_VERSION = "storage-v2.retention-policy.v1"
REPORT_VERSION = "storage-v2.retention-dry-run.v1"
MAX_ARTIFACTS = 1000
MAX_HOLDS = 1000


def _aware(value: datetime | str) -> datetime:
    parsed = value if isinstance(value, datetime) else datetime.fromisoformat(
        str(value).replace("Z", "+00:00")
    )
    if parsed.tzinfo is None:
        raise ValueError("retention as_of must include timezone")
    return parsed.astimezone(timezone.utc)


def _policy_hash(value: Mapping[str, Any]) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


@dataclass(frozen=True)
class RetentionPolicy:
    dataset: str = "market_price_bar_raw"
    default_retain_days: int = 400
    safety_window_days: int = 30
    source_retain_days: Mapping[str, int] = field(default_factory=dict)
    version: str = POLICY_VERSION

    def __post_init__(self) -> None:
        if self.version != POLICY_VERSION:
            raise ValueError("unsupported retention policy version")
        if self.dataset != "market_price_bar_raw":
            raise ValueError("unsupported retention dataset")
        if (isinstance(self.default_retain_days, bool)
                or not isinstance(self.default_retain_days, int)
                or not 30 <= self.default_retain_days <= 3650):
            raise ValueError("default retention must be between 30 and 3650 days")
        if (isinstance(self.safety_window_days, bool)
                or not isinstance(self.safety_window_days, int)
                or not 1 <= self.safety_window_days <= 365):
            raise ValueError("safety window must be between 1 and 365 days")
        normalized: Dict[str, int] = {}
        for source, days in self.source_retain_days.items():
            if not isinstance(source, str) or not source.strip() or len(source.strip()) > 64:
                raise ValueError("source retention rule is invalid")
            if (isinstance(days, bool) or not isinstance(days, int)
                    or not 30 <= days <= 3650):
                raise ValueError("source retention rule is invalid")
            key = source.strip()
            if key in normalized:
                raise ValueError("source retention rules collide after normalization")
            normalized[key] = days
        if len(normalized) > 1000:
            raise ValueError("source retention rules exceed limit")
        object.__setattr__(self, "source_retain_days",
                           MappingProxyType(dict(sorted(normalized.items()))))

    def days_for(self, source: str) -> int:
        return int(self.source_retain_days.get(source, self.default_retain_days))

    def document(self) -> Dict[str, Any]:
        return {
            "version": self.version, "dataset": self.dataset,
            "default_retain_days": self.default_retain_days,
            "safety_window_days": self.safety_window_days,
            "source_retain_days": dict(sorted(self.source_retain_days.items())),
        }


class RetentionDryRun:
    """Read-only retention candidate planner; it has no deletion capability."""

    def __init__(self, archive: ParquetColdArchive, policy: RetentionPolicy) -> None:
        self.archive = archive
        self.policy = policy

    @staticmethod
    def _partition_id(partition: Mapping[str, Any]) -> str:
        return (f"market={partition['market']}/interval={partition['interval']}/"
                f"source={partition['source']}/year={int(partition['year']):04d}/"
                f"month={int(partition['month']):02d}")

    @staticmethod
    def _partition_end(partition: Mapping[str, Any]) -> datetime:
        year, month = int(partition["year"]), int(partition["month"])
        return datetime(year + (month == 12), 1 if month == 12 else month + 1, 1,
                        tzinfo=timezone.utc)

    def _online_rows(self, manifest: Mapping[str, Any]) -> list[dict[str, Any]]:
        partition = manifest["partition"]
        start = datetime(int(partition["year"]), int(partition["month"]), 1,
                         tzinfo=timezone.utc)
        end = self._partition_end(partition)
        params = [partition["market"], partition["interval"], partition["source"],
                  int(start.timestamp()), int(end.timestamp())]
        with duckdb.connect(str(self.archive.database_path), read_only=True) as con:
            rows = con.execute(self.archive._select_sql(), params).fetchall()
        return [dict(zip(DATA_COLUMNS, row)) for row in rows]

    def _coverage(self, artifact: Path, manifest: Mapping[str, Any]) -> tuple[bool, str]:
        online = self._online_rows(manifest)
        if len(online) != manifest["row_count"]:
            return False, "online_row_count_mismatch"
        if _json_hash(online) != manifest["logical_checksum"]:
            return False, "online_logical_checksum_mismatch"
        keys = [[row[column] for column in BUSINESS_KEY] for row in online]
        if _json_hash(keys) != manifest["business_key_checksum"]:
            return False, "online_business_key_checksum_mismatch"
        cold = self.archive.read_for_backfill(artifact)
        if _json_hash(cold) != _json_hash(online):
            return False, "cold_query_not_equivalent"
        return True, "complete"

    @staticmethod
    def _bounded_strings(values: Iterable[Any], limit: int, label: str,
                         path_values: bool = False) -> list[str]:
        result = set()
        for value in values:
            text = str(Path(value)) if path_values else str(value)
            result.add(text)
            if len(result) > limit:
                raise ValueError(f"retention {label} exceed limit {limit}")
        return sorted(result)

    def dry_run(
        self, artifacts: Iterable[Path], as_of: datetime | str,
        holds: Iterable[str] = (),
    ) -> Dict[str, Any]:
        observed = _aware(as_of)
        supplied_artifacts = self._bounded_strings(
            artifacts, MAX_ARTIFACTS, "artifacts", path_values=True
        )
        held = frozenset(self._bounded_strings(holds, MAX_HOLDS, "holds"))
        policy = self.policy.document()
        policy_sha256 = _policy_hash(policy)
        input_document = {
            "policy_sha256": policy_sha256, "as_of": observed.isoformat(),
            "holds": sorted(held), "artifacts": supplied_artifacts,
        }
        input_sha256 = _policy_hash(input_document)
        candidates = []
        excluded = []
        for supplied in supplied_artifacts:
            artifact = Path(supplied)
            try:
                manifest = self.archive.verify(artifact)
                partition_id = self._partition_id(manifest["partition"])
            except Exception as error:
                excluded.append({
                    "artifact_ref": hashlib.sha256(supplied.encode()).hexdigest()[:16],
                    "partition_id": None,
                    "reason": "artifact_verification_failed",
                    "detail": type(error).__name__,
                })
                continue
            if partition_id in held:
                excluded.append({"artifact_id": manifest["artifact_id"],
                                 "partition_id": partition_id, "reason": "held"})
                continue
            retain_days = self.policy.days_for(manifest["partition"]["source"])
            cutoff = observed - timedelta(days=max(
                retain_days, self.policy.safety_window_days
            ))
            partition_end = self._partition_end(manifest["partition"])
            if partition_end > cutoff:
                excluded.append({
                    "artifact_id": manifest["artifact_id"],
                    "partition_id": partition_id, "reason": "inside_retention_window",
                    "eligible_after": (
                        partition_end + timedelta(days=max(
                            retain_days, self.policy.safety_window_days
                        ))
                    ).isoformat(),
                })
                continue
            try:
                complete, reason = self._coverage(artifact, manifest)
            except Exception as error:
                excluded.append({
                    "artifact_id": manifest["artifact_id"],
                    "partition_id": partition_id, "reason": "artifact_read_failed",
                    "detail": type(error).__name__,
                })
                continue
            if not complete:
                excluded.append({"artifact_id": manifest["artifact_id"],
                                 "partition_id": partition_id, "reason": reason})
                continue
            relative = artifact.resolve().relative_to(self.archive.archive_root).as_posix()
            candidates.append({
                "candidate_id": hashlib.sha256(
                    f"{policy_sha256}:{input_sha256}:{manifest['artifact_id']}".encode()
                ).hexdigest()[:24],
                "dataset": self.policy.dataset, "partition_id": partition_id,
                "artifact_id": manifest["artifact_id"],
                "artifact": f"archive://{relative}",
                "manifest_payload_sha256": manifest["manifest_payload_sha256"],
                "parquet_sha256": manifest["parquet_sha256"],
                "watermark": manifest["watermark"],
                "row_count": manifest["row_count"],
                "estimated_reclaim_bytes": manifest["logical_json_bytes"],
                "policy_sha256": policy_sha256, "input_sha256": input_sha256,
                "cold_query_equivalent": True, "action": "candidate_only_no_delete",
            })
        candidates.sort(key=lambda item: (item["partition_id"], item["artifact_id"]))
        excluded.sort(key=lambda item: (str(item.get("partition_id")),
                                        str(item.get("artifact_id") or
                                            item.get("artifact_ref"))))
        return {
            "schema": REPORT_VERSION, "dry_run": True,
            "action": "candidate_only_no_delete",
            "as_of": observed.isoformat(), "policy": policy,
            "policy_sha256": policy_sha256, "input_sha256": input_sha256,
            "holds": sorted(held),
            "candidate_count": len(candidates), "candidates": candidates,
            "excluded_count": len(excluded), "excluded": excluded,
            "estimated_reclaim_bytes": sum(
                item["estimated_reclaim_bytes"] for item in candidates
            ),
            "mutations_performed": 0,
            "limits": {"artifacts": MAX_ARTIFACTS, "holds": MAX_HOLDS,
                       "excluded": MAX_ARTIFACTS},
        }
