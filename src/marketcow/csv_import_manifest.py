from __future__ import annotations

import hashlib
import json
import os
import shutil
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from .csv_import import CSV_IMPORT_CONTRACT_VERSION, CsvImportRequest, file_sha256


@dataclass(frozen=True)
class CsvImportManifest:
    manifest_id: str
    contract_version: str
    file_name: str
    file_sha256: str
    byte_size: int
    source: str
    namespace: str
    profile_name: str
    profile_version: str
    interval: str
    adjustment: str
    instrument_mappings: Mapping[str, str]
    first_bar_at: str | None
    last_bar_at: str | None
    created_at: str
    created_by: str
    source_proof: str
    retention_policy: str

    def __post_init__(self) -> None:
        if self.contract_version != CSV_IMPORT_CONTRACT_VERSION:
            raise ValueError("CSV manifest contract version is not supported")
        if self.adjustment not in {"raw", "qfq", "hfq"}:
            raise ValueError("CSV manifest adjustment must be raw, qfq or hfq")

    @classmethod
    def create(
        cls,
        path: Path,
        request: CsvImportRequest,
        dry_run_report: Mapping[str, Any] | None = None,
    ) -> "CsvImportManifest":
        resolved = path.resolve(strict=True)
        mappings = dict(sorted(request.instruments.canonical_mappings.items()))
        summaries = list(
            (dry_run_report or {}).get("instruments", {}).values()
        )
        first_bar_at = min(
            (str(value["first_bar_at"]) for value in summaries),
            default=None,
        )
        last_bar_at = max(
            (str(value["last_bar_at"]) for value in summaries),
            default=None,
        )
        identity = {
            "contract_version": CSV_IMPORT_CONTRACT_VERSION,
            "file_sha256": file_sha256(resolved),
            "source": request.source,
            "namespace": request.instruments.namespace,
            "profile": request.profile.identity,
            "interval": request.interval,
            "adjustment": request.adjustment,
            "instrument_mappings": mappings,
            "first_bar_at": first_bar_at,
            "last_bar_at": last_bar_at,
            "source_proof": request.source_proof,
            "retention_policy": request.retention_policy,
        }
        manifest_id = hashlib.sha256(json.dumps(
            identity, sort_keys=True, separators=(",", ":")
        ).encode()).hexdigest()
        return cls(
            manifest_id=manifest_id,
            contract_version=CSV_IMPORT_CONTRACT_VERSION,
            file_name=resolved.name,
            file_sha256=identity["file_sha256"],
            byte_size=resolved.stat().st_size,
            source=request.source,
            namespace=request.instruments.namespace,
            profile_name=request.profile.name,
            profile_version=request.profile.version,
            interval=request.interval,
            adjustment=request.adjustment,
            instrument_mappings=mappings,
            first_bar_at=first_bar_at,
            last_bar_at=last_bar_at,
            created_at=datetime.now(timezone.utc).isoformat(
                timespec="microseconds"
            ),
            created_by=request.created_by,
            source_proof=request.source_proof,
            retention_policy=request.retention_policy,
        )

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def plan_csv_shards(
    manifest: CsvImportManifest, rows_total: int, chunk_rows: int
) -> list[dict[str, Any]]:
    if rows_total < 0:
        raise ValueError("rows_total must not be negative")
    if chunk_rows <= 0:
        raise ValueError("chunk_rows must be positive")
    shards = []
    for start in range(0, rows_total, chunk_rows):
        end = min(rows_total, start + chunk_rows)
        payload = {
            "schema_version": 1,
            "manifest_id": manifest.manifest_id,
            "row_start": start,
            "row_end": end,
            "interval": manifest.interval,
            "adjustment": manifest.adjustment,
        }
        ingestion_id = hashlib.sha256(json.dumps(
            payload, sort_keys=True, separators=(",", ":")
        ).encode()).hexdigest()
        shards.append({
            "shard_index": len(shards),
            "row_start": start,
            "row_end": end,
            "ingestion_id": ingestion_id,
        })
    return shards


def archive_csv(
    source_path: Path, storage_root: Path, manifest: CsvImportManifest
) -> dict[str, Any]:
    source = source_path.resolve(strict=True)
    root = storage_root.resolve()
    target_folder = root / "csv-imports" / manifest.manifest_id[:2]
    target_folder.mkdir(parents=True, exist_ok=True)
    target = target_folder / f"{manifest.manifest_id}.csv"
    if target.exists():
        if file_sha256(target) != manifest.file_sha256:
            raise RuntimeError("archived CSV hash conflicts with manifest")
        return {"storage_path": str(target), "deduplicated": True}
    temporary = target_folder / f".{target.name}.{uuid.uuid4().hex}.tmp"
    try:
        with source.open("rb") as reader, temporary.open("xb") as writer:
            shutil.copyfileobj(reader, writer, length=1024 * 1024)
            writer.flush()
            os.fsync(writer.fileno())
        if file_sha256(temporary) != manifest.file_sha256:
            raise RuntimeError("archived CSV hash verification failed")
        os.replace(temporary, target)
    finally:
        temporary.unlink(missing_ok=True)
    return {"storage_path": str(target), "deduplicated": False}
