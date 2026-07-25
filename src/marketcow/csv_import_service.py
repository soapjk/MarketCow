from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .csv_import import CsvImportRequest, dry_run_csv
from .csv_import_ingestion import CsvShardImporter
from .csv_import_jobs import CsvImportJobManager
from .csv_import_manifest import (
    CsvImportManifest,
    archive_csv,
    plan_csv_shards,
)
from .csv_import_quality import CsvImportQualityVerifier


class CsvImportService:
    """One orchestration boundary shared by CLI, admin API and UI."""

    def __init__(
        self,
        *,
        allowed_root: Path,
        storage_root: Path,
        metadata_repository: Any,
        market_bar_repository: Any,
        artifact_store: Any,
        canonical_builder: Any = None,
        max_workers: int = 2,
        lease_seconds: float = 30,
        max_file_bytes: int = 100 * 1024 * 1024 * 1024,
    ):
        self.allowed_root = allowed_root.resolve()
        self.storage_root = storage_root.resolve()
        self.metadata_repository = metadata_repository
        self.market_bar_repository = market_bar_repository
        self.artifact_store = artifact_store
        self.canonical_builder = canonical_builder
        self.max_file_bytes = max_file_bytes
        self.shard_importer = CsvShardImporter(market_bar_repository)
        self.quality = CsvImportQualityVerifier(market_bar_repository)
        self.manager = CsvImportJobManager(
            metadata_repository, self._import_shard,
            finalize_job=self._finalize_job,
            max_workers=max_workers, lease_seconds=lease_seconds,
        )

    def _allowed_path(self, value: str | Path) -> Path:
        path = Path(value).expanduser().resolve(strict=True)
        if not path.is_relative_to(self.allowed_root):
            raise ValueError("CSV path is outside MARKETCOW_ALLOWED_ROOT")
        if not path.is_file():
            raise ValueError("CSV path must be a regular file")
        if path.stat().st_size > self.max_file_bytes:
            raise ValueError("CSV file exceeds the configured size limit")
        return path

    def dry_run(
        self, path: str | Path, request: CsvImportRequest,
        *, max_error_samples: int = 100,
    ) -> dict[str, Any]:
        return dry_run_csv(
            self._allowed_path(path), request,
            max_error_samples=max_error_samples,
        )

    def create_import(
        self,
        path: str | Path,
        request: CsvImportRequest,
        *,
        idempotency_key: str,
        chunk_rows: int = 100000,
        max_attempts: int = 3,
    ) -> tuple[dict[str, Any], bool]:
        source = self._allowed_path(path)
        report = dry_run_csv(source, request)
        if report["status"] != "valid":
            raise ValueError("CSV dry-run must pass before import")
        if int(report["rows_valid"]) == 0:
            raise ValueError("CSV contains no valid bars")
        manifest = CsvImportManifest.create(source, request, report)
        archived = archive_csv(source, self.storage_root, manifest)
        now = datetime.now(timezone.utc).isoformat(timespec="microseconds")
        artifact_id = "csv-" + manifest.manifest_id
        self.artifact_store.save_artifact({
            "artifact_id": artifact_id,
            "dataset": "csv_history_bars",
            "source": request.source,
            "source_url": "",
            "observed_at": now,
            "ingested_at": now,
            "raw_response_locator": "immutable CSV file",
            "storage_path": archived["storage_path"],
            "sha256": manifest.file_sha256,
            "byte_size": manifest.byte_size,
            "metadata_json": json.dumps({
                "manifest_id": manifest.manifest_id,
                "contract_version": manifest.contract_version,
                "profile": request.profile.identity,
                "namespace": request.instruments.namespace,
                "interval": request.interval,
                "adjustment": request.adjustment,
                "instrument_mappings": dict(manifest.instrument_mappings),
                "first_bar_at": manifest.first_bar_at,
                "last_bar_at": manifest.last_bar_at,
                "created_by": manifest.created_by,
                "source_proof": manifest.source_proof,
                "retention_policy": manifest.retention_policy,
            }, sort_keys=True),
        })
        shards = plan_csv_shards(
            manifest, int(report["rows_valid"]), int(chunk_rows)
        )
        request_json = {
            "request": request.as_dict(),
            "manifest": manifest.as_dict(),
            "dry_run_report": report,
            "max_attempts": max(1, min(int(max_attempts), 10)),
            "chunk_rows": int(chunk_rows),
        }
        return self.manager.create(
            idempotency_key=idempotency_key,
            manifest_id=manifest.manifest_id,
            request_json=request_json,
            storage_path=archived["storage_path"],
            raw_artifact_id=artifact_id,
            rows_total=int(report["rows_valid"]),
            shards=shards,
        )

    def _import_shard(
        self, job: dict[str, Any], shard: dict[str, Any]
    ) -> dict[str, Any]:
        request = CsvImportRequest.from_dict(job["request_json"]["request"])
        manifest = CsvImportManifest(**job["request_json"]["manifest"])
        return self.shard_importer.import_archived_shard(
            self._allowed_path(job["storage_path"]), request, manifest, shard,
            raw_artifact_id=job["raw_artifact_id"],
            ingested_at=datetime.now(timezone.utc).isoformat(
                timespec="microseconds"
            ),
            should_cancel=lambda: (
                self.metadata_repository.get_csv_import_job(job["job_id"])[
                    "status"
                ] == "cancel_requested"
            ),
        )

    def _finalize_job(self, job: dict[str, Any]) -> dict[str, Any]:
        request = CsvImportRequest.from_dict(job["request_json"]["request"])
        if self.canonical_builder is not None:
            ranges: dict[str, tuple[int, int]] = {}
            for shard in job["shards"]:
                for receipt in (
                    shard.get("write_receipt_json") or {}
                ).get("receipts") or ():
                    durable = self.market_bar_repository.get_raw_ingestion_receipt(
                        receipt["ingestion_id"]
                    )
                    if durable is None:
                        continue
                    instrument_id = str(receipt["instrument_id"])
                    start, end = ranges.get(instrument_id, (
                        int(durable["first_bar_at_ms"]),
                        int(durable["last_bar_at_ms"]),
                    ))
                    ranges[instrument_id] = (
                        min(start, int(durable["first_bar_at_ms"])),
                        max(end, int(durable["last_bar_at_ms"])),
                    )
            for instrument_id, (start_ms, end_ms) in sorted(ranges.items()):
                self.canonical_builder.rebuild(
                    instrument_id, request.interval, request.adjustment,
                    datetime.fromtimestamp(start_ms / 1000, timezone.utc),
                    datetime.fromtimestamp(end_ms / 1000, timezone.utc),
                )
        return self.quality.verify(job)

    def get(self, job_id: str) -> dict[str, Any] | None:
        return self.manager.get(job_id)

    def list(
        self, limit: int = 50, manifest_id: str = ""
    ) -> list[dict[str, Any]]:
        rows = []
        for job in self.metadata_repository.list_csv_import_jobs(
            limit, manifest_id
        ):
            current = self.manager.get(job["job_id"])
            if current is not None:
                rows.append(current)
        return rows

    def cancel(self, job_id: str) -> dict[str, Any] | None:
        return self.manager.cancel(job_id)

    def manifest(self, job_id: str) -> dict[str, Any] | None:
        job = self.get(job_id)
        if job is None:
            return None
        return dict(job["request_json"]["manifest"])

    def quality_report(self, job_id: str) -> dict[str, Any] | None:
        job = self.get(job_id)
        if job is None:
            return None
        report = job.get("quality_report_json")
        return None if report is None else dict(report)

    def errors(self, job_id: str) -> list[dict[str, Any]] | None:
        job = self.get(job_id)
        if job is None:
            return None
        return [{
            "shard_index": shard["shard_index"],
            "status": shard["status"],
            "attempt": shard["attempt"],
            "error_code": shard.get("error_code"),
            "error_message": shard.get("error_message"),
        } for shard in job["shards"] if shard.get("error_code")]

    def retry(
        self, job_id: str, *, idempotency_key: str
    ) -> tuple[dict[str, Any], bool]:
        job = self.get(job_id)
        if job is None:
            raise ValueError("CSV import job not found")
        if job["status"] not in {"failed", "canceled"}:
            raise ValueError("only failed or canceled CSV imports can be retried")
        request_json = dict(job["request_json"])
        manifest = CsvImportManifest(**request_json["manifest"])
        shards = plan_csv_shards(
            manifest,
            int(job["rows_total"]),
            int(request_json["chunk_rows"]),
        )
        return self.manager.create(
            idempotency_key=idempotency_key,
            manifest_id=manifest.manifest_id,
            request_json=request_json,
            storage_path=job["storage_path"],
            raw_artifact_id=job["raw_artifact_id"],
            rows_total=int(job["rows_total"]),
            shards=shards,
        )

    def close(self) -> None:
        self.manager.close()


def create_csv_import_service(settings: Any, service: Any) -> CsvImportService:
    resources = getattr(service, "online_resources", None)
    return CsvImportService(
        allowed_root=settings.allowed_root or settings.storage_root,
        storage_root=settings.storage_root,
        metadata_repository=service.metadata_repository,
        market_bar_repository=service.market_bar_repository,
        artifact_store=service.artifact_store,
        canonical_builder=getattr(resources, "canonical_builder", None),
        max_workers=min(
            16, max(1, getattr(settings, "history_job_max_workers", 4))
        ),
        lease_seconds=getattr(settings, "history_job_lease_seconds", 30),
        max_file_bytes=getattr(
            settings, "csv_import_max_file_bytes", 100 * 1024 * 1024 * 1024
        ),
    )
