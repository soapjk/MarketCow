from __future__ import annotations

import hashlib
from collections import defaultdict
from pathlib import Path
from typing import Any, Callable, TextIO

from .csv_import import CsvImportRequest, ParsedCsvBar, iter_csv_bars
from .csv_import_manifest import CsvImportManifest


class CsvImportCanceled(RuntimeError):
    pass


def instrument_ingestion_id(shard_ingestion_id: str, instrument_id: str) -> str:
    return hashlib.sha256(
        f"{shard_ingestion_id}|{instrument_id}".encode()
    ).hexdigest()


def microbatch_id(
    shard_ingestion_id: str, batch_index: int, instrument_id: str
) -> str:
    return hashlib.sha256(
        f"{shard_ingestion_id}|batch:{batch_index}|{instrument_id}".encode()
    ).hexdigest()


class CsvShardImporter:
    """Import one bounded, pre-validated CSV row range through raw-bar authority."""

    def __init__(self, market_bar_repository: Any, *, batch_rows: int = 5000):
        self.market_bar_repository = market_bar_repository
        self.batch_rows = max(1, int(batch_rows))

    def import_shard(
        self,
        stream: TextIO,
        request: CsvImportRequest,
        manifest: CsvImportManifest,
        shard: dict[str, Any],
        *,
        raw_artifact_id: str,
        ingested_at: str,
        should_cancel: Any = None,
        on_progress: Callable[[int, int], None] | None = None,
    ) -> dict[str, Any]:
        row_start = int(shard["row_start"])
        row_end = int(shard["row_end"])
        if row_start < 0 or row_end <= row_start:
            raise ValueError("CSV shard row range is invalid")
        grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
        rows_read = 0
        rows_written = 0
        receipt_counts: dict[str, int] = defaultdict(int)
        batch_index = 0

        def flush() -> None:
            nonlocal grouped, rows_written, batch_index
            if not grouped:
                return
            batch_written = 0
            for instrument_id in sorted(grouped):
                ingestion_id = instrument_ingestion_id(
                    str(shard["ingestion_id"]), instrument_id
                )
                count = self.market_bar_repository.upsert_price_bars(
                    instrument_id,
                    request.interval,
                    request.adjustment,
                    request.source,
                    ingested_at,
                    grouped[instrument_id],
                    {
                        "raw_artifact_id": raw_artifact_id,
                        "ingestion_id": ingestion_id,
                        "batch_id": microbatch_id(
                            str(shard["ingestion_id"]),
                            batch_index,
                            instrument_id,
                        ),
                        "manifest_id": manifest.manifest_id,
                        "namespace": request.instruments.namespace,
                        "profile": request.profile.identity,
                    },
                )
                batch_written += int(count)
                receipt_counts[instrument_id] += int(count)
            rows_written += batch_written
            if on_progress is not None:
                on_progress(rows_read, rows_written)
            grouped = defaultdict(list)
            batch_index += 1

        for row_index, parsed in enumerate(iter_csv_bars(stream, request)):
            if (
                should_cancel is not None
                and row_index % 1000 == 0
                and should_cancel()
            ):
                raise CsvImportCanceled("CSV import cancellation requested")
            if row_index < row_start:
                continue
            if row_index >= row_end:
                break
            assert isinstance(parsed, ParsedCsvBar)
            grouped[parsed.instrument_id].append(dict(parsed.bar))
            rows_read += 1
            if rows_read % self.batch_rows == 0:
                flush()
        if rows_read != row_end - row_start:
            raise RuntimeError("CSV shard ended before its declared row range")
        flush()
        receipts = [
            {
                "instrument_id": instrument_id,
                "ingestion_id": instrument_ingestion_id(
                    str(shard["ingestion_id"]), instrument_id
                ),
                "rows": count,
            }
            for instrument_id, count in sorted(receipt_counts.items())
        ]
        return {
            "rows_read": rows_read,
            "rows_written": rows_written,
            "receipts": receipts,
        }

    def import_archived_shard(
        self,
        storage_path: Path,
        request: CsvImportRequest,
        manifest: CsvImportManifest,
        shard: dict[str, Any],
        *,
        raw_artifact_id: str,
        ingested_at: str,
        should_cancel: Any = None,
        on_progress: Callable[[int, int], None] | None = None,
    ) -> dict[str, Any]:
        with storage_path.open(
            "r", encoding=request.profile.encoding, newline=""
        ) as stream:
            return self.import_shard(
                stream, request, manifest, shard,
                raw_artifact_id=raw_artifact_id,
                ingested_at=ingested_at,
                should_cancel=should_cancel,
                on_progress=on_progress,
            )
