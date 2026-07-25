from __future__ import annotations

import tempfile
import time
import unittest
from datetime import datetime
from pathlib import Path

from marketcow.csv_import import (
    CsvImportRequest,
    CsvSchemaProfile,
    InstrumentMapping,
)
from marketcow.csv_import_service import CsvImportService
from tests.test_csv_import_jobs import MemoryRepository


class Artifacts:
    def __init__(self):
        self.rows = []

    def save_artifact(self, row):
        self.rows.append(row)


class Bars:
    def __init__(self):
        self.receipts = {}
        self.canonical = False

    def upsert_price_bars(
        self, symbol, interval, adjustment, source, ingested_at, bars, provenance
    ):
        timestamps = [
            int(datetime.fromisoformat(row["bar_at"]).timestamp() * 1000)
            for row in bars
        ]
        self.receipts[provenance["ingestion_id"]] = {
            "ingestion_id": provenance["ingestion_id"],
            "row_count": len(bars),
            "raw_artifact_id": provenance["raw_artifact_id"],
            "first_bar_at_ms": min(timestamps),
            "last_bar_at_ms": max(timestamps),
        }
        return len(bars)

    def get_raw_ingestion_receipt(self, ingestion_id):
        return self.receipts.get(ingestion_id)

    def get_canonical_ingestion_coverage(self, ingestion_ids):
        count = sum(self.receipts[value]["row_count"] for value in ingestion_ids)
        return {
            "raw_rows": count,
            "canonical_rows": count if self.canonical else 0,
            "first_bar_at_ms": min(
                self.receipts[value]["first_bar_at_ms"]
                for value in ingestion_ids
            ),
            "last_bar_at_ms": max(
                self.receipts[value]["last_bar_at_ms"]
                for value in ingestion_ids
            ),
        }


class Builder:
    def __init__(self, bars):
        self.bars = bars
        self.calls = []

    def rebuild(self, *args):
        self.calls.append(args)
        self.bars.canonical = True
        return {"status": "success"}


def declaration():
    return CsvImportRequest(
        "vendor", "1m", "raw",
        CsvSchemaProfile(
            name="vendor", version="1",
            columns={
                "symbol": "symbol", "timestamp": "time", "open": "open",
                "high": "high", "low": "low", "close": "close",
                "volume": "volume",
            },
            timezone_name="UTC",
        ),
        InstrumentMapping("provider:vendor", {"AAPL.US": "AAPL.XNAS"}),
    )


class CsvImportServiceTest(unittest.TestCase):
    def test_one_service_drives_dry_run_archive_job_canonical_and_quality(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            source = root / "vendor.csv"
            source.write_text(
                "symbol,time,open,high,low,close,volume\n"
                "AAPL.US,2026-01-01T14:30:00Z,1,2,0.5,1.5,10\n"
            )
            bars = Bars()
            artifacts = Artifacts()
            builder = Builder(bars)
            metadata = MemoryRepository()
            service = CsvImportService(
                allowed_root=root, storage_root=root / "storage",
                metadata_repository=metadata,
                market_bar_repository=bars, artifact_store=artifacts,
                canonical_builder=builder,
            )
            try:
                report = service.dry_run(source, declaration())
                self.assertEqual(report["status"], "valid")
                job, created = service.create_import(
                    source, declaration(), idempotency_key="stable-key",
                    chunk_rows=1,
                )
                self.assertTrue(created)
                deadline = time.monotonic() + 2
                while time.monotonic() < deadline:
                    completed = service.get(job["job_id"])
                    if completed["status"] in {"succeeded", "failed"}:
                        break
                    time.sleep(0.01)
                self.assertEqual(completed["status"], "succeeded")
                self.assertEqual(
                    completed["quality_report_json"]["status"], "passed"
                )
                self.assertEqual(len(builder.calls), 1)
                self.assertEqual(len(artifacts.rows), 1)
                self.assertTrue(Path(
                    artifacts.rows[0]["storage_path"]
                ).is_relative_to(root.resolve()))
                self.assertEqual(
                    service.manifest(job["job_id"])["manifest_id"],
                    completed["manifest_id"],
                )
                self.assertEqual(
                    service.quality_report(job["job_id"])["status"], "passed"
                )
                metadata.jobs[job["job_id"]]["status"] = "failed"
                retried, created = service.retry(
                    job["job_id"], idempotency_key="stable-retry"
                )
                self.assertTrue(created)
                self.assertNotEqual(retried["job_id"], job["job_id"])
                self.assertEqual(
                    retried["manifest_id"], completed["manifest_id"]
                )
            finally:
                service.close()

    def test_path_outside_allowed_root_is_rejected(self):
        with tempfile.TemporaryDirectory() as allowed, tempfile.TemporaryDirectory() as other:
            path = Path(other) / "bars.csv"
            path.write_text("x")
            service = CsvImportService(
                allowed_root=Path(allowed), storage_root=Path(allowed) / "storage",
                metadata_repository=MemoryRepository(),
                market_bar_repository=Bars(), artifact_store=Artifacts(),
            )
            try:
                with self.assertRaisesRegex(ValueError, "ALLOWED_ROOT"):
                    service.dry_run(path, declaration())
            finally:
                service.close()

    def test_empty_csv_is_not_accepted_as_a_successful_import(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            source = root / "empty.csv"
            source.write_text(
                "symbol,time,open,high,low,close,volume\n",
                encoding="utf-8",
            )
            service = CsvImportService(
                allowed_root=root, storage_root=root / "storage",
                metadata_repository=MemoryRepository(),
                market_bar_repository=Bars(), artifact_store=Artifacts(),
            )
            try:
                with self.assertRaisesRegex(ValueError, "no valid bars"):
                    service.create_import(
                        source, declaration(), idempotency_key="empty"
                    )
            finally:
                service.close()

    def test_file_size_limit_is_enforced_before_parsing(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            source = root / "too-large.csv"
            source.write_text("more than ten bytes", encoding="utf-8")
            service = CsvImportService(
                allowed_root=root, storage_root=root / "storage",
                metadata_repository=MemoryRepository(),
                market_bar_repository=Bars(), artifact_store=Artifacts(),
                max_file_bytes=10,
            )
            try:
                with self.assertRaisesRegex(ValueError, "size limit"):
                    service.dry_run(source, declaration())
            finally:
                service.close()


if __name__ == "__main__":
    unittest.main()
