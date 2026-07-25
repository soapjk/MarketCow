from __future__ import annotations

from typing import Any


class CsvImportQualityVerifier:
    """Prove raw receipts and canonical coverage for one completed import job."""

    def __init__(self, market_bar_repository: Any):
        self.market_bar_repository = market_bar_repository

    def verify(self, job: dict[str, Any]) -> dict[str, Any]:
        if str(job.get("status")) != "succeeded":
            raise ValueError("CSV import quality requires a succeeded job")
        shards = list(job.get("shards") or ())
        receipt_rows = []
        failures = []
        ingestion_ids = []
        expected_rows = 0
        for shard in shards:
            receipt = shard.get("write_receipt_json") or {}
            for item in receipt.get("receipts") or ():
                ingestion_id = str(item.get("ingestion_id") or "")
                expected = int(item.get("rows") or 0)
                if not ingestion_id or expected < 0:
                    failures.append({
                        "code": "write_receipt_invalid",
                        "shard_index": shard.get("shard_index"),
                    })
                    continue
                durable = self.market_bar_repository.get_raw_ingestion_receipt(
                    ingestion_id
                )
                if durable is None:
                    failures.append({
                        "code": "raw_receipt_missing",
                        "ingestion_id": ingestion_id,
                    })
                    continue
                actual = int(durable["row_count"])
                if actual != expected:
                    failures.append({
                        "code": "raw_row_count_mismatch",
                        "ingestion_id": ingestion_id,
                        "expected": expected,
                        "actual": actual,
                    })
                expected_artifact = str(job.get("raw_artifact_id") or "")
                actual_artifact = str(durable.get("raw_artifact_id") or "")
                if expected_artifact and actual_artifact != expected_artifact:
                    failures.append({
                        "code": "raw_artifact_mismatch",
                        "ingestion_id": ingestion_id,
                    })
                ingestion_ids.append(ingestion_id)
                expected_rows += expected
                receipt_rows.append(durable)
        if not ingestion_ids and int(job.get("rows_written") or 0) > 0:
            failures.append({"code": "write_receipts_missing"})
            coverage = {
                "raw_rows": 0, "canonical_rows": 0,
                "first_bar_at_ms": None, "last_bar_at_ms": None,
            }
        elif ingestion_ids:
            coverage = self.market_bar_repository.get_canonical_ingestion_coverage(
                ingestion_ids
            )
            if int(coverage["raw_rows"]) != expected_rows:
                failures.append({
                    "code": "raw_coverage_mismatch",
                    "expected": expected_rows,
                    "actual": int(coverage["raw_rows"]),
                })
            if int(coverage["canonical_rows"]) != int(coverage["raw_rows"]):
                failures.append({
                    "code": "canonical_coverage_incomplete",
                    "raw_rows": int(coverage["raw_rows"]),
                    "canonical_rows": int(coverage["canonical_rows"]),
                })
        else:
            coverage = {
                "raw_rows": 0, "canonical_rows": 0,
                "first_bar_at_ms": None, "last_bar_at_ms": None,
            }
        return {
            "schema": "marketcow.csv-import-quality.v1",
            "job_id": job["job_id"],
            "manifest_id": job["manifest_id"],
            "status": "passed" if not failures else "failed",
            "expected_rows": expected_rows,
            "raw_receipts": len(receipt_rows),
            "coverage": coverage,
            "failures": failures,
        }
