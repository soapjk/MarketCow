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
        ingestion_shards = {}
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
                ingestion_shards[ingestion_id] = shard.get("shard_index")
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
        manifest = dict(job.get("request_json", {}).get("manifest") or {})
        dry_run = dict(job.get("request_json", {}).get("dry_run_report") or {})
        if dry_run:
            if int(dry_run.get("rows_valid") or 0) != expected_rows:
                failures.append({
                    "code": "manifest_row_count_mismatch",
                    "manifest_rows": int(dry_run.get("rows_valid") or 0),
                    "receipt_rows": expected_rows,
                })
            for metric in ("rows_invalid", "duplicate_rows"):
                if int(dry_run.get(metric) or 0):
                    failures.append({
                        "code": f"preimport_{metric}_nonzero",
                        "actual": int(dry_run[metric]),
                    })
        if (
            coverage.get("first_bar_at_ms") is not None
            and manifest.get("first_bar_at") is not None
            and manifest.get("last_bar_at") is not None
        ):
            actual_first = _iso_from_milliseconds(coverage["first_bar_at_ms"])
            actual_last = _iso_from_milliseconds(coverage["last_bar_at_ms"])
            if actual_first != manifest.get("first_bar_at"):
                failures.append({
                    "code": "first_bar_at_mismatch",
                    "expected": manifest.get("first_bar_at"),
                    "actual": actual_first,
                })
            if actual_last != manifest.get("last_bar_at"):
                failures.append({
                    "code": "last_bar_at_mismatch",
                    "expected": manifest.get("last_bar_at"),
                    "actual": actual_last,
                })
        invalid_canonical = int(
            coverage.get("canonical_invalid_ohlc_rows") or 0
        )
        if invalid_canonical:
            failures.append({
                "code": "canonical_ohlc_invalid",
                "actual": invalid_canonical,
            })
        warnings = list(dry_run.get("warnings") or ())
        abnormal_canonical = int(
            coverage.get("canonical_abnormal_price_rows") or 0
        )
        if abnormal_canonical:
            warnings.append({
                "code": "canonical_abnormal_price_ratio",
                "count": abnormal_canonical,
                "threshold": 100,
                "policy": "warning",
            })
        shard_diagnostics = []
        if ingestion_ids and hasattr(
            self.market_bar_repository, "get_canonical_ingestion_quality"
        ):
            shard_diagnostics = list(
                self.market_bar_repository.get_canonical_ingestion_quality(
                    ingestion_ids
                )
            )
            found = {
                str(value["ingestion_id"]) for value in shard_diagnostics
            }
            for ingestion_id in sorted(set(ingestion_ids) - found):
                failures.append({
                    "code": "ingestion_quality_missing",
                    "ingestion_id": ingestion_id,
                    "shard_index": ingestion_shards.get(ingestion_id),
                })
            for diagnostic in shard_diagnostics:
                diagnostic["shard_index"] = ingestion_shards.get(
                    str(diagnostic["ingestion_id"])
                )
                if int(diagnostic["canonical_rows"]) != int(
                    diagnostic["raw_rows"]
                ):
                    failures.append({
                        "code": "shard_canonical_coverage_incomplete",
                        **diagnostic,
                    })
                if int(diagnostic["canonical_invalid_ohlc_rows"]):
                    failures.append({
                        "code": "shard_canonical_ohlc_invalid",
                        **diagnostic,
                    })
        return {
            "schema": "marketcow.csv-import-quality.v1",
            "job_id": job["job_id"],
            "manifest_id": job["manifest_id"],
            "status": "passed" if not failures else "failed",
            "expected_rows": expected_rows,
            "raw_receipts": len(receipt_rows),
            "shard_diagnostics": shard_diagnostics,
            "coverage": coverage,
            "checks": {
                "ohlc_and_finite_values": "preimport-validated",
                "duplicate_keys": "exact-raw-to-canonical-key-coverage",
                "time_range": "manifest-compared",
                "intraday_gaps": int(dry_run.get("gap_count") or 0),
                "abnormal_price_rows": int(
                    dry_run.get("abnormal_price_rows") or 0
                ),
                "trading_calendar": (
                    "profile-enforced"
                    if (
                        job.get("request_json", {})
                        .get("request", {})
                        .get("profile", {})
                        .get("trading_sessions")
                    )
                    else "warning-not-configured"
                ),
            },
            "warnings": warnings,
            "failures": failures,
        }


def _iso_from_milliseconds(value: Any) -> str:
    from datetime import datetime, timezone

    return datetime.fromtimestamp(
        int(value) / 1000, timezone.utc
    ).isoformat()
