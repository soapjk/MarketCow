from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from typing import Any, Dict, List
from zoneinfo import ZoneInfo

from .price_adjustment import PriceAdjustmentContract


class AdjustmentBackfillService:
    """Conservative legacy-row repair. Ambiguous adjusted data is never guessed."""

    def __init__(self, repository: Any) -> None:
        self.repository = repository

    @staticmethod
    def _trade_date(row: Dict[str, Any]) -> str:
        value = row["bar_time"]
        parsed = value if isinstance(value, datetime) else datetime.fromisoformat(
            str(value).replace("Z", "+00:00")
        )
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(ZoneInfo("Asia/Shanghai")).date().isoformat()

    @staticmethod
    def _utc_iso(value: Any) -> str:
        parsed = value if isinstance(value, datetime) else datetime.fromisoformat(
            str(value).replace("Z", "+00:00")
        )
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc).isoformat(timespec="milliseconds")

    @staticmethod
    def _factor_symbol(symbol: str) -> str:
        suffixes = {
            ".SH": ".XSHG", ".SZ": ".XSHE", ".BJ": ".XBSE",
        }
        for suffix, mic in suffixes.items():
            if symbol.endswith(suffix):
                return symbol[:-len(suffix)] + mic
        return symbol

    def plan(self, limit: int = 10000) -> Dict[str, Any]:
        candidates = self.repository.list_adjustment_contract_candidates(limit)
        repaired: List[Dict[str, Any]] = []
        quarantined: List[Dict[str, Any]] = []
        factor_cache: Dict[tuple[str, str, str], Dict[str, Any]] = {}
        for row in candidates:
            identity = {
                "symbol": str(row["symbol"]), "interval": str(row["interval"]),
                "adjustment": str(row["adjustment"]),
                "bar_time": str(row["bar_time"]), "source": str(row["source"]),
            }
            if row["adjustment"] == "adjusted":
                quarantined.append({
                    **identity, "reason": "legacy_adjusted_is_ambiguous",
                })
                continue
            if row["adjustment"] != "raw":
                quarantined.append({
                    **identity, "reason": "unsupported_adjustment_value",
                })
                continue
            if str(row.get("market") or "") == "CRYPTO":
                contract = PriceAdjustmentContract(
                    adjustment="raw", factor_applicability="not_applicable",
                    applied_adjustment_multiplier="1",
                )
            elif "tushare" in str(row["source"]):
                trade_date = self._trade_date(row)
                key = (
                    self._factor_symbol(str(row["symbol"])),
                    trade_date,
                    str(row["source"]),
                )
                if key not in factor_cache:
                    factors = self.repository.get_adjustment_factors(
                        key[0], trade_date, trade_date, key[2]
                    )
                    factor_cache[key] = factors[0] if factors else {}
                factor = factor_cache[key]
                if not factor:
                    quarantined.append({
                        **identity, "reason": "daily_factor_missing",
                    })
                    continue
                contract = PriceAdjustmentContract(
                    adjustment="raw", factor_applicability="applicable",
                    corporate_action_factor=str(factor["adjustment_factor"]),
                    applied_adjustment_multiplier="1",
                    factor_source=str(factor["source"]),
                    factor_artifact_id=str(factor["raw_artifact_id"]),
                    factor_as_of=str(factor["ingested_at"]),
                )
            else:
                quarantined.append({
                    **identity, "reason": "factor_semantics_not_proven",
                })
                continue
            repaired.append({
                **row,
                **contract.model_dump(),
                "adjustment_factor": (
                    contract.corporate_action_factor
                    if contract.factor_applicability == "applicable"
                    else row.get("adjustment_factor")
                ),
            })
        digest = hashlib.sha256(
            ("adjustment-backfill-v2:" + repr(sorted(
                (row["symbol"], row["interval"], str(row["bar_time"]), row["source"])
                for row in repaired
            ))).encode()
        ).hexdigest()[:24]
        return {
            "schema": "marketcow.adjustment-backfill-plan.v1",
            "scanned": len(candidates), "repairable": len(repaired),
            "quarantined": len(quarantined), "rows": repaired,
            "quarantine": quarantined,
            "batch_id": f"adjustment-backfill-{digest}",
        }

    def run(self, limit: int = 10000, apply: bool = False) -> Dict[str, Any]:
        plan = self.plan(limit)
        if not apply or not plan["rows"]:
            return {**plan, "applied": False, "written": 0}
        now = datetime.now(timezone.utc).isoformat(timespec="milliseconds")
        rows = []
        for row in plan["rows"]:
            rows.append({
                **row,
                "bar_time": self._utc_iso(row["bar_time"]),
                "ingested_at": now,
                "observed_at": self._utc_iso(row["observed_at"]),
                "factor_as_of": (
                    None if row.get("factor_as_of") is None
                    else self._utc_iso(row["factor_as_of"])
                ),
                "ingestion_id": plan["batch_id"],
            })
            rows[-1].pop("content_rank", None)
            rows[-1].pop("content_version", None)
        written = self.repository.insert_raw_bars(
            rows, batch_id=plan["batch_id"]
        )
        return {
            **plan, "rows": [], "applied": True, "written": written,
        }
