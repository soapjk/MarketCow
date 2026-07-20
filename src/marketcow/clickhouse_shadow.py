from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence

from .clickhouse_writer import ReliableClickHouseWriter, normalize_bar


def _market(symbol: str, provenance: Dict[str, Any]) -> str:
    if provenance.get("market"):
        return str(provenance["market"])
    if symbol.endswith((".SH", ".SZ", ".BJ")):
        return "CN"
    if symbol.endswith(".HK"):
        return "HK"
    return "US"


class ShadowMarketBarRepository:
    """DuckDB-primary shadow adapter with an opt-in canonical history read."""

    def __init__(self, primary: Any, writer: ReliableClickHouseWriter,
                 canonical_builder: Any = None,
                 canonical_reads_enabled: bool = False) -> None:
        self.primary = primary
        self.writer = writer
        self.canonical_builder = canonical_builder
        self.canonical_reads_enabled = canonical_reads_enabled
        self._last_batch: Optional[Dict[str, Any]] = None
        self._last_shadow: Dict[str, Any] = {"status": "idle"}
        self._last_reconciliation: Dict[str, Any] = {"status": "not_run"}
        self._last_read: Dict[str, Any] = {
            "backend": "duckdb", "fallback": False, "status": "not_run"
        }

    def upsert_quote(self, row: Dict[str, Any]) -> None:
        self.primary.upsert_quote(row)

    def get_latest_quotes(self, symbols: Sequence[str]) -> List[Dict[str, Any]]:
        return self.primary.get_latest_quotes(symbols)

    def get_price_bars(
        self, symbol: str, interval: str, adjustment: str, limit: int
    ) -> List[Dict[str, Any]]:
        if not self.canonical_reads_enabled:
            self._last_read = {
                "backend": "duckdb", "fallback": False, "status": "ok"
            }
            return self.primary.get_price_bars(symbol, interval, adjustment, limit)
        try:
            rows = self.writer.repository.get_canonical_price_bars(
                symbol, interval, adjustment, limit
            )
            self._last_read = {
                "backend": "clickhouse_canonical", "fallback": False,
                "status": "ok", "count": len(rows),
            }
            return rows
        except Exception as error:
            rows = self.primary.get_price_bars(symbol, interval, adjustment, limit)
            self._last_read = {
                "backend": "duckdb", "attempted_backend": "clickhouse_canonical",
                "fallback": True, "status": "fallback", "count": len(rows),
                "error": str(error)[:4000],
            }
            return rows

    def get_price_bars_range(
        self, symbol: str, interval: str, adjustment: str,
        start: str, end: str, limit: int,
    ) -> tuple[List[Dict[str, Any]], bool]:
        if not self.canonical_reads_enabled:
            rows, truncated = self.primary.get_price_bars_range(
                symbol, interval, adjustment, start, end, limit
            )
            self._last_read = {
                "backend": "duckdb", "fallback": False, "status": "ok",
                "count": len(rows), "truncated": truncated, "range": True,
            }
            return rows, truncated
        try:
            rows, truncated = self.writer.repository.get_canonical_price_bars_range(
                symbol, interval, adjustment, start, end, limit
            )
            self._last_read = {
                "backend": "clickhouse_canonical", "fallback": False,
                "status": "ok", "count": len(rows), "truncated": truncated,
                "range": True,
            }
            return rows, truncated
        except Exception as error:
            rows, truncated = self.primary.get_price_bars_range(
                symbol, interval, adjustment, start, end, limit
            )
            self._last_read = {
                "backend": "duckdb", "attempted_backend": "clickhouse_canonical",
                "fallback": True, "status": "fallback", "count": len(rows),
                "truncated": truncated, "range": True, "error": str(error)[:4000],
            }
            return rows, truncated

    def get_price_bars_cross_section(
        self, interval: str, adjustment: str, bar_at: str, limit: int,
        symbols: Optional[Sequence[str]] = None,
    ) -> tuple[List[Dict[str, Any]], bool]:
        if not self.canonical_reads_enabled:
            rows, truncated = self.primary.get_price_bars_cross_section(
                interval, adjustment, bar_at, limit, symbols
            )
            self._last_read = {
                "backend": "duckdb", "fallback": False, "status": "ok",
                "count": len(rows), "truncated": truncated, "cross_section": True,
            }
            return rows, truncated
        try:
            rows, truncated = self.writer.repository.get_canonical_price_bars_cross_section(
                interval, adjustment, bar_at, limit,
                None if symbols is None else list(symbols),
            )
            self._last_read = {
                "backend": "clickhouse_canonical", "fallback": False,
                "status": "ok", "count": len(rows), "truncated": truncated,
                "cross_section": True,
            }
            return rows, truncated
        except Exception as error:
            rows, truncated = self.primary.get_price_bars_cross_section(
                interval, adjustment, bar_at, limit, symbols
            )
            self._last_read = {
                "backend": "duckdb", "attempted_backend": "clickhouse_canonical",
                "fallback": True, "status": "fallback", "count": len(rows),
                "truncated": truncated, "cross_section": True,
                "error": str(error)[:4000],
            }
            return rows, truncated
    @staticmethod
    def _raw_rows(
        symbol: str, interval: str, adjustment: str, source: str,
        ingested_at: str, bars: List[Dict[str, Any]], provenance: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        observed_at = provenance.get("observed_at") or ingested_at
        return [normalize_bar("raw", {
            "symbol": symbol, "market": _market(symbol, provenance),
            "interval": interval, "adjustment": adjustment,
            "bar_time": bar.get("bar_at") or datetime.fromtimestamp(
                int(bar["timestamp"]), timezone.utc
            ),
            "open": bar.get("open"), "high": bar.get("high"), "low": bar.get("low"),
            "close": bar.get("close"), "raw_close": bar.get("raw_close"),
            "adjustment_factor": bar.get("adjustment_factor"),
            "volume": bar.get("volume"),
            "amount": bar.get("amount"), "source": source,
            "source_sequence": str(bar.get("timestamp")),
            "observed_at": observed_at, "ingested_at": ingested_at,
            "raw_artifact_id": provenance.get("raw_artifact_id"),
        }) for bar in bars]

    def upsert_price_bars(
        self, symbol: str, interval: str, adjustment: str, source: str,
        ingested_at: str, bars: List[Dict[str, Any]],
        provenance: Optional[Dict[str, Any]] = None,
    ) -> int:
        provenance = provenance or {}
        count = self.primary.upsert_price_bars(
            symbol, interval, adjustment, source, ingested_at, bars, provenance
        )
        self._last_batch = None
        try:
            rows = self._raw_rows(
                symbol, interval, adjustment, source, ingested_at, bars, provenance
            )
            result = self.writer.write("raw", rows)
            self._last_batch = {
                "symbol": symbol, "interval": interval, "adjustment": adjustment,
                "source": source, "timestamps": [int(bar["timestamp"]) for bar in bars],
                "bar_times": [row["bar_time"] for row in rows],
            }
            self._last_shadow = {"status": "ok" if not result["spooled"] else "spooled",
                                 **result}
        except Exception as error:
            self._last_shadow = {"status": "error", "error": str(error)[:4000]}
        return count

    @staticmethod
    def _key(row: Dict[str, Any]) -> tuple[Any, ...]:
        bar_time = row.get("bar_time") or row.get("bar_at")
        if isinstance(bar_time, datetime):
            if bar_time.tzinfo is None:
                bar_time = bar_time.replace(tzinfo=timezone.utc)
            bar_time = bar_time.astimezone(timezone.utc).isoformat(timespec="milliseconds")
        else:
            bar_time = str(bar_time).replace("Z", "+00:00")
            parsed = datetime.fromisoformat(bar_time)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            bar_time = parsed.astimezone(timezone.utc).isoformat(timespec="milliseconds")
        return (row["symbol"], row["interval"], row["adjustment"], bar_time, row["source"])

    def reconcile_last_write(self, mismatch_limit: int = 100) -> Dict[str, Any]:
        if not self._last_batch:
            return {"status": "not_run", "reason": "no successful primary batch"}
        if not 1 <= mismatch_limit <= 1000:
            raise ValueError("mismatch limit must be between 1 and 1000")
        batch = self._last_batch
        try:
            duck = self.primary.get_price_bars_for_reconciliation(
                batch["symbol"], batch["interval"], batch["adjustment"],
                batch["source"], batch["timestamps"],
            )
            click = self.writer.repository.query_raw_batch(
                batch["symbol"], batch["interval"], batch["adjustment"],
                batch["source"], batch["bar_times"],
            )
            duck_map = {self._key(row): row for row in duck}
            click_map = {self._key(row): row for row in click}
            keys = sorted(set(duck_map) | set(click_map))
            mismatches = []
            fields = ("open", "high", "low", "close", "volume", "amount")
            for key in keys:
                left, right = duck_map.get(key), click_map.get(key)
                changed = [field for field in fields if left is None or right is None
                           or left.get(field) != right.get(field)]
                if changed and len(mismatches) < mismatch_limit:
                    mismatches.append({"key": key, "fields": changed,
                                       "duckdb": left, "clickhouse": right})
            all_mismatch_count = sum(
                1 for key in keys if duck_map.get(key) is None or click_map.get(key) is None
                or any(duck_map[key].get(field) != click_map[key].get(field)
                       for field in fields)
            )
            duck_times = [key[3] for key in duck_map]
            click_times = [key[3] for key in click_map]
            def utc_time(value: Any) -> datetime:
                parsed = value if isinstance(value, datetime) else datetime.fromisoformat(
                    str(value).replace("Z", "+00:00")
                )
                if parsed.tzinfo is None:
                    parsed = parsed.replace(tzinfo=timezone.utc)
                return parsed.astimezone(timezone.utc)

            duck_ingested = [utc_time(row["ingested_at"]) for row in duck
                             if row.get("ingested_at")]
            click_ingested = [utc_time(row["ingested_at"]) for row in click
                              if row.get("ingested_at")]
            lag = None
            if duck_ingested and click_ingested:
                lag = abs((max(duck_ingested) - max(click_ingested)).total_seconds())
            result = {
                "status": "consistent" if not all_mismatch_count else "mismatch",
                "duckdb_count": len(duck_map), "clickhouse_count": len(click_map),
                "duckdb_time_min": min(duck_times) if duck_times else None,
                "duckdb_time_max": max(duck_times) if duck_times else None,
                "clickhouse_time_min": min(click_times) if click_times else None,
                "clickhouse_time_max": max(click_times) if click_times else None,
                "ingestion_lag_seconds": lag,
                "mismatch_count": all_mismatch_count, "mismatches": mismatches,
                "mismatches_truncated": all_mismatch_count > len(mismatches),
                "shadow": self._last_shadow,
                "spool": self.writer.spool.diagnostics(),
            }
        except Exception as error:
            result = {"status": "error", "error": str(error)[:4000],
                      "shadow": self._last_shadow}
        self._last_reconciliation = result
        return result

    def diagnostics(self) -> Dict[str, Any]:
        return {"shadow": self._last_shadow, "reconciliation": self._last_reconciliation,
                "read": self._last_read,
                "canonical": (self.canonical_builder.last_diagnostics
                              if self.canonical_builder else {"status": "disabled"}),
                "spool": self.writer.spool.diagnostics()}

    def rebuild_canonical(
        self, symbol: str, interval: str, adjustment: str,
        start: Any, end: Any, limit: int = 50000,
    ) -> Dict[str, Any]:
        if self.canonical_builder is None:
            return {"status": "disabled", "written": 0, "spooled": 0}
        return self.canonical_builder.rebuild(
            symbol, interval, adjustment, start, end, limit
        )
