from __future__ import annotations

import hashlib
import json
import math
from collections import Counter, defaultdict
from datetime import datetime
from typing import Any, Dict, Iterable, List, Sequence

from .clickhouse_repositories import ClickHouseMarketBarRepository
from .clickhouse_writer import ReliableClickHouseWriter, normalize_bar
from .canonical_selection import (
    DEFAULT_SOURCE_PRIORITY,
    canonical_selection_key,
    utc_datetime,
)


VALUE_FIELDS = ("open", "high", "low", "close", "volume", "amount")
CONTRACT_FIELDS = ("raw_close", "adjustment_factor")


def _utc(value: Any) -> datetime:
    return utc_datetime(value)


def _iso(value: Any) -> str:
    return _utc(value).isoformat(timespec="milliseconds")


class CanonicalMarketBarBuilder:
    """Bounded ClickHouse raw FINAL to ClickHouse canonical builder."""

    def __init__(
        self, repository: ClickHouseMarketBarRepository,
        writer: ReliableClickHouseWriter,
        source_priority: Sequence[str] = DEFAULT_SOURCE_PRIORITY,
        rel_tol: float = 1e-6, abs_tol: float = 1e-9,
    ) -> None:
        self.repository = repository
        self.writer = writer
        self.priority = {source: index for index, source in enumerate(source_priority)}
        self.rel_tol = rel_tol
        self.abs_tol = abs_tol
        self.last_diagnostics: Dict[str, Any] = {"status": "not_run"}

    @staticmethod
    def _group_key(row: Dict[str, Any]) -> tuple[str, str, str, str]:
        return (row["symbol"], row["interval"], row["adjustment"], _iso(row["bar_time"]))

    def _selection_key(self, row: Dict[str, Any]) -> tuple[Any, ...]:
        ordered = tuple(source for source, _ in sorted(
            self.priority.items(), key=lambda item: item[1]
        ))
        return canonical_selection_key(row, ordered)

    def _fingerprint(self, rows: Iterable[Dict[str, Any]]) -> str:
        fields = ("symbol", "market", "interval", "adjustment", "bar_time", *VALUE_FIELDS,
                  *CONTRACT_FIELDS,
                  "source", "source_sequence", "observed_at", "ingested_at",
                  "raw_artifact_id")
        normalized = []
        for row in rows:
            item = {key: row.get(key) for key in fields}
            for key in ("bar_time", "observed_at", "ingested_at"):
                item[key] = _iso(item[key])
            normalized.append(item)
        encoded = json.dumps(sorted(normalized, key=lambda item: json.dumps(
            item, sort_keys=True, default=str)), sort_keys=True, separators=(",", ":"),
            default=str).encode()
        return hashlib.sha256(encoded).hexdigest()

    def _consistent(self, selected: Dict[str, Any], other: Dict[str, Any]) -> bool:
        for field in VALUE_FIELDS:
            left, right = selected.get(field), other.get(field)
            if left is None or right is None:
                if left is not right:
                    return False
            elif not math.isclose(float(left), float(right), rel_tol=self.rel_tol,
                                  abs_tol=self.abs_tol):
                return False
        return True

    def build_rows(
        self, raw_rows: List[Dict[str, Any]], existing_rows: List[Dict[str, Any]],
    ) -> tuple[List[Dict[str, Any]], Counter[str], Counter[str]]:
        groups: Dict[tuple[str, str, str, str], List[Dict[str, Any]]] = defaultdict(list)
        for row in raw_rows:
            groups[self._group_key(row)].append(row)
        existing = {self._group_key(row): row for row in existing_rows}
        quality_counts: Counter[str] = Counter()
        selected_counts: Counter[str] = Counter()
        canonical = []
        for key in sorted(groups):
            rows = groups[key]
            selected = min(rows, key=self._selection_key)
            sources = {str(row["source"]) for row in rows}
            if len(sources) == 1:
                quality = "single_source"
            elif all(self._consistent(selected, row) for row in rows):
                quality = "multi_source_consistent"
            else:
                quality = "multi_source_ohlcva_difference"
            fingerprint = self._fingerprint(rows)
            old = existing.get(key)
            version = int(old.get("version", 0)) if old and old.get(
                "input_fingerprint"
            ) == fingerprint else int(old.get("version", 0) if old else 0) + 1
            updated_at = max(_utc(row["ingested_at"]) for row in rows)
            row = normalize_bar("canonical", {
                **{field: selected.get(field) for field in (
                    "symbol", "market", "interval", "adjustment", "bar_time", *VALUE_FIELDS,
                    *CONTRACT_FIELDS,
                    "observed_at", "ingested_at", "raw_artifact_id")},
                "selected_source": selected["source"], "source_count": len(sources),
                "quality_status": quality, "input_fingerprint": fingerprint,
                "version": version, "updated_at": updated_at,
            })
            canonical.append(row)
            quality_counts[quality] += 1
            selected_counts[str(selected["source"])] += 1
        return canonical, quality_counts, selected_counts

    def rebuild(
        self, symbol: str, interval: str, adjustment: str,
        start: Any, end: Any, limit: int = 50000,
    ) -> Dict[str, Any]:
        try:
            if _utc(start) > _utc(end):
                raise ValueError("canonical range start must not be after end")
            raw, truncated = self.repository.query_range(
                "raw", symbol, interval, adjustment, start, end, limit
            )
            if truncated:
                result = {"status": "truncated", "scanned_rows": len(raw),
                          "scanned_groups": 0, "written": 0, "spooled": 0,
                          "quality_counts": {}, "selected_source_counts": {},
                          "truncated": True}
            else:
                existing, _ = self.repository.query_range(
                    "canonical", symbol, interval, adjustment, start, end, limit
                )
                rows, qualities, sources = self.build_rows(raw, existing)
                write = self.writer.write("canonical", rows)
                complete = bool(write.get("acknowledged") and write.get("verified"))
                result = {"status": "ok" if complete else write["status"],
                          "scanned_rows": len(raw), "scanned_groups": len(rows),
                          "written": write["written"], "spooled": write["spooled"],
                          "quality_counts": dict(qualities),
                          "selected_source_counts": dict(sources), "truncated": False}
        except Exception as error:
            result = {"status": "error", "error": str(error)[:4000], "written": 0,
                      "spooled": 0, "truncated": False}
        self.last_diagnostics = result
        return result
