from __future__ import annotations

import copy
import threading
from datetime import datetime, timezone
from typing import Any, Callable, Iterable, Mapping, Protocol

from .providers.eastmoney_realtime import normalize_a_symbol
from .providers.yahoo_quote import normalize_yahoo_symbol


CONTRACT_VERSION = "marketcow.exposure-facts.v1"
SUPPORTED_MARKETS = ("CN", "HK", "US")


def _utc(value: Any) -> datetime:
    parsed = value if isinstance(value, datetime) else datetime.fromisoformat(
        str(value).replace("Z", "+00:00")
    )
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def normalize_exposure_symbol(value: str) -> tuple[str, str]:
    try:
        symbol = normalize_a_symbol(value)
        return symbol, "CN"
    except ValueError:
        symbol, market = normalize_yahoo_symbol(value)
        if market not in {"HK", "US"}:
            raise ValueError("exposure facts support CN, HK and US securities")
        return symbol, market


class ExposureFactSource(Protocol):
    source_id: str
    source_tier: str

    def fetch(self, symbol: str, market: str) -> Mapping[str, Any] | None: ...


def evidence(
    source_id: str,
    *,
    fetched_at: str,
    effective_at: str,
    source_tier: str,
    source_url: str = "",
    source_record_id: str = "",
    confidence: str = "reported",
) -> dict[str, Any]:
    if not source_url and not source_record_id:
        raise ValueError("evidence requires source_url or source_record_id")
    return {
        "source_id": source_id,
        "source_url": source_url or None,
        "source_record_id": source_record_id or None,
        "fetched_at": _utc(fetched_at).isoformat(),
        "effective_at": _utc(effective_at).isoformat(),
        "source_tier": source_tier,
        "confidence": confidence,
    }


class RepositoryExposureFactSource:
    """Read only facts already persisted by MarketCow repositories."""

    source_id = "marketcow_repository"
    source_tier = "aggregated_primary"

    def __init__(self, market_bars: Any, fundamentals: Any):
        self.market_bars = market_bars
        self.fundamentals = fundamentals

    def fetch(self, symbol: str, market: str) -> Mapping[str, Any] | None:
        quotes = self.market_bars.get_latest_quotes([symbol])
        quote = quotes[0] if quotes else None
        code = symbol.split(".")[0]
        rows = (
            self.fundamentals.query_fundamentals(symbol=code, limit=1)
            if market == "CN" else []
        )
        fundamental = rows[0] if rows else None
        if quote is None and fundamental is None:
            return None
        observed = (
            (quote or {}).get("ingested_at")
            or (quote or {}).get("observed_at")
            or (fundamental or {}).get("ingested_at")
            or datetime.now(timezone.utc).isoformat()
        )
        source_url = (quote or {}).get("source_url") or ""
        provenance = evidence(
            str((quote or {}).get("source") or "marketcow_fundamental_snapshot"),
            source_url=source_url,
            source_record_id="" if source_url else (
                "fundamental_snapshot:"
                + str((fundamental or {}).get("report_period") or "latest")
                + ":" + code
            ),
            fetched_at=observed,
            effective_at=(quote or {}).get("quote_at") or observed,
            source_tier="primary_market_data" if quote else "primary_filing_aggregation",
        )
        classifications = []
        if fundamental and fundamental.get("industry"):
            classifications.append({
                "scheme": "eastmoney_basic_industry",
                "value": str(fundamental["industry"]),
                "level": None,
                "evidence": provenance,
            })
        asset_type = "equity" if fundamental else "unknown"
        return {
            "asset_type": asset_type,
            "listing_market": market,
            "currency": (quote or {}).get("currency"),
            "as_of": (quote or {}).get("quote_at") or observed,
            "classifications": classifications,
            "company_materials": [],
            "holdings": None,
            "evidence": provenance,
        }


class ExposureFactsService:
    """Priority merge, provenance validation and bounded stale-cache fallback."""

    def __init__(
        self,
        sources: Iterable[ExposureFactSource],
        *,
        ttl_seconds: float = 86400,
        stale_max_seconds: float = 604800,
        clock: Callable[[], datetime] | None = None,
    ):
        self.sources = tuple(sources)
        self.ttl_seconds = float(ttl_seconds)
        self.stale_max_seconds = float(stale_max_seconds)
        self.clock = clock or (lambda: datetime.now(timezone.utc))
        self._cache: dict[str, tuple[datetime, dict[str, Any]]] = {}
        self._lock = threading.Lock()

    def get(self, symbol: str, *, refresh: bool = False) -> dict[str, Any]:
        normalized, market = normalize_exposure_symbol(symbol)
        now = _utc(self.clock())
        cached = self._cached(normalized, now)
        if cached and not refresh and cached[0] <= self.ttl_seconds:
            return self._served(cached[1], "fresh", cached[0], now)

        fragments = []
        failures = []
        for source in self.sources:
            try:
                fragment = source.fetch(normalized, market)
                if fragment:
                    fragments.append(dict(fragment))
            except Exception as exc:
                failures.append({
                    "source_id": source.source_id,
                    "code": "source_unavailable",
                    "error_type": type(exc).__name__,
                })
        if not fragments:
            if cached and cached[0] <= self.stale_max_seconds:
                result = self._served(cached[1], "stale", cached[0], now)
                result["degradations"] = failures
                return result
            return self._unavailable(normalized, market, failures, now)

        result = self._merge(normalized, market, fragments, failures, now)
        with self._lock:
            self._cache[normalized] = (now, copy.deepcopy(result))
        return self._served(result, "refreshed", 0.0, now)

    def _cached(
        self, symbol: str, now: datetime
    ) -> tuple[float, dict[str, Any]] | None:
        with self._lock:
            item = self._cache.get(symbol)
        if not item:
            return None
        return max(0.0, (now - item[0]).total_seconds()), copy.deepcopy(item[1])

    def _merge(
        self, symbol: str, market: str, fragments: list[dict[str, Any]],
        failures: list[dict[str, Any]], now: datetime,
    ) -> dict[str, Any]:
        first = fragments[0]
        asset_type = next(
            (row.get("asset_type") for row in fragments
             if row.get("asset_type") not in (None, "", "unknown")),
            first.get("asset_type") or "unknown",
        )
        classifications = [
            item for row in fragments for item in row.get("classifications", [])
        ]
        materials = [
            item for row in fragments for item in row.get("company_materials", [])
        ]
        holdings = next(
            (row.get("holdings") for row in fragments if row.get("holdings") is not None),
            None,
        )
        if asset_type in {"etf", "fund"}:
            holdings = holdings or {
                "status": "unavailable",
                "reason": "no_constituent_source",
                "constituents": [],
                "as_of": None,
                "evidence": [],
            }
        else:
            holdings = {
                "status": "not_applicable",
                "reason": None,
                "constituents": [],
                "as_of": None,
                "evidence": [],
            }
        return {
            "schema": CONTRACT_VERSION,
            "status": "available",
            "symbol": symbol,
            "asset_type": asset_type,
            "listing_market": next(
                (row.get("listing_market") for row in fragments if row.get("listing_market")),
                market,
            ),
            "currency": next(
                (row.get("currency") for row in fragments if row.get("currency")), None
            ),
            "as_of": max(
                (_utc(row["as_of"]) for row in fragments if row.get("as_of")),
                default=now,
            ).isoformat(),
            "classifications": classifications,
            "classification_notice": (
                "Basic classifications are facts, not a complete risk exposure model."
            ),
            "holdings": holdings,
            "company_materials": materials,
            "company_materials_status": "available" if materials else "unavailable",
            "evidence": [row["evidence"] for row in fragments if row.get("evidence")],
            "degradations": failures,
            "coverage": {"markets": list(SUPPORTED_MARKETS)},
        }

    @staticmethod
    def _served(
        result: dict[str, Any], cache_status: str, age: float, now: datetime
    ) -> dict[str, Any]:
        result.update({
            "cache_status": cache_status,
            "cache_age_seconds": round(age, 3),
            "served_at": now.isoformat(),
        })
        return result

    @staticmethod
    def _unavailable(
        symbol: str, market: str, failures: list[dict[str, Any]], now: datetime
    ) -> dict[str, Any]:
        return {
            "schema": CONTRACT_VERSION,
            "status": "unavailable",
            "symbol": symbol,
            "asset_type": "unknown",
            "listing_market": market,
            "currency": None,
            "as_of": None,
            "classifications": [],
            "classification_notice": (
                "No industry or exposure inference is made without source facts."
            ),
            "holdings": {
                "status": "unavailable",
                "reason": "instrument_type_or_constituents_unavailable",
                "constituents": [],
                "as_of": None,
                "evidence": [],
            },
            "company_materials": [],
            "company_materials_status": "unavailable",
            "evidence": [],
            "degradations": failures,
            "coverage": {"markets": list(SUPPORTED_MARKETS)},
            "cache_status": "empty",
            "cache_age_seconds": None,
            "served_at": now.isoformat(),
        }
