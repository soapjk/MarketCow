from __future__ import annotations

import copy
import hashlib
import json
import threading
import uuid
from concurrent.futures import Future, ThreadPoolExecutor, wait
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

import pandas as pd

from .config import Settings
from .normalize import (
    exchange_for_symbol,
    instrument_id,
    json_safe,
    latest_broad_report_period,
    normalize_report_period,
    safe_record,
)
from .providers.akshare_financials import AkshareFinancialProvider
from .providers.baostock_provider import BaoStockProvider, optional_float
from .providers.eastmoney import EastmoneySpotProvider
from .providers.tdx_financial import TdxFinancialProvider
from .providers.yahoo_quote import YahooQuoteProvider
from .providers.yahoo_fx import YahooFxProvider
from .providers.hyperliquid import HyperliquidProvider
from .cross_market import (
    cross_market_snapshot, exact_relationship, load_cross_market_inputs,
)
from .providers.instrument_search import InstrumentSearchProvider
from .providers.eastmoney_realtime import EastmoneyRealtimeQuoteProvider
from .providers.sina_realtime import SinaRealtimeQuoteProvider
from .providers.calendar import CalendarProvider
from .providers.tushare_provider import TushareProvider
from .providers.longport_quote import LongPortQuoteProvider
from .providers.sec_dividends import SecDividendProvider
from .providers.cn_dividends import CnExchangeDividendProvider
from .providers.hkex_dividends import HkexDividendProvider
from .providers.structured_dividends import (
    CnStructuredDividendProvider,
    LongPortDividendProvider,
    TushareDividendProvider,
    UsStructuredDividendProvider,
)
from .quote_persistence import AsyncQuotePersistence
from .providers.contracts import DEFAULT_PROVIDER_MANIFESTS, ProviderRegistry
from .repositories import Repositories
from .domain_columns import FUNDAMENTAL_COLUMNS
from .dividends import (
    dividend_summary,
    fund_dividend_history,
    normalize_dividend_announcement,
    normalize_dividend_symbol,
)
from .dividend_assessment import assess_dividend
from .instruments import canonical_instrument
from .market_data_contracts import (
    InstrumentContract,
    canonical_hash,
    validate_instrument_identity,
)
from .provider_routing import (
    MARKET_BAR_HISTORY,
    REALTIME_QUOTE,
    ProviderNotSupported,
    ProviderUnavailable,
    select_providers,
)
from .price_adjustment import PriceAdjustmentContract
from .history_coverage import assess_history_response
from .exposure_facts import ExposureFactsService, RepositoryExposureFactSource


EASTMONEY_DATA_URL = "https://datacenter-web.eastmoney.com/api/data/v1/get"
EASTMONEY_QUOTE_URL = "https://push2.eastmoney.com/api/qt/clist/get"
BAOSTOCK_SOURCE_URL = "http://baostock.com/baostock/index.php/Python_API文档"
DIVIDEND_CACHE_SCHEMA = "dividend-cache-v2"
DIVIDEND_REFRESH_STRATEGY = "official-fund-v7-payment-year"
DIVIDEND_SUCCESS_STATES = frozenset({"success_data", "success_empty"})


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _decode_database_value(value: Any) -> Any:
    if isinstance(value, bytes):
        return value.decode("utf-8")
    if isinstance(value, dict):
        return {
            _decode_database_value(key): _decode_database_value(item)
            for key, item in value.items()
        }
    if isinstance(value, (list, tuple)):
        return [_decode_database_value(item) for item in value]
    return value


def _instrument_contract_payload(row: Dict[str, Any]) -> Dict[str, Any]:
    payload = {
        key: _decode_database_value(row[key])
        for key in InstrumentContract.model_fields
    }
    for field in ("tick_size", "size_increment", "lot_size"):
        payload[field] = format(Decimal(str(payload[field])), "f")
    for field in ("ts_event", "ts_init"):
        if isinstance(payload[field], datetime):
            payload[field] = payload[field].isoformat()
    return payload


def _number(value: Any) -> Optional[float]:
    value = json_safe(value)
    if value in (None, "", "-"):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _text(value: Any) -> Optional[str]:
    value = json_safe(value)
    if value in (None, ""):
        return None
    return str(value)


def _coalesce_number(*values: Any) -> Optional[float]:
    for value in values:
        number = _number(value)
        if number is not None:
            return number
    return None


def _records_by_symbol(frame: pd.DataFrame) -> Dict[str, Dict[str, Any]]:
    if frame is None or frame.empty:
        return {}
    result: Dict[str, Dict[str, Any]] = {}
    for record in frame.to_dict("records"):
        record = safe_record(record)
        symbol = str(record.get("股票代码") or record.get("代码") or "").zfill(6)
        if symbol.strip("0"):
            result[symbol] = record
    return result


def _first(record: Dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in record and json_safe(record.get(key)) is not None:
            return record.get(key)
    return None


class FundamentalService:
    def __init__(
        self,
        settings: Settings,
        warehouse: Optional[Any] = None,
        repositories: Optional[Repositories] = None,
        spot_provider: Optional[EastmoneySpotProvider] = None,
        financial_provider: Optional[AkshareFinancialProvider] = None,
        baostock_provider: Optional[BaoStockProvider] = None,
        tdx_provider: Optional[TdxFinancialProvider] = None,
        quote_provider: Optional[YahooQuoteProvider] = None,
        fx_provider: Optional[YahooFxProvider] = None,
        search_provider: Optional[InstrumentSearchProvider] = None,
        sina_quote_provider: Optional[SinaRealtimeQuoteProvider] = None,
        a_quote_provider: Optional[EastmoneyRealtimeQuoteProvider] = None,
        calendar_provider: Optional[CalendarProvider] = None,
        tushare_provider: Optional[TushareProvider] = None,
        longport_quote_provider: Optional[LongPortQuoteProvider] = None,
        hyperliquid_provider: Optional[HyperliquidProvider] = None,
        sec_dividend_provider: Optional[SecDividendProvider] = None,
        cn_dividend_provider: Optional[CnExchangeDividendProvider] = None,
        hkex_dividend_provider: Optional[HkexDividendProvider] = None,
        cn_structured_dividend_provider: Optional[Any] = None,
        longport_dividend_provider: Optional[LongPortDividendProvider] = None,
        exposure_facts_service: Optional[ExposureFactsService] = None,
    ):
        self.settings = settings
        self.warehouse = warehouse
        self.repository_database = None
        self.online_resources = None
        if repositories is None:
            from .artifact_store import LocalArtifactStore
            from .factory import create_online_repositories
            from .market_bars import AuthoritativeMarketBarRepository

            self.online_resources = create_online_repositories(settings)
            market_bars = AuthoritativeMarketBarRepository(
                self.online_resources.market_bars, self.online_resources.writer,
                self.online_resources.telemetry,
                self.online_resources.canonical_scheduler,
            )
            repositories = Repositories(
                metadata=self.online_resources.postgres,
                fundamentals=self.online_resources.postgres,
                market_bars=market_bars,
                artifacts=LocalArtifactStore(self.online_resources.postgres),
            )
        self.repositories = repositories
        self.metadata_repository = self.repositories.metadata
        self.fundamental_repository = self.repositories.fundamentals
        self.market_bar_repository = self.repositories.market_bars
        self.exposure_facts_service = exposure_facts_service or ExposureFactsService((
            RepositoryExposureFactSource(
                self.market_bar_repository, self.fundamental_repository
            ),
        ))
        self.artifact_store = self.repositories.artifacts
        self.spot_provider = spot_provider or EastmoneySpotProvider()
        self.financial_provider = financial_provider or AkshareFinancialProvider()
        self.baostock_provider = baostock_provider or BaoStockProvider()
        self.tdx_provider = tdx_provider or TdxFinancialProvider(
            settings.raw_path.parent / "tdx/financial"
        )
        self.quote_provider = quote_provider or YahooQuoteProvider()
        self.fx_provider = fx_provider or YahooFxProvider(
            cache_ttl_seconds=settings.fx_cache_ttl_seconds,
            stale_max_seconds=settings.fx_stale_max_seconds,
        )
        self.search_provider = search_provider or InstrumentSearchProvider()
        self.sina_quote_provider = sina_quote_provider or SinaRealtimeQuoteProvider()
        self.a_quote_provider = a_quote_provider or EastmoneyRealtimeQuoteProvider()
        self.calendar_provider = calendar_provider or CalendarProvider()
        self.tushare_provider = tushare_provider or TushareProvider(
            settings.tushare_token, settings.tushare_base_url,
            settings.tushare_realtime_url, settings.tushare_min_interval,
        )
        self.longport_quote_provider = longport_quote_provider or LongPortQuoteProvider(
            settings.longport_app_key,
            settings.longport_app_secret,
            settings.longport_access_token,
            enable_overnight=settings.longport_enable_overnight,
        )
        self.hyperliquid_provider = hyperliquid_provider or HyperliquidProvider(
            settings.hyperliquid_base_url, settings.hyperliquid_timeout_seconds,
            settings.hyperliquid_request_budget_seconds,
        )
        self.sec_dividend_provider = sec_dividend_provider or SecDividendProvider(
            settings.sec_user_agent
        )
        self.cn_dividend_provider = cn_dividend_provider or CnExchangeDividendProvider()
        self.hkex_dividend_provider = hkex_dividend_provider or HkexDividendProvider()
        self.longport_dividend_provider = (
            longport_dividend_provider or LongPortDividendProvider(
                settings.longport_app_key,
                settings.longport_app_secret,
                settings.longport_access_token,
                min_interval_seconds=settings.dividend_longport_min_interval_seconds,
                max_attempts=settings.dividend_longport_max_attempts,
            )
        )
        self.cn_structured_dividend_provider = (
            cn_structured_dividend_provider or CnStructuredDividendProvider(
                TushareDividendProvider(self.tushare_provider),
                self.longport_dividend_provider,
            )
        )
        self.us_structured_dividend_provider = UsStructuredDividendProvider(
            self.longport_dividend_provider, self.sec_dividend_provider
        )
        self.provider_registry = ProviderRegistry(DEFAULT_PROVIDER_MANIFESTS)
        quote_capability = (REALTIME_QUOTE,)
        self.provider_registry.bind("sina", self.sina_quote_provider, quote_capability)
        self.provider_registry.bind("eastmoney", self.a_quote_provider, quote_capability)
        self.provider_registry.bind("yahoo", self.quote_provider, quote_capability)
        self.provider_registry.bind("tushare", self.tushare_provider, quote_capability)
        self.provider_registry.bind("longport", self.longport_quote_provider, quote_capability)
        self.provider_registry.bind(
            "hyperliquid", self.hyperliquid_provider,
            (REALTIME_QUOTE, MARKET_BAR_HISTORY),
        )
        self.quote_persistence = AsyncQuotePersistence(
            capacity=settings.quote_persistence_queue_size
        )
        self._dividend_refresh_executor = ThreadPoolExecutor(
            max_workers=settings.dividend_refresh_workers,
            thread_name_prefix="dividend-refresh",
        )
        self._dividend_refresh_guard = threading.Lock()
        self._dividend_refresh_active: set[tuple[str, int]] = set()
        self._dividend_refresh_futures: set[Future[Any]] = set()
        self._dividend_refresh_locks: Dict[tuple[str, int], threading.Lock] = {}

    def close(self) -> None:
        with self._dividend_refresh_guard:
            futures = set(self._dividend_refresh_futures)
        if futures:
            wait(futures, timeout=self.settings.dividend_refresh_shutdown_seconds)
        self._dividend_refresh_executor.shutdown(wait=False, cancel_futures=True)
        self.quote_persistence.close(self.settings.quote_persistence_shutdown_seconds)
        self.longport_quote_provider.close()
        self.longport_dividend_provider.close()
        if self.online_resources is not None:
            self.online_resources.close()
        if self.repository_database is not None:
            self.repository_database.close()

    def _persist_tushare_response(
        self, api_name: str, params: Dict[str, Any], fields: str,
        result: Dict[str, Any], metadata: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        result = json_safe(result)
        ingested_at = utc_now()
        rows = self.tushare_provider.rows(result)
        artifact = self._write_artifact(
            self.settings.raw_path / "tushare" / api_name,
            "tushare-" + api_name,
            result,
            self.tushare_provider.name,
            self.tushare_provider.base_url + "/",
            "data.fields + data.items",
            ingested_at,
            ingested_at,
            {
                "api_name": api_name, "params": params, "fields": fields,
                "row_count": len(rows), **(metadata or {}),
            },
        )
        self.metadata_repository.save_tushare_response({
            "request_id": uuid.uuid4().hex, "api_name": api_name, "params": params,
            "requested_fields": fields, "response_fields": (result.get("data") or {}).get("fields") or [],
            "response_code": result.get("code"), "response_message": result.get("msg"),
            "source": self.tushare_provider.name, "source_url": self.tushare_provider.base_url + "/",
            "observed_at": ingested_at, "ingested_at": ingested_at,
            "raw_path": artifact["storage_path"], "raw_artifact_id": artifact["artifact_id"],
        }, rows)
        return artifact

    def call_tushare(self, api_name: str, params: Dict[str, Any], fields: str = "") -> Dict[str, Any]:
        result = self.tushare_provider.call(api_name, params, fields)
        self._persist_tushare_response(api_name, params, fields, result)
        self.metadata_repository.record_provider_health(self.tushare_provider.name, True, utc_now())
        return result

    def tushare_realtime_quote(self, ts_code: str) -> List[Dict[str, Any]]:
        rows = json_safe(self.tushare_provider.realtime_quote(ts_code))
        fields = list(rows[0]) if rows else []
        result = {"code": 0, "msg": None, "data": {"fields": fields, "items": [[row.get(key) for key in fields] for row in rows]}}
        self._persist_tushare_response("realtime_quote", {"ts_code": ts_code}, "", result)
        return rows

    def refresh_tushare_minute_history(
        self, symbol: str, range_: str, interval: str, adjustment: str
    ) -> Dict[str, Any]:
        if adjustment != "raw":
            raise ValueError("Tushare minute bars currently require adjustment=raw")
        frequencies = {"1m": "1min", "5m": "5min", "15m": "15min", "30m": "30min", "60m": "60min", "1h": "60min"}
        if interval not in frequencies:
            raise ValueError("unsupported Tushare minute interval")
        range_days = {"1d": 1, "5d": 5, "1mo": 31, "3mo": 93, "6mo": 186, "1y": 366, "2y": 732, "5y": 1830, "10y": 3660, "ytd": 366, "max": 3660}
        if range_ not in range_days:
            raise ValueError("unsupported range")
        end = datetime.now().astimezone()
        start = end - timedelta(days=range_days[range_])
        return self.refresh_tushare_minute_history_window(
            symbol, start, end, interval, adjustment, range_label=range_
        )

    def refresh_tushare_minute_history_window(
        self, symbol: str, start: datetime, end: datetime, interval: str,
        adjustment: str, range_label: str = "window",
        ingestion_id: str | None = None,
    ) -> Dict[str, Any]:
        if adjustment != "raw":
            raise ValueError("Tushare minute bars currently require adjustment=raw")
        frequencies = {
            "1m": "1min", "5m": "5min", "15m": "15min", "30m": "30min",
            "60m": "60min", "1h": "60min",
        }
        if interval not in frequencies:
            raise ValueError("unsupported Tushare minute interval")
        if start.tzinfo is None or end.tzinfo is None or start >= end:
            raise ValueError("history window must be ordered and timezone-aware")
        if range_label == "window":
            range_label = (
                f"{start.astimezone(timezone.utc).isoformat()}/"
                f"{end.astimezone(timezone.utc).isoformat()}"
            )
        instrument = canonical_instrument(symbol)
        if instrument.market != "CN":
            raise ValueError("Tushare minute history requires a CN instrument")
        provider_symbol = instrument.provider_symbol("provider:tushare")
        params = {
            "ts_code": provider_symbol, "freq": frequencies[interval],
            "start_date": start.strftime("%Y-%m-%d %H:%M:%S"),
            "end_date": end.strftime("%Y-%m-%d %H:%M:%S"),
        }
        result = self.tushare_provider.call("stk_mins", params, "")
        artifact = self._persist_tushare_response(
            "stk_mins", params, "", result,
            {"ingestion_id": ingestion_id} if ingestion_id else None,
        )
        bars = self.tushare_provider.minute_bars(result)
        ingested_at = utc_now()
        shanghai = ZoneInfo("Asia/Shanghai")
        preliminary_coverage = assess_history_response(
            "tushare", interval, bars, start, end,
            instrument_id=instrument.instrument_id,
        )
        coverage_exempt_dates: list[str] = []
        coverage_evidence_artifact_id = None
        missing_sessions = list(
            preliminary_coverage.get("missing_session_dates") or []
        )
        if missing_sessions:
            suspension_params = {
                "ts_code": provider_symbol,
                "start_date": missing_sessions[0].replace("-", ""),
                "end_date": (
                    datetime.fromisoformat(missing_sessions[-1]).date()
                    + timedelta(days=1)
                ).strftime("%Y%m%d"),
            }
            suspension_fields = "ts_code,trade_date,suspend_type"
            suspension_result = self.tushare_provider.call(
                "suspend_d", suspension_params, suspension_fields
            )
            suspension_artifact = self._persist_tushare_response(
                "suspend_d", suspension_params, suspension_fields,
                suspension_result,
                {
                    "ingestion_id": ingestion_id,
                    "instrument_id": instrument.instrument_id,
                    "coverage_evidence": True,
                },
            )
            coverage_evidence_artifact_id = suspension_artifact["artifact_id"]
            missing_set = set(missing_sessions)
            coverage_exempt_dates = sorted({
                datetime.strptime(
                    str(row.get("trade_date")), "%Y%m%d"
                ).date().isoformat()
                for row in self.tushare_provider.rows(suspension_result)
                if str(row.get("trade_date") or "").isdigit()
                and len(str(row.get("trade_date"))) == 8
                and datetime.strptime(
                    str(row.get("trade_date")), "%Y%m%d"
                ).date().isoformat() in missing_set
            })
        final_prewrite_coverage = assess_history_response(
            "tushare", interval, bars, start, end, coverage_exempt_dates,
            instrument.instrument_id,
        )
        if final_prewrite_coverage["status"] == "split_required":
            self.metadata_repository.record_provider_health(
                self.tushare_provider.name, True, ingested_at
            )
            return {
                "status": "split_required",
                "source": self.tushare_provider.name,
                "range": range_label, "interval": interval,
                "adjustment": adjustment, "bars": bars, "count": 0,
                "observed_at": ingested_at,
                "raw_path": artifact["storage_path"],
                "raw_artifact_id": artifact["artifact_id"],
                "ingestion_id": ingestion_id,
                "coverage_exempt_dates": coverage_exempt_dates,
                "coverage_evidence_artifact_id": (
                    coverage_evidence_artifact_id
                ),
                "coverage": final_prewrite_coverage,
            }
        factor_start = start.astimezone(shanghai).date()
        factor_end = (end - timedelta(microseconds=1)).astimezone(shanghai).date()
        factor_params = {
            "ts_code": provider_symbol,
            "start_date": factor_start.strftime("%Y%m%d"),
            # The configured Tushare-compatible endpoint treats end_date as
            # exclusive for multi-day adj_factor queries, although the
            # upstream contract describes a date range. Widen the provider
            # request while keeping MarketCow's internal window unchanged.
            "end_date": (factor_end + timedelta(days=1)).strftime("%Y%m%d"),
        }
        factor_fields = "ts_code,trade_date,adj_factor"
        factor_result = self.tushare_provider.call(
            "adj_factor", factor_params, factor_fields
        )
        factor_artifact = self._persist_tushare_response(
            "adj_factor", factor_params, factor_fields, factor_result,
            {"ingestion_id": ingestion_id, "instrument_id": instrument.instrument_id},
        )
        factors = self.tushare_provider.adjustment_factors(
            factor_result, provider_symbol
        )
        factor_artifacts_by_date = {
            factor["trade_date"]: factor_artifact["artifact_id"]
            for factor in factors
        }
        expected_dates = {
            datetime.fromisoformat(str(bar["bar_at"]).replace("Z", "+00:00"))
            .astimezone(shanghai).date().isoformat()
            for bar in bars
        }
        factor_dates = {factor["trade_date"] for factor in factors}
        missing_dates = sorted(expected_dates - factor_dates)
        supplement_artifact_ids = []
        for missing_date in missing_dates:
            exact_params = {
                "ts_code": provider_symbol,
                "trade_date": missing_date.replace("-", ""),
            }
            exact_result = self.tushare_provider.call(
                "adj_factor", exact_params, factor_fields
            )
            exact_artifact = self._persist_tushare_response(
                "adj_factor", exact_params, factor_fields, exact_result,
                {
                    "ingestion_id": ingestion_id,
                    "instrument_id": instrument.instrument_id,
                    "supplement_for_trade_date": missing_date,
                },
            )
            exact_factors = self.tushare_provider.adjustment_factors(
                exact_result, provider_symbol
            )
            for factor in exact_factors:
                if factor["trade_date"] == missing_date:
                    factors.append(factor)
                    factor_dates.add(missing_date)
                    factor_artifacts_by_date[missing_date] = exact_artifact[
                        "artifact_id"
                    ]
                    supplement_artifact_ids.append(exact_artifact["artifact_id"])
                    break
        missing_dates = sorted(expected_dates - factor_dates)
        if missing_dates:
            preview = ",".join(missing_dates[:3])
            raise ValueError(
                f"Tushare adj_factor is missing {len(missing_dates)} bar dates: {preview}"
            )
        factors_by_date = {
            factor["trade_date"]: factor["adjustment_factor"]
            for factor in factors
        }
        for bar in bars:
            trade_date = (
                datetime.fromisoformat(str(bar["bar_at"]).replace("Z", "+00:00"))
                .astimezone(shanghai).date().isoformat()
            )
            factor = factors_by_date[trade_date]
            # Keep adjustment_factor during the compatibility window. New
            # consumers must use the two explicit fields below.
            contract = PriceAdjustmentContract.model_validate({
                "adjustment": "raw",
                "factor_applicability": "applicable",
                "corporate_action_factor": factor,
                "applied_adjustment_multiplier": "1",
                "adjustment_reference_date": None,
                "reference_factor": None,
                "factor_source": self.tushare_provider.name,
                "factor_artifact_id": factor_artifacts_by_date[trade_date],
                "factor_as_of": ingested_at,
            })
            bar["adjustment_factor"] = factor
            bar.update(contract.model_dump())
        factor_count = self.market_bar_repository.upsert_adjustment_factors(
            instrument.instrument_id, self.tushare_provider.name, ingested_at,
            factors,
            {
                "observed_at": ingested_at,
                "raw_artifact_id": factor_artifact["artifact_id"],
                "ingestion_id": ingestion_id,
            },
        )
        count = self.market_bar_repository.upsert_price_bars(
            instrument.instrument_id, interval, "raw",
            self.tushare_provider.name, ingested_at, bars,
            {"source_url": self.tushare_provider.base_url + "/", "observed_at": ingested_at,
             "raw_response_locator": "data.items", "raw_path": artifact["storage_path"],
             "raw_artifact_id": artifact["artifact_id"],
             "ingestion_id": ingestion_id},
        )
        self.metadata_repository.record_provider_health(self.tushare_provider.name, True, ingested_at)
        return {
            "symbol": instrument.instrument_id,
            "provider_symbol": provider_symbol, "range": range_label,
            "interval": interval, "adjustment": "raw",
            "source": self.tushare_provider.name, "source_url": self.tushare_provider.base_url + "/",
            "raw_response_locator": "data.items", "bars": bars, "count": count,
            "observed_at": ingested_at, "ingested_at": ingested_at,
            "raw_path": artifact["storage_path"],
            "raw_artifact_id": artifact["artifact_id"],
            "ingestion_id": ingestion_id,
            "adjustment_factors": factors,
            "adjustment_factor_count": factor_count,
            "adjustment_factor_raw_path": factor_artifact["storage_path"],
            "adjustment_factor_raw_artifact_id": factor_artifact["artifact_id"],
            "adjustment_factor_supplement_artifact_ids": supplement_artifact_ids,
            "coverage_exempt_dates": coverage_exempt_dates,
            "coverage_evidence_artifact_id": coverage_evidence_artifact_id,
        }

    def search_instruments(self, query: str, limit: int = 12) -> List[Dict[str, Any]]:
        return self.search_provider.search(query, limit)

    def get_fx_rates(
        self, base: str, symbols: list[str], *, refresh: bool = False
    ) -> Dict[str, Any]:
        return self.fx_provider.get_rates(base, symbols, refresh=refresh)

    def ingest_dividend_announcements(
        self, announcements: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        ingested_at = utc_now()
        rows = [
            normalize_dividend_announcement(announcement, ingested_at)
            for announcement in announcements
        ]
        count = self.fundamental_repository.upsert_dividend_announcements(rows)
        return {"status": "success", "count": count, "ingested_at": ingested_at}

    @staticmethod
    def _timestamp(value: Any) -> Optional[datetime]:
        if not value:
            return None
        parsed = value if isinstance(value, datetime) else datetime.fromisoformat(
            str(value).replace("Z", "+00:00")
        )
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)

    def _read_dividends(self, symbol: str, fiscal_year: int) -> Dict[str, Any]:
        if not 1991 <= fiscal_year <= 2100:
            raise ValueError("fiscal_year must be between 1991 and 2100")
        normalized_symbol = normalize_dividend_symbol(symbol)
        rows = self.fundamental_repository.get_dividend_announcements(
            normalized_symbol, fiscal_year - 1, fiscal_year
        )
        return dividend_summary(normalized_symbol, fiscal_year, rows)

    def _dividend_state(
        self, symbol: str, fiscal_year: int
    ) -> Optional[Dict[str, Any]]:
        state = self.fundamental_repository.get_dividend_refresh_state(
            symbol, fiscal_year
        )
        if state:
            state = {
                key: value.decode("utf-8") if isinstance(value, bytes) else value
                for key, value in state.items()
            }
        if state and (
            state.get("strategy_version") != DIVIDEND_REFRESH_STRATEGY
            or state.get("cache_schema_version") != DIVIDEND_CACHE_SCHEMA
            or state.get("parser_version") != DIVIDEND_REFRESH_STRATEGY
        ):
            return None
        return state

    def _with_dividend_cache_metadata(
        self, data: Dict[str, Any], state: Optional[Dict[str, Any]], status: str
    ) -> Dict[str, Any]:
        result = dict(data)
        result["data_status"] = status
        result["last_refreshed_at"] = (
            str(state["last_success_at"]) if state and state.get("last_success_at")
            else None
        )
        result["cache_schema_version"] = DIVIDEND_CACHE_SCHEMA
        result["parser_version"] = DIVIDEND_REFRESH_STRATEGY
        result["refresh_status"] = state.get("status") if state else None
        result["query_source"] = state.get("query_source") if state else None
        result["refresh_completed_at"] = (
            str(state["completed_at"]) if state and state.get("completed_at") else None
        )
        result["assessment"] = self._assess_dividend(result, state)
        return result

    def _dividend_asset_type(self, symbol: str) -> str:
        instrument = canonical_instrument(symbol)
        settings = getattr(self, "settings", None)
        configured = {
            normalize_dividend_symbol(value)
            for value in getattr(settings, "dividend_etf_symbols", ())
        }
        if instrument.instrument_id in configured:
            return "etf"
        if instrument.market == "CN" and instrument.symbol[:2] in {
            "15", "16", "50", "51", "52", "56", "58",
        }:
            return "etf"
        try:
            master = self.metadata_repository.get_instrument(instrument.instrument_id)
        except (AttributeError, RuntimeError):
            master = None
        value = str(
            (master or {}).get("asset_class")
            or (master or {}).get("instrument_type")
            or ""
        ).lower()
        if value in {"etf", "fund"}:
            return "etf"
        if value == "equity":
            return "equity"
        return "equity" if instrument.market in {"US", "HK", "CN"} else "unknown"

    def _assess_dividend(
        self, data: Dict[str, Any], state: Optional[Dict[str, Any]]
    ) -> Dict[str, Any]:
        fiscal_year = int(data["fiscal_year"])
        settings = getattr(self, "settings", None)
        years = max(2, getattr(settings, "dividend_likely_zero_years", 3))
        historical_states = {}
        for year in range(fiscal_year - years, fiscal_year):
            candidate = self._dividend_state(data["symbol"], year)
            if candidate:
                historical_states[year] = candidate
        policy_evidence = []
        for item in getattr(settings, "dividend_zero_policy_evidence", ()):
            try:
                evidence_symbol = normalize_dividend_symbol(
                    str(item.get("symbol") or "")
                )
            except ValueError:
                continue
            if evidence_symbol == data["symbol"]:
                policy_evidence.append(item)
        return assess_dividend(
            symbol=data["symbol"],
            fiscal_year=fiscal_year,
            announced_count=int(data.get("announced_count") or 0),
            asset_type=self._dividend_asset_type(data["symbol"]),
            refresh_state=state,
            historical_states=historical_states,
            previous_complete_year=data.get("previous_complete_year"),
            likely_zero_years=years,
            policy_evidence=policy_evidence,
        )

    @staticmethod
    def _dividend_failure_status(exc: Exception) -> str:
        text = str(exc).lower()
        if isinstance(exc, TimeoutError) or "timeout" in text or "timed out" in text:
            return "failed_timeout"
        if "429" in text or "rate limit" in text or "too many requests" in text:
            return "failed_rate_limited"
        if isinstance(exc, (ValueError, KeyError, TypeError)) and any(
            marker in text for marker in ("parse", "format", "date", "field", "payload")
        ):
            return "failed_parse"
        return "failed_source"

    @staticmethod
    def _dividend_provider_name(provider: Any) -> str:
        return str(
            getattr(provider, "name", "") or provider.__class__.__name__
        )

    def _refresh_lock(self, key: tuple[str, int]) -> threading.Lock:
        with self._dividend_refresh_guard:
            return self._dividend_refresh_locks.setdefault(key, threading.Lock())

    def _fetch_and_ingest_dividends(
        self, provider: Any, symbol: str, fiscal_year: int
    ) -> Dict[str, Any]:
        instrument = canonical_instrument(symbol)
        announcements = provider.fetch(instrument.instrument_id, fiscal_year)
        query_sources = sorted({
            str(row.get("source_name") or "").strip()
            for row in announcements if row.get("source_name")
        })
        for announcement in announcements:
            content = announcement.pop("_raw_content", None)
            extension = announcement.pop("_raw_extension", ".bin")
            if content is None:
                continue
            digest = hashlib.sha256(content).hexdigest()
            folder = self.settings.raw_path / "dividends" / instrument.market
            folder.mkdir(parents=True, exist_ok=True)
            path = folder / f"{digest}{extension}"
            if not path.exists():
                path.write_bytes(content)
            artifact = self._register_file_artifact(
                path, "dividend_announcement", announcement["source_name"],
                announcement["source_url"], "document", utc_now(),
                {"symbol": instrument.instrument_id, "fiscal_year": fiscal_year},
            )
            announcement["raw_artifact_id"] = artifact["artifact_id"]
        result = (
            self.ingest_dividend_announcements(announcements)
            if announcements else
            {"status": "success", "count": 0, "ingested_at": utc_now()}
        )
        return {
            **result,
            "query_source": (
                ", ".join(query_sources) if query_sources
                else self._dividend_provider_name(provider)
            ),
        }

    def _refresh_dividends_now(
        self, symbol: str, fiscal_year: int
    ) -> Dict[str, Any]:
        instrument = canonical_instrument(symbol)
        if instrument.market == "US":
            provider = getattr(
                self, "us_structured_dividend_provider", self.sec_dividend_provider
            )
        elif instrument.market == "CN":
            provider = (
                self.cn_dividend_provider
                if self._dividend_asset_type(instrument.instrument_id) == "etf"
                else self.cn_structured_dividend_provider
            )
        elif instrument.market == "HK":
            provider = self.longport_dividend_provider
        else:
            raise ValueError(
                "automatic official refresh is not yet available for this market"
            )
        attempted_at = utc_now()
        prior_state = self._dividend_state(instrument.instrument_id, fiscal_year)
        source = self._dividend_provider_name(provider)
        self.fundamental_repository.upsert_dividend_refresh_state({
            "symbol": instrument.instrument_id, "fiscal_year": fiscal_year,
            "status": "refreshing", "last_attempt_at": attempted_at,
            "last_success_at": (
                prior_state.get("last_success_at") if prior_state else None
            ),
            "last_error": "", "strategy_version": DIVIDEND_REFRESH_STRATEGY,
            "cache_schema_version": DIVIDEND_CACHE_SCHEMA,
            "parser_version": DIVIDEND_REFRESH_STRATEGY,
            "query_source": source, "result_count": None, "completed_at": None,
        })
        try:
            ingestion = self._fetch_and_ingest_dividends(
                provider, instrument.instrument_id, fiscal_year
            )
            source = str(ingestion.get("query_source") or source)
            succeeded_at = utc_now()
            refreshed_data = self._read_dividends(instrument.instrument_id, fiscal_year)
            count = int(refreshed_data.get("announced_count") or 0)
            state = {
                "symbol": instrument.instrument_id, "fiscal_year": fiscal_year,
                "status": "success_data" if count else "success_empty",
                "last_attempt_at": attempted_at,
                "last_success_at": succeeded_at, "last_error": "",
                "strategy_version": DIVIDEND_REFRESH_STRATEGY,
                "cache_schema_version": DIVIDEND_CACHE_SCHEMA,
                "parser_version": DIVIDEND_REFRESH_STRATEGY,
                "query_source": source, "result_count": count,
                "completed_at": succeeded_at,
            }
            self.fundamental_repository.upsert_dividend_refresh_state(state)
            data = self._with_dividend_cache_metadata(
                refreshed_data, state, "fresh"
            )
            return {**ingestion, "data": data}
        except Exception as exc:
            failed_at = utc_now()
            self.fundamental_repository.upsert_dividend_refresh_state({
                "symbol": instrument.instrument_id, "fiscal_year": fiscal_year,
                "status": self._dividend_failure_status(exc),
                "last_attempt_at": attempted_at,
                "last_success_at": (
                    prior_state.get("last_success_at") if prior_state else None
                ),
                "last_error": str(exc)[:2000],
                "strategy_version": DIVIDEND_REFRESH_STRATEGY,
                "cache_schema_version": DIVIDEND_CACHE_SCHEMA,
                "parser_version": DIVIDEND_REFRESH_STRATEGY,
                "query_source": source, "result_count": None,
                "completed_at": failed_at,
            })
            raise

    def _refresh_dividends_locked(
        self, symbol: str, fiscal_year: int, force: bool = False
    ) -> Dict[str, Any]:
        normalized_symbol = normalize_dividend_symbol(symbol)
        key = (normalized_symbol, fiscal_year)
        with self._refresh_lock(key):
            state = self._dividend_state(normalized_symbol, fiscal_year)
            last_success = self._timestamp(
                state.get("last_success_at") if state else None
            )
            if not force and last_success is not None:
                age = (datetime.now(timezone.utc) - last_success).total_seconds()
                ttl = (
                    getattr(
                        self.settings, "dividend_empty_cache_ttl_seconds", 900
                    )
                    if state and state.get("status") == "success_empty"
                    else self.settings.dividend_cache_ttl_seconds
                )
                if (
                    state and state.get("status") in DIVIDEND_SUCCESS_STATES
                    and age <= ttl
                ):
                    data = self._with_dividend_cache_metadata(
                        self._read_dividends(normalized_symbol, fiscal_year),
                        state, "fresh",
                    )
                    return {"status": "success", "count": 0, "data": data}
            last_attempt = self._timestamp(
                state.get("last_attempt_at") if state else None
            )
            if (
                not force
                and state
                and str(state.get("status", "")).startswith("failed_")
                and last_attempt is not None
                and (datetime.now(timezone.utc) - last_attempt).total_seconds()
                < self.settings.dividend_refresh_retry_seconds
            ):
                raise RuntimeError(
                    state.get("last_error") or "dividend refresh temporarily unavailable"
                )
            return self._refresh_dividends_now(normalized_symbol, fiscal_year)

    def _schedule_dividend_refresh(self, symbol: str, fiscal_year: int) -> bool:
        key = (symbol, fiscal_year)
        with self._dividend_refresh_guard:
            if key in self._dividend_refresh_active:
                return False
            self._dividend_refresh_active.add(key)
            future = self._dividend_refresh_executor.submit(
                self._refresh_dividends_locked, symbol, fiscal_year
            )
            self._dividend_refresh_futures.add(future)

        def completed(done: Future[Any]) -> None:
            # Consume background errors; freshness metadata exposes failed refreshes.
            try:
                done.exception()
            except Exception:
                pass
            finally:
                with self._dividend_refresh_guard:
                    self._dividend_refresh_active.discard(key)
                    self._dividend_refresh_futures.discard(done)

        future.add_done_callback(completed)
        return True

    def get_dividends(self, symbol: str, fiscal_year: int) -> Dict[str, Any]:
        if not 1991 <= fiscal_year <= 2100:
            raise ValueError("fiscal_year must be between 1991 and 2100")
        normalized_symbol = normalize_dividend_symbol(symbol)
        data = self._read_dividends(normalized_symbol, fiscal_year)
        state = self._dividend_state(normalized_symbol, fiscal_year)
        now = datetime.now(timezone.utc)
        last_success = self._timestamp(
            state.get("last_success_at") if state else None
        )
        if last_success is not None:
            age = (now - last_success).total_seconds()
            ttl = (
                getattr(self.settings, "dividend_empty_cache_ttl_seconds", 900)
                if state and state.get("status") == "success_empty"
                else self.settings.dividend_cache_ttl_seconds
            )
            if (
                state and state.get("status") in DIVIDEND_SUCCESS_STATES
                and age <= ttl
            ):
                return self._with_dividend_cache_metadata(data, state, "fresh")

        has_cache = bool(data["announcements"]) or last_success is not None
        if not has_cache:
            return self._refresh_dividends_locked(
                normalized_symbol, fiscal_year
            )["data"]

        last_attempt = self._timestamp(
            state.get("last_attempt_at") if state else None
        )
        retry_due = (
            last_attempt is None
            or (now - last_attempt).total_seconds()
            >= self.settings.dividend_refresh_retry_seconds
        )
        refreshing = False
        if retry_due:
            refreshing = self._schedule_dividend_refresh(
                normalized_symbol, fiscal_year
            )
        return self._with_dividend_cache_metadata(
            data, state, "refreshing" if refreshing else "stale"
        )

    def refresh_dividends(self, symbol: str, fiscal_year: int) -> Dict[str, Any]:
        return self._refresh_dividends_locked(symbol, fiscal_year, force=True)

    def get_fund_dividend_history(
        self,
        symbol: str,
        date_from: str,
        date_to: str,
        *,
        refresh: bool = True,
    ) -> Dict[str, Any]:
        instrument = canonical_instrument(symbol)
        if self._dividend_asset_type(instrument.instrument_id) != "etf":
            raise ValueError("instrument is not recognized as a fund or ETF")
        try:
            start = datetime.strptime(date_from, "%Y-%m-%d").date()
            end = datetime.strptime(date_to, "%Y-%m-%d").date()
        except ValueError as exc:
            raise ValueError("from and to must use YYYY-MM-DD") from exc
        if start > end:
            raise ValueError("from must be on or before to")
        if (end - start).days > 3660:
            raise ValueError("fund dividend history range cannot exceed 10 years")
        yearly_results = []
        for year in range(start.year, end.year + 1):
            if refresh:
                result = self.get_dividends(instrument.instrument_id, year)
            else:
                state = self._dividend_state(instrument.instrument_id, year)
                result = self._with_dividend_cache_metadata(
                    self._read_dividends(instrument.instrument_id, year),
                    state,
                    "fresh" if state else "cache_only",
                )
            yearly_results.append(result)
        return fund_dividend_history(
            instrument.instrument_id, date_from, date_to, yearly_results
        )

    def discover_dividends(self, symbol: str, fiscal_year: int) -> Dict[str, Any]:
        instrument = canonical_instrument(symbol)
        if instrument.market != "CN":
            raise ValueError("third-party dividend discovery currently supports A shares")
        ts_code = instrument.provider_symbol("provider:tushare")
        result = self.tushare_provider.call("dividend", {"ts_code": ts_code}, "")
        announcements = []
        for row in self.tushare_provider.rows(result):
            end_date = str(row.get("end_date") or "")
            if not end_date.startswith(str(fiscal_year)):
                continue
            amount = row.get("cash_div_tax")
            announced = str(row.get("ann_date") or "")
            if amount in (None, "", 0) or len(announced) != 8:
                continue
            pay_date = str(row.get("pay_date") or "")
            announcements.append({
                "symbol": instrument.instrument_id, "fiscal_year": fiscal_year,
                "amount_per_share": str(amount), "currency": "CNY",
                "announcement_date": (
                    f"{announced[:4]}-{announced[4:6]}-{announced[6:]}"
                ),
                "payment_date": (
                    f"{pay_date[:4]}-{pay_date[4:6]}-{pay_date[6:]}"
                    if len(pay_date) == 8 else None
                ),
                "record_date": (
                    f"{row['record_date'][:4]}-{row['record_date'][4:6]}-"
                    f"{row['record_date'][6:]}"
                    if len(str(row.get("record_date") or "")) == 8 else None
                ),
                "ex_date": (
                    f"{row['ex_date'][:4]}-{row['ex_date'][4:6]}-"
                    f"{row['ex_date'][6:]}"
                    if len(str(row.get("ex_date") or "")) == 8 else None
                ),
                "confirmation_status": "unverified", "source_type": "third_party",
                "source_name": self.tushare_provider.name,
                "source_url": self.tushare_provider.base_url + "/",
                "source_document_id": str(row.get("div_proc") or ""),
                "payload": row,
            })
        ingestion = self.ingest_dividend_announcements(announcements) if announcements else {
            "status": "success", "count": 0, "ingested_at": utc_now()
        }
        return {
            **ingestion,
            "data": self.get_dividends(instrument.instrument_id, fiscal_year),
        }

    def _start_run(self, job_name: str, report_period: str = "") -> tuple[str, str]:
        run_id, started_at = uuid.uuid4().hex, utc_now()
        self.metadata_repository.save_run([run_id, job_name, "running", report_period or None, started_at, None, 0, None])
        return run_id, started_at

    def _finish_run(self, run_id: str, job_name: str, started_at: str, report_period: str, row_count: int, error: str = "") -> None:
        self.metadata_repository.save_run([
            run_id, job_name, "failed" if error else "success", report_period or None,
            started_at, utc_now(), row_count, error or None,
        ])

    def _write_artifact(
        self,
        folder: Path,
        dataset: str,
        payload: Any,
        source: str,
        source_url: str,
        raw_response_locator: str,
        observed_at: str,
        ingested_at: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        return self.artifact_store.write_json(
            folder, dataset, payload, source, source_url, raw_response_locator,
            observed_at, ingested_at, metadata,
        )

    def _register_file_artifact(
        self, path: Path, dataset: str, source: str, source_url: str,
        raw_response_locator: str, observed_at: str, metadata: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        content = path.read_bytes()
        artifact_id = uuid.uuid4().hex
        manifest = {
            "artifact_id": artifact_id, "dataset": dataset, "source": source,
            "source_url": source_url, "observed_at": observed_at, "ingested_at": observed_at,
            "raw_response_locator": raw_response_locator, "storage_path": str(path),
            "sha256": hashlib.sha256(content).hexdigest(), "byte_size": len(content),
            "metadata_json": json.dumps(metadata or {}, ensure_ascii=False),
        }
        self.artifact_store.save_artifact(manifest)
        return manifest

    def _save_quote_raw(
        self, symbol: str, dataset: str, payload: Dict[str, Any],
        ingested_at: str, source: str, source_url: str, locator: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        safe_symbol = "".join(ch for ch in symbol if ch.isalnum() or ch in ("-", "."))
        folder = self.settings.raw_path / "quotes" / safe_symbol
        return self._write_artifact(
            folder, "quote_" + dataset, payload, source, source_url, locator,
            ingested_at, ingested_at, {"symbol": symbol, **(metadata or {})},
        )

    @staticmethod
    def _quote_market(symbol: str) -> tuple[str, str]:
        instrument = canonical_instrument(symbol)
        return instrument.market, instrument.instrument_id

    def _quote_provider(self, name: str) -> Any:
        return self.provider_registry.get(name)

    def _fetch_realtime_quote(self, provider_name: str, symbol: str) -> Dict[str, Any]:
        provider = self._quote_provider(provider_name)
        instrument = canonical_instrument(symbol)
        if provider_name != "tushare":
            provider_input = (
                instrument.provider_symbol(f"provider:{provider_name}")
                if provider_name in {"eastmoney", "sina"}
                else instrument.instrument_id
            )
            row = provider.fetch_quote(provider_input)
            row["provider_symbol"] = row.get("symbol") or provider_input
            row["instrument_id"] = instrument.instrument_id
            row["symbol"] = instrument.instrument_id
            row["exchange"] = instrument.mic
            return row
        provider_symbol = instrument.provider_symbol("provider:tushare")
        rows = json_safe(provider.realtime_quote(provider_symbol))
        if not rows:
            raise RuntimeError("Tushare returned no realtime quote")
        item = rows[0]
        price = _number(item.get("PRICE"))
        if price is None:
            raise RuntimeError("Tushare returned no usable price")
        previous_close = _number(item.get("PRE_CLOSE"))
        code = instrument.symbol
        date_value, time_value = item.get("DATE"), item.get("TIME")
        quote_at = None
        if date_value and time_value:
            try:
                quote_at = datetime.strptime(
                    f"{date_value} {time_value}", "%Y%m%d %H:%M:%S"
                ).replace(tzinfo=timezone(timedelta(hours=8))).isoformat(timespec="seconds")
            except ValueError:
                pass
        return {
            "instrument_id": instrument.instrument_id,
            "symbol": instrument.instrument_id,
            "provider_symbol": provider_symbol,
            "name": item.get("NAME") or code, "market": "CN",
            "exchange": instrument.mic, "currency": "CNY",
            "price": price, "previous_close": previous_close,
            "change": None if previous_close is None else price - previous_close,
            "change_pct": None if not previous_close else (price / previous_close - 1) * 100,
            "session": "regular", "quote_at": quote_at, "price_adjustment": "raw",
            "quality_status": "single_source_unverified", "source": "tushare",
            "source_url": provider.realtime_url,
            "raw_response_locator": "realtime_quote row", "_raw_payload": item,
        }

    def _persist_quote(
        self, row: Dict[str, Any], ingested_at: str | None = None
    ) -> Dict[str, Any]:
        raw_payload = row.pop("_raw_payload")
        ingested_at = ingested_at or utc_now()
        artifact = self._save_quote_raw(
            row["symbol"], "latest", raw_payload, ingested_at, row["source"],
            row["source_url"], row["raw_response_locator"],
        )
        row.update({
            "observed_at": row.get("quote_at") or ingested_at,
            "ingested_at": ingested_at,
            "raw_path": artifact["storage_path"],
            "raw_artifact_id": artifact["artifact_id"],
            "is_cached": False, "cached": False, "stale": False,
            "cache_status": "refreshed",
        })
        self.market_bar_repository.upsert_quote(row)
        return row

    def _persist_quotes_async(
        self, rows: list[Dict[str, Any]], provider: str
    ) -> list[Dict[str, Any]]:
        ingested_at = utc_now()
        pending_rows = copy.deepcopy(rows)

        def persist() -> None:
            for pending in pending_rows:
                self._persist_quote(pending, ingested_at)
            self.metadata_repository.record_provider_health(provider, True, ingested_at)

        queued = self.quote_persistence.submit(persist)
        status = "queued" if queued else "rejected"
        responses = []
        for row in rows:
            response = {key: value for key, value in row.items() if key != "_raw_payload"}
            response.update({
                "observed_at": row.get("quote_at") or ingested_at,
                "ingested_at": ingested_at,
                "raw_path": None,
                "raw_artifact_id": None,
                "is_cached": False,
                "cached": False,
                "stale": False,
                "cache_status": "refreshed",
                "persistence_status": status,
            })
            responses.append(response)
        return responses

    def refresh_quotes_batch(
        self, symbols: list[str], provider: str, allow_fallback: bool = False
    ) -> list[Dict[str, Any]] | None:
        adapter = self._quote_provider(provider)
        fetch = getattr(adapter, "fetch_quotes", None)
        if not callable(fetch):
            return None
        normalized: list[str] = []
        for symbol in symbols:
            market, value = self._quote_market(symbol)
            select_providers(
                REALTIME_QUOTE, market, provider, (), allow_fallback=allow_fallback
            )
            normalized.append(value)
        if not getattr(adapter, "configured", True):
            raise ProviderUnavailable(
                f"{provider}: provider is not configured",
                provider=provider, capability=REALTIME_QUOTE, market="mixed",
            )
        try:
            rows = fetch(normalized)
        except Exception as exc:
            self.metadata_repository.record_provider_health(provider, False, utc_now(), str(exc))
            raise
        if len(rows) != len(normalized):
            raise ProviderUnavailable(
                f"{provider}: incomplete quote batch",
                provider=provider, capability=REALTIME_QUOTE, market="mixed",
            )
        return self._persist_quotes_async(rows, provider)

    def resolve_instruments_batch(
        self, namespace: str, external_symbols: list[str]
    ) -> Dict[str, Any]:
        normalized_namespace = str(namespace or "").strip().lower()
        normalized_symbols = [
            str(symbol or "").strip().upper().replace(" ", "")
            for symbol in external_symbols
        ]
        items: list[Dict[str, Any] | None] = [None] * len(normalized_symbols)
        missing_positions: list[int] = []
        for position, external_symbol in enumerate(normalized_symbols):
            row = self.metadata_repository.find_instrument_by_mapping(
                normalized_namespace, external_symbol
            )
            if row is None:
                missing_positions.append(position)
                continue
            row = _decode_database_value(row)
            instrument = canonical_instrument(row["instrument_id"])
            items[position] = {
                "namespace": normalized_namespace,
                "external_symbol": external_symbol,
                "status": "resolved",
                "instrument_id": instrument.instrument_id,
                "symbol": instrument.symbol,
                "mic": instrument.mic,
                "market": instrument.market,
                "currency": str(row.get("currency") or ""),
                "source": "instrument_mapping_registry",
                "source_exchange": None,
                "observed_at": str(row.get("updated_at") or ""),
                "resolution": "registry",
            }

        if missing_positions and normalized_namespace != "provider:longport":
            for position in missing_positions:
                items[position] = {
                    "namespace": normalized_namespace,
                    "external_symbol": normalized_symbols[position],
                    "status": "error",
                    "error": {
                        "code": "provider_unavailable",
                        "message": (
                            "dynamic instrument resolution is unavailable for "
                            f"{normalized_namespace}"
                        ),
                    },
                }
        elif missing_positions:
            provider = self.longport_quote_provider
            if not provider.configured:
                provider_results = [{
                    "external_symbol": normalized_symbols[position],
                    "status": "error",
                    "error": {
                        "code": "provider_unavailable",
                        "message": "LongPort credentials are not configured",
                    },
                } for position in missing_positions]
            else:
                try:
                    provider_results = provider.resolve_instruments([
                        normalized_symbols[position] for position in missing_positions
                    ])
                    record_health = getattr(
                        self.metadata_repository, "record_provider_health", None
                    )
                    if callable(record_health):
                        record_health("longport", True, utc_now())
                except Exception as exc:
                    record_health = getattr(
                        self.metadata_repository, "record_provider_health", None
                    )
                    if callable(record_health):
                        record_health("longport", False, utc_now(), str(exc))
                    provider_results = [{
                        "external_symbol": normalized_symbols[position],
                        "status": "error",
                        "error": {
                            "code": "provider_unavailable",
                            "message": str(exc),
                        },
                    } for position in missing_positions]

            observed_at = utc_now()
            provider_name = normalized_namespace.split(":", 1)[1]
            for position, resolved in zip(missing_positions, provider_results):
                if resolved["status"] == "error":
                    items[position] = {
                        "namespace": normalized_namespace,
                        "external_symbol": normalized_symbols[position],
                        "status": "error",
                        "error": resolved["error"],
                    }
                    continue
                try:
                    existing = self.metadata_repository.get_instrument(
                        resolved["instrument_id"]
                    )
                    existing = _decode_database_value(existing)
                    if existing is None:
                        payload = {
                            "schema_version": 1,
                            "instrument_id": resolved["instrument_id"],
                            "instrument_type": "equity",
                            "asset_class": "equity",
                            "symbol": resolved["symbol"],
                            "market": resolved["market"],
                            "mic": resolved["mic"],
                            "currency": resolved["currency"],
                            "price_precision": 2,
                            "size_precision": 0,
                            "tick_size": "0.01",
                            "size_increment": "1",
                            "lot_size": str(resolved["lot_size"]),
                            "ts_event": observed_at,
                            "ts_init": observed_at,
                            "provider_symbols": {
                                provider_name: normalized_symbols[position]
                            },
                            "broker_symbols": {},
                        }
                    else:
                        payload = _instrument_contract_payload(existing)
                        payload["provider_symbols"] = dict(
                            payload["provider_symbols"]
                        )
                        payload["provider_symbols"][provider_name] = (
                            normalized_symbols[position]
                        )
                    contract = InstrumentContract.model_validate(payload)
                    validate_instrument_identity(contract)
                    normalized = contract.model_dump(mode="json")
                    saved = self.metadata_repository.upsert_instrument({
                        **normalized,
                        "content_hash": canonical_hash(normalized),
                        "updated_at": observed_at,
                    })
                    saved = _decode_database_value(saved)
                except ValueError as exc:
                    items[position] = {
                        "namespace": normalized_namespace,
                        "external_symbol": normalized_symbols[position],
                        "status": "error",
                        "error": {"code": "ambiguous", "message": str(exc)},
                    }
                    continue
                except Exception:
                    items[position] = {
                        "namespace": normalized_namespace,
                        "external_symbol": normalized_symbols[position],
                        "status": "error",
                        "error": {
                            "code": "provider_unavailable",
                            "message": "instrument registry write failed",
                        },
                    }
                    continue
                instrument = canonical_instrument(saved["instrument_id"])
                items[position] = {
                    "namespace": normalized_namespace,
                    "external_symbol": normalized_symbols[position],
                    "status": "resolved",
                    "instrument_id": instrument.instrument_id,
                    "symbol": instrument.symbol,
                    "mic": instrument.mic,
                    "market": instrument.market,
                    "currency": str(saved.get("currency") or resolved["currency"]),
                    "source": resolved["source"],
                    "source_exchange": resolved["source_exchange"],
                    "observed_at": observed_at,
                    "resolution": "upstream",
                }

        resolved_count = sum(item["status"] == "resolved" for item in items if item)
        return {
            "namespace": normalized_namespace,
            "count": len(items),
            "resolved_count": resolved_count,
            "error_count": len(items) - resolved_count,
            "items": items,
        }

    def refresh_quote(
        self, symbol: str, provider: str | None = None, allow_fallback: bool = False,
        stale_max_seconds: float | None = None,
    ) -> Dict[str, Any]:
        run_id, started_at = self._start_run("refresh_quote", symbol)
        market, normalized = self._quote_market(symbol)
        priority = tuple(
            {"sina_finance_hq": "sina", "eastmoney_quote_center": "eastmoney",
             "yahoo_chart": "yahoo"}.get(item, item)
            for item in self.settings.clickhouse_source_priority
        )
        provider_names = select_providers(
            REALTIME_QUOTE, market, provider, priority,
            allow_fallback=allow_fallback if provider else True,
        )
        row = None
        provider_errors = []
        for provider_name in provider_names:
            selected = self._quote_provider(provider_name)
            if not getattr(selected, "configured", True):
                if provider:
                    provider_errors.append(f"{provider_name}: provider is not configured")
                continue
            try:
                row = self._fetch_realtime_quote(provider_name, normalized)
                break
            except Exception as exc:
                self.metadata_repository.record_provider_health(provider_name, False, utc_now(), str(exc))
                provider_errors.append(f"{provider_name}: {exc}")
        if row is None:
            cached = self.market_bar_repository.get_latest_quotes([normalized])
            if cached and (
                stale_max_seconds is None
                or self._quote_cache_age_seconds(cached[0]) <= stale_max_seconds
            ):
                cached_row = cached[0]
                cached_row.update({
                    "is_cached": True, "cached": True, "stale": True,
                    "cache_status": "stale_fallback",
                    "cache_reason": "; ".join(provider_errors),
                    "served_at": utc_now(),
                })
                self._finish_run(run_id, "refresh_quote", started_at, symbol, 1, "; ".join(provider_errors))
                return cached_row
            error = "; ".join(provider_errors) or "no quote provider available"
            self._finish_run(run_id, "refresh_quote", started_at, symbol, 0, error)
            raise ProviderUnavailable(
                error, provider=provider or "auto", capability=REALTIME_QUOTE, market=market
            )
        row = self._persist_quotes_async([row], provider_name)[0]
        self._finish_run(run_id, "refresh_quote", started_at, symbol, 1)
        return row

    @staticmethod
    def _quote_cache_age_seconds(row: Dict[str, Any]) -> float:
        value = row.get("ingested_at") or row.get("observed_at") or row.get("quote_at")
        if not value:
            return float("inf")
        try:
            timestamp = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
            if timestamp.tzinfo is None:
                timestamp = timestamp.replace(tzinfo=timezone.utc)
            return max(
                0.0,
                (datetime.now(timezone.utc) - timestamp.astimezone(timezone.utc)).total_seconds(),
            )
        except (TypeError, ValueError):
            return float("inf")

    def get_quote(
        self, symbol: str, force_refresh: bool = False,
        provider: str | None = None, allow_fallback: bool = False,
    ) -> Dict[str, Any]:
        cached = self.market_bar_repository.get_latest_quotes([symbol])
        cached_row = cached[0] if cached else None
        cache_age = self._quote_cache_age_seconds(cached_row) if cached_row else float("inf")
        if cached_row and not force_refresh and cache_age <= self.settings.quote_cache_ttl_seconds:
            cached_row.update({
                "is_cached": True, "cached": True, "stale": False,
                "cache_status": "fresh", "cache_age_seconds": round(cache_age, 3),
                "served_at": utc_now(),
            })
            return cached_row
        try:
            return self.refresh_quote(
                symbol, provider=provider, allow_fallback=allow_fallback,
                stale_max_seconds=self.settings.quote_stale_max_seconds,
            )
        except Exception:
            if cached_row and cache_age <= self.settings.quote_stale_max_seconds:
                cached_row.update({
                    "is_cached": True, "cached": True, "stale": True,
                    "cache_status": "stale_fallback",
                    "cache_age_seconds": round(cache_age, 3), "served_at": utc_now(),
                })
                return cached_row
            raise

    def get_quote_spread(self, symbol: str) -> Dict[str, Any]:
        """Return an uncached LongPort order-book snapshot and top-of-book spread."""

        return self.longport_quote_provider.fetch_spread(symbol)

    def get_instrument_relationship(
        self, derivative_instrument_id: str, underlying_instrument_id: str,
    ) -> Dict[str, Any]:
        relationship_id = (
            f"{derivative_instrument_id}~{underlying_instrument_id}"
        )
        getter = getattr(
            self.metadata_repository, "get_instrument_relationship", None
        )
        if getter is not None:
            saved = getter(relationship_id)
            if saved is not None:
                result = dict(saved)
                for field in ("quantity_multiplier", "price_multiplier"):
                    result[field] = format(Decimal(str(result[field])), "f")
                result.pop("updated_at", None)
                return result
        derivative = self.metadata_repository.get_instrument(
            derivative_instrument_id
        )
        underlying = self.metadata_repository.get_instrument(
            underlying_instrument_id
        )
        if derivative is None or underlying is None:
            raise ValueError("relationship instrument is unavailable")
        return exact_relationship(derivative, underlying)

    def get_cross_market_snapshot(
        self, derivative_instrument_id: str, underlying_instrument_id: str,
        *, depth: int, max_age_ms: int, max_skew_ms: int,
    ) -> Dict[str, Any]:
        relationship = self.get_instrument_relationship(
            derivative_instrument_id, underlying_instrument_id
        )
        underlying = self.metadata_repository.get_instrument(
            underlying_instrument_id
        )
        if underlying is None:
            raise ValueError("underlying instrument is unavailable")
        derivative_book, underlying_book, context = load_cross_market_inputs(
            self.hyperliquid_provider, self.longport_quote_provider,
            derivative_instrument_id, underlying["symbol"], depth,
        )
        return cross_market_snapshot(
            relationship, derivative_book, underlying_book, context,
            max_age_ms=max_age_ms, max_skew_ms=max_skew_ms,
        )

    def refresh_quote_history(
        self, symbol: str, range_: str, interval: str, adjustment: str,
        provider: str | None = None, allow_fallback: bool = False,
    ) -> Dict[str, Any]:
        instrument = canonical_instrument(symbol)
        market, normalized = instrument.market, instrument.instrument_id
        priority = tuple(
            {"yahoo_chart": "yahoo"}.get(item, item)
            for item in self.settings.clickhouse_source_priority
        )
        names = select_providers(
            MARKET_BAR_HISTORY, market, provider, priority,
            allow_fallback=allow_fallback if provider else True,
        )
        errors: list[str] = []
        for name in names:
            if name == "tushare":
                if not self.tushare_provider.configured:
                    errors.append("tushare: provider is not configured")
                    continue
                if interval not in {"1m", "5m", "15m", "30m", "60m", "1h"}:
                    errors.append("tushare: interval is not supported by this adapter")
                    continue
                return self.refresh_tushare_minute_history(normalized, range_, interval, adjustment)
            if name == "yahoo":
                break
            if name == "hyperliquid":
                run_id, started_at = self._start_run(
                    "refresh_quote_history", symbol
                )
                try:
                    result = self.hyperliquid_provider.fetch_history(
                        normalized, range_, interval, adjustment
                    )
                    return self._persist_history_result(
                        result, range_, interval, adjustment,
                        run_id, started_at, symbol,
                    )
                except Exception as exc:
                    self.metadata_repository.record_provider_health(
                        "hyperliquid", False, utc_now(), str(exc)
                    )
                    self._finish_run(
                        run_id, "refresh_quote_history", started_at, symbol, 0,
                        str(exc),
                    )
                    raise
        else:
            raise ProviderUnavailable(
                "; ".join(errors) or "no history provider available",
                provider=provider or "auto", capability=MARKET_BAR_HISTORY, market=market,
            )
        run_id, started_at = self._start_run("refresh_quote_history", symbol)
        try:
            result = self.quote_provider.fetch_history(
                instrument.instrument_id, range_, interval, adjustment,
            )
        except Exception as exc:
            provider = getattr(self.quote_provider, "name", self.quote_provider.__class__.__name__)
            self.metadata_repository.record_provider_health(provider, False, utc_now(), str(exc))
            self._finish_run(run_id, "refresh_quote_history", started_at, symbol, 0, str(exc))
            raise
        return self._persist_history_result(
            result, range_, interval, adjustment, run_id, started_at, symbol
        )

    def refresh_quote_history_window(
        self, symbol: str, start: datetime, end: datetime, interval: str,
        adjustment: str, provider: str, allow_fallback: bool = False,
        ingestion_id: str | None = None,
    ) -> Dict[str, Any]:
        if start.tzinfo is None or end.tzinfo is None or start >= end:
            raise ValueError("history window must be ordered and timezone-aware")
        instrument = canonical_instrument(symbol)
        market, normalized = instrument.market, instrument.instrument_id
        priority = tuple(
            {"yahoo_chart": "yahoo"}.get(item, item)
            for item in self.settings.clickhouse_source_priority
        )
        names = select_providers(
            MARKET_BAR_HISTORY, market, provider, priority,
            allow_fallback=allow_fallback,
        )
        errors: list[str] = []
        range_label = (
            f"{start.astimezone(timezone.utc).isoformat()}/"
            f"{end.astimezone(timezone.utc).isoformat()}"
        )
        for name in names:
            run_id, started_at = self._start_run(
                "refresh_quote_history_window", symbol
            )
            try:
                if name == "tushare":
                    if not self.tushare_provider.configured:
                        raise ProviderUnavailable(
                            "tushare provider is not configured",
                            provider=name, capability=MARKET_BAR_HISTORY,
                            market=market,
                        )
                    return self.refresh_tushare_minute_history_window(
                        normalized, start, end, interval, adjustment,
                        range_label=range_label, ingestion_id=ingestion_id,
                    )
                if name == "hyperliquid":
                    result = self.hyperliquid_provider.fetch_history_window(
                        normalized, start, end, interval, adjustment
                    )
                elif name == "yahoo":
                    result = self.quote_provider.fetch_history_window(
                        instrument.instrument_id, start, end, interval, adjustment
                    )
                else:
                    raise ProviderNotSupported(
                        f"provider {name!r} has no history window adapter",
                        provider=name, capability=MARKET_BAR_HISTORY,
                        market=market,
                    )
                return self._persist_history_result(
                    result, range_label, interval, adjustment,
                    run_id, started_at, symbol, ingestion_id=ingestion_id,
                )
            except Exception as exc:
                self._finish_run(
                    run_id, "refresh_quote_history_window", started_at,
                    symbol, 0, str(exc),
                )
                errors.append(f"{name}: {exc}")
                if not allow_fallback:
                    raise
        raise ProviderUnavailable(
            "; ".join(errors) or "no history window provider available",
            provider=provider, capability=MARKET_BAR_HISTORY, market=market,
        )

    def _persist_history_result(
        self, result: Dict[str, Any], range_: str, interval: str,
        adjustment: str, run_id: str, started_at: str, requested_symbol: str,
        ingestion_id: str | None = None,
    ) -> Dict[str, Any]:
        raw_payload = result.pop("_raw_payload")
        ingested_at = utc_now()
        result["provider_symbol"] = result.get("provider_symbol") or result["symbol"]
        result["symbol"] = canonical_instrument(requested_symbol).instrument_id
        artifact = self._save_quote_raw(
            result["symbol"],
            "history-{0}-{1}-{2}".format(range_, interval, adjustment),
            raw_payload, ingested_at, result["source"], result["source_url"],
            result["raw_response_locator"],
            {"ingestion_id": ingestion_id} if ingestion_id else None,
        )
        for bar in result["bars"]:
            applicability = bar.get("factor_applicability")
            if applicability == "applicable":
                bar["factor_artifact_id"] = artifact["artifact_id"]
                bar["factor_as_of"] = ingested_at
            if applicability in {"applicable", "not_applicable"}:
                contract = PriceAdjustmentContract.model_validate({
                    key: bar.get(key) for key in (
                        "factor_applicability", "corporate_action_factor",
                        "applied_adjustment_multiplier",
                        "adjustment_reference_date", "reference_factor",
                        "factor_source", "factor_artifact_id", "factor_as_of",
                    )
                } | {"adjustment": adjustment})
                bar.update(contract.model_dump())
        count = self.market_bar_repository.upsert_price_bars(
            result["symbol"], interval, adjustment, result["source"], ingested_at, result["bars"],
            {
                **result, "raw_path": artifact["storage_path"],
                "raw_artifact_id": artifact["artifact_id"],
                "observed_at": ingested_at, "ingestion_id": ingestion_id,
            },
        )
        result.update({
            "count": count, "observed_at": ingested_at,
            "ingested_at": ingested_at, "raw_path": artifact["storage_path"],
            "raw_artifact_id": artifact["artifact_id"],
            "ingestion_id": ingestion_id,
        })
        self.metadata_repository.record_provider_health(result["source"], True, ingested_at)
        self._finish_run(
            run_id, "refresh_quote_history", started_at, requested_symbol, count
        )
        return result

    def refresh_hyperliquid_instruments(self) -> Dict[str, Any]:
        rows = self.hyperliquid_provider.instruments(force=True)
        saved = []
        observed_at = utc_now()
        for source in rows:
            payload = {
                key: value for key, value in source.items()
                if key != "venue_metadata"
            }
            payload["schema_version"] = 1
            contract = InstrumentContract.model_validate(payload)
            validate_instrument_identity(contract)
            normalized = contract.model_dump(mode="json")
            row = {
                **normalized, "content_hash": canonical_hash(normalized),
                "updated_at": observed_at,
            }
            self.metadata_repository.upsert_instrument(row)
            saved.append(row)
        relationship_count = 0
        upsert_relationship = getattr(
            self.metadata_repository, "upsert_instrument_relationship", None
        )
        if upsert_relationship is not None:
            for derivative in saved:
                if derivative["instrument_type"] != "equity_perpetual":
                    continue
                ticker = derivative["symbol"].removesuffix("-PERP")
                underlying = None
                for mic in ("XNAS", "XNYS", "ARCX"):
                    underlying = self.metadata_repository.get_instrument(
                        f"{ticker}.{mic}"
                    )
                    if underlying is not None:
                        break
                if underlying is None:
                    continue
                relationship = exact_relationship(derivative, underlying)
                upsert_relationship({
                    **relationship, "updated_at": observed_at,
                })
                relationship_count += 1
        return {
            "provider": "hyperliquid", "count": len(saved),
            "relationship_count": relationship_count,
            "observed_at": observed_at,
        }

    def refresh_hyperliquid_funding(
        self, symbol: str, start: datetime, end: datetime,
    ) -> Dict[str, Any]:
        result = self.hyperliquid_provider.fetch_funding_history(
            symbol, start, end
        )
        payload = result.pop("_raw_payload")
        observed_at = utc_now()
        artifact = self._write_artifact(
            self.settings.raw_path / "hyperliquid" / "funding",
            "hyperliquid_funding", payload, result["source"],
            result["source_url"], "fundingHistory", observed_at, observed_at,
            {
                "instrument_id": result["instrument_id"],
                "start": start.astimezone(timezone.utc).isoformat(),
                "end": end.astimezone(timezone.utc).isoformat(),
                "row_count": result["count"],
            },
        )
        return {
            **result, "observed_at": observed_at,
            "raw_path": artifact["storage_path"],
            "raw_artifact_id": artifact["artifact_id"],
        }

    def _prepare_calendar_rows(self, dataset: str, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        observed_at = utc_now()
        deduplicated: Dict[str, Dict[str, Any]] = {}
        for index, row in enumerate(rows):
            identity = str(row.get("event_id") or row.get("indicator_id") or index)
            deduplicated[identity] = row
        grouped: Dict[str, List[Dict[str, Any]]] = {}
        for row in deduplicated.values():
            grouped.setdefault(str(row.get("source") or "unknown"), []).append(row)
        prepared: List[Dict[str, Any]] = []
        for source, source_rows in grouped.items():
            source_url = str(source_rows[0].get("source_url") or "")
            artifact = self._write_artifact(
                self.settings.raw_path / "calendars" / dataset,
                dataset,
                [row.get("_raw_payload") for row in source_rows],
                source,
                source_url,
                "items",
                observed_at,
                observed_at,
                {"row_count": len(source_rows)},
            )
            for source_row in source_rows:
                raw_payload = source_row.pop("_raw_payload", None)
                source_row.update({
                    "payload": raw_payload,
                    "observed_at": observed_at,
                    "ingested_at": observed_at,
                    "raw_path": artifact["storage_path"],
                    "raw_artifact_id": artifact["artifact_id"],
                })
                prepared.append(source_row)
        return prepared

    def refresh_economic_calendar(self, date_from: str, date_to: str, country: str = "US") -> Dict[str, Any]:
        job_name = "refresh_economic_calendar"
        run_id, started_at = self._start_run(job_name, date_from + ":" + date_to)
        try:
            rows = self.calendar_provider.fetch_economic_calendar(date_from, date_to, country)
            prepared = self._prepare_calendar_rows("economic_calendar", rows)
            count = self.metadata_repository.upsert_economic_calendar(prepared)
            self.metadata_repository.record_provider_health("economic_calendar", True, utc_now())
            self._finish_run(run_id, job_name, started_at, date_from + ":" + date_to, count)
            return {"status": "success", "saved": count, "events": prepared}
        except Exception as exc:
            self.metadata_repository.record_provider_health("economic_calendar", False, utc_now(), str(exc))
            self._finish_run(run_id, job_name, started_at, date_from + ":" + date_to, 0, str(exc))
            raise

    def refresh_economic_indicators(self) -> Dict[str, Any]:
        job_name = "refresh_economic_indicators"
        run_id, started_at = self._start_run(job_name)
        try:
            rows = self.calendar_provider.fetch_economic_indicators()
            prepared = self._prepare_calendar_rows("economic_indicators", rows)
            count = self.metadata_repository.upsert_economic_indicators(prepared)
            self.metadata_repository.record_provider_health("economic_indicators", True, utc_now())
            self._finish_run(run_id, job_name, started_at, "", count)
            return {"status": "success", "saved": count, "indicators": prepared}
        except Exception as exc:
            self.metadata_repository.record_provider_health("economic_indicators", False, utc_now(), str(exc))
            self._finish_run(run_id, job_name, started_at, "", 0, str(exc))
            raise

    def refresh_earnings_calendar(
        self, date_from: str, date_to: str, market: str = "", symbols: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        job_name = "refresh_earnings_calendar"
        run_id, started_at = self._start_run(job_name, date_from + ":" + date_to)
        try:
            rows = self.calendar_provider.fetch_earnings_calendar(date_from, date_to, market, symbols)
            prepared = self._prepare_calendar_rows("earnings_calendar", rows)
            count = self.metadata_repository.upsert_earnings_calendar(prepared)
            self.metadata_repository.record_provider_health("earnings_calendar", True, utc_now())
            self._finish_run(run_id, job_name, started_at, date_from + ":" + date_to, count)
            return {"status": "success", "saved": count, "events": prepared}
        except Exception as exc:
            self.metadata_repository.record_provider_health("earnings_calendar", False, utc_now(), str(exc))
            self._finish_run(run_id, job_name, started_at, date_from + ":" + date_to, 0, str(exc))
            raise

    def calendar_snapshot(self, date_from: str, date_to: str, limit: int = 50) -> Dict[str, Any]:
        return {
            "generated_at": utc_now(),
            "filter_timezone": "Asia/Shanghai",
            "date_format": "YYYY-MM-DD",
            "quotes": [],
            "economic_calendar": self.metadata_repository.get_economic_calendar(date_from, date_to, limit=limit),
            "economic_indicators": self.metadata_repository.get_economic_indicators(limit=limit),
            "earnings_calendar": self.metadata_repository.get_earnings_calendar(date_from, date_to, limit=limit),
        }

    def _save_raw(self, dataset: str, report_period: str, records: List[Dict[str, Any]]) -> Dict[str, Any]:
        folder = self.settings.raw_path / "a_share_fundamentals" / report_period
        ingested_at = utc_now()
        source_url = EASTMONEY_QUOTE_URL if dataset == "valuation" else EASTMONEY_DATA_URL
        source = "eastmoney_quote_center" if dataset == "valuation" else "akshare_eastmoney_financials"
        return self._write_artifact(
            folder, "a_share_fundamentals_" + dataset,
            [safe_record(row) for row in records], source, source_url, "payload.records",
            ingested_at, ingested_at, {"report_period": report_period, "dataset_name": dataset},
        )

    def _load_raw(self, dataset: str, report_period: str) -> Optional[Dict[str, Any]]:
        manifest = self.artifact_store.latest_artifact("a_share_fundamentals_" + dataset, "report_period", report_period)
        if manifest:
            try:
                body = json.loads(Path(manifest["storage_path"]).read_text(encoding="utf-8"))
                return {"records": body.get("payload") or [], "observed_at": body.get("observed_at"), "manifest": manifest}
            except (OSError, ValueError, TypeError):
                pass
        path = self.settings.raw_path / "a_share_fundamentals" / report_period / (dataset + ".json")
        if not path.exists():
            return None
        try:
            artifact = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return None
        if not isinstance(artifact, dict) or not isinstance(artifact.get("records"), list):
            return None
        return artifact

    def refresh_market_fundamentals(
        self, report_period: str = "", include_valuation: bool = True
    ) -> Dict[str, Any]:
        report_period = normalize_report_period(report_period or latest_broad_report_period())
        run_id = uuid.uuid4().hex
        started_at = utc_now()
        self.metadata_repository.save_run(
            [run_id, "refresh_market_fundamentals", "running", report_period, started_at, None, 0, None]
        )
        try:
            frames = self.financial_provider.fetch_market_summaries(report_period)
            self.metadata_repository.record_provider_health(getattr(self.financial_provider, "name", "akshare_eastmoney_financials"), True, utc_now())
            raw_records: Dict[str, List[Dict[str, Any]]] = {
                name: [safe_record(item) for item in frame.to_dict("records")]
                for name, frame in frames.items()
            }
            valuation_status = "disabled"
            valuation_warning = None
            valuation_observed_at = None
            valuations: List[Dict[str, Any]] = []
            if include_valuation:
                try:
                    valuations = self.spot_provider.fetch_all()
                    self.metadata_repository.record_provider_health(getattr(self.spot_provider, "name", "eastmoney_quote_center"), True, utc_now())
                    valuation_status = "fresh"
                    valuation_observed_at = utc_now()
                except Exception as exc:
                    self.metadata_repository.record_provider_health(getattr(self.spot_provider, "name", "eastmoney_quote_center"), False, utc_now(), str(exc))
                    cached = self._load_raw("valuation", report_period)
                    if not cached:
                        raise
                    valuations = [safe_record(item) for item in cached["records"]]
                    valuation_status = "cached_after_provider_error"
                    valuation_observed_at = _text(cached.get("observed_at"))
                    if cached.get("manifest"):
                        artifacts_from_cache = cached["manifest"]
                    else:
                        artifacts_from_cache = None
                    valuation_warning = str(exc)
            raw_records["valuation"] = [safe_record(item) for item in valuations]
            artifacts: Dict[str, Dict[str, Any]] = {}
            for name, records in raw_records.items():
                if name != "valuation" or valuation_status == "fresh":
                    artifacts[name] = self._save_raw(name, report_period, records)
            if include_valuation and valuation_status == "cached_after_provider_error" and artifacts_from_cache:
                artifacts["valuation"] = artifacts_from_cache

            performance = _records_by_symbol(frames["performance"])
            balance = _records_by_symbol(frames["balance"])
            income = _records_by_symbol(frames["income"])
            cashflow = _records_by_symbol(frames["cashflow"])
            valuation = {str(row["symbol"]).zfill(6): row for row in valuations}
            symbols = sorted(set(performance) | set(balance) | set(income) | set(cashflow) | set(valuation))
            fetched_at = utc_now()
            row_artifacts = [artifacts[name] for name in ("performance", "balance", "income", "cashflow", "valuation") if name in artifacts]
            provenance_join = lambda key: ";".join(str(item[key]) for item in row_artifacts if item.get(key)) or None
            valuation_as_of = valuation_observed_at[:10] if valuation_observed_at else None
            rows: List[Dict[str, Any]] = []
            for symbol in symbols:
                p = performance.get(symbol, {})
                b = balance.get(symbol, {})
                i = income.get(symbol, {})
                c = cashflow.get(symbol, {})
                v = valuation.get(symbol, {})
                publication_dates = [
                    _text(_first(p, "最新公告日期", "公告日期")),
                    _text(_first(b, "公告日期")),
                    _text(_first(i, "公告日期")),
                    _text(_first(c, "公告日期")),
                ]
                publication_dates = [item for item in publication_dates if item]
                row: Dict[str, Any] = {
                    "instrument_id": instrument_id(symbol),
                    "symbol": symbol,
                    "exchange": exchange_for_symbol(symbol),
                    "name": _text(_first(p, "股票简称")) or _text(_first(i, "股票简称")) or _text(v.get("name")),
                    "is_active": symbol in valuation if include_valuation else None,
                    "report_period": report_period,
                    "published_at": max(publication_dates) if publication_dates else None,
                    "valuation_as_of": valuation_as_of,
                    "price": _number(v.get("price")),
                    "change_pct": _number(v.get("change_pct")),
                    "pe_dynamic": _number(v.get("pe_dynamic")),
                    "pb": _number(v.get("pb")),
                    "total_market_cap": _number(v.get("total_market_cap")),
                    "float_market_cap": _number(v.get("float_market_cap")),
                    "roe_weighted": _number(_first(p, "净资产收益率")),
                    "eps": _number(_first(p, "每股收益")),
                    "revenue": _coalesce_number(
                        _first(i, "营业总收入", "营业总收入-营业总收入", "营业收入"),
                        _first(p, "营业总收入-营业总收入"),
                    ),
                    "revenue_yoy": _coalesce_number(
                        _first(i, "营业总收入同比", "营业收入同比"),
                        _first(p, "营业总收入-同比增长"),
                    ),
                    "revenue_qoq": _number(_first(p, "营业总收入-季度环比增长")),
                    "net_profit": _coalesce_number(_first(i, "净利润"), _first(p, "净利润-净利润")),
                    "net_profit_yoy": _coalesce_number(_first(i, "净利润同比"), _first(p, "净利润-同比增长")),
                    "net_profit_qoq": _number(_first(p, "净利润-季度环比增长")),
                    "book_value_per_share": _number(_first(p, "每股净资产")),
                    "ocf_per_share": _number(_first(p, "每股经营现金流量")),
                    "gross_margin": _number(_first(p, "销售毛利率")),
                    "industry": _text(_first(p, "所处行业")),
                    "cash": _number(_first(b, "资产-货币资金")),
                    "accounts_receivable": _number(_first(b, "资产-应收账款")),
                    "inventory": _number(_first(b, "资产-存货")),
                    "total_assets": _number(_first(b, "资产-总资产")),
                    "total_assets_yoy": _number(_first(b, "资产-总资产同比")),
                    "accounts_payable": _number(_first(b, "负债-应付账款")),
                    "advance_receipts": _number(_first(b, "负债-预收账款")),
                    "total_liabilities": _number(_first(b, "负债-总负债")),
                    "total_liabilities_yoy": _number(_first(b, "负债-总负债同比")),
                    "debt_ratio": _number(_first(b, "资产负债率")),
                    "total_equity": _number(_first(b, "股东权益合计")),
                    "operating_cost": _number(_first(i, "营业总支出-营业支出")),
                    "sales_expense": _number(_first(i, "营业总支出-销售费用")),
                    "admin_expense": _number(_first(i, "营业总支出-管理费用")),
                    "financial_expense": _number(_first(i, "营业总支出-财务费用")),
                    "total_operating_expense": _number(_first(i, "营业总支出-营业总支出")),
                    "operating_profit": _number(_first(i, "营业利润")),
                    "total_profit": _number(_first(i, "利润总额")),
                    "net_cashflow": _number(_first(c, "净现金流-净现金流")),
                    "net_cashflow_yoy": _number(_first(c, "净现金流-同比增长")),
                    "operating_cashflow": _number(_first(c, "经营性现金流-现金流量净额")),
                    "investing_cashflow": _number(_first(c, "投资性现金流-现金流量净额")),
                    "financing_cashflow": _number(_first(c, "融资性现金流-现金流量净额")),
                    "source": "eastmoney via akshare; eastmoney quote center",
                    "source_url": provenance_join("source_url") or EASTMONEY_DATA_URL,
                    "observed_at": max((item.get("observed_at") or fetched_at) for item in row_artifacts) if row_artifacts else fetched_at,
                    "ingested_at": max((item.get("ingested_at") or fetched_at) for item in row_artifacts) if row_artifacts else fetched_at,
                    "raw_response_locator": "performance|balance|income|cashflow|valuation payload.records[symbol={0}]".format(symbol),
                    "raw_path": provenance_join("storage_path"),
                    "raw_artifact_id": provenance_join("artifact_id"),
                    "quality_status": (
                        "single_independent_source_unverified;valuation_cached"
                        if valuation_status == "cached_after_provider_error" and symbol in valuation
                        else "single_independent_source_unverified"
                    ),
                    "fetched_at": fetched_at,
                }
                rows.append({column: row.get(column) for column in FUNDAMENTAL_COLUMNS})
            count = self.fundamental_repository.replace_fundamentals(report_period, rows)
            finished_at = utc_now()
            self.metadata_repository.save_run(
                [run_id, "refresh_market_fundamentals", "success", report_period, started_at, finished_at, count, None]
            )
            return {
                "run_id": run_id,
                "status": "success",
                "report_period": report_period,
                "row_count": count,
                "active_count": len(valuation) if include_valuation else None,
                "valuation_status": valuation_status,
                "warnings": [valuation_warning] if valuation_warning else [],
                "coverage": {name: len(records) for name, records in raw_records.items()},
                "started_at": started_at,
                "finished_at": finished_at,
            }
        except Exception as exc:
            self.metadata_repository.record_provider_health(getattr(self.financial_provider, "name", "akshare_eastmoney_financials"), False, utc_now(), str(exc))
            self.metadata_repository.save_run(
                [run_id, "refresh_market_fundamentals", "failed", report_period, started_at, utc_now(), 0, str(exc)]
            )
            raise

    @staticmethod
    def eastmoney_symbol(symbol: str) -> str:
        symbol = "".join(ch for ch in str(symbol) if ch.isdigit()).zfill(6)
        exchange = exchange_for_symbol(symbol)
        prefix = {"XSHG": "SH", "XSHE": "SZ", "XBSE": "BJ"}[exchange]
        return prefix + symbol

    def refresh_company_statements(self, symbol: str) -> Dict[str, Any]:
        symbol = "".join(ch for ch in str(symbol) if ch.isdigit()).zfill(6)
        if len(symbol) != 6:
            raise ValueError("symbol must contain a six-digit A-share code")
        source_symbol = self.eastmoney_symbol(symbol)
        run_id, started_at = self._start_run("refresh_company_statements", symbol)
        frames = self.financial_provider.fetch_company_statements(source_symbol)
        fetched_at = utc_now()
        counts: Dict[str, int] = {}
        for statement, frame in frames.items():
            records = [safe_record(record) for record in frame.to_dict("records")]
            folder = self.settings.raw_path / "company_statements" / symbol
            artifact = self._write_artifact(
                folder, "company_statement_" + statement, records,
                "akshare_eastmoney_financials", EASTMONEY_DATA_URL, "payload.records",
                fetched_at, fetched_at, {"symbol": symbol, "statement": statement},
            )
            rows: List[Dict[str, Any]] = []
            for record in records:
                report_date = _text(_first(record, "REPORT_DATE", "报告日期", "报告日", "REPORT_DATE_NAME"))
                if not report_date:
                    continue
                report_date = report_date[:10]
                published_at = _text(_first(record, "NOTICE_DATE", "公告日期", "UPDATE_DATE"))
                rows.append(
                    {
                        "instrument_id": instrument_id(symbol),
                        "symbol": symbol,
                        "statement": statement,
                        "report_date": report_date,
                        "published_at": published_at[:10] if published_at else None,
                        "source": "eastmoney via akshare",
                        "source_url": EASTMONEY_DATA_URL,
                        "observed_at": fetched_at,
                        "ingested_at": fetched_at,
                        "raw_response_locator": "payload.records[report_date={0}]".format(report_date),
                        "raw_path": artifact["storage_path"],
                        "raw_artifact_id": artifact["artifact_id"],
                        "payload": record,
                        "fetched_at": fetched_at,
                    }
                )
            counts[statement] = self.fundamental_repository.replace_statement_rows(symbol, statement, rows)
        self.metadata_repository.record_provider_health("akshare_eastmoney_financials", True, fetched_at)
        self._finish_run(run_id, "refresh_company_statements", started_at, symbol, sum(counts.values()))
        return {"symbol": symbol, "source_symbol": source_symbol, "counts": counts, "fetched_at": fetched_at}

    def refresh_baostock(self, symbol: str, report_period: str) -> Dict[str, Any]:
        symbol = "".join(ch for ch in str(symbol) if ch.isdigit()).zfill(6)
        report_period = normalize_report_period(report_period)
        run_id, started_at = self._start_run("refresh_baostock", report_period)
        valuation = self.baostock_provider.fetch_valuation(symbol)
        financials = self.baostock_provider.fetch_financials(symbol, report_period)
        profit = financials.get("profit") or {}
        operation = financials.get("operation") or {}
        growth = financials.get("growth") or {}
        balance = financials.get("balance") or {}
        cashflow = financials.get("cashflow") or {}
        dupont = financials.get("dupont") or {}
        fetched_at = utc_now()
        folder = self.settings.raw_path / "baostock" / symbol
        artifact = self._write_artifact(
            folder, "baostock_snapshot_" + report_period,
            {"valuation": valuation, "financials": financials}, "baostock",
            BAOSTOCK_SOURCE_URL, "payload", fetched_at, fetched_at,
            {"symbol": symbol, "report_period": report_period},
        )
        row = {
            "symbol": symbol,
            "report_period": report_period,
            "published_at": profit.get("pubDate") or balance.get("pubDate"),
            "trade_date": valuation.get("date"),
            "close": optional_float(valuation.get("close")),
            "pe_ttm": optional_float(valuation.get("peTTM")),
            "pb_mrq": optional_float(valuation.get("pbMRQ")),
            "ps_ttm": optional_float(valuation.get("psTTM")),
            "pcf_ncf_ttm": optional_float(valuation.get("pcfNcfTTM")),
            "trade_status": int(valuation.get("tradestatus")) if valuation.get("tradestatus") else None,
            "is_st": valuation.get("isST") == "1",
            "roe_avg": optional_float(profit.get("roeAvg"), 100.0),
            "net_margin": optional_float(profit.get("npMargin"), 100.0),
            "gross_margin": optional_float(profit.get("gpMargin"), 100.0),
            "net_profit_all": optional_float(profit.get("netProfit")),
            "eps_ttm": optional_float(profit.get("epsTTM")),
            "total_share": optional_float(profit.get("totalShare")),
            "current_ratio": optional_float(balance.get("currentRatio")),
            "quick_ratio": optional_float(balance.get("quickRatio")),
            "liability_to_asset": optional_float(balance.get("liabilityToAsset"), 100.0),
            "asset_turnover": optional_float(operation.get("AssetTurnRatio")),
            "inventory_turnover": optional_float(operation.get("INVTurnRatio")),
            "net_profit_yoy": optional_float(growth.get("YOYNI"), 100.0),
            "equity_yoy": optional_float(growth.get("YOYEquity"), 100.0),
            "asset_yoy": optional_float(growth.get("YOYAsset"), 100.0),
            "cfo_to_revenue": optional_float(cashflow.get("CFOToOR"), 100.0),
            "cfo_to_net_profit": optional_float(cashflow.get("CFOToNP"), 100.0),
            "dupont_roe": optional_float(dupont.get("dupontROE"), 100.0),
            "payload_json": json.dumps(
                {"valuation": valuation, "financials": financials}, ensure_ascii=False
            ),
            "source": "baostock", "source_url": BAOSTOCK_SOURCE_URL,
            "observed_at": fetched_at, "ingested_at": fetched_at,
            "raw_response_locator": "payload", "raw_path": artifact["storage_path"],
            "raw_artifact_id": artifact["artifact_id"],
            "fetched_at": fetched_at,
        }
        self.fundamental_repository.upsert_baostock(row)
        self.metadata_repository.record_provider_health("baostock", True, fetched_at)
        self.fundamental_repository.rebuild_funnel_metrics(utc_now())
        self._finish_run(run_id, "refresh_baostock", started_at, report_period, 1)
        return {key: value for key, value in row.items() if key != "payload_json"}

    def sync_tdx_financials(
        self, limit_periods: int = 12, report_periods: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        run_id, started_at = self._start_run("sync_tdx_financials")
        files = self.tdx_provider.list_files()
        if report_periods:
            wanted = {normalize_report_period(value) for value in report_periods}
            files = [item for item in files if item["report_period"] in wanted]
        else:
            files = files[:limit_periods]
        results: List[Dict[str, Any]] = []
        for item in files:
            frame = self.tdx_provider.fetch_and_parse(item["filename"])
            rows = self.tdx_provider.normalize(frame, item["filename"])
            ingested_at = utc_now()
            raw_path = self.settings.raw_path.parent / "tdx/financial" / item["filename"]
            source_url = "https://down.tdx.com.cn:8001/fin/" + item["filename"]
            artifact = self._register_file_artifact(
                raw_path, "tdx_financial_zip", "tdx_financial_via_mootdx", source_url,
                item["filename"], ingested_at, {"report_period": item["report_period"]},
            )
            for row in rows:
                row.update({
                    "source": "tdx_financial_via_mootdx", "source_url": source_url,
                    "observed_at": ingested_at, "ingested_at": ingested_at,
                    "raw_response_locator": item["filename"] + ":symbol=" + row["symbol"],
                    "raw_path": artifact["storage_path"], "raw_artifact_id": artifact["artifact_id"],
                    "fetched_at": ingested_at,
                })
            count = self.fundamental_repository.replace_tdx_period(item["report_period"], rows)
            results.append(
                {
                    "report_period": item["report_period"],
                    "filename": item["filename"],
                    "row_count": count,
                    "filesize": item.get("filesize"),
                }
            )
        metric_count = self.fundamental_repository.rebuild_funnel_metrics(utc_now())
        self.metadata_repository.record_provider_health("tdx_financial_via_mootdx", True, utc_now())
        self._finish_run(run_id, "sync_tdx_financials", started_at, "", sum(item["row_count"] for item in results))
        return {
            "status": "success",
            "period_count": len(results),
            "periods": results,
            "funnel_metric_count": metric_count,
        }

    def rebuild_funnel_metrics(self) -> Dict[str, Any]:
        run_id, started_at = self._start_run("rebuild_funnel_metrics")
        rebuilt_at = utc_now()
        count = self.fundamental_repository.rebuild_funnel_metrics(rebuilt_at)
        self._finish_run(run_id, "rebuild_funnel_metrics", started_at, "", count)
        return {"status": "success", "row_count": count, "rebuilt_at": rebuilt_at}

    @staticmethod
    def _difference(primary: Optional[float], secondary: Optional[float]) -> Dict[str, Any]:
        if primary is None or secondary is None or primary == 0:
            return {"difference_pct": None, "status": "missing_comparison"}
        difference = abs(primary - secondary) / abs(primary) * 100.0
        return {
            "difference_pct": round(difference, 4),
            "status": "consistent" if difference <= 1.0 else "difference_over_1pct",
        }

    def validate_company(self, symbol: str, report_period: str) -> Dict[str, Any]:
        symbol = "".join(ch for ch in str(symbol) if ch.isdigit()).zfill(6)
        report_period = normalize_report_period(report_period)
        run_id, started_at = self._start_run("validate_company", report_period)
        eastmoney_rows = self.fundamental_repository.query_fundamentals(
            symbol=symbol, report_period=report_period, limit=1, active_only=False
        )
        eastmoney = eastmoney_rows[0] if eastmoney_rows else None
        baostock = self.fundamental_repository.get_baostock(symbol, report_period)
        tdx = self.fundamental_repository.get_tdx(symbol, report_period)
        comparisons: Dict[str, Any] = {}
        pairs = {
            "roe_eastmoney_vs_baostock": (
                eastmoney.get("roe_weighted") if eastmoney else None,
                baostock.get("roe_avg") if baostock else None,
            ),
            "roe_eastmoney_vs_tdx": (
                eastmoney.get("roe_weighted") if eastmoney else None,
                tdx.get("roe_weighted") if tdx else None,
            ),
            "revenue_eastmoney_vs_tdx": (
                eastmoney.get("revenue") if eastmoney else None,
                tdx.get("revenue") if tdx else None,
            ),
            "net_profit_eastmoney_vs_tdx": (
                eastmoney.get("net_profit") if eastmoney else None,
                tdx.get("net_profit_parent") if tdx else None,
            ),
            "assets_eastmoney_vs_tdx": (
                eastmoney.get("total_assets") if eastmoney else None,
                tdx.get("total_assets") if tdx else None,
            ),
            "liabilities_eastmoney_vs_tdx": (
                eastmoney.get("total_liabilities") if eastmoney else None,
                tdx.get("total_liabilities") if tdx else None,
            ),
            "ocf_eastmoney_vs_tdx": (
                eastmoney.get("operating_cashflow") if eastmoney else None,
                tdx.get("operating_cashflow") if tdx else None,
            ),
        }
        for name, pair in pairs.items():
            comparisons[name] = {
                "primary": pair[0],
                "secondary": pair[1],
                **self._difference(pair[0], pair[1]),
            }
        observed_at = utc_now()
        validation_rows = []
        for metric, comparison in comparisons.items():
            parts = metric.split("_vs_")
            left = parts[0].rsplit("_", 1)
            source_a = left[-1] if len(left) > 1 else "primary"
            source_b = parts[1] if len(parts) > 1 else "secondary"
            validation_rows.append({
                "symbol": symbol, "report_period": report_period, "metric": metric,
                "source_a": source_a, "source_b": source_b,
                "value_a": comparison["primary"], "value_b": comparison["secondary"],
                "difference_pct": comparison["difference_pct"], "status": comparison["status"],
                "observed_at": observed_at,
            })
        self.fundamental_repository.save_validation_results(validation_rows)
        self.fundamental_repository.rebuild_funnel_metrics(observed_at)
        self._finish_run(run_id, "validate_company", started_at, report_period, len(validation_rows))
        return {
            "symbol": symbol,
            "report_period": report_period,
            "sources": {"eastmoney": eastmoney, "baostock": baostock, "tdx": tdx},
            "comparisons": comparisons,
            "persisted_count": len(validation_rows),
        }

    def rebuild_cached_validation(self, report_period: str) -> Dict[str, Any]:
        report_period = normalize_report_period(report_period)
        run_id, started_at = self._start_run("rebuild_cached_validation", report_period)
        observed_at = utc_now()
        count = self.fundamental_repository.rebuild_validation_results(report_period, observed_at)
        self.fundamental_repository.rebuild_funnel_metrics(observed_at)
        self._finish_run(run_id, "rebuild_cached_validation", started_at, report_period, count)
        return {
            "status": "success", "report_period": report_period,
            "row_count": count, "observed_at": observed_at,
        }
