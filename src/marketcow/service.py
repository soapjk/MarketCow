from __future__ import annotations

import json
import hashlib
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

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
from .providers.instrument_search import InstrumentSearchProvider
from .providers.eastmoney_realtime import EastmoneyRealtimeQuoteProvider, normalize_a_symbol
from .providers.sina_realtime import SinaRealtimeQuoteProvider
from .providers.calendar import CalendarProvider
from .providers.tushare_provider import TushareProvider
from .storage import FUNDAMENTAL_COLUMNS, Warehouse


EASTMONEY_DATA_URL = "https://datacenter-web.eastmoney.com/api/data/v1/get"
EASTMONEY_QUOTE_URL = "https://push2.eastmoney.com/api/qt/clist/get"
BAOSTOCK_SOURCE_URL = "http://baostock.com/baostock/index.php/Python_API文档"


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


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
        warehouse: Optional[Warehouse] = None,
        spot_provider: Optional[EastmoneySpotProvider] = None,
        financial_provider: Optional[AkshareFinancialProvider] = None,
        baostock_provider: Optional[BaoStockProvider] = None,
        tdx_provider: Optional[TdxFinancialProvider] = None,
        quote_provider: Optional[YahooQuoteProvider] = None,
        search_provider: Optional[InstrumentSearchProvider] = None,
        sina_quote_provider: Optional[SinaRealtimeQuoteProvider] = None,
        a_quote_provider: Optional[EastmoneyRealtimeQuoteProvider] = None,
        calendar_provider: Optional[CalendarProvider] = None,
        tushare_provider: Optional[TushareProvider] = None,
    ):
        self.settings = settings
        self.warehouse = warehouse or Warehouse(settings.database_path)
        self.spot_provider = spot_provider or EastmoneySpotProvider()
        self.financial_provider = financial_provider or AkshareFinancialProvider()
        self.baostock_provider = baostock_provider or BaoStockProvider()
        self.tdx_provider = tdx_provider or TdxFinancialProvider(
            settings.raw_path.parent / "tdx/financial"
        )
        self.quote_provider = quote_provider or YahooQuoteProvider()
        self.search_provider = search_provider or InstrumentSearchProvider()
        self.sina_quote_provider = sina_quote_provider or SinaRealtimeQuoteProvider()
        self.a_quote_provider = a_quote_provider or EastmoneyRealtimeQuoteProvider()
        self.calendar_provider = calendar_provider or CalendarProvider()
        self.tushare_provider = tushare_provider or TushareProvider(
            settings.tushare_token, settings.tushare_base_url,
            settings.tushare_realtime_url, settings.tushare_min_interval,
        )

    def _persist_tushare_response(
        self, api_name: str, params: Dict[str, Any], fields: str, result: Dict[str, Any]
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
            {"api_name": api_name, "params": params, "fields": fields, "row_count": len(rows)},
        )
        self.warehouse.save_tushare_response({
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
        self.warehouse.record_provider_health(self.tushare_provider.name, True, utc_now())
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
        params = {
            "ts_code": symbol, "freq": frequencies[interval],
            "start_date": start.strftime("%Y-%m-%d %H:%M:%S"),
            "end_date": end.strftime("%Y-%m-%d %H:%M:%S"),
        }
        result = self.tushare_provider.call("stk_mins", params, "")
        artifact = self._persist_tushare_response("stk_mins", params, "", result)
        bars = self.tushare_provider.minute_bars(result)
        ingested_at = utc_now()
        count = self.warehouse.upsert_price_bars(
            symbol, interval, "raw", self.tushare_provider.name, ingested_at, bars,
            {"source_url": self.tushare_provider.base_url + "/", "observed_at": ingested_at,
             "raw_response_locator": "data.items", "raw_path": artifact["storage_path"],
             "raw_artifact_id": artifact["artifact_id"]},
        )
        self.warehouse.record_provider_health(self.tushare_provider.name, True, ingested_at)
        return {
            "symbol": symbol, "range": range_, "interval": interval, "adjustment": "raw",
            "source": self.tushare_provider.name, "source_url": self.tushare_provider.base_url + "/",
            "raw_response_locator": "data.items", "bars": bars, "count": count,
            "observed_at": ingested_at, "ingested_at": ingested_at,
            "raw_path": artifact["storage_path"], "raw_artifact_id": artifact["artifact_id"],
        }

    def search_instruments(self, query: str, limit: int = 12) -> List[Dict[str, Any]]:
        return self.search_provider.search(query, limit)

    def _start_run(self, job_name: str, report_period: str = "") -> tuple[str, str]:
        run_id, started_at = uuid.uuid4().hex, utc_now()
        self.warehouse.save_run([run_id, job_name, "running", report_period or None, started_at, None, 0, None])
        return run_id, started_at

    def _finish_run(self, run_id: str, job_name: str, started_at: str, report_period: str, row_count: int, error: str = "") -> None:
        self.warehouse.save_run([
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
        artifact_id = uuid.uuid4().hex
        folder.mkdir(parents=True, exist_ok=True)
        stamp = ingested_at.replace(":", "").replace("+", "_").replace(".", "")
        path = folder / (dataset + "-" + stamp + "-" + artifact_id[:8] + ".json")
        body = {
            "artifact_id": artifact_id, "dataset": dataset, "source": source,
            "source_url": source_url, "observed_at": observed_at,
            "ingested_at": ingested_at, "raw_response_locator": raw_response_locator,
            "metadata": metadata or {}, "payload": payload,
        }
        encoded = json.dumps(body, ensure_ascii=False, allow_nan=False, sort_keys=True).encode("utf-8")
        path.write_bytes(encoded)
        manifest = {
            "artifact_id": artifact_id, "dataset": dataset, "source": source,
            "source_url": source_url, "observed_at": observed_at,
            "ingested_at": ingested_at, "raw_response_locator": raw_response_locator,
            "storage_path": str(path), "sha256": hashlib.sha256(encoded).hexdigest(),
            "byte_size": len(encoded), "metadata_json": json.dumps(metadata or {}, ensure_ascii=False),
        }
        self.warehouse.save_artifact(manifest)
        return manifest

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
        self.warehouse.save_artifact(manifest)
        return manifest

    def _save_quote_raw(self, symbol: str, dataset: str, payload: Dict[str, Any], ingested_at: str, source: str, source_url: str, locator: str) -> Dict[str, Any]:
        safe_symbol = "".join(ch for ch in symbol if ch.isalnum() or ch in ("-", "."))
        folder = self.settings.raw_path / "quotes" / safe_symbol
        return self._write_artifact(folder, "quote_" + dataset, payload, source, source_url, locator, ingested_at, ingested_at, {"symbol": symbol})

    def refresh_quote(
        self, symbol: str, stale_max_seconds: Optional[float] = None
    ) -> Dict[str, Any]:
        run_id, started_at = self._start_run("refresh_quote", symbol)
        try:
            a_symbol = normalize_a_symbol(symbol)
        except ValueError:
            a_symbol = None
        providers = (
            [self.sina_quote_provider, self.a_quote_provider]
            if a_symbol
            else [self.quote_provider]
        )
        normalized = a_symbol or symbol
        row = None
        provider_errors = []
        for provider in providers:
            try:
                row = provider.fetch_quote(normalized)
                self.warehouse.record_provider_health(getattr(provider, "name", provider.__class__.__name__), True, utc_now())
                break
            except Exception as exc:
                self.warehouse.record_provider_health(getattr(provider, "name", provider.__class__.__name__), False, utc_now(), str(exc))
                provider_errors.append(f"{getattr(provider, 'name', provider.__class__.__name__)}: {exc}")
        if row is None:
            cached = self.warehouse.get_latest_quotes([normalized])
            if cached and (
                stale_max_seconds is None
                or self._quote_cache_age_seconds(cached[0]) <= stale_max_seconds
            ):
                cached_row = cached[0]
                cached_row.update({
                    "is_cached": True,
                    "cached": True,
                    "stale": True,
                    "cache_status": "stale_fallback",
                    "cache_reason": "; ".join(provider_errors),
                    "served_at": utc_now(),
                })
                self._finish_run(run_id, "refresh_quote", started_at, symbol, 1, "; ".join(provider_errors))
                return cached_row
            error = "; ".join(provider_errors) or "no quote provider available"
            self._finish_run(run_id, "refresh_quote", started_at, symbol, 0, error)
            raise RuntimeError(error)
        raw_payload = row.pop("_raw_payload")
        ingested_at = utc_now()
        artifact = self._save_quote_raw(row["symbol"], "latest", raw_payload, ingested_at, row["source"], row["source_url"], row["raw_response_locator"])
        row.update({
            "observed_at": row.get("quote_at") or ingested_at,
            "ingested_at": ingested_at,
            "raw_path": artifact["storage_path"],
            "raw_artifact_id": artifact["artifact_id"],
            "is_cached": False,
            "cached": False,
            "stale": False,
            "cache_status": "refreshed",
        })
        self.warehouse.upsert_quote(row)
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
            return max(0.0, (datetime.now(timezone.utc) - timestamp.astimezone(timezone.utc)).total_seconds())
        except (TypeError, ValueError):
            return float("inf")

    def get_quote(self, symbol: str, force_refresh: bool = False) -> Dict[str, Any]:
        cached = self.warehouse.get_latest_quotes([symbol])
        cached_row = cached[0] if cached else None
        cache_age = self._quote_cache_age_seconds(cached_row) if cached_row else float("inf")
        if cached_row and not force_refresh and cache_age <= self.settings.quote_cache_ttl_seconds:
            cached_row.update({
                "is_cached": True,
                "cached": True,
                "stale": False,
                "cache_status": "fresh",
                "cache_age_seconds": round(cache_age, 3),
                "served_at": utc_now(),
            })
            return cached_row
        try:
            return self.refresh_quote(
                symbol, stale_max_seconds=self.settings.quote_stale_max_seconds
            )
        except Exception:
            # refresh_quote already attempts a cache fallback; this guard primarily
            # documents and enforces the maximum age if that behavior changes.
            if cached_row and cache_age <= self.settings.quote_stale_max_seconds:
                cached_row.update({
                    "is_cached": True,
                    "cached": True,
                    "stale": True,
                    "cache_status": "stale_fallback",
                    "cache_age_seconds": round(cache_age, 3),
                    "served_at": utc_now(),
                })
                return cached_row
            raise

    def refresh_quote_history(self, symbol: str, range_: str, interval: str, adjustment: str) -> Dict[str, Any]:
        if interval in {"1m", "5m", "15m", "30m", "60m", "1h"} and symbol.endswith((".SH", ".SZ", ".BJ")):
            return self.refresh_tushare_minute_history(symbol, range_, interval, adjustment)
        run_id, started_at = self._start_run("refresh_quote_history", symbol)
        try:
            result = self.quote_provider.fetch_history(symbol, range_, interval, adjustment)
        except Exception as exc:
            provider = getattr(self.quote_provider, "name", self.quote_provider.__class__.__name__)
            self.warehouse.record_provider_health(provider, False, utc_now(), str(exc))
            self._finish_run(run_id, "refresh_quote_history", started_at, symbol, 0, str(exc))
            raise
        raw_payload = result.pop("_raw_payload")
        ingested_at = utc_now()
        artifact = self._save_quote_raw(result["symbol"], "history-{0}-{1}-{2}".format(range_, interval, adjustment), raw_payload, ingested_at, result["source"], result["source_url"], result["raw_response_locator"])
        count = self.warehouse.upsert_price_bars(
            result["symbol"], interval, adjustment, result["source"], ingested_at, result["bars"],
            {**result, "raw_path": artifact["storage_path"], "raw_artifact_id": artifact["artifact_id"], "observed_at": ingested_at},
        )
        result.update({"count": count, "observed_at": ingested_at, "ingested_at": ingested_at, "raw_path": artifact["storage_path"], "raw_artifact_id": artifact["artifact_id"]})
        self.warehouse.record_provider_health(result["source"], True, ingested_at)
        self._finish_run(run_id, "refresh_quote_history", started_at, symbol, count)
        return result

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
            count = self.warehouse.upsert_economic_calendar(prepared)
            self.warehouse.record_provider_health("economic_calendar", True, utc_now())
            self._finish_run(run_id, job_name, started_at, date_from + ":" + date_to, count)
            return {"status": "success", "saved": count, "events": prepared}
        except Exception as exc:
            self.warehouse.record_provider_health("economic_calendar", False, utc_now(), str(exc))
            self._finish_run(run_id, job_name, started_at, date_from + ":" + date_to, 0, str(exc))
            raise

    def refresh_economic_indicators(self) -> Dict[str, Any]:
        job_name = "refresh_economic_indicators"
        run_id, started_at = self._start_run(job_name)
        try:
            rows = self.calendar_provider.fetch_economic_indicators()
            prepared = self._prepare_calendar_rows("economic_indicators", rows)
            count = self.warehouse.upsert_economic_indicators(prepared)
            self.warehouse.record_provider_health("economic_indicators", True, utc_now())
            self._finish_run(run_id, job_name, started_at, "", count)
            return {"status": "success", "saved": count, "indicators": prepared}
        except Exception as exc:
            self.warehouse.record_provider_health("economic_indicators", False, utc_now(), str(exc))
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
            count = self.warehouse.upsert_earnings_calendar(prepared)
            self.warehouse.record_provider_health("earnings_calendar", True, utc_now())
            self._finish_run(run_id, job_name, started_at, date_from + ":" + date_to, count)
            return {"status": "success", "saved": count, "events": prepared}
        except Exception as exc:
            self.warehouse.record_provider_health("earnings_calendar", False, utc_now(), str(exc))
            self._finish_run(run_id, job_name, started_at, date_from + ":" + date_to, 0, str(exc))
            raise

    def calendar_snapshot(self, date_from: str, date_to: str, limit: int = 50) -> Dict[str, Any]:
        return {
            "generated_at": utc_now(),
            "filter_timezone": "Asia/Shanghai",
            "date_format": "YYYY-MM-DD",
            "quotes": [],
            "economic_calendar": self.warehouse.get_economic_calendar(date_from, date_to, limit=limit),
            "economic_indicators": self.warehouse.get_economic_indicators(limit=limit),
            "earnings_calendar": self.warehouse.get_earnings_calendar(date_from, date_to, limit=limit),
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
        manifest = self.warehouse.latest_artifact("a_share_fundamentals_" + dataset, "report_period", report_period)
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

    def backfill_legacy_artifacts(self) -> Dict[str, Any]:
        existing = self.warehouse.artifact_paths()
        candidates = list(self.settings.raw_path.rglob("*"))
        tdx_dir = self.settings.raw_path.parent / "tdx" / "financial"
        if tdx_dir.exists():
            candidates.extend(tdx_dir.glob("*.zip"))
        rows: List[Dict[str, Any]] = []
        skipped = 0
        for path in candidates:
            if not path.is_file() or str(path) in existing:
                continue
            try:
                content = path.read_bytes()
            except OSError:
                skipped += 1
                continue
            observed_at = datetime.fromtimestamp(path.stat().st_mtime, timezone.utc).isoformat(timespec="seconds")
            dataset, source, source_url, locator = "legacy_raw", "legacy_unknown", "https://example.invalid/legacy", path.name
            metadata: Dict[str, Any] = {"legacy": True}
            parts = path.parts
            if path.suffix == ".zip" and path.name.startswith("gpcw"):
                dataset, source = "tdx_financial_zip", "tdx_financial_via_mootdx"
                source_url = "https://down.tdx.com.cn:8001/fin/" + path.name
                locator = path.name
                metadata["report_period"] = "".join(ch for ch in path.stem if ch.isdigit())
            elif "quotes" in parts:
                symbol = path.parent.name
                metadata["symbol"] = symbol
                dataset = "legacy_quote_history" if path.name.startswith("history-") else "legacy_quote_latest"
                try:
                    payload = json.loads(content)
                except (ValueError, UnicodeDecodeError):
                    payload = {}
                if isinstance(payload, dict) and "chart" in payload:
                    source, source_url, locator = "yahoo_chart", "https://query1.finance.yahoo.com/v8/finance/chart/" + symbol, "chart.result[0]"
                elif isinstance(payload, dict) and "raw_line" in payload:
                    source, source_url, locator = "sina_finance_hq", "https://hq.sinajs.cn/", "hq_str payload fields"
                else:
                    source, source_url, locator = "eastmoney_quote_center", "https://push2.eastmoney.com/api/qt/stock/get", "data"
            elif "a_share_fundamentals" in parts:
                dataset, source, source_url = "legacy_a_share_fundamentals", "akshare_eastmoney_financials", EASTMONEY_DATA_URL
                locator = "records"
                metadata.update({"report_period": path.parent.name, "dataset_name": path.stem})
                try:
                    payload = json.loads(content)
                    observed_at = payload.get("observed_at") or observed_at
                except (ValueError, UnicodeDecodeError, AttributeError):
                    pass
            elif "company_statements" in parts:
                dataset, source, source_url, locator = "legacy_company_statement", "akshare_eastmoney_financials", EASTMONEY_DATA_URL, "records"
                metadata.update({"symbol": path.parent.name, "statement": path.stem})
            elif "baostock" in parts:
                dataset, source, source_url, locator = "legacy_baostock_snapshot", "baostock", BAOSTOCK_SOURCE_URL, "payload"
                metadata.update({"symbol": path.parent.name, "report_period": path.stem})
            digest = hashlib.sha256(content).hexdigest()
            rows.append({
                "artifact_id": "legacy_" + hashlib.sha256(str(path).encode("utf-8")).hexdigest(),
                "dataset": dataset, "source": source, "source_url": source_url,
                "observed_at": observed_at, "ingested_at": observed_at,
                "raw_response_locator": locator, "storage_path": str(path),
                "sha256": digest, "byte_size": len(content),
                "metadata_json": json.dumps(metadata, ensure_ascii=False),
            })
        count = self.warehouse.save_artifacts(rows)
        return {"status": "success", "registered": count, "skipped": skipped}

    def refresh_market_fundamentals(
        self, report_period: str = "", include_valuation: bool = True
    ) -> Dict[str, Any]:
        report_period = normalize_report_period(report_period or latest_broad_report_period())
        run_id = uuid.uuid4().hex
        started_at = utc_now()
        self.warehouse.save_run(
            [run_id, "refresh_market_fundamentals", "running", report_period, started_at, None, 0, None]
        )
        try:
            frames = self.financial_provider.fetch_market_summaries(report_period)
            self.warehouse.record_provider_health(getattr(self.financial_provider, "name", "akshare_eastmoney_financials"), True, utc_now())
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
                    self.warehouse.record_provider_health(getattr(self.spot_provider, "name", "eastmoney_quote_center"), True, utc_now())
                    valuation_status = "fresh"
                    valuation_observed_at = utc_now()
                except Exception as exc:
                    self.warehouse.record_provider_health(getattr(self.spot_provider, "name", "eastmoney_quote_center"), False, utc_now(), str(exc))
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
            count = self.warehouse.replace_fundamentals(report_period, rows)
            finished_at = utc_now()
            self.warehouse.save_run(
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
            self.warehouse.record_provider_health(getattr(self.financial_provider, "name", "akshare_eastmoney_financials"), False, utc_now(), str(exc))
            self.warehouse.save_run(
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
            counts[statement] = self.warehouse.replace_statement_rows(symbol, statement, rows)
        self.warehouse.record_provider_health("akshare_eastmoney_financials", True, fetched_at)
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
        self.warehouse.upsert_baostock(row)
        self.warehouse.record_provider_health("baostock", True, fetched_at)
        self.warehouse.rebuild_funnel_metrics(utc_now())
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
            count = self.warehouse.replace_tdx_period(item["report_period"], rows)
            results.append(
                {
                    "report_period": item["report_period"],
                    "filename": item["filename"],
                    "row_count": count,
                    "filesize": item.get("filesize"),
                }
            )
        metric_count = self.warehouse.rebuild_funnel_metrics(utc_now())
        self.warehouse.record_provider_health("tdx_financial_via_mootdx", True, utc_now())
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
        count = self.warehouse.rebuild_funnel_metrics(rebuilt_at)
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
        eastmoney_rows = self.warehouse.query_fundamentals(
            symbol=symbol, report_period=report_period, limit=1, active_only=False
        )
        eastmoney = eastmoney_rows[0] if eastmoney_rows else None
        baostock = self.warehouse.get_baostock(symbol, report_period)
        tdx = self.warehouse.get_tdx(symbol, report_period)
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
        self.warehouse.save_validation_results(validation_rows)
        self.warehouse.rebuild_funnel_metrics(observed_at)
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
        count = self.warehouse.rebuild_validation_results(report_period, observed_at)
        self.warehouse.rebuild_funnel_metrics(observed_at)
        self._finish_run(run_id, "rebuild_cached_validation", started_at, report_period, count)
        return {
            "status": "success", "report_period": report_period,
            "row_count": count, "observed_at": observed_at,
        }
