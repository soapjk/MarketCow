from __future__ import annotations

from datetime import date, datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, Optional
from zoneinfo import ZoneInfo

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field

from . import __version__
from .config import Settings
from .normalize import normalize_as_of, normalize_report_period
from .providers.yahoo_quote import normalize_yahoo_symbol
from .providers.eastmoney_realtime import normalize_a_symbol
from .service import FundamentalService


class TushareRequest(BaseModel):
    params: Dict[str, Any] = Field(default_factory=dict)
    fields: str = ""


class TushareRealtimeRequest(BaseModel):
    ts_code: str


def create_app(
    settings: Optional[Settings] = None,
    service: Optional[FundamentalService] = None,
) -> FastAPI:
    settings = settings or Settings.from_env()
    service = service or FundamentalService(settings)
    app = FastAPI(title="MarketCow", version=__version__)
    app.state.service = service

    def parse_as_of(value: str) -> str:
        if not value:
            return ""
        try:
            return normalize_as_of(value)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    def calendar_range(
        date_from: str, date_to: str, days: int = 30, include_past: bool = False
    ) -> tuple[str, str]:
        today = datetime.now(ZoneInfo("Asia/Shanghai")).date()
        try:
            start = date.fromisoformat(date_from) if date_from else today
            end = date.fromisoformat(date_to) if date_to else today + timedelta(days=days)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail="calendar dates must use YYYY-MM-DD") from exc
        if start > end:
            raise HTTPException(status_code=400, detail="from must be on or before to")
        if not include_past and start < today:
            start = today
        return start.isoformat(), end.isoformat()

    @app.get("/v1/health")
    def health():
        return {
            "status": "ok",
            "version": __version__,
            "profile": settings.profile,
            "database": str(settings.database_path),
        }

    @app.post("/v1/tushare/realtime-quote")
    def tushare_realtime_quote(request: TushareRealtimeRequest):
        try:
            items = service.tushare_realtime_quote(request.ts_code)
            return {"count": len(items), "items": items, "source": "tushare_realtime"}
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except Exception as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc

    @app.post("/v1/tushare/{api_name}")
    def tushare_call(api_name: str, request: TushareRequest):
        try:
            return service.call_tushare(api_name, request.params, request.fields)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except Exception as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc

    @app.get("/v1/economic-calendar")
    def economic_calendar(
        country: str = "US",
        date_from: str = Query("", alias="from"),
        date_to: str = Query("", alias="to"),
        impact: str = "",
        limit: int = Query(50, ge=1, le=500),
        include_past: bool = False,
    ):
        start, end = calendar_range(date_from, date_to, include_past=include_past)
        events = service.warehouse.get_economic_calendar(start, end, country, impact, limit)
        return {
            "count": len(events), "from": start, "to": end,
            "filter_timezone": "Asia/Shanghai", "past_events_excluded": not include_past,
            "events": events,
        }

    @app.get("/v1/economic-indicators")
    def economic_indicators(
        country: str = "US", source: str = "", limit: int = Query(50, ge=1, le=500)
    ):
        indicators = service.warehouse.get_economic_indicators(country, source, limit)
        return {"count": len(indicators), "indicators": indicators}

    @app.get("/v1/earnings-calendar")
    def earnings_calendar(
        market: str = "",
        symbols: str = "",
        date_from: str = Query("", alias="from"),
        date_to: str = Query("", alias="to"),
        limit: int = Query(50, ge=1, le=500),
        include_past: bool = False,
    ):
        start, end = calendar_range(date_from, date_to, include_past=include_past)
        requested = [item.strip().upper() for item in symbols.split(",") if item.strip()]
        events = service.warehouse.get_earnings_calendar(start, end, market, requested, limit)
        return {
            "count": len(events), "from": start, "to": end,
            "filter_timezone": "Asia/Shanghai", "past_events_excluded": not include_past,
            "events": events,
        }

    @app.get("/v1/snapshot")
    def data_snapshot(limit: int = Query(50, ge=1, le=500), days: int = Query(30, ge=1, le=120)):
        start, end = calendar_range("", "", days=days)
        result = service.calendar_snapshot(start, end, limit)
        result.update({"from": start, "to": end, "past_events_excluded": True})
        return result

    @app.post("/v1/admin/economic-calendar/refresh")
    def refresh_economic_calendar(
        country: str = "US",
        date_from: str = Query("", alias="from"),
        date_to: str = Query("", alias="to"),
        days: int = Query(30, ge=1, le=120),
    ):
        start, end = calendar_range(date_from, date_to, days=days, include_past=True)
        try:
            return service.refresh_economic_calendar(start, end, country)
        except Exception as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc

    @app.post("/v1/admin/economic-indicators/refresh")
    def refresh_economic_indicators():
        try:
            return service.refresh_economic_indicators()
        except Exception as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc

    @app.post("/v1/admin/earnings-calendar/refresh")
    def refresh_earnings_calendar(
        market: str = "",
        symbols: str = "",
        date_from: str = Query("", alias="from"),
        date_to: str = Query("", alias="to"),
        days: int = Query(30, ge=1, le=120),
    ):
        start, end = calendar_range(date_from, date_to, days=days, include_past=True)
        requested = [item.strip().upper() for item in symbols.split(",") if item.strip()]
        try:
            return service.refresh_earnings_calendar(start, end, market, requested)
        except Exception as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc

    @app.get("/v1/quotes")
    def quotes(symbols: str, refresh: bool = False):
        requested = [item.strip() for item in symbols.split(",") if item.strip()]
        if not requested:
            raise HTTPException(status_code=400, detail="symbols is required")
        if len(requested) > 20:
            raise HTTPException(status_code=400, detail="at most 20 symbols per request")
        normalized_symbols, normalization_errors = [], []
        for symbol in requested:
            try:
                try:
                    normalized = normalize_a_symbol(symbol)
                except ValueError:
                    normalized, _ = normalize_yahoo_symbol(symbol)
                normalized_symbols.append(normalized)
            except Exception as exc:
                normalization_errors.append({"symbol": symbol, "error": str(exc)})
        by_symbol, errors = {}, list(normalization_errors)
        workers = max(1, min(settings.quote_refresh_workers, len(normalized_symbols)))
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {
                executor.submit(service.get_quote, normalized, refresh): normalized
                for normalized in normalized_symbols
            }
            for future in as_completed(futures):
                normalized = futures[future]
                try:
                    by_symbol[normalized] = future.result()
                except Exception as exc:
                    errors.append({
                        "symbol": normalized,
                        "status": "unavailable",
                        "error": str(exc),
                    })
        items = [by_symbol[symbol] for symbol in normalized_symbols if symbol in by_symbol]
        return {"count": len(items), "items": items, "errors": errors}

    @app.get("/v1/instruments/search")
    def instrument_search(q: str, limit: int = Query(12, ge=1, le=30)):
        query = q.strip()
        if not query:
            return {"count": 0, "items": []}
        try:
            items = service.search_instruments(query, limit)
            return {"count": len(items), "items": items}
        except Exception as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc

    @app.get("/v1/quotes/{symbol}/history")
    def quote_history(
        symbol: str,
        range_: str = Query("1y", alias="range"),
        interval: str = "1d",
        adjustment: str = Query("adjusted", pattern="^(adjusted|raw)$"),
        refresh: bool = True,
        limit: int = Query(500, ge=1, le=5000),
    ):
        try:
            if interval in {"1m", "5m", "15m", "30m", "60m", "1h"}:
                try:
                    normalized = normalize_a_symbol(symbol)
                except ValueError:
                    normalized, _ = normalize_yahoo_symbol(symbol)
            else:
                normalized, _ = normalize_yahoo_symbol(symbol)
            if refresh:
                result = service.refresh_quote_history(normalized, range_, interval, adjustment)
                result["bars"] = result["bars"][-limit:]
                result["count"] = len(result["bars"])
                return result
            bars = service.warehouse.get_price_bars(normalized, interval, adjustment, limit)
            return {"symbol": normalized, "interval": interval, "adjustment": adjustment, "count": len(bars), "bars": bars, "cached": True}
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except Exception as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc

    @app.get("/v1/quotes/{symbol}")
    def quote(symbol: str, refresh: bool = False):
        try:
            try:
                normalized = normalize_a_symbol(symbol)
            except ValueError:
                normalized, _ = normalize_yahoo_symbol(symbol)
            return service.get_quote(normalized, force_refresh=refresh)
        except HTTPException:
            raise
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except Exception as exc:
            raise HTTPException(
                status_code=503,
                detail={"symbol": symbol, "status": "unavailable", "error": str(exc)},
            ) from exc

    @app.get("/v1/fundamentals")
    def fundamentals(
        limit: int = Query(100, ge=1, le=5000),
        offset: int = Query(0, ge=0),
        symbol: str = "",
        report_period: str = "",
        industry: str = "",
        min_roe: Optional[float] = None,
        max_pe: Optional[float] = None,
        active_only: bool = True,
        as_of: str = "",
    ):
        if report_period:
            report_period = normalize_report_period(report_period)
        if as_of:
            as_of = parse_as_of(as_of)
        rows = service.warehouse.query_fundamentals(
            limit=limit,
            offset=offset,
            symbol="".join(ch for ch in symbol if ch.isdigit()).zfill(6) if symbol else "",
            report_period=report_period,
            industry=industry,
            min_roe=min_roe,
            max_pe=max_pe,
            active_only=active_only,
            as_of=as_of,
        )
        return {"count": len(rows), "limit": limit, "offset": offset, "as_of": as_of or None, "point_in_time": bool(as_of), "items": rows}

    @app.get("/v1/fundamentals/{symbol}")
    def fundamental(symbol: str, report_period: str = "", as_of: str = ""):
        code = "".join(ch for ch in symbol if ch.isdigit()).zfill(6)
        rows = service.warehouse.query_fundamentals(
            limit=1,
            symbol=code,
            report_period=normalize_report_period(report_period) if report_period else "",
            active_only=False,
            as_of=parse_as_of(as_of),
        )
        if not rows:
            raise HTTPException(status_code=404, detail="fundamental data not found")
        return rows[0]

    @app.get("/v1/financials/{symbol}/statements")
    def statements(
        symbol: str,
        statement: str = Query("", pattern="^(|income|balance|cashflow)$"),
        limit_periods: int = Query(20, ge=1, le=200),
        as_of: str = "",
    ):
        code = "".join(ch for ch in symbol if ch.isdigit()).zfill(6)
        normalized_as_of = parse_as_of(as_of)
        rows = service.warehouse.get_statement_rows(code, statement, limit_periods, normalized_as_of)
        return {"symbol": code, "count": len(rows), "as_of": normalized_as_of or None, "point_in_time": bool(normalized_as_of), "items": rows}

    @app.post("/v1/admin/fundamentals/refresh")
    def refresh_fundamentals(report_period: str = "", include_valuation: bool = True):
        try:
            return service.refresh_market_fundamentals(report_period, include_valuation)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except Exception as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc

    @app.post("/v1/admin/financials/{symbol}/refresh")
    def refresh_statements(symbol: str):
        try:
            return service.refresh_company_statements(symbol)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except Exception as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc

    @app.get("/v1/admin/jobs")
    def jobs(limit: int = Query(20, ge=1, le=200)):
        return {"items": service.warehouse.latest_runs(limit)}

    @app.get("/v1/admin/artifacts")
    def artifacts(dataset: str = "", limit: int = Query(100, ge=1, le=1000)):
        rows = service.warehouse.list_artifacts(dataset, limit)
        return {"count": len(rows), "items": rows}

    @app.post("/v1/admin/artifacts/backfill")
    def backfill_artifacts():
        return service.backfill_legacy_artifacts()

    @app.post("/v1/admin/baostock/{symbol}/refresh")
    def refresh_baostock(symbol: str, report_period: str):
        try:
            return service.refresh_baostock(symbol, report_period)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except Exception as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc

    @app.post("/v1/admin/tdx/financials/sync")
    def sync_tdx_financials(
        limit_periods: int = Query(12, ge=1, le=40),
        report_periods: str = "",
    ):
        periods = [item.strip() for item in report_periods.split(",") if item.strip()]
        try:
            return service.sync_tdx_financials(limit_periods, periods or None)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except Exception as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc

    @app.get("/v1/sources/tdx/coverage")
    def tdx_coverage():
        return {"periods": service.warehouse.tdx_coverage()}

    @app.get("/v1/validation/{symbol}")
    def validation(symbol: str, report_period: str):
        try:
            return service.validate_company(symbol, report_period)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/v1/fundamentals/{symbol}/history")
    def fundamental_history(
        symbol: str,
        annual_only: bool = False,
        limit: int = Query(40, ge=1, le=100),
        as_of: str = "",
    ):
        code = "".join(ch for ch in symbol if ch.isdigit()).zfill(6)
        normalized_as_of = parse_as_of(as_of)
        rows = service.warehouse.get_tdx_history(code, annual_only, limit, normalized_as_of)
        return {"symbol": code, "count": len(rows), "as_of": normalized_as_of or None, "point_in_time": bool(normalized_as_of), "items": rows}

    @app.get("/v1/sources/health")
    def source_health():
        return {"items": service.warehouse.provider_health()}

    @app.get("/v1/validation/{symbol}/results")
    def validation_results(symbol: str, report_period: str):
        code = "".join(ch for ch in symbol if ch.isdigit()).zfill(6)
        period = normalize_report_period(report_period)
        rows = service.warehouse.get_validation_results(code, period)
        return {"symbol": code, "report_period": period, "count": len(rows), "items": rows}

    @app.post("/v1/admin/validation/rebuild")
    def rebuild_validation(report_period: str):
        try:
            return service.rebuild_cached_validation(report_period)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post("/v1/admin/funnel/metrics/rebuild")
    def rebuild_funnel_metrics():
        return service.rebuild_funnel_metrics()

    @app.get("/v1/funnel/metrics")
    def funnel_metrics(
        limit: int = Query(100, ge=1, le=5000),
        offset: int = Query(0, ge=0),
        min_roe_median: Optional[float] = None,
        min_revenue_cagr: Optional[float] = None,
        min_profit_cagr: Optional[float] = None,
        max_pe: Optional[float] = None,
        max_debt_ratio: Optional[float] = None,
        min_annual_periods: int = Query(0, ge=0, le=20),
        active_only: bool = True,
        as_of: str = "",
    ):
        normalized_as_of = parse_as_of(as_of)
        rows = service.warehouse.query_funnel_metrics(
            limit=limit,
            offset=offset,
            min_roe_median=min_roe_median,
            min_revenue_cagr=min_revenue_cagr,
            min_profit_cagr=min_profit_cagr,
            max_pe=max_pe,
            max_debt_ratio=max_debt_ratio,
            min_annual_periods=min_annual_periods,
            active_only=active_only,
            as_of=normalized_as_of,
        )
        return {"count": len(rows), "limit": limit, "offset": offset, "as_of": normalized_as_of or None, "point_in_time": bool(normalized_as_of), "items": rows}

    return app


app = create_app()
