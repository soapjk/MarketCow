from __future__ import annotations

import hashlib
import json
import os
import random
import re
import stat
import time
import uuid
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Sequence
from urllib.parse import urlencode

import exchange_calendars
import requests

from ..instruments import canonical_instrument


ALPACA_SIP_SOURCE = "alpaca_historical_sip"
ALPACA_DATA_URL = "https://data.alpaca.markets"
BAR_PATH = "/v2/stocks/bars"
CORPORATE_ACTION_PATH = "/v1/corporate-actions"
PANEL_INSTRUMENTS: Mapping[str, str] = {
    "QQQ": "QQQ.ARCX",
    "AAPL": "AAPL.XNAS",
    "MSFT": "MSFT.XNAS",
    "AMZN": "AMZN.XNAS",
    "GOOGL": "GOOGL.XNAS",
    "META": "META.XNAS",
    "NVDA": "NVDA.XNAS",
    "TSLA": "TSLA.XNAS",
    "INTC": "INTC.XNAS",
    "MU": "MU.XNAS",
}
POINT_IN_TIME_SYMBOL_ALIASES: Mapping[str, tuple[dict[str, str | None], ...]] = {
    "META.XNAS": (
        {"provider_symbol": "FB", "effective_from": None,
         "effective_to": "2022-06-08"},
        {"provider_symbol": "META", "effective_from": "2022-06-09",
         "effective_to": None},
    ),
}
ALLOWED_ADJUSTMENTS = frozenset({"raw", "split", "dividend", "spin-off", "all"})
ALLOWED_SORTS = frozenset({"asc", "desc"})
_SYMBOL = re.compile(r"^[A-Z][A-Z0-9.-]{0,31}$")
_CORPORATE_ACTION_TYPES = (
    "reverse_split", "forward_split", "unit_split", "cash_dividend",
    "stock_dividend", "spin_off", "cash_merger", "stock_merger",
    "stock_and_cash_merger", "redemption", "name_change",
    "worthless_removal", "rights_distribution", "partial_call",
    "reorganization", "capital_gains_distribution",
)


class AlpacaSipError(RuntimeError):
    """Fail-closed provider error whose text never contains credentials."""


@dataclass(frozen=True)
class AlpacaCredentials:
    key_id: str
    secret_key: str

    @classmethod
    def load(cls, secret_file: Path | None = None) -> "AlpacaCredentials":
        if secret_file is not None:
            path = secret_file.expanduser().resolve(strict=True)
            mode = stat.S_IMODE(path.stat().st_mode)
            if mode != 0o600:
                raise AlpacaSipError("Alpaca credential file must have mode 0600")
            try:
                value = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError) as exc:
                raise AlpacaSipError("Alpaca credential file is unreadable") from exc
            key_id = str(value.get("key_id") or "").strip()
            secret_key = str(value.get("secret_key") or "").strip()
        else:
            key_id = (
                os.getenv("MARKETCOW_ALPACA_API_KEY_ID", "").strip()
                or os.getenv("APCA_API_KEY_ID", "").strip()
            )
            secret_key = (
                os.getenv("MARKETCOW_ALPACA_API_SECRET_KEY", "").strip()
                or os.getenv("APCA_API_SECRET_KEY", "").strip()
            )
        if not key_id or not secret_key:
            raise AlpacaSipError("Alpaca SIP credentials are not configured")
        return cls(key_id, secret_key)

    def headers(self) -> dict[str, str]:
        return {
            "APCA-API-KEY-ID": self.key_id,
            "APCA-API-SECRET-KEY": self.secret_key,
            "Accept": "application/json",
            "User-Agent": "MarketCow/alpaca-sip-v1",
        }


@dataclass(frozen=True)
class AlpacaBudgets:
    maximum_requests: int = 200
    maximum_bytes: int = 256 * 1024 * 1024
    maximum_retries: int = 3
    timeout_seconds: float = 15.0
    total_seconds: float = 1800.0
    page_limit: int = 10_000
    concurrency: int = 1

    def validate(self) -> None:
        if not 1 <= self.maximum_requests <= 100_000:
            raise ValueError("maximum_requests must be between 1 and 100000")
        if not 1 <= self.maximum_bytes <= 1024**4:
            raise ValueError("maximum_bytes must be positive and bounded")
        if not 0 <= self.maximum_retries <= 10:
            raise ValueError("maximum_retries must be between 0 and 10")
        if not 0.1 <= self.timeout_seconds <= 120:
            raise ValueError("timeout_seconds must be between 0.1 and 120")
        if not self.timeout_seconds <= self.total_seconds <= 86_400:
            raise ValueError("total_seconds must cover one request and be bounded")
        if not 1 <= self.page_limit <= 10_000:
            raise ValueError("page_limit must be between 1 and 10000")
        if self.concurrency != 1:
            raise ValueError("Alpaca SIP capture v1 requires serial requests")


@dataclass(frozen=True)
class AlpacaBarsRequest:
    symbols: tuple[str, ...]
    start: datetime
    end: datetime
    timeframe: str = "1Day"
    feed: str = "sip"
    adjustment: str = "raw"
    asof: date | None = None
    sort: str = "asc"
    limit: int = 10_000

    def __post_init__(self) -> None:
        symbols = tuple(str(item).strip().upper() for item in self.symbols)
        if not symbols or len(set(symbols)) != len(symbols):
            raise ValueError("symbols must be non-empty and unique")
        if any(not _SYMBOL.fullmatch(item) for item in symbols):
            raise ValueError("invalid Alpaca stock symbol")
        if tuple(sorted(symbols)) != symbols:
            raise ValueError("symbols must be lexicographically sorted")
        if self.start.tzinfo is None or self.end.tzinfo is None or self.start >= self.end:
            raise ValueError("start and end must be ordered timezone-aware timestamps")
        if self.feed != "sip":
            raise ValueError("Alpaca SIP provider requires feed=sip")
        if self.adjustment not in ALLOWED_ADJUSTMENTS:
            raise ValueError("unsupported Alpaca adjustment")
        if self.sort not in ALLOWED_SORTS:
            raise ValueError("sort must be asc or desc")
        if not re.fullmatch(
            r"(?:[1-9]|[1-5][0-9])(?:Min|T)|(?:[1-9]|1[0-9]|2[0-3])(?:Hour|H)|"
            r"1(?:Day|D|Week|W)|(?:1|2|3|4|6|12)(?:Month|M)",
            self.timeframe,
        ):
            raise ValueError("unsupported Alpaca timeframe")
        if not 1 <= self.limit <= 10_000:
            raise ValueError("limit must be between 1 and 10000")
        object.__setattr__(self, "symbols", symbols)

    def validate_delay(self, now: datetime) -> None:
        if now.tzinfo is None:
            raise ValueError("now must be timezone-aware")
        if self.end > now.astimezone(timezone.utc) - timedelta(minutes=15):
            raise ValueError("Alpaca SIP historical end must be at least 15 minutes old")

    def params(self, page_token: str | None = None) -> dict[str, str]:
        result = {
            "symbols": ",".join(self.symbols),
            "start": _rfc3339(self.start),
            "end": _rfc3339(self.end),
            "timeframe": self.timeframe,
            "feed": self.feed,
            "adjustment": self.adjustment,
            "asof": (self.asof or self.end.astimezone(timezone.utc).date()).isoformat(),
            "sort": self.sort,
            "limit": str(self.limit),
        }
        if page_token:
            result["page_token"] = page_token
        return result

    def public_identity(self) -> dict[str, Any]:
        return self.params()


@dataclass(frozen=True)
class CapturedPage:
    ordinal: int
    kind: str
    request_params: dict[str, str]
    status: int
    received_at: str
    byte_size: int
    sha256: str
    path: str
    next_page_token_sha256: str | None


class _BudgetState:
    def __init__(self, budgets: AlpacaBudgets, monotonic: Callable[[], float]):
        budgets.validate()
        self.budgets = budgets
        self.monotonic = monotonic
        self.started = monotonic()
        self.requests = 0
        self.bytes = 0
        self.retries = 0

    def before_request(self) -> float:
        remaining = self.budgets.total_seconds - (self.monotonic() - self.started)
        if remaining <= 0:
            raise AlpacaSipError("Alpaca request time budget exhausted")
        if self.requests >= self.budgets.maximum_requests:
            raise AlpacaSipError("Alpaca request count budget exhausted")
        self.requests += 1
        return min(self.budgets.timeout_seconds, remaining)

    def add_bytes(self, count: int) -> None:
        if count < 0 or self.bytes + count > self.budgets.maximum_bytes:
            raise AlpacaSipError("Alpaca response byte budget exhausted")
        self.bytes += count


class AlpacaSipProvider:
    name = ALPACA_SIP_SOURCE

    def __init__(
        self,
        credentials: AlpacaCredentials,
        *,
        base_url: str = ALPACA_DATA_URL,
        budgets: AlpacaBudgets | None = None,
        session: requests.Session | Any | None = None,
        clock: Callable[[], datetime] | None = None,
        monotonic: Callable[[], float] = time.monotonic,
        sleeper: Callable[[float], None] = time.sleep,
        random_value: Callable[[], float] = random.random,
    ):
        if base_url.rstrip("/") != ALPACA_DATA_URL:
            raise ValueError("Alpaca SIP v1 only supports the official production origin")
        self.credentials = credentials
        self.base_url = base_url.rstrip("/")
        self.budgets = budgets or AlpacaBudgets()
        self.budgets.validate()
        self.session = session or requests.Session()
        self.session.trust_env = True
        self.clock = clock or (lambda: datetime.now(timezone.utc))
        self.monotonic = monotonic
        self.sleeper = sleeper
        self.random_value = random_value

    @property
    def configured(self) -> bool:
        return bool(self.credentials.key_id and self.credentials.secret_key)

    def fetch_history_window(
        self,
        value: str,
        start: datetime,
        end: datetime,
        interval: str,
        adjustment: str,
    ) -> dict[str, Any]:
        instrument = canonical_instrument(value)
        if instrument.market != "US":
            raise ValueError("Alpaca SIP only supports US instruments")
        provider_symbol = instrument.provider_symbol("provider:alpaca_sip")
        timeframe = _alpaca_timeframe(interval)
        if adjustment != "raw":
            raise ValueError(
                "MarketCow history ingestion exposes Alpaca raw bars only; "
                "use the dataset capture for separate adjusted views"
            )
        request_end = end - timedelta(microseconds=1)
        request = AlpacaBarsRequest(
            symbols=(provider_symbol,), start=start, end=request_end,
            timeframe=timeframe, adjustment=_alpaca_adjustment(adjustment),
            asof=request_end.astimezone(timezone.utc).date(),
            limit=self.budgets.page_limit,
        )
        request.validate_delay(self.clock())
        state = _BudgetState(self.budgets, self.monotonic)
        pages, bars = self._bars(request, state, capture=None)
        normalized = [_service_bar(item) for item in bars]
        source_url = self.base_url + BAR_PATH + "?" + urlencode(request.params())
        return {
            "instrument_id": instrument.instrument_id,
            "symbol": instrument.instrument_id,
            "provider_symbol": provider_symbol,
            "name": provider_symbol,
            "market": "US",
            "exchange": instrument.mic,
            "currency": "USD",
            "range": f"{_rfc3339(start)}/{_rfc3339(end)}",
            "interval": interval,
            "adjustment": request.adjustment,
            "quality_status": "official_provider_validated",
            "exchange_timezone": "America/New_York",
            "source": self.name,
            "source_url": source_url,
            "raw_response_locator": "pages[*].bars",
            "bars": normalized,
            "_raw_payload": {"request": request.public_identity(), "pages": pages},
        }

    def fetch_history(
        self,
        value: str,
        range_: str = "1y",
        interval: str = "1d",
        adjustment: str = "raw",
    ) -> dict[str, Any]:
        days = {
            "1d": 1, "5d": 5, "1mo": 31, "3mo": 93, "6mo": 186,
            "1y": 366, "2y": 732, "5y": 1830, "10y": 3660,
        }.get(range_)
        if days is None:
            raise ValueError("Alpaca SIP range must be finite")
        end = self.clock().astimezone(timezone.utc) - timedelta(minutes=15)
        return self.fetch_history_window(
            value, end - timedelta(days=days), end, interval, adjustment
        )

    def capture_dataset(
        self,
        output: Path,
        requests_: Sequence[AlpacaBarsRequest],
        *,
        corporate_start: date,
        corporate_end: date,
        provider_neutral_symbols: Mapping[str, str],
    ) -> dict[str, Any]:
        target = output.expanduser().resolve()
        target.mkdir(parents=True, exist_ok=False)
        (target / "raw").mkdir()
        (target / "normalized").mkdir()
        state = _BudgetState(self.budgets, self.monotonic)
        pages: list[CapturedPage] = []
        normalized: list[dict[str, Any]] = []
        started_at = _utc_now(self.clock())
        try:
            for request in requests_:
                request.validate_delay(self.clock())
                _, bars = self._bars(
                    request, state, capture=target / "raw", captured_sink=pages
                )
                normalized.extend(_normalize_bars(
                    bars, request, provider_neutral_symbols, started_at,
                ))
            corporate_symbols = set(provider_neutral_symbols)
            if "META" in corporate_symbols:
                corporate_symbols.add("FB")
            _, corporate_payloads = self._corporate_actions(
                tuple(sorted(corporate_symbols)), corporate_start,
                corporate_end, state, target / "raw", captured_sink=pages,
            )
            _write_jsonl_atomic(target / "normalized" / "bars.jsonl", normalized)
            _write_json_atomic(
                target / "normalized" / "corporate-actions.json",
                {"schema": "marketcow.alpaca-sip-corporate-actions.v1",
                 "pages": corporate_payloads},
            )
            coverage = _coverage(normalized, provider_neutral_symbols, requests_)
            config_payload = {
                "requests": [item.public_identity() for item in requests_],
                "corporate_start": corporate_start.isoformat(),
                "corporate_end": corporate_end.isoformat(),
                "provider_symbols": dict(sorted(provider_neutral_symbols.items())),
                "point_in_time_symbol_aliases": POINT_IN_TIME_SYMBOL_ALIASES,
                "budgets": asdict(self.budgets),
            }
            manifest = {
                "schema": "marketcow.alpaca-sip-dataset.v1",
                "complete": True,
                "dataset_id": "alpaca-sip-" + uuid.uuid4().hex,
                "provider": "alpaca_sip",
                "source": self.name,
                "feed": "sip",
                "started_at": started_at,
                "completed_at": _utc_now(self.clock()),
                "requests": [item.public_identity() for item in requests_],
                "corporate_actions": {
                    "start": corporate_start.isoformat(),
                    "end": corporate_end.isoformat(),
                    "types": list(_CORPORATE_ACTION_TYPES),
                    "data_quality": "complete",
                },
                "provider_symbols": dict(sorted(provider_neutral_symbols.items())),
                "pages": [asdict(item) for item in pages],
                "budgets": asdict(self.budgets),
                "usage": {
                    "requests": state.requests,
                    "bytes": state.bytes,
                    "retries": state.retries,
                },
                "normalized": {
                    "bars": _file_identity(target / "normalized" / "bars.jsonl"),
                    "corporate_actions": _file_identity(
                        target / "normalized" / "corporate-actions.json"
                    ),
                },
                "coverage": coverage,
                "config_sha256": hashlib.sha256(json.dumps(
                    config_payload, sort_keys=True, separators=(",", ":")
                ).encode()).hexdigest(),
                "code_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                "official_contract": {
                    "bars": "https://docs.alpaca.markets/us/reference/stockbars",
                    "corporate_actions": (
                        "https://docs.alpaca.markets/us/reference/corporateactions-1"
                    ),
                    "market_data_plans": (
                        "https://docs.alpaca.markets/us/docs/about-market-data-api"
                    ),
                    "free_plan_boundary": "historical SIP excludes latest 15 minutes",
                    "redistribution_rights": "not_granted_by_this_dataset",
                    "terms_acceptance": "operator_responsibility",
                },
                "limitations": {
                    "available_at": "unknown_provider_not_documented",
                    "late_reports_and_corrections": "unknown_provider_not_documented",
                    "daily_bar_session": "unknown_provider_not_documented",
                    "historical_first_seen": "not_reconstructable",
                },
            }
            _write_json_atomic(target / "manifest.json", manifest)
            manifest["manifest"] = _file_identity(target / "manifest.json")
            return manifest
        except Exception as exc:
            failure = {
                "schema": "marketcow.alpaca-sip-capture-failure.v1",
                "complete": False,
                "started_at": started_at,
                "failed_at": _utc_now(self.clock()),
                "error_type": type(exc).__name__,
                "usage": {
                    "requests": state.requests,
                    "bytes": state.bytes,
                    "retries": state.retries,
                },
                "pages": [asdict(item) for item in pages],
            }
            _write_json_atomic(target / "failure.json", failure)
            raise

    def _bars(
        self, request: AlpacaBarsRequest, state: _BudgetState,
        capture: Path | None, captured_sink: list[CapturedPage] | None = None,
    ) -> tuple[list[Any], list[dict[str, Any]]]:
        token: str | None = None
        seen_tokens: set[str] = set()
        pages: list[Any] = []
        rows: list[dict[str, Any]] = []
        last_key: tuple[str, str] | None = None
        ordinal = 0
        while True:
            params = request.params(token)
            captured_page: CapturedPage | None = None
            identity = hashlib.sha256(json.dumps(
                request.public_identity(), sort_keys=True, separators=(",", ":")
            ).encode()).hexdigest()[:12]
            page_path = (
                capture / f"bars-{request.adjustment}-{identity}-{ordinal:06d}.json"
                if capture is not None else None
            )

            def capture_response(raw: bytes, status: int, received: str) -> None:
                nonlocal captured_page
                if page_path is None:
                    return
                _write_bytes_atomic(page_path, raw)
                captured_page = CapturedPage(
                    ordinal=ordinal, kind=f"bars:{request.adjustment}",
                    request_params=params, status=status, received_at=received,
                    byte_size=len(raw), sha256=hashlib.sha256(raw).hexdigest(),
                    path=str(page_path), next_page_token_sha256=None,
                )
                pages.append(captured_page)
                if captured_sink is not None:
                    captured_sink.append(captured_page)
            payload, raw, status, received = self._get(
                BAR_PATH, params, state,
                raw_observer=capture_response if capture is not None else None,
            )
            if payload.get("feed", "sip") != "sip":
                raise AlpacaSipError("Alpaca response feed drifted from sip")
            if payload.get("adjustment", request.adjustment) != request.adjustment:
                raise AlpacaSipError("Alpaca response adjustment identity drifted")
            next_token = payload.get("next_page_token")
            if next_token is not None and not isinstance(next_token, str):
                raise AlpacaSipError("Alpaca returned an invalid next_page_token")
            if next_token and next_token in seen_tokens:
                raise AlpacaSipError("Alpaca repeated a pagination token")
            page_rows: list[dict[str, Any]] = []
            bars = payload.get("bars")
            if not isinstance(bars, dict):
                raise AlpacaSipError("Alpaca bars response has no bars object")
            for symbol, values in bars.items():
                if symbol not in request.symbols or not isinstance(values, list):
                    raise AlpacaSipError("Alpaca bars response identity drifted")
                for value in values:
                    row = _validate_bar(symbol, value)
                    key = (symbol, row["t"])
                    if last_key is not None:
                        ordered = key > last_key if request.sort == "asc" else key < last_key
                        if not ordered:
                            raise AlpacaSipError("Alpaca bars are duplicate or out of order")
                    last_key = key
                    page_rows.append(row)
            rows.extend(page_rows)
            if capture is None:
                pages.append(payload)
            else:
                replacement = CapturedPage(
                    ordinal=ordinal, kind=f"bars:{request.adjustment}",
                    request_params=params, status=status, received_at=received,
                    byte_size=len(raw), sha256=hashlib.sha256(raw).hexdigest(),
                    path=captured_page.path,
                    next_page_token_sha256=(
                        hashlib.sha256(next_token.encode()).hexdigest()
                        if next_token else None
                    ),
                )
                pages[-1] = replacement
                if captured_sink is not None:
                    captured_sink[-1] = replacement
            if not next_token:
                break
            seen_tokens.add(next_token)
            token = next_token
            ordinal += 1
        return pages, rows

    def _corporate_actions(
        self, symbols: tuple[str, ...], start: date, end: date,
        state: _BudgetState, capture: Path,
        captured_sink: list[CapturedPage] | None = None,
    ) -> tuple[list[CapturedPage], list[dict[str, Any]]]:
        if start > end:
            raise ValueError("corporate action range must be ordered")
        base = {
            "symbols": ",".join(symbols),
            "types": ",".join(_CORPORATE_ACTION_TYPES),
            "start": start.isoformat(), "end": end.isoformat(),
            "region": "USA", "limit": "1000", "data_quality": "complete",
            "sort": "asc",
        }
        token: str | None = None
        seen: set[str] = set()
        last_by_type: dict[str, tuple[str, str]] = {}
        captured: list[CapturedPage] = []
        payloads: list[dict[str, Any]] = []
        ordinal = 0
        while True:
            params = dict(base)
            if token:
                params["page_token"] = token
            page_path = capture / f"corporate-actions-{ordinal:06d}.json"
            def capture_response(raw: bytes, status: int, received: str) -> None:
                _write_bytes_atomic(page_path, raw)
                captured_page = CapturedPage(
                    ordinal=ordinal, kind="corporate_actions", request_params=params,
                    status=status, received_at=received, byte_size=len(raw),
                    sha256=hashlib.sha256(raw).hexdigest(), path=str(page_path),
                    next_page_token_sha256=None,
                )
                captured.append(captured_page)
                if captured_sink is not None:
                    captured_sink.append(captured_page)
            payload, raw, status, received = self._get(
                CORPORATE_ACTION_PATH, params, state, raw_observer=capture_response
            )
            next_token = payload.get("next_page_token")
            if next_token is not None and not isinstance(next_token, str):
                raise AlpacaSipError("Alpaca returned an invalid corporate action token")
            if next_token and next_token in seen:
                raise AlpacaSipError("Alpaca repeated a corporate action token")
            _validate_corporate_actions(payload, set(symbols), last_by_type)
            replacement = CapturedPage(
                ordinal=ordinal, kind="corporate_actions", request_params=params,
                status=status, received_at=received, byte_size=len(raw),
                sha256=hashlib.sha256(raw).hexdigest(), path=str(page_path),
                next_page_token_sha256=(
                    hashlib.sha256(next_token.encode()).hexdigest()
                    if next_token else None
                ),
            )
            captured[-1] = replacement
            if captured_sink is not None:
                captured_sink[-1] = replacement
            payloads.append(payload)
            if not next_token:
                break
            seen.add(next_token)
            token = next_token
            ordinal += 1
        return captured, payloads

    def _get(
        self, path: str, params: dict[str, str], state: _BudgetState,
        raw_observer: Callable[[bytes, int, str], None] | None = None,
    ) -> tuple[dict[str, Any], bytes, int, str]:
        attempt = 0
        while True:
            timeout = state.before_request()
            try:
                response = self.session.get(
                    self.base_url + path,
                    params=params,
                    headers=self.credentials.headers(),
                    timeout=timeout,
                    allow_redirects=False,
                    stream=True,
                )
            except requests.RequestException as exc:
                retryable = True
                response = None
                error = exc
            else:
                retryable = response.status_code == 429 or 500 <= response.status_code <= 599
                error = None
            if response is not None and 200 <= response.status_code < 300:
                remaining = state.budgets.maximum_bytes - state.bytes
                raw = _read_bounded_response(response, remaining)
                state.add_bytes(len(raw))
                received_at = _utc_now(self.clock())
                if raw_observer is not None:
                    raw_observer(raw, response.status_code, received_at)
                try:
                    payload = json.loads(raw)
                except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                    raise AlpacaSipError("Alpaca returned invalid JSON") from exc
                if not isinstance(payload, dict):
                    raise AlpacaSipError("Alpaca returned a non-object response")
                return payload, raw, response.status_code, received_at
            if not retryable or attempt >= self.budgets.maximum_retries:
                status = response.status_code if response is not None else "network"
                raise AlpacaSipError(f"Alpaca request failed with status {status}") from error
            state.retries += 1
            delay = min(8.0, (2**attempt) * (0.5 + self.random_value()))
            if self.monotonic() - state.started + delay >= self.budgets.total_seconds:
                raise AlpacaSipError("Alpaca retry would exceed the time budget") from error
            self.sleeper(delay)
            attempt += 1


def _rfc3339(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _utc_now(value: datetime) -> str:
    return _rfc3339(value)


def _alpaca_timeframe(interval: str) -> str:
    return {
        "1m": "1Min", "5m": "5Min", "15m": "15Min", "30m": "30Min",
        "60m": "1Hour", "1h": "1Hour", "1d": "1Day", "1wk": "1Week",
    }.get(interval) or (_raise_value("unsupported Alpaca interval"))


def _alpaca_adjustment(adjustment: str) -> str:
    if adjustment in ALLOWED_ADJUSTMENTS:
        return adjustment
    raise ValueError("Alpaca adjustment must be raw, split, dividend, spin-off or all")


def _raise_value(message: str) -> Any:
    raise ValueError(message)


def _decimal(value: Any, name: str, *, nonnegative: bool = False) -> str:
    if isinstance(value, bool) or value is None:
        raise AlpacaSipError(f"Alpaca bar {name} is missing")
    try:
        number = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise AlpacaSipError(f"Alpaca bar {name} is invalid") from exc
    if not number.is_finite() or (nonnegative and number < 0):
        raise AlpacaSipError(f"Alpaca bar {name} is invalid")
    return format(number, "f")


def _validate_bar(symbol: str, value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise AlpacaSipError("Alpaca bar is not an object")
    timestamp = value.get("t")
    if not isinstance(timestamp, str):
        raise AlpacaSipError("Alpaca bar timestamp is missing")
    try:
        parsed = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
    except ValueError as exc:
        raise AlpacaSipError("Alpaca bar timestamp is invalid") from exc
    if parsed.tzinfo is None:
        raise AlpacaSipError("Alpaca bar timestamp has no timezone")
    o, h, low, c = (
        Decimal(_decimal(value.get(name), name)) for name in ("o", "h", "l", "c")
    )
    if low > min(o, c) or h < max(o, c) or low > h:
        raise AlpacaSipError("Alpaca bar violates OHLC bounds")
    row = {
        "symbol": symbol,
        "t": _rfc3339(parsed),
        "o": format(o, "f"), "h": format(h, "f"),
        "l": format(low, "f"), "c": format(c, "f"),
        "v": _decimal(value.get("v"), "v", nonnegative=True),
    }
    if value.get("n") is not None:
        row["n"] = _decimal(value["n"], "n", nonnegative=True)
    if value.get("vw") is not None:
        row["vw"] = _decimal(value["vw"], "vw", nonnegative=True)
    return row


def _normalize_bars(
    bars: Iterable[dict[str, Any]], request: AlpacaBarsRequest,
    identities: Mapping[str, str], received_at: str,
) -> list[dict[str, Any]]:
    result = []
    for row in bars:
        instrument_id = identities.get(row["symbol"])
        if instrument_id is None:
            raise AlpacaSipError("Alpaca bar has no provider-neutral identity")
        canonical_instrument(instrument_id)
        result.append({
            "schema": "marketcow.canonical-bar.v1",
            "instrument_id": instrument_id,
            "provider_symbol": row["symbol"],
            "provider": "alpaca_sip", "source": ALPACA_SIP_SOURCE,
            "feed": "sip", "timeframe": request.timeframe,
            "adjustment": request.adjustment,
            "source_timestamp": row["t"], "available_at": None,
            "available_at_missing_reason": "provider_not_documented",
            "received_at": received_at,
            "session": "unknown",
            "session_missing_reason": "daily_bar_session_not_documented",
            "currency": "USD", "open": row["o"], "high": row["h"],
            "low": row["l"], "close": row["c"], "volume": row["v"],
            "trade_count": row.get("n"), "vwap": row.get("vw"),
            "asof": (request.asof or request.end.date()).isoformat(),
        })
    return result


def _service_bar(row: dict[str, Any]) -> dict[str, Any]:
    timestamp = int(datetime.fromisoformat(row["t"].replace("Z", "+00:00")).timestamp())
    return {
        "timestamp": timestamp, "bar_at": row["t"],
        "open": float(row["o"]), "high": float(row["h"]),
        "low": float(row["l"]), "close": float(row["c"]),
        "raw_close": float(row["c"]), "volume": float(row["v"]),
        "trade_count": None if row.get("n") is None else float(row["n"]),
        "vwap": None if row.get("vw") is None else float(row["vw"]),
        "factor_applicability": "not_applicable",
        "corporate_action_factor": None,
        "applied_adjustment_multiplier": "1",
        "adjustment_reference_date": None,
        "reference_factor": None,
        "factor_source": None,
    }


def _coverage(
    rows: Sequence[dict[str, Any]], identities: Mapping[str, str],
    requests_: Sequence[AlpacaBarsRequest],
) -> dict[str, Any]:
    calendar = exchange_calendars.get_calendar("XNAS")
    raw = [row for row in rows if row["adjustment"] == "raw"]
    by_symbol: dict[str, list[dict[str, Any]]] = {symbol: [] for symbol in identities}
    for row in raw:
        by_symbol[row["provider_symbol"]].append(row)
    raw_requests = [item for item in requests_ if item.adjustment == "raw"]
    expected: set[str] = set()
    if raw_requests:
        start = min(item.start for item in raw_requests).date().isoformat()
        end = max(item.end for item in raw_requests).date().isoformat()
        expected = {
            item.date().isoformat() for item in calendar.sessions_in_range(start, end)
        }
    result: dict[str, Any] = {}
    for symbol, values in by_symbol.items():
        dates = {
            datetime.fromisoformat(row["source_timestamp"].replace("Z", "+00:00"))
            .astimezone(timezone.utc).date().isoformat()
            for row in values
        }
        if values:
            first, last = min(dates), max(dates)
        else:
            first = last = None
        missing = expected - dates
        jumps = []
        ordered = sorted(values, key=lambda item: item["source_timestamp"])
        for previous, current in zip(ordered, ordered[1:]):
            prior = Decimal(previous["close"])
            if prior:
                change = abs(Decimal(current["close"]) / prior - 1)
                if change >= Decimal("0.5"):
                    jumps.append({
                        "from": previous["source_timestamp"],
                        "to": current["source_timestamp"],
                        "absolute_return": format(change, "f"),
                        "corporate_action_nearby": "not_yet_joined",
                    })
        result[symbol] = {
            "instrument_id": identities[symbol], "count": len(values),
            "first_session": first, "last_session": last,
            "expected_sessions": len(expected),
            "missing_sessions": sorted(missing),
            "duplicate_sessions": len(values) - len(dates),
            "anomalous_jumps": jumps,
            "complete_requested_interval": bool(expected) and not missing,
        }
    adjustment_counts: dict[str, dict[str, int]] = {}
    for row in rows:
        counts = adjustment_counts.setdefault(
            row["adjustment"], {symbol: 0 for symbol in identities}
        )
        counts[row["provider_symbol"]] += 1
    return {
        "calendar": "XNAS",
        "symbols": result,
        "adjustment_symbol_counts": adjustment_counts,
        "formal_interval_assessable": bool(result) and all(
            item["complete_requested_interval"] and item["duplicate_sessions"] == 0
            for item in result.values()
        ),
    }


def _validate_corporate_actions(
    payload: dict[str, Any], symbols: set[str],
    last_by_type: dict[str, tuple[str, str]],
) -> None:
    actions = payload.get("corporate_actions")
    if not isinstance(actions, dict):
        raise AlpacaSipError("Alpaca corporate action response has no action object")
    for action_type, values in actions.items():
        if not isinstance(action_type, str) or not isinstance(values, list):
            raise AlpacaSipError("Alpaca corporate action response is invalid")
        for value in values:
            if not isinstance(value, dict):
                raise AlpacaSipError("Alpaca corporate action item is invalid")
            symbol = value.get("symbol") or value.get("initiating_symbol")
            if symbol is not None and symbol not in symbols:
                raise AlpacaSipError("Alpaca corporate action identity drifted")
            process_date = str(value.get("process_date") or "")
            identity = str(value.get("id") or value.get("corporate_action_id") or "")
            if process_date:
                key = (process_date, identity)
                previous = last_by_type.get(action_type)
                if previous is not None and key < previous:
                    raise AlpacaSipError("Alpaca corporate actions are out of order")
                last_by_type[action_type] = key


def _read_bounded_response(response: Any, remaining: int) -> bytes:
    if remaining < 0:
        raise AlpacaSipError("Alpaca response byte budget exhausted")
    chunks: list[bytes] = []
    count = 0
    iterator = getattr(response, "iter_content", None)
    if callable(iterator):
        for chunk in iterator(chunk_size=65536):
            if not chunk:
                continue
            count += len(chunk)
            if count > remaining:
                close = getattr(response, "close", None)
                if callable(close):
                    close()
                raise AlpacaSipError("Alpaca response byte budget exhausted")
            chunks.append(bytes(chunk))
        return b"".join(chunks)
    raw = bytes(response.content)
    if len(raw) > remaining:
        raise AlpacaSipError("Alpaca response byte budget exhausted")
    return raw


def _write_bytes_atomic(path: Path, body: bytes) -> None:
    if path.exists():
        raise FileExistsError(str(path))
    temporary = path.with_name(path.name + ".tmp")
    with temporary.open("xb") as stream:
        stream.write(body)
        stream.flush()
        os.fsync(stream.fileno())
    temporary.replace(path)
    _fsync_directory(path.parent)


def _write_json_atomic(path: Path, payload: Any) -> None:
    _write_bytes_atomic(path, json.dumps(
        payload, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False,
    ).encode("utf-8") + b"\n")


def _write_jsonl_atomic(path: Path, rows: Iterable[dict[str, Any]]) -> None:
    encoded = b"".join(
        json.dumps(row, ensure_ascii=False, sort_keys=True, allow_nan=False).encode("utf-8") + b"\n"
        for row in rows
    )
    _write_bytes_atomic(path, encoded)


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _file_identity(path: Path) -> dict[str, Any]:
    body = path.read_bytes()
    return {"path": str(path), "bytes": len(body), "sha256": hashlib.sha256(body).hexdigest()}
