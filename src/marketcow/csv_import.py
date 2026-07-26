from __future__ import annotations

import csv
import hashlib
import sqlite3
import tempfile
from dataclasses import dataclass, field
from datetime import datetime, time, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_EVEN
from pathlib import Path
from typing import Any, Iterator, Mapping, TextIO
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from .instruments import canonical_instrument


CSV_IMPORT_CONTRACT_VERSION = "marketcow.csv-bars.v2"
CANONICAL_FIELDS = frozenset({
    "symbol", "timestamp", "open", "high", "low", "close", "volume", "amount",
})
REQUIRED_FIELDS = frozenset({
    "timestamp", "open", "high", "low", "close",
})
SUPPORTED_INTERVALS = frozenset({
    "1m", "2m", "5m", "15m", "30m", "60m", "90m", "1h",
    "1d", "5d", "1wk", "1mo", "3mo",
})
INTRADAY_INTERVAL_SECONDS = {
    "1m": 60, "2m": 120, "5m": 300, "15m": 900, "30m": 1800,
    "60m": 3600, "90m": 5400, "1h": 3600,
}


class CsvImportContractError(ValueError):
    """The import declaration cannot be interpreted deterministically."""


class CsvRowError(ValueError):
    def __init__(
        self, code: str, message: str, *, line_number: int | None = None
    ):
        super().__init__(message)
        self.code = code
        self.line_number = line_number


@dataclass(frozen=True)
class CsvSchemaProfile:
    name: str
    version: str
    columns: Mapping[str, str]
    timezone_name: str
    timestamp_format: str = "iso8601"
    encoding: str = "utf-8-sig"
    delimiter: str = ","
    fixed_external_symbol: str | None = None
    allow_extra_columns: bool = True
    defaults: Mapping[str, str] = field(default_factory=dict)
    price_multiplier: str = "1"
    volume_multiplier: str = "1"
    amount_multiplier: str = "1"
    price_precision: int | None = None
    volume_precision: int | None = None
    amount_precision: int | None = None
    trading_sessions: tuple[Mapping[str, Any], ...] = ()

    def __post_init__(self) -> None:
        name = self.name.strip()
        version = self.version.strip()
        if not name or not version:
            raise CsvImportContractError("profile name and version are required")
        if len(self.delimiter) != 1:
            raise CsvImportContractError("delimiter must contain one character")
        normalized = {
            str(key).strip().lower(): str(value).strip()
            for key, value in self.columns.items()
        }
        unknown = set(normalized) - CANONICAL_FIELDS
        missing = REQUIRED_FIELDS - set(normalized)
        if unknown:
            raise CsvImportContractError(
                "profile contains unknown canonical fields: "
                + ", ".join(sorted(unknown))
            )
        if missing:
            raise CsvImportContractError(
                "profile is missing required fields: "
                + ", ".join(sorted(missing))
            )
        if "symbol" not in normalized and not self.fixed_external_symbol:
            raise CsvImportContractError(
                "profile requires a symbol column or fixed_external_symbol"
            )
        if any(not value for value in normalized.values()):
            raise CsvImportContractError("profile source column names must not be empty")
        if len(set(normalized.values())) != len(normalized):
            raise CsvImportContractError("profile source columns must be unique")
        defaults = {
            str(key).strip().lower(): str(value).strip()
            for key, value in self.defaults.items()
        }
        if set(defaults) - {"volume", "amount"}:
            raise CsvImportContractError(
                "profile defaults are only supported for volume and amount"
            )
        multipliers = {}
        for field_name in (
            "price_multiplier", "volume_multiplier", "amount_multiplier"
        ):
            try:
                multiplier = Decimal(str(getattr(self, field_name)))
            except InvalidOperation as exc:
                raise CsvImportContractError(
                    f"{field_name} must be a finite positive decimal"
                ) from exc
            if not multiplier.is_finite() or multiplier <= 0:
                raise CsvImportContractError(
                    f"{field_name} must be a finite positive decimal"
                )
            multipliers[field_name] = format(multiplier, "f")
        for field_name in (
            "price_precision", "volume_precision", "amount_precision"
        ):
            precision = getattr(self, field_name)
            if precision is not None and (
                not isinstance(precision, int) or precision < 0 or precision > 18
            ):
                raise CsvImportContractError(
                    f"{field_name} must be an integer between 0 and 18"
                )
        try:
            zone = ZoneInfo(self.timezone_name)
        except ZoneInfoNotFoundError as exc:
            raise CsvImportContractError("profile timezone is unknown") from exc
        sessions = tuple(
            _normalize_trading_session(value, zone)
            for value in self.trading_sessions
        )
        object.__setattr__(self, "name", name)
        object.__setattr__(self, "version", version)
        object.__setattr__(self, "columns", normalized)
        object.__setattr__(self, "defaults", defaults)
        object.__setattr__(self, "trading_sessions", sessions)
        for key, value in multipliers.items():
            object.__setattr__(self, key, value)

    @property
    def identity(self) -> str:
        return f"{self.name}@{self.version}"


@dataclass(frozen=True)
class InstrumentMapping:
    namespace: str
    symbols: Mapping[str, str]
    _normalized: Mapping[str, str] = field(init=False, repr=False)

    def __post_init__(self) -> None:
        namespace = self.namespace.strip().lower()
        if not namespace.startswith(("provider:", "broker:")):
            raise CsvImportContractError(
                "mapping namespace must use provider:<name> or broker:<name>"
            )
        normalized: dict[str, str] = {}
        for external, instrument_id in self.symbols.items():
            key = str(external).strip().upper()
            if not key:
                raise CsvImportContractError("external symbol must not be empty")
            canonical = canonical_instrument(instrument_id).instrument_id
            previous = normalized.get(key)
            if previous is not None and previous != canonical:
                raise CsvImportContractError(
                    f"conflicting mapping for external symbol {key}"
                )
            normalized[key] = canonical
        if not normalized:
            raise CsvImportContractError("at least one instrument mapping is required")
        object.__setattr__(self, "namespace", namespace)
        object.__setattr__(self, "_normalized", normalized)

    def resolve(self, external_symbol: str) -> str:
        key = str(external_symbol).strip().upper()
        try:
            return self._normalized[key]
        except KeyError as exc:
            raise CsvRowError(
                "instrument_mapping_missing",
                f"no {self.namespace} mapping exists for {key or '<empty>'}",
            ) from exc

    @property
    def canonical_mappings(self) -> Mapping[str, str]:
        return dict(self._normalized)


@dataclass(frozen=True)
class CsvImportRequest:
    source: str
    interval: str
    adjustment: str
    profile: CsvSchemaProfile
    instruments: InstrumentMapping
    created_by: str = "local-operator"
    source_proof: str = "operator-declared"
    retention_policy: str = "retain-until-explicit-deletion"

    def __post_init__(self) -> None:
        source = self.source.strip().lower()
        if not source:
            raise CsvImportContractError("source is required")
        if self.interval not in SUPPORTED_INTERVALS:
            raise CsvImportContractError("unsupported CSV bar interval")
        if self.adjustment not in {"raw", "qfq", "hfq"}:
            raise CsvImportContractError("adjustment must be raw, qfq or hfq")
        if not self.created_by.strip():
            raise CsvImportContractError("created_by is required")
        if not self.source_proof.strip():
            raise CsvImportContractError("source_proof is required")
        if not self.retention_policy.strip():
            raise CsvImportContractError("retention_policy is required")
        object.__setattr__(self, "source", source)

    def as_dict(self) -> dict[str, Any]:
        return {
            "contract_version": CSV_IMPORT_CONTRACT_VERSION,
            "source": self.source,
            "interval": self.interval,
            "adjustment": self.adjustment,
            "created_by": self.created_by,
            "source_proof": self.source_proof,
            "retention_policy": self.retention_policy,
            "profile": {
                "name": self.profile.name,
                "version": self.profile.version,
                "columns": dict(self.profile.columns),
                "timezone_name": self.profile.timezone_name,
                "timestamp_format": self.profile.timestamp_format,
                "encoding": self.profile.encoding,
                "delimiter": self.profile.delimiter,
                "fixed_external_symbol": self.profile.fixed_external_symbol,
                "allow_extra_columns": self.profile.allow_extra_columns,
                "defaults": dict(self.profile.defaults),
                "price_multiplier": self.profile.price_multiplier,
                "volume_multiplier": self.profile.volume_multiplier,
                "amount_multiplier": self.profile.amount_multiplier,
                "price_precision": self.profile.price_precision,
                "volume_precision": self.profile.volume_precision,
                "amount_precision": self.profile.amount_precision,
                "trading_sessions": [
                    dict(value) for value in self.profile.trading_sessions
                ],
            },
            "instruments": {
                "namespace": self.instruments.namespace,
                "symbols": dict(self.instruments.canonical_mappings),
            },
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "CsvImportRequest":
        allowed = {
            "contract_version", "source", "interval", "adjustment",
            "created_by", "source_proof", "retention_policy",
            "profile", "instruments",
        }
        unknown = set(value) - allowed
        if unknown:
            raise CsvImportContractError(
                "CSV import declaration contains unknown fields: "
                + ", ".join(sorted(unknown))
            )
        if value.get("contract_version") != CSV_IMPORT_CONTRACT_VERSION:
            raise CsvImportContractError("unsupported CSV import contract version")
        profile = value.get("profile")
        instruments = value.get("instruments")
        if not isinstance(profile, Mapping) or not isinstance(instruments, Mapping):
            raise CsvImportContractError(
                "CSV import profile and instruments are required"
            )
        profile_allowed = {
            field_name for field_name in CsvSchemaProfile.__dataclass_fields__
        }
        profile_unknown = set(profile) - profile_allowed
        if profile_unknown:
            raise CsvImportContractError(
                "CSV profile contains unknown fields: "
                + ", ".join(sorted(profile_unknown))
            )
        instrument_unknown = set(instruments) - {"namespace", "symbols"}
        if instrument_unknown:
            raise CsvImportContractError(
                "CSV instrument mapping contains unknown fields: "
                + ", ".join(sorted(instrument_unknown))
            )
        return cls(
            source=str(value.get("source") or ""),
            interval=str(value.get("interval") or ""),
            adjustment=str(value.get("adjustment") or ""),
            created_by=str(value.get("created_by") or "local-operator"),
            source_proof=str(value.get("source_proof") or "operator-declared"),
            retention_policy=str(
                value.get("retention_policy")
                or "retain-until-explicit-deletion"
            ),
            profile=CsvSchemaProfile(**dict(profile)),
            instruments=InstrumentMapping(
                str(instruments.get("namespace") or ""),
                dict(instruments.get("symbols") or {}),
            ),
        )


@dataclass(frozen=True)
class ParsedCsvBar:
    line_number: int
    instrument_id: str
    bar: Mapping[str, Any]


def _normalize_trading_session(
    value: Mapping[str, Any], zone: ZoneInfo
) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise CsvImportContractError("trading session must be an object")
    try:
        start = time.fromisoformat(str(value["start"]))
        end = time.fromisoformat(str(value["end"]))
        weekdays = tuple(sorted({int(day) for day in value["weekdays"]}))
    except (KeyError, TypeError, ValueError) as exc:
        raise CsvImportContractError(
            "trading session requires start, end and weekdays"
        ) from exc
    if start.tzinfo is not None or end.tzinfo is not None or start >= end:
        raise CsvImportContractError(
            "trading session start/end must be increasing local times"
        )
    if not weekdays or any(day < 0 or day > 6 for day in weekdays):
        raise CsvImportContractError(
            "trading session weekdays must contain values from 0 through 6"
        )
    return {
        "start": start.isoformat(),
        "end": end.isoformat(),
        "weekdays": list(weekdays),
        "timezone": zone.key,
    }


def _decimal(raw: Any, field_name: str, *, required: bool) -> Decimal | None:
    text = str(raw or "").strip()
    if not text and not required:
        return None
    try:
        value = Decimal(text)
    except InvalidOperation as exc:
        raise CsvRowError(
            "numeric_invalid", f"{field_name} must be a finite decimal"
        ) from exc
    if not value.is_finite():
        raise CsvRowError(
            "numeric_invalid", f"{field_name} must be a finite decimal"
        )
    return value


def _timestamp(raw: Any, profile: CsvSchemaProfile) -> datetime:
    text = str(raw or "").strip()
    if not text:
        raise CsvRowError("timestamp_missing", "timestamp is required")
    try:
        if profile.timestamp_format == "iso8601":
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        else:
            parsed = datetime.strptime(text, profile.timestamp_format)
    except ValueError as exc:
        raise CsvRowError(
            "timestamp_invalid",
            f"timestamp does not match {profile.timestamp_format}",
        ) from exc
    if parsed.tzinfo is None:
        zone = ZoneInfo(profile.timezone_name)
        fold_zero = parsed.replace(tzinfo=zone, fold=0)
        fold_one = parsed.replace(tzinfo=zone, fold=1)
        round_trip = (
            fold_zero.astimezone(timezone.utc)
            .astimezone(zone)
            .replace(tzinfo=None)
        )
        if round_trip != parsed:
            raise CsvRowError(
                "timestamp_nonexistent",
                "naive timestamp does not exist in the configured timezone",
            )
        if fold_zero.utcoffset() != fold_one.utcoffset():
            raise CsvRowError(
                "timestamp_ambiguous",
                "naive timestamp is ambiguous in the configured timezone; "
                "include an explicit UTC offset",
            )
        parsed = fold_zero
    return parsed.astimezone(timezone.utc)


def _source_value(
    row: Mapping[str, Any], profile: CsvSchemaProfile, field_name: str
) -> Any:
    source_name = profile.columns.get(field_name)
    value = None if source_name is None else row.get(source_name)
    if (value is None or str(value).strip() == "") and field_name in profile.defaults:
        return profile.defaults[field_name]
    return value


def _scaled(
    value: Decimal | None, multiplier: str, precision: int | None
) -> Decimal | None:
    if value is None:
        return None
    scaled = value * Decimal(multiplier)
    if precision is not None:
        scaled = scaled.quantize(
            Decimal(1).scaleb(-precision), rounding=ROUND_HALF_EVEN
        )
    return scaled


def _validate_trading_session(
    observed: datetime, profile: CsvSchemaProfile
) -> None:
    if not profile.trading_sessions:
        return
    local = observed.astimezone(ZoneInfo(profile.timezone_name))
    local_time = local.timetz().replace(tzinfo=None)
    if not any(
        local.weekday() in session["weekdays"]
        and time.fromisoformat(session["start"]) <= local_time
        < time.fromisoformat(session["end"])
        for session in profile.trading_sessions
    ):
        raise CsvRowError(
            "outside_trading_session",
            "timestamp is outside the configured trading sessions",
        )


def _parse_row(
    row: Mapping[str, Any], line_number: int, request: CsvImportRequest
) -> ParsedCsvBar:
    profile = request.profile
    external_symbol = (
        profile.fixed_external_symbol
        if profile.fixed_external_symbol is not None
        else _source_value(row, profile, "symbol")
    )
    instrument_id = request.instruments.resolve(str(external_symbol or ""))
    observed = _timestamp(_source_value(row, profile, "timestamp"), profile)
    _validate_trading_session(observed, profile)
    prices = {
        name: _scaled(
            _decimal(_source_value(row, profile, name), name, required=True),
            profile.price_multiplier,
            profile.price_precision,
        )
        for name in ("open", "high", "low", "close")
    }
    open_, high, low, close = (
        prices["open"], prices["high"], prices["low"], prices["close"]
    )
    assert all(value is not None for value in (open_, high, low, close))
    if any(value <= 0 for value in (open_, high, low, close)):
        raise CsvRowError("price_non_positive", "OHLC prices must be positive")
    if low > min(open_, close) or high < max(open_, close) or low > high:
        raise CsvRowError(
            "ohlc_inconsistent",
            "OHLC must satisfy low <= open/close <= high",
        )
    volume = _scaled(
        _decimal(
            _source_value(row, profile, "volume"), "volume", required=False
        ),
        profile.volume_multiplier,
        profile.volume_precision,
    )
    amount = _scaled(
        _decimal(
            _source_value(row, profile, "amount"), "amount", required=False
        ),
        profile.amount_multiplier,
        profile.amount_precision,
    )
    if volume is not None and volume < 0:
        raise CsvRowError("volume_negative", "volume must not be negative")
    if amount is not None and amount < 0:
        raise CsvRowError("amount_negative", "amount must not be negative")
    bar = {
        "bar_at": observed.isoformat(),
        "open": float(open_),
        "high": float(high),
        "low": float(low),
        "close": float(close),
        "volume": None if volume is None else float(volume),
        "amount": None if amount is None else float(amount),
        "source_sequence": str(line_number),
    }
    return ParsedCsvBar(line_number, instrument_id, bar)


def iter_csv_bars(
    stream: TextIO, request: CsvImportRequest
) -> Iterator[ParsedCsvBar]:
    reader = csv.DictReader(stream, delimiter=request.profile.delimiter)
    _validate_header(reader, request.profile)
    for line_number, row in enumerate(reader, start=2):
        try:
            yield _parse_row(row, line_number, request)
        except CsvRowError as exc:
            exc.line_number = line_number
            raise


def _validate_header(
    reader: csv.DictReader, profile: CsvSchemaProfile
) -> None:
    if reader.fieldnames is None:
        raise CsvImportContractError("CSV header is required")
    actual = {str(name).strip() for name in reader.fieldnames}
    expected = set(profile.columns.values())
    missing = expected - actual
    if missing:
        raise CsvImportContractError(
            "CSV is missing source columns: " + ", ".join(sorted(missing))
        )
    unexpected = actual - expected
    if unexpected and not profile.allow_extra_columns:
        raise CsvImportContractError(
            "CSV contains unexpected source columns: "
            + ", ".join(sorted(unexpected))
        )


def file_sha256(path: Path, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(chunk_size):
            digest.update(chunk)
    return digest.hexdigest()


def dry_run_csv(
    path: Path,
    request: CsvImportRequest,
    *,
    max_error_samples: int = 100,
) -> dict[str, Any]:
    resolved = path.resolve(strict=True)
    if not resolved.is_file():
        raise CsvImportContractError("CSV path must be a regular file")
    if max_error_samples < 0:
        raise ValueError("max_error_samples must not be negative")
    rows_total = rows_valid = rows_invalid = duplicate_rows = 0
    abnormal_price_rows = 0
    errors: list[dict[str, Any]] = []
    instruments: dict[str, dict[str, Any]] = {}
    last_timestamp: dict[str, datetime] = {}
    unordered_rows = 0
    gap_count = 0
    gap_samples: list[dict[str, Any]] = []
    error_counts: dict[str, int] = {}
    with tempfile.TemporaryDirectory(prefix="marketcow-csv-dry-run-") as folder:
        database = sqlite3.connect(str(Path(folder) / "keys.sqlite3"))
        try:
            database.execute(
                "CREATE TABLE bar_key ("
                "instrument_id TEXT NOT NULL, bar_at TEXT NOT NULL, "
                "PRIMARY KEY (instrument_id, bar_at))"
            )
            with resolved.open(
                "r", encoding=request.profile.encoding, newline=""
            ) as stream:
                reader = csv.DictReader(
                    stream, delimiter=request.profile.delimiter
                )
                _validate_header(reader, request.profile)
                for line_number, row in enumerate(reader, start=2):
                    rows_total += 1
                    try:
                        parsed = _parse_row(row, line_number, request)
                    except CsvRowError as exc:
                        rows_invalid += 1
                        error_counts[exc.code] = error_counts.get(exc.code, 0) + 1
                        if len(errors) < max_error_samples:
                            errors.append({
                                "line": line_number,
                                "code": exc.code,
                                "message": str(exc),
                            })
                        continue
                    bar_at = str(parsed.bar["bar_at"])
                    try:
                        database.execute(
                            "INSERT INTO bar_key(instrument_id, bar_at) VALUES (?, ?)",
                            (parsed.instrument_id, bar_at),
                        )
                    except sqlite3.IntegrityError:
                        duplicate_rows += 1
                        rows_invalid += 1
                        error_counts["duplicate_bar"] = (
                            error_counts.get("duplicate_bar", 0) + 1
                        )
                        if len(errors) < max_error_samples:
                            errors.append({
                                "line": parsed.line_number,
                                "code": "duplicate_bar",
                                "message": "instrument and timestamp are duplicated",
                            })
                        continue
                    observed = datetime.fromisoformat(bar_at)
                    if (
                        float(parsed.bar["high"]) / float(parsed.bar["low"])
                        >= 100
                    ):
                        abnormal_price_rows += 1
                    previous = last_timestamp.get(parsed.instrument_id)
                    if previous is not None and observed < previous:
                        unordered_rows += 1
                    last_timestamp[parsed.instrument_id] = observed
                    rows_valid += 1
                    summary = instruments.setdefault(parsed.instrument_id, {
                        "rows": 0, "first_bar_at": bar_at, "last_bar_at": bar_at,
                    })
                    summary["rows"] += 1
                    summary["first_bar_at"] = min(summary["first_bar_at"], bar_at)
                    summary["last_bar_at"] = max(summary["last_bar_at"], bar_at)
            interval_seconds = INTRADAY_INTERVAL_SECONDS.get(request.interval)
            if interval_seconds is not None:
                previous_key: tuple[str, datetime] | None = None
                for instrument_id, bar_at in database.execute(
                    "SELECT instrument_id, bar_at FROM bar_key "
                    "ORDER BY instrument_id, bar_at"
                ):
                    observed = datetime.fromisoformat(bar_at)
                    if (
                        previous_key is not None
                        and previous_key[0] == instrument_id
                        and observed.date() == previous_key[1].date()
                        and (
                            observed - previous_key[1]
                        ).total_seconds() > interval_seconds
                    ):
                        gap_count += 1
                        if len(gap_samples) < max_error_samples:
                            gap_samples.append({
                                "instrument_id": instrument_id,
                                "after": previous_key[1].isoformat(),
                                "before": observed.isoformat(),
                                "missing_intervals": int(
                                    (
                                        observed - previous_key[1]
                                    ).total_seconds() // interval_seconds
                                ) - 1,
                            })
                    previous_key = (instrument_id, observed)
        finally:
            database.close()
    return {
        "contract_version": CSV_IMPORT_CONTRACT_VERSION,
        "status": "valid" if rows_invalid == 0 else "invalid",
        "dry_run": True,
        "file": {
            "name": resolved.name,
            "byte_size": resolved.stat().st_size,
            "sha256": file_sha256(resolved),
        },
        "source": request.source,
        "profile": request.profile.identity,
        "namespace": request.instruments.namespace,
        "interval": request.interval,
        "adjustment": request.adjustment,
        "rows_total": rows_total,
        "rows_valid": rows_valid,
        "rows_invalid": rows_invalid,
        "duplicate_rows": duplicate_rows,
        "unordered_rows": unordered_rows,
        "gap_count": gap_count,
        "abnormal_price_rows": abnormal_price_rows,
        "gap_samples": gap_samples,
        "warnings": (
            [{
                "code": "intraday_gaps_detected",
                "count": gap_count,
                "policy": "warning",
            }] if gap_count else []
        ) + (
            [{
                "code": "abnormal_intrabar_price_ratio",
                "count": abnormal_price_rows,
                "threshold": 100,
                "policy": "warning",
            }] if abnormal_price_rows else []
        ) + (
            [] if request.profile.trading_sessions else [{
                "code": "trading_calendar_not_configured",
                "policy": "warning",
            }]
        ),
        "error_counts": dict(sorted(error_counts.items())),
        "error_samples": errors,
        "errors_truncated": rows_invalid > len(errors),
        "instruments": dict(sorted(instruments.items())),
    }
