from __future__ import annotations

import csv
import hashlib
import sqlite3
import tempfile
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Iterator, Mapping, TextIO
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from .instruments import canonical_instrument


CSV_IMPORT_CONTRACT_VERSION = "marketcow.csv-bars.v1"
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
        try:
            ZoneInfo(self.timezone_name)
        except ZoneInfoNotFoundError as exc:
            raise CsvImportContractError("profile timezone is unknown") from exc
        object.__setattr__(self, "name", name)
        object.__setattr__(self, "version", version)
        object.__setattr__(self, "columns", normalized)

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


@dataclass(frozen=True)
class CsvImportRequest:
    source: str
    interval: str
    adjustment: str
    profile: CsvSchemaProfile
    instruments: InstrumentMapping

    def __post_init__(self) -> None:
        source = self.source.strip().lower()
        if not source:
            raise CsvImportContractError("source is required")
        if self.interval not in SUPPORTED_INTERVALS:
            raise CsvImportContractError("unsupported CSV bar interval")
        if self.adjustment not in {"raw", "adjusted"}:
            raise CsvImportContractError("adjustment must be raw or adjusted")
        object.__setattr__(self, "source", source)


@dataclass(frozen=True)
class ParsedCsvBar:
    line_number: int
    instrument_id: str
    bar: Mapping[str, Any]


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
        parsed = parsed.replace(tzinfo=ZoneInfo(profile.timezone_name))
    return parsed.astimezone(timezone.utc)


def _source_value(
    row: Mapping[str, Any], profile: CsvSchemaProfile, field_name: str
) -> Any:
    source_name = profile.columns.get(field_name)
    return None if source_name is None else row.get(source_name)


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
    prices = {
        name: _decimal(_source_value(row, profile, name), name, required=True)
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
    volume = _decimal(
        _source_value(row, profile, "volume"), "volume", required=False
    )
    amount = _decimal(
        _source_value(row, profile, "amount"), "amount", required=False
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
    errors: list[dict[str, Any]] = []
    instruments: dict[str, dict[str, Any]] = {}
    last_timestamp: dict[str, datetime] = {}
    unordered_rows = 0
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
        "error_counts": dict(sorted(error_counts.items())),
        "error_samples": errors,
        "errors_truncated": rows_invalid > len(errors),
        "instruments": dict(sorted(instruments.items())),
    }
