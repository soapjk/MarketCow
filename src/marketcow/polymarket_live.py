from __future__ import annotations

import asyncio
import fcntl
import hashlib
import json
import logging
import os
import shutil
import sqlite3
import tempfile
import threading
import time
from collections import defaultdict, deque
from contextlib import contextmanager
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Callable, Iterable, Literal
from urllib.parse import quote

import requests
import websockets
from pydantic import BaseModel, Field, model_validator

from .polymarket_contracts import (
    CONTRACT_VERSION,
    GapEntry,
    OutcomeToken,
    PredictionMarketIdentity,
    canonical_json,
    content_sha256,
    decimal_text,
)
from .polymarket_history import _validate_book
from .polymarket_sources import _atomic_write, utc_now


LIVE_SCHEMA_VERSION = "marketcow.polymarket.live.v2"
PUBLIC_DATA_KINDS = frozenset({"trades", "activity", "positions", "holders"})
LOGGER = logging.getLogger(__name__)
SETTLEMENT_CURRENCY = "pUSD"
SIZE_INCREMENT = "0.01"
SETTLEMENT_SOURCE_URL = "https://docs.polymarket.com/concepts/pusd"
FEE_SOURCE_URL = "https://docs.polymarket.com/trading/fees"
SIZE_INCREMENT_SOURCE_URL = (
    "https://github.com/Polymarket/py-clob-client/blob/"
    "b076b04d61135657e25dccc1bbd6866a96bd8c6e/"
    "py_clob_client/order_builder/constants.py"
)


_PUBLICATION_LOCKS = threading.local()


@contextmanager
def _publication_lock(root: Path, *, exclusive: bool):
    """Serialize cross-process catalog/event/index publication with API reads."""
    path = (root.resolve() / ".publication.lock")
    key = str(path)
    held = getattr(_PUBLICATION_LOCKS, "held", None)
    if held is None:
        held = {}
        _PUBLICATION_LOCKS.held = held
    current = held.get(key)
    if current is not None:
        if exclusive and not current[2]:
            raise RuntimeError("cannot upgrade a shared live publication lock")
        held[key] = (current[0], current[1] + 1, current[2])
        try:
            yield
        finally:
            stream, count, mode = held[key]
            held[key] = (stream, count - 1, mode)
        return

    path.parent.mkdir(parents=True, exist_ok=True)
    gate = (root.resolve() / ".publication.gate.lock").open("a+b")
    stream = path.open("a+b")
    fcntl.flock(gate.fileno(), fcntl.LOCK_EX)
    try:
        fcntl.flock(
            stream.fileno(), fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH,
        )
    except BaseException:
        fcntl.flock(gate.fileno(), fcntl.LOCK_UN)
        gate.close()
        stream.close()
        raise
    if not exclusive:
        fcntl.flock(gate.fileno(), fcntl.LOCK_UN)
        gate.close()
        gate = None
    held[key] = ((stream, gate), 1, exclusive)
    try:
        yield
    finally:
        held.pop(key, None)
        fcntl.flock(stream.fileno(), fcntl.LOCK_UN)
        stream.close()
        if gate is not None:
            fcntl.flock(gate.fileno(), fcntl.LOCK_UN)
            gate.close()


def _instant(value: Any) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, (int, str)) and str(value).isdigit():
        number = int(value)
        if number > 10**12:
            number /= 1000
        parsed = datetime.fromtimestamp(number, timezone.utc)
    else:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("timestamp must include a timezone")
    return parsed.astimezone(timezone.utc)


def _list(value: Any) -> list[str]:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError:
            value = [part.strip() for part in value.split(",")]
    return [str(item) for item in (value or [])]


def _bool(value: Any) -> bool:
    if isinstance(value, str):
        if value.lower() not in {"true", "false"}:
            raise ValueError("boolean source field must be true or false")
        return value.lower() == "true"
    return bool(value)


def _bps_rate(value: Any, field: str) -> str:
    bps = Decimal(decimal_text(value, field))
    return format(bps / Decimal("10000"), "f")


def _missing_fields(
    required: dict[str, Any],
    *,
    intervals: Iterable[tuple[str, datetime | None, datetime | None]] = (),
) -> list[str]:
    """Single source of truth for typed-fact completeness."""
    missing = {name for name, value in required.items() if value is None}
    for name, start, end in intervals:
        if start is not None and end is not None and start >= end:
            missing.add(name)
    return sorted(missing)


def _instrument_missing_fields(values: dict[str, Any]) -> list[str]:
    return _missing_fields(
        values,
        intervals=[(
            "activation_expiration_interval",
            values.get("activation_at"),
            values.get("expiration_at"),
        )],
    )


def _fee_missing_fields(
    values: dict[str, Any], effective_to: datetime | None,
) -> list[str]:
    return _missing_fields(
        values,
        intervals=[(
            "effective_interval", values.get("effective_from"), effective_to,
        )],
    )


def _levels(value: Any, field: str) -> list[dict[str, str]]:
    levels = {}
    for item in value or []:
        price = decimal_text(item.get("price"), f"{field}.price")
        levels[price] = decimal_text(item.get("size"), f"{field}.size")
    return [
        {"price": price, "size": levels[price]}
        for price in sorted(
            levels, key=Decimal, reverse=field == "bids",
        )
    ]


def _state_checksum(token_id: str, tick_size: str, bids: dict[str, str], asks: dict[str, str]) -> str:
    return content_sha256({
        "token_id": token_id,
        "tick_size": tick_size,
        "bids": [
            {"price": price, "size": bids[price]}
            for price in sorted(bids, key=Decimal, reverse=True)
        ],
        "asks": [
            {"price": price, "size": asks[price]}
            for price in sorted(asks, key=Decimal)
        ],
    })


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _atomic_copy(source: Path, target: Path) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(dir=target.parent, delete=False) as output:
            temporary = Path(output.name)
            with source.open("rb") as input_stream:
                shutil.copyfileobj(input_stream, output, length=1024 * 1024)
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, target)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def _market_sequence_sha256(markets: list[LiveMarket]) -> str:
    digest = hashlib.sha256()
    digest.update(b"[")
    for index, market in enumerate(markets):
        if index:
            digest.update(b",")
        digest.update(canonical_json(market.model_dump(mode="json")))
    digest.update(b"]")
    return digest.hexdigest()


CATALOG_INDEX_SCHEMA_VERSION = "marketcow.polymarket.catalog-index.v1"
LIVE_READ_SCHEMA_VERSION = "marketcow.polymarket.live-read-health.v1"
STATE_INDEX_SCHEMA_VERSION = "marketcow.polymarket.state-index.v1"


def _readonly_sqlite(path: Path) -> sqlite3.Connection:
    uri = f"file:{quote(str(path), safe='/')}?mode=ro&immutable=1"
    connection = sqlite3.connect(uri, uri=True)
    connection.row_factory = sqlite3.Row
    return connection


def _readonly_state_sqlite(path: Path) -> sqlite3.Connection:
    uri = f"file:{quote(str(path), safe='/')}?mode=ro"
    connection = sqlite3.connect(uri, uri=True)
    connection.row_factory = sqlite3.Row
    return connection


def _catalog_index_metadata(connection: sqlite3.Connection) -> dict[str, str]:
    return {
        str(row["key"]): str(row["value"])
        for row in connection.execute("SELECT key, value FROM metadata")
    }


def _write_catalog_index(
    path: Path,
    *,
    revision: str,
    catalog_sha256: str,
    rows: Iterable[tuple[LiveMarket, bytes]],
    expected_market_count: int,
) -> dict[str, Any]:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary: Path | None = None
    token_count = 0
    market_count = 0
    try:
        descriptor, temporary_name = tempfile.mkstemp(
            dir=path.parent, prefix=f".{path.name}.", suffix=".tmp",
        )
        os.close(descriptor)
        temporary = Path(temporary_name)
        with sqlite3.connect(temporary) as connection:
            connection.executescript("""
                PRAGMA journal_mode=DELETE;
                PRAGMA synchronous=FULL;
                CREATE TABLE metadata (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                ) WITHOUT ROWID;
                CREATE TABLE markets (
                    market_id TEXT PRIMARY KEY,
                    byte_offset INTEGER NOT NULL CHECK(byte_offset >= 0),
                    byte_length INTEGER NOT NULL CHECK(byte_length > 0),
                    row_sha256 TEXT NOT NULL,
                    active INTEGER NOT NULL CHECK(active IN (0, 1)),
                    closed INTEGER NOT NULL CHECK(closed IN (0, 1)),
                    catalog_revision TEXT NOT NULL
                ) WITHOUT ROWID;
                CREATE TABLE tokens (
                    token_id TEXT PRIMARY KEY,
                    market_id TEXT NOT NULL,
                    outcome_label TEXT NOT NULL,
                    FOREIGN KEY(market_id) REFERENCES markets(market_id)
                ) WITHOUT ROWID;
                CREATE INDEX tokens_market_id ON tokens(market_id, token_id);
            """)
            offset = 0
            for market, body in rows:
                market_count += 1
                market_id = market.identity.market_id
                connection.execute(
                    """INSERT INTO markets(
                        market_id, byte_offset, byte_length, row_sha256,
                        active, closed, catalog_revision
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    (
                        market_id, offset, len(body), hashlib.sha256(body).hexdigest(),
                        int(market.active), int(market.closed), revision,
                    ),
                )
                connection.executemany(
                    "INSERT INTO tokens(token_id, market_id, outcome_label) VALUES (?, ?, ?)",
                    [
                        (outcome.token_id, market_id, outcome.outcome)
                        for outcome in market.identity.outcomes
                    ],
                )
                token_count += len(market.identity.outcomes)
                offset += len(body) + 1
            if market_count != expected_market_count:
                raise RuntimeError("live catalog index row count mismatch")
            metadata = {
                "schema_version": CATALOG_INDEX_SCHEMA_VERSION,
                "catalog_revision": revision,
                "catalog_sha256": catalog_sha256,
                "market_count": str(market_count),
                "token_count": str(token_count),
            }
            connection.executemany(
                "INSERT INTO metadata(key, value) VALUES (?, ?)",
                sorted(metadata.items()),
            )
            connection.commit()
        with temporary.open("rb") as stream:
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        temporary = None
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)

    index_sha256 = _file_sha256(path)
    with _readonly_sqlite(path) as connection:
        observed = _catalog_index_metadata(connection)
        observed_markets = connection.execute(
            "SELECT COUNT(*) FROM markets"
        ).fetchone()[0]
        observed_tokens = connection.execute(
            "SELECT COUNT(*) FROM tokens"
        ).fetchone()[0]
    if (
        observed.get("schema_version") != CATALOG_INDEX_SCHEMA_VERSION
        or observed.get("catalog_revision") != revision
        or observed.get("catalog_sha256") != catalog_sha256
        or int(observed.get("market_count", "-1")) != market_count
        or int(observed.get("token_count", "-1")) != token_count
        or observed_markets != market_count
        or observed_tokens != token_count
    ):
        raise RuntimeError("live catalog index integrity failed before publication")
    return {
        "format": "sqlite-offset-v1",
        "schema_version": CATALOG_INDEX_SCHEMA_VERSION,
        "path": str(path),
        "sha256": index_sha256,
        "catalog_revision": revision,
        "catalog_sha256": catalog_sha256,
        "market_count": market_count,
        "token_count": token_count,
    }


def _reuse_catalog_index(
    path: Path,
    *,
    revision: str,
    catalog_sha256: str,
    rows: list[tuple[LiveMarket, bytes]],
) -> dict[str, Any]:
    try:
        with _readonly_sqlite(path) as connection:
            metadata = _catalog_index_metadata(connection)
            indexed_markets = {
                str(row["market_id"]): row
                for row in connection.execute("SELECT * FROM markets")
            }
            indexed_tokens = {
                (str(row["token_id"]), str(row["market_id"]), str(row["outcome_label"]))
                for row in connection.execute(
                    "SELECT token_id, market_id, outcome_label FROM tokens"
                )
            }
    except sqlite3.DatabaseError as exc:
        raise RuntimeError("existing live catalog index is invalid") from exc
    offset = 0
    expected_tokens = set()
    for market, body in rows:
        market_id = market.identity.market_id
        indexed = indexed_markets.get(market_id)
        if indexed is None or (
            int(indexed["byte_offset"]) != offset
            or int(indexed["byte_length"]) != len(body)
            or indexed["row_sha256"] != hashlib.sha256(body).hexdigest()
            or indexed["catalog_revision"] != revision
        ):
            raise RuntimeError("existing live catalog index row binding mismatch")
        expected_tokens.update(
            (outcome.token_id, market_id, outcome.outcome)
            for outcome in market.identity.outcomes
        )
        offset += len(body) + 1
    if (
        metadata.get("schema_version") != CATALOG_INDEX_SCHEMA_VERSION
        or metadata.get("catalog_revision") != revision
        or metadata.get("catalog_sha256") != catalog_sha256
        or int(metadata.get("market_count", "-1")) != len(rows)
        or indexed_tokens != expected_tokens
    ):
        raise RuntimeError("existing live catalog index metadata mismatch")
    return {
        "format": "sqlite-offset-v1",
        "schema_version": CATALOG_INDEX_SCHEMA_VERSION,
        "path": str(path),
        "sha256": _file_sha256(path),
        "catalog_revision": revision,
        "catalog_sha256": catalog_sha256,
        "market_count": len(rows),
        "token_count": len(expected_tokens),
    }


def _atomic_write_catalog(
    path: Path,
    *,
    revision: str,
    markets: list[LiveMarket],
    source: dict[str, Any],
) -> dict[str, Any]:
    normalized_root = path.parent / "catalogs"
    normalized_root.mkdir(parents=True, exist_ok=True)
    normalized_path = normalized_root / f"{revision}.jsonl"
    normalized_rows = [
        (market, canonical_json(market.model_dump(mode="json")))
        for market in markets
    ]
    normalized_hasher = hashlib.sha256()
    for _, body in normalized_rows:
        normalized_hasher.update(body + b"\n")
    expected_normalized_sha256 = normalized_hasher.hexdigest()
    temporary = None
    if not normalized_path.exists():
        try:
            with tempfile.NamedTemporaryFile(
                dir=normalized_root, delete=False,
            ) as stream:
                temporary = Path(stream.name)
                for _, body in normalized_rows:
                    stream.write(body + b"\n")
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, normalized_path)
        finally:
            if temporary is not None:
                temporary.unlink(missing_ok=True)
    if _file_sha256(normalized_path) != expected_normalized_sha256:
        raise RuntimeError("normalized live catalog integrity failed before publication")
    normalized = {
        "format": "canonical_jsonl",
        "market_count": len(markets),
        "path": str(normalized_path),
        "sha256": expected_normalized_sha256,
    }
    index_path = path.parent / "catalog-indexes" / f"{revision}.sqlite3"
    if index_path.exists():
        catalog_index = _reuse_catalog_index(
            index_path,
            revision=revision,
            catalog_sha256=expected_normalized_sha256,
            rows=normalized_rows,
        )
    else:
        catalog_index = _write_catalog_index(
            index_path,
            revision=revision,
            catalog_sha256=expected_normalized_sha256,
            rows=normalized_rows,
            expected_market_count=len(normalized_rows),
        )
    _atomic_write(path, canonical_json({
        "catalog_revision": revision,
        "catalog_source": source,
        "catalog_index": catalog_index,
        "normalized_catalog": normalized,
        "schema_version": LIVE_SCHEMA_VERSION,
    }))
    return normalized


def build_live_catalog_index(root: Path) -> dict[str, Any]:
    """Build and atomically publish an offset index for one verified legacy catalog."""
    root = root.resolve()
    manifest_path = root / "catalog.json"
    normalized_root = root / "catalogs"
    index_root = root / "catalog-indexes"
    try:
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise RuntimeError("live catalog manifest is unavailable or invalid") from exc
    normalized = payload.get("normalized_catalog")
    if not isinstance(normalized, dict):
        raise RuntimeError("live catalog lacks normalized JSONL metadata")
    normalized_path = Path(str(normalized.get("path") or "")).resolve()
    if not normalized_path.is_relative_to(normalized_root):
        raise RuntimeError("normalized live catalog escapes local storage root")
    expected_sha256 = str(normalized.get("sha256") or "")
    expected_count = int(normalized.get("market_count") or -1)
    revision = str(payload.get("catalog_revision") or "")
    if not normalized_path.is_file() or _file_sha256(normalized_path) != expected_sha256:
        raise RuntimeError("normalized live catalog integrity failed")

    digest = hashlib.sha256()
    digest.update(b"[")
    count = 0
    with normalized_path.open("rb") as stream:
        for line in stream:
            body = line.removesuffix(b"\n")
            if not body:
                continue
            market = LiveMarket.model_validate_json(body)
            canonical = canonical_json(market.model_dump(mode="json"))
            if canonical != body:
                raise RuntimeError("normalized live catalog row is not canonical")
            if count:
                digest.update(b",")
            digest.update(body)
            count += 1
    digest.update(b"]")
    if count != expected_count or digest.hexdigest() != revision:
        raise RuntimeError("live catalog revision or row count mismatch")

    def rows() -> Iterable[tuple[LiveMarket, bytes]]:
        with normalized_path.open("rb") as stream:
            for line in stream:
                body = line.removesuffix(b"\n")
                if body:
                    yield LiveMarket.model_validate_json(body), body

    index_path = index_root / f"{revision}.sqlite3"
    catalog_index = _write_catalog_index(
        index_path,
        revision=revision,
        catalog_sha256=expected_sha256,
        rows=rows(),
        expected_market_count=expected_count,
    )
    payload["catalog_index"] = catalog_index
    _atomic_write(manifest_path, canonical_json(payload))
    return catalog_index


class LiveFactProvenance(BaseModel):
    source: Literal["polymarket_gamma", "polymarket_docs", "polymarket_sdk"]
    revision: str
    source_url: str
    observed_at: datetime
    payload_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    field_paths: list[str] = Field(min_length=1)


class LiveInstrumentFacts(BaseModel):
    facts_version: Literal["marketcow.polymarket.live-instrument-facts.v1"] = (
        "marketcow.polymarket.live-instrument-facts.v1"
    )
    revision: str = Field(pattern=r"^[0-9a-f]{64}$")
    settlement_currency: str | None = None
    activation_at: datetime | None = None
    expiration_at: datetime | None = None
    price_increment: str | None = None
    size_increment: str | None = None
    minimum_order_size: str | None = None
    provenance: list[LiveFactProvenance] = Field(min_length=1)
    missing_fields: list[str] = Field(default_factory=list)
    complete: bool

    @model_validator(mode="after")
    def complete_nautilus_facts(self):
        for field in ("price_increment", "size_increment", "minimum_order_size"):
            value = getattr(self, field)
            if value is not None:
                setattr(self, field, decimal_text(value, field, allow_zero=False))
        required = {
            "settlement_currency": self.settlement_currency,
            "activation_at": self.activation_at,
            "expiration_at": self.expiration_at,
            "price_increment": self.price_increment,
            "size_increment": self.size_increment,
            "minimum_order_size": self.minimum_order_size,
        }
        missing = _instrument_missing_fields(required)
        if sorted(set(self.missing_fields)) != sorted(set(missing)):
            raise ValueError("instrument missing_fields must describe the typed facts")
        if self.complete != (not missing):
            raise ValueError("instrument completeness disagrees with typed facts")
        return self


class LiveFeeSchedule(BaseModel):
    schedule_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    schedule_version: str
    currency: str | None = None
    maker_rate: str | None = None
    taker_rate: str | None = None
    formula: str | None = None
    exponent: str | None = None
    quantum: str | None = None
    rounding_mode: Literal[
        "ROUND_DOWN", "ROUND_HALF_EVEN", "ROUND_HALF_UP", "UNSPECIFIED"
    ] = "UNSPECIFIED"
    tie_semantics: Literal[
        "toward_zero", "ties_to_even", "ties_away_from_zero", "unspecified"
    ] = "unspecified"
    calculation_status: Literal["executable_pnl", "informational_only"] = (
        "informational_only"
    )
    effective_from: datetime | None = None
    effective_to: datetime | None = None
    provenance: list[LiveFactProvenance] = Field(min_length=1)
    missing_fields: list[str] = Field(default_factory=list)
    complete: bool

    @model_validator(mode="after")
    def typed_schedule(self):
        for field in ("maker_rate", "taker_rate", "exponent", "quantum"):
            value = getattr(self, field)
            if value is not None:
                setattr(
                    self, field,
                    decimal_text(value, field, allow_zero=field != "quantum"),
                )
        expected_ties = {
            "ROUND_DOWN": "toward_zero",
            "ROUND_HALF_EVEN": "ties_to_even",
            "ROUND_HALF_UP": "ties_away_from_zero",
            "UNSPECIFIED": "unspecified",
        }
        if self.tie_semantics != expected_ties[self.rounding_mode]:
            raise ValueError("fee rounding mode and tie semantics disagree")
        if self.calculation_status == "executable_pnl" and self.rounding_mode == "UNSPECIFIED":
            raise ValueError("executable PnL requires deterministic fee rounding")
        required = {
            "currency": self.currency,
            "maker_rate": self.maker_rate,
            "taker_rate": self.taker_rate,
            "formula": self.formula,
            "exponent": self.exponent,
            "quantum": self.quantum,
            "effective_from": self.effective_from,
        }
        missing = _fee_missing_fields(required, self.effective_to)
        if sorted(set(self.missing_fields)) != sorted(set(missing)):
            raise ValueError("fee missing_fields must describe the typed schedule")
        if self.complete != (not missing):
            raise ValueError("fee completeness disagrees with typed schedule")
        return self


class LiveRuleSet(BaseModel):
    rule_version: str
    instrument: LiveInstrumentFacts
    fee_schedule: LiveFeeSchedule
    rules_complete: bool

    @model_validator(mode="after")
    def completeness(self):
        if self.rules_complete != self.instrument.complete:
            raise ValueError("rule completeness must match instrument facts")
        return self


class LiveOutcomePair(BaseModel):
    pair_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    pair_revision: str = Field(pattern=r"^[0-9a-f]{64}$")
    event_id: str
    market_id: str
    condition_id: str
    outcome_label: str
    yes_token_id: str
    yes_instrument_id: str
    no_token_id: str
    no_instrument_id: str
    valid_from: datetime
    valid_to: datetime | None = None
    provenance: LiveFactProvenance


class LiveRelation(BaseModel):
    relation_id: str
    relation_type: Literal["binary_complements", "standard_negative_risk"]
    members: list[str]
    convertible: bool
    revision: str
    rule_version: str
    valid_from: datetime
    valid_to: datetime | None = None
    source: Literal["polymarket_gamma"] = "polymarket_gamma"
    provenance: LiveFactProvenance
    outcome_pairs: list[LiveOutcomePair] = Field(default_factory=list)
    missing_fields: list[str] = Field(default_factory=list)
    complete: bool

    @model_validator(mode="after")
    def typed_members(self):
        if len(set(self.members)) != len(self.members):
            raise ValueError("relation members must be unique")
        if self.relation_type == "binary_complements":
            if len(self.members) != 2 or self.outcome_pairs:
                raise ValueError("binary relation requires exactly two instruments")
        else:
            yes_members = [item.yes_instrument_id for item in self.outcome_pairs]
            if self.members != sorted(set(yes_members)):
                raise ValueError("negative-risk members must be the explicit YES set")
            if self.complete != (len(self.outcome_pairs) >= 2 and not self.missing_fields):
                raise ValueError("negative-risk relation completeness disagrees")
        return self


class LiveMarket(BaseModel):
    identity: PredictionMarketIdentity
    question: str
    title: str
    active: bool
    closed: bool
    accepting_orders: bool
    lifecycle_state: Literal["active", "closed", "resolved", "invalid"]
    resolution: str | None = None
    start_at: datetime | None = None
    end_at: datetime | None = None
    metadata_revision: str
    observed_at: datetime
    rules: LiveRuleSet
    relations: list[LiveRelation]
    raw_payload_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def fail_closed_metadata(self):
        if len(self.identity.outcomes) != 2:
            raise ValueError("live binary market requires two outcome tokens")
        if self.identity.neg_risk and not any(
            item.relation_type == "standard_negative_risk" for item in self.relations
        ):
            raise ValueError("negative-risk market requires source-backed relation")
        if self.lifecycle_state == "resolved" and not self.resolution:
            raise ValueError("resolved live market requires a resolution")
        return self


class LiveBook(BaseModel):
    token_id: str
    condition_id: str
    book_epoch: str
    sequence: int = Field(ge=1)
    sequence_semantics: Literal["deterministic_normalized"] = "deterministic_normalized"
    exchange_at: datetime
    received_at: datetime
    tick_version: str
    tick_size: str
    bids: list[dict[str, str]]
    asks: list[dict[str, str]]
    last_trade_price: str | None = None
    state_checksum: str = Field(pattern=r"^[0-9a-f]{64}$")
    source_hash: str | None = None


class LiveEventEnvelope(BaseModel):
    contract_version: Literal["marketcow.prediction_market.v1"] = CONTRACT_VERSION
    schema_version: Literal["marketcow.polymarket.live.v2"] = LIVE_SCHEMA_VERSION
    cursor: int = Field(ge=1)
    event_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    event_type: Literal[
        "book", "price_change", "best_bid_ask", "last_trade_price",
        "tick_size_change", "new_market", "market_resolved", "catalog_revision",
        "recovery_started", "recovery_completed", "subscription_change",
    ]
    market_id: str | None = None
    condition_id: str | None = None
    token_id: str | None = None
    book_epoch: str | None = None
    sequence: int | None = None
    exchange_at: datetime
    received_at: datetime
    canonical_payload: dict[str, Any]
    canonical_payload_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    raw_payload: dict[str, Any]
    raw_payload_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    applied: bool
    fail_closed_reason: str | None = None
    gaps: list[GapEntry] = Field(default_factory=list)


def live_event_identity(event: LiveEventEnvelope) -> str:
    payload = event.model_dump(mode="json")
    payload.pop("event_id", None)
    return content_sha256(payload)


class LiveCheckpoint(BaseModel):
    schema_version: Literal["marketcow.polymarket.live-checkpoint.v2"] = (
        "marketcow.polymarket.live-checkpoint.v2"
    )
    cursor: int = Field(ge=0)
    catalog_revision: str | None = None
    created_at: datetime
    books: dict[str, LiveBook]
    unresolved_gaps: list[GapEntry]
    state_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")


class MarketFrame(BaseModel):
    market_id: str
    condition_id: str
    frame_at: datetime
    cursor: int
    status: Literal["ready", "fail_closed"]
    reason_codes: list[str]
    tokens: list[LiveBook]
    relation_ids: list[str]
    relation_tokens: list[LiveBook]
    relation_pairs: list[LiveOutcomePair]
    instrument_revision: str
    fee_schedule_id: str


class LiveHealth(BaseModel):
    status: Literal["ready", "degraded", "empty"]
    catalog_revision: str | None
    active_market_count: int
    subscribed_token_count: int
    book_token_count: int
    missing_book_token_count: int
    ready_market_count: int
    unresolved_gap_count: int
    latest_cursor: int
    latest_received_at: datetime | None
    lag_ms: int | None
    source_policy: Literal["official_free_only"] = "official_free_only"


class LiveBootstrapResponse(BaseModel):
    contract_version: Literal["marketcow.prediction_market.v1"] = CONTRACT_VERSION
    schema_version: Literal["marketcow.polymarket.live-bootstrap.v2"] = (
        "marketcow.polymarket.live-bootstrap.v2"
    )
    catalog_revision: str | None
    catalog_source: dict[str, Any] | None
    cursor: int = Field(ge=0)
    markets: list[LiveMarket]
    active_token_ids: list[str]
    sequence_semantics: Literal["deterministic_normalized"]
    recovery: dict[str, str]
    source_policy: Literal["official_free_only"]


class LiveSnapshotPage(BaseModel):
    schema_version: Literal["marketcow.polymarket.live-snapshot.v2"] = (
        "marketcow.polymarket.live-snapshot.v2"
    )
    catalog_revision: str | None
    cursor: int = Field(ge=0)
    count: int = Field(ge=0)
    items: list[MarketFrame]


class LiveEventPage(BaseModel):
    schema_version: Literal["marketcow.polymarket.live-events.v2"] = (
        "marketcow.polymarket.live-events.v2"
    )
    after_cursor: int = Field(ge=0)
    next_cursor: int = Field(ge=0)
    has_more: bool
    items: list[LiveEventEnvelope]


class LiveGapPage(BaseModel):
    schema_version: Literal["marketcow.polymarket.live-gaps.v2"] = (
        "marketcow.polymarket.live-gaps.v2"
    )
    count: int = Field(ge=0)
    items: list[GapEntry]


class PublicDataPage(BaseModel):
    schema_version: Literal["marketcow.polymarket.public-data-page.v1"] = (
        "marketcow.polymarket.public-data-page.v1"
    )
    kind: Literal["trades", "activity", "positions", "holders"]
    count: int = Field(ge=0)
    items: list[dict[str, Any]]


class LiveReadHealth(BaseModel):
    schema_version: Literal[
        "marketcow.polymarket.live-read-health.v1"
    ] = LIVE_READ_SCHEMA_VERSION
    status: Literal[
        "not_configured", "legacy_unindexed", "index_ready",
        "degraded", "integrity_failed",
    ]
    catalog_revision: str | None = None
    catalog_index_ready: bool = False
    latest_state_ready: bool = False
    market_count: int = Field(default=0, ge=0)
    token_count: int = Field(default=0, ge=0)
    book_token_count: int = Field(default=0, ge=0)
    book_complete_market_count: int = Field(default=0, ge=0)
    unresolved_gap_count: int = Field(default=0, ge=0)
    latest_cursor: int = Field(default=0, ge=0)
    reason_codes: list[str] = Field(default_factory=list)
    source_policy: Literal["official_free_only"] = "official_free_only"


class PolymarketLiveReadError(RuntimeError):
    def __init__(self, code: str, message: str, status_code: int):
        super().__init__(message)
        self.code = code
        self.status_code = status_code


class PolymarketLiveReadStore:
    """Bounded, provider-neutral reads over immutable live indexes."""

    max_scope_markets = 100

    def __init__(self, root: Path, *, now_provider: Callable[[], datetime] = utc_now):
        self.root = root.resolve()
        self.now_provider = now_provider
        self.catalog_path = self.root / "catalog.json"
        self.normalized_catalog_root = self.root / "catalogs"
        self.catalog_index_root = self.root / "catalog-indexes"
        self.state_index_root = self.root / "indexes"
        self.state_manifest_path = self.root / "state-index.json"
        self.event_path = self.root / "events.jsonl"
        self._binding_cache: tuple[
            str, tuple[int, int], dict[str, Any], Path, Path, dict[str, str]
        ] | None = None

    def _manifest_binding(
        self,
    ) -> tuple[dict[str, Any], Path, Path, dict[str, str]]:
        # The manifest is atomically replaced and its referenced catalog/index
        # files are immutable, so readers do not need to contend with live
        # event publication.
        return self._manifest_binding_unlocked()

    def _manifest_binding_unlocked(
        self,
    ) -> tuple[dict[str, Any], Path, Path, dict[str, str]]:
        if not self.catalog_path.exists():
            raise PolymarketLiveReadError(
                "polymarket_catalog_index_unavailable",
                "Polymarket live catalog is not configured",
                503,
            )
        try:
            manifest_body = self.catalog_path.read_bytes()
            manifest_sha256 = hashlib.sha256(manifest_body).hexdigest()
            payload = json.loads(manifest_body)
        except (OSError, ValueError) as exc:
            raise PolymarketLiveReadError(
                "polymarket_catalog_integrity_failed",
                "Polymarket live catalog manifest is invalid",
                409,
            ) from exc
        normalized = payload.get("normalized_catalog")
        catalog_index = payload.get("catalog_index")
        if not isinstance(normalized, dict):
            raise PolymarketLiveReadError(
                "polymarket_catalog_integrity_failed",
                "Polymarket live catalog lacks normalized metadata",
                409,
            )
        if not isinstance(catalog_index, dict):
            raise PolymarketLiveReadError(
                "polymarket_catalog_index_unavailable",
                "Polymarket live catalog is legacy and has no offset index",
                503,
            )
        normalized_path = Path(str(normalized.get("path") or "")).resolve()
        index_path = Path(str(catalog_index.get("path") or "")).resolve()
        if not normalized_path.is_relative_to(self.normalized_catalog_root):
            raise PolymarketLiveReadError(
                "polymarket_catalog_integrity_failed",
                "Normalized catalog escapes the live storage root",
                409,
            )
        if not index_path.is_relative_to(self.catalog_index_root):
            raise PolymarketLiveReadError(
                "polymarket_catalog_integrity_failed",
                "Catalog index escapes the live storage root",
                409,
            )
        if not normalized_path.is_file() or not index_path.is_file():
            raise PolymarketLiveReadError(
                "polymarket_catalog_index_unavailable",
                "Polymarket catalog or index file is missing",
                503,
            )
        index_stat = index_path.stat()
        index_signature = (index_stat.st_size, index_stat.st_mtime_ns)
        if self._binding_cache is not None:
            cached_manifest, cached_index, *cached_binding = self._binding_cache
            if (
                cached_manifest == manifest_sha256
                and cached_index == index_signature
            ):
                cached_payload, cached_normalized, cached_path, cached_metadata = (
                    cached_binding
                )
                return (
                    cached_payload, cached_normalized, cached_path, cached_metadata,
                )
        if _file_sha256(index_path) != catalog_index.get("sha256"):
            raise PolymarketLiveReadError(
                "polymarket_catalog_integrity_failed",
                "Polymarket catalog index hash mismatch",
                409,
            )
        try:
            with _readonly_sqlite(index_path) as connection:
                metadata = _catalog_index_metadata(connection)
        except sqlite3.DatabaseError as exc:
            raise PolymarketLiveReadError(
                "polymarket_catalog_integrity_failed",
                "Polymarket catalog index is not readable",
                409,
            ) from exc
        revision = str(payload.get("catalog_revision") or "")
        expected = {
            "schema_version": CATALOG_INDEX_SCHEMA_VERSION,
            "catalog_revision": revision,
            "catalog_sha256": str(normalized.get("sha256") or ""),
            "market_count": str(normalized.get("market_count") or ""),
        }
        if any(metadata.get(key) != value for key, value in expected.items()):
            raise PolymarketLiveReadError(
                "polymarket_catalog_integrity_failed",
                "Polymarket catalog manifest and index binding disagree",
                409,
            )
        self._binding_cache = (
            manifest_sha256, index_signature, payload, normalized_path,
            index_path, metadata,
        )
        return payload, normalized_path, index_path, metadata

    @staticmethod
    def _scope(market_ids: Iterable[str]) -> list[str]:
        selected = list(dict.fromkeys(str(item).strip() for item in market_ids))
        if not selected or any(not item for item in selected):
            raise PolymarketLiveReadError(
                "polymarket_market_scope_required",
                "At least one non-empty market_id is required",
                400,
            )
        if len(selected) > PolymarketLiveReadStore.max_scope_markets:
            raise PolymarketLiveReadError(
                "polymarket_scope_too_large",
                "At most 100 distinct market_id values may be requested",
                400,
            )
        return selected

    def bootstrap(self, market_ids: Iterable[str]) -> LiveBootstrapResponse:
        selected_ids = self._scope(market_ids)
        payload, normalized_path, index_path, _ = self._manifest_binding()
        placeholders = ",".join("?" for _ in selected_ids)
        try:
            with _readonly_sqlite(index_path) as connection:
                indexed_rows = {
                    str(row["market_id"]): row
                    for row in connection.execute(
                        f"""SELECT market_id, byte_offset, byte_length, row_sha256
                            FROM markets WHERE market_id IN ({placeholders})""",
                        selected_ids,
                    )
                }
                token_rows = list(connection.execute(
                    f"""SELECT token_id, market_id, outcome_label
                        FROM tokens WHERE market_id IN ({placeholders})
                        ORDER BY market_id, token_id""",
                    selected_ids,
                ))
        except sqlite3.DatabaseError as exc:
            raise PolymarketLiveReadError(
                "polymarket_catalog_integrity_failed",
                "Polymarket catalog index query failed",
                409,
            ) from exc
        missing = [market_id for market_id in selected_ids if market_id not in indexed_rows]
        if missing:
            raise PolymarketLiveReadError(
                "polymarket_live_market_not_found",
                f"Unknown Polymarket live market_id: {missing[0]}",
                404,
            )
        markets = []
        with normalized_path.open("rb") as stream:
            for market_id in selected_ids:
                indexed = indexed_rows[market_id]
                stream.seek(int(indexed["byte_offset"]))
                body = stream.read(int(indexed["byte_length"]))
                delimiter = stream.read(1)
                if (
                    delimiter != b"\n"
                    or hashlib.sha256(body).hexdigest() != indexed["row_sha256"]
                ):
                    raise PolymarketLiveReadError(
                        "polymarket_catalog_row_integrity_failed",
                        f"Catalog row integrity failed for market_id {market_id}",
                        409,
                    )
                try:
                    market = LiveMarket.model_validate_json(body)
                except ValueError as exc:
                    raise PolymarketLiveReadError(
                        "polymarket_catalog_row_integrity_failed",
                        f"Catalog row schema failed for market_id {market_id}",
                        409,
                    ) from exc
                if market.identity.market_id != market_id:
                    raise PolymarketLiveReadError(
                        "polymarket_catalog_row_integrity_failed",
                        f"Catalog row identity failed for market_id {market_id}",
                        409,
                    )
                markets.append(market)
        expected_tokens = sorted(
            (outcome.token_id, market.identity.market_id, outcome.outcome)
            for market in markets for outcome in market.identity.outcomes
        )
        observed_tokens = sorted(
            (str(row["token_id"]), str(row["market_id"]), str(row["outcome_label"]))
            for row in token_rows
        )
        if expected_tokens != observed_tokens:
            raise PolymarketLiveReadError(
                "polymarket_catalog_integrity_failed",
                "Polymarket catalog token index disagrees with selected rows",
                409,
            )
        response = LiveBootstrapResponse(
            catalog_revision=str(payload["catalog_revision"]),
            catalog_source=payload.get("catalog_source"),
            cursor=0,
            markets=markets,
            active_token_ids=sorted(
                outcome.token_id
                for market in markets if market.active and not market.closed
                for outcome in market.identity.outcomes
            ),
            sequence_semantics="deterministic_normalized",
            recovery={
                "bootstrap": "CLOB POST /books full snapshots",
                "disconnect": "new book_epoch followed by full /books recovery",
                "resume": "state index required before event resume",
            },
            source_policy="official_free_only",
        )
        return self._bind_bootstrap_to_live_books(response)

    def _state_path(self) -> Path:
        _, path = self._state_manifest()
        return path

    def _state_manifest(self) -> tuple[dict[str, Any], Path]:
        if not self.state_manifest_path.is_file():
            raise PolymarketLiveReadError(
                "polymarket_latest_state_index_unavailable",
                "Polymarket latest-state index is not published",
                503,
            )
        try:
            manifest = json.loads(self.state_manifest_path.read_text(encoding="utf-8"))
            path = Path(str(manifest.get("path") or "")).resolve()
        except (OSError, ValueError) as exc:
            raise PolymarketLiveReadError(
                "polymarket_state_integrity_failed",
                "Polymarket state index manifest is invalid",
                409,
            ) from exc
        if (
            manifest.get("schema_version") != STATE_INDEX_SCHEMA_VERSION
            or not path.is_relative_to(self.state_index_root)
            or not path.is_file()
        ):
            raise PolymarketLiveReadError(
                "polymarket_state_integrity_failed",
                "Polymarket state index manifest binding is invalid",
                409,
            )
        return manifest, path

    def _validate_state_connection(
        self, path: Path, connection: sqlite3.Connection,
    ) -> dict[str, str]:
        try:
            metadata = LiveStateIndex._metadata(connection)
            last = connection.execute(
                """SELECT cursor, byte_offset, byte_length
                   FROM event_offsets ORDER BY cursor DESC LIMIT 1"""
            ).fetchone()
        except sqlite3.DatabaseError as exc:
            raise PolymarketLiveReadError(
                "polymarket_state_integrity_failed",
                "Polymarket state index is not readable",
                409,
            ) from exc
        if metadata.get("schema_version") != STATE_INDEX_SCHEMA_VERSION:
            raise PolymarketLiveReadError(
                "polymarket_state_integrity_failed", "State index schema mismatch", 409,
            )
        _, _, _, catalog_metadata = self._manifest_binding()
        if metadata.get("catalog_revision") != catalog_metadata.get("catalog_revision"):
            raise PolymarketLiveReadError(
                "polymarket_state_integrity_failed",
                "State and catalog revisions disagree",
                409,
            )
        event_log_size = int(metadata.get("event_log_size", "-1"))
        actual_size = self.event_path.stat().st_size if self.event_path.exists() else 0
        if event_log_size > actual_size:
            raise PolymarketLiveReadError(
                "polymarket_state_integrity_failed",
                "State index is ahead of the durable event log",
                409,
            )
        # A writer durably appends its batch before committing the derived WAL
        # transaction.  Bytes beyond this metadata boundary are an unpublished
        # tail; readers remain on the last complete indexed prefix.
        latest_cursor = int(metadata.get("latest_cursor", "0"))
        if (latest_cursor == 0) != (last is None):
            raise PolymarketLiveReadError(
                "polymarket_state_integrity_failed",
                "State cursor and event offset cardinality disagree",
                409,
            )
        if last is not None and (
            int(last["cursor"]) != latest_cursor
            or int(last["byte_offset"]) + int(last["byte_length"]) != event_log_size
        ):
            raise PolymarketLiveReadError(
                "polymarket_state_integrity_failed",
                "State cursor and durable event boundary disagree",
                409,
            )
        return metadata

    @contextmanager
    def _state_snapshot(self):
        revision_error = None
        for attempt in range(3):
            path = self._state_path()
            connection = _readonly_state_sqlite(path)
            try:
                connection.execute("BEGIN")
                try:
                    metadata = self._validate_state_connection(path, connection)
                except PolymarketLiveReadError as exc:
                    if (
                        exc.code == "polymarket_state_integrity_failed"
                        and "revisions disagree" in str(exc)
                        and attempt < 2
                    ):
                        revision_error = exc
                        connection.close()
                        time.sleep(0.01)
                        continue
                    raise
                try:
                    yield path, connection, metadata
                finally:
                    connection.close()
                return
            except BaseException:
                connection.close()
                raise
        if revision_error is not None:  # pragma: no cover - loop always raises
            raise revision_error

    def _state_binding(self) -> tuple[Path, dict[str, str]]:
        with self._state_snapshot() as (path, _, metadata):
            return path, metadata

    @staticmethod
    def _indexed_book(row: sqlite3.Row) -> LiveBook:
        body = bytes(row["payload_json"])
        if hashlib.sha256(body).hexdigest() != row["payload_sha256"]:
            raise PolymarketLiveReadError(
                "polymarket_state_integrity_failed", "Book payload hash mismatch", 409,
            )
        book = LiveBook.model_validate_json(body)
        if "confirmed_received_at" not in row.keys() or row["confirmed_received_at"] is None:
            return book
        confirmation = {
            "token_id": book.token_id,
            "exchange_at": str(row["confirmed_exchange_at"]),
            "received_at": str(row["confirmed_received_at"]),
            "state_checksum": str(row["confirmed_state_checksum"]),
            "source_hash": row["confirmed_source_hash"],
        }
        if (
            confirmation["state_checksum"] != book.state_checksum
            or content_sha256(confirmation) != row["confirmation_sha256"]
        ):
            raise PolymarketLiveReadError(
                "polymarket_state_integrity_failed",
                "Book freshness confirmation integrity failed",
                409,
            )
        return book.model_copy(update={
            "exchange_at": _instant(confirmation["exchange_at"]),
            "received_at": _instant(confirmation["received_at"]),
            "source_hash": confirmation["source_hash"],
        })

    def _bind_bootstrap_to_live_books(
        self, response: LiveBootstrapResponse,
    ) -> LiveBootstrapResponse:
        """Project dynamic CLOB tick facts into one scoped catalog boundary.

        Gamma's minimum tick is an initial market fact; Polymarket can tighten
        it while a market is live. The latest-state index is authoritative for
        that dynamic instrument fact, while every other catalog field remains
        bound to the immutable normalized row.
        """
        market_ids = [market.identity.market_id for market in response.markets]
        if not market_ids:
            return response
        placeholders = ",".join("?" for _ in market_ids)
        try:
            with self._state_snapshot() as (_, connection, metadata):
                rows = list(connection.execute(
                    f"""SELECT b.market_id, b.payload_json, b.payload_sha256,
                        c.exchange_at AS confirmed_exchange_at,
                        c.received_at AS confirmed_received_at,
                        c.state_checksum AS confirmed_state_checksum,
                        c.source_hash AS confirmed_source_hash,
                        c.confirmation_sha256
                        FROM books b LEFT JOIN book_confirmations c
                        ON c.token_id=b.token_id
                        WHERE b.market_id IN ({placeholders})""",
                    market_ids,
                ))
        except PolymarketLiveReadError as exc:
            if exc.status_code == 503:
                return response
            raise
        books_by_market: dict[str, list[LiveBook]] = defaultdict(list)
        for row in rows:
            books_by_market[str(row["market_id"])].append(
                self._indexed_book(row)
            )
        result = response.model_copy(deep=True)
        result.cursor = int(metadata["latest_cursor"])
        for market in result.markets:
            books = books_by_market.get(market.identity.market_id, [])
            expected_tokens = {outcome.token_id for outcome in market.identity.outcomes}
            if {book.token_id for book in books} != expected_tokens:
                continue
            if any(book.condition_id != market.identity.condition_id for book in books):
                raise PolymarketLiveReadError(
                    "polymarket_state_integrity_failed",
                    "Live book condition differs from scoped catalog facts",
                    409,
                )
            ticks = {book.tick_size for book in books}
            if len(ticks) != 1:
                raise PolymarketLiveReadError(
                    "polymarket_instrument_book_binding_incomplete",
                    "Outcome books disagree on the live market tick",
                    503,
                )
            tick = next(iter(ticks))
            instrument = market.rules.instrument
            if instrument.price_increment == tick:
                continue
            base_revision = instrument.revision
            instrument.price_increment = tick
            instrument.revision = content_sha256({
                "base_revision": base_revision,
                "live_price_increment": tick,
                "binding": "polymarket_clob_book_tick_v1",
            })
            market.metadata_revision = content_sha256({
                "base_revision": market.metadata_revision,
                "instrument_revision": instrument.revision,
            })
        return result

    def snapshot(self, market_ids: Iterable[str]) -> LiveSnapshotPage:
        selected_ids = self._scope(market_ids)
        bootstrap = self.bootstrap(selected_ids)
        relation_ids = sorted({
            pair.market_id
            for market in bootstrap.markets
            for relation in market.relations
            for pair in relation.outcome_pairs
            if pair.market_id not in selected_ids
        })
        related = self.bootstrap(relation_ids).markets if relation_ids else []
        markets = bootstrap.markets + related
        all_market_ids = [market.identity.market_id for market in markets]
        placeholders = ",".join("?" for _ in all_market_ids)
        with self._state_snapshot() as (_, connection, metadata):
            if metadata["catalog_revision"] != bootstrap.catalog_revision:
                raise PolymarketLiveReadError(
                    "polymarket_state_integrity_failed",
                    "Scoped catalog and state snapshot revisions disagree",
                    409,
                )
            book_rows = list(connection.execute(
                f"""SELECT b.payload_json, b.payload_sha256,
                    c.exchange_at AS confirmed_exchange_at,
                    c.received_at AS confirmed_received_at,
                    c.state_checksum AS confirmed_state_checksum,
                    c.source_hash AS confirmed_source_hash,
                    c.confirmation_sha256
                    FROM books b LEFT JOIN book_confirmations c
                    ON c.token_id=b.token_id
                    WHERE b.market_id IN ({placeholders})""",
                all_market_ids,
            ))
            gap_rows = list(connection.execute(
                f"""SELECT payload_json, payload_sha256 FROM gaps
                    WHERE market_id IN ({placeholders}) AND resolved=0""",
                all_market_ids,
            ))
        books = {}
        for row in book_rows:
            book = self._indexed_book(row)
            books[book.token_id] = book
        gaps = []
        for row in gap_rows:
            body = bytes(row["payload_json"])
            if hashlib.sha256(body).hexdigest() != row["payload_sha256"]:
                raise PolymarketLiveReadError(
                    "polymarket_state_integrity_failed", "Gap payload hash mismatch", 409,
                )
            gaps.append(GapEntry.model_validate_json(body))
        transient = LiveStateStore(self.root / ".bounded-read")
        transient.now_provider = self.now_provider
        transient._recovered = True
        transient.catalog = {market.identity.market_id: market for market in markets}
        transient.token_to_market = {
            outcome.token_id: market.identity.market_id
            for market in markets for outcome in market.identity.outcomes
        }
        transient.catalog_revision = metadata["catalog_revision"]
        transient.cursor = int(metadata["latest_cursor"])
        transient.active_recovery_id = metadata.get("active_recovery_id") or None
        transient.books = books
        transient.gaps = gaps
        return LiveSnapshotPage(
            catalog_revision=transient.catalog_revision,
            cursor=transient.cursor,
            count=len(selected_ids),
            items=[transient.frame(market_id) for market_id in selected_ids],
        )

    def events_after(
        self, market_ids: Iterable[str], after_cursor: int, limit: int,
    ) -> LiveEventPage:
        selected_ids = self._scope(market_ids)
        bootstrap = self.bootstrap(selected_ids)
        placeholders = ",".join("?" for _ in selected_ids)
        with self._state_snapshot() as (_, connection, metadata):
            if metadata["catalog_revision"] != bootstrap.catalog_revision:
                raise PolymarketLiveReadError(
                    "polymarket_state_integrity_failed",
                    "Scoped catalog and state snapshot revisions disagree",
                    409,
                )
            if after_cursor > 0:
                transition_rows = list(connection.execute(
                    """SELECT cursor, byte_offset, byte_length, line_sha256
                       FROM event_offsets
                       WHERE market_id IS NULL AND cursor > ?
                       ORDER BY cursor""",
                    (after_cursor,),
                ))
                with self.event_path.open("rb") as stream:
                    for row in transition_rows:
                        stream.seek(int(row["byte_offset"]))
                        line = stream.read(int(row["byte_length"]))
                        if hashlib.sha256(line).hexdigest() != row["line_sha256"]:
                            raise PolymarketLiveReadError(
                                "polymarket_state_integrity_failed",
                                "Catalog transition event hash mismatch",
                                409,
                            )
                        event = LiveEventEnvelope.model_validate_json(line)
                        if event.event_type == "catalog_revision":
                            raise PolymarketLiveReadError(
                                "resume_cursor_expired",
                                (
                                    "Resume cursor predates the current catalog; "
                                    "perform a fresh scoped snapshot"
                                ),
                                409,
                            )
            rows = list(connection.execute(
                f"""SELECT * FROM event_offsets
                    WHERE market_id IN ({placeholders}) AND cursor > ?
                    ORDER BY cursor LIMIT ?""",
                [*selected_ids, after_cursor, limit + 1],
            ))
        has_more = len(rows) > limit
        rows = rows[:limit]
        items = []
        with self.event_path.open("rb") as stream:
            for row in rows:
                stream.seek(int(row["byte_offset"]))
                line = stream.read(int(row["byte_length"]))
                if hashlib.sha256(line).hexdigest() != row["line_sha256"]:
                    raise PolymarketLiveReadError(
                        "polymarket_state_integrity_failed", "Event row hash mismatch", 409,
                    )
                event = LiveEventEnvelope.model_validate_json(line)
                if (
                    event.cursor != int(row["cursor"])
                    or event.event_id != row["event_id"]
                    or event.market_id not in selected_ids
                    or content_sha256(event.canonical_payload)
                    != event.canonical_payload_sha256
                    or content_sha256(event.raw_payload) != event.raw_payload_sha256
                    or live_event_identity(event) != event.event_id
                ):
                    raise PolymarketLiveReadError(
                        "polymarket_state_integrity_failed", "Event index identity mismatch", 409,
                    )
                items.append(event)
        next_cursor = items[-1].cursor if items else after_cursor
        return LiveEventPage(
            after_cursor=after_cursor, next_cursor=next_cursor,
            has_more=has_more, items=items,
        )

    def gaps(
        self, market_ids: Iterable[str], *, unresolved_only: bool,
    ) -> LiveGapPage:
        selected_ids = self._scope(market_ids)
        bootstrap = self.bootstrap(selected_ids)
        placeholders = ",".join("?" for _ in selected_ids)
        resolved_clause = " AND resolved=0" if unresolved_only else ""
        with self._state_snapshot() as (_, connection, metadata):
            if metadata["catalog_revision"] != bootstrap.catalog_revision:
                raise PolymarketLiveReadError(
                    "polymarket_state_integrity_failed",
                    "Scoped catalog and state snapshot revisions disagree",
                    409,
                )
            rows = list(connection.execute(
                f"""SELECT payload_json, payload_sha256 FROM gaps
                    WHERE market_id IN ({placeholders}){resolved_clause}
                    ORDER BY cursor, gap_id""",
                selected_ids,
            ))
        items = []
        for row in rows:
            body = bytes(row["payload_json"])
            if hashlib.sha256(body).hexdigest() != row["payload_sha256"]:
                raise PolymarketLiveReadError(
                    "polymarket_state_integrity_failed", "Gap payload hash mismatch", 409,
                )
            items.append(GapEntry.model_validate_json(body))
        return LiveGapPage(count=len(items), items=items)

    def checkpoint(self, market_ids: Iterable[str]) -> LiveCheckpoint:
        selected_ids = self._scope(market_ids)
        bootstrap = self.bootstrap(selected_ids)
        placeholders = ",".join("?" for _ in selected_ids)
        with self._state_snapshot() as (_, connection, metadata):
            if metadata["catalog_revision"] != bootstrap.catalog_revision:
                raise PolymarketLiveReadError(
                    "polymarket_state_integrity_failed",
                    "Scoped catalog and state snapshot revisions disagree",
                    409,
                )
            book_rows = list(connection.execute(
                f"""SELECT b.token_id, b.payload_json, b.payload_sha256,
                    c.exchange_at AS confirmed_exchange_at,
                    c.received_at AS confirmed_received_at,
                    c.state_checksum AS confirmed_state_checksum,
                    c.source_hash AS confirmed_source_hash,
                    c.confirmation_sha256
                    FROM books b LEFT JOIN book_confirmations c
                    ON c.token_id=b.token_id
                    WHERE b.market_id IN ({placeholders}) ORDER BY b.token_id""",
                selected_ids,
            ))
            gap_rows = list(connection.execute(
                f"""SELECT payload_json, payload_sha256 FROM gaps
                    WHERE market_id IN ({placeholders}) AND resolved=0
                    ORDER BY cursor, gap_id""",
                selected_ids,
            ))
        books = {}
        for row in book_rows:
            books[str(row["token_id"])] = self._indexed_book(row)
        gaps = []
        for row in gap_rows:
            body = bytes(row["payload_json"])
            if hashlib.sha256(body).hexdigest() != row["payload_sha256"]:
                raise PolymarketLiveReadError(
                    "polymarket_state_integrity_failed", "Gap payload hash mismatch", 409,
                )
            gaps.append(GapEntry.model_validate_json(body))
        state = {
            "cursor": int(metadata["latest_cursor"]),
            "catalog_revision": metadata["catalog_revision"],
            "books": {
                key: value.model_dump(mode="json") for key, value in sorted(books.items())
            },
            "unresolved_gaps": [gap.model_dump(mode="json") for gap in gaps],
        }
        return LiveCheckpoint(
            **state, created_at=self.now_provider(), state_sha256=content_sha256(state)
        )

    def health(self) -> LiveReadHealth:
        if not self.catalog_path.exists():
            return LiveReadHealth(status="not_configured")
        try:
            _, _, _, metadata = self._manifest_binding()
        except PolymarketLiveReadError as exc:
            status = (
                "legacy_unindexed"
                if exc.code == "polymarket_catalog_index_unavailable"
                else "integrity_failed"
            )
            return LiveReadHealth(status=status, reason_codes=[exc.code])
        try:
            state_manifest, _ = self._state_manifest()
        except PolymarketLiveReadError as exc:
            return LiveReadHealth(
                status="degraded",
                catalog_revision=metadata["catalog_revision"],
                catalog_index_ready=True,
                latest_state_ready=False,
                market_count=int(metadata["market_count"]),
                token_count=int(metadata["token_count"]),
                reason_codes=[exc.code],
            )
        summary_keys = {
            "catalog_revision", "latest_cursor", "active_recovery_id",
            "book_token_count", "book_complete_market_count",
            "unresolved_gap_count",
        }
        if summary_keys.issubset(state_manifest):
            state_metadata = state_manifest
            if state_metadata["catalog_revision"] != metadata["catalog_revision"]:
                return LiveReadHealth(
                    status="degraded",
                    catalog_revision=metadata["catalog_revision"],
                    catalog_index_ready=True,
                    latest_state_ready=False,
                    market_count=int(metadata["market_count"]),
                    token_count=int(metadata["token_count"]),
                    reason_codes=["polymarket_state_integrity_failed"],
                )
            book_token_count = int(state_metadata["book_token_count"])
            book_complete_market_count = int(
                state_metadata["book_complete_market_count"]
            )
            unresolved_gap_count = int(state_metadata["unresolved_gap_count"])
        else:
            # Compatibility for an index published by an older writer. The next
            # committed batch upgrades the manifest and removes these scans from
            # the request path.
            state_path, state_metadata = self._state_binding()
            with _readonly_state_sqlite(state_path) as connection:
                book_token_count = int(
                    connection.execute("SELECT COUNT(*) FROM books").fetchone()[0]
                )
                book_complete_market_count = int(connection.execute(
                    "SELECT COUNT(*) FROM ("
                    "SELECT market_id FROM books GROUP BY market_id HAVING COUNT(*) = 2"
                    ")"
                ).fetchone()[0])
                unresolved_gap_count = int(connection.execute(
                    "SELECT COUNT(*) FROM gaps WHERE resolved=0"
                ).fetchone()[0])
        recovery_active = bool(state_metadata.get("active_recovery_id"))
        return LiveReadHealth(
            status="degraded" if recovery_active else "index_ready",
            catalog_revision=metadata["catalog_revision"],
            catalog_index_ready=True,
            latest_state_ready=True,
            market_count=int(metadata["market_count"]),
            token_count=int(metadata["token_count"]),
            book_token_count=book_token_count,
            book_complete_market_count=book_complete_market_count,
            unresolved_gap_count=unresolved_gap_count,
            latest_cursor=int(state_metadata["latest_cursor"]),
            reason_codes=["recovery_in_progress"] if recovery_active else [],
        )


def _gap_identity(gap: GapEntry) -> str:
    payload = gap.model_dump(mode="json")
    payload.pop("resolved", None)
    payload.pop("resolution", None)
    return content_sha256(payload)


class LiveStateIndex:
    """Mutable derived state; the append-only event log remains authoritative."""

    rebuild_batch_size = 1_000

    def __init__(self, root: Path):
        self.root = root.resolve()
        self.path = self.root / "indexes" / "latest-state.sqlite3"
        self.manifest_path = self.root / "state-index.json"
        self._batch_state = threading.local()

    def _writer_connection(self) -> sqlite3.Connection:
        initialize = not self.path.exists()
        connection = sqlite3.connect(self.path, timeout=30)
        if initialize:
            self._schema(connection)
        else:
            # WAL mode is persistent. Re-running DDL and journal-mode PRAGMAs
            # for every live batch takes a schema lock and stalls readers.
            connection.execute("PRAGMA synchronous=FULL")
            connection.execute("PRAGMA busy_timeout=30000")
        return connection

    @contextmanager
    def batch(self):
        """Commit a writer-visible event batch at one SQLite boundary."""
        if getattr(self._batch_state, "connection", None) is not None:
            yield
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        connection = self._writer_connection()
        self._batch_state.connection = connection
        published_metadata = None
        try:
            connection.execute("BEGIN IMMEDIATE")
            yield
            connection.commit()
            published_metadata = self._metadata(connection)
        except BaseException:
            connection.rollback()
            raise
        finally:
            del self._batch_state.connection
            connection.close()
        self._publish_manifest(published_metadata)

    @staticmethod
    def _schema(connection: sqlite3.Connection, *, wal: bool = True) -> None:
        connection.execute(f"PRAGMA journal_mode={'WAL' if wal else 'DELETE'}")
        connection.executescript("""
            PRAGMA synchronous=FULL;
            CREATE TABLE IF NOT EXISTS metadata (
                key TEXT PRIMARY KEY, value TEXT NOT NULL
            ) WITHOUT ROWID;
            CREATE TABLE IF NOT EXISTS books (
                token_id TEXT PRIMARY KEY, market_id TEXT NOT NULL,
                cursor INTEGER NOT NULL, payload_json BLOB NOT NULL,
                payload_sha256 TEXT NOT NULL
            ) WITHOUT ROWID;
            CREATE INDEX IF NOT EXISTS books_market_id ON books(market_id, token_id);
            CREATE TABLE IF NOT EXISTS book_confirmations (
                token_id TEXT PRIMARY KEY, exchange_at TEXT NOT NULL,
                received_at TEXT NOT NULL, state_checksum TEXT NOT NULL,
                source_hash TEXT, confirmation_sha256 TEXT NOT NULL
            ) WITHOUT ROWID;
            CREATE TABLE IF NOT EXISTS gaps (
                gap_id TEXT PRIMARY KEY, market_id TEXT,
                token_id TEXT, cursor INTEGER NOT NULL,
                payload_json BLOB NOT NULL, payload_sha256 TEXT NOT NULL,
                resolved INTEGER NOT NULL CHECK(resolved IN (0, 1))
            ) WITHOUT ROWID;
            CREATE INDEX IF NOT EXISTS gaps_market_id ON gaps(market_id, resolved);
            CREATE TABLE IF NOT EXISTS event_offsets (
                cursor INTEGER PRIMARY KEY, byte_offset INTEGER NOT NULL,
                byte_length INTEGER NOT NULL, event_id TEXT NOT NULL,
                market_id TEXT, token_id TEXT, line_sha256 TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS events_market_cursor
                ON event_offsets(market_id, cursor);
        """)

    def ensure_runtime_schema(self) -> None:
        """Apply small forward-compatible tables once, never in the hot batch path."""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with _publication_lock(self.root, exclusive=True):
            with sqlite3.connect(self.path, timeout=30) as connection:
                connection.execute("PRAGMA busy_timeout=30000")
                connection.execute("""CREATE TABLE IF NOT EXISTS book_confirmations (
                    token_id TEXT PRIMARY KEY, exchange_at TEXT NOT NULL,
                    received_at TEXT NOT NULL, state_checksum TEXT NOT NULL,
                    source_hash TEXT, confirmation_sha256 TEXT NOT NULL
                ) WITHOUT ROWID""")
                connection.commit()

    def confirm_book(self, book: LiveBook) -> None:
        """Publish freshness for unchanged content without another full book event."""
        connection = getattr(self._batch_state, "connection", None)
        owns_connection = connection is None
        if owns_connection:
            connection = self._writer_connection()
        confirmation = {
            "token_id": book.token_id,
            "exchange_at": book.exchange_at.isoformat(),
            "received_at": book.received_at.isoformat(),
            "state_checksum": book.state_checksum,
            "source_hash": book.source_hash,
        }
        try:
            connection.execute(
                """INSERT INTO book_confirmations(
                    token_id, exchange_at, received_at, state_checksum,
                    source_hash, confirmation_sha256
                ) VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(token_id) DO UPDATE SET
                    exchange_at=excluded.exchange_at,
                    received_at=excluded.received_at,
                    state_checksum=excluded.state_checksum,
                    source_hash=excluded.source_hash,
                    confirmation_sha256=excluded.confirmation_sha256""",
                (
                    book.token_id, confirmation["exchange_at"],
                    confirmation["received_at"], book.state_checksum,
                    book.source_hash, content_sha256(confirmation),
                ),
            )
            if owns_connection:
                connection.commit()
        except BaseException:
            if owns_connection:
                connection.rollback()
            raise
        finally:
            if owns_connection:
                connection.close()

    @staticmethod
    def _metadata(connection: sqlite3.Connection) -> dict[str, str]:
        return {
            str(row[0]): str(row[1])
            for row in connection.execute("SELECT key, value FROM metadata")
        }

    @staticmethod
    def _set_metadata(connection: sqlite3.Connection, values: dict[str, Any]) -> None:
        connection.executemany(
            """INSERT INTO metadata(key, value) VALUES (?, ?)
               ON CONFLICT(key) DO UPDATE SET value=excluded.value""",
            [(key, str(value)) for key, value in sorted(values.items())],
        )

    @staticmethod
    def _health_counts(connection: sqlite3.Connection) -> dict[str, int]:
        return {
            "book_token_count": int(
                connection.execute("SELECT COUNT(*) FROM books").fetchone()[0]
            ),
            "book_complete_market_count": int(connection.execute(
                "SELECT COUNT(*) FROM ("
                "SELECT market_id FROM books GROUP BY market_id HAVING COUNT(*) = 2"
                ")"
            ).fetchone()[0]),
            "unresolved_gap_count": int(connection.execute(
                "SELECT COUNT(*) FROM gaps WHERE resolved=0"
            ).fetchone()[0]),
        }

    def _publish_manifest(self, metadata: dict[str, str] | None = None) -> None:
        if metadata is None:
            with sqlite3.connect(self.path) as connection:
                metadata = self._metadata(connection)
        summary = {
            key: metadata[key]
            for key in (
                "catalog_revision", "latest_cursor", "active_recovery_id",
                "book_token_count", "book_complete_market_count",
                "unresolved_gap_count",
            )
            if key in metadata
        }
        _atomic_write(self.manifest_path, canonical_json({
            "schema_version": STATE_INDEX_SCHEMA_VERSION,
            "path": str(self.path),
            **summary,
        }))

    def append(
        self,
        event: LiveEventEnvelope,
        *,
        byte_offset: int,
        byte_length: int,
        line_sha256: str,
        book: LiveBook | None,
        gaps: list[GapEntry],
        catalog_revision: str | None,
        event_log_size: int,
        token_to_market: dict[str, str],
        active_recovery_id: str | None,
        book_token_count: int | None = None,
        book_complete_market_count: int | None = None,
        unresolved_gap_count: int | None = None,
    ) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        connection = getattr(self._batch_state, "connection", None)
        owns_connection = connection is None
        if owns_connection:
            connection = self._writer_connection()
        try:
            metadata = self._metadata(connection)
            previous_cursor = int(metadata.get("latest_cursor", "0"))
            if previous_cursor != event.cursor - 1:
                raise RuntimeError("live state index cursor is not contiguous")
            connection.execute(
                """INSERT INTO event_offsets(
                    cursor, byte_offset, byte_length, event_id,
                    market_id, token_id, line_sha256
                ) VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (
                    event.cursor, byte_offset, byte_length, event.event_id,
                    event.market_id, event.token_id, line_sha256,
                ),
            )
            if book is not None and event.market_id is not None:
                body = canonical_json(book.model_dump(mode="json"))
                connection.execute(
                    """INSERT INTO books(
                        token_id, market_id, cursor, payload_json, payload_sha256
                    ) VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT(token_id) DO UPDATE SET
                        market_id=excluded.market_id, cursor=excluded.cursor,
                        payload_json=excluded.payload_json,
                        payload_sha256=excluded.payload_sha256""",
                    (
                        book.token_id, event.market_id, event.cursor, body,
                        hashlib.sha256(body).hexdigest(),
                    ),
                )
                connection.execute(
                    "DELETE FROM book_confirmations WHERE token_id=?",
                    (book.token_id,),
                )
            for gap in gaps:
                body = canonical_json(gap.model_dump(mode="json"))
                market_id = token_to_market.get(gap.token_id or "")
                connection.execute(
                    """INSERT INTO gaps(
                        gap_id, market_id, token_id, cursor, payload_json,
                        payload_sha256, resolved
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(gap_id) DO UPDATE SET
                        cursor=excluded.cursor, payload_json=excluded.payload_json,
                        payload_sha256=excluded.payload_sha256,
                        resolved=excluded.resolved""",
                    (
                        _gap_identity(gap), market_id, gap.token_id, event.cursor,
                        body, hashlib.sha256(body).hexdigest(), int(gap.resolved),
                    ),
                )
            if event.event_type == "recovery_completed" and event.applied:
                recovered = list(
                    event.canonical_payload.get("resolved_gap_token_ids")
                    or event.canonical_payload.get("recovered_token_ids") or []
                )
                if recovered:
                    placeholders = ",".join("?" for _ in recovered)
                    connection.execute(
                        f"UPDATE gaps SET resolved=1 WHERE token_id IN ({placeholders})",
                        recovered,
                    )
            health_metadata = {
                key: value for key, value in {
                    "book_token_count": book_token_count,
                    "book_complete_market_count": book_complete_market_count,
                    "unresolved_gap_count": unresolved_gap_count,
                }.items() if value is not None
            }
            self._set_metadata(connection, {
                "schema_version": STATE_INDEX_SCHEMA_VERSION,
                "catalog_revision": catalog_revision or "",
                "latest_cursor": event.cursor,
                "event_log_size": event_log_size,
                "active_recovery_id": active_recovery_id or "",
                **health_metadata,
            })
            if owns_connection:
                connection.commit()
                published_metadata = self._metadata(connection)
        except BaseException:
            if owns_connection:
                connection.rollback()
            raise
        finally:
            if owns_connection:
                connection.close()
        if owns_connection:
            self._publish_manifest(published_metadata)

    def rebuild(
        self,
        *,
        event_path: Path,
        books: dict[str, LiveBook],
        gaps: list[GapEntry],
        catalog_revision: str | None,
        token_to_market: dict[str, str],
        active_recovery_id: str | None = None,
    ) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.path.with_name(f".{self.path.name}.{os.getpid()}.tmp")
        temporary.unlink(missing_ok=True)
        latest_cursor = 0
        event_log_size = 0
        try:
            with sqlite3.connect(temporary) as connection:
                self._schema(connection, wal=False)
                if event_path.exists():
                    with event_path.open("rb") as stream:
                        while True:
                            byte_offset = stream.tell()
                            line = stream.readline()
                            if not line:
                                event_log_size = stream.tell()
                                break
                            if not line.endswith(b"\n"):
                                raise RuntimeError("live event log ends with a partial row")
                            event = LiveEventEnvelope.model_validate_json(line)
                            if event.cursor != latest_cursor + 1:
                                raise RuntimeError("live event cursor is not contiguous")
                            if content_sha256(event.canonical_payload) != event.canonical_payload_sha256:
                                raise RuntimeError("live event canonical payload hash mismatch")
                            if content_sha256(event.raw_payload) != event.raw_payload_sha256:
                                raise RuntimeError("live event raw payload hash mismatch")
                            if live_event_identity(event) != event.event_id:
                                raise RuntimeError("live event identity mismatch")
                            connection.execute(
                                """INSERT INTO event_offsets(
                                    cursor, byte_offset, byte_length, event_id,
                                    market_id, token_id, line_sha256
                                ) VALUES (?, ?, ?, ?, ?, ?, ?)""",
                                (
                                    event.cursor, byte_offset, len(line), event.event_id,
                                    event.market_id, event.token_id,
                                    hashlib.sha256(line).hexdigest(),
                                ),
                            )
                            latest_cursor = event.cursor
                            event_log_size = stream.tell()
                for token_id, book in sorted(books.items()):
                    market_id = token_to_market.get(token_id)
                    if market_id is None:
                        continue
                    body = canonical_json(book.model_dump(mode="json"))
                    connection.execute(
                        """INSERT INTO books(
                            token_id, market_id, cursor, payload_json, payload_sha256
                        ) VALUES (?, ?, ?, ?, ?)""",
                        (
                            token_id, market_id, latest_cursor,
                            body, hashlib.sha256(body).hexdigest(),
                        ),
                    )
                for gap in gaps:
                    body = canonical_json(gap.model_dump(mode="json"))
                    connection.execute(
                        """INSERT INTO gaps(
                            gap_id, market_id, token_id, cursor, payload_json,
                            payload_sha256, resolved
                        ) VALUES (?, ?, ?, ?, ?, ?, ?)""",
                        (
                            _gap_identity(gap), token_to_market.get(gap.token_id or ""),
                            gap.token_id, latest_cursor, body,
                            hashlib.sha256(body).hexdigest(), int(gap.resolved),
                        ),
                    )
                self._set_metadata(connection, {
                    "schema_version": STATE_INDEX_SCHEMA_VERSION,
                    "catalog_revision": catalog_revision or "",
                    "latest_cursor": latest_cursor,
                    "event_log_size": event_log_size,
                    "active_recovery_id": active_recovery_id or "",
                    "book_token_count": len(books),
                    "book_complete_market_count": len({
                        market_id for market_id in token_to_market.values()
                        if sum(
                            indexed_market_id == market_id
                            for indexed_market_id in (
                                token_to_market.get(token_id)
                                for token_id in books
                            )
                        ) == 2
                    }),
                    "unresolved_gap_count": sum(not gap.resolved for gap in gaps),
                })
                connection.commit()
            os.replace(temporary, self.path)
        finally:
            temporary.unlink(missing_ok=True)
        self._publish_manifest()

    @staticmethod
    def _commit_rebuild_batch(
        connection: sqlite3.Connection, values: dict[str, Any],
    ) -> None:
        """Persist one restart-safe rebuild boundary."""
        LiveStateIndex._set_metadata(connection, values)
        connection.commit()

    @staticmethod
    def _checkpoint_matches_index(
        connection: sqlite3.Connection,
        checkpoint: LiveCheckpoint,
        token_to_market: dict[str, str] | None = None,
    ) -> bool:
        expected_books = sorted(
            (token_id, canonical_json(book.model_dump(mode="json")))
            for token_id, book in checkpoint.books.items()
        )
        observed_books = [
            (str(row[0]), bytes(row[1]))
            for row in connection.execute(
                "SELECT token_id, payload_json FROM books ORDER BY token_id"
            )
        ]
        expected_gaps: dict[str, bytes] = {}
        for gap in checkpoint.unresolved_gaps:
            gap_id = _gap_identity(gap)
            body = canonical_json(gap.model_dump(mode="json"))
            if gap_id in expected_gaps and expected_gaps[gap_id] != body:
                return False
            expected_gaps[gap_id] = body
        observed_gaps = {
            str(row[0]): bytes(row[1])
            for row in connection.execute(
                "SELECT gap_id, payload_json FROM gaps WHERE resolved=0"
            )
        }
        # The mutable writer may retain repeated, byte-identical gap entries;
        # the derived index deliberately collapses them by semantic gap identity.
        if (
            expected_books != observed_books
            or any(expected_gaps.get(key) != body for key, body in observed_gaps.items())
        ):
            return False
        if token_to_market is None:
            return expected_gaps == observed_gaps
        for gap in checkpoint.unresolved_gaps:
            gap_id = _gap_identity(gap)
            if gap_id in observed_gaps:
                continue
            body = expected_gaps[gap_id]
            connection.execute(
                """INSERT INTO gaps(
                    gap_id, market_id, token_id, cursor, payload_json,
                    payload_sha256, resolved
                ) VALUES (?, ?, ?, ?, ?, ?, 0)
                ON CONFLICT(gap_id) DO UPDATE SET
                    market_id=excluded.market_id,
                    token_id=excluded.token_id,
                    cursor=excluded.cursor,
                    payload_json=excluded.payload_json,
                    payload_sha256=excluded.payload_sha256,
                    resolved=0""",
                (
                    gap_id, token_to_market.get(gap.token_id or ""), gap.token_id,
                    checkpoint.cursor, body, hashlib.sha256(body).hexdigest(),
                ),
            )
            observed_gaps[gap_id] = body
        return expected_gaps == observed_gaps

    @staticmethod
    def _resume_rebuild(
        connection: sqlite3.Connection,
        *,
        event_path: Path,
        catalog_revision: str,
        checkpoint_sha256: str,
    ) -> tuple[int, int, str | None, bool] | None:
        metadata = LiveStateIndex._metadata(connection)
        if any(
            metadata.get(key) != expected
            for key, expected in {
                "schema_version": STATE_INDEX_SCHEMA_VERSION,
                "build_status": "in_progress",
                "catalog_revision": catalog_revision,
                "checkpoint_sha256": checkpoint_sha256,
            }.items()
        ):
            return None
        try:
            latest_cursor = int(metadata["latest_cursor"])
            event_log_size = int(metadata["event_log_size"])
            count, minimum, maximum = connection.execute(
                "SELECT COUNT(*), MIN(cursor), MAX(cursor) FROM event_offsets"
            ).fetchone()
        except (KeyError, TypeError, ValueError, sqlite3.DatabaseError):
            return None
        if (
            event_log_size < 0
            or (latest_cursor == 0 and count != 0)
            or (
                latest_cursor > 0
                and (count != latest_cursor or minimum != 1 or maximum != latest_cursor)
            )
        ):
            return None
        if latest_cursor:
            row = connection.execute(
                """SELECT byte_offset, byte_length, line_sha256
                   FROM event_offsets WHERE cursor=?""",
                (latest_cursor,),
            ).fetchone()
            if row is None or int(row[0]) + int(row[1]) != event_log_size:
                return None
            try:
                with event_path.open("rb") as stream:
                    stream.seek(int(row[0]))
                    line = stream.read(int(row[1]))
            except OSError:
                return None
            if (
                len(line) != int(row[1])
                or hashlib.sha256(line).hexdigest() != row[2]
            ):
                return None
        elif event_log_size != 0:
            return None
        return (
            latest_cursor,
            event_log_size,
            metadata.get("active_recovery_id") or None,
            metadata.get("checkpoint_verified") == "1",
        )

    def rebuild_from_durable(
        self,
        *,
        event_path: Path,
        checkpoint: LiveCheckpoint | None,
        checkpoint_sha256: str,
        catalog_revision: str,
        token_to_market: dict[str, str],
        batch_size: int | None = None,
    ) -> None:
        """Single-pass, restart-safe rebuild from verified durable evidence."""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.path.with_name(f".{self.path.name}.rebuild")
        batch_size = max(1, batch_size or self.rebuild_batch_size)

        resume = None
        if temporary.exists():
            try:
                with sqlite3.connect(temporary) as connection:
                    resume = self._resume_rebuild(
                        connection,
                        event_path=event_path,
                        catalog_revision=catalog_revision,
                        checkpoint_sha256=checkpoint_sha256,
                    )
            except sqlite3.DatabaseError:
                resume = None
            if resume is None:
                temporary.unlink()

        with sqlite3.connect(temporary) as connection:
            self._schema(connection, wal=False)
            if resume is None:
                latest_cursor = 0
                event_log_size = 0
                active_recovery_id = None
                checkpoint_verified = checkpoint is None
                if checkpoint is not None and checkpoint.cursor == 0:
                    checkpoint_verified = self._checkpoint_matches_index(
                        connection, checkpoint, token_to_market,
                    )
                    if not checkpoint_verified:
                        raise RuntimeError(
                            "live checkpoint state disagrees with event replay"
                        )
                self._commit_rebuild_batch(connection, {
                    "schema_version": STATE_INDEX_SCHEMA_VERSION,
                    "build_status": "in_progress",
                    "catalog_revision": catalog_revision,
                    "checkpoint_sha256": checkpoint_sha256,
                    "checkpoint_verified": int(checkpoint_verified),
                    "latest_cursor": 0,
                    "event_log_size": 0,
                    "active_recovery_id": "",
                })
            else:
                (
                    latest_cursor, event_log_size, active_recovery_id,
                    checkpoint_verified,
                ) = resume

            processed = 0
            if event_path.exists():
                with event_path.open("rb") as stream:
                    stream.seek(event_log_size)
                    while True:
                        byte_offset = stream.tell()
                        line = stream.readline()
                        if not line:
                            event_log_size = stream.tell()
                            break
                        if not line.endswith(b"\n"):
                            raise RuntimeError(
                                "live event log ends with a partial row"
                            )
                        try:
                            event = LiveEventEnvelope.model_validate_json(line)
                        except ValueError as exc:
                            raise RuntimeError(
                                "live event log contains invalid JSON"
                            ) from exc
                        LiveStateStore._validate_event(event, latest_cursor + 1)
                        connection.execute(
                            """INSERT INTO event_offsets(
                                cursor, byte_offset, byte_length, event_id,
                                market_id, token_id, line_sha256
                            ) VALUES (?, ?, ?, ?, ?, ?, ?)""",
                            (
                                event.cursor, byte_offset, len(line), event.event_id,
                                event.market_id, event.token_id,
                                hashlib.sha256(line).hexdigest(),
                            ),
                        )
                        if (
                            event.applied and event.token_id
                            and event.event_type in {
                                "book", "price_change", "best_bid_ask",
                                "last_trade_price", "tick_size_change",
                            }
                        ):
                            book = LiveBook.model_validate(event.canonical_payload)
                            body = canonical_json(book.model_dump(mode="json"))
                            connection.execute(
                                """INSERT INTO books(
                                    token_id, market_id, cursor,
                                    payload_json, payload_sha256
                                ) VALUES (?, ?, ?, ?, ?)
                                ON CONFLICT(token_id) DO UPDATE SET
                                    market_id=excluded.market_id,
                                    cursor=excluded.cursor,
                                    payload_json=excluded.payload_json,
                                    payload_sha256=excluded.payload_sha256""",
                                (
                                    event.token_id,
                                    event.market_id
                                    or token_to_market.get(event.token_id) or "",
                                    event.cursor, body,
                                    hashlib.sha256(body).hexdigest(),
                                ),
                            )
                        for gap in event.gaps:
                            body = canonical_json(gap.model_dump(mode="json"))
                            connection.execute(
                                """INSERT INTO gaps(
                                    gap_id, market_id, token_id, cursor,
                                    payload_json, payload_sha256, resolved
                                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                                ON CONFLICT(gap_id) DO UPDATE SET
                                    market_id=excluded.market_id,
                                    cursor=excluded.cursor,
                                    payload_json=excluded.payload_json,
                                    payload_sha256=excluded.payload_sha256,
                                    resolved=excluded.resolved""",
                                (
                                    _gap_identity(gap),
                                    token_to_market.get(gap.token_id or ""),
                                    gap.token_id, event.cursor, body,
                                    hashlib.sha256(body).hexdigest(),
                                    int(gap.resolved),
                                ),
                            )
                        if event.event_type == "recovery_completed" and event.applied:
                            recovered = list(
                                event.canonical_payload.get(
                                    "resolved_gap_token_ids"
                                )
                                or event.canonical_payload.get(
                                    "recovered_token_ids"
                                )
                                or []
                            )
                            if recovered:
                                placeholders = ",".join("?" for _ in recovered)
                                recovery_id = str(
                                    event.canonical_payload.get("recovery_id") or ""
                                )
                                rows = connection.execute(
                                    f"""SELECT gap_id, payload_json FROM gaps
                                        WHERE token_id IN ({placeholders})
                                        AND resolved=0""",
                                    recovered,
                                ).fetchall()
                                for gap_id, payload_json in rows:
                                    gap = GapEntry.model_validate_json(payload_json)
                                    gap.resolved = True
                                    gap.resolution = (
                                        f"rest_books_snapshot:{recovery_id}"
                                    )
                                    body = canonical_json(
                                        gap.model_dump(mode="json")
                                    )
                                    connection.execute(
                                        """UPDATE gaps SET
                                            payload_json=?, payload_sha256=?,
                                            resolved=1 WHERE gap_id=?""",
                                        (
                                            body,
                                            hashlib.sha256(body).hexdigest(),
                                            gap_id,
                                        ),
                                    )
                            active_recovery_id = None
                        elif event.event_type == "recovery_started" and event.applied:
                            active_recovery_id = str(
                                event.canonical_payload.get("recovery_id") or ""
                            ) or None

                        latest_cursor = event.cursor
                        event_log_size = stream.tell()
                        processed += 1
                        if checkpoint is not None and event.cursor == checkpoint.cursor:
                            checkpoint_verified = self._checkpoint_matches_index(
                                connection, checkpoint, token_to_market,
                            )
                            if not checkpoint_verified:
                                raise RuntimeError(
                                    "live checkpoint state disagrees with event replay"
                                )
                        if processed % batch_size == 0:
                            self._commit_rebuild_batch(connection, {
                                "latest_cursor": latest_cursor,
                                "event_log_size": event_log_size,
                                "active_recovery_id": active_recovery_id or "",
                                "checkpoint_verified": int(checkpoint_verified),
                            })

            if checkpoint is not None and not checkpoint_verified:
                raise RuntimeError(
                    "live checkpoint cursor exceeds verified event log"
                )

            for token_id, market_id in connection.execute(
                "SELECT token_id, market_id FROM books"
            ).fetchall():
                expected_market_id = token_to_market.get(str(token_id))
                if expected_market_id is None:
                    connection.execute(
                        "DELETE FROM books WHERE token_id=?", (token_id,)
                    )
                elif market_id != expected_market_id:
                    connection.execute(
                        "UPDATE books SET market_id=? WHERE token_id=?",
                        (expected_market_id, token_id),
                    )
            self._commit_rebuild_batch(connection, {
                "schema_version": STATE_INDEX_SCHEMA_VERSION,
                "build_status": "complete",
                "catalog_revision": catalog_revision,
                "checkpoint_sha256": checkpoint_sha256,
                "checkpoint_verified": int(checkpoint_verified),
                "latest_cursor": latest_cursor,
                "event_log_size": event_log_size,
                "active_recovery_id": active_recovery_id or "",
                **self._health_counts(connection),
            })

        with temporary.open("rb") as stream:
            os.fsync(stream.fileno())
        # This is an offline operation. Stale sidecars belong to the database
        # being replaced and must never be replayed over the completed rebuild.
        for suffix in ("-wal", "-shm"):
            self.path.with_name(f"{self.path.name}{suffix}").unlink(missing_ok=True)
        os.replace(temporary, self.path)
        self._publish_manifest()


class GammaCatalogRows:
    """One complete disk-backed Gamma snapshot; iteration keeps memory page-bounded."""

    def __init__(
        self,
        path: Path,
        *,
        row_count: int,
        sha256: str,
        retry_manifest_path: Path | None = None,
    ):
        self.path = path.resolve()
        self.row_count = row_count
        self.sha256 = sha256
        self.retry_manifest_path = retry_manifest_path

    def __len__(self) -> int:
        return self.row_count

    def __iter__(self):
        with self.path.open("r", encoding="utf-8") as stream:
            for line in stream:
                if line.strip():
                    yield json.loads(line, parse_float=str, parse_int=str)

    def cleanup(self) -> None:
        if self.retry_manifest_path is None:
            self.path.unlink(missing_ok=True)

    def mark_published(self) -> None:
        if self.retry_manifest_path is not None:
            self.retry_manifest_path.unlink(missing_ok=True)


class GammaKeysetCatalog:
    """Complete active-market discovery using Gamma's keyset endpoint."""

    endpoint = "https://gamma-api.polymarket.com/markets/keyset"
    spool_schema_version = "marketcow.polymarket.gamma-keyset-spool.v1"

    def __init__(
        self,
        *,
        requester: Callable[..., Any] | None = None,
        session: requests.Session | None = None,
        timeout: float = 20,
        page_limit: int = 100,
        max_retries_per_page: int = 5,
        progress_every_pages: int = 25,
        progress: Callable[[dict[str, Any]], None] | None = None,
        spool_root: Path | None = None,
        sleeper: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.monotonic,
    ):
        self.session = session or requests.Session()
        self.requester = requester or self.session.get
        self.timeout = timeout
        self.page_limit = min(100, max(1, page_limit))
        self.max_retries_per_page = max(0, max_retries_per_page)
        self.progress_every_pages = max(1, progress_every_pages)
        self.progress = progress or self._log_progress
        self.spool_root = spool_root.resolve() if spool_root else None
        self.sleeper = sleeper
        self.clock = clock

    @property
    def _request_identity(self) -> dict[str, Any]:
        return {
            "endpoint": self.endpoint,
            "params": {
                "limit": self.page_limit,
                "closed": "false",
                "ascending": "true",
            },
            "schema_version": self.spool_schema_version,
        }

    @property
    def _retry_manifest_path(self) -> Path | None:
        if self.spool_root is None:
            return None
        return self.spool_root / "gamma-keyset-verified-retry.json"

    def _reuse_verified_spool(
        self,
    ) -> tuple[GammaCatalogRows, dict[str, Any]] | None:
        manifest_path = self._retry_manifest_path
        if manifest_path is None or not manifest_path.exists():
            return None
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise RuntimeError("verified Gamma spool manifest is unreadable") from exc
        if any(
            manifest.get(name) != expected
            for name, expected in self._request_identity.items()
        ):
            raise RuntimeError("verified Gamma spool request identity mismatch")
        if (
            manifest.get("complete") is not True
            or manifest.get("terminal_next_cursor") is not None
            or manifest.get("format") != "canonical_jsonl"
        ):
            raise RuntimeError("verified Gamma spool is not terminal and complete")
        spool_path = Path(str(manifest.get("path") or "")).resolve()
        verified_root = (self.spool_root / "verified").resolve()
        if not spool_path.is_relative_to(verified_root):
            raise RuntimeError("verified Gamma spool escapes its storage root")
        expected_size = int(manifest.get("raw_byte_size") or -1)
        if not spool_path.exists() or spool_path.stat().st_size != expected_size:
            raise RuntimeError("verified Gamma spool byte-size integrity failed")
        expected_sha256 = str(manifest.get("raw_payload_sha256") or "")
        if _file_sha256(spool_path) != expected_sha256:
            raise RuntimeError("verified Gamma spool hash integrity failed")
        market_count = int(manifest.get("market_count") or -1)
        pages = int(manifest.get("pages") or -1)
        if market_count < 0 or pages < 1:
            raise RuntimeError("verified Gamma spool count metadata is invalid")
        evidence = {
            key: manifest[key]
            for key in (
                "pages", "market_count", "complete", "last_cursor",
                "elapsed_seconds", "retry_count", "raw_payload_sha256",
                "raw_byte_size", "http_connection_reuse",
            )
        }
        evidence.update({
            "reused_verified_spool": True,
            "verified_spool_path": str(spool_path),
            "verified_spool_manifest": str(manifest_path),
        })
        self.progress(evidence)
        return GammaCatalogRows(
            spool_path,
            row_count=market_count,
            sha256=expected_sha256,
            retry_manifest_path=manifest_path,
        ), evidence

    @staticmethod
    def _log_progress(evidence: dict[str, Any]) -> None:
        LOGGER.info(
            "gamma_catalog_progress pages=%s markets=%s elapsed_seconds=%.3f retries=%s cursor=%s",
            evidence["pages"], evidence["market_count"], evidence["elapsed_seconds"],
            evidence["retry_count"], evidence["last_cursor"],
        )

    def fetch_all(self) -> tuple[GammaCatalogRows, dict[str, Any]]:
        if self.spool_root:
            self.spool_root.mkdir(parents=True, exist_ok=True)
            reusable = self._reuse_verified_spool()
            if reusable is not None:
                return reusable
        file_descriptor, spool_name = tempfile.mkstemp(
            prefix="marketcow-gamma-catalog-", suffix=".jsonl",
            dir=self.spool_root,
        )
        spool_path = Path(spool_name)
        index_path = spool_path.with_suffix(".sqlite3")
        cursor: str | None = None
        page_number = 0
        market_count = 0
        retry_count = 0
        started = self.clock()
        raw_hasher = hashlib.sha256()
        try:
            with os.fdopen(file_descriptor, "wb") as spool, sqlite3.connect(index_path) as index:
                index.execute("CREATE TABLE market_ids (value TEXT PRIMARY KEY)")
                index.execute("CREATE TABLE cursors (value TEXT PRIMARY KEY)")
                index.execute("CREATE TABLE page_hashes (value TEXT PRIMARY KEY)")
                while True:
                    page_number += 1
                    params: dict[str, Any] = {
                        "limit": self.page_limit,
                        "closed": "false",
                        "ascending": "true",
                    }
                    if cursor:
                        params["after_cursor"] = cursor
                    attempts = 0
                    while True:
                        response = self.requester(
                            self.endpoint, params=params, timeout=self.timeout,
                            headers={
                                "Accept": "application/json",
                                "User-Agent": "MarketCow/0.2",
                            },
                        )
                        if response.status_code != 429 and response.status_code < 500:
                            break
                        if attempts >= self.max_retries_per_page:
                            response.raise_for_status()
                        attempts += 1
                        retry_count += 1
                        retry_after = min(
                            8.0,
                            float(response.headers.get("Retry-After") or 2 ** (attempts - 1)),
                        )
                        self.sleeper(retry_after)
                    response.raise_for_status()
                    payload = json.loads(response.text, parse_float=str, parse_int=str)
                    if not isinstance(payload, dict):
                        raise RuntimeError("Gamma keyset response must be an object")
                    page = payload.get("markets")
                    if not isinstance(page, list):
                        raise RuntimeError("Gamma keyset response markets must be a list")
                    next_cursor_value = payload.get("next_cursor")
                    if next_cursor_value and str(next_cursor_value) == cursor:
                        raise RuntimeError("Gamma keyset cursor loop: cursor did not advance")
                    if not page and next_cursor_value:
                        raise RuntimeError("Gamma keyset made no progress before terminal cursor")
                    page_hash = content_sha256(page)
                    try:
                        index.execute("INSERT INTO page_hashes VALUES (?)", (page_hash,))
                    except sqlite3.IntegrityError as exc:
                        raise RuntimeError("Gamma keyset repeated a page without progress") from exc
                    for row in page:
                        market_id = str(row.get("id") or "")
                        if not market_id:
                            raise RuntimeError("Gamma keyset market lacks an id")
                        try:
                            index.execute("INSERT INTO market_ids VALUES (?)", (market_id,))
                        except sqlite3.IntegrityError as exc:
                            raise RuntimeError(
                                f"Gamma keyset repeated market id {market_id}"
                            ) from exc
                        line = canonical_json(row) + b"\n"
                        spool.write(line)
                        raw_hasher.update(line)
                        market_count += 1
                    index.commit()
                    elapsed = self.clock() - started
                    if page_number % self.progress_every_pages == 0:
                        self.progress({
                            "pages": page_number, "market_count": market_count,
                            "elapsed_seconds": elapsed, "retry_count": retry_count,
                            "last_cursor": cursor, "complete": False,
                        })
                    if not next_cursor_value:
                        spool.flush()
                        os.fsync(spool.fileno())
                        evidence = {
                            "pages": page_number, "market_count": market_count,
                            "complete": True, "last_cursor": cursor,
                            "elapsed_seconds": elapsed, "retry_count": retry_count,
                            "raw_payload_sha256": raw_hasher.hexdigest(),
                            "raw_byte_size": spool.tell(),
                            "http_connection_reuse": isinstance(
                                getattr(self.requester, "__self__", None), requests.Session
                            ),
                            "reused_verified_spool": False,
                        }
                        if self.spool_root is not None:
                            verified_root = self.spool_root / "verified"
                            verified_root.mkdir(parents=True, exist_ok=True)
                            verified_path = (
                                verified_root / f"{raw_hasher.hexdigest()}.jsonl"
                            )
                            if not verified_path.exists():
                                _atomic_copy(spool_path, verified_path)
                            if _file_sha256(verified_path) != raw_hasher.hexdigest():
                                raise RuntimeError(
                                    "verified Gamma spool integrity failed"
                                )
                            manifest_path = self._retry_manifest_path
                            manifest = {
                                **self._request_identity,
                                **evidence,
                                "format": "canonical_jsonl",
                                "terminal_next_cursor": None,
                                "path": str(verified_path.resolve()),
                            }
                            _atomic_write(manifest_path, canonical_json(manifest))
                            evidence.update({
                                "verified_spool_path": str(verified_path.resolve()),
                                "verified_spool_manifest": str(manifest_path),
                            })
                            spool_path.unlink(missing_ok=True)
                            self.progress(evidence)
                            return GammaCatalogRows(
                                verified_path,
                                row_count=market_count,
                                sha256=raw_hasher.hexdigest(),
                                retry_manifest_path=manifest_path,
                            ), evidence
                        self.progress(evidence)
                        return GammaCatalogRows(
                            spool_path, row_count=market_count,
                            sha256=raw_hasher.hexdigest(),
                        ), evidence
                    next_cursor = str(next_cursor_value)
                    try:
                        index.execute("INSERT INTO cursors VALUES (?)", (next_cursor,))
                        index.commit()
                    except sqlite3.IntegrityError as exc:
                        raise RuntimeError("Gamma keyset cursor loop detected") from exc
                    cursor = next_cursor
        except BaseException:
            spool_path.unlink(missing_ok=True)
            raise
        finally:
            index_path.unlink(missing_ok=True)


class GammaLiveNormalizer:
    """Provider boundary for live catalog metadata and explicit relations."""

    @staticmethod
    def _provenance(
        *, source: str, revision: str, source_url: str,
        observed_at: datetime, payload: Any, field_paths: list[str],
    ) -> LiveFactProvenance:
        return LiveFactProvenance(
            source=source, revision=revision, source_url=source_url,
            observed_at=observed_at, payload_sha256=content_sha256(payload),
            field_paths=field_paths,
        )

    @staticmethod
    def normalize(rows: Iterable[dict[str, Any]], observed_at: datetime) -> list[LiveMarket]:
        result = []
        pair_labels: dict[str, str] = {}
        for row in rows:
            tokens = _list(row.get("clobTokenIds") or row.get("clob_token_ids"))
            outcomes = _list(row.get("outcomes"))
            if len(tokens) != 2 or len(outcomes) != 2:
                continue
            condition_id = str(row.get("conditionId") or row.get("condition_id") or "")
            market_id = str(row.get("id") or "")
            if not condition_id or not market_id:
                continue
            pair_labels[market_id] = str(
                row.get("groupItemTitle") or row.get("group_item_title") or ""
            )
            event = (row.get("events") or [{}])[0]
            event_id = str(row.get("event_id") or event.get("id") or market_id)
            instruments = [f"POLY:{condition_id}:{token}" for token in tokens]
            neg_risk = bool(row.get("negRisk") or row.get("neg_risk"))
            neg_risk_id = str(
                row.get("negRiskMarketID") or row.get("neg_risk_market_id")
                or event.get("negRiskMarketID") or ""
            ) or None
            identity = PredictionMarketIdentity(
                event_id=event_id,
                market_id=market_id,
                condition_id=condition_id,
                slug=str(row.get("slug") or market_id),
                outcomes=[
                    OutcomeToken(token_id=token, outcome=outcome, instrument_id=instrument)
                    for token, outcome, instrument in zip(tokens, outcomes, instruments)
                ],
                neg_risk=neg_risk,
                neg_risk_market_id=neg_risk_id,
            )
            raw_hash = content_sha256(row)
            revision = content_sha256({
                "market_id": market_id,
                "updated_at": row.get("updatedAt") or row.get("updated_at"),
                "raw_payload_sha256": raw_hash,
            })
            gamma_provenance = GammaLiveNormalizer._provenance(
                source="polymarket_gamma", revision=revision,
                source_url=GammaKeysetCatalog.endpoint, observed_at=observed_at,
                payload=row, field_paths=[
                    "conditionId", "clobTokenIds", "outcomes", "events",
                    "negRisk", "negRiskMarketID",
                ],
            )
            relations = [LiveRelation(
                relation_id=f"binary:{condition_id}",
                relation_type="binary_complements",
                members=instruments,
                convertible=True,
                revision=revision,
                rule_version="gamma-binary-complements-v1",
                valid_from=observed_at,
                provenance=gamma_provenance,
                complete=True,
            )]
            if neg_risk:
                relations.append(LiveRelation(
                    relation_id=f"neg-risk:{neg_risk_id}",
                    relation_type="standard_negative_risk",
                    members=[],
                    convertible=True,
                    revision=revision,
                    rule_version="gamma-standard-negative-risk-v1",
                    valid_from=observed_at,
                    provenance=gamma_provenance,
                    missing_fields=["complete_outcome_pairs"],
                    complete=False,
                ))
            start_at = _instant(row["startDate"]) if row.get("startDate") else None
            end_at = _instant(row["endDate"]) if row.get("endDate") else None
            fee_schedule = row.get("fee_schedule") or row.get("feeSchedule") or {}
            raw_taker_base_fee = (
                row.get("takerBaseFee")
                if row.get("takerBaseFee") is not None
                else row.get("taker_base_fee")
            )
            fee_source_payload = {
                "fee_schedule": fee_schedule,
                "feesEnabled": row.get("feesEnabled"),
                "takerBaseFee": raw_taker_base_fee,
            }
            maker_rate = fee_schedule.get("maker_rate") or fee_schedule.get("makerRate")
            taker_rate = fee_schedule.get("taker_rate") or fee_schedule.get("takerRate")
            if maker_rate is None:
                maker_bps = fee_schedule.get("maker_fee_bps") or fee_schedule.get("makerFeeBps")
                maker_rate = _bps_rate(maker_bps, "maker_fee_bps") if maker_bps is not None else "0"
            if taker_rate is None:
                taker_rate = fee_schedule.get("rate")
            if taker_rate is None:
                taker_bps = (
                    fee_schedule.get("taker_fee_bps")
                    or fee_schedule.get("takerFeeBps")
                    or raw_taker_base_fee
                )
                if taker_bps is not None:
                    taker_rate = _bps_rate(taker_bps, "taker_fee_bps")
            if taker_rate is None and row.get("feesEnabled") is False:
                taker_rate = "0"
            fee_currency = fee_schedule.get("currency") or "USDC"
            fee_formula = fee_schedule.get("formula") or "fee = C * feeRate * p * (1 - p)"
            fee_exponent = fee_schedule.get("exponent") or "1"
            fee_quantum = fee_schedule.get("quantum") or "0.00001"
            fee_version = str(
                fee_schedule.get("version") or fee_schedule.get("scheduleVersion")
                or "gamma-plus-polymarket-fees-docs-v1"
            )
            effective_from = (
                _instant(fee_schedule["effectiveFrom"])
                if fee_schedule.get("effectiveFrom") else start_at
            )
            effective_to = (
                _instant(fee_schedule["effectiveTo"])
                if fee_schedule.get("effectiveTo") else None
            )
            tick = row.get("orderPriceMinTickSize") or row.get("minimumTickSize") or row.get("tickSize")
            minimum = row.get("orderMinSize") or row.get("minimumOrderSize")
            docs_provenance = GammaLiveNormalizer._provenance(
                source="polymarket_docs", revision="pusd-docs-2026-04-17",
                source_url=SETTLEMENT_SOURCE_URL, observed_at=observed_at,
                payload={"settlement_currency": SETTLEMENT_CURRENCY},
                field_paths=["collateral_token", "settlement_currency"],
            )
            sdk_provenance = GammaLiveNormalizer._provenance(
                source="polymarket_sdk",
                revision="b076b04d61135657e25dccc1bbd6866a96bd8c6e",
                source_url=SIZE_INCREMENT_SOURCE_URL, observed_at=observed_at,
                payload={"size_increment": SIZE_INCREMENT},
                field_paths=["ROUNDING_CONFIG.*.size"],
            )
            instrument_values = {
                "settlement_currency": SETTLEMENT_CURRENCY,
                "activation_at": start_at,
                "expiration_at": end_at,
                "price_increment": str(tick) if tick is not None else None,
                "size_increment": SIZE_INCREMENT,
                "minimum_order_size": str(minimum) if minimum is not None else None,
            }
            instrument_missing = _instrument_missing_fields(instrument_values)
            instrument_revision = content_sha256({
                "market_revision": revision,
                "values": {
                    key: value.isoformat() if isinstance(value, datetime) else value
                    for key, value in instrument_values.items()
                },
                "protocol_revisions": [docs_provenance.revision, sdk_provenance.revision],
            })
            fee_values = {
                "currency": str(fee_currency) if fee_currency is not None else None,
                "maker_rate": str(maker_rate) if maker_rate is not None else None,
                "taker_rate": str(taker_rate) if taker_rate is not None else None,
                "formula": str(fee_formula) if fee_formula is not None else None,
                "exponent": str(fee_exponent) if fee_exponent is not None else None,
                "quantum": str(fee_quantum) if fee_quantum is not None else None,
                "effective_from": effective_from,
            }
            fee_missing = _fee_missing_fields(fee_values, effective_to)
            fee_provenance = GammaLiveNormalizer._provenance(
                source="polymarket_gamma", revision=fee_version,
                source_url=GammaKeysetCatalog.endpoint, observed_at=observed_at,
                payload=fee_source_payload,
                field_paths=["fee_schedule", "feesEnabled", "takerBaseFee"],
            )
            fee_docs_provenance = GammaLiveNormalizer._provenance(
                source="polymarket_docs", revision="trading-fees-docs-v1",
                source_url=FEE_SOURCE_URL, observed_at=observed_at,
                payload={
                    "currency": "USDC", "maker_rate": "0",
                    "formula": "fee = C * feeRate * p * (1 - p)",
                    "exponent": "1", "quantum": "0.00001",
                    "rounding_mode": "UNSPECIFIED",
                },
                field_paths=["fee_structure", "fee_precision"],
            )
            closed = bool(row.get("closed"))
            resolution = row.get("resolution")
            lifecycle = "resolved" if resolution not in {None, ""} else "closed" if closed else "active"
            result.append(LiveMarket(
                identity=identity,
                question=str(row.get("question") or row.get("title") or market_id),
                title=str(row.get("title") or row.get("question") or market_id),
                active=bool(row.get("active", not closed)),
                closed=closed,
                accepting_orders=bool(row.get("acceptingOrders", row.get("accepting_orders", False))),
                lifecycle_state=lifecycle,
                resolution=str(resolution) if resolution not in {None, ""} else None,
                start_at=start_at,
                end_at=end_at,
                metadata_revision=revision,
                observed_at=observed_at,
                rules=LiveRuleSet(
                    rule_version="marketcow-polymarket-live-rules-v1",
                    instrument=LiveInstrumentFacts(
                        revision=instrument_revision, **instrument_values,
                        provenance=[gamma_provenance, docs_provenance, sdk_provenance],
                        missing_fields=instrument_missing, complete=not instrument_missing,
                    ),
                    fee_schedule=LiveFeeSchedule(
                        schedule_id=content_sha256({
                            "market_id": market_id, "version": fee_version,
                            "payload": fee_source_payload,
                        }),
                        schedule_version=fee_version,
                        **fee_values,
                        effective_to=effective_to,
                        rounding_mode="UNSPECIFIED", tie_semantics="unspecified",
                        calculation_status="informational_only",
                        provenance=[fee_provenance, fee_docs_provenance],
                        missing_fields=fee_missing,
                        complete=not fee_missing,
                    ),
                    rules_complete=not instrument_missing,
                ),
                relations=relations,
                raw_payload_sha256=raw_hash,
            ))
        neg_risk_groups: dict[str, list[LiveMarket]] = defaultdict(list)
        for market in result:
            if market.identity.neg_risk and market.identity.neg_risk_market_id:
                neg_risk_groups[market.identity.neg_risk_market_id].append(market)
        for group_id, group in neg_risk_groups.items():
            pairs = []
            for member_market in group:
                by_label = {
                    outcome.outcome.strip().casefold(): outcome
                    for outcome in member_market.identity.outcomes
                }
                label = pair_labels.get(member_market.identity.market_id, "")
                if set(by_label) != {"yes", "no"} or not label:
                    continue
                yes, no = by_label["yes"], by_label["no"]
                pair_payload = {
                    "event_id": member_market.identity.event_id,
                    "market_id": member_market.identity.market_id,
                    "condition_id": member_market.identity.condition_id,
                    "outcome_label": label,
                    "yes_token_id": yes.token_id,
                    "yes_instrument_id": yes.instrument_id,
                    "no_token_id": no.token_id,
                    "no_instrument_id": no.instrument_id,
                }
                pair_revision = content_sha256({
                    "pair": pair_payload,
                    "market_revision": member_market.metadata_revision,
                })
                relation_provenance = next(
                    item.provenance for item in member_market.relations
                    if item.relation_type == "standard_negative_risk"
                )
                pairs.append(LiveOutcomePair(
                    pair_id=content_sha256({
                        "neg_risk_market_id": group_id,
                        "condition_id": member_market.identity.condition_id,
                    }),
                    pair_revision=pair_revision, **pair_payload,
                    valid_from=observed_at, provenance=relation_provenance,
                ))
            pairs.sort(key=lambda item: (item.outcome_label, item.market_id))
            members = sorted({item.yes_instrument_id for item in pairs})
            missing_fields = []
            if len(pairs) != len(group):
                missing_fields.append("complete_outcome_pairs")
            if len(pairs) < 2:
                missing_fields.append("mutually_exclusive_yes_member_set")
            missing_fields = sorted(set(missing_fields))
            relation_id = f"neg-risk:{group_id}"
            relation_revision = content_sha256({
                "relation_id": relation_id, "members": members,
                "outcome_pairs": [item.model_dump(mode="json") for item in pairs],
                "market_revisions": sorted(
                    item.metadata_revision for item in group
                ),
            })
            for market in group:
                relation_index, previous = next(
                    (index, item) for index, item in enumerate(market.relations)
                    if item.relation_type == "standard_negative_risk"
                )
                market.relations[relation_index] = LiveRelation(
                    relation_id=relation_id,
                    relation_type="standard_negative_risk",
                    members=members,
                    convertible=previous.convertible,
                    revision=relation_revision,
                    rule_version=previous.rule_version,
                    valid_from=previous.valid_from,
                    valid_to=previous.valid_to,
                    source=previous.source,
                    provenance=previous.provenance,
                    outcome_pairs=[item.model_copy() for item in pairs],
                    missing_fields=missing_fields,
                    complete=not missing_fields,
                )
        return result


class ClobBooksClient:
    endpoint = "https://clob.polymarket.com/books"

    def __init__(
        self,
        *,
        requester: Callable[..., Any] | None = None,
        session: requests.Session | None = None,
        timeout: float = 20,
        batch_size: int = 500,
        max_retries_per_batch: int = 5,
        progress_every_batches: int = 25,
        progress: Callable[[dict[str, Any]], None] | None = None,
        sleeper: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.monotonic,
    ):
        self.session = session or requests.Session()
        self.requester = requester or self.session.post
        self.timeout = timeout
        self.batch_size = min(500, max(1, batch_size))
        self.max_retries_per_batch = max(0, max_retries_per_batch)
        self.progress_every_batches = max(1, progress_every_batches)
        self.progress = progress or self._log_progress
        self.sleeper = sleeper
        self.clock = clock
        self.last_evidence: dict[str, Any] | None = None

    @staticmethod
    def _log_progress(evidence: dict[str, Any]) -> None:
        LOGGER.info(
            "clob_books_progress batches=%s/%s requested=%s received=%s elapsed_seconds=%.3f retries=%s",
            evidence["batches"], evidence["batch_count"],
            evidence["requested_token_count"], evidence["received_book_count"],
            evidence["elapsed_seconds"], evidence["retry_count"],
        )

    def fetch(self, token_ids: Iterable[str]) -> list[dict[str, Any]]:
        return self.fetch_stream(token_ids)

    def fetch_stream(
        self,
        token_ids: Iterable[str],
        *,
        batch_consumer: Callable[[list[dict[str, Any]]], None] | None = None,
    ) -> list[dict[str, Any]]:
        tokens = list(dict.fromkeys(str(item) for item in token_ids))
        rows = []
        received_book_count = 0
        batch_count = (len(tokens) + self.batch_size - 1) // self.batch_size
        retry_count = 0
        started = self.clock()
        for batch_number, start in enumerate(
            range(0, len(tokens), self.batch_size), start=1,
        ):
            request_body = [
                {"token_id": token}
                for token in tokens[start:start + self.batch_size]
            ]
            attempts = 0
            while True:
                try:
                    response = self.requester(
                        self.endpoint, json=request_body, timeout=self.timeout,
                        headers={
                            "Content-Type": "application/json",
                            "User-Agent": "MarketCow/0.2",
                        },
                    )
                except requests.RequestException:
                    if attempts >= self.max_retries_per_batch:
                        raise
                    attempts += 1
                    retry_count += 1
                    continue
                if response.status_code != 429 and response.status_code < 500:
                    break
                if attempts >= self.max_retries_per_batch:
                    response.raise_for_status()
                attempts += 1
                retry_count += 1
                retry_after = min(
                    8.0,
                    float(response.headers.get("Retry-After") or 2 ** (attempts - 1)),
                )
                self.sleeper(retry_after)
            response.raise_for_status()
            payload = json.loads(response.text, parse_float=str, parse_int=str)
            if not isinstance(payload, list):
                raise RuntimeError("CLOB /books response must be a list")
            received_book_count += len(payload)
            if batch_consumer is None:
                rows.extend(payload)
            else:
                batch_consumer(payload)
            evidence = {
                "batches": batch_number, "batch_count": batch_count,
                "requested_token_count": len(tokens),
                "received_book_count": received_book_count,
                "elapsed_seconds": self.clock() - started,
                "retry_count": retry_count,
                "complete": batch_number == batch_count,
            }
            if evidence["complete"] or batch_number % self.progress_every_batches == 0:
                self.progress(evidence)
        self.last_evidence = {
            "batches": batch_count, "batch_count": batch_count,
            "requested_token_count": len(tokens),
            "received_book_count": received_book_count,
            "elapsed_seconds": self.clock() - started,
            "retry_count": retry_count, "complete": True,
        }
        return rows


class SubscriptionPlanner:
    """Deterministic full-market subscription sharding and dynamic diffs."""

    def __init__(self, shard_size: int = 500):
        self.shard_size = max(1, shard_size)
        self.current: set[str] = set()

    def shards(self, tokens: Iterable[str]) -> list[list[str]]:
        ordered = sorted(set(tokens))
        return [ordered[index:index + self.shard_size] for index in range(0, len(ordered), self.shard_size)]

    def connection_groups(
        self, tokens: Iterable[str], max_connections: int,
    ) -> list[list[str]]:
        shards = self.shards(tokens)
        connection_count = min(max(1, max_connections), len(shards))
        if not connection_count:
            return []
        groups: list[list[str]] = [[] for _ in range(connection_count)]
        for index, shard in enumerate(shards):
            groups[index % connection_count].extend(shard)
        return groups

    def initial_messages(self, tokens: Iterable[str]) -> list[dict[str, Any]]:
        self.current = set(tokens)
        return [
            {"assets_ids": shard, "type": "market", "custom_feature_enabled": True}
            for shard in self.shards(self.current)
        ]

    def update_messages(self, tokens: Iterable[str]) -> list[dict[str, Any]]:
        desired = set(tokens)
        added, removed = desired - self.current, self.current - desired
        messages = [
            {"assets_ids": shard, "operation": "subscribe", "custom_feature_enabled": True}
            for shard in self.shards(added)
        ] + [
            {"assets_ids": shard, "operation": "unsubscribe"}
            for shard in self.shards(removed)
        ]
        self.current = desired
        return messages


class LiveStateStore:
    """Append-only live state, deterministic recovery, and fail-closed frames."""

    def __init__(
        self,
        root: Path,
        *,
        replay_capacity: int = 100_000,
        max_frame_skew_ms: int = 5_000,
        stale_after_ms: int = 30_000,
        now_provider: Callable[[], datetime] = utc_now,
    ):
        self.root = root.resolve()
        self.event_path = self.root / "events.jsonl"
        self.checkpoint_path = self.root / "checkpoint.json"
        self.catalog_path = self.root / "catalog.json"
        self.raw_catalog_root = self.root / "raw" / "gamma-catalog"
        self.normalized_catalog_root = self.root / "catalogs"
        self.raw_catalog_path: Path | None = None
        self.state_index = LiveStateIndex(self.root)
        self.replay_capacity = max(1, replay_capacity)
        self.max_frame_skew_ms = max(0, max_frame_skew_ms)
        self.stale_after_ms = max(1, stale_after_ms)
        self.now_provider = now_provider
        self.catalog: dict[str, LiveMarket] = {}
        self.token_to_market: dict[str, str] = {}
        self.books: dict[str, LiveBook] = {}
        self._rest_refresh_generation: dict[str, datetime] = {}
        self.gaps: list[GapEntry] = []
        self.events: deque[LiveEventEnvelope] = deque(maxlen=self.replay_capacity)
        self.seen_raw_hashes: set[str] = set()
        self._seen_raw_hash_order: deque[str] = deque()
        self.active_recovery_id: str | None = None
        self.cursor = 0
        self.catalog_revision: str | None = None
        self.catalog_source: dict[str, Any] | None = None
        self._catalog_file_sha256: str | None = None
        self._checkpoint_file_sha256: str | None = None
        self._checkpoint_cursor = 0
        self._event_offset = 0
        self._last_log_cursor = 0
        self._sync_lock = threading.RLock()
        self._event_batch_state = threading.local()
        # API construction must not deserialize the multi-GB catalog or replay the
        # event log. Stateful operations retain compatibility through _ensure_loaded.
        self._recovered = False

    def _remember_raw_hash(self, raw_hash: str) -> None:
        if raw_hash in self.seen_raw_hashes:
            return
        self.seen_raw_hashes.add(raw_hash)
        self._seen_raw_hash_order.append(raw_hash)
        while len(self._seen_raw_hash_order) > self.replay_capacity:
            expired = self._seen_raw_hash_order.popleft()
            self.seen_raw_hashes.discard(expired)

    @contextmanager
    def durable_event_batch(self):
        """Append a group of events with one durable flush.

        The durable log is entered after the SQLite batch, so it flushes before
        the derived index commits.  A failed batch truncates its unpublished
        tail while the publication lock is still held.
        """
        if getattr(self._event_batch_state, "stream", None) is not None:
            yield
            return
        self.event_path.parent.mkdir(parents=True, exist_ok=True)
        stream = self.event_path.open("a+b")
        stream.seek(0, os.SEEK_END)
        initial_offset = stream.tell()
        self._event_batch_state.stream = stream
        try:
            yield
            if stream.tell() != initial_offset:
                stream.flush()
                os.fsync(stream.fileno())
        except BaseException:
            stream.seek(initial_offset)
            stream.truncate()
            stream.flush()
            os.fsync(stream.fileno())
            raise
        finally:
            del self._event_batch_state.stream
            stream.close()

    def _append(self, value: dict[str, Any]) -> tuple[int, int, str]:
        self.event_path.parent.mkdir(parents=True, exist_ok=True)
        body = canonical_json(value) + b"\n"
        stream = getattr(self._event_batch_state, "stream", None)
        if stream is not None:
            byte_offset = stream.tell()
            stream.write(body)
            return byte_offset, len(body), hashlib.sha256(body).hexdigest()
        with self.event_path.open("ab") as standalone:
            byte_offset = standalone.tell()
            standalone.write(body)
            standalone.flush()
            os.fsync(standalone.fileno())
            return byte_offset, len(body), hashlib.sha256(body).hexdigest()

    def replace_catalog(
        self,
        markets: list[LiveMarket],
        raw_rows: Iterable[dict[str, Any]] | GammaCatalogRows,
    ) -> dict[str, Any]:
        self._ensure_loaded()
        by_id = {market.identity.market_id: market for market in markets}
        for market_id, previous in self.catalog.items():
            if market_id not in by_id and previous.lifecycle_state == "resolved":
                by_id[market_id] = previous
        markets = list(by_id.values())
        markets.sort(key=lambda item: item.identity.market_id)
        revision = _market_sequence_sha256(markets)
        previous_tokens = set(self.token_to_market)
        next_token_to_market = {
            outcome.token_id: market.identity.market_id
            for market in markets if market.active and not market.closed
            for outcome in market.identity.outcomes
        }
        if isinstance(raw_rows, GammaCatalogRows):
            raw_sha256 = raw_rows.sha256
            raw_format = "canonical_jsonl"
            raw_suffix = ".jsonl"
            raw_market_count = len(raw_rows)
        else:
            raw_rows = list(raw_rows)
            raw_body = canonical_json(raw_rows)
            raw_sha256 = hashlib.sha256(raw_body).hexdigest()
            raw_format = "canonical_json_array"
            raw_suffix = ".json"
            raw_market_count = len(raw_rows)
        next_raw_catalog_path = self.raw_catalog_root / f"{raw_sha256}{raw_suffix}"
        next_catalog_source = {
            "source": "polymarket_gamma",
            "source_url": GammaKeysetCatalog.endpoint,
            "observed_at": self.now_provider().astimezone(timezone.utc).isoformat(),
            "raw_payload_sha256": raw_sha256,
            "raw_path": str(next_raw_catalog_path),
            "raw_format": raw_format,
            "market_count": raw_market_count,
        }
        if not next_raw_catalog_path.exists():
            if isinstance(raw_rows, GammaCatalogRows):
                _atomic_copy(raw_rows.path, next_raw_catalog_path)
            else:
                _atomic_write(next_raw_catalog_path, raw_body)
        if _file_sha256(next_raw_catalog_path) != raw_sha256:
            raise RuntimeError("live raw catalog integrity failed before publication")
        with _publication_lock(self.root, exclusive=True):
            _atomic_write_catalog(
                self.catalog_path, revision=revision, markets=markets,
                source=next_catalog_source,
            )
            self.catalog = by_id
            self.token_to_market = next_token_to_market
            self.catalog_revision = revision
            self.raw_catalog_path = next_raw_catalog_path
            self.catalog_source = next_catalog_source
            self._catalog_file_sha256 = _file_sha256(self.catalog_path)
            next_tokens = set(next_token_to_market)
            added_tokens = sorted(next_tokens - previous_tokens)
            removed_tokens = sorted(previous_tokens - next_tokens)
            token_changes = {
                "added_count": len(added_tokens),
                "removed_count": len(removed_tokens),
                "added_sha256": content_sha256(added_tokens),
                "removed_sha256": content_sha256(removed_tokens),
                "requires_bootstrap": True,
            }
            self._emit(
                "catalog_revision",
                {"catalog_revision": revision, "token_changes": token_changes},
                {
                    "catalog_source": self.catalog_source,
                    "market_count": raw_market_count,
                },
                applied=True, _publication_locked=True,
            )
        return {"catalog_revision": revision, "token_changes": token_changes}

    def _emit(
        self,
        event_type: str,
        canonical_payload: dict[str, Any],
        raw_payload: dict[str, Any],
        *,
        applied: bool,
        market_id: str | None = None,
        condition_id: str | None = None,
        token_id: str | None = None,
        book_epoch: str | None = None,
        sequence: int | None = None,
        exchange_at: datetime | None = None,
        received_at: datetime | None = None,
        reason: str | None = None,
        gaps: list[GapEntry] | None = None,
        _publication_locked: bool = False,
    ) -> LiveEventEnvelope:
        if not _publication_locked:
            with _publication_lock(self.root, exclusive=True):
                return self._emit(
                    event_type, canonical_payload, raw_payload,
                    applied=applied, market_id=market_id,
                    condition_id=condition_id, token_id=token_id,
                    book_epoch=book_epoch, sequence=sequence,
                    exchange_at=exchange_at, received_at=received_at,
                    reason=reason, gaps=gaps, _publication_locked=True,
                )
        received = received_at or self.now_provider()
        exchange = exchange_at or received
        self.cursor += 1
        raw_hash = content_sha256(raw_payload)
        canonical_hash = content_sha256(canonical_payload)
        envelope = LiveEventEnvelope(
            cursor=self.cursor, event_id="0" * 64, event_type=event_type,
            market_id=market_id, condition_id=condition_id, token_id=token_id,
            book_epoch=book_epoch, sequence=sequence,
            exchange_at=exchange, received_at=received,
            canonical_payload=canonical_payload,
            canonical_payload_sha256=canonical_hash,
            raw_payload=raw_payload, raw_payload_sha256=raw_hash,
            applied=applied, fail_closed_reason=reason,
            gaps=gaps or [],
        )
        envelope.event_id = live_event_identity(envelope)
        self.events.append(envelope)
        byte_offset, byte_length, line_sha256 = self._append(
            envelope.model_dump(mode="json")
        )
        self._event_offset = byte_offset + byte_length
        self._last_log_cursor = envelope.cursor
        self._remember_raw_hash(raw_hash)
        index_gaps = list(gaps or [])
        if event_type == "recovery_completed" and applied:
            recovered = set(
                canonical_payload.get("resolved_gap_token_ids")
                or canonical_payload.get("recovered_token_ids") or []
            )
            index_gaps.extend(
                gap for gap in self.gaps
                if gap.token_id in recovered and gap not in index_gaps
            )
        self.state_index.append(
            envelope,
            byte_offset=byte_offset,
            byte_length=byte_length,
            line_sha256=line_sha256,
            book=self.books.get(token_id or "") if applied else None,
            gaps=index_gaps,
            catalog_revision=self.catalog_revision,
            event_log_size=self._event_offset,
            token_to_market=self.token_to_market,
            active_recovery_id=self.active_recovery_id,
            book_token_count=len(self.books),
            book_complete_market_count=sum(
                all(outcome.token_id in self.books for outcome in market.identity.outcomes)
                for market in self.catalog.values()
                if market.active and not market.closed
            ),
            unresolved_gap_count=sum(not gap.resolved for gap in self.gaps),
        )
        return envelope

    def _market_for_token(self, token_id: str) -> LiveMarket:
        market_id = self.token_to_market.get(token_id)
        if not market_id or market_id not in self.catalog:
            raise ValueError("token is outside the active canonical catalog")
        return self.catalog[market_id]

    def apply_snapshot(
        self,
        raw: dict[str, Any],
        *,
        recovery_id: str | None = None,
        received_at: datetime | None = None,
        allow_freshness_confirmation: bool = False,
    ) -> LiveEventEnvelope | None:
        self._ensure_loaded()
        token_id = str(raw.get("asset_id") or raw.get("token_id") or "")
        market = self._market_for_token(token_id)
        tick_value = raw.get("tick_size") or market.rules.instrument.price_increment
        if tick_value is None:
            raise ValueError("snapshot lacks source-backed price increment")
        tick = decimal_text(tick_value, "tick_size", allow_zero=False)
        bids, asks = _levels(raw.get("bids"), "bids"), _levels(raw.get("asks"), "asks")
        state_bids = {item["price"]: item["size"] for item in bids}
        state_asks = {item["price"]: item["size"] for item in asks}
        _validate_book({"tick_size": tick, "bids": state_bids, "asks": state_asks})
        received = received_at or self.now_provider()
        exchange = _instant(raw.get("timestamp") or received)
        previous = self.books.get(token_id)
        source_hash = str(raw.get("hash") or "") or None
        last_trade_price = (
            decimal_text(raw["last_trade_price"], "last_trade_price")
            if raw.get("last_trade_price") is not None else None
        )
        epoch = content_sha256({
            "token_id": token_id, "recovery_id": recovery_id or "initial",
            "source_hash": raw.get("hash"), "exchange_at": exchange.isoformat(),
        })
        sequence = 1 if previous is None or previous.book_epoch != epoch else previous.sequence + 1
        checksum = _state_checksum(token_id, tick, state_bids, state_asks)
        if (
            allow_freshness_confirmation
            and previous is not None
            and previous.tick_size == tick
            and previous.state_checksum == checksum
            and previous.last_trade_price == last_trade_price
        ):
            confirmed = previous.model_copy(update={
                "exchange_at": exchange,
                "received_at": received,
                "source_hash": source_hash,
            })
            self.books[token_id] = confirmed
            self.state_index.confirm_book(confirmed)
            return None
        book = LiveBook(
            token_id=token_id, condition_id=market.identity.condition_id,
            book_epoch=epoch, sequence=sequence,
            exchange_at=exchange, received_at=received,
            tick_version=content_sha256({"tick_size": tick}), tick_size=tick,
            bids=bids, asks=asks,
            last_trade_price=last_trade_price,
            state_checksum=checksum, source_hash=source_hash,
        )
        self.books[token_id] = book
        return self._emit(
            "book", book.model_dump(mode="json"), raw, applied=True,
            market_id=market.identity.market_id, condition_id=market.identity.condition_id,
            token_id=token_id, book_epoch=epoch, sequence=sequence,
            exchange_at=exchange, received_at=received,
        )

    def apply_websocket(
        self,
        raw: dict[str, Any],
        *,
        received_at: datetime | None = None,
        stale_events_are_resolved: bool = False,
    ) -> list[LiveEventEnvelope]:
        self._ensure_loaded()
        raw_hash = content_sha256(raw)
        event_type = str(raw.get("event_type") or raw.get("type") or "")
        received = received_at or self.now_provider()
        exchange = _instant(raw.get("timestamp") or received)
        if raw_hash in self.seen_raw_hashes:
            gap = GapEntry(
                code="duplicate", observed=raw_hash, event_at=exchange,
                detected_at=received, resolved=True, resolution="ignored_idempotently",
            )
            self.gaps.append(gap)
            safe_type = event_type if event_type in {
                "book", "price_change", "best_bid_ask", "last_trade_price",
                "tick_size_change", "new_market", "market_resolved",
            } else "price_change"
            self._emit(
                safe_type, {"ignored": "duplicate"}, raw, applied=False,
                exchange_at=exchange, received_at=received,
                reason="duplicate", gaps=[gap],
            )
            return []
        if event_type == "book":
            return [self.apply_snapshot(raw, received_at=received)]
        if event_type in {"new_market", "market_resolved"}:
            condition_id = str(raw.get("condition_id") or raw.get("market") or "")
            affected = next((
                market for market in self.catalog.values()
                if market.identity.condition_id == condition_id
            ), None)
            canonical = {
                "requires_catalog_refresh": True,
                "condition_id": condition_id,
            }
            if event_type == "market_resolved" and affected is not None:
                affected.active = False
                affected.closed = True
                affected.accepting_orders = False
                affected.lifecycle_state = "resolved"
                affected.resolution = str(
                    raw.get("winning_outcome") or raw.get("winning_asset_id") or ""
                ) or "resolved"
                affected.metadata_revision = content_sha256({
                    "previous_revision": affected.metadata_revision,
                    "market_resolved_raw_sha256": raw_hash,
                })
                for relation in affected.relations:
                    relation.valid_to = exchange
                    relation.revision = content_sha256({
                        "previous_revision": relation.revision,
                        "valid_to": exchange.isoformat(),
                        "market_resolved_raw_sha256": raw_hash,
                    })
                    for pair in relation.outcome_pairs:
                        pair.valid_to = exchange
                for outcome in affected.identity.outcomes:
                    self.token_to_market.pop(outcome.token_id, None)
                canonical.update({
                    "market_id": affected.identity.market_id,
                    "lifecycle_state": "resolved",
                    "resolution": affected.resolution,
                    "metadata_revision": affected.metadata_revision,
                })
            return [self._emit(event_type, {
                **canonical,
            }, raw, applied=affected is not None or event_type == "new_market",
            market_id=(affected.identity.market_id if affected else None),
            condition_id=condition_id or None,
            exchange_at=exchange, received_at=received,
            reason=(None if affected is not None else "catalog_refresh_required"))]
        if event_type == "price_change":
            changes = raw.get("price_changes") or raw.get("changes") or []
            envelopes = []
            for change in changes:
                token_id = str(change.get("asset_id") or change.get("token_id") or "")
                envelopes.append(self._apply_token_event(
                    event_type,
                    token_id,
                    change,
                    raw,
                    exchange,
                    received,
                    stale_events_are_resolved=stale_events_are_resolved,
                ))
            return envelopes
        token_id = str(raw.get("asset_id") or raw.get("token_id") or "")
        return [self._apply_token_event(
            event_type,
            token_id,
            raw,
            raw,
            exchange,
            received,
            stale_events_are_resolved=stale_events_are_resolved,
        )]

    def _apply_token_event(
        self,
        event_type: str,
        token_id: str,
        payload: dict[str, Any],
        raw: dict[str, Any],
        exchange: datetime,
        received: datetime,
        *,
        stale_events_are_resolved: bool = False,
    ) -> LiveEventEnvelope:
        if event_type not in {"price_change", "best_bid_ask", "last_trade_price", "tick_size_change"}:
            raise ValueError("unsupported public market-channel event")
        market = self._market_for_token(token_id)
        previous = self.books.get(token_id)
        if previous is None:
            gap = GapEntry(
                code="missing_snapshot", token_id=token_id,
                observed=content_sha256(raw), event_at=exchange, detected_at=received,
            )
            self.gaps.append(gap)
            return self._emit(
                event_type, {"requires_snapshot_recovery": True}, raw, applied=False,
                market_id=market.identity.market_id, condition_id=market.identity.condition_id,
                token_id=token_id, exchange_at=exchange, received_at=received,
                reason="missing_snapshot", gaps=[gap],
            )
        if exchange < previous.exchange_at:
            gap = GapEntry(
                code="out_of_order", token_id=token_id,
                expected=previous.exchange_at.isoformat(), observed=exchange.isoformat(),
                event_at=exchange, detected_at=received,
                resolved=stale_events_are_resolved,
                resolution=(
                    "superseded_by_newer_snapshot"
                    if stale_events_are_resolved else None
                ),
            )
            self.gaps.append(gap)
            return self._emit(
                event_type, {"ignored": "out_of_order"}, raw, applied=False,
                market_id=market.identity.market_id, condition_id=market.identity.condition_id,
                token_id=token_id, book_epoch=previous.book_epoch, sequence=previous.sequence,
                exchange_at=exchange, received_at=received, reason="out_of_order",
                gaps=[gap],
            )
        book = previous.model_copy(deep=True)
        book.sequence += 1
        book.exchange_at = exchange
        book.received_at = received
        if event_type == "price_change":
            side = str(payload.get("side") or "").upper()
            side_name = "bids" if side in {"BUY", "BID"} else "asks" if side in {"SELL", "ASK"} else ""
            if not side_name:
                raise ValueError("price_change side must be BUY/SELL")
            price = decimal_text(payload.get("price"), "price_change.price")
            size = decimal_text(payload.get("size"), "price_change.size")
            levels = {item["price"]: item["size"] for item in getattr(book, side_name)}
            if Decimal(size) == 0:
                levels.pop(price, None)
            else:
                levels[price] = size
            setattr(book, side_name, [
                {"price": item, "size": levels[item]}
                for item in sorted(levels, key=Decimal, reverse=side_name == "bids")
            ])
        elif event_type == "last_trade_price":
            book.last_trade_price = decimal_text(payload.get("price"), "last_trade_price")
        elif event_type == "tick_size_change":
            new_tick = payload.get("new_tick_size") or payload.get("tick_size")
            book.tick_size = decimal_text(new_tick, "new_tick_size", allow_zero=False)
            book.tick_version = content_sha256({"tick_size": book.tick_size})
        bids = {item["price"]: item["size"] for item in book.bids}
        asks = {item["price"]: item["size"] for item in book.asks}
        try:
            _validate_book({"tick_size": book.tick_size, "bids": bids, "asks": asks})
        except ValueError as exc:
            gap = GapEntry(
                code="source_mismatch", token_id=token_id, observed=str(exc),
                event_at=exchange, detected_at=received,
            )
            self.gaps.append(gap)
            return self._emit(
                event_type, {"requires_snapshot_recovery": True}, raw, applied=False,
                market_id=market.identity.market_id, condition_id=market.identity.condition_id,
                token_id=token_id, book_epoch=previous.book_epoch, sequence=previous.sequence,
                exchange_at=exchange, received_at=received, reason="invalid_book_update",
                gaps=[gap],
            )
        book.state_checksum = _state_checksum(token_id, book.tick_size, bids, asks)
        self.books[token_id] = book
        return self._emit(
            event_type, book.model_dump(mode="json"), raw, applied=True,
            market_id=market.identity.market_id, condition_id=market.identity.condition_id,
            token_id=token_id, book_epoch=book.book_epoch, sequence=book.sequence,
            exchange_at=exchange, received_at=received,
        )

    def mark_recovery_started(self, reason: str) -> str:
        self._ensure_loaded()
        recovery_id = content_sha256({"cursor": self.cursor, "reason": reason, "at": self.now_provider().isoformat()})
        self.active_recovery_id = recovery_id
        self._emit(
            "recovery_started", {"recovery_id": recovery_id, "reason": reason}, {},
            applied=True,
        )
        return recovery_id

    def new_book_recovery_tracker(self) -> dict[str, Any]:
        return {
            "expected": set(self.token_to_market),
            "recovered": set(),
            "invalid": {},
            "duplicate_response_count": 0,
            "unknown_response_count": 0,
        }

    def recover_book_batch(
        self,
        rows: list[dict[str, Any]],
        recovery_id: str,
        tracker: dict[str, Any],
        *,
        minimum_book_age_seconds: float = 0,
        refresh_started_at: datetime | None = None,
        received_at: datetime | None = None,
    ) -> None:
        self._ensure_loaded()
        recovered = tracker["recovered"]
        invalid = tracker["invalid"]
        for row in rows:
            token_id = str(row.get("asset_id") or row.get("token_id") or "")
            if token_id not in tracker["expected"]:
                tracker["unknown_response_count"] += 1
                continue
            if token_id in recovered or token_id in invalid:
                tracker["duplicate_response_count"] += 1
                continue
            previous = self.books.get(token_id)
            if (
                refresh_started_at is not None
                and refresh_started_at
                < self._rest_refresh_generation.get(token_id, datetime.min.replace(
                    tzinfo=timezone.utc
                ))
            ):
                recovered.add(token_id)
                continue
            market_id = self.token_to_market.get(token_id)
            market_token_ids = [
                candidate
                for candidate, candidate_market_id in self.token_to_market.items()
                if candidate_market_id == market_id
            ]
            if (
                previous is not None
                and minimum_book_age_seconds > 0
                and market_token_ids
                and all(
                    candidate in self.books
                    and (
                        self.now_provider() - self.books[candidate].received_at
                    ).total_seconds() < minimum_book_age_seconds
                    for candidate in market_token_ids
                )
            ):
                recovered.add(token_id)
                continue
            try:
                self.apply_snapshot(
                    row, recovery_id=recovery_id, received_at=received_at,
                    allow_freshness_confirmation=(refresh_started_at is not None),
                )
            except ValueError as exc:
                reason = str(exc)
                invalid[token_id] = reason
                detected = self.now_provider()
                gap = GapEntry(
                    code="source_mismatch", token_id=token_id,
                    observed=reason, detected_at=detected,
                )
                self.gaps.append(gap)
                market_id = self.token_to_market[token_id]
                market = self.catalog[market_id]
                self._emit(
                    "book", {"requires_snapshot_recovery": True}, row,
                    applied=False, market_id=market_id,
                    condition_id=market.identity.condition_id,
                    token_id=token_id, received_at=detected,
                    reason="invalid_rest_book", gaps=[gap],
                )
                continue
            if refresh_started_at is not None:
                self._rest_refresh_generation[token_id] = refresh_started_at
            recovered.add(token_id)

    def complete_book_recovery(
        self,
        recovery_id: str,
        tracker: dict[str, Any],
        *,
        write_checkpoint: bool = True,
        publish_completion_event: bool = True,
    ) -> dict[str, Any]:
        self._ensure_loaded()
        recovered = tracker["recovered"]
        invalid = tracker["invalid"]
        missing = tracker["expected"] - recovered
        resolved_gap_token_ids = set()
        for gap in self.gaps:
            if not gap.resolved and gap.token_id in recovered:
                gap.resolved = True
                gap.resolution = f"rest_books_snapshot:{recovery_id}"
                resolved_gap_token_ids.add(gap.token_id)
        now = self.now_provider()
        existing_coverage = {
            gap.token_id for gap in self.gaps
            if not gap.resolved and gap.code == "coverage_gap"
        }
        new_coverage_gaps = []
        for token_id in sorted(missing - existing_coverage - set(invalid)):
            gap = GapEntry(
                code="coverage_gap", token_id=token_id,
                observed=recovery_id, detected_at=now,
            )
            self.gaps.append(gap)
            new_coverage_gaps.append(gap)
        self.active_recovery_id = None
        coverage = {
            "recovery_id": recovery_id,
            "requested_token_count": len(tracker["expected"]),
            "recovered_token_count": len(recovered),
            "recovered_token_sha256": content_sha256(sorted(recovered)),
            "missing_token_count": len(missing),
            "missing_token_sha256": content_sha256(sorted(missing)),
            "invalid_book_token_count": len(invalid),
            "invalid_book_token_sha256": content_sha256(sorted(invalid)),
            "invalid_book_reason_sha256": content_sha256(invalid),
            "duplicate_response_count": tracker["duplicate_response_count"],
            "unknown_response_count": tracker["unknown_response_count"],
            "coverage_complete": not missing,
        }
        if (
            publish_completion_event
            or resolved_gap_token_ids
            or new_coverage_gaps
            or invalid
        ):
            self._emit("recovery_completed", {
                **coverage,
                "resolved_gap_token_ids": sorted(resolved_gap_token_ids),
            }, {}, applied=True, gaps=new_coverage_gaps)
        if write_checkpoint:
            self.checkpoint()
        return coverage

    def recover_from_books(
        self, rows: list[dict[str, Any]], recovery_id: str,
    ) -> dict[str, Any]:
        self._ensure_loaded()
        tracker = self.new_book_recovery_tracker()
        self.recover_book_batch(rows, recovery_id, tracker)
        return self.complete_book_recovery(recovery_id, tracker)

    def frame(self, market_id: str, *, now: datetime | None = None) -> MarketFrame:
        self._ensure_loaded()
        market = self.catalog.get(market_id)
        if market is None:
            raise KeyError(market_id)
        books = [self.books[token.token_id] for token in market.identity.outcomes if token.token_id in self.books]
        reasons = []
        if self.active_recovery_id is not None:
            reasons.append("recovery_in_progress")
        if len(books) != 2:
            reasons.append("missing_outcome_book")
        if not market.rules.rules_complete:
            reasons.append("instrument_facts_incomplete")
        if not market.rules.fee_schedule.complete:
            reasons.append("fee_schedule_incomplete")
        if any(not gap.resolved and gap.token_id in {item.token_id for item in market.identity.outcomes} for gap in self.gaps):
            reasons.append("unresolved_gap")
        current = now or self.now_provider()
        if books:
            skew = (max(item.exchange_at for item in books) - min(item.exchange_at for item in books)).total_seconds() * 1000
            if skew > self.max_frame_skew_ms:
                reasons.append("token_frame_skew")
            if any((current - item.received_at).total_seconds() * 1000 > self.stale_after_ms for item in books):
                reasons.append("stale_book")
        relation_ids = [item.relation_id for item in market.relations]
        negative_relations = [
            relation for relation in market.relations
            if relation.relation_type == "standard_negative_risk"
        ]
        relation_pairs = [
            pair for relation in negative_relations for pair in relation.outcome_pairs
        ]
        relation_token_ids = {pair.yes_token_id for pair in relation_pairs}
        if any(not relation.complete for relation in negative_relations):
            reasons.append("negative_risk_relation_incomplete")
        pair_markets = [self.catalog.get(pair.market_id) for pair in relation_pairs]
        if any(item is None for item in pair_markets):
            reasons.append("negative_risk_member_catalog_missing")
        if any(
            item is not None and not item.rules.instrument.complete
            for item in pair_markets
        ):
            reasons.append("negative_risk_member_instrument_facts_incomplete")
        if any(
            item is not None and not item.rules.fee_schedule.complete
            for item in pair_markets
        ):
            reasons.append("negative_risk_member_fee_schedule_incomplete")
        relation_books = [
            self.books[token_id] for token_id in sorted(relation_token_ids)
            if token_id in self.books
        ]
        if relation_token_ids:
            if len(relation_books) != len(relation_token_ids):
                reasons.append("negative_risk_member_missing")
            if relation_books:
                relation_skew = (
                    max(item.exchange_at for item in relation_books)
                    - min(item.exchange_at for item in relation_books)
                ).total_seconds() * 1000
                if relation_skew > self.max_frame_skew_ms:
                    reasons.append("negative_risk_frame_skew")
                if any(
                    (current - item.received_at).total_seconds() * 1000 > self.stale_after_ms
                    for item in relation_books
                ):
                    reasons.append("negative_risk_member_stale")
            if any(
                not gap.resolved and gap.token_id in relation_token_ids
                for gap in self.gaps
            ):
                reasons.append("negative_risk_member_gap")
        return MarketFrame(
            market_id=market_id, condition_id=market.identity.condition_id,
            frame_at=current, cursor=self.cursor,
            status="fail_closed" if reasons else "ready",
            reason_codes=sorted(set(reasons)), tokens=books,
            relation_ids=relation_ids, relation_tokens=relation_books,
            relation_pairs=relation_pairs,
            instrument_revision=market.rules.instrument.revision,
            fee_schedule_id=market.rules.fee_schedule.schedule_id,
        )

    def checkpoint_payload(self) -> LiveCheckpoint:
        self._ensure_loaded()
        state = {
            "cursor": self.cursor, "catalog_revision": self.catalog_revision,
            "books": {key: value.model_dump(mode="json") for key, value in sorted(self.books.items())},
            "unresolved_gaps": [gap.model_dump(mode="json") for gap in self.gaps if not gap.resolved],
        }
        checkpoint = LiveCheckpoint(
            **state, created_at=self.now_provider(), state_sha256=content_sha256(state)
        )
        return checkpoint

    def checkpoint(self) -> LiveCheckpoint:
        self._ensure_loaded()
        checkpoint = self.checkpoint_payload()
        body = canonical_json(checkpoint.model_dump(mode="json"))
        _atomic_write(self.checkpoint_path, body)
        self._checkpoint_file_sha256 = hashlib.sha256(body).hexdigest()
        self._checkpoint_cursor = checkpoint.cursor
        return checkpoint

    def _load_catalog(self, path: Path) -> None:
        with path.open("r", encoding="utf-8") as stream:
            payload = json.load(stream, parse_float=str, parse_int=str)
        normalized = payload.get("normalized_catalog")
        if not isinstance(normalized, dict):
            raise RuntimeError("live catalog lacks normalized JSONL metadata")
        normalized_path = Path(str(normalized.get("path") or "")).resolve()
        if not normalized_path.is_relative_to(self.normalized_catalog_root):
            raise RuntimeError("normalized live catalog escapes local storage root")
        if (
            not normalized_path.exists()
            or _file_sha256(normalized_path) != normalized.get("sha256")
        ):
            raise RuntimeError("normalized live catalog integrity failed")
        markets = []
        with normalized_path.open("r", encoding="utf-8") as stream:
            for line in stream:
                if line.strip():
                    markets.append(LiveMarket.model_validate_json(line))
        if len(markets) != int(normalized.get("market_count") or -1):
            raise RuntimeError("normalized live catalog row count mismatch")
        markets.sort(key=lambda item: item.identity.market_id)
        observed_revision = _market_sequence_sha256(markets)
        if observed_revision != payload.get("catalog_revision"):
            raise RuntimeError("live catalog integrity failed")
        catalog_source = payload.get("catalog_source")
        raw_path = None
        if catalog_source:
            raw_path = Path(catalog_source["raw_path"]).resolve()
            if not raw_path.is_relative_to(self.raw_catalog_root):
                raise RuntimeError("live raw catalog escapes local evidence root")
            if (
                not raw_path.exists()
                or _file_sha256(raw_path) != catalog_source.get("raw_payload_sha256")
            ):
                raise RuntimeError("live raw catalog integrity failed")
        self.catalog = {item.identity.market_id: item for item in markets}
        self.token_to_market = {
            outcome.token_id: market.identity.market_id
            for market in markets if market.active and not market.closed
            for outcome in market.identity.outcomes
        }
        self.catalog_revision = payload.get("catalog_revision")
        self.catalog_source = catalog_source
        self.raw_catalog_path = raw_path
        self._catalog_file_sha256 = _file_sha256(path)

    @staticmethod
    def _validate_checkpoint(body: bytes) -> LiveCheckpoint:
        checkpoint = LiveCheckpoint.model_validate_json(body)
        state = {
            "cursor": checkpoint.cursor,
            "catalog_revision": checkpoint.catalog_revision,
            "books": {
                key: value.model_dump(mode="json")
                for key, value in sorted(checkpoint.books.items())
            },
            "unresolved_gaps": [
                item.model_dump(mode="json") for item in checkpoint.unresolved_gaps
            ],
        }
        if content_sha256(state) != checkpoint.state_sha256:
            raise RuntimeError("live checkpoint integrity failed")
        return checkpoint

    @staticmethod
    def _validate_event(event: LiveEventEnvelope, expected_cursor: int) -> None:
        if event.cursor != expected_cursor:
            raise RuntimeError(
                f"live event cursor discontinuity: expected {expected_cursor}, "
                f"observed {event.cursor}"
            )
        if content_sha256(event.canonical_payload) != event.canonical_payload_sha256:
            raise RuntimeError("live event canonical payload hash mismatch")
        if content_sha256(event.raw_payload) != event.raw_payload_sha256:
            raise RuntimeError("live event raw payload hash mismatch")
        if live_event_identity(event) != event.event_id:
            raise RuntimeError("live event identity mismatch")

    def _apply_replayed_event(self, event: LiveEventEnvelope) -> None:
        existing_gaps = {
            content_sha256(item.model_dump(mode="json")) for item in self.gaps
        }
        for gap in event.gaps:
            identity = content_sha256(gap.model_dump(mode="json"))
            if identity not in existing_gaps:
                self.gaps.append(gap.model_copy(deep=True))
                existing_gaps.add(identity)
        if (
            event.applied and event.token_id
            and event.event_type in {
                "book", "price_change", "best_bid_ask",
                "last_trade_price", "tick_size_change",
            }
        ):
            self.books[event.token_id] = LiveBook.model_validate(
                event.canonical_payload
            )
        if event.event_type == "recovery_completed" and event.applied:
            recovered = set(
                event.canonical_payload.get("resolved_gap_token_ids")
                or event.canonical_payload.get("recovered_token_ids") or []
            )
            recovery_id = str(event.canonical_payload.get("recovery_id") or "")
            for gap in self.gaps:
                if not gap.resolved and gap.token_id in recovered:
                    gap.resolved = True
                    gap.resolution = f"rest_books_snapshot:{recovery_id}"
            self.active_recovery_id = None
        if event.event_type == "recovery_started" and event.applied:
            self.active_recovery_id = str(
                event.canonical_payload.get("recovery_id") or ""
            ) or None
        self.cursor = max(self.cursor, event.cursor)

    def _read_all_events(self) -> tuple[list[LiveEventEnvelope], int]:
        if not self.event_path.exists():
            return [], 0
        events = []
        offset = 0
        expected = 1
        with self.event_path.open("rb") as stream:
            while True:
                start = stream.tell()
                line = stream.readline()
                if not line:
                    offset = stream.tell()
                    break
                if not line.endswith(b"\n"):
                    offset = start
                    break
                try:
                    event = LiveEventEnvelope.model_validate_json(line)
                except ValueError as exc:
                    raise RuntimeError("live event log contains invalid JSON") from exc
                self._validate_event(event, expected)
                events.append(event)
                expected += 1
                offset = stream.tell()
        return events, offset

    def _rebuild_from_checkpoint(
        self,
        checkpoint: LiveCheckpoint | None,
        events: list[LiveEventEnvelope],
    ) -> None:
        checkpoint_cursor = checkpoint.cursor if checkpoint else 0
        if checkpoint_cursor and (
            not events or events[-1].cursor < checkpoint_cursor
        ):
            raise RuntimeError("live checkpoint cursor exceeds verified event log")
        self.books = {}
        self.gaps = []
        self.cursor = 0
        self._checkpoint_cursor = checkpoint_cursor
        self.events.clear()
        self.seen_raw_hashes.clear()
        self._seen_raw_hash_order.clear()
        self.active_recovery_id = None
        for event in events:
            self.events.append(event)
            self._remember_raw_hash(event.raw_payload_sha256)
            if event.cursor <= checkpoint_cursor:
                self._apply_replayed_event(event)
        if checkpoint:
            replayed_books = {
                key: value.model_dump(mode="json")
                for key, value in sorted(self.books.items())
            }
            checkpoint_books = {
                key: value.model_dump(mode="json")
                for key, value in sorted(checkpoint.books.items())
            }
            if replayed_books != checkpoint_books:
                raise RuntimeError("live checkpoint books disagree with event replay")
            replayed_unresolved = sorted(
                content_sha256(item.model_dump(mode="json"))
                for item in self.gaps if not item.resolved
            )
            checkpoint_unresolved = sorted(
                content_sha256(item.model_dump(mode="json"))
                for item in checkpoint.unresolved_gaps
            )
            if replayed_unresolved != checkpoint_unresolved:
                raise RuntimeError("live checkpoint gaps disagree with event replay")
            self.cursor = checkpoint_cursor
        for event in events:
            if event.cursor > checkpoint_cursor:
                self._apply_replayed_event(event)
        self._last_log_cursor = events[-1].cursor if events else 0

    def _recover_unlocked(self) -> None:
        """Rebuild state from durable files while the caller holds _sync_lock."""
        if self.catalog_path.exists():
            self._load_catalog(self.catalog_path)
        checkpoint = None
        if self.checkpoint_path.exists():
            body = self.checkpoint_path.read_bytes()
            checkpoint = self._validate_checkpoint(body)
            self._checkpoint_file_sha256 = hashlib.sha256(body).hexdigest()
        events, offset = self._read_all_events()
        self._rebuild_from_checkpoint(checkpoint, events)
        self._event_offset = offset
        self.state_index.rebuild(
            event_path=self.event_path,
            books=self.books,
            gaps=self.gaps,
            catalog_revision=self.catalog_revision,
            token_to_market=self.token_to_market,
            active_recovery_id=self.active_recovery_id,
        )

    def recover(self) -> None:
        """Explicitly perform the full deterministic recovery once."""
        with self._sync_lock:
            self._recover_unlocked()
            self._recovered = True

    def _ensure_loaded(self) -> None:
        """Preserve legacy stateful behavior without blocking app construction."""
        with self._sync_lock:
            if self._recovered:
                return
            self._recover_unlocked()
            self._recovered = True

    def sync(self) -> None:
        """Tail a single writer's durable files into this read-side state."""
        self._ensure_loaded()
        with self._sync_lock:
            if self.catalog_path.exists():
                digest = _file_sha256(self.catalog_path)
                if digest != self._catalog_file_sha256:
                    self._load_catalog(self.catalog_path)

            checkpoint_changed = False
            checkpoint = None
            if self.checkpoint_path.exists():
                body = self.checkpoint_path.read_bytes()
                digest = hashlib.sha256(body).hexdigest()
                if digest != self._checkpoint_file_sha256:
                    checkpoint = self._validate_checkpoint(body)
                    self._checkpoint_file_sha256 = digest
                    checkpoint_changed = True

            if checkpoint_changed:
                events, offset = self._read_all_events()
                self._rebuild_from_checkpoint(checkpoint, events)
                self._event_offset = offset
                return

            if not self.event_path.exists():
                return
            size = self.event_path.stat().st_size
            if size < self._event_offset:
                raise RuntimeError("live event log was truncated")
            if size == self._event_offset:
                return
            with self.event_path.open("rb") as stream:
                stream.seek(self._event_offset)
                while True:
                    start = stream.tell()
                    line = stream.readline()
                    if not line:
                        self._event_offset = stream.tell()
                        break
                    if not line.endswith(b"\n"):
                        self._event_offset = start
                        break
                    try:
                        event = LiveEventEnvelope.model_validate_json(line)
                    except ValueError as exc:
                        raise RuntimeError(
                            "live event log contains invalid JSON"
                        ) from exc
                    self._validate_event(event, self._last_log_cursor + 1)
                    self.events.append(event)
                    self._remember_raw_hash(event.raw_payload_sha256)
                    self._apply_replayed_event(event)
                    self._last_log_cursor = event.cursor
                    self._event_offset = stream.tell()

    def events_after(self, cursor: int, limit: int) -> tuple[list[LiveEventEnvelope], bool]:
        self._ensure_loaded()
        if self.events and cursor < self.events[0].cursor - 1:
            raise RuntimeError("resume_cursor_expired")
        selected = [event for event in self.events if event.cursor > cursor]
        return selected[:limit], len(selected) > limit

    def health(self) -> LiveHealth:
        self._ensure_loaded()
        active_market_ids = sorted(set(self.token_to_market.values()))
        frames = [self.frame(market_id) for market_id in active_market_ids]
        latest = max((book.received_at for book in self.books.values()), default=None)
        now = self.now_provider()
        lag = int((now - latest).total_seconds() * 1000) if latest else None
        unresolved = sum(not gap.resolved for gap in self.gaps)
        ready = sum(frame.status == "ready" for frame in frames)
        status = (
            "empty" if not active_market_ids
            else "ready" if ready == len(frames)
            else "degraded"
        )
        return LiveHealth(
            status=status, catalog_revision=self.catalog_revision,
            active_market_count=len(active_market_ids),
            subscribed_token_count=len(self.token_to_market),
            book_token_count=len(self.books),
            missing_book_token_count=max(0, len(self.token_to_market) - len(self.books)),
            ready_market_count=ready,
            unresolved_gap_count=unresolved, latest_cursor=self.cursor,
            latest_received_at=latest, lag_ms=lag,
        )


def build_live_state_index(root: Path) -> dict[str, Any]:
    """Single-pass replay of durable files into an atomically published index."""
    root = root.resolve()
    reader = PolymarketLiveReadStore(root)
    payload, normalized_path, catalog_index_path, catalog_metadata = (
        reader._manifest_binding()
    )
    normalized = payload["normalized_catalog"]
    if _file_sha256(normalized_path) != normalized.get("sha256"):
        raise RuntimeError("normalized live catalog integrity failed")
    catalog_source = payload.get("catalog_source")
    if catalog_source:
        raw_path = Path(str(catalog_source.get("raw_path") or "")).resolve()
        raw_root = (root / "raw" / "gamma-catalog").resolve()
        if not raw_path.is_relative_to(raw_root):
            raise RuntimeError("live raw catalog escapes local evidence root")
        if (
            not raw_path.is_file()
            or _file_sha256(raw_path)
            != catalog_source.get("raw_payload_sha256")
        ):
            raise RuntimeError("live raw catalog integrity failed")

    try:
        with _readonly_sqlite(catalog_index_path) as connection:
            token_to_market = {
                str(row[0]): str(row[1])
                for row in connection.execute(
                    """SELECT tokens.token_id, tokens.market_id
                       FROM tokens JOIN markets USING(market_id)
                       WHERE markets.active=1 AND markets.closed=0"""
                )
            }
    except sqlite3.DatabaseError as exc:
        raise RuntimeError("live catalog token index is unreadable") from exc

    checkpoint = None
    checkpoint_sha256 = ""
    checkpoint_path = root / "checkpoint.json"
    if checkpoint_path.exists():
        body = checkpoint_path.read_bytes()
        checkpoint = LiveStateStore._validate_checkpoint(body)
        checkpoint_sha256 = hashlib.sha256(body).hexdigest()

    state_index = LiveStateIndex(root)
    state_index.rebuild_from_durable(
        event_path=root / "events.jsonl",
        checkpoint=checkpoint,
        checkpoint_sha256=checkpoint_sha256,
        catalog_revision=catalog_metadata["catalog_revision"],
        token_to_market=token_to_market,
    )
    path, metadata = reader._state_binding()
    with _readonly_state_sqlite(path) as connection:
        counts = {
            name: int(connection.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0])
            for name in ("books", "gaps", "event_offsets")
        }
    return {
        "schema_version": metadata["schema_version"],
        "catalog_revision": metadata["catalog_revision"],
        "latest_cursor": int(metadata["latest_cursor"]),
        "event_log_size": int(metadata["event_log_size"]),
        "path": str(path),
        **{f"{name}_count": count for name, count in counts.items()},
    }


def catch_up_live_state_index(root: Path) -> dict[str, int]:
    """Replay a crash-truncated event-log tail into the mutable state index."""
    root = root.resolve()
    event_path = root / "events.jsonl"
    state_index = LiveStateIndex(root)
    if not state_index.path.is_file() or not event_path.is_file():
        return {"previous_cursor": 0, "latest_cursor": 0, "replayed_events": 0}
    with _publication_lock(root, exclusive=True):
        with sqlite3.connect(state_index.path) as connection:
            metadata = state_index._metadata(connection)
        previous_cursor = int(metadata.get("latest_cursor", "0"))
        offset = int(metadata.get("event_log_size", "0"))
        actual_size = event_path.stat().st_size
        if offset > actual_size:
            raise RuntimeError("live state index is ahead of the durable event log")
        if offset == actual_size:
            return {
                "previous_cursor": previous_cursor,
                "latest_cursor": previous_cursor,
                "replayed_events": 0,
            }
        catalog_revision = metadata.get("catalog_revision") or None
        active_recovery_id = metadata.get("active_recovery_id") or None
        expected_cursor = previous_cursor + 1
        replayed = 0
        with event_path.open("rb") as stream:
            stream.seek(offset)
            while offset < actual_size:
                line = stream.readline()
                if not line.endswith(b"\n"):
                    raise RuntimeError("durable live event tail is incomplete")
                event = LiveEventEnvelope.model_validate_json(line)
                LiveStateStore._validate_event(event, expected_cursor)
                if event.event_type == "catalog_revision" and event.applied:
                    catalog_revision = str(
                        event.canonical_payload.get("catalog_revision") or ""
                    ) or catalog_revision
                if event.event_type == "recovery_started" and event.applied:
                    active_recovery_id = str(
                        event.canonical_payload.get("recovery_id") or ""
                    ) or None
                elif event.event_type == "recovery_completed" and event.applied:
                    active_recovery_id = None
                book = None
                if (
                    event.applied and event.token_id
                    and event.event_type in {
                        "book", "price_change", "best_bid_ask",
                        "last_trade_price", "tick_size_change",
                    }
                ):
                    book = LiveBook.model_validate(event.canonical_payload)
                next_offset = offset + len(line)
                token_to_market = {
                    gap.token_id: event.market_id
                    for gap in event.gaps
                    if gap.token_id and event.market_id
                }
                state_index.append(
                    event,
                    byte_offset=offset,
                    byte_length=len(line),
                    line_sha256=hashlib.sha256(line).hexdigest(),
                    book=book,
                    gaps=event.gaps,
                    catalog_revision=catalog_revision,
                    event_log_size=next_offset,
                    token_to_market=token_to_market,
                    active_recovery_id=active_recovery_id,
                )
                offset = next_offset
                expected_cursor += 1
                replayed += 1
        if offset != actual_size:
            raise RuntimeError("durable live event size changed during tail recovery")
        return {
            "previous_cursor": previous_cursor,
            "latest_cursor": expected_cursor - 1,
            "replayed_events": replayed,
        }


def load_scoped_live_store(
    root: Path,
    market_ids: Iterable[str],
    *,
    now_provider: Callable[[], datetime] = utc_now,
) -> LiveStateStore:
    """Hydrate a bounded writer from published indexes without full recovery."""
    root = root.resolve()
    LiveStateIndex(root).ensure_runtime_schema()
    recovery = catch_up_live_state_index(root)
    if recovery["replayed_events"]:
        LOGGER.warning("live_state_index_tail_recovered %s", recovery)
    reader = PolymarketLiveReadStore(root, now_provider=now_provider)
    selected_ids = reader._scope(market_ids)
    bootstrap = reader.bootstrap(selected_ids)
    snapshot = reader.snapshot(selected_ids)
    gaps = reader.gaps(selected_ids, unresolved_only=True)
    _, state_metadata = reader._state_binding()

    # Scoped collectors publish through the durable event/state indexes; they
    # do not serve in-process replay. Keep only a small duplicate-detection
    # horizon instead of retaining up to 100k full-depth book events.
    store = LiveStateStore(root, replay_capacity=2_000, now_provider=now_provider)
    store.catalog = {
        market.identity.market_id: market for market in bootstrap.markets
    }
    store.token_to_market = {
        outcome.token_id: market.identity.market_id
        for market in bootstrap.markets if market.active and not market.closed
        for outcome in market.identity.outcomes
    }
    store.books = {
        book.token_id: book
        for frame in snapshot.items
        for book in frame.tokens
    }
    store.gaps = [gap.model_copy(deep=True) for gap in gaps.items]
    store.cursor = int(state_metadata["latest_cursor"])
    store.catalog_revision = bootstrap.catalog_revision
    store.catalog_source = bootstrap.catalog_source
    store.active_recovery_id = state_metadata.get("active_recovery_id") or None
    store._event_offset = int(state_metadata["event_log_size"])
    store._last_log_cursor = store.cursor
    store._catalog_file_sha256 = _file_sha256(store.catalog_path)
    checkpoint_path = root / "checkpoint.json"
    if checkpoint_path.exists():
        store._checkpoint_file_sha256 = _file_sha256(checkpoint_path)
    store._recovered = True
    return store


class DataApiPublicNormalizer:
    """Decimal-safe public wallet facts; never claims orders or real-world identity."""

    @staticmethod
    def normalize(kind: str, rows: list[dict[str, Any]], observed_at: datetime) -> list[dict[str, Any]]:
        if kind not in PUBLIC_DATA_KINDS:
            raise ValueError("unsupported Data API public fact kind")
        normalized = []
        expanded = []
        for row in rows:
            if kind in {"positions", "holders"} and (row.get(kind) or row.get(kind.rstrip("s"))):
                children = row.get(kind) or row.get(kind.rstrip("s")) or []
                for child in children:
                    expanded.append({**child, "token": row.get("token")})
            else:
                expanded.append(row)
        decimal_fields = {
            "size", "price", "amount", "avgPrice", "currPrice", "curPrice",
            "currentValue", "cashPnl", "totalBought", "realizedPnl", "totalPnl", "usdcSize",
        }
        for row in expanded:
            values = {}
            for name in decimal_fields:
                if row.get(name) is not None:
                    values[name] = decimal_text(row[name], f"data_api.{name}")
            profile = {
                key: row.get(key) for key in (
                    "name", "pseudonym", "bio", "profileImage", "profileImageOptimized",
                    "displayUsernamePublic", "verified",
                ) if row.get(key) is not None
            }
            canonical = {
                "schema_version": "marketcow.polymarket.public-fact.v1",
                "fact_type": kind.rstrip("s"),
                "wallet": str(row.get("proxyWallet") or row.get("wallet") or "").lower(),
                "condition_id": str(row.get("conditionId") or row.get("condition_id") or ""),
                "token_id": str(row.get("asset") or row.get("token") or ""),
                "outcome": str(row.get("outcome") or ""),
                "side": str(row.get("side") or "").upper() or None,
                "timestamp": (
                    _instant(row["timestamp"]).isoformat() if row.get("timestamp") is not None else None
                ),
                "transaction_hash": str(row.get("transactionHash") or "").lower() or None,
                "decimal_values": values,
                "public_profile_provenance": profile,
                "source": "polymarket_data_api",
                "observed_at": observed_at.isoformat(),
                "semantic_boundary": (
                    "public trade/activity/position/holder fact; not an unfilled-order owner "
                    "and not a verified real-world identity"
                ),
            }
            normalized.append({
                "contract_version": CONTRACT_VERSION,
                "schema_version": "marketcow.polymarket.public-fact-envelope.v1",
                "fact_id": content_sha256({
                    "kind": kind, "canonical_payload": canonical,
                    "raw_payload_sha256": content_sha256(row),
                }),
                "canonical_payload": canonical,
                "canonical_payload_sha256": content_sha256(canonical),
                "raw_payload": row,
                "raw_payload_sha256": content_sha256(row),
            })
        return normalized


class DataApiPublicClient:
    base_url = "https://data-api.polymarket.com"

    def __init__(self, *, requester: Callable[..., Any] = requests.get, timeout: float = 20):
        self.requester = requester
        self.timeout = timeout

    def fetch(self, kind: str, *, params: dict[str, Any]) -> list[dict[str, Any]]:
        if kind not in PUBLIC_DATA_KINDS:
            raise ValueError("unsupported Data API endpoint")
        endpoint = "v1/market-positions" if kind == "positions" and params.get("market") else kind
        response = self.requester(
            f"{self.base_url}/{endpoint}", params=params, timeout=self.timeout,
            headers={"Accept": "application/json", "User-Agent": "MarketCow/0.2"},
        )
        response.raise_for_status()
        payload = json.loads(response.text, parse_float=str, parse_int=str)
        if not isinstance(payload, list):
            raise RuntimeError("Data API public response must be a list")
        return payload


class PolymarketLiveCollector:
    """Official public collector with sharding, heartbeats, and snapshot recovery."""

    endpoint = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

    def __init__(
        self,
        store: LiveStateStore,
        catalog: GammaKeysetCatalog,
        books: ClobBooksClient,
        *,
        connector: Callable[..., Any] = websockets.connect,
        shard_size: int = 500,
        max_websocket_connections: int = 32,
        heartbeat_seconds: float = 10,
        reconnect_seconds: float = 1,
        snapshot_refresh_seconds: float | None = None,
        catalog_refresh_on_lifecycle_events: bool = True,
        publish_checkpoints: bool = True,
        minimum_snapshot_refresh_age_seconds: float = 0,
        max_concurrent_snapshot_refreshes: int = 1,
    ):
        self.store = store
        self.catalog_client = catalog
        self.books_client = books
        self.connector = connector
        self.planner = SubscriptionPlanner(shard_size)
        self.max_websocket_connections = max(1, max_websocket_connections)
        self.heartbeat_seconds = max(0.01, heartbeat_seconds)
        self.reconnect_seconds = max(0.0, reconnect_seconds)
        self.snapshot_refresh_seconds = (
            max(0.1, snapshot_refresh_seconds)
            if snapshot_refresh_seconds is not None else None
        )
        self.catalog_refresh_on_lifecycle_events = (
            catalog_refresh_on_lifecycle_events
        )
        self.publish_checkpoints = publish_checkpoints
        self.minimum_snapshot_refresh_age_seconds = max(
            0, minimum_snapshot_refresh_age_seconds,
        )
        self.max_concurrent_snapshot_refreshes = max(
            1, max_concurrent_snapshot_refreshes,
        )
        self.sockets: list[Any] = []
        self.socket_tokens: dict[Any, set[str]] = {}

    def refresh_catalog(self) -> dict[str, Any]:
        rows, evidence = self.catalog_client.fetch_all()
        publish_started = time.monotonic()
        try:
            markets = GammaLiveNormalizer.normalize(rows, self.store.now_provider())
            update = self.store.replace_catalog(markets, rows)
            rows.mark_published()
        finally:
            rows.cleanup()
        instrument_interval_invalid = sum(
            "activation_expiration_interval" in market.rules.instrument.missing_fields
            for market in markets
        )
        fee_interval_invalid = sum(
            "effective_interval" in market.rules.fee_schedule.missing_fields
            for market in markets
        )
        result = {
            **evidence, **update,
            "normalized_market_count": len(markets),
            "active_token_count": len(self.store.token_to_market),
            "instrument_interval_invalid_count": instrument_interval_invalid,
            "fee_interval_invalid_count": fee_interval_invalid,
            "publish_elapsed_seconds": time.monotonic() - publish_started,
        }
        LOGGER.info("gamma_catalog_published %s", result)
        return result

    async def bootstrap_books(self, reason: str = "startup") -> str:
        with self.store._sync_lock:
            recovery_id = self.store.mark_recovery_started(reason)
            tracker = self.store.new_book_recovery_tracker()

        def consume(rows: list[dict[str, Any]]) -> None:
            received_at = self.store.now_provider()
            with (
                self.store._sync_lock,
                _publication_lock(self.store.root, exclusive=True),
                self.store.state_index.batch(),
                self.store.durable_event_batch(),
            ):
                self.store.recover_book_batch(
                    rows, recovery_id, tracker, received_at=received_at,
                )

        await asyncio.to_thread(
            self.books_client.fetch_stream,
            sorted(self.store.token_to_market),
            batch_consumer=consume,
        )
        with self.store._sync_lock:
            coverage = self.store.complete_book_recovery(
                recovery_id,
                tracker,
                write_checkpoint=self.publish_checkpoints,
            )
        if self.books_client.last_evidence is not None:
            self.books_client.last_evidence.update(coverage)
        LOGGER.info(
            "clob_books_recovery_complete recovery_id=%s evidence=%s health=%s",
            recovery_id, self.books_client.last_evidence,
            self.store.health().model_dump(mode="json"),
        )
        return recovery_id

    async def refresh_books(self, reason: str = "periodic_snapshot_refresh") -> str:
        """Refresh a healthy scope without exposing recovery-in-progress state."""
        with self.store._sync_lock:
            refresh_started_at = self.store.now_provider()
            recovery_id = content_sha256({
                "reason": reason,
                "cursor": self.store.cursor,
                "observed_at": refresh_started_at.isoformat(),
            })
            tracker = self.store.new_book_recovery_tracker()
            minimum_age = self.minimum_snapshot_refresh_age_seconds
            refresh_market_ids = {
                market_id
                for token_id, market_id in self.store.token_to_market.items()
                if token_id not in self.store.books
                or minimum_age <= 0
                or (
                    refresh_started_at - self.store.books[token_id].received_at
                ).total_seconds() >= minimum_age
            }
            refresh_token_ids = sorted(
                token_id
                for token_id, market_id in self.store.token_to_market.items()
                if market_id in refresh_market_ids
            )
            tracker["recovered"].update(
                tracker["expected"] - set(refresh_token_ids)
            )

        def consume(rows: list[dict[str, Any]]) -> None:
            received_at = self.store.now_provider()
            with (
                self.store._sync_lock,
                _publication_lock(self.store.root, exclusive=True),
                self.store.state_index.batch(),
                self.store.durable_event_batch(),
            ):
                self.store.recover_book_batch(
                    rows,
                    recovery_id,
                    tracker,
                    refresh_started_at=refresh_started_at,
                    received_at=received_at,
                )

        if refresh_token_ids:
            await asyncio.to_thread(
                self.books_client.fetch_stream,
                refresh_token_ids,
                batch_consumer=consume,
            )
        with self.store._sync_lock:
            self.store.complete_book_recovery(
                recovery_id,
                tracker,
                write_checkpoint=self.publish_checkpoints,
                publish_completion_event=False,
            )
        return recovery_id

    async def update_subscriptions(self) -> list[dict[str, Any]]:
        desired = set(self.store.token_to_market)
        messages = self.planner.update_messages(desired)
        if not self.sockets:
            return messages
        removed = set().union(*(self.socket_tokens.values() or [set()])) - desired
        for socket in self.sockets:
            owned_removed = sorted(self.socket_tokens.get(socket, set()) & removed)
            for shard in self.planner.shards(owned_removed):
                await socket.send(json.dumps({
                    "assets_ids": shard, "operation": "unsubscribe",
                }, separators=(",", ":")))
            self.socket_tokens.setdefault(socket, set()).difference_update(owned_removed)
        owned = set().union(*(self.socket_tokens.values() or [set()]))
        additions: dict[Any, list[str]] = defaultdict(list)
        for token_id in sorted(desired - owned):
            socket = min(
                self.sockets,
                key=lambda item: len(self.socket_tokens.get(item, set()))
                + len(additions[item]),
            )
            additions[socket].append(token_id)
        for socket, token_ids in additions.items():
            for shard in self.planner.shards(token_ids):
                await socket.send(json.dumps({
                    "assets_ids": shard, "operation": "subscribe",
                    "custom_feature_enabled": True,
                }, separators=(",", ":")))
            self.socket_tokens.setdefault(socket, set()).update(token_ids)
        if messages:
            self.store._emit(
                "subscription_change", {"messages": messages}, {}, applied=True
            )
        return messages

    async def _consume(self, tokens: list[str], *, message_limit: int | None = None) -> None:
        async with self.connector(self.endpoint) as socket:
            self.sockets.append(socket)
            self.socket_tokens[socket] = set(tokens)
            try:
                shards = self.planner.shards(tokens)
                await socket.send(json.dumps({
                    "assets_ids": shards[0], "type": "market",
                    "custom_feature_enabled": True,
                }, separators=(",", ":")))
                for shard in shards[1:]:
                    await socket.send(json.dumps({
                        "assets_ids": shard, "operation": "subscribe",
                        "custom_feature_enabled": True,
                    }, separators=(",", ":")))
                consumed = 0
                while message_limit is None or consumed < message_limit:
                    try:
                        message = await asyncio.wait_for(
                            socket.recv(), timeout=self.heartbeat_seconds
                        )
                    except asyncio.TimeoutError:
                        await socket.send("PING")
                        continue
                    if message == "PONG":
                        continue
                    payload = json.loads(message, parse_float=str, parse_int=str)
                    for item in payload if isinstance(payload, list) else [payload]:
                        await asyncio.to_thread(self._apply_websocket, item)
                        consumed += 1
                        if (
                            self.catalog_refresh_on_lifecycle_events
                            and str(item.get("event_type") or item.get("type") or "")
                            in {"new_market", "market_resolved"}
                        ):
                            await asyncio.to_thread(self.refresh_catalog)
                            await self.update_subscriptions()
            finally:
                self.sockets.remove(socket)
                self.socket_tokens.pop(socket, None)

    def _apply_websocket(self, item: dict[str, Any]) -> None:
        with (
            self.store._sync_lock,
            _publication_lock(self.store.root, exclusive=True),
            self.store.state_index.batch(),
            self.store.durable_event_batch(),
        ):
            self.store.apply_websocket(
                item,
                stale_events_are_resolved=(
                    self.snapshot_refresh_seconds is not None
                ),
            )

    async def run_once(self, *, message_limit: int | None = None) -> None:
        groups = self.planner.connection_groups(
            self.store.token_to_market, self.max_websocket_connections,
        )
        if not groups:
            raise RuntimeError("live collector has no active tokens")
        await asyncio.gather(*(
            self._consume(group, message_limit=message_limit) for group in groups
        ))

    async def _refresh_snapshots_periodically(self) -> None:
        if self.snapshot_refresh_seconds is None:
            return

        async def worker(initial_delay: float) -> None:
            await asyncio.sleep(self.snapshot_refresh_seconds + initial_delay)
            while True:
                try:
                    await self.refresh_books("periodic_snapshot_refresh")
                except asyncio.CancelledError:
                    raise
                except Exception:
                    LOGGER.exception(
                        "periodic_snapshot_refresh_failed; retrying after %.3f seconds",
                        self.snapshot_refresh_seconds,
                    )
                await asyncio.sleep(self.snapshot_refresh_seconds)

        await asyncio.gather(*(
            worker(
                index * self.snapshot_refresh_seconds
                / self.max_concurrent_snapshot_refreshes
            )
            for index in range(self.max_concurrent_snapshot_refreshes)
        ))

    async def run(self, *, max_connections: int | None = None) -> None:
        refresh_task = (
            asyncio.create_task(self._refresh_snapshots_periodically())
            if self.snapshot_refresh_seconds is not None else None
        )
        attempts = 0
        try:
            while max_connections is None or attempts < max_connections:
                attempts += 1
                if attempts > 1:
                    await self.bootstrap_books(f"websocket_reconnect:{attempts}")
                try:
                    await self.run_once()
                    return
                except asyncio.CancelledError:
                    raise
                except Exception:
                    if max_connections is not None and attempts >= max_connections:
                        raise
                    await asyncio.sleep(self.reconnect_seconds)
        finally:
            if refresh_task is not None:
                refresh_task.cancel()
                try:
                    await refresh_task
                except asyncio.CancelledError:
                    pass


def atomic_write_public_facts(root: Path, kind: str, rows: list[dict[str, Any]]) -> Path:
    if kind not in PUBLIC_DATA_KINDS:
        raise ValueError("unsupported public fact kind")
    root = root.resolve()
    folder = root / "public-data" / kind
    folder.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(prefix=".partial-", dir=folder)
    path = folder / f"{content_sha256(rows)}.json"
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(canonical_json(rows))
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)
    return path
