from __future__ import annotations

import hashlib
import json
import os
import tempfile
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Protocol
from urllib.parse import urlparse

import requests

from .polymarket_contracts import SourceRevision, canonical_json
from .polymarket_contracts import (
    MarketLifecycleRevision,
    OutcomeToken,
    PredictionMarketIdentity,
    content_sha256,
    decimal_text,
)


FREE_OFFICIAL_HOSTS = frozenset({
    "gamma-api.polymarket.com",
    "clob.polymarket.com",
    "data-api.polymarket.com",
    "api.goldsky.com",
    "gateway.thegraph.com",
    "polygon-rpc.com",
})
FREE_DATASET_HOSTS = frozenset({"huggingface.co", "hf.co"})
ALLOWED_DATASET_LICENSES = frozenset({
    "apache-2.0", "mit", "cc-by-4.0", "cc0-1.0", "odc-by-1.0",
})
PROHIBITED_SOURCE_NAMES = frozenset({
    "pmdata", "dome", "polymarketdata", "pmdata.dev", "domeapi",
})
SUPPORTED_FREE_DATASETS = {
    "kinzikdza/polymarket-updown-microstructure": "book_trade_resolution",
    "Alezanello/polymarket-arena-capture": "snapshot_trade_reference",
    "moose-code/polymarket-onchain-v1": "onchain_truth",
    "od2961/polymarket-full-market-dataset": "catalog_daily_only",
}


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _atomic_write(path: Path, body: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(prefix=".partial-", dir=path.parent)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(body)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def _sha256(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


@dataclass(frozen=True)
class SourceRequest:
    dataset_key: str
    source: str
    source_url: str
    revision: str
    parameters: dict[str, Any] = field(default_factory=dict)
    required_start: str | None = None
    required_end: str | None = None
    license: str | None = None
    expected_sha256: str | None = None

    @property
    def cache_key(self) -> str:
        return _sha256(canonical_json({
            "dataset_key": self.dataset_key,
            "source": self.source,
            "source_url": self.source_url,
            "revision": self.revision,
            "parameters": self.parameters,
            "required_start": self.required_start,
            "required_end": self.required_end,
            "license": self.license,
        }))


@dataclass(frozen=True)
class FetchedPayload:
    body: bytes
    observed_at: datetime
    coverage_start: str | None = None
    coverage_end: str | None = None


@dataclass(frozen=True)
class CachedPayload:
    body: bytes
    revision: SourceRevision
    local_hit: bool

    def json(self) -> Any:
        return json.loads(self.body, parse_float=str, parse_int=str)


class SourceAdapter(Protocol):
    def fetch(self, request: SourceRequest) -> FetchedPayload: ...


class OfficialJsonAdapter:
    """Free official JSON API adapter with an explicit host allowlist."""

    def __init__(
        self,
        source: str,
        *,
        timeout: float = 20,
        requester: Callable[..., Any] | None = None,
    ):
        if source not in {
            "polymarket_gamma", "polymarket_clob", "polymarket_data_api",
            "polymarket_subgraph", "polygon_logs",
        }:
            raise ValueError("unsupported official Polymarket source")
        self.source = source
        self.timeout = timeout
        self.requester = requester or requests.get

    def fetch(self, request: SourceRequest) -> FetchedPayload:
        if request.source != self.source:
            raise ValueError("source request does not match adapter")
        host = (urlparse(request.source_url).hostname or "").lower()
        if host not in FREE_OFFICIAL_HOSTS:
            raise ValueError("official source host is not allowlisted")
        request_options = {
            "timeout": self.timeout,
            "headers": {"Accept": "application/json", "User-Agent": "MarketCow/0.2"},
        }
        if self.source in {"polymarket_subgraph", "polygon_logs"}:
            requester = self.requester if self.requester is not requests.get else requests.post
            response = requester(
                request.source_url, json=request.parameters, **request_options
            )
        else:
            response = self.requester(
                request.source_url, params=request.parameters, **request_options
            )
        response.raise_for_status()
        payload = json.loads(response.text, parse_float=str, parse_int=str)
        body = canonical_json(payload)
        observed = getattr(response, "headers", {}).get("Date")
        observed_at = utc_now()
        if observed:
            try:
                from email.utils import parsedate_to_datetime

                observed_at = parsedate_to_datetime(observed).astimezone(timezone.utc)
            except (TypeError, ValueError, OverflowError):
                pass
        return FetchedPayload(body=body, observed_at=observed_at)


class ClobPriceHistoryAdapter(OfficialJsonAdapter):
    """Official low-frequency price history; never labels points as L2."""

    def __init__(self, **kwargs: Any):
        super().__init__("polymarket_clob", **kwargs)

    def fetch(self, request: SourceRequest) -> FetchedPayload:
        if not request.required_start or not request.required_end:
            raise ValueError("CLOB price history requires an explicit coverage window")
        token_ids = request.parameters.get("token_ids") or []
        if not 1 <= len(token_ids) <= 20:
            raise ValueError("CLOB price history supports 1-20 token IDs")
        fetched = super().fetch(request)
        payload = json.loads(fetched.body, parse_float=str, parse_int=str)
        points = []
        for item in payload if isinstance(payload, list) else payload.values():
            if isinstance(item, dict):
                points.extend(item.get("history") or item.get("points") or [])
            elif isinstance(item, list):
                points.extend(item)
        timestamps = sorted(
            _timestamp(point.get("t") or point.get("timestamp"))
            for point in points if point.get("t") or point.get("timestamp")
        )
        return FetchedPayload(
            body=fetched.body, observed_at=fetched.observed_at,
            coverage_start=timestamps[0] if timestamps else None,
            coverage_end=timestamps[-1] if timestamps else None,
        )


class DataApiTradesAdapter(OfficialJsonAdapter):
    """Official trade page adapter with explicit pagination/coverage evidence."""

    def __init__(self, **kwargs: Any):
        super().__init__("polymarket_data_api", **kwargs)

    def fetch(self, request: SourceRequest) -> FetchedPayload:
        if not request.required_start or not request.required_end:
            raise ValueError("Data API trades require an explicit coverage window")
        limit = int(request.parameters.get("limit") or 10000)
        offset = int(request.parameters.get("offset") or 0)
        if not 1 <= limit <= 10000 or not 0 <= offset <= 10000:
            raise ValueError("Data API trade page exceeds documented pagination bounds")
        fetched = super().fetch(request)
        rows = json.loads(fetched.body, parse_float=str, parse_int=str)
        if not isinstance(rows, list):
            rows = rows.get("data") or rows.get("trades") or []
        timestamps = sorted(
            _timestamp(row.get("timestamp")) for row in rows if row.get("timestamp")
        )
        if len(rows) == limit and offset + limit >= 10000:
            raise RuntimeError(
                "Data API pagination ceiling reached; narrow the requested market/window"
            )
        return FetchedPayload(
            body=canonical_json(rows), observed_at=fetched.observed_at,
            coverage_start=timestamps[0] if timestamps else None,
            coverage_end=timestamps[-1] if timestamps else None,
        )


class FixedRevisionDatasetAdapter:
    """Downloads only license-approved, immutable public dataset revisions."""

    def __init__(
        self,
        *,
        timeout: float = 120,
        requester: Callable[..., Any] | None = None,
    ):
        self.timeout = timeout
        self.requester = requester or requests.get

    def fetch(self, request: SourceRequest) -> FetchedPayload:
        if request.source != "huggingface_fixed_revision":
            raise ValueError("dataset adapter requires a Hugging Face source")
        if request.dataset_key not in SUPPORTED_FREE_DATASETS:
            raise ValueError("public dataset is not in the reviewed free-source registry")
        host = (urlparse(request.source_url).hostname or "").lower()
        if host not in FREE_DATASET_HOSTS:
            raise ValueError("public dataset host is not allowlisted")
        revision = request.revision.lower()
        if len(revision) != 40 or any(char not in "0123456789abcdef" for char in revision):
            raise ValueError("dataset revision must be a fixed 40-character commit")
        if revision not in request.source_url.lower():
            raise ValueError("dataset URL must embed the fixed revision")
        license_name = str(request.license or "").strip().lower()
        if license_name not in ALLOWED_DATASET_LICENSES:
            raise ValueError("dataset license is missing or not approved")
        expected = str(request.expected_sha256 or "").lower()
        if len(expected) != 64 or any(char not in "0123456789abcdef" for char in expected):
            raise ValueError("dataset expected_sha256 is required")
        response = self.requester(
            request.source_url, timeout=self.timeout,
            headers={"User-Agent": "MarketCow/0.2"},
        )
        response.raise_for_status()
        body = bytes(response.content)
        if _sha256(body) != expected:
            raise ValueError("downloaded dataset hash does not match fixed manifest")
        return FetchedPayload(body=body, observed_at=utc_now())


class LocalFirstRawCache:
    """Content-verified local-first acquisition with atomic immutable writes."""

    def __init__(self, root: Path, now_provider: Callable[[], datetime] = utc_now):
        self.root = root.resolve()
        self.now_provider = now_provider

    def _paths(self, request: SourceRequest) -> tuple[Path, Path]:
        folder = self.root / "requests" / request.cache_key[:2] / request.cache_key
        return folder / "payload.bin", folder / "manifest.json"

    @staticmethod
    def _coverage(request: SourceRequest, fetched: FetchedPayload) -> None:
        if request.required_start and (
            not fetched.coverage_start or fetched.coverage_start > request.required_start
        ):
            raise RuntimeError("source coverage does not reach required_start")
        if request.required_end and (
            not fetched.coverage_end or fetched.coverage_end < request.required_end
        ):
            raise RuntimeError("source coverage does not reach required_end")

    def get(self, request: SourceRequest, adapter: SourceAdapter) -> CachedPayload:
        payload_path, manifest_path = self._paths(request)
        if payload_path.exists() and manifest_path.exists():
            body = payload_path.read_bytes()
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            if _sha256(body) != manifest.get("payload_sha256"):
                raise RuntimeError("local immutable payload hash mismatch")
            return CachedPayload(
                body=body,
                revision=SourceRevision.model_validate(manifest["source_revision"]),
                local_hit=True,
            )

        fetched = adapter.fetch(request)
        self._coverage(request, fetched)
        payload_sha = _sha256(fetched.body)
        if request.expected_sha256 and payload_sha != request.expected_sha256.lower():
            raise RuntimeError("source payload hash does not match request")
        ingested_at = self.now_provider().astimezone(timezone.utc)
        revision = SourceRevision(
            source=request.source,
            revision=request.revision,
            source_url=request.source_url,
            observed_at=fetched.observed_at,
            ingested_at=ingested_at,
            payload_sha256=payload_sha,
            raw_path=str(payload_path),
            license=request.license,
        )
        manifest = {
            "schema": "marketcow.polymarket.raw-cache.v1",
            "request": {
                "dataset_key": request.dataset_key,
                "source": request.source,
                "source_url": request.source_url,
                "revision": request.revision,
                "parameters": request.parameters,
                "required_start": request.required_start,
                "required_end": request.required_end,
            },
            "source_revision": revision.model_dump(mode="json"),
            "payload_sha256": payload_sha,
            "byte_size": len(fetched.body),
            "coverage_start": fetched.coverage_start,
            "coverage_end": fetched.coverage_end,
        }
        _atomic_write(payload_path, fetched.body)
        _atomic_write(manifest_path, canonical_json(manifest))
        return CachedPayload(body=fetched.body, revision=revision, local_hit=False)


def assert_free_source_policy(name: str, url: str = "") -> None:
    normalized = name.strip().lower()
    host = (urlparse(url).hostname or "").lower()
    if any(blocked in normalized or blocked in host for blocked in PROHIBITED_SOURCE_NAMES):
        raise ValueError("paid/trial Polymarket sources are prohibited")


def _list_field(value: Any) -> list[str]:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError:
            value = [item.strip() for item in value.split(",")]
    return [str(item) for item in (value or [])]


class GammaCatalogNormalizer:
    """Converts archived Gamma rows into canonical identity and SCD revisions."""

    @staticmethod
    def normalize(
        rows: list[dict[str, Any]], source: SourceRevision
    ) -> tuple[list[PredictionMarketIdentity], list[MarketLifecycleRevision]]:
        identities, revisions = [], []
        for row in rows:
            tokens = _list_field(row.get("clobTokenIds") or row.get("clob_token_ids"))
            outcomes = _list_field(row.get("outcomes"))
            if len(tokens) != 2 or len(outcomes) != 2:
                continue
            market_id = str(row.get("id") or row.get("market_id") or "")
            condition_id = str(row.get("conditionId") or row.get("condition_id") or "")
            event_id = str(
                row.get("event_id")
                or ((row.get("events") or [{}])[0]).get("id") or market_id
            )
            identity = PredictionMarketIdentity(
                event_id=event_id,
                market_id=market_id,
                condition_id=condition_id,
                slug=str(row.get("slug") or market_id),
                outcomes=[
                    OutcomeToken(
                        token_id=token_id, outcome=outcome,
                        instrument_id=f"POLY:{condition_id}:{token_id}",
                    )
                    for token_id, outcome in zip(tokens, outcomes)
                ],
                neg_risk=bool(row.get("negRisk") or row.get("neg_risk")),
                neg_risk_market_id=(
                    str(row.get("negRiskMarketID") or row.get("neg_risk_market_id"))
                    or None
                ),
            )
            closed = bool(row.get("closed"))
            resolved = row.get("resolution") not in {None, ""}
            state = "resolved" if resolved else "closed" if closed else "active"
            fee = row.get("fees") or row.get("fee_schedule") or {}
            tick_size = row.get("minimumTickSize") or row.get("tickSize")
            minimum_order_size = row.get("minimumOrderSize")
            if tick_size is None or minimum_order_size is None:
                raise ValueError("Gamma rules require tick and minimum order size")
            fee_enabled = row.get("feesEnabled")
            maker_fee = fee.get("makerFeeBps") or fee.get("maker_fee_bps")
            taker_fee = fee.get("takerFeeBps") or fee.get("taker_fee_bps")
            if maker_fee is None or taker_fee is None:
                if fee_enabled is False:
                    maker_fee = taker_fee = "0"
                else:
                    raise ValueError("Gamma rules require an explicit fee schedule")
            valid_from = source.observed_at
            revision_id = content_sha256({
                "market_id": market_id,
                "payload_sha256": source.payload_sha256,
                "observed_at": source.observed_at.isoformat(),
            })
            revisions.append(MarketLifecycleRevision(
                identity=identity,
                revision_id=revision_id,
                valid_from=valid_from,
                state=state,
                resolution=(str(row.get("resolution")) if resolved else None),
                resolution_source=(
                    str(row.get("resolutionSource") or row.get("resolution_source"))
                    or None
                ),
                tick_size=decimal_text(tick_size, "tick_size", allow_zero=False),
                minimum_order_size=decimal_text(
                    minimum_order_size, "minimum_order_size", allow_zero=False,
                ),
                maker_fee_bps=maker_fee,
                taker_fee_bps=taker_fee,
                source=source,
            ))
            identities.append(identity)
        return identities, revisions


class TradeTruthNormalizer:
    @staticmethod
    def data_api(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        normalized = []
        for row in rows:
            normalized.append({
                "record_type": "trade",
                "trade_id": str(row.get("id") or content_sha256(row)),
                "condition_id": str(row.get("conditionId") or row.get("condition_id") or ""),
                "token_id": str(row.get("asset") or row.get("token_id") or ""),
                "side": str(row.get("side") or "").lower(),
                "price": decimal_text(row.get("price"), "trade.price"),
                "size": decimal_text(row.get("size"), "trade.size"),
                "timestamp": _timestamp(row.get("timestamp")),
                "transaction_hash": str(
                    row.get("transactionHash") or row.get("transaction_hash") or ""
                ).lower(),
                "source": "polymarket_data_api",
                "raw_payload": row,
            })
        return normalized

    @staticmethod
    def onchain(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        allowed = {"fill", "split", "merge", "convert", "redemption", "resolution"}
        normalized = []
        for row in rows:
            record_type = str(row.get("type") or "").lower()
            if record_type not in allowed:
                raise ValueError("unsupported on-chain truth event")
            item = {
                "record_type": record_type,
                "condition_id": str(row.get("condition_id") or ""),
                "token_id": str(row.get("token_id") or ""),
                "timestamp": _timestamp(row.get("timestamp") or row.get("block_time")),
                "transaction_hash": str(row.get("transaction_hash") or "").lower(),
                "log_index": str(row.get("log_index") or "0"),
                "source": str(row.get("source") or "polygon_logs"),
                "raw_payload": row,
            }
            if record_type == "fill":
                item["price"] = decimal_text(row.get("price"), "fill.price")
                item["size"] = decimal_text(row.get("size"), "fill.size")
            normalized.append(item)
        return normalized


def _timestamp(value: Any) -> str:
    if isinstance(value, (int, str)) and str(value).isdigit():
        number = int(value)
        if number > 10**12:
            number //= 1000
        return datetime.fromtimestamp(number, timezone.utc).isoformat()
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("source timestamp must include timezone")
    return parsed.astimezone(timezone.utc).isoformat()
