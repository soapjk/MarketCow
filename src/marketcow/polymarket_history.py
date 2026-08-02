from __future__ import annotations

import asyncio
import copy
import json
import os
import tempfile
from collections import defaultdict
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import websockets

from .polymarket_contracts import (
    CONTRACT_VERSION,
    CertificationCheck,
    CoverageFacts,
    DatasetPart,
    GapEntry,
    MarketBootstrap,
    PredictionMarketBootstrap,
    PredictionMarketIdentity,
    PredictionMarketManifest,
    ReplayContract,
    SourceRevision,
    bootstrap_identity,
    canonical_json,
    content_sha256,
    decimal_text,
    manifest_identity,
)
from .polymarket_sources import _atomic_write, utc_now


def _instant(value: Any) -> datetime:
    parsed = value if isinstance(value, datetime) else datetime.fromisoformat(
        str(value).replace("Z", "+00:00")
    )
    if parsed.tzinfo is None:
        raise ValueError("timestamps must include a timezone")
    return parsed.astimezone(timezone.utc)


def _levels(value: Any, field: str) -> list[dict[str, str]]:
    result = []
    for item in value or []:
        price = decimal_text(item.get("price"), f"{field}.price")
        size = decimal_text(item.get("size"), f"{field}.size")
        if Decimal(price) > 1:
            raise ValueError("prediction-market prices must not exceed 1")
        result.append({"price": price, "size": size})
    return result


def _state_hash(state: dict[str, Any]) -> str:
    return content_sha256({
        "token_id": state["token_id"],
        "tick_size": state["tick_size"],
        "bids": [
            {"price": price, "size": state["bids"][price]}
            for price in sorted(state["bids"], key=Decimal, reverse=True)
        ],
        "asks": [
            {"price": price, "size": state["asks"][price]}
            for price in sorted(state["asks"], key=Decimal)
        ],
    })


def _validate_book(state: dict[str, Any]) -> None:
    tick = Decimal(state["tick_size"])
    if tick <= 0:
        raise ValueError("tick_size must be positive")
    for side in ("bids", "asks"):
        for price_text, size_text in state[side].items():
            price, size = Decimal(price_text), Decimal(size_text)
            if price < 0 or price > 1 or price % tick != 0:
                raise ValueError("book price must be within [0,1] and tick aligned")
            if size < 0:
                raise ValueError("book size must be nonnegative")
    if state["bids"] and state["asks"]:
        if max(map(Decimal, state["bids"])) >= min(map(Decimal, state["asks"])):
            raise ValueError("order book must not be crossed or locked")


class PolymarketWebSocketRecorder:
    """Append-only public WebSocket recorder with deterministic book recovery."""

    def __init__(
        self,
        root: Path,
        identity: PredictionMarketIdentity,
        *,
        checkpoint_every: int = 1000,
        now_provider=utc_now,
    ):
        self.root = root.resolve()
        self.identity = identity
        self.checkpoint_every = max(1, checkpoint_every)
        self.now_provider = now_provider
        self.raw_path = self.root / "raw" / f"{identity.market_id}.jsonl"
        self.gap_path = self.root / "gaps" / f"{identity.market_id}.jsonl"
        self.checkpoint_path = (
            self.root / "checkpoints" / f"{identity.market_id}.json"
        )
        self.states: dict[str, dict[str, Any]] = {}
        self.seen_ids: set[str] = set()
        self.gaps: list[GapEntry] = []
        self.line_count = 0
        self.applied_count = 0
        self._recover()

    @property
    def token_ids(self) -> set[str]:
        return {item.token_id for item in self.identity.outcomes}

    def _append_jsonl(self, path: Path, value: dict[str, Any]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("ab") as stream:
            stream.write(canonical_json(value) + b"\n")
            stream.flush()
            os.fsync(stream.fileno())

    def _record_gap(self, gap: GapEntry) -> None:
        self.gaps.append(gap)
        self._append_jsonl(self.gap_path, gap.model_dump(mode="json"))

    def record_gap(self, gap: GapEntry) -> None:
        self._record_gap(gap)

    def _normalize(self, raw: dict[str, Any]) -> dict[str, Any]:
        event = dict(raw)
        event_type = str(event.get("type") or "").strip()
        if event_type not in {
            "book", "price_change", "last_trade_price", "tick_size_change",
            "new_market", "market_resolved",
        }:
            raise ValueError("unsupported Polymarket WebSocket event")
        token_id = str(event.get("token_id") or event.get("asset_id") or "")
        if event_type not in {"new_market", "market_resolved"}:
            if token_id not in self.token_ids:
                raise ValueError("event token is outside canonical market identity")
        event["type"] = event_type
        event["token_id"] = token_id or None
        event["event_id"] = str(
            event.get("event_id") or content_sha256(raw)
        )
        event["exchange_ts"] = _instant(
            event.get("exchange_ts") or event.get("timestamp")
        ).isoformat()
        event["received_ts"] = _instant(
            event.get("received_ts") or self.now_provider()
        ).isoformat()
        if event.get("sequence") is not None:
            event["sequence"] = int(event["sequence"])
        if event_type == "book":
            event["tick_size"] = decimal_text(
                event.get("tick_size"), "tick_size", allow_zero=False
            )
            event["bids"] = _levels(event.get("bids"), "bids")
            event["asks"] = _levels(event.get("asks"), "asks")
            event["book_epoch"] = str(
                event.get("book_epoch")
                or content_sha256({
                    "market_id": self.identity.market_id,
                    "token_id": token_id,
                    "first_snapshot": event["event_id"],
                })
            )
            event["update_semantics"] = "absolute_size"
            event["tick_version"] = str(
                event.get("tick_version")
                or content_sha256({"tick_size": event["tick_size"]})
            )
            state = {
                "token_id": token_id,
                "tick_size": event["tick_size"],
                "bids": {item["price"]: item["size"] for item in event["bids"]},
                "asks": {item["price"]: item["size"] for item in event["asks"]},
            }
            event["state_checksum"] = str(
                event.get("state_checksum") or _state_hash(state)
            )
        elif event_type == "price_change":
            changes = []
            raw_changes = event.get("changes") or [event]
            for item in raw_changes:
                side = str(item.get("side") or "").lower()
                if side not in {"buy", "sell", "bid", "ask"}:
                    raise ValueError("price change side must be bid/buy or ask/sell")
                changes.append({
                    "side": "bids" if side in {"buy", "bid"} else "asks",
                    "price": decimal_text(item.get("price"), "price"),
                    "size": decimal_text(item.get("size"), "size"),
                })
            event["changes"] = changes
        elif event_type == "last_trade_price":
            event["price"] = decimal_text(event.get("price"), "price")
            event["size"] = decimal_text(event.get("size"), "size")
        elif event_type == "tick_size_change":
            event["tick_size"] = decimal_text(
                event.get("tick_size"), "tick_size", allow_zero=False
            )
        return event

    def _apply(self, event: dict[str, Any], *, record_gaps: bool) -> str:
        token_id = event.get("token_id")
        event_type = event["type"]
        if event_type in {"new_market", "market_resolved", "last_trade_price"}:
            return "applied"
        previous = self.states.get(token_id)
        sequence = event.get("sequence")
        if previous is not None and sequence is not None:
            last_sequence = previous.get("sequence")
            if last_sequence is not None and sequence <= last_sequence:
                if record_gaps:
                    self._record_gap(GapEntry(
                        code="out_of_order", token_id=token_id,
                        expected=str(last_sequence + 1), observed=str(sequence),
                        event_at=_instant(event["exchange_ts"]),
                        detected_at=self.now_provider(),
                    ))
                return "out_of_order"
            if (
                last_sequence is not None and sequence > last_sequence + 1
                and event_type != "book"
            ):
                if record_gaps:
                    self._record_gap(GapEntry(
                        code="sequence_gap", token_id=token_id,
                        expected=str(last_sequence + 1), observed=str(sequence),
                        event_at=_instant(event["exchange_ts"]),
                        detected_at=self.now_provider(),
                    ))
                return "gap"
        if event_type == "book":
            state = {
                "token_id": token_id,
                "tick_size": event["tick_size"],
                "bids": {item["price"]: item["size"] for item in event["bids"]},
                "asks": {item["price"]: item["size"] for item in event["asks"]},
                "sequence": sequence,
                "exchange_ts": event["exchange_ts"],
            }
            _validate_book(state)
            self.states[token_id] = state
            for gap in self.gaps:
                if gap.token_id == token_id and not gap.resolved:
                    gap.resolved = True
                    gap.resolution = f"snapshot_recovery:{event['event_id']}"
        elif previous is None:
            if record_gaps:
                self._record_gap(GapEntry(
                    code="missing_snapshot", token_id=token_id,
                    observed=event["event_id"],
                    event_at=_instant(event["exchange_ts"]),
                    detected_at=self.now_provider(),
                ))
            return "missing_snapshot"
        elif event_type == "price_change":
            for item in event["changes"]:
                book = previous[item["side"]]
                if Decimal(item["size"]) == 0:
                    book.pop(item["price"], None)
                else:
                    book[item["price"]] = item["size"]
            previous["sequence"] = sequence
            previous["exchange_ts"] = event["exchange_ts"]
            _validate_book(previous)
        elif event_type == "tick_size_change":
            previous["tick_size"] = event["tick_size"]
            previous["sequence"] = sequence
            previous["exchange_ts"] = event["exchange_ts"]
            _validate_book(previous)
        if token_id in self.states and event.get("expected_state_hash"):
            observed_hash = _state_hash(self.states[token_id])
            if observed_hash != event["expected_state_hash"]:
                if record_gaps:
                    self._record_gap(GapEntry(
                        code="hash_mismatch", token_id=token_id,
                        expected=event["expected_state_hash"],
                        observed=observed_hash,
                        event_at=_instant(event["exchange_ts"]),
                        detected_at=self.now_provider(),
                    ))
                return "hash_mismatch"
        return "applied"

    def append(self, raw: dict[str, Any]) -> dict[str, Any]:
        event = self._normalize(raw)
        if event["event_id"] in self.seen_ids:
            gap = GapEntry(
                code="duplicate", token_id=event.get("token_id"),
                observed=event["event_id"],
                event_at=_instant(event["exchange_ts"]),
                detected_at=self.now_provider(), resolved=True,
                resolution="ignored_idempotently",
            )
            self._record_gap(gap)
            return {"status": "duplicate", "event": event}
        envelope = {
            "schema": "marketcow.polymarket.websocket-raw.v1",
            "market_id": self.identity.market_id,
            "condition_id": self.identity.condition_id,
            "payload_sha256": content_sha256(raw),
            "event": event,
            "raw_payload": raw,
            "accepted": True,
        }
        state_before = copy.deepcopy(self.states)
        try:
            self._apply(event, record_gaps=False)
        except ValueError as exc:
            self.states = state_before
            envelope["accepted"] = False
            envelope["validation_error"] = str(exc)
            self._append_jsonl(self.raw_path, envelope)
            self.line_count += 1
            self.seen_ids.add(event["event_id"])
            raise
        self.states = state_before
        self._append_jsonl(self.raw_path, envelope)
        self.line_count += 1
        self.seen_ids.add(event["event_id"])
        status = self._apply(event, record_gaps=True)
        if status == "applied":
            self.applied_count += 1
        if self.applied_count and self.applied_count % self.checkpoint_every == 0:
            self.checkpoint()
        return {"status": status, "event": event}

    def checkpoint(self) -> dict[str, Any]:
        payload = {
            "schema": "marketcow.polymarket.checkpoint.v1",
            "market_id": self.identity.market_id,
            "line_count": self.line_count,
            "applied_count": self.applied_count,
            "states": self.states,
            "state_hashes": {
                token_id: _state_hash(state)
                for token_id, state in sorted(self.states.items())
            },
            "created_at": self.now_provider().astimezone(timezone.utc).isoformat(),
        }
        _atomic_write(self.checkpoint_path, canonical_json(payload))
        return payload

    def _recover(self) -> None:
        start_line = 0
        if self.checkpoint_path.exists():
            checkpoint = json.loads(self.checkpoint_path.read_text(encoding="utf-8"))
            self.states = checkpoint.get("states") or {}
            self.line_count = int(checkpoint.get("line_count") or 0)
            self.applied_count = int(checkpoint.get("applied_count") or 0)
            start_line = self.line_count
        if self.gap_path.exists():
            self.gaps = [
                GapEntry.model_validate(json.loads(line))
                for line in self.gap_path.read_text(encoding="utf-8").splitlines()
                if line.strip()
            ]
        if not self.raw_path.exists():
            return
        for index, line in enumerate(
            self.raw_path.read_text(encoding="utf-8").splitlines()
        ):
            envelope = json.loads(line)
            event = envelope["event"]
            self.seen_ids.add(event["event_id"])
            if index >= start_line and envelope.get("accepted", True):
                status = self._apply(event, record_gaps=False)
                if status == "applied":
                    self.applied_count += 1
        self.line_count = len(self.raw_path.read_text(encoding="utf-8").splitlines())

    def events(self) -> list[dict[str, Any]]:
        if not self.raw_path.exists():
            return []
        return [
            json.loads(line) for line in self.raw_path.read_text(
                encoding="utf-8"
            ).splitlines() if line.strip()
        ]


PARQUET_SCHEMA = pa.schema([
    ("contract_version", pa.string()),
    ("record_id", pa.string()),
    ("market_id", pa.string()),
    ("condition_id", pa.string()),
    ("token_id", pa.string()),
    ("record_type", pa.string()),
    ("event_at", pa.timestamp("us", tz="UTC")),
    ("received_at", pa.timestamp("us", tz="UTC")),
    ("book_epoch", pa.string()),
    ("sequence", pa.int64()),
    ("source_sequence", pa.int64()),
    ("update_semantics", pa.string()),
    ("tick_version", pa.string()),
    ("state_checksum", pa.string()),
    ("price", pa.string()),
    ("size", pa.string()),
    ("transaction_hash", pa.string()),
    ("source", pa.string()),
    ("payload_json", pa.string()),
    ("payload_sha256", pa.string()),
])


class PredictionMarketMaterializer:
    def __init__(self, root: Path, now_provider=utc_now):
        self.root = root.resolve()
        self.now_provider = now_provider

    @staticmethod
    def _parquet_row(record: dict[str, Any]) -> dict[str, Any]:
        payload = record.get("event") or record
        raw = record.get("raw_payload") or payload
        return {
            "contract_version": CONTRACT_VERSION,
            "record_id": str(
                payload.get("event_id") or payload.get("trade_id")
                or payload.get("transaction_hash") or content_sha256(payload)
            ),
            "market_id": str(record.get("market_id") or payload.get("market_id") or ""),
            "condition_id": str(record.get("condition_id") or payload.get("condition_id") or ""),
            "token_id": str(payload.get("token_id") or payload.get("asset_id") or ""),
            "record_type": str(payload.get("type") or payload.get("record_type") or ""),
            "event_at": _instant(
                payload.get("exchange_ts") or payload.get("timestamp")
                or payload.get("event_at") or payload.get("observed_at")
            ),
            "received_at": _instant(
                payload.get("received_ts") or payload.get("received_at")
                or payload.get("exchange_ts") or payload.get("timestamp")
                or payload.get("event_at") or payload.get("observed_at")
            ),
            "book_epoch": str(payload.get("book_epoch") or ""),
            "sequence": (
                int(payload["sequence"]) if payload.get("sequence") is not None else None
            ),
            "source_sequence": (
                int(payload["source_sequence"])
                if payload.get("source_sequence") is not None else None
            ),
            "update_semantics": str(payload.get("update_semantics") or ""),
            "tick_version": str(payload.get("tick_version") or ""),
            "state_checksum": str(
                payload.get("state_checksum") or payload.get("expected_state_hash") or ""
            ),
            "price": (
                decimal_text(payload["price"], "price")
                if payload.get("price") is not None else None
            ),
            "size": (
                decimal_text(payload["size"], "size")
                if payload.get("size") is not None else None
            ),
            "transaction_hash": str(payload.get("transaction_hash") or ""),
            "source": str(record.get("source") or payload.get("source") or ""),
            "payload_json": canonical_json(raw).decode("utf-8"),
            "payload_sha256": content_sha256(raw),
        }

    def _write_part(self, table_name: str, records: list[dict[str, Any]]) -> DatasetPart:
        rows = [self._parquet_row(record) for record in records]
        if table_name == "books":
            rows.sort(key=lambda row: (
                row["event_at"], row["received_at"], row["book_epoch"],
                row["sequence"] if row["sequence"] is not None else -1,
                row["record_id"],
            ))
        table = pa.Table.from_pylist(rows, schema=PARQUET_SCHEMA)
        staging = self.root / "staging"
        staging.mkdir(parents=True, exist_ok=True)
        descriptor, temporary = tempfile.mkstemp(prefix=f".{table_name}-", dir=staging)
        os.close(descriptor)
        try:
            pq.write_table(
                table, temporary, compression="zstd", version="2.6",
                use_dictionary=False, write_statistics=True,
            )
            body = Path(temporary).read_bytes()
            digest = content_sha256({"file_sha256": __import__("hashlib").sha256(body).hexdigest()})
            destination = self.root / "parts" / table_name / f"{digest}.parquet"
            destination.parent.mkdir(parents=True, exist_ok=True)
            if destination.exists():
                if destination.read_bytes() != body:
                    raise RuntimeError("immutable Parquet part hash collision")
            else:
                os.replace(temporary, destination)
            times = [row["event_at"] for row in rows]
            return DatasetPart(
                table=table_name, path=str(destination), sha256=__import__("hashlib").sha256(body).hexdigest(),
                rows=len(rows), byte_size=len(body),
                start=min(times) if times else None, end=max(times) if times else None,
            )
        finally:
            if os.path.exists(temporary):
                os.unlink(temporary)

    def write_manifest(self, manifest: PredictionMarketManifest) -> Path:
        path = self.root / "manifests" / f"{manifest.manifest_id}.json"
        body = canonical_json(manifest.model_dump(mode="json"))
        if path.exists() and path.read_bytes() != body:
            raise RuntimeError("immutable manifest conflicts with existing content")
        if not path.exists():
            _atomic_write(path, body)
        return path

    def materialize(
        self,
        dataset_id: str,
        revision: str,
        identities: list[PredictionMarketIdentity],
        sources: list[SourceRevision],
        tables: dict[str, list[dict[str, Any]]],
        gaps: list[GapEntry],
        *,
        intended_use: str,
        replay: ReplayContract,
        bootstrap_markets: list[MarketBootstrap],
    ) -> PredictionMarketManifest:
        if not identities or not sources or not bootstrap_markets:
            raise ValueError(
                "materialization requires identities, sources, and typed bootstrap"
            )
        required_by_use = {
            "nautilus_snapshot_replay": ["catalog", "lifecycle", "books", "trades"],
            "nautilus_full_l2_replay": ["catalog", "lifecycle", "books", "trades"],
            "official_onchain_reconciliation": [
                "catalog", "lifecycle", "books", "trades", "onchain"
            ],
        }
        required_parts = required_by_use.get(intended_use)
        if required_parts is None:
            raise ValueError("unsupported prediction-market intended use")
        bootstrap_payload = {
            "contract_version": CONTRACT_VERSION,
            "schema_version": "marketcow.polymarket.bootstrap.v1",
            "dataset_id": dataset_id,
            "manifest_id": "0" * 64,
            "bootstrap_id": "0" * 64,
            "intended_use": intended_use,
            "replay": replay.model_dump(mode="json"),
            "markets": [item.model_dump(mode="json") for item in bootstrap_markets],
        }
        bootstrap_payload["bootstrap_id"] = bootstrap_identity(bootstrap_payload)
        bootstrap = PredictionMarketBootstrap.model_validate(bootstrap_payload)
        definition_path = (
            self.root / "bootstrap-definitions" / f"{bootstrap.bootstrap_id}.json"
        )
        definition_body = canonical_json(bootstrap.model_dump(mode="json"))
        if definition_path.exists() and definition_path.read_bytes() != definition_body:
            raise RuntimeError("immutable bootstrap definition conflict")
        if not definition_path.exists():
            _atomic_write(definition_path, definition_body)
        parts = [
            self._write_part(name, records)
            for name, records in sorted(tables.items()) if records
        ]
        all_records = [record for records in tables.values() for record in records]
        coverage_records = [
            record for table_name, records in tables.items()
            if table_name != "catalog" for record in records
        ]
        event_times = [
            self._parquet_row(record)["event_at"] for record in coverage_records
        ]
        token_ids = {
            outcome.token_id for identity in identities for outcome in identity.outcomes
        }
        coverage = CoverageFacts(
            market_count=len(identities), token_count=len(token_ids),
            start=min(event_times) if event_times else None,
            end=max(event_times) if event_times else None,
            event_count=len(all_records),
            trade_count=len(tables.get("trades") or []),
            onchain_count=len(tables.get("onchain") or []),
            unresolved_gap_count=sum(not gap.resolved for gap in gaps),
        )
        payload = {
            "contract_version": CONTRACT_VERSION,
            "dataset_id": dataset_id,
            "revision": revision,
            "intended_use": intended_use,
            "replay": replay,
            "bootstrap_id": bootstrap.bootstrap_id,
            "required_parts": required_parts,
            "status": "draft",
            "created_at": self.now_provider().astimezone(timezone.utc).isoformat(),
            "identities": identities,
            "sources": sources,
            "parts": parts,
            "coverage": coverage,
            "gap_ledger": gaps,
            "checks": [],
            "supersedes": None,
        }
        serialized = {
            key: [item.model_dump(mode="json") for item in value]
            if key in {"identities", "sources", "parts", "gap_ledger"}
            else value.model_dump(mode="json")
            if key in {"coverage", "replay"} else value
            for key, value in payload.items()
        }
        payload["manifest_id"] = manifest_identity(serialized)
        manifest = PredictionMarketManifest.model_validate(payload)
        self.write_manifest(manifest)
        return manifest


class PredictionMarketCertifier:
    def __init__(self, materializer: PredictionMarketMaterializer):
        self.materializer = materializer

    @staticmethod
    def _trade_key(record: dict[str, Any]) -> tuple[str, str, str, str]:
        return (
            str(record.get("transaction_hash") or "").lower(),
            str(record.get("token_id") or record.get("asset_id") or ""),
            decimal_text(record.get("price"), "trade.price"),
            decimal_text(record.get("size"), "trade.size"),
        )

    def certify(
        self,
        draft: PredictionMarketManifest,
        *,
        book_states: dict[str, dict[str, Any]],
        official_trades: list[dict[str, Any]],
        onchain_trades: list[dict[str, Any]],
    ) -> PredictionMarketManifest:
        checks: list[CertificationCheck] = []
        definition_path = (
            self.materializer.root / "bootstrap-definitions"
            / f"{draft.bootstrap_id}.json"
        )
        bootstrap = None
        bootstrap_error = None
        try:
            bootstrap = PredictionMarketBootstrap.model_validate_json(
                definition_path.read_text(encoding="utf-8")
            )
            if bootstrap_identity(bootstrap.model_dump(mode="json")) != draft.bootstrap_id:
                raise ValueError("bootstrap content identity mismatch")
            if bootstrap.dataset_id != draft.dataset_id:
                raise ValueError("bootstrap dataset identity mismatch")
            if bootstrap.replay != draft.replay:
                raise ValueError("bootstrap replay contract mismatch")
            if {item.identity.market_id for item in bootstrap.markets} != {
                item.market_id for item in draft.identities
            }:
                raise ValueError("bootstrap market coverage mismatch")
        except (FileNotFoundError, ValueError) as exc:
            bootstrap_error = str(exc)
        checks.append(CertificationCheck(
            name="typed_bootstrap_complete",
            passed=bootstrap is not None and bootstrap_error is None,
            details={"bootstrap_id": draft.bootstrap_id, "error": bootstrap_error},
        ))
        available_parts = {part.table for part in draft.parts}
        missing_parts = sorted(set(draft.required_parts) - available_parts)
        checks.append(CertificationCheck(
            name="intended_use_required_parts",
            passed=not missing_parts,
            details={
                "intended_use": draft.intended_use,
                "required": draft.required_parts,
                "missing": missing_parts,
            },
        ))
        book_part = next((part for part in draft.parts if part.table == "books"), None)
        replay_errors = []
        if book_part is not None:
            rows = pq.read_table(book_part.path).to_pylist()
            previous = None
            for row in rows:
                if row["record_type"] not in {"snapshot", "book", "delta", "price_change"}:
                    replay_errors.append("unsupported book record_type")
                    continue
                if draft.replay.mode == "snapshot_only" and row["record_type"] in {
                    "delta", "price_change"
                }:
                    replay_errors.append("snapshot-only dataset contains a delta")
                required = (
                    "received_at", "book_epoch", "sequence", "update_semantics",
                    "tick_version", "state_checksum",
                )
                if any(row.get(field) in {None, ""} for field in required):
                    replay_errors.append(f"incomplete replay row:{row['record_id']}")
                key = (
                    row["event_at"], row["received_at"], row["book_epoch"],
                    row["sequence"], row["record_id"],
                )
                if previous is not None and key < previous:
                    replay_errors.append("books are not deterministically ordered")
                previous = key
        checks.append(CertificationCheck(
            name="historical_book_replay_semantics",
            passed=book_part is not None and not replay_errors,
            details={"mode": draft.replay.mode, "errors": replay_errors[:20]},
        ))
        expected_tokens = {
            outcome.token_id
            for identity in draft.identities for outcome in identity.outcomes
        }
        checks.append(CertificationCheck(
            name="two_outcome_tokens",
            passed=all(len(identity.outcomes) == 2 for identity in draft.identities),
            details={"expected_tokens": sorted(expected_tokens)},
        ))
        book_errors = []
        for token_id, state in book_states.items():
            try:
                _validate_book(state)
            except ValueError as exc:
                book_errors.append({"token_id": token_id, "error": str(exc)})
        checks.append(CertificationCheck(
            name="book_not_crossed_and_tick_aligned",
            passed=not book_errors and expected_tokens <= set(book_states),
            details={"errors": book_errors, "tokens_seen": sorted(book_states)},
        ))
        hash_gaps = [
            gap.model_dump(mode="json") for gap in draft.gap_ledger
            if gap.code == "hash_mismatch" and not gap.resolved
        ]
        checks.append(CertificationCheck(
            name="snapshot_delta_hash_consistency",
            passed=not hash_gaps,
            details={"unresolved": hash_gaps},
        ))
        official_keys = {self._trade_key(row) for row in official_trades}
        onchain_keys = {self._trade_key(row) for row in onchain_trades}
        missing_onchain = sorted(official_keys - onchain_keys)
        reconciliation_required = (
            draft.intended_use == "official_onchain_reconciliation"
        )
        checks.append(CertificationCheck(
            name="official_onchain_trade_reconciliation",
            passed=(
                bool(official_keys) and not missing_onchain
                if reconciliation_required else bool(official_keys)
            ),
            details={
                "official": len(official_keys), "onchain": len(onchain_keys),
                "missing_onchain": missing_onchain if reconciliation_required else [],
                "reconciliation_required": reconciliation_required,
            },
        ))
        unresolved = [gap for gap in draft.gap_ledger if not gap.resolved]
        checks.append(CertificationCheck(
            name="coverage_and_gap_ledger",
            passed=not unresolved and draft.coverage.start is not None
            and draft.coverage.end is not None,
            details={"unresolved_gap_count": len(unresolved)},
        ))
        passed = all(check.passed for check in checks)
        payload = draft.model_dump(mode="json")
        payload.update({
            "manifest_id": "0" * 64,
            "status": "certified" if passed else "rejected",
            "checks": [item.model_dump(mode="json") for item in checks],
            "attestation_sha256": None,
        })
        payload["manifest_id"] = manifest_identity(payload)
        if passed:
            payload["attestation_sha256"] = content_sha256({
                "manifest_id": payload["manifest_id"],
                "parts": payload["parts"],
                "sources": payload["sources"],
                "checks": payload["checks"],
                "bootstrap_id": draft.bootstrap_id,
            })
        result = PredictionMarketManifest.model_validate(payload)
        self.materializer.write_manifest(result)
        if passed and bootstrap is not None:
            bound = bootstrap.model_copy(update={"manifest_id": result.manifest_id})
            bootstrap_path = (
                self.materializer.root / "bootstraps" / f"{result.manifest_id}.json"
            )
            bootstrap_body = canonical_json(bound.model_dump(mode="json"))
            if bootstrap_path.exists() and bootstrap_path.read_bytes() != bootstrap_body:
                raise RuntimeError("immutable certified bootstrap conflict")
            if not bootstrap_path.exists():
                _atomic_write(bootstrap_path, bootstrap_body)
        return result


class PublishedPredictionMarketStore:
    """Read boundary exposed to Tradude; draft manifests are never visible."""

    def __init__(self, root: Path):
        self.root = root.resolve()

    def _pointer(self, dataset_id: str) -> Path:
        return self.root / "published" / f"{content_sha256(dataset_id)}.json"

    def publish(self, manifest: PredictionMarketManifest) -> Path:
        if manifest.status != "certified":
            raise ValueError("only certified manifests can be published")
        manifest_path = self.root / "manifests" / f"{manifest.manifest_id}.json"
        if not manifest_path.exists():
            raise FileNotFoundError("certified manifest is not materialized")
        bootstrap_path = self.root / "bootstraps" / f"{manifest.manifest_id}.json"
        if not bootstrap_path.exists():
            raise FileNotFoundError("certified bootstrap is not materialized")
        bootstrap = PredictionMarketBootstrap.model_validate_json(
            bootstrap_path.read_text(encoding="utf-8")
        )
        if (
            bootstrap.dataset_id != manifest.dataset_id
            or bootstrap.manifest_id != manifest.manifest_id
            or bootstrap.bootstrap_id != manifest.bootstrap_id
            or bootstrap_identity(bootstrap.model_dump(mode="json"))
            != manifest.bootstrap_id
        ):
            raise RuntimeError("certified bootstrap binding failed")
        for part in manifest.parts:
            path = Path(part.path).resolve()
            if not path.is_relative_to(self.root) or not path.exists():
                raise RuntimeError("manifest part is outside the immutable store")
            import hashlib

            if hashlib.sha256(path.read_bytes()).hexdigest() != part.sha256:
                raise RuntimeError("manifest part hash verification failed")
        pointer = self._pointer(manifest.dataset_id)
        _atomic_write(pointer, canonical_json({
            "contract_version": CONTRACT_VERSION,
            "dataset_id": manifest.dataset_id,
            "manifest_id": manifest.manifest_id,
            "published_at": utc_now().isoformat(),
        }))
        return pointer

    def manifest(self, dataset_id: str) -> PredictionMarketManifest:
        pointer = self._pointer(dataset_id)
        if not pointer.exists():
            raise FileNotFoundError("certified dataset is not published")
        selected = json.loads(pointer.read_text(encoding="utf-8"))
        if selected.get("dataset_id") != dataset_id:
            raise RuntimeError("published dataset pointer identity mismatch")
        path = self.root / "manifests" / f"{selected['manifest_id']}.json"
        manifest = PredictionMarketManifest.model_validate_json(path.read_text())
        if manifest.status != "certified":
            raise RuntimeError("published manifest is not certified")
        return manifest

    def part(self, dataset_id: str, table: str) -> Path:
        manifest = self.manifest(dataset_id)
        selected = next((item for item in manifest.parts if item.table == table), None)
        if selected is None:
            raise FileNotFoundError("certified dataset table is unavailable")
        path = Path(selected.path).resolve()
        if not path.is_relative_to(self.root):
            raise RuntimeError("published part escapes immutable store")
        import hashlib

        if hashlib.sha256(path.read_bytes()).hexdigest() != selected.sha256:
            raise RuntimeError("published part hash verification failed")
        return path

    def bootstrap(self, dataset_id: str) -> PredictionMarketBootstrap:
        manifest = self.manifest(dataset_id)
        path = self.root / "bootstraps" / f"{manifest.manifest_id}.json"
        bootstrap = PredictionMarketBootstrap.model_validate_json(path.read_text())
        if (
            bootstrap.dataset_id != dataset_id
            or bootstrap.manifest_id != manifest.manifest_id
            or bootstrap.bootstrap_id != manifest.bootstrap_id
            or bootstrap_identity(bootstrap.model_dump(mode="json"))
            != manifest.bootstrap_id
        ):
            raise RuntimeError("published bootstrap identity mismatch")
        return bootstrap


def table_records_from_recorder(
    recorder: PolymarketWebSocketRecorder,
) -> dict[str, list[dict[str, Any]]]:
    tables: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for envelope in recorder.events():
        event_type = envelope["event"]["type"]
        table = "trades" if event_type == "last_trade_price" else "books"
        tables[table].append(envelope)
    return dict(tables)


class PolymarketWebSocketCollector:
    """Public market-channel collector; reconnects only through fresh snapshots."""

    endpoint = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

    def __init__(
        self,
        recorder: PolymarketWebSocketRecorder,
        snapshot_fetcher,
        *,
        connector=websockets.connect,
        reconnect_seconds: float = 1.0,
    ):
        self.recorder = recorder
        self.snapshot_fetcher = snapshot_fetcher
        self.connector = connector
        self.reconnect_seconds = max(0.0, reconnect_seconds)

    async def _recover_snapshots(self) -> None:
        for token_id in sorted(self.recorder.token_ids):
            snapshot = await self.snapshot_fetcher(token_id)
            if snapshot.get("type") != "book":
                raise RuntimeError("snapshot recovery must return a full book")
            self.recorder.append(snapshot)

    async def run(self, *, max_connections: int | None = None) -> None:
        attempts = 0
        while max_connections is None or attempts < max_connections:
            attempts += 1
            if attempts > 1:
                self.recorder.record_gap(GapEntry(
                    code="coverage_gap",
                    detected_at=self.recorder.now_provider(),
                    observed=f"websocket_reconnect:{attempts}",
                ))
                await self._recover_snapshots()
            try:
                async with self.connector(self.endpoint) as socket:
                    await socket.send(json.dumps({
                        "assets_ids": sorted(self.recorder.token_ids),
                        "type": "market",
                        "custom_feature_enabled": True,
                    }))
                    async for message in socket:
                        payload = json.loads(message, parse_float=str, parse_int=str)
                        for event in payload if isinstance(payload, list) else [payload]:
                            self.recorder.append(event)
                return
            except asyncio.CancelledError:
                raise
            except Exception:
                if max_connections is not None and attempts >= max_connections:
                    raise
                await asyncio.sleep(self.reconnect_seconds)
