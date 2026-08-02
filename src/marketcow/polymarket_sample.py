from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pyarrow.dataset as ds
import requests

from .polymarket_contracts import (
    FeeSchedule,
    GapEntry,
    MarketBootstrap,
    OutcomeToken,
    PredictionMarketIdentity,
    ReplayContract,
    RuleFact,
    SourceRevision,
    StructuralRelation,
    canonical_json,
    content_sha256,
)
from .polymarket_history import (
    PredictionMarketCertifier,
    PredictionMarketMaterializer,
    PublishedPredictionMarketStore,
    _state_hash,
    _validate_book,
)
from .polymarket_sources import _atomic_write, utc_now


FIXED_DATASET = "kinzikdza/polymarket-updown-microstructure"
FIXED_REVISION = "eb4e9fc794c059dd9bef69c98eb4d34e70a5bd83"
FIXED_LICENSE = "cc-by-4.0"
FIXED_FILES = {
    "slots.parquet": "893785d09d7daaa1b94cd3a5bd46a968270531d4ae11c6c6523af1cbe9b0c912",
    "book_snapshots.parquet": "074c6f8f367c7fe1517a28adaa70d2fd98b0edf4167175bb8f9fb4c6c183dbb5",
    "pm_trades.parquet": "1378effac3f2d61807b5181155dfd66526cd0199ff1f317cbeab1b4f87ef00a6",
}
README_SHA256 = "f95504550f47e0d43023bfa8af9555e95e2b34745000bbd10bc67b6d5f8ffc1a"
FEE_DOC_SHA256 = "8e246189f6ca85b8b8782e1d76a769cf98672a98c4c63d8bbe6150d659db8d7c"
SDK_REVISION = "f3e1a05f868a1fd0c34ef85dfc45c6ce78f5bb69"
SDK_ROUNDING_SHA256 = "0fd2d5020c1dd9b717788fc4f58d5a4ea28b790ad97170a7b4042b6e9864001f"
FEE_MODULE_REVISION = "1a3c31c48275a9adceb039a05cfcf15aba4629bc"
FEE_MODULE_EVIDENCE_SHA256 = (
    "910a2918cbf71f43db2a3ce8ccc7711d86c1de92c56d628abef8bf34a8acd13e"
)


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _decimal(value: Any) -> str:
    if value is None:
        raise ValueError("required source decimal is missing")
    if isinstance(value, float):
        return str(value)
    return str(value)


def _instant(seconds: int | float) -> datetime:
    return datetime.fromtimestamp(float(seconds), timezone.utc)


def _parse_ladder(value: str | None) -> list[dict[str, str]]:
    levels = []
    for item in (value or "").split("|"):
        if not item:
            continue
        price, size = item.split("x", 1)
        levels.append({"price": price, "size": size})
    return levels


def _source_revision(
    *, source: str, revision: str, source_url: str, path: Path,
    observed_at: datetime, license_name: str | None = None,
) -> SourceRevision:
    return SourceRevision(
        source=source,
        revision=revision,
        source_url=source_url,
        observed_at=observed_at,
        ingested_at=observed_at,
        payload_sha256=_sha256_file(path),
        raw_path=str(path.resolve()),
        license=license_name,
    )


class FixedFreeSampleBuilder:
    """Build a real, snapshot-only certified sample from one pinned free release."""

    def __init__(
        self,
        source_root: Path,
        store_root: Path,
        *,
        now_provider=utc_now,
        requester=requests.get,
    ):
        self.source_root = source_root.resolve()
        self.store_root = store_root.resolve()
        self.now_provider = now_provider
        self.requester = requester

    def verify_sources(self) -> None:
        for name, expected in FIXED_FILES.items():
            path = self.source_root / name
            if not path.exists() or _sha256_file(path) != expected:
                raise RuntimeError(f"fixed dataset hash mismatch: {name}")
        for name, expected in {
            "README.md": README_SHA256,
            "polymarket-fees.md": FEE_DOC_SHA256,
            "clob-v2-roundingConfig.ts": SDK_ROUNDING_SHA256,
            "exchange-fee-module-FeeModule.sol": FEE_MODULE_EVIDENCE_SHA256,
        }.items():
            path = self.source_root / name
            if not path.exists() or _sha256_file(path) != expected:
                raise RuntimeError(f"fixed source documentation hash mismatch: {name}")

    def _fixed_sources(self) -> list[SourceRevision]:
        base = (
            "https://huggingface.co/datasets/kinzikdza/"
            f"polymarket-updown-microstructure/resolve/{FIXED_REVISION}/parquet"
        )
        sources = [
            _source_revision(
                source="huggingface_fixed_revision", revision=FIXED_REVISION,
                source_url=f"{base}/{name}", path=self.source_root / name,
                observed_at=datetime.fromtimestamp(
                    (self.source_root / name).stat().st_mtime, timezone.utc
                ), license_name=FIXED_LICENSE,
            )
            for name in FIXED_FILES
        ]
        sources.append(_source_revision(
            source="polymarket_docs", revision=f"sha256:{FEE_DOC_SHA256}",
            source_url="https://docs.polymarket.com/trading/fees.md",
            path=self.source_root / "polymarket-fees.md",
            observed_at=datetime.fromtimestamp(
                (self.source_root / "polymarket-fees.md").stat().st_mtime, timezone.utc
            ),
        ))
        sources.append(_source_revision(
            source="polymarket_sdk", revision=SDK_REVISION,
            source_url=(
                "https://raw.githubusercontent.com/Polymarket/clob-client-v2/"
                f"{SDK_REVISION}/src/order-builder/helpers/roundingConfig.ts"
            ),
            path=self.source_root / "clob-v2-roundingConfig.ts",
            observed_at=datetime.fromtimestamp(
                (self.source_root / "clob-v2-roundingConfig.ts").stat().st_mtime,
                timezone.utc,
            ),
            license_name="mit",
        ))
        sources.append(_source_revision(
            source="polymarket_fee_module", revision=FEE_MODULE_REVISION,
            source_url=(
                "https://github.com/Polymarket/exchange-fee-module/blob/"
                f"{FEE_MODULE_REVISION}/src/FeeModule.sol"
            ),
            path=(
                self.source_root / "exchange-fee-module-FeeModule.sol"
            ),
            observed_at=datetime.fromtimestamp(
                (
                    self.source_root
                    / "exchange-fee-module-FeeModule.sol"
                ).stat().st_mtime,
                timezone.utc,
            ),
            license_name="mit",
        ))
        return sources

    def _market_metadata(self, condition_id: str) -> tuple[dict[str, Any], SourceRevision]:
        raw_path = self.source_root / "clob-markets" / f"{condition_id}.json"
        url = f"https://clob.polymarket.com/markets/{condition_id}"
        if not raw_path.exists():
            response = self.requester(
                url, timeout=30,
                headers={"Accept": "application/json", "User-Agent": "MarketCow/0.2"},
            )
            response.raise_for_status()
            payload = json.loads(response.text, parse_float=str, parse_int=str)
            _atomic_write(raw_path, canonical_json(payload))
        payload = json.loads(raw_path.read_text(), parse_float=str, parse_int=str)
        if payload.get("condition_id") != condition_id:
            raise RuntimeError("CLOB metadata condition identity mismatch")
        observed = datetime.fromtimestamp(raw_path.stat().st_mtime, timezone.utc)
        return payload, _source_revision(
            source="polymarket_clob", revision=f"sha256:{_sha256_file(raw_path)}",
            source_url=url, path=raw_path, observed_at=observed,
        )

    def _select_slots(self, count: int) -> list[dict[str, Any]]:
        slots = ds.dataset(self.source_root / "slots.parquet").to_table().to_pylist()
        book_table = ds.dataset(
            self.source_root / "book_snapshots.parquet"
        ).to_table(columns=[
            "condition_id", "yes_bid", "yes_ask", "no_bid", "no_ask"
        ])
        valid_book_ids = {
            str(row["condition_id"])
            for row in book_table.to_pylist()
            if row["yes_bid"] is not None and row["yes_ask"] is not None
            and row["no_bid"] is not None and row["no_ask"] is not None
            and row["yes_bid"] < row["yes_ask"]
            and row["no_bid"] < row["no_ask"]
        }
        trade_ids = set(ds.dataset(
            self.source_root / "pm_trades.parquet"
        ).to_table(columns=["token_id"]).column("token_id").to_pylist())
        selected = []
        for row in slots:
            if (
                row["condition_id"] in valid_book_ids
                and row["yes_token_id"] in trade_ids
                and row["no_token_id"] in trade_ids
                and row["resolved_side"] in {"Yes", "No", "Up", "Down"}
                and row["fee_rate"] is not None
                and row["rebate_rate"] is not None
            ):
                selected.append(row)
                if len(selected) == count:
                    break
        if len(selected) != count:
            raise RuntimeError("fixed dataset lacks requested complete sample coverage")
        return selected

    @staticmethod
    def _identity(slot: dict[str, Any], metadata: dict[str, Any]) -> PredictionMarketIdentity:
        tokens = metadata.get("tokens") or []
        by_token = {str(item["token_id"]): item for item in tokens}
        expected = {str(slot["yes_token_id"]), str(slot["no_token_id"])}
        if set(by_token) != expected or len(tokens) != 2:
            raise RuntimeError("CLOB token mapping differs from fixed dataset")
        condition = str(slot["condition_id"])
        question_id = str(metadata.get("question_id") or "")
        if not question_id:
            raise RuntimeError("CLOB metadata lacks question identity")
        return PredictionMarketIdentity(
            event_id=f"POLY:EVENT:{question_id}",
            market_id=f"POLY:MARKET:{question_id}",
            condition_id=condition,
            slug=str(metadata.get("market_slug") or ""),
            outcomes=[
                OutcomeToken(
                    token_id=token_id,
                    outcome=str(by_token[token_id]["outcome"]),
                    instrument_id=f"POLY:{condition}:{token_id}",
                )
                for token_id in (str(slot["yes_token_id"]), str(slot["no_token_id"]))
            ],
            neg_risk=bool(metadata.get("neg_risk")),
            neg_risk_market_id=(str(metadata.get("neg_risk_market_id") or "") or None),
        )

    def _bootstrap(
        self, slot: dict[str, Any], metadata: dict[str, Any],
        identity: PredictionMarketIdentity, dataset_source: SourceRevision,
        clob_source: SourceRevision, docs_source: SourceRevision,
        sdk_source: SourceRevision, fee_module_source: SourceRevision,
    ) -> MarketBootstrap:
        winners = [item for item in metadata["tokens"] if item.get("winner") is True]
        if len(winners) != 1:
            raise RuntimeError("resolved sample market requires exactly one CLOB winner")
        winner = str(winners[0]["outcome"])
        source_winner = str(slot["resolved_side"])
        aliases = {"Yes": "Up", "No": "Down"}
        if winner != source_winner and aliases.get(source_winner) != winner:
            raise RuntimeError("fixed dataset and CLOB resolution disagree")
        if metadata.get("closed") is not True or metadata.get("accepting_orders") is not False:
            raise RuntimeError("sample requires closed non-accepting markets")
        question = str(metadata.get("question") or "")
        tick = _decimal(metadata.get("minimum_tick_size"))
        minimum = _decimal(metadata.get("minimum_order_size"))
        activation = _instant(slot["open_ts"])
        expiration = _instant(slot["close_ts"])
        instruments = [item.instrument_id for item in identity.outcomes]
        relation = StructuralRelation(
            relation_id=content_sha256({"condition": identity.condition_id, "type": "binary"}),
            relation_version="clob-metadata-v1", relation_type="binary_complements",
            members=instruments, convertible=True, provenance=clob_source,
            valid_from=activation, valid_to=None,
        )
        facts = [
            RuleFact(
                fact_id=content_sha256({"condition": identity.condition_id, "fact": fact}),
                rule_version="source-evidence-v1", fact_type=fact,
                value=value, provenance=source, valid_from=activation,
            )
            for fact, value, source in (
                ("binary_settlement", {
                    "winner": winner, "winner_payout": "1", "loser_payout": "0",
                    "description": metadata.get("description"),
                }, clob_source),
                ("settlement_currency", {"currency": "USDC.e"}, docs_source),
                ("price_increment", {"increment": tick}, clob_source),
                ("size_increment", {
                    "increment": "0.01",
                    "evidence": "official CLOB v2 SDK ROUNDING_CONFIG size precision = 2",
                }, sdk_source),
                ("minimum_order_size", {"size": minimum}, clob_source),
            )
        ]
        return MarketBootstrap(
            identity=identity, question=question, title=question,
            settlement_currency="USDC.e", activation_at=activation,
            expiration_at=expiration, price_increment=tick, size_increment="0.01",
            minimum_order_size=minimum, accepting_orders=False,
            lifecycle_state="resolved", resolution=winner,
            external_ids={
                "clob_question_id": str(metadata["question_id"]),
                "condition_id": identity.condition_id,
                "slug": identity.slug,
            },
            relations=[relation], rule_facts=facts,
            fee_schedule=FeeSchedule(
                schedule_id=content_sha256({
                    "condition": identity.condition_id, "fee_rate": slot["fee_rate"]
                }),
                schedule_version="fixed-dataset-plus-official-docs-v1",
                currency="USDC.e", maker_rate="0",
                taker_rate=_decimal(slot["fee_rate"]),
                formula="fee = C * taker_rate * p * (1 - p)", exponent="1",
                quantum="0.00001",
                # The public executable accepts an operator-chosen uint256 fee amount;
                # it does not publish the decimal tie-breaking rule used upstream.
                rounding_mode="UNSPECIFIED", tie_semantics="unspecified",
                calculation_status="informational_only",
                effective_from=activation, effective_to=expiration,
                provenance=[
                    dataset_source, docs_source, clob_source, fee_module_source
                ],
            ),
        )

    @staticmethod
    def _book_records(
        source_root: Path, selected: list[dict[str, Any]],
        identities: dict[str, PredictionMarketIdentity],
        metadata_by_condition: dict[str, dict[str, Any]],
    ) -> tuple[
        list[dict[str, Any]], dict[str, dict[str, Any]], list[GapEntry]
    ]:
        selected_ids = set(identities)
        table = ds.dataset(source_root / "book_snapshots.parquet").to_table(
            filter=ds.field("condition_id").isin(sorted(selected_ids))
        )
        rows = sorted(table.to_pylist(), key=lambda row: (row["ts_ms"], row["condition_id"]))
        sequences: dict[str, int] = defaultdict(int)
        records = []
        states = {}
        excluded: dict[str, int] = defaultdict(int)
        last_instant = datetime.fromtimestamp(0, timezone.utc)
        for raw in rows:
            condition = str(raw["condition_id"])
            identity = identities[condition]
            metadata = metadata_by_condition[condition]
            tick = _decimal(metadata["minimum_tick_size"])
            for label, token_id in (
                ("yes", identity.outcomes[0].token_id),
                ("no", identity.outcomes[1].token_id),
            ):
                bid = raw.get(f"{label}_bid")
                ask = raw.get(f"{label}_ask")
                bids = [] if bid is None else [{
                    "price": _decimal(bid), "size": _decimal(raw[f"{label}_bid_size"]),
                }]
                asks = _parse_ladder(raw.get(f"{label}_ladder"))
                if not asks and ask is not None:
                    asks = [{
                        "price": _decimal(ask), "size": _decimal(raw[f"{label}_ask_size"]),
                    }]
                if not bids or not asks:
                    continue
                sequences[token_id] += 1
                book_epoch = content_sha256({
                    "revision": FIXED_REVISION, "condition": condition, "token": token_id,
                })
                state = {
                    "token_id": token_id, "tick_size": tick,
                    "bids": {item["price"]: item["size"] for item in bids},
                    "asks": {item["price"]: item["size"] for item in asks},
                }
                try:
                    _validate_book(state)
                except ValueError:
                    excluded[token_id] += 1
                    continue
                checksum = _state_hash(state)
                instant = datetime.fromtimestamp(raw["ts_ms"] / 1000, timezone.utc)
                last_instant = max(last_instant, instant)
                event = {
                    "event_id": content_sha256({
                        "revision": FIXED_REVISION, "condition": condition,
                        "token": token_id, "ts_ms": raw["ts_ms"],
                    }),
                    "type": "snapshot", "market_id": identity.market_id,
                    "condition_id": condition, "token_id": token_id,
                    "exchange_ts": instant.isoformat(), "received_ts": instant.isoformat(),
                    "timestamp_provenance": "source capture ts_ms supplies both timestamps",
                    "book_epoch": book_epoch, "sequence": sequences[token_id],
                    "source_sequence": None, "update_semantics": "absolute_size",
                    "tick_version": content_sha256({"tick_size": tick}),
                    "state_checksum": checksum, "tick_size": tick,
                    "bids": bids, "asks": asks,
                }
                records.append({
                    "market_id": identity.market_id, "condition_id": condition,
                    "source": "huggingface_fixed_revision", "event": event,
                    "raw_payload": raw,
                })
                states[token_id] = state
        gaps = [GapEntry(
            code="source_mismatch", token_id=token_id,
            observed=f"excluded_invalid_source_snapshots:{count}",
            detected_at=last_instant, resolved=True,
            resolution="raw fixed-revision source retained; invalid snapshots excluded",
        ) for token_id, count in sorted(excluded.items())]
        return records, states, gaps

    @staticmethod
    def _trade_records(
        source_root: Path, identities: dict[str, PredictionMarketIdentity]
    ) -> list[dict[str, Any]]:
        token_to_identity = {
            outcome.token_id: identity
            for identity in identities.values() for outcome in identity.outcomes
        }
        table = ds.dataset(source_root / "pm_trades.parquet").to_table(
            filter=ds.field("token_id").isin(sorted(token_to_identity))
        )
        records = []
        for index, raw in enumerate(sorted(
            table.to_pylist(), key=lambda row: (row["ts_ms"], row["token_id"])
        )):
            identity = token_to_identity[str(raw["token_id"])]
            records.append({
                "record_type": "trade",
                "trade_id": content_sha256({
                    "revision": FIXED_REVISION, "row": index, "payload": raw,
                }),
                "market_id": identity.market_id,
                "condition_id": identity.condition_id,
                "token_id": str(raw["token_id"]),
                "timestamp": datetime.fromtimestamp(
                    raw["ts_ms"] / 1000, timezone.utc
                ).isoformat(),
                "price": _decimal(raw["price"]), "size": _decimal(raw["size"]),
                "side": "buy" if int(raw["taker_buy"]) else "sell",
                "transaction_hash": "", "source": "huggingface_fixed_revision",
                "raw_payload": raw,
            })
        return records

    def build(self, dataset_id: str, *, market_count: int = 20) -> dict[str, Any]:
        if market_count < 20:
            raise ValueError("certified production sample requires at least 20 markets")
        self.verify_sources()
        fixed_sources = self._fixed_sources()
        dataset_source = next(
            source for source in fixed_sources
            if source.source == "huggingface_fixed_revision"
        )
        docs_source = next(
            source for source in fixed_sources if source.source == "polymarket_docs"
        )
        sdk_source = next(
            source for source in fixed_sources if source.source == "polymarket_sdk"
        )
        fee_module_source = next(
            source for source in fixed_sources
            if source.source == "polymarket_fee_module"
        )
        slots = self._select_slots(market_count)
        identities = []
        bootstraps = []
        metadata_by_condition = {}
        clob_sources = []
        for slot in slots:
            condition = str(slot["condition_id"])
            metadata, clob_source = self._market_metadata(condition)
            identity = self._identity(slot, metadata)
            identities.append(identity)
            metadata_by_condition[condition] = metadata
            clob_sources.append(clob_source)
            bootstraps.append(self._bootstrap(
                slot, metadata, identity, dataset_source, clob_source, docs_source,
                sdk_source, fee_module_source,
            ))
        by_condition = {item.condition_id: item for item in identities}
        books, book_states, gaps = self._book_records(
            self.source_root, slots, by_condition, metadata_by_condition
        )
        trades = self._trade_records(self.source_root, by_condition)
        build_time = max(
            source.ingested_at for source in fixed_sources + clob_sources
        )
        timestamp = build_time.isoformat()
        catalog = [{
            "record_type": "catalog", "market_id": item.market_id,
            "condition_id": item.condition_id, "timestamp": timestamp,
            "source": "polymarket_clob", "raw_payload": metadata_by_condition[item.condition_id],
        } for item in identities]
        lifecycle = [{
            "record_type": "resolved", "market_id": item.identity.market_id,
            "condition_id": item.identity.condition_id,
            "timestamp": item.expiration_at.isoformat(), "source": "huggingface_fixed_revision",
            "raw_payload": {
                "activation_at": item.activation_at.isoformat(),
                "expiration_at": item.expiration_at.isoformat(),
                "resolution": item.resolution,
            },
        } for item in bootstraps]
        replay = ReplayContract(
            mode="snapshot_only",
            ordering=[
                "exchange_at", "received_at", "book_epoch", "sequence", "record_id"
            ],
            sequence_semantics="deterministic_normalized", delta_supported=False,
            cancellation_supported=False, queue_position_supported=False,
            depth="source best bid plus top-five ask ladder when present",
        )
        materializer = PredictionMarketMaterializer(
            self.store_root, now_provider=lambda: build_time
        )
        draft = materializer.materialize(
            dataset_id, FIXED_REVISION, identities, fixed_sources + clob_sources,
            {"catalog": catalog, "lifecycle": lifecycle, "books": books, "trades": trades},
            gaps, intended_use="nautilus_snapshot_replay", replay=replay,
            bootstrap_markets=bootstraps,
        )
        certified = PredictionMarketCertifier(materializer).certify(
            draft, book_states=book_states, official_trades=trades, onchain_trades=[]
        )
        if certified.status != "certified":
            failures = [item.model_dump(mode="json") for item in certified.checks if not item.passed]
            raise RuntimeError(f"sample certification failed: {failures}")
        PublishedPredictionMarketStore(self.store_root).publish(certified)
        return {
            "dataset_id": certified.dataset_id,
            "manifest_id": certified.manifest_id,
            "bootstrap_id": certified.bootstrap_id,
            "store_root": str(self.store_root),
            "market_count": certified.coverage.market_count,
            "token_count": certified.coverage.token_count,
            "coverage_start": certified.coverage.start.isoformat(),
            "coverage_end": certified.coverage.end.isoformat(),
            "parts": [item.model_dump(mode="json") for item in certified.parts],
            "checks": [item.model_dump(mode="json") for item in certified.checks],
        }
