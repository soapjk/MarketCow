from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pytest

from scripts.migration.build_polymarket_rust_scope import build_scope, write_atomic_json
from scripts.migration.build_polymarket_dynamic_universe import build_dynamic_universe
from scripts.migration.fetch_polymarket_dynamic_candidate_books import (
    build_candidate_book_snapshot,
)
from scripts.migration.refresh_polymarket_dynamic_universe_books import refresh_candidate


class _BookResponse:
    def __init__(self, payload: list[dict[str, object]]) -> None:
        self._payload = payload
        self.content = json.dumps(payload, separators=(",", ":")).encode()

    def raise_for_status(self) -> None:
        return None

    def json(self, **_: object) -> list[dict[str, object]]:
        return self._payload


class _BookPoster:
    def __init__(self, books: dict[str, dict[str, object]]) -> None:
        self.books = books
        self.requests: list[list[dict[str, str]]] = []

    def post(
        self, _: str, *, json: list[dict[str, str]], timeout: float
    ) -> _BookResponse:
        assert timeout > 0
        self.requests.append(json)
        return _BookResponse([
            self.books[item["token_id"]]
            for item in json
            if item["token_id"] in self.books
        ])


def _fixture(tmp_path: Path) -> tuple[Path, Path, Path, Path]:
    manifest = tmp_path / "manifest.json"
    manifest.write_text(
        json.dumps(
            {
                "scope_id": "scope-2",
                "catalog_revision": "catalog-v1",
                "market_ids": ["2", "1"],
                "negative_risk_relation_ids": [],
            },
            separators=(",", ":"),
        )
    )
    index = tmp_path / "catalog.sqlite3"
    with sqlite3.connect(index) as connection:
        connection.executescript(
            """
            CREATE TABLE metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE tokens (token_id TEXT PRIMARY KEY, market_id TEXT NOT NULL);
            CREATE TABLE markets (
                market_id TEXT PRIMARY KEY, byte_offset INTEGER NOT NULL,
                byte_length INTEGER NOT NULL
            );
            """
        )
        connection.execute("INSERT INTO metadata VALUES (?, ?)", ("catalog_revision", "catalog-v1"))
        connection.executemany(
            "INSERT INTO tokens VALUES (?, ?)",
            [("40", "2"), ("30", "2"), ("20", "1"), ("10", "1")],
        )
        catalog = tmp_path / "catalog.jsonl"
        offset = 0
        for market_id, tokens in (("1", ("10", "20")), ("2", ("30", "40"))):
            row = {
                "identity": {
                    "market_id": market_id,
                    "condition_id": f"condition-{market_id}",
                    "outcomes": [
                        {"token_id": tokens[0], "outcome": "Yes", "instrument_id": f"POLY:{market_id}:{tokens[0]}"},
                        {"token_id": tokens[1], "outcome": "No", "instrument_id": f"POLY:{market_id}:{tokens[1]}"},
                    ],
                },
                "rules": {
                    "instrument": {
                        "price_increment": "0.01", "size_increment": "0.01",
                        "minimum_order_size": "5", "settlement_currency": "pUSD",
                        "revision": f"instrument-{market_id}",
                    },
                    "fee_schedule": {
                        "schedule_id": market_id * 64,
                        "schedule_version": "polymarket-fees-v1",
                        "currency": "USDC",
                        "maker_rate": "0",
                        "taker_rate": "0" if market_id == "1" else "0.05",
                        "formula": "fee = C * feeRate * p * (1 - p)",
                        "exponent": "1",
                        "quantum": "0.00001",
                        "rounding_mode": "UNSPECIFIED",
                        "tie_semantics": "unspecified",
                        "calculation_status": "informational_only",
                        "effective_from": "2026-01-01T00:00:00Z",
                        "effective_to": None,
                        "complete": True,
                        "missing_fields": [],
                        "provenance": [{
                            "source": "polymarket_docs",
                            "source_url": "https://docs.polymarket.com/trading/fees",
                            "revision": "fees-docs-v1",
                            "payload_sha256": market_id * 64,
                            "observed_at": "2026-08-28T00:00:00Z",
                            "field_paths": ["fee_structure", "fee_precision"],
                        }],
                    },
                },
                "start_at": "2026-01-01T00:00:00Z",
                "end_at": "2026-09-01T00:00:00Z",
                "lifecycle_state": "active", "resolution": None,
                "metadata_revision": f"metadata-{market_id}",
                "observed_at": "2026-01-02T00:00:00Z",
            }
            encoded = (json.dumps(row, separators=(",", ":")) + "\n").encode()
            with catalog.open("ab") as output:
                output.write(encoded)
            connection.execute("INSERT INTO markets VALUES (?, ?, ?)", (market_id, offset, len(encoded)))
            offset += len(encoded)
    registry = tmp_path / "registry.json"
    registry.write_text(json.dumps({"negative_risk_groups": []}))
    return manifest, index, catalog, registry


def test_build_scope_is_deterministic_and_hash_pinned(tmp_path: Path) -> None:
    manifest, index, catalog, registry = _fixture(tmp_path)
    manifest_sha256 = hashlib.sha256(manifest.read_bytes()).hexdigest()
    scope = build_scope(
        manifest,
        index,
        catalog,
        registry,
        expected_market_count=2,
        expected_token_count=4,
        expected_manifest_sha256=manifest_sha256,
        validated_at=datetime(2026, 8, 29, tzinfo=timezone.utc),
    )
    assert scope["schema_version"] == "marketcow.polymarket.rust-live-scope.v3"
    assert scope["market_ids"] == ["1", "2"]
    assert scope["token_ids"] == ["10", "20", "30", "40"]
    assert scope["source"]["manifest_sha256"] == manifest_sha256
    assert len(scope["source"]["catalog_index_sha256"]) == 64
    assert len(scope["catalog_frame"]["markets"]) == 2
    assert scope["catalog_frame"]["markets"][0]["instrument_facts"]["price_increment"] == "0.01"
    fees = [
        market["instrument_facts"]["fee_schedule"]
        for market in scope["catalog_frame"]["markets"]
    ]
    assert [fee["taker_rate"] for fee in fees] == ["0", "0.05"]
    assert all(len(fee["revision"]) == 64 for fee in fees)
    assert all(fee["formula_id"] == "polymarket_probability_fee.v1" for fee in fees)
    assert all(fee["calculation_status"] == "informational_only" for fee in fees)

    first = tmp_path / "first.json"
    second = tmp_path / "second.json"
    write_atomic_json(first, scope)
    write_atomic_json(second, scope)
    assert first.read_bytes() == second.read_bytes()


def test_build_scope_fails_closed_on_missing_token_pair(tmp_path: Path) -> None:
    manifest, index, catalog, registry = _fixture(tmp_path)
    with sqlite3.connect(index) as connection:
        connection.execute("DELETE FROM tokens WHERE token_id = '40'")
    with pytest.raises(ValueError, match="exactly two"):
        build_scope(
            manifest,
            index,
            catalog,
            registry,
            expected_market_count=2,
            expected_token_count=4,
        )


def test_build_scope_fails_closed_on_manifest_hash_mismatch(tmp_path: Path) -> None:
    manifest, index, catalog, registry = _fixture(tmp_path)
    with pytest.raises(ValueError, match="SHA-256 mismatch"):
        build_scope(
            manifest,
            index,
            catalog,
            registry,
            expected_market_count=2,
            expected_token_count=4,
            expected_manifest_sha256="0" * 64,
        )


def test_build_scope_fails_closed_on_expired_active_market(tmp_path: Path) -> None:
    manifest, index, catalog, registry = _fixture(tmp_path)
    with pytest.raises(ValueError, match="expires before required validity boundary"):
        build_scope(
            manifest,
            index,
            catalog,
            registry,
            expected_market_count=2,
            expected_token_count=4,
            validated_at=datetime(2028, 1, 1, tzinfo=timezone.utc),
        )


def test_build_scope_fails_closed_on_missing_fee_facts(tmp_path: Path) -> None:
    manifest, index, catalog, registry = _fixture(tmp_path)
    rows = [json.loads(line) for line in catalog.read_text().splitlines()]
    rows[0]["rules"].pop("fee_schedule")
    catalog.write_text("".join(json.dumps(row, separators=(",", ":")) + "\n" for row in rows))
    with sqlite3.connect(index) as connection:
        connection.execute("DELETE FROM markets")
        offset = 0
        for row in rows:
            encoded = (json.dumps(row, separators=(",", ":")) + "\n").encode()
            connection.execute(
                "INSERT INTO markets VALUES (?, ?, ?)",
                (row["identity"]["market_id"], offset, len(encoded)),
            )
            offset += len(encoded)
    with pytest.raises(ValueError, match="fee_schedule must be explicitly complete"):
        build_scope(
            manifest,
            index,
            catalog,
            registry,
            expected_market_count=2,
            expected_token_count=4,
            validated_at=datetime(2026, 8, 29, tzinfo=timezone.utc),
        )


def test_build_scope_fails_closed_outside_fee_effective_interval(tmp_path: Path) -> None:
    manifest, index, catalog, registry = _fixture(tmp_path)
    rows = [json.loads(line) for line in catalog.read_text().splitlines()]
    rows[0]["rules"]["fee_schedule"]["effective_to"] = "2026-08-28T12:00:00Z"
    encoded_rows = [
        (json.dumps(row, separators=(",", ":")) + "\n").encode() for row in rows
    ]
    catalog.write_bytes(b"".join(encoded_rows))
    with sqlite3.connect(index) as connection:
        connection.execute("DELETE FROM markets")
        offset = 0
        for row, encoded in zip(rows, encoded_rows, strict=True):
            connection.execute(
                "INSERT INTO markets VALUES (?, ?, ?)",
                (row["identity"]["market_id"], offset, len(encoded)),
            )
            offset += len(encoded)
    with pytest.raises(ValueError, match="not effective at validation time"):
        build_scope(
            manifest,
            index,
            catalog,
            registry,
            expected_market_count=2,
            expected_token_count=4,
            validated_at=datetime(2026, 8, 29, tzinfo=timezone.utc),
        )


def _books(tmp_path: Path, *, one_sided: set[str] | None = None) -> Path:
    one_sided = one_sided or set()
    path = tmp_path / "books.json"
    path.write_text(json.dumps({"books": [{
        "event_type": "book",
        "asset_id": token,
        "tick_size": "0.01",
        "bids": [{"price": "0.40", "size": "10"}],
        "asks": [] if token in one_sided else [{"price": "0.60", "size": "10"}],
        "timestamp": "2026-08-29T00:00:00Z",
    } for token in ("10", "20", "30", "40")]}))
    return path


def test_dynamic_universe_isolates_failure_and_atomically_replenishes(tmp_path: Path) -> None:
    manifest, index, catalog, registry = _fixture(tmp_path)
    result = build_dynamic_universe(
        manifest, index, catalog, registry, _books(tmp_path, one_sided={"10"}),
        universe_id="a" * 64,
        generation=7,
        target_market_count=1,
        minimum_market_count=1,
        maximum_capital_lock_seconds=30 * 24 * 60 * 60,
        previous_market_ids=["1"],
        validated_at=datetime(2026, 8, 29, tzinfo=timezone.utc),
    )
    assert result["schema_version"] == "marketcow.polymarket.rust-live-scope.v4"
    assert result["scope_id"] == "a" * 64
    assert result["market_ids"] == ["2"]
    assert result["universe"]["generation"] == 7
    assert result["universe"]["added_markets"] == ["2"]
    assert result["universe"]["removed_markets"] == ["1"]
    assert result["universe"]["removed_market_identities"] == [{
        "market_id": "1",
        "condition_id": "condition-1",
        "token_ids": ["10", "20"],
        "end_at": "2026-09-01T00:00:00Z",
    }]
    assert result["universe"]["excluded_markets"][0]["reason_code"] == "one_sided_book"
    assert len(result["initial_book_frames"]) == 2


def test_dynamic_universe_records_capacity_exclusion(tmp_path: Path) -> None:
    manifest, index, catalog, registry = _fixture(tmp_path)
    result = build_dynamic_universe(
        manifest, index, catalog, registry, _books(tmp_path),
        universe_id="b" * 64,
        generation=1,
        target_market_count=1,
        minimum_market_count=1,
        maximum_capital_lock_seconds=30 * 24 * 60 * 60,
        validated_at=datetime(2026, 8, 29, tzinfo=timezone.utc),
    )
    assert result["market_ids"] == ["2"]  # ranked manifest order is [2, 1]
    assert result["universe"]["excluded_markets"] == [{
        "market_id": "1",
        "reason_code": "target_capacity",
        "retryable": False,
        "retry_after": None,
        "observed_at": "2026-08-29T00:00:00Z",
    }]


def test_dynamic_universe_keeps_qualified_incumbent_ahead_of_ranked_replacement(
    tmp_path: Path,
) -> None:
    manifest, index, catalog, registry = _fixture(tmp_path)
    result = build_dynamic_universe(
        manifest, index, catalog, registry, _books(tmp_path),
        universe_id="8" * 64,
        generation=2,
        target_market_count=1,
        minimum_market_count=1,
        maximum_capital_lock_seconds=30 * 24 * 60 * 60,
        previous_market_ids=["1"],
        validated_at=datetime(2026, 8, 29, tzinfo=timezone.utc),
    )
    assert result["market_ids"] == ["1"]
    assert result["universe"]["added_markets"] == []
    assert result["universe"]["removed_markets"] == []


def test_dynamic_universe_preserves_removed_identity_when_catalog_member_is_unbuildable(
    tmp_path: Path,
) -> None:
    manifest, index, catalog, registry = _fixture(tmp_path)
    previous_identity = {
        "market_id": "999",
        "condition_id": "condition-expired",
        "token_ids": ["991", "992"],
        "end_at": "2026-08-28T00:00:00Z",
    }
    result = build_dynamic_universe(
        manifest, index, catalog, registry, _books(tmp_path),
        universe_id="9" * 64,
        generation=8,
        target_market_count=1,
        minimum_market_count=1,
        maximum_capital_lock_seconds=30 * 24 * 60 * 60,
        previous_market_ids=["999"],
        previous_market_identities=[previous_identity],
        validated_at=datetime(2026, 8, 29, tzinfo=timezone.utc),
    )
    assert result["universe"]["removed_markets"] == ["999"]
    assert result["universe"]["removed_market_identities"] == [previous_identity]


def test_dynamic_universe_accepts_atomic_two_token_tick_change(tmp_path: Path) -> None:
    manifest, index, catalog, registry = _fixture(tmp_path)
    books = _books(tmp_path)
    payload = json.loads(books.read_text())
    for frame in payload["books"]:
        if frame["asset_id"] in {"30", "40"}:
            frame["tick_size"] = "0.001"
    books.write_text(json.dumps(payload))

    result = build_dynamic_universe(
        manifest, index, catalog, registry, books,
        universe_id="d" * 64,
        generation=1,
        target_market_count=2,
        minimum_market_count=2,
        maximum_capital_lock_seconds=30 * 24 * 60 * 60,
        validated_at=datetime(2026, 8, 29, tzinfo=timezone.utc),
    )

    assert result["market_ids"] == ["1", "2"]
    changed_ticks = {
        frame["asset_id"]: frame["tick_size"]
        for frame in result["initial_book_frames"]
        if frame["asset_id"] in {"30", "40"}
    }
    assert changed_ticks == {"30": "0.001", "40": "0.001"}


def test_dynamic_universe_fails_closed_below_minimum(tmp_path: Path) -> None:
    manifest, index, catalog, registry = _fixture(tmp_path)
    with pytest.raises(ValueError, match="below minimum"):
        build_dynamic_universe(
            manifest, index, catalog, registry,
            _books(tmp_path, one_sided={"10", "30"}),
            universe_id="c" * 64,
            generation=1,
            target_market_count=2,
            minimum_market_count=2,
            maximum_capital_lock_seconds=30 * 24 * 60 * 60,
            validated_at=datetime(2026, 8, 29, tzinfo=timezone.utc),
        )


def test_dynamic_universe_rejects_capital_lock_policy_above_thirty_days(tmp_path: Path) -> None:
    manifest, index, catalog, registry = _fixture(tmp_path)
    with pytest.raises(ValueError, match="configuration is invalid"):
        build_dynamic_universe(
            manifest, index, catalog, registry, _books(tmp_path),
            universe_id="e" * 64,
            generation=1,
            target_market_count=1,
            minimum_market_count=1,
            maximum_capital_lock_seconds=30 * 24 * 60 * 60 + 1,
            validated_at=datetime(2026, 8, 29, tzinfo=timezone.utc),
        )


def _clob_books() -> dict[str, dict[str, object]]:
    return {
        token: {
            "market": f"condition-{token}",
            "asset_id": token,
            "timestamp": "1787976000000",
            "hash": f"hash-{token}",
            "bids": [{"price": "0.40", "size": "10.00"}],
            "asks": [{"price": "0.60", "size": "11.00"}],
            "min_order_size": "5",
            "tick_size": "0.01",
            "neg_risk": False,
        }
        for token in ("10", "20", "30", "40")
    }


def test_dynamic_universe_book_refresh_is_exact_complete_and_auditable(tmp_path: Path) -> None:
    manifest, index, catalog, registry = _fixture(tmp_path)
    candidate = build_dynamic_universe(
        manifest, index, catalog, registry, _books(tmp_path),
        universe_id="f" * 64,
        generation=2,
        target_market_count=2,
        minimum_market_count=2,
        maximum_capital_lock_seconds=30 * 24 * 60 * 60,
        validated_at=datetime(2026, 8, 29, tzinfo=timezone.utc),
    )
    poster = _BookPoster(_clob_books())

    refreshed = refresh_candidate(
        candidate,
        poster=poster,
        batch_size=2,
        observed_at=datetime(2026, 8, 29, 1, 2, 3, tzinfo=timezone.utc),
    )

    assert len(poster.requests) == 2
    assert [book["asset_id"] for book in refreshed["initial_book_frames"]] == candidate["token_ids"]
    assert all(book["event_type"] == "book" for book in refreshed["initial_book_frames"])
    assert refreshed["initial_book_frames"][0]["bids"][0]["price"] == "0.40"
    assert refreshed["universe"]["validated_at"] == "2026-08-29T01:02:03Z"
    assert refreshed["source"]["book_snapshot_observed_at"] == "2026-08-29T01:02:03Z"
    assert len(refreshed["source"]["book_snapshot_sha256"]) == 64


def test_dynamic_universe_book_refresh_fails_closed_on_missing_or_float_facts(
    tmp_path: Path,
) -> None:
    manifest, index, catalog, registry = _fixture(tmp_path)
    candidate = build_dynamic_universe(
        manifest, index, catalog, registry, _books(tmp_path),
        universe_id="1" * 64,
        generation=2,
        target_market_count=2,
        minimum_market_count=2,
        maximum_capital_lock_seconds=30 * 24 * 60 * 60,
        validated_at=datetime(2026, 8, 29, tzinfo=timezone.utc),
    )
    missing = _clob_books()
    missing.pop("40")
    with pytest.raises(ValueError, match="identity mismatch"):
        refresh_candidate(candidate, poster=_BookPoster(missing))

    floating = _clob_books()
    floating["40"]["bids"] = [{"price": 0.40, "size": "10.00"}]
    with pytest.raises(ValueError, match="exact decimal string"):
        refresh_candidate(candidate, poster=_BookPoster(floating))


def test_candidate_book_snapshot_preserves_isolatable_missing_and_one_sided_books(
    tmp_path: Path,
) -> None:
    manifest, index, _, _ = _fixture(tmp_path)
    books = _clob_books()
    books.pop("40")
    books["10"]["asks"] = []

    snapshot = build_candidate_book_snapshot(
        manifest,
        index,
        poster=_BookPoster(books),
        batch_size=2,
        observed_at=datetime(2026, 8, 29, 1, 2, 3, tzinfo=timezone.utc),
    )

    assert snapshot["schema_version"] == "marketcow.polymarket.candidate-books.v1"
    assert snapshot["requested_token_count"] == 4
    assert snapshot["book_count"] == 3
    assert snapshot["missing_token_ids"] == ["40"]
    assert snapshot["unresolved_market_ids"] == []
    assert snapshot["observed_at"] == "2026-08-29T01:02:03Z"
    assert next(book for book in snapshot["books"] if book["asset_id"] == "10")["asks"] == []


def test_candidate_book_snapshot_fails_closed_on_invalid_numeric_payload(tmp_path: Path) -> None:
    manifest, index, _, _ = _fixture(tmp_path)
    books = _clob_books()
    books["40"]["bids"] = [{"price": 0.40, "size": "10.00"}]

    with pytest.raises(ValueError, match="exact decimal string"):
        build_candidate_book_snapshot(manifest, index, poster=_BookPoster(books))
