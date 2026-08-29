from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pytest

from scripts.migration.build_polymarket_rust_scope import build_scope, write_atomic_json
from scripts.migration.build_polymarket_dynamic_universe import build_dynamic_universe


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
                "end_at": "2027-01-01T00:00:00Z",
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
        maximum_capital_lock_seconds=365 * 24 * 60 * 60,
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
        "end_at": "2027-01-01T00:00:00Z",
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
        maximum_capital_lock_seconds=365 * 24 * 60 * 60,
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
        maximum_capital_lock_seconds=365 * 24 * 60 * 60,
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
            maximum_capital_lock_seconds=365 * 24 * 60 * 60,
            validated_at=datetime(2026, 8, 29, tzinfo=timezone.utc),
        )
