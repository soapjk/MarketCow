from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pytest

from scripts.migration.build_polymarket_rust_scope import build_scope, write_atomic_json


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
                "rules": {"instrument": {
                    "price_increment": "0.01", "size_increment": "0.01",
                    "minimum_order_size": "5", "settlement_currency": "pUSD",
                    "revision": f"instrument-{market_id}",
                }},
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
    assert scope["schema_version"] == "marketcow.polymarket.rust-live-scope.v2"
    assert scope["market_ids"] == ["1", "2"]
    assert scope["token_ids"] == ["10", "20", "30", "40"]
    assert scope["source"]["manifest_sha256"] == manifest_sha256
    assert len(scope["source"]["catalog_index_sha256"]) == 64
    assert len(scope["catalog_frame"]["markets"]) == 2
    assert scope["catalog_frame"]["markets"][0]["instrument_facts"]["price_increment"] == "0.01"

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
