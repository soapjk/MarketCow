from __future__ import annotations

import hashlib
import json
import sqlite3
from pathlib import Path

import pytest

from scripts.migration.build_polymarket_rust_scope import build_scope, write_atomic_json


def _fixture(tmp_path: Path) -> tuple[Path, Path]:
    manifest = tmp_path / "manifest.json"
    manifest.write_text(
        json.dumps(
            {
                "scope_id": "scope-2",
                "catalog_revision": "catalog-v1",
                "market_ids": ["2", "1"],
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
            """
        )
        connection.execute("INSERT INTO metadata VALUES (?, ?)", ("catalog_revision", "catalog-v1"))
        connection.executemany(
            "INSERT INTO tokens VALUES (?, ?)",
            [("40", "2"), ("30", "2"), ("20", "1"), ("10", "1")],
        )
    return manifest, index


def test_build_scope_is_deterministic_and_hash_pinned(tmp_path: Path) -> None:
    manifest, index = _fixture(tmp_path)
    manifest_sha256 = hashlib.sha256(manifest.read_bytes()).hexdigest()
    scope = build_scope(
        manifest,
        index,
        expected_market_count=2,
        expected_token_count=4,
        expected_manifest_sha256=manifest_sha256,
    )
    assert scope["schema_version"] == "marketcow.polymarket.rust-live-scope.v1"
    assert scope["market_ids"] == ["1", "2"]
    assert scope["token_ids"] == ["10", "20", "30", "40"]
    assert scope["source"]["manifest_sha256"] == manifest_sha256
    assert len(scope["source"]["catalog_index_sha256"]) == 64

    first = tmp_path / "first.json"
    second = tmp_path / "second.json"
    write_atomic_json(first, scope)
    write_atomic_json(second, scope)
    assert first.read_bytes() == second.read_bytes()


def test_build_scope_fails_closed_on_missing_token_pair(tmp_path: Path) -> None:
    manifest, index = _fixture(tmp_path)
    with sqlite3.connect(index) as connection:
        connection.execute("DELETE FROM tokens WHERE token_id = '40'")
    with pytest.raises(ValueError, match="exactly two"):
        build_scope(
            manifest,
            index,
            expected_market_count=2,
            expected_token_count=4,
        )


def test_build_scope_fails_closed_on_manifest_hash_mismatch(tmp_path: Path) -> None:
    manifest, index = _fixture(tmp_path)
    with pytest.raises(ValueError, match="SHA-256 mismatch"):
        build_scope(
            manifest,
            index,
            expected_market_count=2,
            expected_token_count=4,
            expected_manifest_sha256="0" * 64,
        )
