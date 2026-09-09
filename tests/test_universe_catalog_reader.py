import hashlib
import json
import sqlite3

import pytest

from marketcow.universe_catalog_reader import CatalogReadError, IndexedCatalogReader


@pytest.fixture
def source(tmp_path):
    catalog = tmp_path / "catalog.jsonl"
    index = tmp_path / "index.sqlite"
    bodies = [json.dumps({"identity": {"market_id": mid}, "active": active,
                         "closed": not active}).encode()
              for mid, active in [("1", True), ("2", False), ("3", True)]]
    catalog.write_bytes(b"\n".join(bodies) + b"\n")
    digest = hashlib.sha256(catalog.read_bytes()).hexdigest()
    with sqlite3.connect(index) as db:
        db.executescript("CREATE TABLE metadata(key TEXT PRIMARY KEY,value TEXT);"
                         "CREATE TABLE markets(market_id TEXT PRIMARY KEY,byte_offset INTEGER,"
                         "byte_length INTEGER,row_sha256 TEXT,catalog_revision TEXT);")
        db.executemany("INSERT INTO metadata VALUES(?,?)", [
            ("catalog_revision", "a"*64), ("catalog_sha256", digest), ("market_count", "3")])
        offset = 0
        for i, body in enumerate(bodies, 1):
            db.execute("INSERT INTO markets VALUES(?,?,?,?,?)",
                       (str(i), offset, len(body), hashlib.sha256(body).hexdigest(), "a"*64))
            offset += len(body)+1
    return catalog, index, digest


def open_reader(source):
    catalog, index, digest = source
    return IndexedCatalogReader(catalog, index, revision="a"*64,
                                expected_sha256=digest, max_row_bytes=1024)


def test_all_rows_including_closed_and_stable_retry(source):
    reader = open_reader(source)
    try:
        first = reader.page(after=None, limit=2, byte_budget=2048)
        assert first == reader.page(after=None, limit=2, byte_budget=2048)
        assert [r["identity"]["market_id"] for r in first[0]] == ["1", "2"]
        assert first[0][1]["closed"] is True
        last = reader.page(after=first[1], limit=2, byte_budget=2048)
        assert last[1] is None
        assert len(first[0]) + len(last[0]) == reader.count == 3
    finally:
        reader.close()


def test_budget_never_skips_record(source):
    reader = open_reader(source)
    try:
        with pytest.raises(CatalogReadError, match="response_size_exceeded"):
            reader.page(after=None, limit=2, byte_budget=1)
        one, cursor, used = reader.page(after=None, limit=2, byte_budget=90)
        assert len(one) == 1 and cursor == "1" and used <= 90
    finally:
        reader.close()


def test_whole_file_and_row_corruption(source):
    catalog, _, _ = source
    reader = open_reader(source)
    try:
        with catalog.open("r+b") as f:
            f.write(b"!")
        with pytest.raises(CatalogReadError, match="catalog_row_hash_mismatch"):
            reader.page(after=None, limit=1, byte_budget=1024)
    finally:
        reader.close()
    with pytest.raises(CatalogReadError, match="catalog_hash_mismatch"):
        open_reader(source)


def test_path_replacement_keeps_pinned_generation(source, tmp_path):
    catalog, _, _ = source
    reader = open_reader(source)
    try:
        replacement = tmp_path / "replacement"
        replacement.write_bytes(b"not the original catalog")
        replacement.replace(catalog)
        rows, end, _ = reader.page(after=None, limit=3, byte_budget=4096)
        assert len(rows) == 3 and end is None
    finally:
        reader.close()
