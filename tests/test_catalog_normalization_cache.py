import sqlite3
from datetime import datetime, timezone
import pytest

from marketcow import catalog_normalization_cache as cache
from marketcow.catalog_incremental import encoded
from tests.test_catalog_refresh_worker import market


def test_group_budget_and_retry(tmp_path):
    with sqlite3.connect(tmp_path / "cache.sqlite") as db:
        db.execute("CREATE TABLE raw(id TEXT PRIMARY KEY,payload BLOB,observed TEXT)")
        cache.initialize(db)
        raw = market()
        db.execute("INSERT INTO raw VALUES(?,?,?)", ("42", encoded(raw), "2026-09-09T00:00:00Z"))
        cache.changed(db, "42", raw)
        db.commit()
        now = datetime(2026, 9, 9, tzinfo=timezone.utc)
        with pytest.raises(ValueError, match="group budget"):
            cache.normalize(db, tmp_path / "bad.jsonl", now, max_group_bytes=1, max_group_records=1)
        assert db.execute("SELECT count(*) FROM norm_dirty").fetchone()[0] == 1
        result, count = cache.normalize(db, tmp_path / "ok.jsonl", now, max_group_bytes=10000, max_group_records=1)
        assert result.market_count == count == 1
        assert db.execute("SELECT count(*) FROM norm_dirty").fetchone()[0] == 0
