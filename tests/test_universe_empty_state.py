import json
import sqlite3

import pytest

from marketcow.universe_empty_state import initialize_empty_candidate


def test_empty_candidate_never_invents_books_or_resets(tmp_path):
    (tmp_path/"catalog.json").write_text(json.dumps({"catalog_revision": "a"*64}))
    result = initialize_empty_candidate(tmp_path, catalog_revision="a"*64)
    assert result["requires_real_preheat"] is True
    with sqlite3.connect(tmp_path/"indexes/latest-state.sqlite3") as db:
        assert db.execute("PRAGMA integrity_check").fetchone() == ("ok",)
        assert db.execute("SELECT count(*) FROM books").fetchone() == (0,)
        assert db.execute("SELECT value FROM metadata WHERE key='latest_cursor'").fetchone() == ("0",)
    with pytest.raises(ValueError, match="reset prohibited"):
        initialize_empty_candidate(tmp_path, catalog_revision="a"*64)


def test_wrong_catalog_never_creates_state(tmp_path):
    (tmp_path/"catalog.json").write_text(json.dumps({"catalog_revision": "a"*64}))
    with pytest.raises(ValueError, match="catalog mismatch"):
        initialize_empty_candidate(tmp_path, catalog_revision="b"*64)
    assert not (tmp_path/"indexes").exists()
