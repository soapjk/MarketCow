from copy import deepcopy
import sqlite3
import pytest
from marketcow.catalog_incremental import CatalogChanges


@pytest.fixture
def store(tmp_path):
    s = CatalogChanges(
        tmp_path / "catalog.sqlite", max_database_bytes=8 * 1024 * 1024, max_record_bytes=4096, max_batch_records=10
    )
    yield s
    s.close()


def row(mid="1", day="09", **updates):
    return dict(
        market_id=mid,
        observed_at=f"2026-09-{day}T01:00:00Z",
        evidence_sha256=day * 32,
        closed=False,
        question="A?",
        **updates,
    )


def publish(s, records, cid="a", day="09"):
    return s.publish(
        records,
        capture_id=cid,
        started_at=f"2026-09-{day}T00:00:00Z",
        completed_at=f"2026-09-{day}T02:00:00Z",
        expected_revision=s.head()["revision"],
        coverage={"complete": False},
    )


def test_added_unchanged_changed_closed(store):
    publish(store, [row()])
    publish(store, [row(day="10")], "b", "10")
    assert store.head()["sequence"] == 1
    r = row(day="10")
    r["closed"] = True
    publish(store, [r], "c", "10")
    result = store.changes(after_sequence=0, limit=10, byte_budget=10000)
    assert [e["kind"] for e in result["events"]] == ["market_added", "market_closed"]


def test_absence_not_closed(store):
    publish(store, [row(), row("2")])
    publish(store, [row(day="10")], "b", "10")
    assert store.head()["sequence"] == 2
    assert store.db.execute("select count(*) from markets").fetchone()[0] == 2


def test_atomic_bad_record(store):
    before = store.head()
    with pytest.raises(ValueError):
        publish(store, [row(), row("2", day="08")])
    assert store.head() == before
    assert store.db.execute("select count(*) from markets").fetchone()[0] == 0


def test_duplicate_and_reused_capture(store):
    with pytest.raises(sqlite3.IntegrityError):
        publish(store, [row(), row()])
    publish(store, [row()])
    with pytest.raises(ValueError, match="capture_id_reused"):
        publish(store, [row()])


def test_snapshot_stable(store, tmp_path):
    publish(store, [row()])
    dest = tmp_path / "snapshot.sqlite"
    store.snapshot(dest)
    publish(store, [row("2", day="10")], "b", "10")
    with sqlite3.connect(dest) as s:
        assert s.execute("select sequence from head").fetchone()[0] == 1
        assert s.execute("select count(*) from markets").fetchone()[0] == 1
    with pytest.raises(FileExistsError):
        store.snapshot(dest)


def test_retention_and_bytes(store):
    publish(store, [row(), row("2")])
    with pytest.raises(ValueError, match="response_size_exceeded"):
        store.changes(after_sequence=0, limit=10, byte_budget=1)
    store.prune_through(1)
    with pytest.raises(ValueError, match="resnapshot"):
        store.changes(after_sequence=0, limit=10, byte_budget=10000)
    assert len(store.changes(after_sequence=1, limit=10, byte_budget=10000)["events"]) == 1


def test_metrics_observation_not_change(store):
    a = row(liquidity={"value": "10", "unit": "source", "observed_at": "2026-09-09T01:00:00Z"})
    publish(store, [a])
    b = deepcopy(a)
    b["observed_at"] = "2026-09-10T01:00:00Z"
    b["liquidity"]["observed_at"] = b["observed_at"]
    publish(store, [b], "b", "10")
    assert store.head()["sequence"] == 1
    b["liquidity"]["value"] = "11"
    publish(store, [b], "c", "10")
    assert store.head()["sequence"] == 2


def test_revision_conflict_and_observation_regression(store):
    original = store.head()["revision"]
    publish(store, [row(day="10")], day="10")
    with pytest.raises(ValueError, match="catalog_revision_conflict"):
        store.publish(
            [],
            capture_id="conflict",
            started_at="2026-09-10T00:00:00Z",
            completed_at="2026-09-10T02:00:00Z",
            expected_revision=original,
            coverage={},
        )
    with pytest.raises(ValueError, match="observation_regression"):
        publish(store, [row()], "regression")
    assert store.head()["sequence"] == 1


def test_generator_failure_rolls_back(store):
    def broken():
        yield row()
        raise RuntimeError("source interrupted")

    with pytest.raises(RuntimeError):
        publish(store, broken())
    assert store.head()["sequence"] == 0
    assert store.db.execute("select count(*) from captures").fetchone()[0] == 0


def test_count_and_record_budget_roll_back(store):
    with pytest.raises(ValueError, match="batch_budget"):
        publish(store, (row(str(n)) for n in range(11)))
    large = row()
    large["question"] = "x" * 5000
    with pytest.raises(ValueError, match="record_budget"):
        publish(store, [large])
    assert store.head()["sequence"] == 0


def test_reopen_and_capture_page_boundary(store):
    closed = row()
    closed["closed"] = True
    publish(store, [closed, row("2")])
    page = store.changes(after_sequence=0, limit=1, byte_budget=10000)
    assert page["capture_end_sequences"] == {"a": 2}
    assert page["events"][0]["batch_start_sequence"] == 1
    assert page["has_more"]
    publish(store, [row(day="10")], "b", "10")
    assert store.changes(after_sequence=2, limit=1, byte_budget=10000)["events"][0]["kind"] == "market_reopened"


def test_reopen_database_preserves_waterline(tmp_path):
    path = tmp_path / "durable.sqlite"
    options = dict(max_database_bytes=8 * 1024 * 1024, max_record_bytes=4096, max_batch_records=10)
    first = CatalogChanges(path, **options)
    publish(first, [row()])
    head = first.head()
    first.close()
    second = CatalogChanges(path, **options)
    try:
        assert second.head() == head
        assert second.changes(after_sequence=0, limit=1, byte_budget=10000)["events"][0]["market_id"] == "1"
    finally:
        second.close()
