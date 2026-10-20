import hashlib
import json
from unittest.mock import patch

import pytest

from marketcow.catalog_archive import CatalogArchive, changes, apply, compact, encode


def record(key, **extra):
    return {"identity": {"market_id": key}, "observed_at": "2026-09-01T00:00:00Z", **extra}


def catalog(root, values):
    directory = root / "catalogs"
    directory.mkdir(exist_ok=True)
    body = b"".join(encode(v) + b"\n" for v in values)
    revision = hashlib.sha256(body).hexdigest()
    path = directory / f"{revision}.jsonl"
    path.write_bytes(body)
    return path


def test_nested_delta_preserves_all_fields_and_types():
    old = {"a": [None, {"fee": "0", "source": 1}], "deleted": True, "typed": 1}
    new = {"a": [False, {"fee": "0.01", "added": []}], "typed": True, "new": None}
    assert apply(old, changes(old, new)) == new
    assert old["deleted"] is True
    assert apply(old, changes(old, [])) == []


def test_base_delta_add_update_delete_and_exact_restore(tmp_path):
    old = catalog(tmp_path, [record("a", rules={"fee": 0}), record("c"), record("d")])
    new = catalog(tmp_path, [record("a", rules={"fee": 1}), record("b"), record("d")])
    archive = CatalogArchive(tmp_path)
    first = archive.archive(old)
    second = archive.archive(new, parent=old.stem, parent_path=old)
    assert first["depth"] == 0
    assert second["changes"] == {"added": 1, "updated": 1, "deleted": 1, "unchanged": 1}
    expected = new.read_bytes()
    old.unlink()
    new.unlink()
    output = tmp_path / "restored.jsonl"
    archive.restore(second["revision"], output)
    assert output.read_bytes() == expected
    with pytest.raises(FileExistsError):
        archive.restore(second["revision"], output)
    assert not list(tmp_path.glob("*.pending"))


def test_observation_and_provenance_are_not_discarded(tmp_path):
    old = catalog(tmp_path, [record("a", provenance={"observed_at": "old", "hash": "x"})])
    new = catalog(tmp_path, [record("a", provenance={"observed_at": "new", "hash": "y"})])
    archive = CatalogArchive(tmp_path)
    archive.archive(old)
    meta = archive.archive(new, parent=old.stem, parent_path=old)
    assert meta["changes"]["updated"] == 1
    assert b"".join(row[2] for row in archive.iter_rows(new.stem)) == new.read_bytes()


def test_corruption_and_missing_parent_fail_closed(tmp_path):
    old = catalog(tmp_path, [record("a")])
    new = catalog(tmp_path, [record("b")])
    archive = CatalogArchive(tmp_path)
    archive.archive(old)
    archive.archive(new, parent=old.stem, parent_path=old)
    parent = archive.directory / f"{old.stem}.jsonl.gz"
    parent.write_bytes(parent.read_bytes()[:-5])
    with pytest.raises(ValueError, match="checksum"):
        archive.restore(new.stem, tmp_path / "bad.jsonl")
    assert not (tmp_path / "bad.jsonl").exists()
    parent.unlink()
    with pytest.raises(FileNotFoundError):
        list(archive.iter_rows(new.stem))
    assert old.exists() and new.exists()


@pytest.mark.parametrize("values", [[record("b"), record("a")], [record("a"), record("a")]])
def test_unsorted_or_duplicate_markets_keep_source(tmp_path, values):
    path = catalog(tmp_path, values)
    with pytest.raises(ValueError, match="unique and sorted"):
        CatalogArchive(tmp_path).archive(path)
    assert path.exists()
    assert not list((tmp_path / "catalog-deltas").glob("*.json"))


def test_noncanonical_and_partial_input_keep_source(tmp_path):
    path = catalog(tmp_path, [record("a")])
    path.write_text(json.dumps(record("a")))
    with pytest.raises(ValueError, match="incomplete"):
        CatalogArchive(tmp_path).archive(path)
    assert path.exists()


def test_failed_fsync_never_publishes_receipt_or_deletes_source(tmp_path):
    path = catalog(tmp_path, [record("a")])
    with patch("marketcow.catalog_archive.os.fsync", side_effect=OSError("disk full")):
        with pytest.raises(OSError, match="disk full"):
            CatalogArchive(tmp_path).archive(path)
    assert path.exists()
    assert not list((tmp_path / "catalog-deltas").glob("*.json"))


def test_pruning_protects_both_live_manifests_and_keeps_restorable_history(tmp_path):
    files = [catalog(tmp_path, [record("a", price=i)]) for i in range(4)]
    (tmp_path / "catalog.json").write_text(json.dumps({"catalog_revision": files[1].stem}))
    (tmp_path / "state-index.json").write_text(json.dumps({"catalog_revision": files[2].stem}))
    # Storage/pruning tests use only a process-local stub for the existing
    # publication lock; integration tests exercise the actual live lock.
    from contextlib import nullcontext
    import sys
    from types import SimpleNamespace
    with patch.dict(sys.modules, {"marketcow.polymarket_live": SimpleNamespace(
        _publication_lock=lambda *a, **kw: nullcontext()
    )}):
        compact(tmp_path, prune=True)
    archive = CatalogArchive(tmp_path)
    for file in files:
        assert hashlib.sha256(b"".join(r[2] for r in archive.iter_rows(file.stem))).hexdigest() == file.stem
    assert files[1].exists() and files[2].exists()
    head = json.loads((archive.directory / "head.json").read_bytes())["revision"]
    for file in files:
        assert file.exists() == (file.stem in {files[1].stem, files[2].stem, head})


def test_parent_does_not_match_receipt(tmp_path):
    old = catalog(tmp_path, [record("a")])
    new = catalog(tmp_path, [record("b")])
    archive = CatalogArchive(tmp_path)
    archive.archive(old)
    old.write_bytes(b"changed")
    with pytest.raises(ValueError, match="parent source checksum"):
        archive.archive(new, parent=old.stem, parent_path=old)
    assert new.exists()


def test_bounded_line_reader_rejects_oversized_record(tmp_path):
    path = catalog(tmp_path, [record("a", payload="x" * 300)])
    with patch("marketcow.catalog_archive.MAX_ROW_BYTES", 128):
        with pytest.raises(ValueError, match="budget"):
            CatalogArchive(tmp_path).archive(path)


def test_archive_path_traversal_rejected(tmp_path):
    with pytest.raises(ValueError, match="revision"):
        CatalogArchive(tmp_path).metadata("../catalog.json")


def test_background_archival_is_coalesced_and_does_not_wait_for_disk(tmp_path):
    import threading
    import time
    import marketcow.catalog_archive as module
    started, release, done = threading.Event(), threading.Event(), threading.Event()
    calls = []

    def slow(root, **kwargs):
        calls.append(root)
        started.set()
        assert release.wait(5)
        if len(calls) == 2:
            done.set()

    with patch.object(module, "compact", side_effect=slow):
        begin = time.monotonic()
        module.request_compaction(tmp_path)
        assert time.monotonic() - begin < 0.5
        assert started.wait(2)
        for _ in range(100):
            module.request_compaction(tmp_path)
        with module._maintenance_lock:
            assert module._maintenance_pending == {tmp_path.resolve()}
        release.set()
        assert done.wait(2)
    # The second coalesced request has returned before another test starts.
    deadline = time.monotonic() + 2
    while module._maintenance_running and time.monotonic() < deadline:
        time.sleep(0.001)
    assert not module._maintenance_running


def test_missing_dependency_prevents_pruning(tmp_path):
    from marketcow.catalog_archive import _prune
    old = catalog(tmp_path, [record("a")])
    new = catalog(tmp_path, [record("b")])
    archive = CatalogArchive(tmp_path)
    archive.archive(old)
    archive.archive(new, parent=old.stem, parent_path=old)
    (archive.directory / f"{old.stem}.jsonl.gz").unlink()
    with pytest.raises(FileNotFoundError):
        _prune(tmp_path, archive, new, "f" * 64)
    assert new.exists()
