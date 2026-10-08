import hashlib
import json
import threading
import time
import sqlite3
import subprocess
import sys

import pytest

from marketcow.btc_fact_log import FactLog, audit_committed, read_page, recover_tail


def make(root, **kwargs):
    return FactLog(root, maximum_pending=3, maximum_pending_bytes=4096,
                   maximum_disk_bytes=8192, maximum_subscribers=3, **kwargs)


def test_continuity_recovered_only_from_committed_facts(tmp_path):
    from marketcow.btc_continuity import Continuity
    state = Continuity()
    state.states["1m"] = dict(version=(120000, 121000), position=120000,
                               final=False, missing=True,
                               gap={"start": 0, "end_exclusive": 120000})
    log = make(tmp_path)
    log.publish({"continuity_checkpoint": state.checkpoint()})
    log.close(2)
    gate = threading.Event()
    log = make(tmp_path, before_write=lambda: gate.wait(3))
    assert log.recovered_continuity["states"]["1m"]["missing"]
    state.repair_completed("1m", 0, 120000)
    log.publish({"continuity_checkpoint": state.checkpoint()})
    assert log.status()["durable"] == 1
    assert log.recovered_continuity["states"]["1m"]["missing"]
    gate.set()
    log.close(2)
    log = make(tmp_path)
    try:
        assert not log.recovered_continuity["states"]["1m"]["missing"]
        assert log.status()["durable"] == 2
    finally:
        log.close(2)


def test_blocked_disk_publication_and_capacity(tmp_path):
    gate = threading.Event()
    entered = threading.Event()
    def block():
        entered.set()
        assert gate.wait(5)
    log = make(tmp_path, before_write=block)
    sub = log.subscribe(count=4, size=4096)
    try:
        log.publish({"x": 1})
        assert entered.wait(1)
        log.publish({"x": 2})
        log.publish({"x": 3})
        assert sub.receive() is not None
        assert log.status()["durable"] == 0
        with pytest.raises(RuntimeError, match="persistence_capacity"):
            log.publish({"x": 4})
        assert log.status()["published"] == 3
    finally:
        gate.set()
        log.close(2)
    assert [v["sequence"] for v in read_page(tmp_path, after=0, through=3, limit=10, maximum_bytes=4096)] == [1, 2, 3]


def test_slow_consumer_independent(tmp_path):
    log = make(tmp_path)
    slow = log.subscribe(count=1, size=1024)
    fast = log.subscribe(count=3, size=4096)
    try:
        log.publish({"x": 1})
        log.publish({"x": 2})
        with pytest.raises(RuntimeError, match="slow_consumer"):
            slow.receive()
        assert fast.receive() and fast.receive()
    finally:
        log.close(2)


def test_restart_and_byte_budget(tmp_path):
    log = make(tmp_path)
    with pytest.raises(RuntimeError, match="capacity"):
        log.publish({"payload": "x" * 4096})
    log.publish({"x": 1})
    old_epoch = log.epoch
    log.close(2)
    log = make(tmp_path)
    assert log.epoch != old_epoch and log.status()["durable"] == 1
    log.publish({"x": 2})
    log.close(2)
    assert len(read_page(tmp_path, after=0, through=1, limit=10, maximum_bytes=4096)) == 1
    with pytest.raises(ValueError, match="size"):
        read_page(tmp_path, after=0, through=2, limit=1, maximum_bytes=1)


def test_disk_failure_closes_consumers(tmp_path):
    def fail():
        raise OSError("synthetic_disk")
    log = make(tmp_path, before_write=fail)
    sub = log.subscribe(count=3, size=4096)
    log.publish({"x": 1})
    deadline = time.monotonic() + 2
    while not log.status()["error"] and time.monotonic() < deadline:
        time.sleep(.01)
    assert log.status()["durable"] == 0
    with pytest.raises(RuntimeError, match="persistence_failed"):
        sub.receive()
    with pytest.raises(RuntimeError):
        log.publish({"x": 2})
    with pytest.raises(RuntimeError):
        log.close(2)


def test_single_writer_and_torn_tail(tmp_path):
    log = make(tmp_path)
    with pytest.raises(BlockingIOError):
        make(tmp_path)
    log.close(2)
    with (tmp_path / "facts.jsonl").open("ab") as stream:
        stream.write(b"partial")
    with pytest.raises(ValueError, match="tail"):
        make(tmp_path)
    receipt = recover_tail(tmp_path, maximum_tail_bytes=1024)
    assert receipt["recovered_bytes"] == 7
    assert (tmp_path / receipt["preserved_file"]).read_bytes() == b"partial"
    log = make(tmp_path)
    assert log.status()["durable"] == 0
    log.close(2)


@pytest.mark.parametrize("mutation", ["DELETE FROM facts WHERE seq=1", "UPDATE facts SET length=-1 WHERE seq=1"])
def test_middle_corruption_is_offline_audit_responsibility(tmp_path, mutation):
    log = make(tmp_path)
    log.publish({"x": 1})
    log.publish({"x": 2})
    log.close(2)
    with sqlite3.connect(tmp_path / "index.sqlite") as db:
        db.execute(mutation)
    # Normal startup checks only the transactionally bound tail and remains O(1).
    log = make(tmp_path)
    assert log.status()["startup_validation"]["records_checked"] == 1
    log.close(2)
    # Explicit audit remains strict over the complete history.
    with pytest.raises(ValueError, match="continuity"):
        audit_committed(tmp_path, maximum_record_bytes=4096)


def test_tail_state_mismatch_rejected_and_lock_released(tmp_path):
    log = make(tmp_path)
    log.publish({"x": 1})
    log.close(2)
    with sqlite3.connect(tmp_path / "index.sqlite") as db:
        db.execute("UPDATE log_state SET durable_end=durable_end+1")
    for _ in range(2):
        with pytest.raises(ValueError, match="tail_mismatch"):
            make(tmp_path)


def test_startup_checks_one_record_and_full_audit_is_explicit(tmp_path):
    log = FactLog(tmp_path, maximum_pending=6000, maximum_pending_bytes=8 * 1024 * 1024,
                  maximum_disk_bytes=32 * 1024 * 1024, maximum_subscribers=1)
    for value in range(5000):
        log.publish({"value": value})
    log.close(10)

    log = FactLog(tmp_path, maximum_pending=3, maximum_pending_bytes=8 * 1024 * 1024,
                  maximum_disk_bytes=32 * 1024 * 1024, maximum_subscribers=1)
    try:
        assert log.status()["durable"] == 5000
        assert log.status()["startup_validation"] == {
            "mode": "transactional_tail", "records_checked": 1}
    finally:
        log.close(2)
    receipt = audit_committed(tmp_path, maximum_record_bytes=4096)
    assert receipt["complete"] and receipt["records_checked"] == 5000


def test_checkpoint_survives_later_fact_without_checkpoint(tmp_path):
    from marketcow.btc_continuity import Continuity
    state = Continuity()
    state.states["1m"] = dict(version=(120000, 121000), position=120000,
                              final=False, missing=True,
                              gap={"start": 0, "end_exclusive": 120000})
    log = make(tmp_path)
    log.publish({"continuity_checkpoint": state.checkpoint()})
    log.publish({"unrelated": True})
    log.close(2)
    log = make(tmp_path)
    try:
        assert log.recovered_continuity["states"]["1m"]["missing"]
    finally:
        log.close(2)


def test_legacy_archive_bootstraps_from_tail_without_history_scan(tmp_path):
    rows = []
    offset = 0
    with (tmp_path / "facts.jsonl").open("wb") as output:
        for seq in range(1, 101):
            raw = (json.dumps({"epoch": "legacy", "sequence": seq, "fact": {"x": seq}},
                              sort_keys=True, separators=(",", ":")) + "\n").encode()
            output.write(raw)
            rows.append((seq, offset, len(raw), hashlib.sha256(raw).hexdigest()))
            offset += len(raw)
    with sqlite3.connect(tmp_path / "index.sqlite") as db:
        db.execute("CREATE TABLE facts(seq INTEGER PRIMARY KEY, offset INTEGER, length INTEGER, sha TEXT)")
        db.executemany("INSERT INTO facts VALUES(?,?,?,?)", rows)
    log = FactLog(tmp_path, maximum_pending=3, maximum_pending_bytes=4096,
                  maximum_disk_bytes=16384, maximum_subscribers=1)
    try:
        assert log.status()["startup_validation"] == {
            "mode": "legacy_tail_bootstrap", "records_checked": 1}
    finally:
        log.close(2)


def test_close_timeout_retains_owner(tmp_path):
    gate = threading.Event()
    entered = threading.Event()
    def block():
        entered.set()
        gate.wait(5)
    log = make(tmp_path, before_write=block)
    try:
        log.publish({"x": 1})
        assert entered.wait(1)
        with pytest.raises(TimeoutError):
            log.close(.001)
        with pytest.raises(BlockingIOError):
            make(tmp_path)
    finally:
        gate.set()
        log.close(2)


def test_process_exit_between_raw_fsync_and_index_commit(tmp_path):
    # Actual child exits at the fsync seam, not a fabricated partial-file fixture.
    code = '''
import os, sys, threading
from pathlib import Path
from marketcow.btc_fact_log import FactLog
original = os.fsync
def crash(fd):
    original(fd)
    os._exit(73)
log = FactLog(Path(sys.argv[1]), maximum_pending=3, maximum_pending_bytes=4096,
              maximum_disk_bytes=8192, maximum_subscribers=1)
os.fsync = crash
log.publish({"retained": "crash evidence"})
threading.Event().wait(10)
'''
    result = subprocess.run([sys.executable, "-c", code, str(tmp_path)], timeout=15)
    assert result.returncode == 73
    original = (tmp_path / "facts.jsonl").read_bytes()
    assert b"crash evidence" in original
    with pytest.raises(ValueError, match="tail"):
        make(tmp_path)
    receipt = recover_tail(tmp_path, maximum_tail_bytes=4096)
    assert (tmp_path / receipt["preserved_file"]).read_bytes() == original
    log = make(tmp_path)
    assert log.status()["durable"] == 0
    log.publish({"after_resync": True})
    log.close(2)
    assert read_page(tmp_path, after=0, through=1, limit=1, maximum_bytes=4096)[0]["fact"] == {"after_resync": True}
