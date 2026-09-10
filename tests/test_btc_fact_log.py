import threading
import time
import sqlite3
import subprocess
import sys

import pytest

from marketcow.btc_fact_log import FactLog, read_page, recover_tail


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
def test_corrupt_index_rejected_and_lock_released(tmp_path, mutation):
    log = make(tmp_path)
    log.publish({"x": 1})
    log.publish({"x": 2})
    log.close(2)
    with sqlite3.connect(tmp_path / "index.sqlite") as db:
        db.execute(mutation)
    for _ in range(2):
        with pytest.raises(ValueError, match="continuity"):
            make(tmp_path)
    with pytest.raises(ValueError, match="continuity"):
        read_page(tmp_path, after=0, through=2, limit=3, maximum_bytes=4096)


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
