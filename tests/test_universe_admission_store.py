import json
import subprocess
import sys
from pathlib import Path

import pytest

from marketcow.universe_admission_store import AdmissionStore, StoreError, StoreLimits
from marketcow.universe_phase1 import DiscoverySelectionRequest


NOW = 1788769800000


def request(nonce="c"):
    value = json.loads((Path(__file__).parent / "contracts/phase1/selection-request.json").read_bytes())
    value["request_id"] = f"{NOW}:" + nonce * 32
    return DiscoverySelectionRequest.model_validate(value)


def store(path):
    return AdmissionStore(path, StoreLimits(900000, 86400000, 1, 128))


def test_reopen_returns_original_bytes(tmp_path):
    path = tmp_path / "admit.sqlite"
    first = store(path)
    assert first.record("caller", request(), b"original", now_ms=NOW) == b"original"
    first.close()
    second = store(path)
    try:
        assert second.record("caller", request(), b"changed", now_ms=NOW+1) == b"original"
        with pytest.raises(StoreError, match="clock_regression"):
            second.record("caller", request(), b"x", now_ms=NOW)
    finally:
        second.close()


def test_capacity_does_not_evict_valid_and_clock_survives_rejection(tmp_path):
    path = tmp_path / "admit.sqlite"
    db = store(path)
    db.record("caller", request(), b"original", now_ms=NOW)
    with pytest.raises(StoreError, match="resource_unavailable"):
        db.record("caller", request("d"), b"new", now_ms=NOW+10)
    db.close()
    db = store(path)
    try:
        with pytest.raises(StoreError, match="clock_regression"):
            db.record("caller", request(), b"x", now_ms=NOW+9)
        assert db.record("caller", request(), b"x", now_ms=NOW+10) == b"original"
    finally:
        db.close()


def test_digest_conflict_expiry_and_corruption(tmp_path):
    db = store(tmp_path / "admit.sqlite")
    try:
        db.record("caller", request(), b"original", now_ms=NOW)
        altered = request().model_dump()
        altered["expires_at"] = "2026-09-07T08:44:00.000Z"
        with pytest.raises(StoreError, match="idempotency_conflict"):
            db.record("caller", DiscoverySelectionRequest.model_validate(altered), b"x", now_ms=NOW)
        db.db.execute("UPDATE requests SET response=?", (b"corrupt",))
        with pytest.raises(StoreError, match="store_corruption"):
            db.record("caller", request(), b"x", now_ms=NOW)
        with pytest.raises(StoreError, match="request_expired"):
            db.record("caller", request(), b"x", now_ms=NOW+900000)
        db.db.execute("DELETE FROM requests")
        with pytest.raises(StoreError, match="request_expired"):
            db.record("caller", request(), b"x", now_ms=NOW+86400000)
    finally:
        db.close()


def test_response_budget_and_caller_separation(tmp_path):
    db = store(tmp_path / "admit.sqlite")
    try:
        with pytest.raises(StoreError, match="response_size_exceeded"):
            db.record("caller", request(), b"x"*129, now_ms=NOW)
        db.record("caller", request(), b"first", now_ms=NOW)
        with pytest.raises(StoreError, match="resource_unavailable"):
            db.record("other", request(), b"other", now_ms=NOW)
    finally:
        db.close()


def test_failed_insert_rolls_back_request(tmp_path):
    db = store(tmp_path / "admit.sqlite")
    try:
        db.db.execute("CREATE TRIGGER fail_insert BEFORE INSERT ON requests BEGIN SELECT RAISE(ABORT,'injected'); END")
        with pytest.raises(Exception, match="injected"):
            db.record("caller", request(), b"original", now_ms=NOW)
        assert db.db.execute("SELECT count(*) FROM requests").fetchone()[0] == 0
        assert not db.db.in_transaction
        db.db.execute("DROP TRIGGER fail_insert")
        assert db.record("caller", request(), b"retry", now_ms=NOW) == b"retry"
    finally:
        db.close()


def test_store_rejects_changed_lifetime_profile(tmp_path):
    path = tmp_path / "admit.sqlite"
    db = store(path)
    db.close()
    with pytest.raises(StoreError, match="store_profile_mismatch"):
        AdmissionStore(path, StoreLimits(1800000, 86400000, 1, 128))


def test_process_exit_during_insert_keeps_clock_but_no_response(tmp_path):
    path = tmp_path/"crash.sqlite"
    child = subprocess.run([sys.executable, "-c", """
import os, sys
from pathlib import Path
from marketcow.universe_phase1 import DiscoverySelectionRequest
from marketcow.universe_admission_store import AdmissionStore, StoreLimits
db = AdmissionStore(Path(sys.argv[1]), StoreLimits(900000,86400000,1,128))
db.db.create_function('crash_now', 0, lambda: os._exit(23))
db.db.execute('CREATE TEMP TRIGGER crash BEFORE INSERT ON requests BEGIN SELECT crash_now(); END')
db.record('caller', DiscoverySelectionRequest.model_validate_json(sys.argv[2]), b'not committed', now_ms=1788769800010)
""", str(path), request().model_dump_json()], timeout=10, capture_output=True)
    assert child.returncode == 23, child.stderr.decode()
    db = store(path)
    try:
        assert db.db.execute("SELECT count(*) FROM requests").fetchone()[0] == 0
        with pytest.raises(StoreError, match="clock_regression"):
            db.record("caller", request(), None, now_ms=NOW+9)
        assert db.record("caller", request(), b"after crash", now_ms=NOW+10) == b"after crash"
    finally:
        db.close()
