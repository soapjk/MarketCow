"""Synthetic control intents; no Rust runtime or production switch is claimed."""
import pytest

from marketcow.universe_generation import Generation, GenerationStore


def generation(name="a", pool="discovery", ids=("1",), **kw):
    return Generation(pool=pool, selection_id=name, catalog_revision="a"*64,
                      artifact_sha256="b"*64, market_ids=ids, dependency_market_ids=(),
                      protected_market_ids=(), parent_selection_id="a" if pool == "live" else None,
                      stream_instance_id=name, baseline_cursor=kw.get("cursor", 100))


@pytest.fixture
def store(tmp_path):
    s = GenerationStore(tmp_path/"generations.sqlite", maximum_generations=3, maximum_record_bytes=10000)
    yield s
    s.close()


def choose(store, gid, pool="discovery", protected=(), now=10):
    return store.select(pool, gid, expected=store.desired(pool), protected_market_ids=protected, now_ms=now)


def test_register_does_not_activate_and_cas_aba_rollback(store):
    a = store.register(generation(), expires_ms=1000, now_ms=1)
    b = store.register(generation("b", cursor=1), expires_ms=1000, now_ms=1)
    assert store.desired("discovery") is None
    receipt = choose(store, a)
    assert receipt["status"] == "pending_runtime_application"
    old = store.desired("discovery")
    choose(store, b)  # Different instance's smaller cursor is valid.
    choose(store, a)
    assert store.desired("discovery")["epoch"] == 3
    with pytest.raises(ValueError, match="incumbent_conflict"):
        store.select("discovery", b, expected=old, protected_market_ids=(), now_ms=10)


def test_restart_and_independent_process_cas(store, tmp_path):
    gid = store.register(generation(), expires_ms=1000, now_ms=1)
    choose(store, gid)
    other = GenerationStore(tmp_path/"generations.sqlite", maximum_generations=3, maximum_record_bytes=10000)
    try:
        assert other.desired("discovery") == store.desired("discovery")
        with pytest.raises(ValueError, match="incumbent_conflict"):
            other.select("discovery", gid, expected=None, protected_market_ids=(), now_ms=10)
        with pytest.raises(ValueError, match="clock_regression"):
            choose(other, gid, now=9)
    finally:
        other.close()


def test_expiry_capacity_retry_and_protection_preserve_incumbent(store):
    a = store.register(generation(), expires_ms=100, now_ms=1)
    assert store.register(generation(), expires_ms=100, now_ms=1) == a
    with pytest.raises(ValueError, match="retry conflict"):
        store.register(generation(), expires_ms=200, now_ms=1)
    choose(store, a)
    old = store.desired("discovery")
    with pytest.raises(ValueError, match="protected_omission"):
        choose(store, a, protected=("2",))
    with pytest.raises(ValueError, match="candidate_expired"):
        choose(store, a, now=100)
    assert store.desired("discovery") == old


def test_live_parent_and_protected_exception(store):
    a = store.register(generation(), expires_ms=1000, now_ms=1)
    live = store.register(generation("live", "live", ("2",)), expires_ms=1000, now_ms=1)
    with pytest.raises(ValueError, match="parent_selection_mismatch"):
        choose(store, live, "live")
    choose(store, a)
    with pytest.raises(ValueError, match="parent_selection_mismatch"):
        choose(store, live, "live", protected=("2",))
    ack(store, a)
    with pytest.raises(ValueError, match="unprotected_parent_exception"):
        choose(store, live, "live")
    choose(store, live, "live", protected=("2",))
    assert store.desired("discovery")["epoch"] == 1


def test_live_cannot_bind_parent_during_unapplied_transition(store):
    a = store.register(generation(), expires_ms=1000, now_ms=1)
    b = store.register(generation("b"), expires_ms=1000, now_ms=1)
    live = store.register(generation("live", "live"), expires_ms=1000, now_ms=1)
    choose(store, a)
    ack(store, a)
    choose(store, b)
    with pytest.raises(ValueError, match="parent_transition_pending"):
        choose(store, live, "live")
    assert store.desired("live") is None


def test_no_eviction_of_valid_candidate(store):
    for name in ("a", "b", "c"):
        store.register(generation(name), expires_ms=1000, now_ms=1)
    with pytest.raises(ValueError, match="generation capacity"):
        store.register(generation("d"), expires_ms=1000, now_ms=1)


@pytest.mark.parametrize("pool,count", [("discovery", 1000), ("live", 250)])
def test_pool_budgets_are_separate(pool, count):
    ids = tuple(sorted(str(i) for i in range(count)))
    generation(pool=pool, ids=ids)
    with pytest.raises(ValueError, match="capacity"):
        generation(pool=pool, ids=tuple(sorted((*ids, "extra"))))


def test_corruption_fails_before_desired_change(store):
    gid = store.register(generation(), expires_ms=1000, now_ms=1)
    store.db.execute("UPDATE generations SET body=replace(cast(body as text),'\"baseline_cursor\":100','\"baseline_cursor\":99')")
    with pytest.raises(ValueError, match="identity_mismatch"):
        choose(store, gid)
    assert store.desired("discovery") is None


def ack(store, gid, epoch=1, instance="a", **kw):
    return store.acknowledge("discovery", generation_id=gid, epoch=epoch,
                             stream_instance_id=instance, baseline_cursor=100, ready_cursor=101,
                             endpoint="http://127.0.0.1:8795", now_ms=kw.get("now", 10))


def test_applied_receipt_is_separate_and_restart_durable(store, tmp_path):
    a = store.register(generation(), expires_ms=1000, now_ms=1)
    choose(store, a)
    assert store.applied("discovery") is None
    receipt = ack(store, a)
    assert ack(store, a) == receipt
    other = GenerationStore(tmp_path/"generations.sqlite", maximum_generations=3, maximum_record_bytes=10000)
    try:
        assert other.applied("discovery") == receipt
    finally:
        other.close()
    b = store.register(generation("b"), expires_ms=1000, now_ms=10)
    choose(store, b)
    assert store.applied("discovery") == receipt
    with pytest.raises(ValueError, match="stale_runtime_receipt"):
        ack(store, a)
    with pytest.raises(ValueError, match="runtime_instance_mismatch"):
        ack(store, b, epoch=2, instance="")
    assert store.applied("discovery") == receipt


def test_applied_root_not_evicted_while_successor_pending(store):
    a = store.register(generation(), expires_ms=11, now_ms=1)
    choose(store, a)
    ack(store, a)
    b = store.register(generation("b"), expires_ms=1000, now_ms=10)
    choose(store, b)
    store.register(generation("c"), expires_ms=1000, now_ms=12)
    with pytest.raises(ValueError, match="generation capacity"):
        store.register(generation("d"), expires_ms=1000, now_ms=12)
    assert store.applied("discovery")["generation_id"] == a
