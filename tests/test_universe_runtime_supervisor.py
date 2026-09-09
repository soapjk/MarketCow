"""Fault-injected adapters: not a real Rust launch or full-sync test."""
import asyncio

import pytest

from marketcow.universe_generation import Generation, GenerationStore
from marketcow.universe_runtime_supervisor import RuntimeBoundary, RuntimeSupervisor


class Runtime:
    def __init__(self, fail=None):
        self.calls, self.fail = [], fail

    async def prepare(self, g):
        self.calls.append("prepare")
        if self.fail == "prepare":
            raise RuntimeError("prepare")

    async def publish(self, g, epoch):
        self.calls.append("publish")

    async def probe(self, g, epoch):
        self.calls.append("probe")
        if self.fail == "probe":
            raise RuntimeError("probe")
        return RuntimeBoundary(g.stream_instance_id, 100, 101, "http://127.0.0.1:8795")


@pytest.fixture
def state(tmp_path):
    s = GenerationStore(tmp_path/"state.sqlite", maximum_generations=3, maximum_record_bytes=10000)
    g = Generation(pool="discovery", selection_id="s", catalog_revision="a"*64,
                   artifact_sha256="b"*64, market_ids=("1",), dependency_market_ids=(),
                   protected_market_ids=(), parent_selection_id=None,
                   stream_instance_id="instance", baseline_cursor=100)
    gid = s.register(g, expires_ms=1000, now_ms=1)
    s.select("discovery", gid, expected=None, protected_market_ids=(), now_ms=2)
    yield s
    s.close()


def test_apply_and_probe_only_reconciliation(state):
    runtime = Runtime()
    supervisor = RuntimeSupervisor(state, runtime, operation_timeout_seconds=1)
    result = asyncio.run(supervisor.apply("discovery", now_ms=lambda: 3))
    assert runtime.calls == ["prepare", "publish", "probe"]
    runtime.calls.clear()
    assert asyncio.run(supervisor.reconcile("discovery", now_ms=lambda: 4)) == result
    assert runtime.calls == ["probe"]


@pytest.mark.parametrize("failure", ["prepare", "probe"])
def test_failure_never_acknowledges(state, failure):
    runtime = Runtime(failure)
    supervisor = RuntimeSupervisor(state, runtime, operation_timeout_seconds=1)
    with pytest.raises(RuntimeError):
        asyncio.run(supervisor.apply("discovery", now_ms=lambda: 3))
    assert state.applied("discovery") is None
    if failure == "prepare":
        assert runtime.calls == ["prepare"]
    else:
        runtime.fail = None
        asyncio.run(supervisor.reconcile("discovery", now_ms=lambda: 4))
        assert runtime.calls == ["prepare", "publish", "probe", "probe"]


def test_superseded_preheat_never_publishes(state):
    runtime = Runtime()

    async def prepare(g):
        state.select("discovery", g.generation_id, expected=state.desired("discovery"),
                     protected_market_ids=(), now_ms=3)
    runtime.prepare = prepare
    supervisor = RuntimeSupervisor(state, runtime, operation_timeout_seconds=1)
    with pytest.raises(ValueError, match="superseded"):
        asyncio.run(supervisor.apply("discovery", now_ms=lambda: 4))
    assert runtime.calls == []
    assert state.applied("discovery") is None
