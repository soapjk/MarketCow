"""Single-owner control orchestration; never relays market data.

Runtime adapters own preheat, direct-Rust publication and full-sync/WS probing.
Every external action is followed by an epoch fence. A failed probe leaves the
last verified applied receipt intact; reconciliation probes before any retry.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from marketcow.universe_generation import Generation, GenerationStore


@dataclass(frozen=True)
class RuntimeBoundary:
    stream_instance_id: str
    baseline_cursor: int
    ready_cursor: int
    endpoint: str
    identity_kind: str = "stream_instance"


class RustRuntime(Protocol):
    async def prepare(self, generation: Generation) -> None: ...
    async def publish(self, generation: Generation, epoch: int) -> None: ...
    async def probe(self, generation: Generation, epoch: int) -> RuntimeBoundary: ...


class RuntimeSupervisor:
    """Requires an exclusive lifetime owner lock supplied by its launcher.

    This deliberately does not spawn retries or background tasks. The caller
    gives a finite timeout; cancellation is propagated. Runtime publish must be
    idempotent and epoch-fenced itself, including across supervisor restarts.
    """
    def __init__(self, store: GenerationStore, runtime: RustRuntime, *, operation_timeout_seconds: float):
        if not 0 < operation_timeout_seconds < float("inf"):
            raise ValueError("explicit finite operation deadline required")
        self.store, self.runtime, self.timeout = store, runtime, operation_timeout_seconds

    def _generation(self, pool):
        desired = self.store.desired(pool)
        if desired is None:
            raise ValueError("no_desired_generation")
        row = self.store.db.execute("SELECT body FROM generations WHERE id=?", (desired["generation_id"],)).fetchone()
        generation = Generation.model_validate_json(row[0])
        if generation.generation_id != desired["generation_id"] or generation.pool != pool:
            raise ValueError("generation_identity_mismatch")
        return desired, generation

    def _fence(self, pool, desired):
        if self.store.desired(pool) != desired:
            raise ValueError("superseded_generation")

    async def apply(self, pool, *, now_ms):
        import asyncio
        desired, generation = self._generation(pool)
        async with asyncio.timeout(self.timeout):
            await self.runtime.prepare(generation)
            self._fence(pool, desired)
            await self.runtime.publish(generation, desired["epoch"])
            self._fence(pool, desired)
            boundary = await self.runtime.probe(generation, desired["epoch"])
            self._fence(pool, desired)
        return self.store.acknowledge(pool, **desired, stream_instance_id=boundary.stream_instance_id,
                                     baseline_cursor=boundary.baseline_cursor, ready_cursor=boundary.ready_cursor,
                                     endpoint=boundary.endpoint, now_ms=now_ms(), identity_kind=boundary.identity_kind)

    async def reconcile(self, pool, *, now_ms):
        """Read actual runtime after restart, including publish-before-ack crash.

        Probe failure does not auto-publish/restart a service or claim rollback;
        an explicit apply or rollback decision is required by the owner.
        """
        import asyncio
        desired, generation = self._generation(pool)
        async with asyncio.timeout(self.timeout):
            boundary = await self.runtime.probe(generation, desired["epoch"])
        self._fence(pool, desired)
        previous = self.store.applied(pool)
        if previous and (previous["generation_id"], previous["epoch"]) == (desired["generation_id"], desired["epoch"]):
            if (boundary.stream_instance_id != previous["stream_instance_id"]
                    or boundary.identity_kind != previous["identity_kind"]
                    or boundary.endpoint != previous["endpoint"]
                    or boundary.baseline_cursor < previous["baseline_cursor"]
                    or boundary.ready_cursor < boundary.baseline_cursor):
                raise ValueError("runtime_reconciliation_mismatch")
            return previous
        return self.store.acknowledge(pool, **desired, stream_instance_id=boundary.stream_instance_id,
                                     baseline_cursor=boundary.baseline_cursor, ready_cursor=boundary.ready_cursor,
                                     endpoint=boundary.endpoint, now_ms=now_ms(), identity_kind=boundary.identity_kind)
