"""Turn verified operator artifacts into a real prepared-generation record.

The launcher must hold SupervisorOwner across this call. No desired pointer or
formal unit changes occur here. Admission verification precedes artifact creation.
"""
import asyncio
from dataclasses import replace

from marketcow.universe_generation import Generation
from marketcow.universe_systemd import SystemdDiscoveryRuntime, SystemdLiveRuntime


async def preheat_and_register(store, units, binding, *, pool, selection_id,
        catalog_revision, market_ids, dependency_market_ids, protected_market_ids,
        parent_selection_id, expires_ms, now_ms, operation_timeout_seconds):
    if not 0 < operation_timeout_seconds < float("inf") or expires_ms <= now_ms():
        raise ValueError("finite unexpired preparation window required")
    # Provisional identity is never installed in the store or exposed as ready.
    provisional = Generation(pool=pool, selection_id=selection_id,
        catalog_revision=catalog_revision, artifact_sha256=binding.artifact_digest(),
        market_ids=market_ids, dependency_market_ids=dependency_market_ids,
        protected_market_ids=protected_market_ids, parent_selection_id=parent_selection_id,
        stream_instance_id="unpublished-preheat", baseline_cursor=0)
    runtime_binding = replace(binding, generation_id=provisional.generation_id)
    kind = SystemdLiveRuntime if pool == "live" else SystemdDiscoveryRuntime
    runtime = kind(units, runtime_binding)
    try:
        async with asyncio.timeout(operation_timeout_seconds):
            await runtime.prepare(provisional)
            boundary = runtime.boundary
            if boundary is None:
                raise ValueError("missing actual preheat boundary")
    finally:
        # Preparation failures and caller cancellation must not leave a second
        # writer behind. Preserve the original failure if cleanup itself fails.
        cleanup = asyncio.create_task(units.stop(binding.preheat))
        try:
            stopped = await asyncio.shield(cleanup)
        except asyncio.CancelledError:
            await cleanup
            raise
    if stopped.get("Result") != "success" or stopped.get("ExecMainStatus") != "0":
        raise ValueError("preheat did not stop cleanly")
    generation = provisional.model_copy(update={"stream_instance_id": boundary.stream_instance_id,
                                               "baseline_cursor": boundary.baseline_cursor})
    gid = store.register(generation, expires_ms=expires_ms, now_ms=now_ms())
    return generation, replace(binding, generation_id=gid), boundary
