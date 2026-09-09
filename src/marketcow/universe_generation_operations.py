"""Operator-registered runtime operations, not arbitrary client deployment input.

Only registered immutable operator config references can be addressed. A client
can request a CAS transition, never supply a source path, executable or unit.
"""
import asyncio
import json
import time
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from marketcow.universe_control_server import read_json
from marketcow.universe_generation import Generation, GenerationStore
from marketcow.universe_owner import SupervisorOwner
from marketcow.universe_phase1 import selection_sha256
from marketcow.universe_runtime_command import parse_binding
from marketcow.universe_runtime_supervisor import RuntimeSupervisor
from marketcow.universe_systemd import SystemdUnits, SystemdLiveRuntime, SystemdDiscoveryRuntime


class ExpectedGeneration(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    generation_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    epoch: int = Field(gt=0)


class ApplyGeneration(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    schema_version: Literal["marketcow.polymarket.generation-apply.v1"]
    operation: Literal["activate", "rollback", "reconcile"]
    pool: Literal["discovery", "live"]
    generation_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    expected: ExpectedGeneration | None
    protected_market_ids: list[str] = Field(max_length=1000)

    @model_validator(mode="after")
    def identities(self):
        ids = self.protected_market_ids
        if ids != sorted(set(ids)) or any(not x or not x.isascii() for x in ids):
            raise ValueError("invalid protection identities")
        return self


def parse_operation(raw):
    def unique(pairs):
        value = {}
        for key, item in pairs:
            if key in value:
                raise ValueError("duplicate operation field")
            value[key] = item
        return value
    return ApplyGeneration.model_validate(json.loads(raw, object_pairs_hook=unique))


class GenerationOperations:
    def __init__(self, *, store_path, owner_lock, maximum_generations, maximum_record_bytes, registrations):
        self.path, self.lock = Path(store_path), Path(owner_lock)
        if not self.path.is_absolute() or not self.path.is_file() or not self.lock.is_absolute():
            raise ValueError("existing operator store and absolute owner lock required")
        if type(registrations) is not dict or len(registrations) > maximum_generations:
            raise ValueError("bounded operator registration map required")
        self.maximum, self.bytes = maximum_generations, maximum_record_bytes
        self.registrations = dict(registrations)
        self.registration_provider = None

    def _store(self):
        return GenerationStore(self.path, maximum_generations=self.maximum, maximum_record_bytes=self.bytes)

    def status(self, pool):
        if pool not in ("discovery", "live"):
            raise ValueError("invalid pool")
        with SupervisorOwner(self.lock):
            store = self._store()
            try:
                return {"schema_version": "marketcow.polymarket.generation-status.v1", "pool": pool,
                        "desired": store.desired(pool), "applied": store.applied(pool)}
            finally:
                store.close()

    def apply(self, request: ApplyGeneration):
        registration = self.registrations.get(request.generation_id)
        if registration is None and self.registration_provider is not None:
            registration = self.registration_provider(request.generation_id)
        if registration is None or set(registration) != {"path", "sha256"}:
            raise ValueError("generation_not_registered")
        config = read_json(Path(registration["path"]), 65536)
        if selection_sha256(config) != registration["sha256"]:
            raise ValueError("operation_config_hash_mismatch")
        fields = {"pool", "store_path", "owner_lock", "maximum_generations", "maximum_record_bytes",
            "release_root", "user_unit_root", "command_timeout_seconds", "operation_timeout_seconds", "binding"}
        if set(config) != fields or (config["pool"], config["store_path"], config["owner_lock"],
            config["maximum_generations"], config["maximum_record_bytes"]) != (
                request.pool, str(self.path), str(self.lock), self.maximum, self.bytes):
            raise ValueError("operation_store_binding_mismatch")
        binding = parse_binding(config["binding"], request.pool)
        if binding.generation_id != request.generation_id:
            raise ValueError("operation_generation_mismatch")
        # One lifetime lock spans intent CAS, preheat, publication and durable
        # receipt. No secondary process can select a parent during publication.
        with SupervisorOwner(self.lock):
            store = self._store()
            try:
                row = store.db.execute("SELECT body FROM generations WHERE id=?", (request.generation_id,)).fetchone()
                if row is None:
                    raise ValueError("generation_not_registered")
                generation = Generation.model_validate_json(row[0])
                if not set(generation.protected_market_ids) <= set(request.protected_market_ids):
                    raise ValueError("protected_omission")
                units = SystemdUnits(release_root=Path(config["release_root"]), user_unit_root=Path(config["user_unit_root"]),
                                     command_timeout_seconds=config["command_timeout_seconds"])
                kind = SystemdLiveRuntime if request.pool == "live" else SystemdDiscoveryRuntime
                runtime = kind(units, binding)
                runtime._check(generation)  # Verify trusted files before recording intent.
                supervisor = RuntimeSupervisor(store, runtime, operation_timeout_seconds=config["operation_timeout_seconds"])
                expected = request.expected.model_dump() if request.expected else None
                if request.operation == "reconcile":
                    if store.desired(request.pool) != expected or expected is None or expected["generation_id"] != request.generation_id:
                        raise ValueError("incumbent_conflict")
                    method = supervisor.reconcile
                else:
                    store.select(request.pool, request.generation_id, expected=expected,
                        protected_market_ids=tuple(request.protected_market_ids), now_ms=time.time_ns()//1000000)
                    method = supervisor.apply
                # CAS is not success. Only actual baseline+WS verification can
                # produce the applied receipt returned by this operation.
                return asyncio.run(method(request.pool, now_ms=lambda: time.time_ns()//1000000))
            finally:
                store.close()
