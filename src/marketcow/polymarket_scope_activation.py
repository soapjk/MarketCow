"""Transactional activation coordinator for prepared live.v2 generations.

Reuses the existing scope:activate request and durable atomic pointer primitive.
The runtime factory owns Rust preheating; callers cannot supply commands or paths.
No ranking, replacement selection, account, or order operation belongs here.
"""
from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Awaitable, Callable, Literal

from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel, ConfigDict, Field

from .polymarket_scopes import _atomic_replace


class ActivationRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    schema_version: Literal["marketcow.polymarket.scope-activation.v1"]
    scope_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    scope_file_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")


@dataclass
class LiveGeneration:
    scope_id: str
    artifact_sha256: str
    app: object
    market_ids: tuple[str, ...]
    verify: Callable[[], Awaitable[int]]
    close: Callable[[], Awaitable[None]]
    retired: asyncio.Event = field(default_factory=asyncio.Event)
    requests: int = 0
    drained: asyncio.Event = field(default_factory=asyncio.Event)


class ScopeActivationGateway:
    """One-process atomic dispatch; old HTTP requests keep their immutable view.

    The factory must return a separately warmed runtime, never alter the incumbent.
    Every WS is retired on switch. A failed candidate is closed without publication.
    Startup must resolve the durable pointer before constructing the incumbent.
    """

    path = "/v1/admin/polymarket/scope:activate"

    def __init__(self, *, active: LiveGeneration, registry_root: Path,
                 admin_token: str, factory: Callable[[dict, str], Awaitable[LiveGeneration]],
                 activation_timeout_seconds: float, verification_interval_seconds: float):
        if len(admin_token) < 32 or activation_timeout_seconds <= 0 or verification_interval_seconds <= 0:
            raise ValueError("explicit credential, activation timeout and verification interval required")
        self.active = active
        self.root = registry_root.resolve(strict=True)
        self.token = admin_token
        self.factory = factory
        self.timeout = activation_timeout_seconds
        self.interval = verification_interval_seconds
        self.lock = asyncio.Lock()
        self.retirements: set[asyncio.Task] = set()
        self.admin = FastAPI()
        self.admin.post(self.path)(self.activate)

    def registered(self, request: ActivationRequest) -> dict:
        path = self.root / f"{request.scope_id}.json"
        if path.is_symlink() or not path.is_file() or not path.resolve().is_relative_to(self.root):
            raise HTTPException(422, "scope_artifact_not_registered")
        if path.stat().st_size > 1024 * 1024:
            raise HTTPException(413, "scope_artifact_too_large")
        body = path.read_bytes()
        if hashlib.sha256(body).hexdigest() != request.scope_file_sha256:
            raise HTTPException(422, "scope_artifact_hash_mismatch")
        artifact = json.loads(body)
        if artifact["scope_id"] != request.scope_id:
            raise HTTPException(422, "scope_artifact_identity_mismatch")
        return artifact

    async def activate(self, request: Request, body: ActivationRequest):
        supplied = request.headers.get("authorization", "")
        if not hmac.compare_digest(supplied, "Bearer " + self.token):
            raise HTTPException(401, "admin_authentication_required")
        if self.lock.locked():
            raise HTTPException(409, "scope_activation_in_progress")
        async with self.lock:
            artifact = self.registered(body)
            if (self.active.scope_id, self.active.artifact_sha256) == (body.scope_id, body.scope_file_sha256):
                return {"status": "already_active", "active_scope_id": body.scope_id}
            candidate = None
            try:
                async with asyncio.timeout(self.timeout):
                    candidate = await self.factory(artifact, body.scope_file_sha256)
                    if candidate.scope_id != body.scope_id or candidate.artifact_sha256 != body.scope_file_sha256:
                        raise ValueError("candidate_identity_mismatch")
                    if not 1 <= len(candidate.market_ids) <= 250 or len(set(candidate.market_ids)) != len(candidate.market_ids):
                        raise ValueError("candidate_market_count_invalid")
                    first = await candidate.verify()
                    await asyncio.sleep(self.interval)
                    boundary = await candidate.verify()
                    if type(first) is not int or type(boundary) is not int or first < 0 or boundary <= first:
                        raise ValueError("candidate_cursor_not_advancing")
                    pointer = {"schema_version": "marketcow.polymarket.active-live-generation.v1",
                               "active_scope_id": body.scope_id, "scope_file_sha256": body.scope_file_sha256,
                               "previous_scope_id": self.active.scope_id, "boundary_cursor": boundary}
                    # No await between durable publication and the in-process pointer swap.
                    _atomic_replace(self.root / "active-live-generation.json", pointer)
                    previous, self.active = self.active, candidate
                    candidate = None
                    previous.retired.set()
                    task = asyncio.create_task(self._retire(previous))
                    self.retirements.add(task)
                    task.add_done_callback(self.retirements.discard)
                    return {**pointer, "schema_version": "marketcow.polymarket.scope-activation-receipt.v1",
                            "status": "activated_ready",
                            "configured_market_count": len(self.active.market_ids), "full_sync_required": True}
            except (ValueError, TimeoutError) as error:
                raise HTTPException(409, f"candidate_not_ready:{error}") from error
            finally:
                if candidate is not None:
                    await asyncio.shield(candidate.close())

    async def _retire(self, generation: LiveGeneration):
        # Give WS dispatchers a turn to send scope_changed before closing the runtime.
        await asyncio.sleep(0)
        if generation.requests:
            await generation.drained.wait()
        await generation.close()

    async def __call__(self, scope, receive, send):
        if scope["type"] == "lifespan":
            while True:
                message = await receive()
                if message["type"] == "lifespan.startup":
                    await send({"type": "lifespan.startup.complete"})
                elif message["type"] == "lifespan.shutdown":
                    self.active.retired.set()
                    await self.active.close()
                    await asyncio.gather(*self.retirements, return_exceptions=True)
                    await send({"type": "lifespan.shutdown.complete"})
                    return
        if scope["path"] == self.path:
            return await self.admin(scope, receive, send)
        generation = self.active
        if scope["type"] != "websocket":
            generation.requests += 1
            generation.drained.clear()
            try:
                return await generation.app(scope, receive, send)
            finally:
                generation.requests -= 1
                if not generation.requests:
                    generation.drained.set()
        accepted = False
        async def tracked_send(message):
            nonlocal accepted
            if message["type"] == "websocket.accept":
                accepted = True
            await send(message)
        serving = asyncio.create_task(generation.app(scope, receive, tracked_send))
        retired = asyncio.create_task(generation.retired.wait())
        try:
            done, _ = await asyncio.wait((serving, retired), return_when=asyncio.FIRST_COMPLETED)
            if serving in done:
                await serving
            else:
                serving.cancel()
                await asyncio.gather(serving, return_exceptions=True)
                if accepted:
                    await send({"type": "websocket.send", "text": json.dumps({"type": "resync_required", "reason": "scope_changed", "active_scope_id": self.active.scope_id})})
                await send({"type": "websocket.close", "code": 1012})
        finally:
            serving.cancel()
            retired.cancel()
            await asyncio.gather(serving, retired, return_exceptions=True)
