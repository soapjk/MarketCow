"""Opt-in loopback control routes. Not mounted on the public data API."""
from __future__ import annotations

import asyncio
import hashlib
import hmac
import ipaddress
import json
import sqlite3
import threading
from dataclasses import dataclass

from fastapi import FastAPI, Request
from fastapi.responses import Response

from marketcow.universe_control import ControlError, UniverseControl, wire_bytes
from marketcow.universe_phase1 import RequestValidationError, parse_request_bytes


PREFIX = "/v1/prediction-markets/polymarket"


@dataclass(frozen=True)
class Caller:
    identity: str
    bearer_sha256: str
    scopes: frozenset[str]


def create_control_app(control: UniverseControl, *, callers: tuple[Caller, ...],
                       body_timeout_seconds: float, runtime_operations=None, discovery_preparation=None, hot_operations=None) -> FastAPI:
    if hot_operations is not None and (runtime_operations is not None or discovery_preparation is not None):
        raise ValueError("hot scope and cold replacement controls are mutually exclusive")
    if not callers or len(callers) > 32 or not 0 < body_timeout_seconds <= 60:
        raise ValueError("explicit caller and bounded body-time profile required")
    if len({c.identity for c in callers}) != len(callers) or len({c.bearer_sha256 for c in callers}) != len(callers):
        raise ValueError("duplicate caller binding")
    for caller in callers:
        if (not caller.identity or len(caller.bearer_sha256) != 64
                or any(c not in "0123456789abcdef" for c in caller.bearer_sha256)
                or not caller.scopes <= ({"catalog.read", "discovery.admit"} |
                    ({"runtime.read", "runtime.activate"} if runtime_operations is not None else set()) |
                    ({"discovery.prepare"} if discovery_preparation is not None else set()) |
                    ({"hot.read", "hot.prepare", "hot.activate", "hot.retire"} if hot_operations is not None else set()))):
            raise ValueError("invalid scoped caller")
    app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)
    slots = threading.BoundedSemaphore(control.profile["catalog"]["concurrent_readers"])

    def authenticate(request: Request, scope: str) -> str:
        try:
            if request.client is None or not ipaddress.ip_address(request.client.host).is_loopback:
                raise ValueError("not loopback")
        except ValueError:
            raise ControlError("forbidden", 403) from None
        # Ignore forwarded headers: they never establish a trusted caller.
        headers = request.headers.getlist("authorization")
        if len(headers) != 1 or not headers[0].startswith("Bearer ") or len(headers[0]) > 512:
            raise ControlError("unauthorized", 401)
        digest = hashlib.sha256(headers[0][7:].encode("utf-8")).hexdigest()
        match = next((c for c in callers if hmac.compare_digest(c.bearer_sha256, digest)), None)
        if match is None:
            raise ControlError("unauthorized", 401)
        if scope not in match.scopes:
            raise ControlError("forbidden", 403)
        return match.identity

    def query(request: Request, fields: set[str]) -> dict:
        items = list(request.query_params.multi_items())
        result = dict(items)
        if len(items) != len(result) or set(result) != fields:
            raise ControlError("invalid_schema", 400)
        return result

    async def handle(request: Request, operation: str) -> Response:
        acquired = False
        transferred = False
        rid = None
        details = {}
        try:
            caller = authenticate(request, "discovery.admit" if operation == "admit" else "catalog.read")
            if not slots.acquire(blocking=False):
                raise ControlError("resource_unavailable", 429, retryable=True)
            acquired = True
            if operation == "admit":
                query(request, set())
                if request.headers.get("content-encoding", "identity") != "identity":
                    raise ControlError("invalid_schema", 400)
                maximum = control.profile["admission"]["max_request_bytes"]

                async def read_body():
                    body = bytearray()
                    async for chunk in request.stream():
                        if len(body) + len(chunk) > maximum:
                            raise ControlError("request_size_exceeded", 413)
                        body.extend(chunk)
                    return bytes(body)

                payload = parse_request_bytes(await asyncio.wait_for(read_body(), body_timeout_seconds), maximum_bytes=maximum)
                rid = payload.request_id
                action = lambda: control.admit(caller, payload)
            elif operation in {"snapshot", "snapshot_v2"}:
                params = query(request, {"page_size"})
                page_size = int(params["page_size"])
                action = lambda: control.snapshot(page_size, include_change_sequence=operation == 'snapshot_v2')
            elif operation == "changes":
                params = query(request, {"after_sequence", "limit"})
                after, limit = int(params["after_sequence"]), int(params["limit"])
                action = lambda: control.changes(after, limit)
            elif operation == "catalog_status":
                query(request, set())
                action = control.catalog_status
            else:
                params = query(request, {"snapshot_id", "page_token", "limit"})
                limit = int(params["limit"])
                action = lambda: control.page(params["snapshot_id"], params["page_token"], limit)

            def run():
                try:
                    return action()
                except ControlError:
                    raise
                except sqlite3.Error:
                    raise ControlError("resource_unavailable", 503, retryable=True) from None
                except ValueError:
                    raise ControlError("catalog_source_invalid", 503) from None
                finally:
                    slots.release()

            # A cancelled client must not free its slot while SQLite work is
            # still running. The worker owns release once submitted.
            worker = asyncio.create_task(asyncio.to_thread(run))
            worker.add_done_callback(lambda future: None if future.cancelled() else future.exception())
            transferred = True
            body = await asyncio.shield(worker)
            return Response(body, media_type="application/json")
        except ControlError as error:
            code, status, retryable = error.code, error.status, error.retryable
            details = error.details
        except RequestValidationError as error:
            code, status, retryable = str(error), 400, False
        except (ValueError, TypeError):
            code, status, retryable = "invalid_schema", 400, False
        except TimeoutError:
            code, status, retryable = "resource_unavailable", 503, True
        finally:
            if acquired and not transferred:
                slots.release()
        return Response(wire_bytes({"schema_version": "marketcow.catalog-selection-error.v1",
            "code": code, "retryable": retryable, "request_id": rid, "details": details}),
            status_code=status, media_type="application/json")

    @app.get(PREFIX + "/catalog/snapshot")
    async def snapshot(request: Request):
        return await handle(request, "snapshot")

    @app.get(PREFIX + "/catalog/page")
    async def page(request: Request):
        return await handle(request, "page")

    @app.get(PREFIX + "/catalog/snapshot-v2")
    async def snapshot_v2(request: Request):
        return await handle(request, "snapshot_v2")

    @app.get(PREFIX + "/catalog/changes")
    async def changes(request: Request):
        return await handle(request, "changes")

    @app.get(PREFIX + "/catalog/status")
    async def catalog_status(request: Request):
        return await handle(request, "catalog_status")

    @app.post(PREFIX + "/discovery-selections/admit")
    async def admit(request: Request):
        return await handle(request, "admit")

    if runtime_operations is not None or discovery_preparation is not None or hot_operations is not None:
        runtime_slot = threading.BoundedSemaphore(1)

        async def runtime_handle(request, operation):
            acquired = transferred = False
            try:
                scope = {"status": "runtime.read", "apply": "runtime.activate", "prepare": "discovery.prepare",
                    "hot_status": "hot.read", "hot_discovery": "hot.prepare", "hot_live": "hot.prepare",
                    "hot_activate": "hot.activate", "hot_retire": "hot.retire", "hot_collect": "hot.retire", "hot_reconcile": "hot.prepare"}[operation]
                caller = authenticate(request, scope)
                if not runtime_slot.acquire(blocking=False):
                    raise ControlError("resource_unavailable", 429, retryable=True)
                acquired = True
                if operation in ("status", "hot_status"):
                    params = query(request, {"pool"})
                    if params["pool"] not in ("discovery", "live"):
                        raise ControlError("invalid_schema", 400)
                    target = hot_operations if operation == "hot_status" else runtime_operations
                    action = lambda: target.status(params["pool"])
                else:
                    from marketcow.universe_generation_operations import parse_operation
                    query(request, set())
                    if request.headers.get("content-encoding", "identity") != "identity":
                        raise ControlError("invalid_schema", 400)
                    async def read_operation():
                        raw = bytearray()
                        maximum = control.profile["admission"]["max_request_bytes"]+65536 if operation in ("prepare", "hot_discovery") else 65536
                        async for chunk in request.stream():
                            if len(raw)+len(chunk) > maximum:
                                raise ControlError("request_size_exceeded", 413)
                            raw.extend(chunk)
                        if operation in ("prepare", "hot_discovery"):
                            from marketcow.universe_discovery_preparation import parse_preparation_request
                            return parse_preparation_request(raw, maximum)
                        if operation.startswith("hot_"):
                            from marketcow.universe_rust_control import _unique
                            def invalid_constant(_):
                                raise ValueError("nonfinite JSON")
                            result = json.loads(raw, object_pairs_hook=_unique, parse_constant=invalid_constant)
                            if not isinstance(result, dict):
                                raise ValueError("object required")
                            return result
                        return parse_operation(raw)
                    payload = await asyncio.wait_for(read_operation(), body_timeout_seconds)
                    if operation == "hot_discovery":
                        action = lambda: hot_operations.prepare_discovery(caller, *payload)
                    elif operation in ("hot_live", "hot_activate", "hot_retire", "hot_reconcile", "hot_collect"):
                        method_name = {"hot_live": "prepare_live", "hot_activate": "activate", "hot_retire": "retire",
                                       "hot_reconcile": "reconcile", "hot_collect": "collect"}[operation]
                        method = getattr(hot_operations, method_name)
                        action = lambda: method(caller, payload)
                    else:
                        action = (lambda: discovery_preparation.prepare(caller, *payload)) if operation == "prepare" else (lambda: runtime_operations.apply(payload))

                def run():
                    try:
                        body = wire_bytes(action())
                        response_cap = control.profile["admission"]["max_response_bytes"] if operation.startswith("hot_") else 65536
                        if len(body) > response_cap:
                            raise ValueError("runtime response byte cap")
                        return body
                    finally:
                        runtime_slot.release()
                worker = asyncio.create_task(asyncio.to_thread(run))
                worker.add_done_callback(lambda f: None if f.cancelled() else f.exception())
                transferred = True
                return Response(await asyncio.shield(worker), media_type="application/json")
            except ControlError as error:
                code, status, retryable = error.code, error.status, error.retryable
            except (ValueError, TypeError):
                # A failed operation may already have persisted desired intent.
                # Never implicitly retry or call it a clean unchanged failure.
                code, status, retryable = ("runtime_operation_rejected", 409, False) if transferred else ("invalid_schema", 400, False)
            except (RuntimeError, OSError, TimeoutError, sqlite3.Error):
                code, status, retryable = "runtime_state_requires_reconciliation", 503, False
            finally:
                if acquired and not transferred:
                    runtime_slot.release()
            return Response(wire_bytes({"schema_version": "marketcow.generation-operation-error.v1",
                "code": code, "retryable": retryable, "reconcile_required": transferred}), status_code=status,
                media_type="application/json")

        if runtime_operations is not None:
            @app.get(PREFIX + "/generations/status")
            async def generation_status(request: Request):
                return await runtime_handle(request, "status")

            @app.post(PREFIX + "/generations/apply")
            async def generation_apply(request: Request):
                return await runtime_handle(request, "apply")

        if discovery_preparation is not None:
            @app.post(PREFIX + "/discovery-selections/prepare")
            async def discovery_prepare(request: Request):
                return await runtime_handle(request, "prepare")

        if hot_operations is not None:
            @app.get(PREFIX + "/hot-scopes/status")
            async def hot_status(request: Request):
                return await runtime_handle(request, "hot_status")

            @app.post(PREFIX + "/hot-scopes/discovery/prepare")
            async def hot_discovery(request: Request):
                return await runtime_handle(request, "hot_discovery")

            @app.post(PREFIX + "/hot-scopes/live/prepare")
            async def hot_live(request: Request):
                return await runtime_handle(request, "hot_live")

            @app.post(PREFIX + "/hot-scopes/activate")
            async def hot_activate(request: Request):
                return await runtime_handle(request, "hot_activate")

            @app.post(PREFIX + "/hot-scopes/retire")
            async def hot_retire(request: Request):
                return await runtime_handle(request, "hot_retire")

            @app.post(PREFIX + "/hot-scopes/reconcile")
            async def hot_reconcile(request: Request):
                return await runtime_handle(request, "hot_reconcile")

            @app.post(PREFIX + "/hot-scopes/collect")
            async def hot_collect(request: Request):
                return await runtime_handle(request, "hot_collect")

    return app
