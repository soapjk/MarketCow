"""Finite direct-Rust live identity/full-sync/ready probe, no Paper execution.

This checks publication binding and stream ordering, not every book's strategy
eligibility. Payload integrity remains the source/consumer decoder's contract.
"""
import asyncio
import json
from urllib.parse import urlencode, urlsplit

import httpx
from websockets.asyncio.client import connect

from marketcow.universe_runtime_supervisor import RuntimeBoundary


def strict_json(raw):
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError("duplicate wire key")
            result[key] = value
        return result
    def invalid(_):
        raise ValueError("nonfinite wire number")
    return json.loads(raw, object_pairs_hook=pairs, parse_constant=invalid)


async def probe_live(endpoint, *, scope_id, catalog_revision, market_ids,
                     timeout_seconds, full_sync_bytes, frame_bytes, maximum_frames,
                     maximum_stream_bytes):
    for value in (full_sync_bytes, frame_bytes, maximum_frames, maximum_stream_bytes):
        if type(value) is not int or value <= 0:
            raise ValueError("explicit positive probe budget")
    if not 0 < timeout_seconds < float("inf"):
        raise ValueError("explicit finite probe timeout")
    url = urlsplit(endpoint)
    if url.scheme not in ("http", "https") or not url.hostname or url.username or url.password or url.query or url.fragment:
        raise ValueError("invalid probe endpoint")
    prefix = endpoint.rstrip("/") + "/v1/prediction-markets/polymarket/live"
    async with asyncio.timeout(timeout_seconds):
        async with httpx.AsyncClient(trust_env=False, follow_redirects=False, timeout=timeout_seconds) as client:
            async with client.stream("GET", prefix+"/full-sync", params={"scope_id": scope_id}) as response:
                response.raise_for_status()
                raw = bytearray()
                async for chunk in response.aiter_bytes(chunk_size=65536):
                    if len(raw)+len(chunk) > full_sync_bytes:
                        raise ValueError("full_sync_byte_budget")
                    raw.extend(chunk)
        baseline = strict_json(raw)
        del raw
        if baseline["schema_version"] != "marketcow.polymarket.live-full-sync.v1":
            raise ValueError("full_sync_schema")
        if baseline["scope_id"] != scope_id or baseline["catalog_revision"] != catalog_revision:
            raise ValueError("full_sync_binding")
        ids = [m["identity"]["market_id"] for m in baseline["bootstrap"]["markets"]]
        if len(set(ids)) != len(ids) or sorted(ids) != sorted(market_ids):
            raise ValueError("full_sync_selection")
        cursor = initial = baseline["cursor"]
        instance = baseline["stream_instance_id"]
        if type(cursor) is not int or cursor < 0 or not isinstance(instance, str) or not instance:
            raise ValueError("full_sync_boundary")
        del baseline
        stream = ("wss" if url.scheme == "https" else "ws") + prefix[len(url.scheme):]
        stream += "/stream?"+urlencode({"scope_id": scope_id, "after_cursor": cursor})
        received = 0
        async with connect(stream, proxy=None, max_size=frame_bytes, max_queue=1,
                           open_timeout=timeout_seconds, close_timeout=1) as ws:
            for _ in range(maximum_frames):
                raw = await ws.recv()
                received += len(raw.encode("utf-8") if isinstance(raw, str) else raw)
                if received > maximum_stream_bytes:
                    raise ValueError("stream_byte_budget")
                frame = strict_json(raw)
                at = frame.get("cursor")
                if type(at) is not int or at < cursor:
                    raise ValueError("stream_cursor_regression")
                if frame["type"] == "event":
                    if at <= cursor or frame["event"]["cursor"] != at:
                        raise ValueError("event_cursor_mismatch")
                    cursor = at
                elif frame["type"] == "ready":
                    if frame["stream_instance_id"] != instance:
                        raise ValueError("ready_instance_mismatch")
                    if type(frame["confirmation_sequence"]) is not int or frame["confirmation_sequence"] < 0 or not isinstance(frame["confirmation_books"], list):
                        raise ValueError("ready_confirmation_baseline")
                    return RuntimeBoundary(instance, initial, at, endpoint)
                else:
                    raise ValueError("unexpected_pre_ready_frame")
            raise ValueError("ready_frame_budget")
