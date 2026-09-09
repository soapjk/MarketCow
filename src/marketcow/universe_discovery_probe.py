"""Bounded real Discovery v3 full-sync/delta check; not Live's ready protocol."""
import asyncio
from urllib.parse import urlencode, urlsplit

import httpx
from websockets.asyncio.client import connect

from marketcow.universe_live_probe import strict_json
from marketcow.universe_runtime_supervisor import RuntimeBoundary


async def probe_discovery(endpoint, *, projection_id, universe_revision, catalog_revision,
                          market_ids, timeout_seconds, full_sync_bytes, frame_bytes,
                          maximum_frames, maximum_stream_bytes):
    if any(type(v) is not int or v <= 0 for v in (full_sync_bytes, frame_bytes, maximum_frames, maximum_stream_bytes)):
        raise ValueError("explicit positive probe budget")
    if not 0 < timeout_seconds < float("inf"):
        raise ValueError("finite probe deadline required")
    url = urlsplit(endpoint)
    if url.scheme not in ("http", "https") or not url.hostname or url.username or url.password or url.query or url.fragment:
        raise ValueError("invalid probe endpoint")
    prefix = endpoint.rstrip("/")+"/v1/prediction-markets/polymarket/live/discovery"
    async with asyncio.timeout(timeout_seconds):
        async with httpx.AsyncClient(trust_env=False, follow_redirects=False, timeout=timeout_seconds) as client:
            async with client.stream("GET", prefix+"/full-sync") as response:
                response.raise_for_status()
                raw = bytearray()
                async for chunk in response.aiter_bytes(chunk_size=65536):
                    if len(raw)+len(chunk) > full_sync_bytes:
                        raise ValueError("full_sync_byte_budget")
                    raw.extend(chunk)
        baseline = strict_json(raw)
        del raw
        # An operator-controlled restart creates a new projection. Learn it
        # only from this atomic full-sync, then pin every delta to that exact
        # value; never reuse a preheat process's projection after publication.
        if projection_id is None:
            projection_id = baseline["projection_id"]
            if not isinstance(projection_id, str) or len(projection_id) != 64 or any(c not in "0123456789abcdef" for c in projection_id):
                raise ValueError("discovery_projection_invalid")
        def binding(value):
            if (value["projection_id"], value["universe_revision"], value["catalog_revision"]) != (projection_id, universe_revision, catalog_revision):
                raise ValueError("discovery_binding_mismatch")
        binding(baseline)
        if baseline["schema_version"] != "marketcow.polymarket.discovery.v3":
            raise ValueError("discovery_schema_mismatch")
        ids = [m["market_id"] for m in baseline["markets"]]
        if len(set(ids)) != len(ids) or sorted(ids) != sorted(market_ids):
            raise ValueError("discovery_selection_mismatch")
        cursor = baseline["boundary_cursor"]
        if type(cursor) is not int or cursor < 0:
            raise ValueError("discovery_boundary_invalid")
        # ready=false can describe local gaps; do not reinstate all-healthy gating.
        del baseline
        stream = ("wss" if url.scheme == "https" else "ws")+prefix[len(url.scheme):]
        stream += "/stream?"+urlencode({"projection_id": projection_id, "after_cursor": cursor})
        received = 0
        async with connect(stream, proxy=None, max_size=frame_bytes, max_queue=1,
                           open_timeout=timeout_seconds, close_timeout=1) as ws:
            for _ in range(maximum_frames):
                raw = await ws.recv()
                received += len(raw.encode() if isinstance(raw, str) else raw)
                if received > maximum_stream_bytes:
                    raise ValueError("stream_byte_budget")
                frame = strict_json(raw)
                binding(frame)
                if frame["schema_version"] != "marketcow.polymarket.discovery-events.v3" or frame["resync_required"] is not False:
                    raise ValueError("discovery_resync_required")
                after, next_cursor, boundary = frame["after_cursor"], frame["next_cursor"], frame["boundary_cursor"]
                if any(type(v) is not int for v in (after, next_cursor, boundary)) or after != cursor or not after <= next_cursor <= boundary:
                    raise ValueError("discovery_delta_discontinuity")
                if not isinstance(frame["items"], list):
                    raise ValueError("discovery_items_invalid")
                if next_cursor > cursor:
                    # Internal common record names retained; identity_kind explicitly
                    # says this is projection_id, not a fabricated stream UUID.
                    return RuntimeBoundary(projection_id, cursor, next_cursor, endpoint, "discovery_projection")
            raise ValueError("discovery_delta_budget")
