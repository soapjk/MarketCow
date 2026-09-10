"""Bounded direct-Rust research capture, not a replacement trading projection."""
import asyncio
import hashlib
import time
from datetime import datetime, timezone
from urllib.parse import urlencode, urlsplit

import httpx
from websockets.asyncio.client import connect

from .btc_polymarket_binding import scope_coverage
from .polymarket_live import LiveFullSyncResponse
from .polymarket_live_stream import _decode_stream_message
from .universe_live_probe import strict_json


def fact(raw, kind, instance, cursor, *, first_received_at, received_monotonic_ns):
    return {"schema_version": "marketcow.btc-research.polymarket-wire.v1",
            "source": "marketcow_direct_rust", "event_type": kind,
            "stream_instance_id": instance, "source_cursor": cursor,
            "first_received_at": first_received_at,
            "received_monotonic_ns": received_monotonic_ns,
            "capture_processed_at": datetime.now(timezone.utc).isoformat(),
            "raw_utf8": raw.decode(), "raw_sha256": hashlib.sha256(raw).hexdigest(),
            "missing_reasons": ["socket_kernel_receive_time_unknown"],
            "execution_ready": False}


class CaptureBoundary:
    def __init__(self, baseline, bindings):
        LiveFullSyncResponse.model_validate(baseline)
        if not bindings or len(bindings) > 3:
            raise ValueError("explicit_hour_binding_budget")
        for binding in bindings:
            if not scope_coverage(binding, baseline)["covered"]:
                raise ValueError("btc_market_outside_scope")
        self.instance, self.cursor = baseline["stream_instance_id"], baseline["cursor"]
        self.ready = False

    def consume(self, raw):
        value = strict_json(raw)
        _, decoded = _decode_stream_message(raw)
        kind = value.get("type")
        if kind == "error":
            raise ValueError("source_error")
        cursor = value.get("cursor")
        if type(cursor) is not int or cursor < self.cursor:
            raise ValueError("source_cursor_regression")
        if kind == "event":
            if cursor <= self.cursor or decoded.cursor != cursor:
                raise ValueError("event_cursor_mismatch")
        elif kind == "ready":
            if self.ready or value.get("stream_instance_id") != self.instance:
                raise ValueError("ready_identity")
            if type(value.get("confirmation_sequence")) is not int or value["confirmation_sequence"] < 0 or not isinstance(value.get("confirmation_books"), list):
                raise ValueError("ready_confirmation_shape")
            self.ready = True
        elif kind not in ("book_confirmation", "book_confirmations") or not self.ready:
            raise ValueError("unsupported_frame_or_phase")
        self.cursor = cursor
        return kind


async def capture(endpoint, scope_id, bindings, publish, *, seconds, maximum_frames,
                  maximum_total_bytes, maximum_frame_bytes, maximum_fullsync_bytes):
    url = urlsplit(endpoint)
    if url.scheme not in ("http", "https") or not url.hostname or url.username or url.password or url.query or url.fragment:
        raise ValueError("invalid_endpoint")
    if not 0 < seconds <= 1800 or min(maximum_frames, maximum_total_bytes, maximum_frame_bytes, maximum_fullsync_bytes) <= 0:
        raise ValueError("invalid_capture_budget")
    prefix = endpoint.rstrip("/")+"/v1/prediction-markets/polymarket/live"
    total, count = 0, 0
    boundary = None
    stop_reason = "source_error"
    timer = asyncio.timeout(seconds)
    try:
        async with timer:
            async with httpx.AsyncClient(trust_env=False, follow_redirects=False, timeout=min(seconds, 20)) as client:
                async with client.stream("GET", prefix+"/full-sync", params={"scope_id": scope_id}) as response:
                    response.raise_for_status()
                    raw = bytearray()
                    async for chunk in response.aiter_bytes(chunk_size=65536):
                        if len(raw)+len(chunk) > min(maximum_fullsync_bytes, maximum_total_bytes):
                            raise ValueError("fullsync_budget")
                        raw.extend(chunk)
            fullsync_received_at = datetime.now(timezone.utc).isoformat()
            fullsync_received_monotonic_ns = time.monotonic_ns()
            baseline = strict_json(raw)
            if baseline.get("scope_id") != scope_id:
                raise ValueError("scope_mismatch")
            boundary = CaptureBoundary(baseline, bindings)
            total = len(raw)
            publish(fact(bytes(raw), "full_sync", boundary.instance, boundary.cursor,
                         first_received_at=fullsync_received_at,
                         received_monotonic_ns=fullsync_received_monotonic_ns))
            del raw, baseline
            stream = ("wss" if url.scheme == "https" else "ws")+prefix[len(url.scheme):]
            stream += "/stream?"+urlencode({"scope_id": scope_id, "after_cursor": boundary.cursor})
            async with connect(stream, proxy=None, max_size=maximum_frame_bytes, max_queue=1,
                               open_timeout=min(seconds, 20), close_timeout=1) as ws:
                while count < maximum_frames:
                    wire = await ws.recv()
                    received_at = datetime.now(timezone.utc).isoformat()
                    received_monotonic_ns = time.monotonic_ns()
                    raw = wire.encode() if isinstance(wire, str) else wire
                    total += len(raw)
                    if total > maximum_total_bytes:
                        raise ValueError("stream_total_budget")
                    kind = boundary.consume(raw)
                    publish(fact(raw, kind, boundary.instance, boundary.cursor,
                                 first_received_at=received_at,
                                 received_monotonic_ns=received_monotonic_ns))
                    count += 1
                stop_reason = "frame_limit"
    except TimeoutError:
        # Only expiration of our observation timer after ready is a normal end.
        # A transport's independent timeout must remain a source failure.
        if timer.expired() and boundary is not None and boundary.ready:
            stop_reason = "observation_window_complete"
        else:
            stop_reason = "ready_deadline" if timer.expired() else "source_timeout"
            raise
    finally:
        publish({"event_type": "polymarket_capture_ended", "frames": count,
                 "application_bytes": total, "execution_ready": False,
                 "stop_reason": stop_reason,
                 "stream_instance_id": boundary.instance if boundary else None})
    return {"frames": count, "bytes": total, "ready_observed": boundary.ready if boundary else False,
            "scope_id": scope_id, "stream_instance_id": boundary.instance if boundary else None,
            "last_cursor": boundary.cursor if boundary else None, "stop_reason": stop_reason}
