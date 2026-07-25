from __future__ import annotations

import asyncio
import json
import uuid
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, AsyncIterator, Iterable, Mapping

from .telemetry import sanitize_text


EVENT_SCHEMA = "marketcow.admin-event.v1"
ALLOWED_EVENT_TYPES = frozenset({
    "request.summary",
    "history_job.updated",
    "provider.status",
    "stream.heartbeat",
    "stream.gap",
})
_SECRET_KEYS = frozenset({
    "authorization", "cookie", "password", "passwd", "secret", "token",
    "api_key", "apikey", "dsn", "body",
})


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _sanitize(value: Any, depth: int = 0) -> Any:
    if depth > 4:
        return "[TRUNCATED]"
    if isinstance(value, Mapping):
        result = {}
        for key, item in list(value.items())[:32]:
            clean_key = sanitize_text(key)[:64]
            result[clean_key] = (
                "[REDACTED]" if clean_key.lower() in _SECRET_KEYS
                else _sanitize(item, depth + 1)
            )
        return result
    if isinstance(value, (list, tuple)):
        return [_sanitize(item, depth + 1) for item in value[:100]]
    if isinstance(value, str):
        return sanitize_text(value)
    if value is None or isinstance(value, (bool, int, float)):
        return value
    return sanitize_text(value)


@dataclass(eq=False)
class _Subscriber:
    queue: asyncio.Queue[dict[str, Any]]
    types: frozenset[str]
    dropped: int = 0


class AdminEventHub:
    """Process-local bounded Pub/Sub hub for sanitized administration summaries."""

    def __init__(
        self, replay_capacity: int = 1024, subscriber_capacity: int = 128,
        heartbeat_seconds: float = 15.0,
    ) -> None:
        if not 1 <= replay_capacity <= 100000:
            raise ValueError("admin event replay capacity is invalid")
        if not 1 <= subscriber_capacity <= 10000:
            raise ValueError("admin event subscriber capacity is invalid")
        if not 1 <= heartbeat_seconds <= 60:
            raise ValueError("admin event heartbeat is invalid")
        self.replay_capacity = replay_capacity
        self.subscriber_capacity = subscriber_capacity
        self.heartbeat_seconds = heartbeat_seconds
        self._replay: deque[dict[str, Any]] = deque(maxlen=replay_capacity)
        self._subscribers: set[_Subscriber] = set()
        self._sequence = 0
        self._lock = asyncio.Lock()

    async def publish(
        self, event_type: str, payload: Mapping[str, Any], source: str = "marketcow-api",
    ) -> dict[str, Any]:
        if event_type not in ALLOWED_EVENT_TYPES - {"stream.heartbeat", "stream.gap"}:
            raise ValueError("admin event type is invalid")
        async with self._lock:
            self._sequence += 1
            event = self._event(event_type, payload, source, self._sequence)
            self._replay.append(event)
            for subscriber in tuple(self._subscribers):
                if event_type not in subscriber.types:
                    continue
                if subscriber.queue.full():
                    try:
                        subscriber.queue.get_nowait()
                    except asyncio.QueueEmpty:
                        pass
                    subscriber.dropped += 1
                subscriber.queue.put_nowait(event)
        return event

    async def request_completed(self, payload: Mapping[str, Any]) -> None:
        await self.publish("request.summary", payload)

    def _event(
        self, event_type: str, payload: Mapping[str, Any], source: str, sequence: int,
    ) -> dict[str, Any]:
        return {
            "schema_version": EVENT_SCHEMA,
            "event_id": uuid.uuid4().hex,
            "type": event_type,
            "source": sanitize_text(source)[:80],
            "occurred_at": _now(),
            "sequence": sequence,
            "payload": _sanitize(payload),
        }

    async def stream(
        self, after_sequence: int = 0, types: Iterable[str] = ("request.summary",),
    ) -> AsyncIterator[dict[str, Any]]:
        selected = frozenset(types)
        if not selected or not selected <= (
            ALLOWED_EVENT_TYPES - {"stream.heartbeat", "stream.gap"}
        ):
            raise ValueError("admin event subscription is invalid")
        if after_sequence < 0:
            raise ValueError("admin event sequence is invalid")
        subscriber = _Subscriber(
            asyncio.Queue(maxsize=self.subscriber_capacity), selected
        )
        initial: list[dict[str, Any]] = []
        async with self._lock:
            oldest = self._replay[0]["sequence"] if self._replay else self._sequence + 1
            if after_sequence and after_sequence < oldest - 1:
                initial.append(self._event(
                    "stream.gap",
                    {"requested_after": after_sequence, "oldest_available": oldest},
                    "marketcow-event-hub", self._sequence,
                ))
            initial.extend(
                event for event in self._replay
                if event["sequence"] > after_sequence and event["type"] in selected
            )
            self._subscribers.add(subscriber)
        try:
            for event in initial:
                yield event
            while True:
                if subscriber.dropped:
                    dropped = subscriber.dropped
                    subscriber.dropped = 0
                    yield self._event(
                        "stream.gap", {"dropped": dropped},
                        "marketcow-event-hub", self._sequence,
                    )
                try:
                    event = await asyncio.wait_for(
                        subscriber.queue.get(), timeout=self.heartbeat_seconds
                    )
                except asyncio.TimeoutError:
                    yield self._event(
                        "stream.heartbeat", {"watermark": self._sequence},
                        "marketcow-event-hub", self._sequence,
                    )
                    continue
                yield event
        finally:
            async with self._lock:
                self._subscribers.discard(subscriber)


def encode_sse(event: Mapping[str, Any]) -> bytes:
    payload = json.dumps(event, ensure_ascii=False, separators=(",", ":"))
    return (
        f"id: {int(event['sequence'])}\n"
        f"event: {event['type']}\n"
        f"data: {payload}\n\n"
    ).encode()
