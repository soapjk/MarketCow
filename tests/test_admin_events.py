from __future__ import annotations

import asyncio
import unittest

from marketcow.admin_events import EVENT_SCHEMA, AdminEventHub, encode_sse


class AdminEventHubTest(unittest.IsolatedAsyncioTestCase):
    async def test_publish_replay_and_redaction(self):
        hub = AdminEventHub(replay_capacity=4, subscriber_capacity=2, heartbeat_seconds=1)
        event = await hub.publish(
            "request.summary",
            {"route": "/v1/health", "authorization": "Bearer secret"},
        )
        stream = hub.stream(0, ("request.summary",))
        replayed = await anext(stream)
        await stream.aclose()
        self.assertEqual(replayed["event_id"], event["event_id"])
        self.assertEqual(replayed["schema_version"], EVENT_SCHEMA)
        self.assertEqual(replayed["payload"]["authorization"], "[REDACTED]")
        encoded = encode_sse(replayed)
        self.assertIn(b"event: request.summary", encoded)
        self.assertNotIn(b"Bearer secret", encoded)

    async def test_replay_gap_is_explicit(self):
        hub = AdminEventHub(replay_capacity=2, subscriber_capacity=2, heartbeat_seconds=1)
        for index in range(3):
            await hub.publish("request.summary", {"index": index})
        stream = hub.stream(0, ("request.summary",))
        self.assertEqual((await anext(stream))["sequence"], 2)
        await stream.aclose()
        stream = hub.stream(1, ("request.summary",))
        self.assertEqual((await anext(stream))["type"], "request.summary")
        await stream.aclose()
        stream = hub.stream(0, ("request.summary",))
        self.assertEqual((await anext(stream))["sequence"], 2)
        await stream.aclose()

        # A positive cursor older than the retained window produces a gap event.
        hub = AdminEventHub(replay_capacity=2, subscriber_capacity=2, heartbeat_seconds=1)
        for index in range(4):
            await hub.publish("request.summary", {"index": index})
        stream = hub.stream(1, ("request.summary",))
        gap = await anext(stream)
        await stream.aclose()
        self.assertEqual(gap["type"], "stream.gap")
        self.assertEqual(gap["payload"]["oldest_available"], 3)

    async def test_slow_subscriber_receives_gap_and_latest_event(self):
        hub = AdminEventHub(replay_capacity=8, subscriber_capacity=1, heartbeat_seconds=1)
        stream = hub.stream(0, ("request.summary",))
        pending = asyncio.create_task(anext(stream))
        await asyncio.sleep(0)
        first = await hub.publish("request.summary", {"index": 1})
        self.assertEqual((await pending)["event_id"], first["event_id"])
        await hub.publish("request.summary", {"index": 2})
        await hub.publish("request.summary", {"index": 3})
        self.assertEqual((await anext(stream))["type"], "stream.gap")
        self.assertEqual((await anext(stream))["payload"]["index"], 3)
        await stream.aclose()

    async def test_heartbeat_reports_watermark(self):
        hub = AdminEventHub(replay_capacity=2, subscriber_capacity=1, heartbeat_seconds=1)
        stream = hub.stream(0, ("request.summary",))
        heartbeat = await asyncio.wait_for(anext(stream), timeout=1.2)
        await stream.aclose()
        self.assertEqual(heartbeat["type"], "stream.heartbeat")
        self.assertEqual(heartbeat["payload"]["watermark"], 0)


if __name__ == "__main__":
    unittest.main()
