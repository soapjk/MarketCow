import json
import unittest
from unittest.mock import patch
from marketcow.polymarket_stream_metrics import StreamMetrics, STAGES


class MetricsTest(unittest.TestCase):
    def test_connection_membership_reuses_only_unchanged_metadata(self):
        from types import SimpleNamespace as N
        from marketcow.polymarket_live_stream import PolymarketLiveProjection
        projection = PolymarketLiveProjection()
        pair = N(market_id='dep', yes_token_id='dy', no_token_id='dn')
        projection._markets['m'] = N(identity=N(outcomes=[N(token_id='own')]),
                                     relations=[N(outcome_pairs=[pair])])
        cached = projection.scope_membership(('m',))
        self.assertEqual(cached[1], {'m', 'dep'})
        self.assertEqual(cached[2], {'own', 'dy', 'dn'})
        for _ in range(10000):
            self.assertIs(projection.scope_membership(('m',), cached), cached)
        pair.no_token_id = 'new'
        projection._membership_revision += 1
        refreshed = projection.scope_membership(('m',), cached)
        self.assertIsNot(refreshed, cached)
        self.assertEqual(refreshed[2], {'own', 'dy', 'new'})
        self.assertNotIn('outside', refreshed[1])

    def test_scope_tokens_include_both_dependencies_without_stale_cache(self):
        from types import SimpleNamespace as N
        from marketcow.polymarket_live_stream import PolymarketLiveProjection
        projection = PolymarketLiveProjection()
        pair = N(yes_token_id='dy', no_token_id='dn')
        projection._markets['m'] = N(identity=N(outcomes=[N(token_id='own')]),
                                     relations=[N(outcome_pairs=[pair])])
        self.assertEqual(projection.scope_token_ids(['m']), {'own', 'dy', 'dn'})
        self.assertEqual(projection.scope_token_ids(['outside']), set())
        pair.no_token_id = 'new'
        self.assertEqual(projection.scope_token_ids(['m']), {'own', 'dy', 'new'})

    def test_decode_details_bounded_and_invalid_input_still_fails(self):
        import time
        from marketcow.polymarket_live_stream import _decode_stream_message_timed
        from marketcow.polymarket_stream_metrics import DECODE_STAGES
        message, validated, detail, finished = _decode_stream_message_timed(
            '{"type":"ready","cursor":123}', time.perf_counter())
        self.assertIsNone(validated)
        detail['await_resume'] = time.perf_counter() - finished
        metrics = StreamMetrics('synthetic')
        for _ in range(10000):
            metrics.record_decode(detail, message)
        self.assertEqual(len(metrics.decode_totals), len(DECODE_STAGES))
        self.assertEqual(metrics.last_frame['ordinal'], 10000)
        self.assertEqual(metrics.last_frame['cursor'], 123)
        self.assertTrue(all(detail[key] >= 0 for key in DECODE_STAGES))
        with self.assertRaises(json.JSONDecodeError):
            _decode_stream_message_timed('{', time.perf_counter())
        with self.assertRaises(ValueError):
            _decode_stream_message_timed('{"type":"events","events":[]}', time.perf_counter())

    def test_fixed_memory_and_explicit_stage_semantics(self):
        metrics = StreamMetrics('test')
        for _ in range(10000):
            metrics.record('decode',.001)
        self.assertEqual(len(metrics.totals),len(STAGES))
        with patch('marketcow.polymarket_stream_metrics.LOGGER.warning') as log:
            metrics.emit(123,force=True)
            payload = json.loads(log.call_args.args[1])
        self.assertEqual(payload['local_cursor'],123)
        self.assertEqual(payload['stages_count_sum_max_seconds']['decode'][0],10000)
        self.assertFalse(payload['receive_wait_is_network_rtt'])
        self.assertEqual(payload['queue_occupancy'],'not_measured')
        self.assertEqual(metrics.totals['decode'],[0,0.0,0.0])


class ClosedSendTest(unittest.IsolatedAsyncioTestCase):
    async def test_instrumented_send_matches_starlette_wire_encoding(self):
        from starlette.websockets import WebSocket
        from marketcow.polymarket_live_read_api import _send_live_message
        reference, actual = [], []
        async def receive():
            return {'type': 'websocket.connect'}
        async def send(value):
            reference.append(value)
        socket = WebSocket({'type': 'websocket'}, receive, send)
        await socket.accept()
        payload = {'type': 'event', 'cursor': 4, 'text': '测试', 'value': None}
        await socket.send_json(payload)
        class Target:
            async def send_text(self, value):
                actual.append(value)
        metrics = StreamMetrics('synthetic')
        self.assertTrue(await _send_live_message(Target(), payload, metrics))
        self.assertEqual(actual, [reference[-1]['text']])
        for stage in ('json_encode', 'socket_send', 'send_yield'):
            self.assertEqual(metrics.totals[stage][0], 1)

    async def test_writable_send_yields_to_other_tasks_without_reordering(self):
        import asyncio
        from marketcow.polymarket_live_read_api import _send_live_message
        sent, observed = [], []
        class Writable:
            async def send_json(self, message):
                sent.append(message)
        async def sibling():
            observed.append(len(sent))
        task = asyncio.create_task(sibling())
        for cursor in range(100):
            self.assertTrue(await _send_live_message(Writable(), cursor))
        await task
        self.assertEqual(sent, list(range(100)))
        self.assertLess(observed[0], 100)

    async def test_only_known_closed_transport_is_suppressed(self):
        from marketcow.polymarket_live_read_api import _send_live_message
        class Closed:
            async def send_json(self, _):
                raise RuntimeError("Unexpected ASGI message 'websocket.send', after sending 'websocket.close'.")
        self.assertFalse(await _send_live_message(Closed(),{}))
        class Bug:
            async def send_json(self, _):
                raise RuntimeError('unrelated bug')
        with self.assertRaisesRegex(RuntimeError,'unrelated bug'):
            await _send_live_message(Bug(),{})
