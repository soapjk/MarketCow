"""Synthetic correctness/operation-count tests, not live performance evidence."""
from datetime import datetime, timezone
from unittest import TestCase
from unittest.mock import patch

from marketcow.polymarket_contracts import GapEntry, content_sha256
from marketcow.polymarket_live_stream import PolymarketLiveProjection, _GapIndex


def gap(token, code='coverage_gap'):
    return GapEntry(code=code, token_id=token,
                    detected_at=datetime(2026, 9, 6, tzinfo=timezone.utc))


class GapIndexTests(TestCase):
    def test_high_population_hashes_only_incoming_fact(self):
        index = _GapIndex(gap(str(i)) for i in range(2000))
        with patch('marketcow.polymarket_live_stream.content_sha256',
                   wraps=content_sha256) as hashed:
            index.add(gap('new'))
            index.add(gap('new'))
            index.remove_tokens(('1000', 'absent'))
        self.assertEqual(hashed.call_count, 2)
        self.assertEqual(len(index.by_id), 2000)
        self.assertNotIn('1000', index.by_token)

    def test_order_identity_ownership_and_multiple_facts_per_token(self):
        a, b = gap('a'), gap('b')
        other = a.model_copy(update={'observed': 'distinct'})
        index = _GapIndex([a, b, other, a])
        expected = list(index.by_id.values())
        self.assertEqual(expected, [a, b, other])
        a.token_id = 'changed'
        self.assertEqual(expected[0].token_id, 'a')
        index.remove_tokens(('a',))
        self.assertEqual(list(index.by_id.values()), [b])
        index.add(gap('a'))
        self.assertEqual([g.token_id for g in index.by_id.values()], ['b', 'a'])

    def test_projection_reset_drops_all_old_index_entries(self):
        projection = PolymarketLiveProjection()
        projection._gaps = [gap('old')]
        projection._gaps = [gap('new')]
        projection._gap_index.remove_tokens(('old',))
        self.assertEqual(projection._gaps, [gap('new')])
        self.assertEqual(set(projection._gap_index.by_token), {'new'})

    def test_no_token_gap_survives_unrelated_recovery(self):
        index = _GapIndex([gap(None), gap('a'), gap('b')])
        index.remove_tokens(('a', 'b'))
        self.assertEqual(list(index.by_id.values()), [gap(None)])

    def test_real_install_boundary_rebuilds_and_filters_resolved_history(self):
        projection = PolymarketLiveProjection()
        def install(gaps):
            projection.install_state({
                'schema_version': 'marketcow.polymarket.live-stream.v1',
                'type': 'state', 'catalog_revision': 'a' * 64,
                'latest_cursor': 0, 'persisted_cursor': 0,
                'markets': [], 'books': [],
                'gaps': [g.model_dump(mode='json') for g in gaps],
            })
        install([gap('old')])
        previous_instance = projection._stream_instance_id
        install([gap('new'), gap('resolved').model_copy(update={'resolved': True})])
        self.assertNotEqual(projection._stream_instance_id, previous_instance)
        self.assertEqual(projection._gaps, [gap('new')])
        self.assertEqual(set(projection._gap_index.by_token), {'new'})

    def test_metrics_are_fixed_size_and_include_projection_scale(self):
        import json
        projection = PolymarketLiveProjection()
        projection._gaps = [gap('a')]
        projection._apply_metrics.started -= 6
        with patch('marketcow.polymarket_stream_metrics.LOGGER.warning') as logged:
            projection.emit_apply_metrics()
        payload = json.loads(logged.call_args.args[1])
        self.assertEqual(payload['projection_scale']['gap_count'], 1)
        self.assertIn('not pure CPU', payload['stage_measurement_boundary'])
        self.assertEqual(projection._apply_metrics.totals['gap_maintenance'], [0, 0.0, 0.0])
