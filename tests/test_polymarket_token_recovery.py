from datetime import datetime, timezone
from types import SimpleNamespace
import unittest

from marketcow.polymarket_contracts import GapEntry
from marketcow.polymarket_live_stream import PolymarketLiveProjection
from marketcow.polymarket_live import LiveEventEnvelope, PolymarketLiveReadError
from marketcow.polymarket_token_recovery import (
    is_authoritative_book_snapshot,
    validate_token_recovery,
)


def event(token, kind, cursor, recovery_id='r1', snapshot=2):
    return SimpleNamespace(
        event_type=kind, cursor=cursor, applied=True, token_id=token,
        market_id='m'+token, condition_id='c'+token,
        canonical_payload={'recovery_scope':'token', 'token_id':token,
            'recovery_id':recovery_id, 'reason':'source_data_delayed',
            'resolved_gap_token_ids':[token], 'snapshot_cursor':snapshot},
        gaps=[GapEntry(code='coverage_gap',token_id=token,detected_at=datetime.now(timezone.utc))]
            if kind=='recovery_started' else [],
    )


class TokenRecoveryTests(unittest.TestCase):
    def projection(self):
        p = PolymarketLiveProjection()
        p._ready = p._connected = True
        p._error_code = None
        for token in ('a','b'):
            p._markets['m'+token] = SimpleNamespace(identity=SimpleNamespace(
                condition_id='c'+token, outcomes=[SimpleNamespace(token_id=token)]))
        return p

    def test_one_token_failure_does_not_change_stream_or_other_recovery(self):
        p = self.projection()
        p._active_recovery_id = 'independent-generation'
        for token in ('a','b'):
            e = event(token,'recovery_started',1)
            p._validate_token_recovery_transition(e)
            p._apply_event_state(e)
        self.assertTrue(p.ready)
        self.assertEqual({g.token_id for g in p._gaps}, {'a','b'})
        # Completion must be bound to an actually applied snapshot and attempt.
        done = event('a','recovery_completed',3)
        with self.assertRaises(ValueError): p._validate_token_recovery_transition(done)
        p._token_recoveries['a'] = ('r1',2)
        p._validate_token_recovery_transition(done)
        p._apply_event_state(done)
        self.assertEqual({g.token_id for g in p._gaps}, {'b'})
        self.assertEqual(p._active_recovery_id, 'independent-generation')
        self.assertTrue(p.ready)

    def test_foreign_token_and_unbound_completion_are_rejected(self):
        e = event('a','recovery_completed',3)
        e.canonical_payload['resolved_gap_token_ids'] = ['a','b']
        with self.assertRaises(ValueError): validate_token_recovery(e)
        e = event('a','recovery_started',1)
        e.gaps[0].token_id = 'b'
        with self.assertRaises(ValueError): validate_token_recovery(e)
        e = event('a','recovery_completed',3,snapshot=3)
        with self.assertRaises(ValueError): validate_token_recovery(e)

    def test_unknown_scope_rejected_and_existing_events_unchanged(self):
        e = event('a','recovery_started',1)
        e.canonical_payload['recovery_scope'] = 'all-but-really-one'
        with self.assertRaises(ValueError): validate_token_recovery(e)
        del e.canonical_payload['recovery_scope']
        self.assertIsNone(validate_token_recovery(e))

    def test_slow_subscriber_gets_explicit_resync_without_affecting_fast_one(self):
        p = self.projection()
        slow, fast = p.subscribe(capacity=1), p.subscribe(capacity=4)
        p.apply_validated_lives([event('a', 'recovery_started', 1)])
        self.assertEqual(fast.get_nowait().cursor, 1)
        p.apply_validated_lives([event('b', 'recovery_started', 2)])
        failure = slow.get_nowait()
        self.assertIsInstance(failure, PolymarketLiveReadError)
        self.assertEqual(failure.code, 'polymarket_live_subscriber_resync_required')
        self.assertEqual(fast.get_nowait().cursor, 2)
        self.assertTrue(p.ready)
        self.assertNotIn(slow, p._subscribers)
        self.assertIn(fast, p._subscribers)
        with self.assertRaises(ValueError): p.subscribe(capacity=0)

    def test_replaced_attempt_does_not_duplicate_token_gap(self):
        p = self.projection()
        p.apply_validated_lives([event('a', 'recovery_started', 1)])
        p.apply_validated_lives([event('a', 'recovery_started', 2, recovery_id='r2')])
        self.assertEqual(len(p._gaps), 1)
        self.assertEqual(p._token_recoveries['a'], ('r2', None))

    def test_token_status_wire_shape_has_explicit_control_observation_time(self):
        now = datetime.now(timezone.utc)
        model = LiveEventEnvelope.model_validate({
            'contract_version':'marketcow.prediction_market.v1',
            'schema_version':'marketcow.polymarket.live.v2', 'cursor':1,
            'event_id':'a'*64, 'event_type':'recovery_started',
            'market_id':'ma', 'condition_id':'ca', 'token_id':'a',
            'book_epoch':None, 'sequence':None, 'exchange_at':now,
            'received_at':now, 'canonical_payload':{
                'recovery_scope':'token', 'token_id':'a',
                'recovery_id':'r1', 'reason':'source_data_delayed'},
            'canonical_payload_sha256':'b'*64,
            'raw_payload':{'recovery_scope':'token'},
            'raw_payload_sha256':'c'*64, 'applied':True,
            'fail_closed_reason':None,
            'gaps':[{'code':'coverage_gap','token_id':'a',
                'detected_at':now,'resolved':False}],
        })
        self.assertEqual(model.exchange_at, model.received_at)

    def test_exact_rest_book_is_the_fresh_snapshot_for_its_fenced_attempt(self):
        p = self.projection()
        started = event('a', 'recovery_started', 1)
        p._validate_token_recovery_transition(started)
        p._apply_event_state(started)
        now = datetime.now(timezone.utc)
        book = SimpleNamespace(
            event_type='book', cursor=2, applied=True, token_id='a',
            market_id=None, condition_id='ca', received_at=now, gaps=[],
            canonical_payload={
                'token_id':'a', 'condition_id':'ca', 'book_epoch':'e',
                'sequence':1, 'sequence_semantics':'deterministic_normalized',
                'exchange_at':now, 'received_at':now,
                'tick_version':'1'*64, 'tick_size':'0.01',
                'bids':[{'price':'0.40','size':'1'}],
                'asks':[{'price':'0.60','size':'1'}],
                'last_trade_price':None, 'state_checksum':'2'*64,
                'source_hash':'source',
            },
            raw_payload={
                'asset_id':'a', 'market':'ca', 'tick_size':'0.01',
                'timestamp':'1700000000000', 'hash':'source',
                'bids':[{'price':'0.40','size':'1'}],
                'asks':[{'price':'0.60','size':'1'}],
            },
        )
        self.assertTrue(is_authoritative_book_snapshot(book))
        p._apply_event_state(book)
        self.assertEqual(p._token_recoveries['a'], ('r1', 2))
        done = event('a', 'recovery_completed', 3)
        p._validate_token_recovery_transition(done)
        p._apply_event_state(done)
        self.assertNotIn('a', p._token_recoveries)

        book.raw_payload['event_type'] = 'book'
        del book.raw_payload['tick_size']
        self.assertTrue(is_authoritative_book_snapshot(book))
        book.raw_payload['tick_size'] = '0.01'
        book.raw_payload['event_type'] = 'price_change'
        self.assertFalse(is_authoritative_book_snapshot(book))
        del book.raw_payload['event_type']
        del book.raw_payload['asks']
        self.assertFalse(is_authoritative_book_snapshot(book))


if __name__ == '__main__': unittest.main()
