"""Terminal dependencies retain identity/evidence, never acquire placeholder books."""
import json
import sqlite3
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from marketcow.polymarket_contracts import GapEntry
from marketcow.polymarket_live import GammaLiveNormalizer, LiveStateStore, LiveStateIndex, PolymarketLiveReadStore
from marketcow.polymarket_live_stream import PolymarketLiveProjection

NOW=datetime(2026,8,3,4,tzinfo=timezone.utc)


class TerminalReadTests(unittest.TestCase):
    def test_terminal_event_removes_l2_requirement_not_economic_dependency(self):
        rows=[{'id':str(i),'conditionId':'0x'+str(i)*64,'slug':f'market-{i}','question':f'Market {i}',
            'active':True,'closed':False,'acceptingOrders':True,'startDate':'2026-08-01T00:00:00Z',
            'endDate':'2027-01-01T00:00:00Z','clobTokenIds':json.dumps([str(i*10),str(i*10+1)]),
            'outcomes':'["Yes","No"]','orderPriceMinTickSize':'0.01','orderMinSize':'5',
            'feesEnabled':False,'negRisk':True,'negRiskMarketID':'group',
            'groupItemTitle':str(i),'updatedAt':NOW.isoformat(),'events':[{'id':'event'}]} for i in [1,2]]
        with tempfile.TemporaryDirectory() as tmp:
            store=LiveStateStore(Path(tmp),now_provider=lambda:NOW)
            store.replace_catalog(GammaLiveNormalizer.normalize(rows,NOW),rows)
            for token in ['10','11']:
                store.apply_snapshot({'event_type':'book','asset_id':token,'timestamp':'1785729600000',
                    'hash':'hash-'+token,'tick_size':'0.01','min_order_size':'5',
                    'bids':[{'price':'0.4','size':'10'}],'asks':[{'price':'0.6','size':'10'}]},received_at=NOW)
            before=store.frame('1',now=NOW)
            self.assertIn('negative_risk_member_missing',before.reason_codes)
            terminal=store.catalog['2'].model_copy(update={'lifecycle_state':'closed','closed':True,
                'accepting_orders':False,'terminal_at':NOW,'lifecycle_source':'polymarket_gamma',
                'lifecycle_source_url':'https://gamma-api.polymarket.com/markets/2',
                'lifecycle_evidence_sha256':'a'*64})
            event=store.mark_market_terminal(terminal,reason='authoritative Gamma closure')
            after=store.frame('1',now=NOW)
            self.assertNotIn('negative_risk_member_missing',after.reason_codes)
            self.assertIn('negative_risk_terminal_dependency_requires_resolution',after.reason_codes)
            self.assertFalse(after.open_position_allowed)
            self.assertTrue(any(p.market_id=='2' for p in after.relation_pairs))
            self.assertNotIn('20',store.books)
            projection=PolymarketLiveProjection()
            projection._markets=dict(store.catalog)
            projection._token_recoveries={'20':('attempt',None),'21':('attempt',None)}
            projection._gaps=[
                GapEntry(code='coverage_gap',token_id=token,detected_at=NOW)
                for token in ('20','21')
            ]
            projection._apply_event_state(event)
            self.assertTrue(projection.event_affects_scope(event,['1']))
            self.assertTrue(projection.event_affects_scope(
                event.model_copy(update={'event_type':'book'}), ['1']))
            self.assertTrue(projection.event_affects_scope(
                event.model_copy(update={'event_type':'recovery_started'}), ['1']))
            self.assertFalse(projection.event_affects_scope(event,['unrelated']))
            self.assertEqual(projection._markets['2'].lifecycle_state,'closed')
            self.assertIsNone(projection._markets['2'].resolution)
            self.assertFalse(projection._token_recoveries)
            self.assertFalse(projection._gaps)
            projection._books=dict(store.books)
            projection._ready=True; projection._connected=True
            projection._error_code=None; projection._active_recovery_id=None
            projection._catalog_revision=store.catalog_revision
            projection._latest_cursor=event.cursor; projection._persisted_cursor=event.cursor
            projection._generation=1
            reader=PolymarketLiveReadStore(
                Path(tmp), now_provider=lambda:NOW,
                stable_snapshot_max_book_age_seconds=5,
                consumer_maximum_book_age_seconds=5,
            )
            capture=projection._capture_scope(reader,['1'],{})
            self.assertEqual(set(capture['books']),{'10','11'})
            self.assertEqual({m.identity.market_id for m in capture['markets']},{'1','2'})
            _, _, closed_full_sync = projection.full_sync_json(reader, ['2'])
            self.assertFalse(closed_full_sync.health.latest_state_ready)
            self.assertEqual(closed_full_sync.health.status, 'degraded')
            with sqlite3.connect(':memory:') as db:
                LiveStateIndex._schema(db,wal=False)
                source_fact = event.model_copy(update={
                    'canonical_payload': {
                        'source_event_id': 'gamma:2:market_terminal',
                        'source_observed_at': NOW.isoformat(),
                        'resolution': None,
                    },
                })
                LiveStateIndex._index_terminal_event(db, source_fact)
                self.assertIsNone(db.execute(
                    'select cursor from market_lifecycle'
                ).fetchone())
                LiveStateIndex._index_terminal_event(db,event)
                row=db.execute('select cursor,payload_json from market_lifecycle').fetchone()
                self.assertEqual(row[0],event.cursor)
                self.assertEqual(json.loads(row[1])['lifecycle_evidence_sha256'],'a'*64)
            resolved = terminal.model_copy(update={
                'lifecycle_state': 'resolved', 'resolution': 'No',
            })
            resolved_event = store.mark_market_terminal(
                resolved, reason='authoritative Gamma resolution',
            )
            projection._apply_event_state(resolved_event)
            resolved_frame = store.frame('1', now=NOW)
            self.assertNotIn(
                'negative_risk_terminal_dependency_requires_resolution',
                resolved_frame.reason_codes,
            )
            # The immutable catalog still says market 2 is active. Bounded
            # readers must overlay the content-addressed lifecycle row from
            # the same SQLite boundary before requiring fresh books.
            reader = PolymarketLiveReadStore(
                Path(tmp), now_provider=lambda:NOW,
                stable_snapshot_max_book_age_seconds=5,
                consumer_maximum_book_age_seconds=5,
            )
            active_page = reader.snapshot(['1'])
            self.assertNotIn(
                'negative_risk_terminal_dependency_requires_resolution',
                active_page.items[0].reason_codes,
            )
            self.assertNotIn(
                'negative_risk_member_missing',
                active_page.items[0].reason_codes,
            )
            self.assertEqual(set(active_page.books), {'10', '11'})
            terminal_bootstrap = reader.bootstrap(['2'])
            self.assertEqual(
                terminal_bootstrap.markets[0].lifecycle_state, 'resolved',
            )
            self.assertEqual(terminal_bootstrap.active_token_ids, [])
            terminal_page = reader.snapshot(['2'])
            self.assertEqual(terminal_page.items[0].status, 'terminal')
            self.assertEqual(terminal_page.books, {})
            _, _, terminal_full_sync = projection.full_sync_json(reader, ['2'])
            self.assertTrue(terminal_full_sync.health.latest_state_ready)
            self.assertEqual(terminal_full_sync.health.status, 'index_ready')
            self.assertEqual(
                terminal_full_sync.bootstrap.markets[0].resolution, 'No',
            )


if __name__=='__main__': unittest.main()
