"""Offline real-state audit, without feeds or listeners; original remains read-only."""
import hashlib
import json
from pathlib import Path
import sqlite3

from marketcow.polymarket_discovery import PolymarketDiscoveryStore
from marketcow.polymarket_live import PolymarketLiveReadStore


def digest_table(db, table):
    digest = hashlib.sha256()
    count = 0
    for row in db.execute(f'SELECT * FROM {table} ORDER BY 1'):
        digest.update(repr(row).encode())
        digest.update(b'\n')
        count += 1
    return {'rows': count, 'sha256': digest.hexdigest()}


def main():
    runtime = Path('/mnt/p44pro/marketcow-shadow-v3-runtime/linux')
    original = runtime / 'discovery-source-r1'
    candidate = runtime / 'bounded-discovery-candidate-r1'
    assert not (candidate / 'events.jsonl').exists()
    def connect(root):
        return sqlite3.connect(f'file:{root}/indexes/latest-state.sqlite3?mode=ro', uri=True)
    with connect(original) as source, connect(candidate) as target:
        before = dict(source.execute('SELECT * FROM metadata'))
        after = dict(target.execute('SELECT * FROM metadata'))
        assert before['latest_cursor'] == after['latest_cursor']
        assert before['unresolved_gap_count'] == after['unresolved_gap_count']
        assert target.execute('PRAGMA integrity_check').fetchone()[0] == 'ok'
        tables = {}
        for table in ('books', 'gaps', 'token_recoveries', 'market_lifecycle'):
            exists = source.execute('SELECT 1 FROM sqlite_master WHERE type="table" AND name=?', (table,)).fetchone()
            if exists:
                tables[table] = digest_table(source, table)
                assert tables[table] == digest_table(target, table), table
    reader = PolymarketLiveReadStore(candidate)
    store = PolymarketDiscoveryStore(reader, depth_notionals=('10', '50', '100', '500'), maximum_book_age_ms=5000)
    try:
        store.capture()
        full = store.full_sync()
        assert full.boundary_cursor == int(after['latest_cursor'])
        assert len(full.markets) == 1000
        frame = store.events_page(full.projection_id, full.boundary_cursor, 1)
        assert not frame.resync_required and not frame.items
        expired = store.events_page(full.projection_id, int(after['history_floor_cursor']) - 1, 1)
        assert expired.resync_required
        report = {'complete': True, 'evidence_kind': 'offline_actual_committed_state_not_live_load',
                  'cursor': full.boundary_cursor, 'market_count': len(full.markets),
                  'projection_id': full.projection_id, 'gap_count': after['unresolved_gap_count'],
                  'legacy_jsonl_absent': True, 'state_tables': tables,
                  'history_floor_cursor': after['history_floor_cursor'],
                  'recent_event_bytes': after['recent_event_bytes'], 'bounded_history_bytes': after['bounded_history_bytes']}
        (runtime / 'logs/bounded-discovery-r1-report.json').write_text(json.dumps(report, indent=2))
        print(json.dumps(report), flush=True)
    finally:
        store.stop_background_materialization()


if __name__ == '__main__':
    main()
