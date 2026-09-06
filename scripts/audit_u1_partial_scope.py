"""Finite real-data audit for delivery, never an all-markets trading gate."""
import asyncio
from collections import Counter
import json
import hashlib
import os
from pathlib import Path
import time
import urllib.request

import websockets

ROOT = Path('/mnt/p44pro/marketcow-shadow-v3-runtime/linux')
REPORT = ROOT / ('logs/partial-scope-' + os.environ.get('PARTIAL_SCOPE_RUN', 'r1') + '-report.json')
BASE = os.environ.get('PARTIAL_SCOPE_BASE', 'http://127.0.0.1:8794/v1/prediction-markets/polymarket/live')
SCOPE = '54b2ad555edfd47bd1863fc71c7bb6443ca57a50c7433644b78ef3a78a4e4a12'


def get(path):
    with urllib.request.urlopen(BASE + path, timeout=20) as response:
        return json.load(response)


async def audit(report):
    scope = await asyncio.to_thread(get, '/scope')
    ids = {m['market_id'] for m in scope['configured_markets']}
    assert len(ids) == 250 and scope['active_scope_id'] == SCOPE
    for cycle in range(2):
        started = time.monotonic()
        full = await asyncio.to_thread(get, '/full-sync?scope_id=' + SCOPE)
        fixture = REPORT.with_name(REPORT.stem + f'-full-sync-{cycle}.json')
        fixture.write_text(json.dumps(full, separators=(',', ':')))
        report.setdefault('fixtures', []).append({
            'path': str(fixture), 'sha256': hashlib.sha256(fixture.read_bytes()).hexdigest(),
            'kind': 'parsed-response-not-byte-exact-http',
        })
        assert full['freshness_policy'] == 'consumer_decides'
        assert set(full['scope_market_ids']) == ids
        assert len(full['snapshot']['items']) == 250
        assert {m['market_id'] for m in full['snapshot']['items']} == ids
        for component in ('bootstrap', 'snapshot'):
            assert full[component]['cursor'] == full['cursor']
        report['snapshots'].append({
            'cursor': full['cursor'], 'duration_ms': (time.monotonic()-started)*1000,
            'statuses': dict(Counter(m['status'] for m in full['snapshot']['items'])),
            'age_ms': full['maximum_book_age_ms'],
            'gaps': full['health']['unresolved_gap_count'],
            'persisted_cursor': full['health']['persisted_cursor'],
        })
        allowed = ids | {pair['market_id'] for market in full['bootstrap']['markets']
                         for rel in market['relations'] for pair in rel['outcome_pairs']}
        cursor = full['cursor']
        books = dict(full['snapshot']['books'])
        version_fields = ('token_id','condition_id','book_epoch','sequence',
                          'tick_version','tick_size','state_checksum','bids','asks','last_trade_price')
        url = BASE.replace('http:', 'ws:') + '/stream?scope_id=' + SCOPE + '&after_cursor=' + str(cursor)
        counts = Counter()
        async with websockets.connect(url, max_size=64*1024*1024) as ws:
            async with asyncio.timeout(25):
                while counts['event'] < 3 or counts['book_confirmations'] < 1:
                    raw = await ws.recv()
                    frame = json.loads(raw)
                    raw_bytes = raw.encode() if isinstance(raw, str) else raw
                    fixture = REPORT.with_name(REPORT.stem + f'-ws-{cycle}-{sum(counts.values())}.json')
                    fixture.write_bytes(raw_bytes)
                    report['fixtures'].append({'path': str(fixture),
                        'sha256': hashlib.sha256(raw_bytes).hexdigest(), 'kind': 'raw-ws-message'})
                    kind = frame['type']
                    if len(report['frames']) < 30:
                        report['frames'].append({k:v for k,v in frame.items() if k not in ('books','event')})
                    assert kind != 'error', frame
                    if kind == 'event':
                        event = frame['event']
                        assert frame['cursor'] == event['cursor'] > cursor
                        assert event['market_id'] in allowed | {None}
                        cursor = event['cursor']
                        payload = event['canonical_payload']
                        if event['applied'] and 'state_checksum' in payload and 'bids' in payload:
                            books[event['token_id']] = payload
                    elif kind == 'ready':
                        assert frame['cursor'] >= cursor
                        assert frame['stream_instance_id'] == full['stream_instance_id']
                        assert isinstance(frame['confirmation_books'], list)
                        cursor = frame['cursor']
                    elif kind == 'book_confirmations':
                        assert frame['cursor'] >= cursor
                        assert frame['stream_instance_id'] == full['stream_instance_id']
                        assert all(book['confirmed_at'] and book['confirmation_source']
                                   for book in frame['books'])
                    counts[kind] += 1
                    if kind in ('ready', 'book_confirmations'):
                        for book in frame.get('books', frame.get('confirmation_books', [])):
                            previous = books.get(book['token_id'])
                            assert previous is not None, ('confirmation_without_baseline', book['token_id'])
                            assert all(previous.get(k) == book.get(k) for k in version_fields), (
                                'confirmation_version_mismatch', book['token_id'])
                            report['confirmation_version_checks'] = report.get('confirmation_version_checks', 0) + 1
        report['streams'].append(dict(counts))


if __name__ == '__main__':
    os.umask(0o077)
    report = {'scope_id': SCOPE, 'passed': False, 'snapshots': [], 'streams': [], 'frames': []}
    try:
        asyncio.run(audit(report))
        report['passed'] = True
    except BaseException as error:
        report['error'] = repr(error)
        raise
    finally:
        REPORT.write_text(json.dumps(report, indent=2))
