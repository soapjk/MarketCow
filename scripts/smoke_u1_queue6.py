import asyncio
from collections import Counter
import hashlib
import json
from pathlib import Path
import urllib.request
import websockets

BASE = 'http://192.168.124.3:8793/v1/prediction-markets/polymarket/live'
SCOPE = '54b2ad555edfd47bd1863fc71c7bb6443ca57a50c7433644b78ef3a78a4e4a12'


async def main():
    with urllib.request.urlopen(BASE + '/full-sync?scope_id=' + SCOPE, timeout=20) as response:
        raw = response.read(134217729)
    assert len(raw) <= 134217728
    full = json.loads(raw)
    assert len(set(full['scope_market_ids'])) == 250
    assert len(full['snapshot']['items']) == 250
    assert full['freshness_policy'] == 'consumer_decides'
    cursor = full['cursor']
    counts = Counter()
    async with websockets.connect(BASE.replace('http:', 'ws:') + '/stream?scope_id=' + SCOPE + '&after_cursor=' + str(cursor), max_size=67108864) as ws:
        async with asyncio.timeout(35):
            while counts['event'] < 250 or counts['ready'] < 1:
                frame = json.loads(await ws.recv())
                kind = frame['type']
                assert kind != 'error', frame
                if kind == 'event':
                    assert frame['event']['cursor'] == frame['cursor'] > cursor
                    cursor = frame['cursor']
                if kind == 'ready':
                    assert frame['stream_instance_id'] == full['stream_instance_id']
                    assert frame['cursor'] >= cursor
                    cursor = frame['cursor']
                counts[kind] += 1
    report = {'passed': True, 'scope': SCOPE, 'markets': 250,
              'instance': full['stream_instance_id'], 'baseline_cursor': full['cursor'],
              'last_cursor': cursor, 'counts': dict(counts), 'fullsync_sha256': hashlib.sha256(raw).hexdigest(),
              'health': full['health'], 'statuses': dict(Counter(x['status'] for x in full['snapshot']['items']))}
    Path('/mnt/p44pro/marketcow-shadow-v3-runtime/linux/logs/queue6-7b19dd2-smoke.json').write_text(json.dumps(report, indent=2))
    print(json.dumps(report))


asyncio.run(main())
