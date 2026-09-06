"""Read-only bounded smoke: no collector or account actions."""
import asyncio
import hashlib
import json
from pathlib import Path
import urllib.request
import websockets

BASE = 'http://192.168.124.3:8793/v1/prediction-markets/polymarket/live'
SCOPE = '54b2ad555edfd47bd1863fc71c7bb6443ca57a50c7433644b78ef3a78a4e4a12'


def get(path):
    with urllib.request.build_opener(urllib.request.ProxyHandler({})).open(BASE + path, timeout=20) as response:
        raw = response.read(134217729)
        assert len(raw) <= 134217728
        return json.loads(raw), hashlib.sha256(raw).hexdigest()


async def main():
    full, sha = await asyncio.to_thread(get, '/full-sync?scope_id=' + SCOPE)
    assert len(full['scope_market_ids']) == 250
    discovery, discovery_sha = await asyncio.to_thread(get, '/discovery/full-sync')
    assert len(discovery['markets']) == 1000
    cursor = full['cursor']
    frames, events, ready = 0, 0, False
    url = BASE.replace('http:', 'ws:') + '/stream?scope_id=' + SCOPE + '&after_cursor=' + str(cursor)
    async with websockets.connect(url, max_size=67108864, max_queue=1, close_timeout=2) as socket:
        async with asyncio.timeout(25):
            while frames < 20000:
                message = json.loads(await socket.recv())
                frames += 1
                if message['type'] == 'error':
                    raise RuntimeError(message)
                if message['type'] == 'event':
                    assert message['cursor'] == message['event']['cursor'] and message['cursor'] > cursor
                    cursor = message['cursor']
                    events += 1
                if message['type'] == 'ready':
                    assert message['stream_instance_id'] == full['stream_instance_id']
                    ready = True
                if ready and events >= 3:
                    break
    assert ready and events >= 3
    report = {'passed': True, 'scope_market_count': 250, 'discovery_market_count': 1000,
              'discovery_ready': discovery.get('ready'), 'discovery_boundary': discovery['boundary_cursor'],
              'full_sync_sha256': sha, 'discovery_sha256': discovery_sha,
              'instance': full['stream_instance_id'], 'before_cursor': full['cursor'],
              'after_cursor': cursor, 'events': events, 'frames': frames, 'ready_frame': ready,
              'not_long_term_or_close_isolation_acceptance': True}
    path = Path('/mnt/p44pro/marketcow-shadow-v3-runtime/linux/logs/main-service-smoke-cd7cf39.json')
    with path.open('x') as file:
        json.dump(report, file, indent=2)
    print(json.dumps(report))


if __name__ == '__main__':
    asyncio.run(main())
