"""Finite read-only installed-release HTTP/WS check; no pool/account mutation."""
import asyncio
import hashlib
import json
from pathlib import Path
from urllib.parse import urlencode

import httpx
from websockets.asyncio.client import connect

from marketcow.universe_rust_control import RustScopeClient

ROOT = Path('/mnt/p44pro/marketcow-shadow-v3-runtime/linux')


async def check(pool):
    client = RustScopeClient(socket_path=ROOT/f'hot-scope-r36-v1-{pool}-ctl/scope.sock',
        maximum_bytes=16777216, timeout_seconds=10)
    actual = await asyncio.to_thread(client.status)
    assert actual['source_readable'], actual
    base = 'http://192.168.124.3:'+('8793' if pool == 'live' else '8795')
    prefix = base+'/v1/prediction-markets/polymarket/live'
    if pool == 'discovery':
        prefix += '/discovery'
    params = {'scope_id': actual['scope_id']} if pool == 'live' else {}
    async with httpx.AsyncClient(trust_env=False, timeout=30) as http:
        async with http.stream('GET', prefix+'/full-sync', params=params) as response:
            response.raise_for_status()
            raw = bytearray()
            async for chunk in response.aiter_bytes():
                assert len(raw)+len(chunk) <= 67108864
                raw.extend(chunk)
    baseline = json.loads(raw)
    if pool == 'live':
        assert baseline['scope_id'] == actual['scope_id']
        assert baseline['stream_instance_id'] == actual['stream_instance_id']
        assert len(baseline['snapshot']['items']) == 250
        cursor = baseline['cursor']
        params = dict(scope_id=actual['scope_id'], after_cursor=cursor)
    else:
        assert baseline['projection_id'] == actual['projection_id']
        cursor = baseline['boundary_cursor']
        params = dict(projection_id=actual['projection_id'], after_cursor=cursor)
    first = cursor
    counts, wire_bytes = {}, 0
    async with asyncio.timeout(45):
        async with connect(prefix.replace('http://', 'ws://')+'/stream?'+urlencode(params),
                proxy=None, max_size=67108864, max_queue=1, close_timeout=1) as ws:
            for _ in range(10000):
                frame_raw = await ws.recv()
                wire_bytes += len(frame_raw.encode() if isinstance(frame_raw, str) else frame_raw)
                assert wire_bytes <= 67108864
                frame = json.loads(frame_raw)
                if pool == 'live':
                    kind = frame['type']
                    assert kind != 'error', frame
                    if kind == 'event':
                        assert frame['cursor'] == frame['event']['cursor'] > cursor
                        cursor = frame['cursor']
                    elif kind == 'ready':
                        assert frame['stream_instance_id'] == actual['stream_instance_id']
                        assert frame['cursor'] >= cursor
                        cursor = frame['cursor']
                    counts[kind] = counts.get(kind, 0)+1
                    if counts.get('ready') and counts.get('event', 0) >= 100:
                        break
                else:
                    assert not frame['resync_required'] and frame['after_cursor'] == cursor
                    assert frame['projection_id'] == actual['projection_id']
                    assert cursor <= frame['next_cursor'] <= frame['boundary_cursor']
                    cursor = frame['next_cursor']
                    counts['delta'] = counts.get('delta', 0)+1
                    if cursor > first:
                        break
            else:
                raise RuntimeError('frame budget without required baseline progress')
    return dict(actual=actual, baseline_cursor=first, final_cursor=cursor, counts=counts,
        fullsync_bytes=len(raw), fullsync_sha256=hashlib.sha256(raw).hexdigest(), wire_bytes=wire_bytes)


async def main():
    report = dict(zip(('live', 'discovery'), await asyncio.gather(check('live'), check('discovery'))))
    destination = ROOT/'releases/hot-scope-r36-v1/installation/http-ws.json'
    with destination.open('x') as stream:
        json.dump(report, stream, sort_keys=True)
    print(json.dumps({k: {f: v[f] for f in ('baseline_cursor', 'final_cursor', 'counts', 'fullsync_sha256')}
        for k, v in report.items()}))


if __name__ == '__main__':
    asyncio.run(main())
