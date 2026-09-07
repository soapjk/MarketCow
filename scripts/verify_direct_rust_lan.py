"""Bounded read-only LAN verification; never controls services or Paper."""
import argparse
import asyncio
import hashlib
import json
from pathlib import Path
import time
import urllib.request

import websockets


def fetch(url):
    with urllib.request.urlopen(url, timeout=15) as response:
        raw = response.read(134217729)
    assert len(raw) <= 134217728
    return raw


async def probe(base, scope, output, live):
    path = '/v1/prediction-markets/polymarket/live/' + ('' if live else 'discovery/')
    raw = await asyncio.to_thread(fetch, base+path+'full-sync'+('?scope_id='+scope if live else ''))
    output.with_suffix('.full-sync.json').write_bytes(raw)
    value = json.loads(raw)
    assert len(value['bootstrap']['markets'] if live else value['markets']) == (250 if live else 1000)
    cursor = value['cursor'] if live else value['boundary_cursor']
    initial = cursor
    query = f'scope_id={scope}&after_cursor={cursor}' if live else f"projection_id={value['projection_id']}&after_cursor={cursor}"
    counts = {}
    size = 0
    started = time.monotonic()
    with output.with_suffix('.ws.jsonl').open('xb') as wire:
        async with websockets.connect(base.replace('http:', 'ws:')+path+'stream?'+query,
                max_size=67108864, max_queue=1, close_timeout=5) as ws:
            while time.monotonic()-started < 45:
                # Discovery is explicitly REST-polled every 30s. A quiet 15s
                # interval is not a failed connection. Keep the overall 45s
                # audit deadline; this changes no source or strategy freshness.
                remaining = 45 - (time.monotonic()-started)
                if remaining <= 0:
                    break
                raw = await asyncio.wait_for(ws.recv(), timeout=min(15 if live else 45, remaining))
                encoded = raw.encode() if isinstance(raw, str) else raw
                assert size+len(encoded)+1 <= 67108864, 'wire byte cap before required sample'
                wire.write(encoded+b'\n')
                size += len(encoded)+1
                frame = json.loads(encoded)
                if live:
                    kind = frame['type']
                    assert kind != 'error', frame
                    if kind == 'event':
                        assert frame['event']['cursor'] == frame['cursor'] > cursor
                    else:
                        assert frame['cursor'] >= cursor
                    cursor = frame['cursor']
                else:
                    kind = 'delta'
                    assert not frame['resync_required'], frame
                    assert frame['after_cursor'] == cursor and frame['next_cursor'] == cursor+1
                    cursor = frame['next_cursor']
                counts[kind] = counts.get(kind, 0)+1
                if live and counts.get('ready') == 1 and counts.get('event', 0) >= 100 and counts.get('book_confirmations', 0) >= 1:
                    break
                if not live and counts.get('delta', 0) >= 100:
                    break
    assert counts.get('ready') == 1 and counts.get('book_confirmations', 0) >= 1 if live else counts.get('delta',0) >= 100
    def sha(path):
        with path.open('rb') as stream:
            return hashlib.file_digest(stream,'sha256').hexdigest()
    return {'initial_cursor': initial, 'final_cursor': cursor, 'counts': counts,
        'instance': value.get('stream_instance_id'), 'projection': value.get('projection_id'),
        'catalog_revision': value['catalog_revision'], 'bytes': size,
        'full_sync_sha256': sha(output.with_suffix('.full-sync.json')),
        'wire_sha256': sha(output.with_suffix('.ws.jsonl'))}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--live-base', required=True)
    parser.add_argument('--discovery-base', required=True)
    parser.add_argument('--scope', required=True)
    parser.add_argument('--minimum-live-cursor', required=True, type=int)
    parser.add_argument('--output', required=True, type=Path)
    args = parser.parse_args()
    args.output.mkdir()
    async def run():
        live = await probe(args.live_base,args.scope,args.output/'live',True)
        assert live['initial_cursor'] >= args.minimum_live_cursor
        discovery = await probe(args.discovery_base,args.scope,args.output/'discovery',False)
        return {'live':live,'discovery':discovery}
    result = asyncio.run(run())
    (args.output/'report.json').write_text(json.dumps(result,indent=2))
    print(json.dumps(result))


if __name__ == '__main__':
    main()
