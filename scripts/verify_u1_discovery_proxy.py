"""Bounded real-source probe; never synthesizes events or changes readiness."""
import asyncio
import json
import os
from pathlib import Path
import subprocess

import requests
import websockets

RUNTIME = Path('/mnt/p44pro/marketcow-shadow-v3-runtime/linux')
BASE = 'http://127.0.0.1:8793/v1/prediction-markets/polymarket/live/discovery'


def snapshot():
    response = requests.get(BASE + '/full-sync', timeout=20)
    response.raise_for_status()
    payload = response.json()
    return {key: value for key, value in payload.items() if key not in ('markets', 'relations')}


async def verify():
    os.umask(0o077)
    report_path = RUNTIME / 'logs/discovery-proxy-r2-http-ws.json'
    if report_path.exists():
        raise RuntimeError('evidence already exists')
    before = snapshot()
    report = {'before': before, 'frames': [], 'passed': False}
    uri = BASE.replace('http:', 'ws:') + '/stream?after_cursor=' + str(before['boundary_cursor']) + '&projection_id=' + before['projection_id']
    process = None
    try:
        async with websockets.connect(uri, max_size=16 * 1024 * 1024) as socket:
            process = subprocess.Popen(['/bin/bash', '/mnt/p44pro/projects/marketcow-shadow-v3/scripts/run_u1_discovery_proxy_smoke.sh', 'r2'])
            expected = before['boundary_cursor']
            for _ in range(5):
                frame = json.loads(await asyncio.wait_for(socket.recv(), timeout=120))
                assert frame['resync_required'] is False
                assert frame['after_cursor'] == expected
                assert frame['next_cursor'] == expected + 1
                items = frame['items']
                for item in items:
                    assert set(item) == {'type', 'payload'}
                    if item['type'] == 'market_update':
                        assert item['payload']['cursor'] == frame['next_cursor']
                report['frames'].append({
                    'after_cursor': frame['after_cursor'], 'next_cursor': frame['next_cursor'],
                    'boundary_cursor': frame['boundary_cursor'], 'resync_required': frame['resync_required'],
                    'items': [{'type': item['type'], 'market_id': item['payload']['market_id'],
                               'cursor': item['payload']['cursor']} for item in items if item['type'] == 'market_update'],
                })
                expected = frame['next_cursor']
        report['collector_exit_code'] = await asyncio.to_thread(process.wait)
        report['after'] = await asyncio.to_thread(snapshot)
        assert report['collector_exit_code'] == 0
        assert report['after']['projection_id'] == before['projection_id']
        assert report['after']['boundary_cursor'] > before['boundary_cursor']
        assert report['after']['ready'] is True
        assert report['after']['unresolved_gap_count'] == 0
        assert report['after']['fail_closed_reason'] is None
        report['passed'] = True
    except Exception as error:
        report['error'] = type(error).__name__ + ': ' + str(error)
        if process is not None:
            report['collector_exit_code'] = await asyncio.to_thread(process.wait)
        raise
    finally:
        report_path.write_text(json.dumps(report, indent=2))
        print(json.dumps(report))


if __name__ == '__main__':
    asyncio.run(verify())
