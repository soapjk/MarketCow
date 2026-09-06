"""Finite real HTTP/full-sync/WS audit over an isolated bounded source."""
import asyncio
import hashlib
import json
import os
from pathlib import Path
import subprocess
import urllib.request
import websockets

RUNTIME = Path('/mnt/p44pro/marketcow-shadow-v3-runtime/linux')
ROOT = RUNTIME / 'bounded-discovery-candidate-r1'
RUN = os.environ.get('BOUNDED_GATE_RUN', 'r2')
BASE = 'http://127.0.0.1:8794/v1/prediction-markets/polymarket/live/discovery'


def get():
    with urllib.request.urlopen(BASE+'/full-sync',timeout=20) as r:
        body = r.read()
    return json.loads(body), hashlib.sha256(body).hexdigest()


async def audit(report):
    before, sha = await asyncio.to_thread(get)
    assert len(before['markets']) == 1000
    report.update(before_cursor=before['boundary_cursor'], full_sync_sha256=sha, projection_id=before['projection_id'])
    cursor = before['boundary_cursor']
    uri = BASE.replace('http:', 'ws:')+f'/stream?after_cursor={cursor}&projection_id='+before['projection_id']
    plan = ROOT / 'rust-source-plan.json'
    args = [str(RUNTIME/'target/release/marketcow-discovery-collector'),'--root',str(ROOT),
        '--plan',str(plan),'--plan-sha256',hashlib.sha256(plan.read_bytes()).hexdigest(),
        '--input-mode','rest-poll','--expected-market-count','1000','--concurrency','16',
        '--request-market-batch-size','20','--response-byte-limit','2097152','--batch-byte-limit','16777216',
        '--persistence-queue-batches','256','--persistence-queue-bytes','67108864',
        '--bounded-history-bytes','67108864','--poll-seconds','1','--request-timeout-seconds','10','--cycles','2']
    env = dict(os.environ, HTTPS_PROXY='http://127.0.0.1:17890', HTTP_PROXY='http://127.0.0.1:17890', NO_PROXY='localhost,127.0.0.1')
    child = None
    with (RUNTIME/f'logs/bounded-http-ws-{RUN}-collector.log').open('xb') as log:
        try:
            async with websockets.connect(uri,max_size=16*1024*1024,max_queue=1) as ws:
                child = subprocess.Popen(args,env=env,stdout=log,stderr=subprocess.STDOUT)
                async with asyncio.timeout(90):
                    while len(report['frames']) < 5:
                        raw = await ws.recv()
                        frame = json.loads(raw)
                        assert not frame['resync_required']
                        assert frame['after_cursor'] == cursor
                        if frame['next_cursor'] == cursor:
                            continue
                        assert frame['next_cursor'] == cursor+1
                        for item in frame['items']:
                            assert set(item) == {'type','payload'}
                            if item['type'] == 'market_update':
                                assert item['payload']['cursor'] == frame['next_cursor']
                        report['frames'].append(frame)
                        cursor = frame['next_cursor']
            code = await asyncio.wait_for(asyncio.to_thread(child.wait),90)
            assert code == 0
            after, sha = await asyncio.to_thread(get)
            assert len(after['markets']) == 1000 and after['boundary_cursor'] >= cursor
            assert after['projection_id'] == before['projection_id']
            assert not (ROOT/'events.jsonl').exists()
            report.update(passed=True,after_cursor=after['boundary_cursor'],after_full_sync_sha256=sha,
                          market_count=1000, collector_exit=code)
        finally:
            if child is not None and child.poll() is None:
                child.terminate()
                try:
                    child.wait(timeout=30)
                except subprocess.TimeoutExpired:
                    child.kill(); child.wait()


if __name__ == '__main__':
    report = {'passed':False,'frames':[]}
    try:
        asyncio.run(audit(report))
    except BaseException as exc:
        report['error'] = repr(exc)
        raise
    finally:
        (RUNTIME/f'logs/bounded-http-ws-{RUN}-report.json').write_text(json.dumps(report,indent=2))
