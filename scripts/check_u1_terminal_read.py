"""Finite real-data terminal WS audit in the existing isolated candidate root."""
import asyncio
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import websockets
import argparse

parser=argparse.ArgumentParser()
parser.add_argument('--run',required=True)
run_id=parser.parse_args().run
if not run_id.isalnum(): raise ValueError('invalid run id')

from marketcow.polymarket_live import LiveEventEnvelope, LiveStateStore, live_event_identity
from marketcow.polymarket_live_stream import PolymarketLiveProjection

r=Path('/mnt/p44pro/marketcow-shadow-v3-runtime/linux')
root=r/'dynamic-live-candidate-r1'
sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
parent=f'marketcow-terminal-read-{run_id}.service'
proxy=f'marketcow-terminal-read-proxy-{run_id}.service'
bridge=f'marketcow-terminal-read-bridge-{run_id}.service'
os.umask(0o077)
def start(unit,command):
    subprocess.run(['systemd-run','--user','--quiet','--collect','--unit='+unit,
        '--property=BindsTo='+parent,'--property=After='+parent,'--property=UMask=0077',
        '--property=MemoryMax=256M','--property=RuntimeMaxSec=180',
        '--property=StandardOutput=append:'+str(r/'logs'/f'{unit}.log'),
        '--property=StandardError=append:'+str(r/'logs'/f'{unit}.log'),*command],check=True)
async def audit(process):
    projection=PolymarketLiveProjection()
    deadline=time.monotonic()+60
    seen=[]
    while True:
        try:
            async with websockets.connect('ws://127.0.0.1:18896',proxy=None,max_size=16777216) as ws:
                await ws.send(json.dumps({'type':'subscribe'}))
                state=json.loads(await ws.recv()); projection.install_state(state)
                while time.monotonic()<deadline:
                    message=json.loads(await asyncio.wait_for(ws.recv(),15))
                    if message['type']=='ready': projection.mark_ready(message)
                    if message['type']=='events':
                        for raw in message['events']:
                            event=LiveEventEnvelope.model_validate(raw)
                            assert live_event_identity(event)==event.event_id
                            projection.apply_live(raw)
                            if event.event_type=='market_terminal': seen.append({'market_id':event.market_id,'cursor':event.cursor})
                    if len({e['market_id'] for e in seen})>=13: return projection,seen
                raise RuntimeError('terminal events not observed before deadline')
        except (OSError,websockets.ConnectionClosed):
            if time.monotonic()>=deadline or process.poll() is not None: raise
            await asyncio.sleep(0.2)
    raise RuntimeError('terminal events not observed')

async def run():
    process=None
    report={}
    try:
        start(proxy,[str(r/'polymarket-proxy/mihomo'),'-d',str(r/'polymarket-proxy'),'-f',str(r/'polymarket-proxy/config.yaml')])
        start(bridge,[str(r/'target/debug/marketcow-live-source-bridge'),'--root',str(root),
            '--plan',str(root/'live-bridge-plan-r1.json'),'--plan-sha256',sha(root/'live-bridge-plan-r1.json'),
            '--listen','127.0.0.1:18896','--maximum-frame-bytes','16777216','--maximum-clients','2','--poll-ms','100'])
        env=os.environ.copy()
        for key in ['ALL_PROXY','all_proxy','https_proxy','http_proxy']: env.pop(key,None)
        env.update(HTTPS_PROXY='http://127.0.0.1:17890',HTTP_PROXY='http://127.0.0.1:17890',NO_PROXY='127.0.0.1,localhost,::1')
        with (r/f'logs/terminal-read-{run_id}-collector.log').open('x') as output:
            process=subprocess.Popen([str(r/'target/debug/marketcow-discovery-collector'),
                '--root',str(root),'--plan',str(root/'rust-scoped-plan-r1.json'),'--plan-sha256',sha(root/'rust-scoped-plan-r1.json'),
                '--configured-scope',str(root/'configured-scope.json'),'--configured-scope-sha256',sha(root/'configured-scope.json'),
                '--dependency-plan',str(root/'live-bridge-plan-r1.json'),'--dependency-plan-sha256',sha(root/'live-bridge-plan-r1.json'),
                '--input-mode','rest-poll','--expected-market-count','100','--concurrency','10','--request-market-batch-size','10',
                '--response-byte-limit','2097152','--persistence-queue-batches','256','--persistence-queue-bytes','67108864','--batch-byte-limit','16777216','--poll-seconds','1',
                '--request-timeout-seconds','10','--lifecycle-refresh-seconds','300','--cycles','2'],
                env=env,stdout=output,stderr=output)
            projection,seen=await audit(process)
            result=await asyncio.to_thread(process.wait,timeout=60)
            assert result==0
        # A new connection must recover terminal metadata at its committed boundary.
        async with websockets.connect('ws://127.0.0.1:18896',proxy=None,max_size=16777216) as ws:
            await ws.send(json.dumps({'type':'subscribe'}))
            state=json.loads(await ws.recv())
        projection.install_state(state)
        closed={m.identity.market_id for m in projection._markets.values() if m.lifecycle_state=='closed'}
        assert {e['market_id'] for e in seen}.issubset(closed)
        store=LiveStateStore(root)
        store._ensure_loaded=lambda:None
        store.catalog=dict(projection._markets);store.books=dict(projection._books)
        store.gaps=[];store.active_recovery_id=None;store.cursor=state['latest_cursor']
        affected={}
        for market in store.catalog.values():
            if market.lifecycle_state!='active': continue
            if any(p.market_id in closed for rel in market.relations for p in rel.outcome_pairs):
                frame=store.frame(market.identity.market_id)
                assert 'negative_risk_terminal_dependency_requires_resolution' in frame.reason_codes
                affected[market.identity.market_id]=frame.reason_codes
        assert affected
        report={'passed':True,'terminal_events':seen,'reconnected_terminal_market_count':len(closed),
            'cursor':state['latest_cursor'],'affected_markets':affected,'collector_exit_code':result,
            'all_markets_ready':False,'freshness_gate_seconds':5,'production_touched':False}
    finally:
        if process is not None and process.poll() is None:
            process.send_signal(2)
            try: await asyncio.to_thread(process.wait,timeout=30)
            except subprocess.TimeoutExpired: process.kill();process.wait()
        for unit in [bridge,proxy]: subprocess.run(['systemctl','--user','stop',unit],check=False)
        (r/f'logs/terminal-read-{run_id}-report.json').write_text(json.dumps(report,indent=2))
    print(json.dumps({k:v for k,v in report.items() if k!='affected_markets'}))

asyncio.run(run())
