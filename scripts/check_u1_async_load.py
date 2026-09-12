"""Finite authoritative-data load audit; no orders, scope activation or synthetic events."""
import asyncio
import argparse
from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import socket
import sqlite3
import subprocess
import sys
import time

import httpx
import websockets
from marketcow.polymarket_live import LiveBook, LiveEventEnvelope, live_event_identity

R = Path('/mnt/p44pro/marketcow-shadow-v3-runtime/linux')
SOURCE = Path('/mnt/p44pro/projects/marketcow-shadow-v3')
ROOT = R / 'dynamic-live-candidate-r1'
parser=argparse.ArgumentParser()
parser.add_argument('--run',required=True,choices=('r2','r3','r4','r5','r6','r7','r8','r10','r11','r12','r13','r14','r15','r16','r17','r18','r19','r20','r21','r22','r23','r24','r25','r26','r27','r28','r29','r30','r31','r32','r33','r34','r35','r36','r37','r38','r40','r41','r42','r43','r44','r45','r46','r47','r48'))
parser.add_argument('--input-mode',required=True,choices=('websocket','rest-poll'))
parser.add_argument('--profile',required=True,choices=('debug','release'))
ARGS=parser.parse_args()
PREFIX = 'async-load-'+ARGS.run
PARENT = {'r2':'marketcow-async-comparison-r2.service',
    'r3':'marketcow-async-comparison-r2.service','r4':'marketcow-async-retry-r4.service',
    'r5':'marketcow-ws-load-r5.service','r6':'marketcow-ws-load-r6.service','r7':'marketcow-ws-load-r7.service','r8':'marketcow-ws-load-r8.service',
    'r10':'marketcow-ws-load-r10.service','r11':'marketcow-ws-load-r11.service',
    'r12':'marketcow-ws-load-r12.service','r13':'marketcow-ws-load-r13.service',
    'r14':'marketcow-ws-load-r14.service','r15':'marketcow-ws-load-r15.service',
    'r16':'marketcow-ws-load-r16.service',
    'r17':'marketcow-ws-load-r17.service',
    'r18':'marketcow-ws-load-r18.service',
    'r19':'marketcow-ws-load-r19.service',
    'r20':'marketcow-ws-load-r20.service',
    'r21':'marketcow-ws-load-r21.service',
    'r22':'marketcow-ws-load-r22.service',
    'r23':'marketcow-ws-load-r23.service',
    'r24':'marketcow-ws-load-r24.service',
    'r25':'marketcow-ws-load-r25.service',
    'r26':'marketcow-ws-load-r26.service',
    'r27':'marketcow-ws-load-r27.service',
    'r28':'marketcow-ws-load-r28.service',
    'r29':'marketcow-ws-load-r29.service',
    'r30':'marketcow-ws-load-r30.service',
    'r31':'marketcow-ws-load-r31.service',
    'r32':'marketcow-ws-load-r32.service','r33':'marketcow-ws-load-r33.service',
    'r34':'marketcow-ws-load-r34.service',
    'r35':'marketcow-ws-load-r35.service',
    'r36':'marketcow-ws-load-r36.service',
    'r37':'marketcow-ws-load-r37.service',
    'r38':'marketcow-ws-load-r38.service',
    'r40':'marketcow-ws-load-r40.service',
    'r41':'marketcow-ws-load-r41.service',
    'r42':'marketcow-ws-load-r42.service',
    'r43':'marketcow-ws-load-r43.service',
    'r44':'marketcow-ws-load-r44.service',
    'r45':'marketcow-ws-load-r45.service',
    'r46':'marketcow-ws-load-r46.service',
    'r47':'marketcow-ws-load-r47.service',
    'r48':'marketcow-ws-load-r48.service'}[ARGS.run]
UNITS = {name: f'marketcow-async-load-{name}-{ARGS.run}.service' for name in ('proxy','collector','api')}
BASE = 'http://127.0.0.1:8793/v1/prediction-markets/polymarket/live'
REPORT = R / 'logs' / f'{PREFIX}-report.json'
PROGRESS = R / 'logs' / f'{PREFIX}-progress.json'
os.umask(0o077)

def save(path, value):
    temporary = path.with_suffix('.tmp')
    temporary.write_text(json.dumps(value, indent=2))
    temporary.replace(path)

def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()

def start(name, command, memory):
    unit = UNITS[name]
    subprocess.run(['systemd-run','--user','--quiet','--unit='+unit,
        '--property=BindsTo='+PARENT,'--property=After='+PARENT,'--property=UMask=0077',
        '--property=MemoryMax='+memory,'--property=RuntimeMaxSec=420',
        '--property=KillSignal=SIGINT','--property=TimeoutStopSec=40',
        '--property=StandardOutput=append:'+str(R/'logs'/f'{PREFIX}-{name}.log'),
        '--property=StandardError=append:'+str(R/'logs'/f'{PREFIX}-{name}.log'),*command],check=True)

def unit_properties(name):
    output = subprocess.check_output(['systemctl','--user','show',UNITS[name],
        '-p','MainPID','-p','ActiveState','-p','Result','-p','ExecMainStatus','-p','MemoryPeak','-p','CPUUsageNSec','-p','ControlGroup'],text=True)
    result=dict(line.split('=',1) for line in output.splitlines() if '=' in line)
    if result['MainPID']!='0':
        try:
            status=Path('/proc')/result['MainPID']/'status'
            result['process_memory']={k:v.strip() for k,v in (line.split(':',1) for line in status.read_text().splitlines()) if k in ('VmRSS','VmHWM','Threads')}
            stat=Path('/sys/fs/cgroup')/result['ControlGroup'].lstrip('/')/'memory.stat'
            result['cgroup_memory']={k:int(v) for k,v in (line.split() for line in stat.read_text().splitlines()) if k in ('anon','file','kernel','slab','file_dirty','file_writeback')}
        except FileNotFoundError: pass
    return result

def summary(values):
    if not values: return {'count':0}
    values = sorted(values)
    return {'count':len(values),'p50':values[int((len(values)-1)*.50)],
        'p95':values[int((len(values)-1)*.95)],'p99':values[int((len(values)-1)*.99)],'max':values[-1]}

async def main():
    assert not REPORT.exists(), 'unique audit output required'
    for port in (17890,18896,8793):
        with socket.socket() as s:
            # Match server restart semantics: TIME_WAIT is not a live listener.
            # Do not enable SO_REUSEPORT, which could conceal an active listener.
            s.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
            s.bind(('127.0.0.1',port))
    scope = json.loads((ROOT/'configured-scope.json').read_bytes())
    ids = [m['market_id'] for m in scope['configured_markets']]
    assert len(ids)==100 and len(set(ids))==100
    report = {'complete':False,'passed':False,'duration_seconds':180,'configured_markets':ids,
        'root':str(ROOT),'scope_sha256':sha(ROOT/'configured-scope.json'),
        'plan_sha256':sha(ROOT/'live-bridge-plan-r1.json'),'freshness_gate_seconds':5,
        'latency_definition':'U1 collector received_at to U1 raw WS frame receipt; excludes upstream request time',
        'http_rounds':[],'errors':[],'samples':[],'production_touched':False}
    latencies=[]; upstream_ages=[]; frame_samples=[]; books={}; market_tokens={}; cursors={}; tasks=[]
    report['book_confirmation_count']=0
    async def raw_ws(deadline):
        startup = time.monotonic()+30
        while True:
            try:
                ws = await websockets.connect('ws://127.0.0.1:18896',proxy=None,max_size=33554432)
                await ws.send(json.dumps({'type':'subscribe'}))
                state = json.loads(await ws.recv())
                break
            except (OSError,websockets.ConnectionClosed):
                if time.monotonic()>startup: raise
                await asyncio.sleep(.2)
        async with ws:
            assert state['type']=='state'
            cursor=state['latest_cursor']; report['initial_cursor']=cursor
            for market in state['markets']:
                market_tokens[market['identity']['market_id']]=[o['token_id'] for o in market['identity']['outcomes']]
            for book in state['books']: books[book['token_id']]=book['received_at']
            while time.monotonic()<deadline:
                message=json.loads(await asyncio.wait_for(ws.recv(),15))
                received=datetime.now(timezone.utc)
                if 'persisted_cursor' in message:
                    cursors['persisted']=message['persisted_cursor']
                    cursors['queue_depth']=message['persistence_queue_depth']
                    assert message['persistence_error'] is None
                if message['type']=='events':
                    first=cursor+1
                    for raw in message['events']:
                        event=LiveEventEnvelope.model_validate(raw)
                        assert event.cursor==cursor+1 and live_event_identity(event)==event.event_id
                        cursor=event.cursor
                        if event.event_type=='book':
                            books[event.token_id]=raw['canonical_payload']['received_at']
                            if len(latencies)<200000:
                                latencies.append((received-event.received_at).total_seconds()*1000)
                                upstream_ages.append((event.received_at-event.exchange_at).total_seconds()*1000)
                    cursors['published']=cursor
                    if len(frame_samples)<10:
                        frame_samples.append({'first':first,'last':cursor,'persisted':message['persisted_cursor'],'items':len(message['events'])})
                elif message['type']=='book_confirmations':
                    for raw in message['books']:
                        book=LiveBook.model_validate(raw)
                        books[book.token_id]=book.received_at.isoformat()
                        report['book_confirmation_count']+=1
            report['final_observed_cursor']=cursor
    async def http_round(client, index):
        semaphore=asyncio.Semaphore(4)
        async def read(mid):
            async with semaphore:
                started=time.monotonic()
                try:
                    response=await client.get(BASE+'/full-sync',params={'market_id':mid})
                    body=response.json()
                    result={'market_id':mid,'status':response.status_code,'elapsed_ms':(time.monotonic()-started)*1000}
                    if response.status_code==200:
                        result.update(cursor=body['cursor'],health=body['health'],maximum_book_age_ms=body['maximum_book_age_ms'])
                    else: result['rejection']=body
                    return result
                except Exception as error: return {'market_id':mid,'error':str(error)}
        report['http_rounds'].append({'index':index,'markets':await asyncio.gather(*(read(mid) for mid in ids))})
    try:
        start('proxy',[str(R/'polymarket-proxy/mihomo'),'-d',str(R/'polymarket-proxy'),'-f',str(R/'polymarket-proxy/config.yaml')],'256M')
        await asyncio.sleep(1)
        command=['/usr/bin/env','-u','ALL_PROXY','-u','all_proxy','-u','https_proxy','-u','http_proxy',
            'HTTPS_PROXY=http://127.0.0.1:17890','HTTP_PROXY=http://127.0.0.1:17890','NO_PROXY=127.0.0.1,localhost,::1',
            str(R/'target'/ARGS.profile/'marketcow-discovery-collector'),'--input-mode',ARGS.input_mode,'--root',str(ROOT),
            '--plan',str(ROOT/'rust-scoped-plan-r1.json'),'--plan-sha256',sha(ROOT/'rust-scoped-plan-r1.json'),
            '--configured-scope',str(ROOT/'configured-scope.json'),'--configured-scope-sha256',sha(ROOT/'configured-scope.json'),
            '--dependency-plan',str(ROOT/'live-bridge-plan-r1.json'),'--dependency-plan-sha256',sha(ROOT/'live-bridge-plan-r1.json'),
            # Confirmations cover configured markets plus their bounded relation
            # closure. Twenty-market requests and sixteen network slots keep one
            # complete confirmation sweep inside the unchanged five-second
            # freshness budget without increasing memory queues or weakening
            # fail-closed validation.
            '--expected-market-count','100','--concurrency','16','--request-market-batch-size','20',
            '--response-byte-limit','2097152','--batch-byte-limit','16777216',
            '--persistence-queue-batches','256','--persistence-queue-bytes','67108864',
            '--websocket-shard-tokens','50','--websocket-recovery-concurrency','16',
            '--websocket-confirmation-seconds','0',
            '--poll-seconds','1','--request-timeout-seconds','10','--lifecycle-refresh-seconds','300',
            '--live-listen','127.0.0.1:18896','--live-frame-bytes','33554432','--live-maximum-clients','2']
        report['collector_command']=command; start('collector',command,'512M')
        api=['/usr/bin/env','PYTHONDONTWRITEBYTECODE=1',
            'PYTHONPATH='+str(R/'read-api-packages')+':'+str(SOURCE/'src'),
            sys.executable,str(SOURCE/'scripts/run_polymarket_live_read_api.py'),'--root',str(ROOT),
            '--discovery-root',str(R/'discovery-source-r1'),'--configured-scope',str(ROOT/'configured-scope.json'),
            '--host','127.0.0.1','--port','8793','--live-stream-uri','ws://127.0.0.1:18896',
            '--stable-snapshot-max-book-age-seconds','5','--consumer-maximum-book-age-seconds','5',
            # Follow a short authoritative replacement for the durable
            # reader's six-second bound. Persistent faults still fail closed;
            # the independent five-second book-age gate is unchanged.
            '--minimum-delivery-headroom-seconds','0','--stable-read-wait-seconds','6','--stable-read-poll-seconds','.025',
            '--executor-workers','2','--discovery-maximum-book-age-ms','5000','--discovery-maximum-full-sync-bytes','268435456',
            '--live-stream-replay-capacity','2048','--no-access-log']
        for depth in ('10','50','100','500'): api.extend(['--discovery-depth-notional',depth])
        report['api_command']=api; start('api',api,'768M')
        began=time.monotonic(); deadline=began+180
        raw=asyncio.create_task(raw_ws(deadline)); tasks.append(raw)
        async with httpx.AsyncClient(timeout=8,trust_env=False) as client:
            rounds=set()
            while time.monotonic()<deadline:
                elapsed=time.monotonic()-began
                if raw.done(): raw.result(); break
                properties=unit_properties('collector')
                assert properties['ActiveState']=='active', properties
                now=datetime.now(timezone.utc)
                fresh=sum(all(t in books and (now-datetime.fromisoformat(books[t])).total_seconds()<=5
                    for t in market_tokens.get(mid,[])) for mid in ids if mid in market_tokens)
                stale = [mid for mid in ids if mid not in market_tokens or not all(
                    t in books and (now-datetime.fromisoformat(books[t])).total_seconds()<=5
                    for t in market_tokens.get(mid,[]))]
                sample={'elapsed_seconds':elapsed,**cursors,'fresh_complete_configured_count':fresh,
                    'stale_configured_market_ids':stale,
                    'collector':properties,'api':unit_properties('api')}
                report['samples'].append(sample)
                save(PROGRESS,{'complete':False,'elapsed_seconds':elapsed,'event_samples':len(latencies),**sample})
                for at in (30,90,150):
                    if elapsed>=at and at not in rounds:
                        rounds.add(at); task=asyncio.create_task(http_round(client,at)); tasks.append(task)
                await asyncio.sleep(2)
            await asyncio.gather(*tasks)
        report['complete']=True
    except Exception as error:
        report['errors'].append(type(error).__name__+': '+str(error))
    finally:
        for task in tasks:
            if not task.done(): task.cancel()
        await asyncio.gather(*tasks,return_exceptions=True)
        report['units_before_stop']={name:unit_properties(name) for name in UNITS}
        for name in ('api','collector','proxy'):
            await asyncio.to_thread(subprocess.run,['systemctl','--user','stop',UNITS[name]],check=False)
        report['units_after_stop']={name:unit_properties(name) for name in UNITS}
        report['raw_ws_latency_ms']=summary(latencies); report['raw_ws_frames']=frame_samples
        report['upstream_timestamp_age_ms']=summary(upstream_ages)
        report['upstream_timestamp_age_definition']='collector received_at minus exchange_at; includes clock skew and upstream event age, not an isolated network RTT'
        report['http_status_counts']=dict(Counter(str(m.get('status','error')) for rr in report['http_rounds'] for m in rr['markets']))
        report['all_configured_http_ready']=bool(report['http_rounds']) and all(
            m.get('status')==200 and m['health']['status']=='index_ready' and m['health']['latest_state_ready']
            for rr in report['http_rounds'] for m in rr['markets'])
        report['passed_definition']='Complete 180s real load, every configured HTTP read ready in all rounds, gap-free consistent durable tail, and clean child shutdown'
        with sqlite3.connect(f'file:{ROOT}/indexes/latest-state.sqlite3?mode=ro',uri=True) as db:
            metadata=dict(db.execute('SELECT key,value FROM metadata'))
        report['durable_final']={'cursor':int(metadata['latest_cursor']),
            'unresolved_gap_count':int(metadata['unresolved_gap_count']),
            'indexed_log_size':int(metadata['event_log_size']),'actual_log_size':(ROOT/'events.jsonl').stat().st_size}
        report['passed']=(report['complete'] and not report['errors'] and bool(latencies)
            and report['all_configured_http_ready']
            and report['durable_final']['unresolved_gap_count']==0
            and report['durable_final']['indexed_log_size']==report['durable_final']['actual_log_size']
            and all(unit['ActiveState']=='inactive' and unit['Result']=='success'
                for unit in report['units_after_stop'].values()))
        save(REPORT,report); save(PROGRESS,{'complete':report['complete'],'passed':report['passed'],'report':str(REPORT),'errors':report['errors']})
    print(json.dumps({k:v for k,v in report.items() if k not in ('http_rounds','samples','collector_command','api_command','configured_markets')}))
    if not report['passed']: raise SystemExit(1)

asyncio.run(main())
