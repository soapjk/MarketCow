"""Finite actual preheat probe with fail-fast child status and durable cleanup."""
import argparse
import asyncio
from dataclasses import asdict
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import subprocess
import time

import httpx

from marketcow.universe_live_probe import probe_live


def state(unit):
    text = subprocess.check_output(['systemctl','--user','show',unit,'-p','ActiveState','-p','Result',
        '-p','ExecMainStatus','-p','MainPID','-p','MemoryPeak','-p','ControlGroup'],timeout=10,text=True)
    return dict(line.split('=',1) for line in text.splitlines() if '=' in line)


async def probe(args, report):
    scope = json.loads((args.root/'configured-scope.json').read_bytes())
    deadline = time.monotonic()+args.seconds
    async with httpx.AsyncClient(trust_env=False, timeout=3) as client:
        while True:
            status = state(args.unit)
            report['last_child_state'] = status
            if status['MainPID'] == '0' or status['ActiveState'] != 'active':
                raise RuntimeError('preheat child exited: '+status['Result']+'/'+status['ExecMainStatus'])
            try:
                response = await client.get(args.base+'/v1/prediction-markets/polymarket/live/health')
                response.raise_for_status()
                health = response.json()
                report['last_health'] = health
                if health.get('latest_cursor',0)>0 and health.get('book_token_count',0)>0:
                    break
            except (httpx.HTTPError, ValueError):
                pass
            if time.monotonic() >= deadline:
                raise TimeoutError('real book preheat deadline')
            await asyncio.sleep(1)
    remaining = deadline-time.monotonic()
    boundary = await probe_live(args.base, scope_id=scope['active_scope_id'], catalog_revision=scope['catalog_revision'],
        market_ids=[x['market_id'] for x in scope['configured_markets']], timeout_seconds=remaining,
        full_sync_bytes=134217728, frame_bytes=67108864, maximum_frames=20000, maximum_stream_bytes=134217728)
    report['boundary'] = asdict(boundary)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--unit', required=True)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--base', required=True)
    parser.add_argument('--seconds', type=int, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if not re.fullmatch(r'marketcow-universe-live-preheat-r[0-9]+\.service', args.unit):
        raise ValueError('only dedicated preheat child may be stopped')
    if not 1 <= args.seconds <= 120 or args.output.exists():
        raise ValueError('explicit finite window and new evidence output required')
    report = {'started_at_ns':time.time_ns(),'passed':False,'error':None}
    try:
        asyncio.run(probe(args, report))
        report['passed'] = True
    except Exception as error:
        report['error'] = str(error)
    finally:
        try:
            subprocess.run(['systemctl','--user','stop',args.unit],check=True,timeout=90)
            report['stopped'] = state(args.unit)
            if report['stopped']['MainPID'] != '0' or report['stopped']['Result'] != 'success' or report['stopped']['ExecMainStatus'] != '0':
                report['passed'] = False
            with sqlite3.connect((args.root/'indexes/latest-state.sqlite3').as_uri()+'?mode=ro',uri=True) as db:
                db.execute('BEGIN')
                assert db.execute('PRAGMA quick_check').fetchone() == ('ok',)
                metadata = dict(db.execute('SELECT key,value FROM metadata'))
                tail = db.execute('SELECT cursor,payload,sha256 FROM recent_events ORDER BY cursor DESC LIMIT 1').fetchone()
                latest = int(metadata['latest_cursor'])
                assert (tail is None and latest == 0) or (tail[0] == latest and hashlib.sha256(tail[1]).hexdigest() == tail[2])
                assert int(metadata['recent_event_bytes']) <= int(metadata['bounded_history_bytes'])
                report['durable'] = {'metadata':metadata,'tail_sha256':tail[2] if tail else None,
                    'books':db.execute('SELECT COUNT(*) FROM books').fetchone()[0]}
        except Exception as error:
            report['cleanup_error'] = str(error); report['passed'] = False
        report['ended_at_ns'] = time.time_ns()
        with args.output.open('x') as stream:
            json.dump(report,stream,sort_keys=True); stream.flush(); os.fsync(stream.fileno())
    print(json.dumps(report,sort_keys=True))
    if not report['passed']:
        raise SystemExit(1)


if __name__ == '__main__':
    main()
