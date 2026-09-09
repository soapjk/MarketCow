"""Finite actual cold Discovery test; creates/stops only its named preheat unit."""
import asyncio
from dataclasses import asdict
import hashlib
import json
import os
from pathlib import Path
import shlex
import socket
import sqlite3
import subprocess
import time

import httpx

from marketcow.universe_discovery_probe import probe_discovery

R = Path('/mnt/p44pro/marketcow-shadow-v3-runtime/linux')
RELEASE = R/'releases/universe-runtime-r5'
ROOT = R/'universe-discovery-candidate-r1'
UNIT = 'marketcow-universe-discovery-preheat-r2.service'
BASE = 'http://127.0.0.1:18900'


def status():
    raw = subprocess.check_output(['systemctl', '--user', 'show', UNIT, '-p', 'ActiveState',
        '-p', 'MainPID', '-p', 'Result', '-p', 'ExecMainStatus', '-p', 'MemoryPeak'], text=True, timeout=10)
    return dict(line.split('=', 1) for line in raw.splitlines() if '=' in line)


async def check(report):
    deadline = time.monotonic()+90
    async with httpx.AsyncClient(trust_env=False, timeout=3) as client:
        while True:
            report['last_process'] = status()
            if report['last_process']['MainPID'] == '0':
                raise RuntimeError('preheat process exited')
            try:
                response = await client.get(BASE+'/v1/prediction-markets/polymarket/live/discovery/status')
                response.raise_for_status()
                if len(response.content) > 16384:
                    raise ValueError('status byte cap')
                report['last_status'] = response.json()
                if (report['last_status'].get('boundary_cursor') or 0) > 0:
                    break
            except httpx.HTTPError:
                pass
            if time.monotonic() >= deadline:
                raise TimeoutError('Discovery real source preheat deadline')
            await asyncio.sleep(1)
    manifest = json.loads((ROOT/'catalog.json').read_bytes())
    report['boundary'] = asdict(await probe_discovery(BASE, projection_id=None,
        universe_revision=manifest['realtime_universe']['universe_id'], catalog_revision=manifest['catalog_revision'],
        market_ids=manifest['realtime_universe']['market_ids'], timeout_seconds=deadline-time.monotonic(),
        full_sync_bytes=67108864, frame_bytes=16777216, maximum_frames=10, maximum_stream_bytes=67108864))


def main():
    os.umask(0o077)
    output = R/'logs/universe-discovery-preheat-r2-report.json'
    if output.exists():
        raise ValueError('refuse overwrite or blind rerun')
    with socket.socket() as probe:
        probe.bind(('127.0.0.1', 18900))
    binary = RELEASE/'marketcow-discovery-collector'
    with binary.open('rb') as stream:
        assert hashlib.file_digest(stream, 'sha256').hexdigest() == '3e1a7c1acbde90a0c6ddf3a9486baa5f8a26b9368c39cd0044795da89729d0cb'
    units = Path('/home/czx/.config/systemd/user')
    incumbent = (units/'marketcow-polymarket-discovery.service').resolve(strict=True)
    text = incumbent.read_text()
    start, = [line for line in text.splitlines() if line.startswith('ExecStart=')]
    args = shlex.split(start.removeprefix('ExecStart='))
    index, = [i for i, value in enumerate(args) if value.endswith('/marketcow-discovery-collector')]
    args[index] = str(binary)
    preparation = json.loads((ROOT/'candidate-preparation.json').read_bytes())
    updates = {'--root': str(ROOT), '--plan': str(ROOT/'rust-discovery-plan.json'),
        '--plan-sha256': preparation['plan_sha256'], '--discovery-seed': str(ROOT/'discovery-public-seed.json'),
        '--discovery-seed-sha256': preparation['seed_sha256'], '--discovery-listen': '127.0.0.1:18900',
        '--log': str(R/'logs/universe-discovery-preheat-r2.log')}
    for flag, value in updates.items():
        assert args.count(flag) == 1
        args[args.index(flag)+1] = value
    assert all(not any(c in value for c in '\n\r\t "\'\\$%') for value in args)
    body = text.replace(start, 'ExecStart='+' '.join(args)).replace('Restart=on-failure', 'Restart=no\nRuntimeMaxSec=180')
    path = RELEASE/'units'/UNIT
    with path.open('x') as stream:
        stream.write(body); stream.flush(); os.fsync(stream.fileno())
    path.chmod(0o400)
    (units/UNIT).symlink_to(path)
    report = {'passed': False, 'error': None, 'started_at_ns': time.time_ns(), 'formal_changed': False}
    try:
        subprocess.run(['systemctl', '--user', 'daemon-reload'], check=True, timeout=10)
        subprocess.run(['systemctl', '--user', 'start', UNIT], check=True, timeout=20)
        asyncio.run(check(report))
        report['passed'] = True
    except Exception as error:
        report['error'] = str(error)
    finally:
        try:
            subprocess.run(['systemctl', '--user', 'stop', UNIT], check=True, timeout=90)
            report['stopped'] = status()
            assert report['stopped']['MainPID'] == '0' and report['stopped']['Result'] == 'success' and report['stopped']['ExecMainStatus'] == '0'
            with sqlite3.connect((ROOT/'indexes/latest-state.sqlite3').as_uri()+'?mode=ro', uri=True) as db:
                db.execute('BEGIN')
                assert db.execute('PRAGMA quick_check').fetchone() == ('ok',)
                metadata = dict(db.execute('SELECT key,value FROM metadata'))
                tail = db.execute('SELECT cursor,payload,sha256 FROM recent_events ORDER BY cursor DESC LIMIT 1').fetchone()
                assert tail[0] == int(metadata['latest_cursor']) and hashlib.sha256(tail[1]).hexdigest() == tail[2]
                assert int(metadata['recent_event_bytes']) <= int(metadata['bounded_history_bytes'])
                report['durable'] = dict(metadata=metadata, tail_sha256=tail[2])
        except Exception as error:
            report['cleanup_error'] = str(error); report['passed'] = False
        report['ended_at_ns'] = time.time_ns()
        with output.open('x') as stream:
            json.dump(report, stream, sort_keys=True); stream.flush(); os.fsync(stream.fileno())
    print(json.dumps(report, sort_keys=True))
    if not report['passed']:
        raise SystemExit(1)


if __name__ == '__main__':
    main()
