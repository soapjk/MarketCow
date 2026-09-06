"""Finite isolated direct-Rust Discovery probe. No formal service mutations."""
import argparse
import asyncio
import hashlib
import json
import socket
import sqlite3
import subprocess
import time
import urllib.request
from pathlib import Path

import websockets

BASE = Path('/mnt/p44pro/marketcow-shadow-v3-runtime/linux')
ROOT = BASE / 'public-discovery-candidate-r1'
PREFIX = BASE / 'logs/public-discovery-smoke-r1'
UNIT = 'marketcow-public-discovery-smoke-collector-r1'
URL = 'http://127.0.0.1:8795/v1/prediction-markets/polymarket/live/discovery/'


def sha(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def props():
    output = subprocess.check_output(['systemctl', '--user', 'show', UNIT,
        '-p', 'MainPID', '-p', 'Result', '-p', 'ExecMainStatus'], text=True)
    return dict(line.split('=', 1) for line in output.splitlines())


def get(endpoint):
    with urllib.request.urlopen(URL + endpoint, timeout=10) as response:
        raw = response.read(67108865)
    assert len(raw) <= 67108864
    return raw


async def audit(report):
    deadline = time.monotonic() + 45
    while True:
        try:
            raw = await asyncio.to_thread(get, 'full-sync')
            break
        except Exception as error:
            report['last_startup_error'] = repr(error)
            if props()['MainPID'] == '0' or time.monotonic() >= deadline:
                raise
            await asyncio.sleep(1)
    path = PREFIX.with_suffix('.full-sync.json')
    with path.open('xb') as stream:
        stream.write(raw)
    baseline = json.loads(raw)
    assert len(baseline['markets']) == 1000
    report['full_sync'] = {'sha256': sha(path), 'bytes': len(raw), 'markets': 1000}
    cursor = baseline['boundary_cursor']
    report['initial_cursor'] = cursor
    report['projection_id'] = baseline['projection_id']
    report['status_before'] = json.loads(await asyncio.to_thread(get, 'status'))
    uri = URL.replace('http:', 'ws:') + f"stream?after_cursor={cursor}&projection_id={baseline['projection_id']}"
    count = 0
    size = 0
    resources = []
    started = time.monotonic()
    async with websockets.connect(uri, max_size=16777216, max_queue=1, close_timeout=5) as ws:
        with PREFIX.with_suffix('.ws.jsonl').open('xb') as wire:
            while time.monotonic() - started < 20 and count < 1000:
                raw = await asyncio.wait_for(ws.recv(), timeout=20)
                encoded = raw.encode() if isinstance(raw, str) else raw
                assert size + len(encoded) < 67108864
                wire.write(encoded + b'\n')
                size += len(encoded) + 1
                frame = json.loads(encoded)
                assert not frame['resync_required'], frame
                assert frame['projection_id'] == baseline['projection_id']
                assert frame['after_cursor'] == cursor and frame['next_cursor'] == cursor + 1
                cursor += 1
                for item in frame['items']:
                    assert set(item) == {'type', 'payload'}
                    if item['type'] == 'market_update':
                        assert item['payload']['cursor'] == cursor
                count += 1
                if count == 1 or count % 100 == 0:
                    pid = int(props()['MainPID'])
                    resources.append({line.split(':')[0]: line.split(':', 1)[1].strip()
                        for line in Path(f'/proc/{pid}/status').read_text().splitlines()
                        if line.startswith(('VmRSS:', 'VmHWM:', 'Threads:'))})
    assert count >= 5
    report.update(frames=count, final_cursor=cursor, resources=resources,
        elapsed_seconds=time.monotonic()-started, wire_sha256=sha(PREFIX.with_suffix('.ws.jsonl')))
    report['status_after'] = json.loads(await asyncio.to_thread(get, 'status'))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--binary-sha256', required=True)
    args = parser.parse_args()
    binary = BASE / 'target/release/marketcow-discovery-collector'
    assert sha(binary) == args.binary_sha256
    assert not PREFIX.with_suffix('.report.json').exists()
    assert json.loads((ROOT/'public-candidate-report.json').read_text())['complete']
    with socket.socket() as check:
        check.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        check.bind(('127.0.0.1', 8795))
    cmd = [str(binary), '--root', str(ROOT), '--plan', str(ROOT/'rust-source-plan.json'),
        '--plan-sha256', sha(ROOT/'rust-source-plan.json'), '--expected-market-count', '1000',
        '--input-mode', 'rest-poll', '--concurrency', '16', '--market-workers', '6',
        '--request-market-batch-size', '10', '--response-byte-limit', '2097152',
        '--batch-byte-limit', '16777216', '--poll-seconds', '30', '--request-timeout-seconds', '10',
        '--bounded-history-bytes', '67108864', '--persistence-queue-batches', '256',
        '--persistence-queue-bytes', '134217728', '--discovery-listen', '127.0.0.1:8795',
        '--discovery-seed', str(ROOT/'discovery-public-seed.json'),
        '--discovery-seed-sha256', sha(ROOT/'discovery-public-seed.json'),
        '--discovery-state-bytes', '134217728', '--discovery-full-sync-bytes', '67108864',
        '--discovery-frame-bytes', '16777216', '--discovery-replay-bytes', '8388608',
        '--discovery-clients', '2', '--discovery-baselines', '2', '--discovery-send-timeout-seconds', '5']
    report = {'passed': False, 'command': cmd, 'binary_sha256': args.binary_sha256}
    try:
        subprocess.run(['systemd-run', '--user', '--unit='+UNIT,
            '--property=MemoryMax=4G', '--property=RuntimeMaxSec=120',
            '--property=KillSignal=SIGINT', '--property=TimeoutStopSec=60',
            '--property=StandardOutput=append:'+str(PREFIX.with_suffix('.collector.log')),
            '--property=StandardError=append:'+str(PREFIX.with_suffix('.collector.log')),
            '/usr/bin/env', '-u', 'ALL_PROXY', '-u', 'all_proxy',
            'HTTPS_PROXY=http://127.0.0.1:17890', 'HTTP_PROXY=http://127.0.0.1:17890',
            'NO_PROXY=127.0.0.1,localhost', *cmd], check=True)
        asyncio.run(audit(report))
        report['passed'] = True
    except Exception as error:
        report['error'] = repr(error)
    finally:
        subprocess.run(['systemctl', '--user', 'stop', UNIT], timeout=70, check=False)
        report['unit'] = props()
        report['passed'] &= report['unit']['MainPID'] == '0' and report['unit']['Result'] == 'success' and report['unit']['ExecMainStatus'] == '0'
        with sqlite3.connect(f'file:{ROOT}/indexes/latest-state.sqlite3?mode=ro', uri=True) as db:
            report['metadata'] = dict(db.execute('SELECT * FROM metadata'))
            report['integrity_check'] = db.execute('PRAGMA integrity_check').fetchone()[0]
        report['no_jsonl'] = not (ROOT/'events.jsonl').exists()
        report['passed'] &= report['integrity_check'] == 'ok' and report['no_jsonl']
        PREFIX.with_suffix('.report.json').write_text(json.dumps(report, indent=2))
        PREFIX.with_suffix('.exit-code').write_text('0\n' if report['passed'] else '1\n')
    raise SystemExit(0 if report['passed'] else 1)


if __name__ == '__main__':
    main()
