"""Finite direct-Rust smoke test; never starts Python API or changes formal units."""
import asyncio
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import socket
import sqlite3
import subprocess
import time
import urllib.request

import websockets

BASE = Path('/mnt/p44pro/marketcow-shadow-v3-runtime/linux')
ROOT = BASE / 'public-rust-candidate-r1'
PREFIX = BASE / 'logs/public-rust-smoke-r1'
UNIT = 'marketcow-public-rust-smoke-collector-r1'
SCOPE = '54b2ad555edfd47bd1863fc71c7bb6443ca57a50c7433644b78ef3a78a4e4a12'
BINARY_SHA = '8ad46042cd084263357925342f046eae89f4166387711b976ee9ea7c72b3196b'
SECONDS = 20
WIRE_BYTES = 16777216


def sha(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def props():
    output = subprocess.check_output(['systemctl', '--user', 'show', UNIT,
        '-p', 'MainPID', '-p', 'Result', '-p', 'ExecMainStatus', '-p', 'MemoryPeak'], text=True)
    return dict(line.split('=', 1) for line in output.splitlines())


async def audit(report):
    url = f'http://127.0.0.1:8794/v1/prediction-markets/polymarket/live/full-sync?scope_id={SCOPE}'
    deadline = time.monotonic() + 60
    while True:
        try:
            with urllib.request.urlopen(url, timeout=5) as response:
                raw = response.read(134217729)
            assert len(raw) <= 134217728
            baseline = json.loads(raw)
            break
        except Exception as error:
            report['last_fullsync_error'] = repr(error)
            if props()['MainPID'] == '0' or time.monotonic() >= deadline:
                raise
            await asyncio.sleep(1)
    path = PREFIX.with_suffix('.full-sync.json')
    with path.open('xb') as stream:
        stream.write(raw)
    report['full_sync'] = {'path': str(path), 'sha256': sha(path), 'bytes': len(raw)}
    snapshot = baseline['snapshot']
    report['snapshot_keys'] = sorted(snapshot)
    cursor = baseline['cursor']
    markets = baseline['bootstrap']['markets']
    assert len(markets) == 250
    report['market_count'] = len(markets)
    report['initial_cursor'] = cursor
    report['counts'] = {}
    uri = f'ws://127.0.0.1:8794/v1/prediction-markets/polymarket/live/stream?scope_id={SCOPE}&after_cursor={cursor}'
    start = time.monotonic()
    pid = int(props()['MainPID'])
    next_sample = start
    report['resources'] = []
    async with websockets.connect(uri, max_size=67108864, max_queue=1, close_timeout=5) as ws:
        with PREFIX.with_suffix('.ws.jsonl').open('xb') as wire:
            total = 0
            while time.monotonic() - start < SECONDS and total < WIRE_BYTES:
                if time.monotonic() >= next_sample:
                    fields = {}
                    for line in Path(f'/proc/{pid}/status').read_text().splitlines():
                        if line.startswith(('VmRSS:', 'VmHWM:', 'Threads:')):
                            key, value = line.split(':', 1)
                            fields[key] = value.strip()
                    report['resources'].append({'elapsed': time.monotonic()-start, **fields})
                    next_sample = time.monotonic() + 1
                raw = await asyncio.wait_for(ws.recv(), timeout=10)
                encoded = raw.encode() if isinstance(raw, str) else raw
                if total + len(encoded) + 1 > WIRE_BYTES:
                    break
                wire.write(encoded + b'\n')
                total += len(encoded) + 1
                frame = json.loads(encoded)
                kind = frame['type']
                report['counts'][kind] = report['counts'].get(kind, 0) + 1
                assert kind != 'error', frame
                if kind == 'event':
                    assert frame['event']['cursor'] == frame['cursor'] > cursor
                    cursor = frame['cursor']
                elif kind in ('ready', 'book_confirmations'):
                    assert frame['cursor'] >= cursor
                    cursor = frame['cursor']
    report['final_cursor'] = cursor
    report['ws_elapsed_seconds'] = time.monotonic() - start
    assert report['counts'].get('ready') == 1 and report['counts'].get('event', 0) > 0


def main():
    global PREFIX, UNIT, BINARY_SHA, SECONDS, WIRE_BYTES
    parser = argparse.ArgumentParser()
    parser.add_argument('--run', required=True)
    parser.add_argument('--binary-sha256', required=True)
    parser.add_argument('--seconds', required=True, type=int)
    parser.add_argument('--wire-bytes', required=True, type=int)
    args = parser.parse_args()
    assert re.fullmatch(r'r[1-9][0-9]*', args.run)
    assert re.fullmatch(r'[0-9a-f]{64}', args.binary_sha256)
    assert 1 <= args.seconds <= 60 and 1 <= args.wire_bytes <= 67108864
    PREFIX = BASE / f'logs/public-rust-smoke-{args.run}'
    UNIT = f'marketcow-public-rust-smoke-collector-{args.run}'
    BINARY_SHA, SECONDS, WIRE_BYTES = args.binary_sha256, args.seconds, args.wire_bytes
    assert not PREFIX.with_suffix('.report.json').exists()
    preparation = json.loads((ROOT / 'public-candidate-report.json').read_text())
    assert preparation['complete'] and preparation['no_jsonl']
    binary = BASE / 'target/release/marketcow-discovery-collector'
    assert sha(binary) == BINARY_SHA
    with socket.socket() as check:
        check.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        check.bind(('127.0.0.1', 8794))
    report = {'passed': False, 'binary_sha256': BINARY_SHA, 'root': str(ROOT),
        'requested_seconds': SECONDS, 'maximum_wire_bytes': WIRE_BYTES}
    cmd = [str(binary), '--input-mode', 'websocket', '--market-workers', '6',
        '--root', str(ROOT), '--plan', str(ROOT/'rust-scoped-plan-r1.json'),
        '--plan-sha256', sha(ROOT/'rust-scoped-plan-r1.json'),
        '--configured-scope', str(ROOT/'configured-scope.json'),
        '--configured-scope-sha256', sha(ROOT/'configured-scope.json'),
        '--dependency-plan', str(ROOT/'live-bridge-plan-r1.json'),
        '--dependency-plan-sha256', sha(ROOT/'live-bridge-plan-r1.json'),
        '--expected-market-count', '250', '--concurrency', '16',
        '--request-market-batch-size', '20', '--response-byte-limit', '2097152',
        '--batch-byte-limit', '16777216', '--persistence-queue-batches', '256',
        '--persistence-queue-bytes', '67108864', '--websocket-shard-tokens', '500',
        '--websocket-recovery-concurrency', '16', '--websocket-confirmation-seconds', '0',
        '--poll-seconds', '1', '--request-timeout-seconds', '10',
        '--lifecycle-refresh-seconds', '300', '--bounded-history-bytes', '67108864',
        '--public-listen', '127.0.0.1:8794', '--public-full-sync-bytes', '134217728',
        '--public-snapshot-concurrency', '1', '--public-frame-bytes', '67108864',
        '--public-replay-bytes', '8388608', '--public-maximum-clients', '2',
        '--public-send-timeout-seconds', '5']
    try:
        subprocess.run(['systemd-run', '--user', '--unit='+UNIT,
            '--property=MemoryMax=4G', '--property=MemorySwapMax=0',
            '--property=RuntimeMaxSec=150', '--property=KillSignal=SIGINT',
            '--property=TimeoutStopSec=80',
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
        subprocess.run(['systemctl', '--user', 'stop', UNIT], check=False, timeout=90)
        report['unit'] = props()
        report['clean_stop'] = report['unit']['MainPID'] == '0' and report['unit']['Result'] == 'success' and report['unit']['ExecMainStatus'] == '0'
        report['passed'] = report['passed'] and report['clean_stop']
        with sqlite3.connect(f'file:{ROOT}/indexes/latest-state.sqlite3?mode=ro', uri=True) as db:
            report['metadata'] = dict(db.execute('SELECT * FROM metadata'))
        report['no_jsonl'] = not (ROOT/'events.jsonl').exists()
        with PREFIX.with_suffix('.report.json').open('x') as stream:
            json.dump(report, stream, indent=2)
        with PREFIX.with_suffix('.exit-code').open('x') as stream:
            stream.write('0\n' if report['passed'] else '1\n')
    raise SystemExit(0 if report['passed'] else 1)


if __name__ == '__main__':
    main()
