"""Finite 6/8-worker authoritative WS comparison on a private SQLite backup.

No formal unit changes, no Paper connections, no shared mutable database files.
Sequential windows are not identical-market-traffic A/B measurements.
"""
import asyncio
import hashlib
import json
import os
from pathlib import Path
import shutil
import socket
import sqlite3
import subprocess
import time

from scripts import audit_bounded_live as audit

RUNTIME = Path('/mnt/p44pro/marketcow-shadow-v3-runtime/linux')
SOURCE = RUNTIME / 'bounded-scoped-candidate-r1'
ROOT = RUNTIME / 'market-scheduler-load-r1'
RUN = os.environ.get('SCHEDULER_RUN', 'r1')
assert RUN in ('r1', 'r2')
REPORT = RUNTIME / f'logs/market-scheduler-load-{RUN}-report.json'
BINARY = RUNTIME / 'target/release/marketcow-discovery-collector'
EXPECTED_BINARY = '11ead64855c26486d64990716f0199fa119195872ec82551a78b28d3b1551d1d'


def sha(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def rebase(value):
    if isinstance(value, dict):
        return {k: rebase(v) for k, v in value.items()}
    if isinstance(value, list):
        return [rebase(v) for v in value]
    if isinstance(value, str) and value.startswith(str(SOURCE) + '/'):
        return str(ROOT / Path(value).relative_to(SOURCE))
    return value


def prepare():
    assert not ROOT.exists(), 'refuse reused target'
    assert shutil.disk_usage(RUNTIME).free > 6 * 1024**3
    stage = ROOT.with_name(ROOT.name + '.preparing')
    stage.mkdir(mode=0o700)
    for name in ('catalogs', 'catalog-indexes', 'raw'):
        def copy(src, dst):
            src = Path(src)
            assert not src.is_symlink()
            if src.stat().st_mode & 0o222 == 0:
                os.link(src, dst)
            else:
                shutil.copyfile(src, dst)
            return dst
        shutil.copytree(SOURCE / name, stage / name, copy_function=copy)
    for name in ('catalog.json', 'scope-runtime.json', 'configured-scope.json',
                 'rust-scoped-plan-r1.json', 'live-bridge-plan-r1.json'):
        value = rebase(json.loads((SOURCE / name).read_text()))
        if name == 'live-bridge-plan-r1.json':
            value['catalog_manifest_sha256'] = sha(stage / 'catalog.json')
        # Scope and scoped plan retain byte-exact content hashes.
        if name in ('configured-scope.json', 'rust-scoped-plan-r1.json', 'scope-runtime.json'):
            shutil.copyfile(SOURCE / name, stage / name)
        else:
            (stage / name).write_text(json.dumps(value, separators=(',', ':')))
    (stage / 'indexes').mkdir()
    with sqlite3.connect(f'file:{SOURCE}/indexes/latest-state.sqlite3?mode=ro', uri=True) as src:
        with sqlite3.connect(stage / 'indexes/latest-state.sqlite3') as dst:
            src.backup(dst, pages=256)
            assert dst.execute('PRAGMA integrity_check').fetchone()[0] == 'ok'
            metadata = dict(dst.execute('SELECT * FROM metadata'))
    assert int(metadata['bounded_history_bytes']) == 67108864
    manifest = {k: metadata[k] for k in ('schema_version', 'catalog_revision', 'latest_cursor',
        'active_recovery_id', 'book_token_count', 'book_complete_market_count',
        'unresolved_gap_count', 'oldest_book_received_at')}
    manifest['path'] = str(ROOT / 'indexes/latest-state.sqlite3')
    (stage / 'state-index.json').write_text(json.dumps(manifest))
    stage.rename(ROOT)
    return metadata['latest_cursor']


def props(unit):
    return dict(x.split('=', 1) for x in subprocess.check_output([
        'systemctl', '--user', 'show', unit, '-p', 'MainPID', '-p', 'Result',
        '-p', 'ExecMainStatus', '-p', 'MemoryPeak'], text=True).splitlines())


def run(workers):
    unit = f'marketcow-market-scheduler-{workers}-{RUN}'
    assert props(unit)['MainPID'] == '0'
    result = {'workers': workers, 'events': 0, 'stream_passed': False}
    log = RUNTIME / f'logs/market-scheduler-{workers}-{RUN}.log'
    assert not log.exists()
    command = ['systemd-run', '--user', '--unit=' + unit, '-p', 'RuntimeMaxSec=180',
        '-p', 'TimeoutStopSec=30', '-p', 'MemoryMax=4G', '-p', 'MemorySwapMax=0',
        '-p', f'CPUQuota={workers * 100}%', '-p', 'StandardOutput=append:' + str(log),
        '-p', 'StandardError=append:' + str(log),
        '--setenv=HTTPS_PROXY=http://127.0.0.1:17890',
        '--setenv=HTTP_PROXY=http://127.0.0.1:17890', '--setenv=NO_PROXY=localhost,127.0.0.1',
        str(BINARY), '--input-mode', 'websocket', '--root', str(ROOT),
        '--market-workers', str(workers)]
    for option, name in [('plan', 'rust-scoped-plan-r1.json'),
                         ('configured-scope', 'configured-scope.json'),
                         ('dependency-plan', 'live-bridge-plan-r1.json')]:
        command += ['--' + option, str(ROOT / name), '--' + option + '-sha256', sha(ROOT / name)]
    command += ('--bounded-history-bytes 67108864 --expected-market-count 250 --concurrency 16 '
        '--request-market-batch-size 20 --response-byte-limit 2097152 --batch-byte-limit 16777216 '
        '--persistence-queue-batches 256 --persistence-queue-bytes 67108864 '
        '--websocket-shard-tokens 500 --websocket-recovery-concurrency 16 '
        '--websocket-confirmation-seconds 1 --poll-seconds 1 --request-timeout-seconds 10 '
        '--lifecycle-refresh-seconds 300 --live-listen 127.0.0.1:18897 '
        '--live-frame-bytes 67108864 --live-maximum-clients 2').split()
    subprocess.run(command, check=True)
    try:
        deadline = time.monotonic() + 35
        while True:
            assert props(unit)['MainPID'] != '0', 'collector exited before listener'
            try:
                with socket.create_connection(('127.0.0.1', 18897), timeout=1):
                    break
            except OSError:
                if time.monotonic() >= deadline:
                    raise
                time.sleep(.5)
        async def preheat():
            deadline = time.monotonic() + 45
            while True:
                try:
                    async with audit.websockets.connect('ws://127.0.0.1:18897/', max_size=67108864, max_queue=1) as ws:
                        await ws.send(json.dumps({'type': 'subscribe'}))
                        first = json.loads(await asyncio.wait_for(ws.recv(), 5))
                        assert first['type'] == 'state', first.get('type')
                        return
                except (OSError, audit.websockets.exceptions.ConnectionClosed, asyncio.TimeoutError):
                    if time.monotonic() >= deadline or props(unit)['MainPID'] == '0':
                        raise
                    await asyncio.sleep(1)
        asyncio.run(preheat())
        def sample():
            p = props(unit)
            status = Path('/proc/' + p['MainPID'] + '/status').read_text()
            return {**p, **{line.split(':')[0]: line.split(':')[1].strip()
                for line in status.splitlines() if line.startswith(('VmRSS:', 'VmHWM:'))}}
        audit.resources = sample
        asyncio.run(audit.main(result, seconds=90))
    finally:
        subprocess.run(['systemctl', '--user', 'stop', unit], check=True)
        result['stopped'] = props(unit)
        result['final_durable'] = audit.durable()
        (RUNTIME / f'logs/market-scheduler-{workers}-{RUN}-report.json').write_text(json.dumps(result, indent=2))
    assert result['stopped']['MainPID'] == '0'
    assert result['stopped']['Result'] == 'success' and result['stopped']['ExecMainStatus'] == '0'
    return result


if __name__ == '__main__':
    os.umask(0o077)
    report = {'passed': False, 'source_unchanged': str(SOURCE), 'windows': []}
    code = 1
    try:
        assert not REPORT.exists()
        assert sha(BINARY) == EXPECTED_BINARY
        with socket.socket() as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind(('127.0.0.1', 18897))
        audit.ROOT = ROOT
        if RUN == 'r1':
            report['baseline_cursor'] = prepare()
        else:
            old = json.loads((RUNTIME / 'logs/market-scheduler-6-r1-report.json').read_text())
            assert old['stopped']['Result'] == 'success' and old['stopped']['MainPID'] == '0'
            assert audit.durable() == old['final_durable'], 'candidate changed since stopped probe'
            report['baseline_cursor'] = old['final_durable']['latest_cursor']
        for workers in (6, 8):
            report['windows'].append(run(workers))
            REPORT.write_text(json.dumps(report, indent=2))
        report['passed'] = True
        code = 0
    finally:
        REPORT.write_text(json.dumps(report, indent=2))
        (RUNTIME / f'logs/market-scheduler-load-{RUN}.exit-code').write_text(str(code))
