"""Finite authoritative source-stream retention observation; no synthesized events."""
import asyncio
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import time
import subprocess
from datetime import datetime, timezone
import websockets

RUNTIME = Path('/mnt/p44pro/marketcow-shadow-v3-runtime/linux')
ROOT = RUNTIME / 'bounded-scoped-candidate-r1'
RUN = os.environ.get('BOUNDED_LIVE_RUN', 'r2')
REPORT = RUNTIME / f'logs/bounded-live-{RUN}-report.json'


def resources():
    result = {}
    for component in ('collector', 'api'):
        unit = f'marketcow-bounded-live-{component}-{RUN}'
        props = dict(line.split('=', 1) for line in subprocess.check_output(
            ['systemctl','--user','show',unit,'-p','MainPID','-p','ControlGroup'], text=True).splitlines())
        pid = int(props['MainPID'])
        if not pid:
            raise RuntimeError(f'{unit} exited during observation')
        status = Path(f'/proc/{pid}/status').read_text()
        sample = {line.split(':')[0]: line.split(':')[1].strip() for line in status.splitlines()
                  if line.startswith(('VmRSS:', 'VmHWM:'))}
        group = Path('/sys/fs/cgroup') / props['ControlGroup'].lstrip('/')
        sample['cgroup'] = {k: int(v) for k, v in (line.split() for line in (group / 'memory.stat').read_text().splitlines())
                            if k in ('anon', 'file', 'kernel')}
        sample['disk_bytes'] = {p.name: p.stat().st_size for p in (ROOT / 'indexes').glob('latest-state.sqlite3*')}
        result[component] = sample
    return result


def durable():
    with sqlite3.connect(f'file:{ROOT}/indexes/latest-state.sqlite3?mode=ro', uri=True) as db:
        db.execute('BEGIN')
        m = dict(db.execute('SELECT * FROM metadata'))
        row = db.execute('SELECT cursor,payload,sha256 FROM recent_events ORDER BY cursor DESC LIMIT 1').fetchone()
        assert row[0] == int(m['latest_cursor'])
        assert hashlib.sha256(row[1]).hexdigest() == row[2]
        assert int(m['recent_event_bytes']) <= int(m['bounded_history_bytes'])
        return {k: m[k] for k in ('latest_cursor','history_floor_cursor','recent_event_bytes','bounded_history_bytes','unresolved_gap_count')}


async def main(report):
    report['before'] = durable()
    started = time.monotonic()
    cursor = None
    next_sample = started
    # Fixed-size millisecond histogram: bounded memory, no per-event archive.
    ages = [0] * 10002
    async with websockets.connect('ws://127.0.0.1:18897/', max_size=64*1024*1024, max_queue=1) as ws:
        await ws.send(json.dumps({'type': 'subscribe'}))
        while time.monotonic() - started < 180:
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=2)
            except asyncio.TimeoutError:
                continue
            frame = json.loads(raw)
            received = datetime.now(timezone.utc)
            assert frame['type'] != 'error', frame
            if frame['type'] == 'state':
                assert cursor is None
                cursor = frame['latest_cursor']
                report['start_cursor'] = cursor
            if frame['type'] == 'events':
                for event in frame['events']:
                    assert event['cursor'] == cursor + 1
                    cursor = event['cursor']
                    report['events'] += 1
                    if event.get('received_at'):
                        age = (received - datetime.fromisoformat(event['received_at'].replace('Z', '+00:00'))).total_seconds() * 1000
                        ages[min(10001, max(0, int(age)))] += 1
            report['last_cursor'] = cursor
            report['last_persisted_cursor'] = frame.get('persisted_cursor')
            if time.monotonic() >= next_sample:
                report.setdefault('resources', []).append(await asyncio.to_thread(resources))
                next_sample = time.monotonic() + 5
    report['seconds'] = time.monotonic() - started
    report['after'] = durable()
    report['retention_rolled'] = int(report['after']['history_floor_cursor']) > report['start_cursor']
    assert report['events'] > 0
    assert not (ROOT / 'events.jsonl').exists()
    report['stream_passed'] = True
    total = sum(ages)
    report['received_to_source_ws_ms_histogram'] = {'samples': total, 'overflow_above_10000ms': ages[-1]}
    for name, fraction in [('p50', .50), ('p95', .95), ('p99', .99)]:
        cumulative = 0
        for millis, count in enumerate(ages):
            cumulative += count
            if total and cumulative >= total * fraction:
                report['received_to_source_ws_ms_histogram'][name] = millis
                break


if __name__ == '__main__':
    os.umask(0o077)
    report = {'stream_passed': False, 'events': 0}
    try:
        asyncio.run(main(report))
    except BaseException as exc:
        report['error'] = repr(exc)
        raise
    finally:
        REPORT.write_text(json.dumps(report, indent=2))
