"""Bounded offline same-process scheduling experiment; never connects to live WS.

Real retained events are frozen once; downstream work is explicitly synthetic.
This is a mechanism experiment, not attribution of a particular production stall.
"""
import asyncio
import argparse
from collections import deque
import contextvars
import hashlib
import json
from pathlib import Path
import sqlite3
import threading
import time

from marketcow.polymarket_live_stream import _decode_stream_message

BASE = Path('/mnt/p44pro/marketcow-shadow-v3-runtime/linux')
OUT = BASE / 'logs/decode-scheduling-offline-r1'
parser = argparse.ArgumentParser()
parser.add_argument('--input', type=Path)
parser.add_argument('--output', type=Path, default=OUT)
args = parser.parse_args()
OUT = args.output


def digest(data):
    return hashlib.sha256(data).hexdigest()


def quantiles(values):
    values = sorted(values)
    return {name: values[min(len(values)-1, int(len(values)*p))] * 1e6
            for name, p in [('p50_us', .5), ('p95_us', .95), ('max_us', 1)]}


async def arm(raws, chunk):
    loop = asyncio.get_running_loop()
    segments = deque(maxlen=4096)
    timeline = deque(maxlen=128)
    lag = deque(maxlen=4096)
    done = asyncio.Event()
    counters = dict(submitted=0, running=0, completed=0, peak_running=0)
    # Decode and validate all fixtures once to construct identical model_dump work.
    models = [_decode_stream_message(raw)[1] for raw in raws]
    async def competing():
        for _ in range(32):
            for start in range(0, len(models), chunk or len(models)):
                a = time.perf_counter()
                for model in models[start:start+(chunk or len(models))]:
                    json.dumps(model.model_dump(mode='json'), separators=(',', ':'))
                segments.append((a, time.perf_counter()))
                await asyncio.sleep(0)
        await done.wait()
    async def ticker():
        while not done.is_set():
            expected = time.perf_counter() + .001
            await asyncio.sleep(.001)
            lag.append(max(0, time.perf_counter() - expected))
    competing_task = asyncio.create_task(competing()) if chunk is not None else None
    ticker_task = asyncio.create_task(ticker())
    started = time.perf_counter()
    output_hash = hashlib.sha256()
    for ordinal in range(128):
        raw = raws[ordinal % len(raws)]
        row = {'ordinal': ordinal, 'bytes': len(raw), 'submit': time.perf_counter()}
        counters['submitted'] += 1
        def worker():
            row['entry'] = time.perf_counter()
            row['thread_id'] = threading.get_native_id()
            counters['running'] += 1
            counters['peak_running'] = max(counters['peak_running'], counters['running'])
            cpu = time.thread_time()
            try:
                message, model = _decode_stream_message(raw)
                row['cursor'] = message['event']['cursor']
                row['type'] = message['type']
                return model
            finally:
                row['thread_cpu'] = time.thread_time() - cpu
                counters['running'] -= 1
                counters['completed'] += 1
                row['finish'] = time.perf_counter()
        # Same default-executor/context-copy scheduling primitive as to_thread;
        # done callback timestamp measures loop callback, not worker completion.
        future = loop.run_in_executor(None, contextvars.copy_context().run, worker)
        future.add_done_callback(lambda _, row=row: row.update(callback=time.perf_counter()))
        model = await future
        row['resume'] = time.perf_counter()
        row['sync_overlap'] = sum(max(0, min(b,row['resume'])-max(a,row['finish']))
                                  for a,b in segments)
        row['unexplained'] = max(0,row['resume']-row['finish']-row['sync_overlap'])
        output_hash.update(model.model_dump_json().encode())
        timeline.append(row)
    done.set()
    await ticker_task
    if competing_task:
        await competing_task
    return {'chunk': chunk, 'seconds': time.perf_counter()-started,
            'decode_only_counters_not_whole_executor': counters,
            'output_sha256': output_hash.hexdigest(), 'timeline': list(timeline),
            'finish_to_resume': quantiles([r['resume']-r['finish'] for r in timeline]),
            'entry_wait': quantiles([r['entry']-r['submit'] for r in timeline]),
            'loop_lag': quantiles(list(lag)),
            'sync_overlap_seconds': sum(r['sync_overlap'] for r in timeline),
            'unexplained_seconds': sum(r['unexplained'] for r in timeline),
            'sync_segments_retained': len(segments),
            'instrumentation': 'same clock, bounded128 frame rows/4096 sync spans/4096 lag samples; overhead not calibrated'}


async def main():
    OUT.mkdir(mode=0o700)
    dbpath = BASE / 'bounded-scoped-candidate-r1/indexes/latest-state.sqlite3'
    if args.input:
        frozen_input = args.input.read_bytes()
        assert len(frozen_input) <= 16777216
        inputs = json.loads(frozen_input)
        assert 0 < len(inputs) <= 16
        rows = [(json.dumps(json.loads(raw)['event']).encode(), None) for raw in inputs]
    else:
        with sqlite3.connect(f'file:{dbpath}?mode=ro', uri=True) as db:
            rows = db.execute('SELECT payload,sha256 FROM recent_events ORDER BY cursor DESC LIMIT 16').fetchall()
    raws = []
    for payload, sha in reversed(rows):
        assert sha is None or digest(payload) == sha
        raw = json.dumps({'type': 'event', 'event': json.loads(payload)}, separators=(',', ':'))
        _decode_stream_message(raw)
        raws.append(raw)
    if args.input:
        raws = inputs
    frozen = json.dumps(raws).encode()
    (OUT / 'input.json').write_bytes(frozen)
    # All arms use byte-identical frozen inputs and identical ordered validation.
    results = [await arm(raws, chunk) for chunk in (None, 16, 1)]
    assert len({r['output_sha256'] for r in results}) == 1
    report = {'input_sha256': digest(frozen), 'real_source_fixture_synthetic_competition': True,
              'arms': results, 'not_production_causal_proof': True}
    (OUT / 'report.json').write_text(json.dumps(report, indent=2))
    print(json.dumps({**report, 'arms': [{k:v for k,v in r.items() if k != 'timeline'} for r in results]}))


asyncio.run(main())
