"""Fixed-work offline scheduling experiment; no production socket or mutations.

Per-frame rendezvous gives all arms exactly 16 synthetic dumps per decode.
This controlled workload is NOT a reconstruction of production task arrivals.
"""
import argparse
import asyncio
from collections import deque
import contextvars
import hashlib
import json
from pathlib import Path
import threading
import time

from marketcow.polymarket_live_stream import _decode_stream_message

EXPECTED = 'a3c4de72f67bb5a136b4b3137b4698cd3cd5a237af3ce8d771fe8930d3db2bec'


def percentile(values):
    values = sorted(values)
    return {k: values[min(len(values)-1,int(len(values)*p))]*1e6
            for k,p in [('p50_us',.5),('p95_us',.95),('max_us',1)]} if values else {}


async def trial(raws, chunk, instrument):
    models = [_decode_stream_message(raw)[1] for raw in raws]
    loop = asyncio.get_running_loop()
    # Same default executor and warmup before every arm.
    for raw in raws:
        await asyncio.to_thread(_decode_stream_message, raw)
    spans = deque(maxlen=4096)
    timeline = deque(maxlen=128)
    lag = deque(maxlen=4096)
    counts = {'decode_submitted':0,'decode_completed':0,'synthetic_operations':0,
              'spans_seen':0,'lag_seen':0}
    request = asyncio.Queue(maxsize=1)
    start = asyncio.Event()
    stop = asyncio.Event()

    async def compete():
        await start.wait()
        for ordinal in range(128):
            done = await request.get()
            for offset in range(0,16,chunk):
                if instrument:
                    begin = time.perf_counter()
                    cpu = time.thread_time()
                for index in range(offset,offset+chunk):
                    json.dumps(models[index % len(models)].model_dump(mode='json'), separators=(',', ':'))
                    counts['synthetic_operations'] += 1
                if instrument:
                    spans.append((begin,time.perf_counter(),ordinal,time.thread_time()-cpu))
                    counts['spans_seen'] += 1
                await asyncio.sleep(0)
            done.set()

    async def ticker():
        await start.wait()
        while not stop.is_set():
            expected = time.perf_counter()+.001
            await asyncio.sleep(.001)
            lag.append(max(0,time.perf_counter()-expected))
            counts['lag_seen'] += 1

    competitor = asyncio.create_task(compete())
    ticker_task = asyncio.create_task(ticker()) if instrument else None
    digest = hashlib.sha256()
    started = time.perf_counter()
    start.set()
    for ordinal in range(128):
        raw = raws[ordinal % len(raws)]
        done = asyncio.Event()
        row = {'ordinal':ordinal, 'bytes':len(raw)}
        if instrument:
            row['submit'] = time.perf_counter()
        counts['decode_submitted'] += 1
        def worker(raw=raw,row=row):
            if instrument:
                row['entry'] = time.perf_counter()
                row['thread_id'] = threading.get_native_id()
                cpu = time.thread_time()
            message, model = _decode_stream_message(raw)
            if instrument:
                row['cursor'] = message['event']['cursor']
                row['type'] = message['type']
                row['thread_cpu'] = time.thread_time()-cpu
                row['finish'] = time.perf_counter()
            return model
        future = loop.run_in_executor(None,contextvars.copy_context().run,worker)
        if instrument:
            future.add_done_callback(lambda _,row=row:row.update(callback=time.perf_counter()))
        request.put_nowait(done)
        model = await future
        counts['decode_completed'] += 1
        if instrument:
            row['resume'] = time.perf_counter()
        # Full roundtrip equality is checked in ALL arms, outside resume timing.
        digest.update(model.model_dump_json().encode())
        await done.wait()
        if instrument:
            row['overlap'] = sum(max(0,min(b,row['resume'])-max(a,row['finish'])) for a,b,_,_ in spans)
            row['unexplained'] = max(0,row['resume']-row['finish']-row['overlap'])
            timeline.append(row)
    await competitor
    stop.set()
    if ticker_task:
        await ticker_task
    assert counts['synthetic_operations'] == 2048
    assert counts['decode_submitted'] == counts['decode_completed'] == 128
    return {'chunk':chunk,'instrumented':instrument,'seconds':time.perf_counter()-started,
            'counts_decode_only':counts,'output_sha256':digest.hexdigest(),
            'finish_resume':percentile([r['resume']-r['finish'] for r in timeline]),
            'finish_callback':percentile([r['callback']-r['finish'] for r in timeline]),
            'callback_resume':percentile([r['resume']-r['callback'] for r in timeline]),
            'entry_wait':percentile([r['entry']-r['submit'] for r in timeline]),
            'loop_lag':percentile(list(lag)), 'timeline':list(timeline),
            'sync_spans':list(spans), 'spans_evicted':max(0,counts['spans_seen']-4096),
            'lag_evicted':max(0,counts['lag_seen']-4096),
            'overlap_seconds':sum(r['overlap'] for r in timeline),
            'unexplained_seconds':sum(r['unexplained'] for r in timeline)}


async def main(args):
    frozen=args.input.read_bytes()
    assert len(frozen)<=16777216 and hashlib.sha256(frozen).hexdigest()==EXPECTED
    raws=json.loads(frozen)
    assert len(raws)==16
    args.output.mkdir(mode=0o700)
    (args.output/'input.json').write_bytes(frozen)
    results=[]
    for repeat in range(3):
        order=[(16,False),(16,True),(1,True),(1,False)]
        if repeat % 2:
            order.reverse()
        for chunk,instrument in order:
            row=await trial(raws,chunk,instrument)
            row['repeat']=repeat
            results.append(row)
    assert len({r['output_sha256'] for r in results})==1
    report={'input_sha256':EXPECTED,'real_input_synthetic_competition':True,
            'bounds':{'trials':12,'decode_per_trial':128,'dump_per_trial':2048,
                      'timeline':128,'spans':4096,'lag':4096,'async_timeout_seconds':30},
            'coverage':'rendezvous per decode; spans cover only synthetic dump+JSON, not whole loop/executor',
            'arms':results}
    content=json.dumps(report,indent=2).encode()
    (args.output/'report.json').write_bytes(content)
    print(json.dumps({'report_sha256':hashlib.sha256(content).hexdigest(),
        'arms':[{k:v for k,v in r.items() if k not in ('timeline','sync_spans')} for r in results]}))


parser=argparse.ArgumentParser()
parser.add_argument('--input',type=Path,required=True)
parser.add_argument('--output',type=Path,required=True)
args=parser.parse_args()
asyncio.run(asyncio.wait_for(main(args),30))
