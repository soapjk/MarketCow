#!/usr/bin/env python3
"""Bounded same-machine SDK delivery comparison. No history or trading calls.

Measures public-minus-LAN arrival, not a one-way network latency or UTC age.
Only scalar identities/timestamps enter the bounded matcher, never book depth.
Tokens come from environment and are never included in the final cold report.
"""
import argparse
import asyncio
from collections import Counter, OrderedDict
from datetime import datetime
import json
import math
import os
import time
import select
import shlex
import subprocess
from urllib.parse import urlsplit


def event_identity(event, mode):
    if mode == "prices":
        if event.kind != "trade":
            return None
        f = event.frame
        return (f.producer_instance_id, f.source, f.stream_id,
                f.stream_sequence, event.trade.trade_id)
    if event.kind != "price_change" or event.key is None:
        return None
    k = event.key
    return (k.market_id, k.condition_id, k.window_id, k.start_at, k.end_at,
            k.up_token_id, k.down_token_id, k.rules.id, k.rules.revision,
            event.stream_id, event.state_sequence)


def distribution(values):
    if not values:
        return {"count": 0}
    ordered = sorted(values)
    def q(p):
        return ordered[max(0, math.ceil(p * len(ordered)) - 1)] / 1e6
    return dict(count=len(values), p50_ms=q(.5), p95_ms=q(.95), p99_ms=q(.99),
                min_ms=ordered[0] / 1e6, max_ms=ordered[-1] / 1e6,
                negative_count=sum(v < 0 for v in ordered))


def iso_ns(value):
    t=datetime.fromisoformat(value.replace('Z','+00:00'))
    if t.tzinfo is None:
        raise ValueError('source timestamp has no timezone')
    return int(t.timestamp()*1_000_000_000)


def source_clock_probe(host):
    """Eight diagnostics over existing SSH, never set a clock or run a daemon.

    Server timestamp lies between local send/receive times. The minimum-RTT
    midpoint is an offset estimate, with half-RTT uncertainty, not a new global
    freshness policy or a replacement supplier timestamp.
    """
    code='import sys,time\nfor line in sys.stdin: print(time.time_ns(),flush=True)'
    p=subprocess.Popen(['ssh','-T','-o','BatchMode=yes','-o','ConnectTimeout=5',host,
                        'python3 -u -c '+shlex.quote(code)],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True,bufsize=1)
    samples=[]
    try:
        for _ in range(8):
            sent=time.time_ns();mono=time.monotonic_ns()
            p.stdin.write('time\n');p.stdin.flush()
            if not select.select([p.stdout],[],[],8)[0]:
                raise TimeoutError('source clock diagnostic timeout')
            remote=int(p.stdout.readline().strip())
            rtt=time.monotonic_ns()-mono;received=time.time_ns()
            if abs((received-sent)-rtt)>1_000_000:
                raise RuntimeError('local clock step during diagnostic')
            samples.append({'source_minus_mac_ns':remote-(sent+received)//2,'rtt_ns':rtt})
    finally:
        p.stdin.close()
        try:p.wait(timeout=3)
        except subprocess.TimeoutExpired:p.kill();p.wait()
    best=min(samples,key=lambda x:x['rtt_ns'])
    return {**best,'uncertainty_ns':best['rtt_ns']//2,'samples':len(samples)}


class QuoteAges:
    """Clock-sensitive diagnostics, separate from same-host arrival pairing."""
    def __init__(self):
        self.last={}
        self.stream={}
        self.quotes={'lan':[],'public':[]}
        self.published={'lan':[],'public':[]}

    def add(self,lane,event,wall_ns):
        if event.kind!='price_change' or event.frame is None:
            return
        if self.stream.get(lane)!=event.stream_id:
            self.last={k:v for k,v in self.last.items() if k[0]!=lane}
            self.stream[lane]=event.stream_id
        if len(self.published[lane])>=200_000:
            raise RuntimeError('measurement_age_capacity')
        self.published[lane].append(wall_ns-iso_ns(event.frame.published_at))
        for meta in event.frame.price_change.post_metadata:
            if not meta.data_valid or meta.quote_source_at is None:
                continue
            key=(lane,meta.token_id)
            if self.last.get(key)==meta.quote_source_at:
                continue
            if len(self.quotes[lane])>=200_000:
                raise RuntimeError('measurement_age_capacity')
            self.last[key]=meta.quote_source_at
            self.quotes[lane].append(wall_ns-iso_ns(meta.quote_source_at))

    def report(self):
        return {'semantics':'signed clock-sensitive diagnostics, not precise one-way network latency; new quote timestamps only',
                'supplier_quote_to_mac':{k:distribution(v) for k,v in self.quotes.items()},
                'source_publication_to_mac':{k:distribution(v) for k,v in self.published.items()}}


class Matcher:
    def __init__(self, maximum_pending=20_000, maximum_pairs=200_000):
        self.pending = OrderedDict()
        self.seen = set()
        self.deltas = []
        self.counts = Counter()
        self.maximum_pending = maximum_pending
        self.maximum_pairs = maximum_pairs

    def add(self, lane, identity, at_ns):
        self.counts[lane + "_increments"] += 1
        if identity in self.seen:
            self.counts[lane + "_duplicates"] += 1
            return
        previous = self.pending.get(identity)
        if previous is None:
            if len(self.pending) >= self.maximum_pending:
                _, (old_lane, _) = self.pending.popitem(last=False)
                self.counts[old_lane + "_unmatched_evicted"] += 1
            self.pending[identity] = (lane, at_ns)
        elif previous[0] == lane:
            self.counts[lane + "_duplicates"] += 1
        else:
            if len(self.deltas) >= self.maximum_pairs:
                raise RuntimeError("measurement_pair_capacity")
            del self.pending[identity]
            self.seen.add(identity)
            self.deltas.append(at_ns - previous[1] if lane == "public" else previous[1] - at_ns)

    def report(self):
        counts = self.counts.copy()
        for lane, _ in self.pending.values():
            counts[lane + "_unmatched_at_end"] += 1
        return {"public_minus_lan_arrival": distribution(self.deltas), "events": dict(counts),
                "pending_limit": self.maximum_pending, "pair_limit": self.maximum_pairs}


async def measure(args):
    # Import only during an explicit run, so unit tests do not load native IO.
    from marketcow_sdk import AsyncClient, Source
    matcher = Matcher()
    ages=QuoteAges()
    cold = {}
    errors = {}
    reasons={}
    started = time.monotonic_ns()
    deadline = asyncio.get_running_loop().time() + args.seconds

    async def receive(lane, endpoint, token):
        try:
            async with AsyncClient(endpoint, api_key=token) as client:
                stream = (client.prices.subscribe(Source.BINANCE_BTCUSDT) if args.mode == "prices"
                          else client.markets.subscribe_current())
                async with stream:
                    iterator = stream.__aiter__()
                    while (remaining := deadline - asyncio.get_running_loop().time()) > 0:
                        try:
                            event = await asyncio.wait_for(iterator.__anext__(), remaining)
                        except (TimeoutError, StopAsyncIteration):
                            break
                        at = time.monotonic_ns()  # before any field inspection
                        kind = event.kind
                        matcher.counts[lane + ":" + kind] += 1
                        if kind=='recovering':
                            reason=str(getattr(event,'reason','unknown'))[:300]
                            if 'Bearer' in reason:reason='redacted recovery detail'
                            bucket=reasons.setdefault(lane,{})
                            if reason in bucket or len(bucket)<16:bucket[reason]=bucket.get(reason,0)+1
                        if kind == "snapshot" and lane not in cold:
                            cold[lane] = (at - started) / 1e6
                        identity = event_identity(event, args.mode)
                        if identity is not None:
                            matcher.add(lane, identity, at)
                            if args.mode=='market':
                                ages.add(lane,event,time.time_ns())
                u = client.usage()
                cold[lane + "_sdk_payload_bytes"] = u.price_message_bytes + u.market_message_bytes + u.http_response_bytes
        except Exception as error:
            # Error class only: upstream exception messages can contain URLs.
            errors[lane] = type(error).__name__

    tasks=[asyncio.create_task(receive("public", args.public, os.environ["MARKETCOW_PUBLIC_API_KEY"]))]
    if not getattr(args,'single_public',False):
        tasks.append(asyncio.create_task(receive("lan", args.lan, os.environ.get("MARKETCOW_LAN_API_KEY"))))
    done,pending=await asyncio.wait(tasks,timeout=args.seconds+15)
    if pending:
        errors["cleanup"]="measurement_cleanup_deadline"
        for task in pending:task.cancel()
        # Do not hide an unbounded native close behind another unbounded wait.
        await asyncio.wait(pending,timeout=3)
    return {"mode": args.mode, "elapsed_seconds": (time.monotonic_ns()-started)/1e9,
            "measurement": ("single public SDK source-clock diagnostics; no same-event LAN pairing" if getattr(args,'single_public',False)
                            else "same-process monotonic SDK delivery increment; not one-way network latency"),
            "path_note": args.path_note, "first_snapshot_ms_and_payload": cold,
            **matcher.report(), "clock_diagnostics":ages.report() if args.mode=='market' else None,
            'recovery_reasons':reasons,'single_public':getattr(args,'single_public',False), "errors": errors}


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--lan", default="http://192.168.124.3:8793")
    p.add_argument("--public", default="https://marketcow.llmay.cn")
    p.add_argument("--seconds", type=int, choices=range(1, 601), default=120)
    p.add_argument("--mode", choices=("prices", "market"), default="prices")
    p.add_argument('--single-public',action='store_true',help='do not consume an extra source market slot')
    p.add_argument("--path-note", required=True, help="record Mac proxy/TUN path, not secrets")
    args = p.parse_args()
    endpoint = urlsplit(args.public)
    if endpoint.scheme != "https" or endpoint.username or endpoint.password or endpoint.query or endpoint.fragment:
        p.error("public endpoint must be HTTPS without URL credentials/query/fragment")
    if not os.environ.get("MARKETCOW_PUBLIC_API_KEY"):
        p.error("MARKETCOW_PUBLIC_API_KEY is required")
    result = asyncio.run(measure(args))
    print(json.dumps(result, ensure_ascii=False, separators=(",", ":")))
    usable=(result['events'].get('public_increments',0) if args.single_public else result['public_minus_lan_arrival']['count'])
    if result["errors"] or usable == 0:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
