"""Thirty-second single-market raw WS age probe, no state publication."""
import asyncio
from collections import defaultdict
import json
import os
from pathlib import Path
import socket
import ssl
import statistics
import time

import websockets
import yaml

R = Path('/mnt/p44pro/marketcow-shadow-v3-runtime/linux')
HOST = 'ws-subscriptions-clob.polymarket.com'

def tunnel():
    sock = socket.create_connection(('127.0.0.1', 17890), timeout=5)
    try:
        sock.sendall(f'CONNECT {HOST}:443 HTTP/1.1\r\nHost: {HOST}:443\r\n\r\n'.encode())
        header = b''
        while not header.endswith(b'\r\n\r\n'):
            piece = sock.recv(1)
            if not piece or len(header) >= 8192:
                raise RuntimeError('invalid CONNECT response')
            header += piece
        if header.split(b'\r\n')[0].split()[1] != b'200':
            raise RuntimeError('CONNECT rejected')
        sock.setblocking(False)
        return sock
    except BaseException:
        sock.close()
        raise

def stats(values):
    v = sorted(values)
    if not v:
        return {'count': 0}
    return {'count': len(v), 'p50_ms': statistics.median(v),
            'p95_ms': v[min(len(v)-1, int((len(v)-1)*.95))],
            'max_ms': max(v), 'min_ms': min(v), 'over_5s': sum(x>5000 for x in v)}

async def main():
    os.umask(0o077)
    output = R/'logs/ws-age-singapore-r3.json'
    assert not output.exists()
    config = yaml.safe_load((R/'polymarket-proxy/config.yaml').read_text())
    node = config['proxy-groups'][0]['proxies'][0]
    assert node == '🇸🇬 Pro-新加坡-BGP-01|v202605'
    market = next(m for m in json.loads((R/'dynamic-live-candidate-r1/rust-scoped-plan-r1.json').read_text())['markets'] if m['market_id']=='1365854')
    report = {'node': node, 'market_id': market['market_id'], 'duration_seconds': 30,
              'definition': 'UTC socket frame receipt minus upstream timestamp; includes clock offset and source age, not network RTT',
              'events': [], 'errors': []}
    proc = await asyncio.create_subprocess_exec(str(R/'polymarket-proxy/mihomo'), '-d',str(R/'polymarket-proxy'),
        '-f',str(R/'polymarket-proxy/config.yaml'),stdout=asyncio.subprocess.DEVNULL,stderr=asyncio.subprocess.DEVNULL)
    grouped = defaultdict(list)
    try:
        await asyncio.sleep(.3)
        sock = await asyncio.to_thread(tunnel)
        async with websockets.connect(f'wss://{HOST}/ws/market', sock=sock, ssl=ssl.create_default_context(),
                                      server_hostname=HOST, open_timeout=10, max_size=2097152) as ws:
            await ws.send(json.dumps({'assets_ids':market['token_ids'], 'type':'market'}))
            async def heartbeat():
                while True:
                    await asyncio.sleep(10)
                    await ws.send('PING')
            ping = asyncio.create_task(heartbeat())
            deadline = time.monotonic()+30
            seen_books = set()
            try:
                while time.monotonic()<deadline:
                    try:
                        raw = await asyncio.wait_for(ws.recv(), deadline-time.monotonic())
                    except asyncio.TimeoutError:
                        break
                    received_ms = time.time_ns()/1e6
                    if raw in ('PONG','PING'):
                        continue
                    data = json.loads(raw)
                    for item in data if isinstance(data,list) else [data]:
                        kind = item['event_type']
                        token = item.get('asset_id')
                        group = 'initial_book' if kind=='book' and token not in seen_books else kind
                        if kind=='book': seen_books.add(token)
                        row = {'type':kind,'group':group,'received_ms':received_ms,'upstream_timestamp':item.get('timestamp')}
                        if item.get('timestamp') is not None:
                            row['age_ms'] = received_ms - float(item['timestamp'])
                            grouped[group].append(row['age_ms'])
                        report['events'].append(row)
                        if len(report['events'])>=10000: raise RuntimeError('sample cap')
                report['both_initial_books'] = seen_books == set(market['token_ids'])
                report['complete'] = True
            finally:
                ping.cancel()
                await asyncio.gather(ping,return_exceptions=True)
    except Exception as error:
        report['errors'].append(type(error).__name__+': '+str(error))
    finally:
        if proc.returncode is None: proc.terminate()
        try: await asyncio.wait_for(proc.wait(),3)
        except asyncio.TimeoutError:
            proc.kill()
            await proc.wait()
        report['proxy_stopped'] = proc.returncode is not None
        report['age_by_type'] = {k:stats(v) for k,v in grouped.items()}
        with output.open('x') as out: json.dump(report,out,indent=2,ensure_ascii=False)
        print(json.dumps({k:v for k,v in report.items() if k!='events'},ensure_ascii=False))

if __name__ == '__main__':
    asyncio.run(main())
