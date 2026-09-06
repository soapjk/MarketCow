"""Finite single-market read-only route comparison; no collector or global proxy."""
import asyncio
import json
import os
from pathlib import Path
import tempfile
import time

import httpx
import yaml

R = Path('/mnt/p44pro/marketcow-shadow-v3-runtime/linux')
P = R / 'polymarket-proxy'


async def main():
    os.umask(0o077)
    config = yaml.safe_load((P / 'config.yaml').read_text())
    market = json.loads((R / 'dynamic-live-candidate-r1/rust-scoped-plan-r1.json').read_text())['markets'][0]
    indexes = [35]
    selected = '🇸🇬 Pro-新加坡-BGP-01|v202605'
    assert config['proxies'][35]['name'] == selected
    group = next(g for g in config['proxy-groups'] if g['name'] == 'POLYMARKET')
    assert selected in group['proxies'] and group['type'] == 'select'
    original = (P / 'config.yaml').read_bytes()
    backup = P / 'config.before-singapore-r3.yaml'
    with backup.open('xb') as out:
        out.write(original)
    group['proxies'] = [selected] + [n for n in group['proxies'] if n != selected]
    pending = P / 'config.singapore-r3.tmp'
    with pending.open('x') as out:
        yaml.safe_dump(config, out, allow_unicode=True)
    pending.replace(P / 'config.yaml')
    semaphore = asyncio.Semaphore(2)

    async def probe(slot, index):
        async with semaphore:
            node = config['proxies'][index]
            result = {'node': node['name'], 'market_id': market['market_id'], 'requests': []}
            port = 17910 + slot
            # Reserve-check the precise loopback port before starting an isolated proxy.
            server = await asyncio.start_server(lambda r, w: w.close(), '127.0.0.1', port)
            server.close()
            await server.wait_closed()
            with tempfile.TemporaryDirectory(prefix='proxy-book-probe-', dir=R / 'tmp') as directory:
                local = dict(config)
                local.update({'mixed-port': port, 'proxies': [node], 'proxy-groups': [],
                              'rules': [f"DOMAIN-SUFFIX,polymarket.com,{node['name']}", 'MATCH,REJECT'],
                              'profile': {'store-selected': False}, 'log-level': 'silent'})
                path = Path(directory) / 'config.yaml'
                path.write_text(yaml.safe_dump(local, allow_unicode=True))
                proc = await asyncio.create_subprocess_exec(str(P / 'mihomo'), '-d', directory, '-f', str(path),
                    stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL)
                try:
                    for _ in range(30):
                        try:
                            _, writer = await asyncio.open_connection('127.0.0.1', port)
                            writer.close()
                            await writer.wait_closed()
                            break
                        except OSError:
                            await asyncio.sleep(0.05)
                    async with httpx.AsyncClient(proxies=f'http://127.0.0.1:{port}', trust_env=False, timeout=4) as client:
                        warmed = False
                        for attempt in range(16):
                            kind = 'warm' if warmed else 'warmup_excluded'
                            started = time.perf_counter()
                            row = {'kind': kind}
                            try:
                                response = await asyncio.wait_for(client.post('https://clob.polymarket.com/books',
                                    json=[{'token_id': t} for t in market['token_ids']]), timeout=5)
                                row.update(status=response.status_code, bytes=len(response.content))
                                warmed = response.status_code == 200
                                if response.status_code == 200:
                                    books = response.json()
                                    row['both_tokens_returned'] = isinstance(books, list) and {b['asset_id'] for b in books} == set(market['token_ids'])
                                else:
                                    row['both_tokens_returned'] = False
                            except Exception as error:
                                row['error'] = type(error).__name__
                                warmed = False
                            row['elapsed_ms'] = round((time.perf_counter() - started) * 1000, 2)
                            result['requests'].append(row)
                finally:
                    if proc.returncode is None:
                        proc.terminate()
                    try:
                        await asyncio.wait_for(proc.wait(), 3)
                    except asyncio.TimeoutError:
                        proc.kill()
                        await proc.wait()
                    result['proxy_stopped'] = proc.returncode is not None
            print(json.dumps(result, ensure_ascii=False), flush=True)
            return result

    results = await asyncio.gather(*(probe(slot, index) for slot, index in enumerate(indexes)))
    report = {'definition': 'single Pro-Singapore route, both-token POST /books; initial warmup excluded; 15 subsequent requests on persistent client; failures recorded', 'results': results}
    with (R / 'logs/proxy-single-market-r3.json').open('x') as output:
        json.dump(report, output, ensure_ascii=False, indent=2)


if __name__ == '__main__':
    asyncio.run(main())
