"""Bounded authenticated GET-only installed control check; secrets never printed."""
import hashlib
import json
from pathlib import Path

import httpx

ROOT = Path('/mnt/p44pro/marketcow-shadow-v3-runtime/linux')
PREFIX = '/v1/prediction-markets/polymarket/hot-scopes/status'


def main():
    token = (ROOT/'phase1/tradude.secret').read_text()
    report = {}
    with httpx.Client(base_url='http://127.0.0.1:18898', trust_env=False, timeout=25) as client:
        denied = client.get(PREFIX, params={'pool': 'live'})
        assert denied.status_code == 401
        report['unauthenticated_status'] = denied.status_code
        for pool in ('live', 'discovery'):
            with client.stream('GET', PREFIX, params={'pool': pool},
                    headers={'Authorization': 'Bearer '+token}) as response:
                response.raise_for_status()
                raw = bytearray()
                for chunk in response.iter_bytes():
                    assert len(raw)+len(chunk) <= 2097152
                    raw.extend(chunk)
            body = json.loads(raw)
            assert body['schema_version'] == 'marketcow.hot-scope-status.v1'
            assert body['pool'] == pool and body['actual']['source_readable']
            report[pool] = dict(body=body, bytes=len(raw), sha256=hashlib.sha256(raw).hexdigest())
    out = ROOT/'releases/hot-scope-r36-v1/installation/control-status.json'
    with out.open('x') as stream:
        json.dump(report, stream, sort_keys=True)
    for pool in ('live', 'discovery'):
        actual = report[pool]['body']['actual']
        print(json.dumps({k: v for k, v in actual.items() if k not in ('admitted_market_ids', 'referenced_market_ids')}))
    print(json.dumps(dict(report=str(out), sha256=hashlib.sha256(out.read_bytes()).hexdigest(), unauthenticated_status=401)))


if __name__ == '__main__':
    main()
