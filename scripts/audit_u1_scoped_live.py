"""Finite endpoint evidence; readiness failures remain explicit per market."""
import asyncio
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
import json
import os
import sys
from pathlib import Path

import requests
import websockets

os.umask(0o077)
BASE = os.environ.get(
    'MARKETCOW_LIVE_BASE',
    'http://127.0.0.1:8793/v1/prediction-markets/polymarket/live',
)
run = sys.argv[1]
assert run.isalnum()
REPORT = Path('/mnt/p44pro/marketcow-shadow-v3-runtime/linux/logs') / ('scoped-http-ws-audit-' + run + '.json')
assert not REPORT.exists()
scope = requests.get(BASE + '/scope', timeout=10).json()


def check(market):
    market_id = market['market_id']
    result = {'market_id': market_id}
    try:
        response = requests.get(BASE + '/full-sync', params={'market_id':market_id}, timeout=10)
        payload = response.json()
        result['http_status'] = response.status_code
        if response.status_code != 200:
            result['rejection'] = payload
        else:
            result.update({key:payload[key] for key in ['cursor','catalog_revision','maximum_book_age_ms','freshness_budget_remaining_ms']})
            result['health_status'] = payload['health']['status']
    except Exception as error:
        result['error'] = type(error).__name__ + ': ' + str(error)
    return result


async def evidence(report):
    successful = [r for r in report['markets'] if r.get('http_status') == 200]
    if not successful:
        report['consumer_ws'] = {'verified':False,'reason':'no successful full-sync baseline'}
        return
    selected = successful[-1]
    market_id = selected['market_id']
    report['endpoint_samples'] = {}
    for endpoint in ['bootstrap','snapshot','events']:
        params = {'market_id':market_id}
        if endpoint == 'events':
            params['after_cursor'] = selected['cursor']
        response = requests.get(BASE + '/' + endpoint, params=params, timeout=10)
        report['endpoint_samples'][endpoint] = {'http_status':response.status_code,'payload':response.json()}
    url = BASE.replace('http:', 'ws:') + '/stream?market_id=' + market_id + '&after_cursor=' + str(selected['cursor'])
    frames = []
    dependencies = {
        pair['market_id']
        for market in report['endpoint_samples']['bootstrap']['payload'].get('markets', [])
        if market['identity']['market_id'] == market_id
        for relation in market.get('relations', [])
        for pair in relation.get('outcome_pairs', [])
    }
    try:
        async with websockets.connect(url, max_size=64*1024*1024) as socket:
            for _ in range(8):
                frame = json.loads(await asyncio.wait_for(socket.recv(),timeout=25))
                frames.append(frame)
                if frame['type'] == 'event':
                    assert frame['cursor'] == frame['event']['cursor'], 'envelope_event_cursor_mismatch'
                    assert frame['event']['market_id'] in {market_id, None} | dependencies, 'event_outside_scope_dependency_closure'
                    if sum(f['type']=='event' for f in frames) >= 2:
                        break
                else:
                    if frame['type'] == 'error':
                        break
        report['consumer_ws'] = {'verified':sum(f['type']=='event' for f in frames)>=2,'frames':frames}
    except Exception as error:
        report['consumer_ws'] = {'verified':False,'frames':frames,'error':type(error).__name__+': '+str(error)}


with ThreadPoolExecutor(max_workers=8) as pool:
    rows = list(pool.map(check,scope['configured_markets']))
report = {'scope_id':scope['active_scope_id'],'configured_market_count':len(rows),'markets':rows,
          'http_status_counts':dict(Counter(str(row.get('http_status','exception')) for row in rows))}
try:
    asyncio.run(evidence(report))
finally:
    REPORT.write_text(json.dumps(report,indent=2))
print(json.dumps({key:value for key,value in report.items() if key not in ('markets','endpoint_samples')}))
