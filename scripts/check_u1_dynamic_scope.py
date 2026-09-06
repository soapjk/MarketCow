"""Finite managed U1 integration: genuine frozen selection, no substituted IDs.

A non-ready upstream candidate MUST be rejected without switching the gateway.
Unit tests cover successful 100->250->50 with deterministic isolated fixtures.
"""
import hashlib
import json
import os
from pathlib import Path
import secrets
import socket
import subprocess
import sys
import time
import urllib.request
import urllib.error
import argparse

parser=argparse.ArgumentParser()
parser.add_argument('--run',required=True)
parser.add_argument('--resume',action='store_true')
args=parser.parse_args()
if not args.run.isalnum(): raise ValueError('invalid run id')
run=args.run

os.umask(0o077)
r=Path('/mnt/p44pro/marketcow-shadow-v3-runtime/linux')
source=Path('/mnt/p44pro/projects/marketcow-shadow-v3')
base=r/'scoped-live-source-r1'
target=r/'dynamic-live-candidate-r1'
registry=r/'dynamic-live-registry-r1'
parent=f'marketcow-dynamic-live-u1-{run}.service'
proxy=f'marketcow-polymarket-proxy-dynamic-{run}.service'
registry.mkdir(mode=0o700,exist_ok=True)
probe=socket.socket(); probe.bind(('127.0.0.1',8793)); probe.close()
token_path=registry/'admin.token'
if not args.resume:
    with token_path.open('x') as stream: stream.write(secrets.token_hex(32))
if token_path.stat().st_mode & 0o077: raise ValueError('token must remain private')
token=token_path.read_text()
config={'storage_root':str(r),'registry_root':str(registry),'admin_token_file':str(token_path),
    'collector_binary':str(r/'target/debug/marketcow-discovery-collector'),
    'bridge_binary':str(r/'target/debug/marketcow-live-source-bridge'),'supervisor_unit':parent,
    'proxy_url':'http://127.0.0.1:17890','preheat_timeout_seconds':20,
    'activation_timeout_seconds':50,'verification_interval_seconds':2,
    'initial_root':str(base),'initial_live_stream_uri':'','host':'127.0.0.1','port':8793,
    'read_options':{'discovery_root':str(r/'discovery-source-r1'),
        'stable_snapshot_max_book_age_seconds':5,'consumer_maximum_book_age_seconds':5,
        'minimum_delivery_headroom_seconds':0,'stable_read_wait_seconds':1,
        'stable_read_poll_seconds':0.025,'executor_workers':2,
        'discovery_depth_notionals':['10','50','100','500'],
        'discovery_maximum_book_age_ms':5000,'discovery_maximum_full_sync_bytes':268435456},
    'collector_options':{'concurrency':10,'request_market_batch_size':10,'response_byte_limit':2097152,
        'batch_byte_limit':16777216,'poll_seconds':1,'request_timeout_seconds':10,'lifecycle_refresh_seconds':300,
        'persistence_queue_batches':256,'persistence_queue_bytes':67108864,
        'websocket_shard_tokens':50,'websocket_recovery_concurrency':8,
        'websocket_confirmation_seconds':2},'collector_memory_max_mib':512}
config_path=registry/f'gateway-{run}.json'
with config_path.open('x') as stream: json.dump(config,stream)
subprocess.run([sys.executable,str(source/'scripts/prepare_polymarket_dynamic_generation.py'),
    '--source',str(base),'--target',str(target),'--scope-path',str(base/'configured-scope.json'),
    '--storage',str(r),'--registry',str(registry),'--bridge-port','18896']+(['--resume'] if args.resume else []),check=True)
scope=json.loads((base/'configured-scope.json').read_bytes())
artifact=(registry/(scope['active_scope_id']+'.json')).read_bytes()
def http(path,body=None,authenticated=False):
    headers={'Content-Type':'application/json'}
    if authenticated: headers['Authorization']='Bearer '+token
    request=urllib.request.Request('http://127.0.0.1:8793'+path,
        data=None if body is None else json.dumps(body).encode(),headers=headers)
    try:
        with urllib.request.urlopen(request,timeout=90) as response:
            return response.status,json.load(response)
    except urllib.error.HTTPError as error: return error.code,json.load(error)
gateway=None
report={}
try:
    subprocess.run(['systemd-run','--user','--quiet','--collect','--unit='+proxy,
        '--property=BindsTo='+parent,'--property=After='+parent,'--property=UMask=0077',
        '--property=MemoryMax=256M','--property=RuntimeMaxSec=120',
        '--property=StandardOutput=append:'+str(r/f'logs/dynamic-proxy-{run}.log'),
        '--property=StandardError=append:'+str(r/f'logs/dynamic-proxy-{run}.log'),
        str(r/'polymarket-proxy/mihomo'),'-d',str(r/'polymarket-proxy'),'-f',str(r/'polymarket-proxy/config.yaml')],check=True)
    with (r/f'logs/dynamic-gateway-{run}.log').open('x') as log:
        gateway=subprocess.Popen([sys.executable,str(source/'scripts/run_polymarket_dynamic_live_api.py'),
            '--config',str(config_path)],stdout=log,stderr=log)
        deadline=time.monotonic()+20
        while True:
            try:
                before=http('/v1/prediction-markets/polymarket/live/scope')
                break
            except (OSError,ValueError):
                if gateway.poll() is not None or time.monotonic()>deadline: raise
                time.sleep(0.2)
        request={'schema_version':'marketcow.polymarket.scope-activation.v1','scope_id':scope['active_scope_id'],
            'scope_file_sha256':hashlib.sha256(artifact).hexdigest()}
        unauth=http('/v1/admin/polymarket/scope:activate',request)
        result=http('/v1/admin/polymarket/scope:activate',request,True)
        after=http('/v1/prediction-markets/polymarket/live/scope')
        report={'unauthorized_http':unauth[0],'activation_http':result[0],'activation_response':result[1],
            'before_scope':before,'after_scope':after,'gateway_pid':gateway.pid,
            'gateway_still_running':gateway.poll() is None,
            'scope_unchanged':before==after,'production_touched':False,'real_orders_enabled':False}
        assert unauth[0]==401
        if result[0]==409:
            assert before==after and not (registry/'active-live-generation.json').exists()
        else:
            assert result[0]==200 and result[1]['status']=='activated_ready'
        assert gateway.poll() is None
finally:
    if gateway is not None and gateway.poll() is None:
        gateway.terminate()
        try: gateway.wait(timeout=35)
        except subprocess.TimeoutExpired: gateway.kill(); gateway.wait()
    subprocess.run(['systemctl','--user','stop',proxy],check=False)
    with (r/f'logs/dynamic-activation-http-{run}.json').open('x') as stream: json.dump(report,stream,indent=2)
print(json.dumps({k:v for k,v in report.items() if k not in ('before_scope','after_scope')}))
