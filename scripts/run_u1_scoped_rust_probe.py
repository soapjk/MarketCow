"""Finite authoritative read-only probe of the existing frozen 100-market scope."""
import hashlib
import json
import os
from pathlib import Path
import subprocess

os.umask(0o077)
runtime = Path('/mnt/p44pro/marketcow-shadow-v3-runtime/linux')
root = runtime / 'scoped-live-source-r1'
scope_path = root / 'configured-scope.json'
scope_bytes = scope_path.read_bytes()
scope_hash = hashlib.sha256(scope_bytes).hexdigest()
assert scope_hash == '1f95a4d8cbe99b40a1a24571f47994ce020640a9e12c03f13ff13f796eb1e7f6'
scope = json.loads(scope_bytes)
report = json.loads((root / 'relocation-report.json').read_bytes())
assert report['complete'] is True and report['configured_scope_sha256'] == scope_hash
plan = {'schema_version': 'marketcow.polymarket.rust-scoped-source-plan.v1',
        'catalog_revision': scope['catalog_revision'],
        'markets': [{k: m[k] for k in ['market_id', 'condition_id', 'token_ids']}
                    for m in scope['configured_markets']]}
assert len(plan['markets']) == 100
plan_bytes = json.dumps(plan, sort_keys=True, separators=(',', ':')).encode()
plan_path = root / 'rust-scoped-plan-r1.json'
with plan_path.open('xb') as stream:
    stream.write(plan_bytes)
    stream.flush()
    os.fsync(stream.fileno())
environment = os.environ.copy()
for key in ['ALL_PROXY', 'all_proxy']:
    environment.pop(key, None)
for key in ['http_proxy', 'HTTP_PROXY', 'https_proxy', 'HTTPS_PROXY']:
    environment[key] = 'http://127.0.0.1:17890'
environment['NO_PROXY'] = environment['no_proxy'] = '127.0.0.1,localhost,::1'
command = ['/usr/bin/time', '-v', str(runtime / 'target/debug/marketcow-discovery-collector'),
           '--root', str(root), '--plan', str(plan_path),
           '--plan-sha256', hashlib.sha256(plan_bytes).hexdigest(),
           '--configured-scope', str(scope_path), '--configured-scope-sha256', scope_hash,
           '--input-mode','rest-poll','--expected-market-count', '100', '--concurrency', '16',
           '--response-byte-limit', '2097152', '--persistence-queue-batches','256','--persistence-queue-bytes','67108864','--batch-byte-limit', '16777216',
           '--poll-seconds', '30', '--request-timeout-seconds', '10', '--request-market-batch-size', '1', '--cycles', '2']
with (runtime / 'logs/rust-scoped-probe-r1.log').open('x') as log:
    result = subprocess.run(command, env=environment, stdout=log, stderr=log)
(runtime / 'logs/rust-scoped-probe-r1.exit-code').write_text(str(result.returncode))
raise SystemExit(result.returncode)
