"""Twenty-cycle bounded batch experiment with two independently recorded API audits."""
import json
import os
from pathlib import Path
import subprocess
import time
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--run', required=True)
parser.add_argument('--audit-runs', nargs=2, required=True)
parser.add_argument('--dependencies', action='store_true')
args = parser.parse_args()
assert args.run.isalnum() and all(run.isalnum() for run in args.audit_runs)

os.umask(0o077)
r = Path('/mnt/p44pro/marketcow-shadow-v3-runtime/linux')
root = r / 'scoped-live-source-r1'
log_path = r / 'logs' / ('scoped-batch-probe-' + args.run + '.log')
assert not log_path.exists()
env = os.environ.copy()
for key in ['ALL_PROXY', 'all_proxy']:
    env.pop(key, None)
for key in ['HTTPS_PROXY','https_proxy','HTTP_PROXY','http_proxy']:
    env[key] = 'http://127.0.0.1:17890'
env['NO_PROXY'] = env['no_proxy'] = '127.0.0.1,localhost,::1'
command = ['/usr/bin/time','-v',str(r/'target/debug/marketcow-discovery-collector'),
    '--root',str(root),'--plan',str(root/'rust-scoped-plan-r1.json'),
    '--plan-sha256','603396c05129df8c598ee941fccd9b6894081719af67057c762f8cade1035389',
    '--configured-scope',str(root/'configured-scope.json'),
    '--configured-scope-sha256','1f95a4d8cbe99b40a1a24571f47994ce020640a9e12c03f13ff13f796eb1e7f6',
    '--input-mode','rest-poll','--expected-market-count','100','--concurrency','10','--request-market-batch-size','10',
    '--response-byte-limit','2097152','--persistence-queue-batches','256','--persistence-queue-bytes','67108864','--batch-byte-limit','16777216',
    '--poll-seconds','1','--request-timeout-seconds','10','--cycles','20']
if args.dependencies:
    command.extend(['--dependency-plan',str(root/'live-bridge-plan-r1.json'),
                    '--dependency-plan-sha256','0958672fbb466a17803779479567fa1efceb889d38749496421630bf060cfb85'])
def cycle():
    count = 0
    for line in log_path.read_text().splitlines():
        if line.startswith('{'):
            item = json.loads(line)
            if 'cycle' in item:
                count = item['cycle']
    return count

with log_path.open('x') as output:
    process = subprocess.Popen(command,env=env,stdout=output,stderr=output)
    try:
        threshold = 1
        for run in args.audit_runs:
            deadline = time.monotonic() + 60
            while cycle() < threshold and process.poll() is None and time.monotonic() < deadline:
                time.sleep(0.25)
            if cycle() < threshold:
                raise RuntimeError('collector did not reach audit boundary')
            subprocess.run(['python3','scripts/audit_u1_scoped_live.py',run],check=True)
            threshold = cycle() + 1
    finally:
        result = process.wait()
        (r/'logs'/('scoped-batch-probe-' + args.run + '.exit-code')).write_text(str(result))
raise SystemExit(result)
