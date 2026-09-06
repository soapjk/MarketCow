"""User-authorized U1-only upgrade; keep old configs/data, never enable boot startup."""
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess

runtime = Path('/mnt/p44pro/marketcow-shadow-v3-runtime/linux')
project = Path('/mnt/p44pro/projects/marketcow-shadow-v3')
release = runtime / 'releases/bounded-v1'
config = Path('/home/czx/.config/systemd/user')
names = ['marketcow-polymarket-proxy.service', 'marketcow-polymarket-collector.service',
         'marketcow-polymarket-discovery.service', 'marketcow-paper-read-api.service']
assert (runtime/'logs/bounded-release-gate-r2.exit-code').read_text().strip() == '0'
assert json.loads((runtime/'logs/bounded-http-ws-r2-report.json').read_text())['passed'] is True
assert not release.exists(), 'release already exists; inspect rather than overwrite'
for name in names:
    assert subprocess.check_output(['systemctl','--user','show',name,'-p','MainPID','--value'],text=True).strip() == '0'
os.umask(0o077)
release.mkdir(parents=True)
(release/'previous-units').mkdir()
(release/'units').mkdir()
shutil.copy2(runtime/'target/release/marketcow-discovery-collector',release/'marketcow-discovery-collector')
shutil.copytree(project/'src',release/'src',ignore=shutil.ignore_patterns('__pycache__','*.pyc'))
(release/'scripts').mkdir()
for name in ('run_with_bounded_log.py','run_polymarket_live_read_api.py'):
    shutil.copy2(project/'scripts'/name,release/'scripts'/name)
changed = {}
for name in names:
    original = (config/name).read_text()
    (release/'previous-units'/name).write_text(original)
    updated = original.replace(str(runtime/'dynamic-live-candidate-r1'),str(runtime/'bounded-scoped-candidate-r1'))
    updated = updated.replace(str(runtime/'discovery-source-r1'),str(runtime/'bounded-discovery-candidate-r1'))
    updated = updated.replace(str(runtime/'target/release/marketcow-discovery-collector'),str(release/'marketcow-discovery-collector'))
    updated = updated.replace(str(project/'src'),str(release/'src')).replace(str(project/'scripts'),str(release/'scripts'))
    if name == 'marketcow-polymarket-collector.service':
        for old,new in [
            ('603396c05129df8c598ee941fccd9b6894081719af67057c762f8cade1035389','3461f8b75ad6078e5e1199c4ea1d67c62e29872b17856603bc64764d297bbdff'),
            ('1f95a4d8cbe99b40a1a24571f47994ce020640a9e12c03f13ff13f796eb1e7f6','4443ccdc6896ed451d9be94e467d8deca2594f69d3f4d6d5f19e63a437acf7df'),
            ('a6802894c3d12c4d42e881e0af1d7442d7af4df59c7219e348429422b6169dfd','b108d8795ba17892f6f2b361db341048cae3c2687aa1277dad0be9089d9019ce'),
            ('--expected-market-count 100 ','--expected-market-count 250 '),
            ('--websocket-shard-tokens 50 ','--websocket-shard-tokens 500 '),
            ('--live-frame-bytes 33554432','--live-frame-bytes 67108864'),
            ('MemoryMax=512M','MemoryMax=3072M')]:
            assert old in updated, ('unexpected previous config',name,old)
            updated = updated.replace(old,new)
    if name == 'marketcow-paper-read-api.service':
        updated = updated.replace('Requires=marketcow-polymarket-collector.service','Wants=marketcow-polymarket-collector.service')
        updated = updated.replace('MemoryMax=384M','MemoryMax=768M').replace('--executor-workers 2','--executor-workers 4')
        updated = updated.replace('--live-stream-replay-capacity 2048','--live-stream-replay-capacity 4096')
    if name == 'marketcow-polymarket-discovery.service':
        updated = updated.replace('MemoryMax=256M','MemoryMax=512M')
    output = []
    for line in updated.splitlines():
        if line.startswith('ExecStart='):
            command = line.removeprefix('ExecStart=')
            if name in ('marketcow-polymarket-collector.service','marketcow-polymarket-discovery.service'):
                command += ' --bounded-history-bytes 67108864'
            log = runtime/'logs'/('bounded-v1-'+name.removesuffix('.service')+'.log')
            line = f'ExecStart=/usr/bin/python3 {release}/scripts/run_with_bounded_log.py --log {log} --max-bytes 8388608 -- {command}'
        elif line.startswith(('StandardOutput=','StandardError=')):
            line = line.split('=')[0]+'=null'
        elif line.startswith('RestartSec='):
            line = 'RestartSec=5'
        elif line.startswith('TimeoutStopSec='):
            line = 'TimeoutStopSec=80'
        output.append(line)
        if line == '[Service]':
            output.extend(['KillMode=mixed','MemorySwapMax=0'])
    text = '\n'.join(output)+'\n'
    (release/'units'/name).write_text(text)
    changed[name] = {'before_sha256':hashlib.sha256(original.encode()).hexdigest(),
                     'after_sha256':hashlib.sha256(text.encode()).hexdigest()}
subprocess.run(['systemd-analyze','--user','verify',*[str(release/'units'/n) for n in names]],check=True)
# Preserve enable/disable state: update exact unit files, do not call enable.
for name in names:
    pending = config/(name+'.bounded-pending')
    assert not pending.exists()
    pending.symlink_to(release/'units'/name)
    os.replace(pending,config/name)
subprocess.run(['systemctl','--user','daemon-reload'],check=True)
report = {'installed':True,'started':False,'boot_enable_changed':False,'units':changed,
          'binary_sha256':hashlib.sha256((release/'marketcow-discovery-collector').read_bytes()).hexdigest(),
          'rollback_units':str(release/'previous-units'),'original_data_unchanged':True}
(runtime/'logs/bounded-v1-install-report.json').write_text(json.dumps(report,indent=2))
print(json.dumps(report))
