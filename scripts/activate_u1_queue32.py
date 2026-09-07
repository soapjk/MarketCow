"""User-authorized collector-only release; preserve old unit and all data."""
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess

R = Path('/mnt/p44pro/marketcow-shadow-v3-runtime/linux')
RELEASE = R / 'releases/queue32-8042e80'
UNIT = 'marketcow-polymarket-collector.service'
LINK = Path('/home/czx/.config/systemd/user') / UNIT
OLD = R / 'releases/queue6-7b19dd2/units' / UNIT
SHA = '20fbc7fab2372a4eae8adaea88a9b20407c67fd788474da6e13e594c92e1d175'


def sha(p):
    with p.open('rb') as f:
        return hashlib.file_digest(f, 'sha256').hexdigest()


def status(unit):
    return dict(line.split('=', 1) for line in subprocess.check_output([
        'systemctl', '--user', 'show', unit, '-p', 'MainPID', '-p', 'Result',
        '-p', 'ExecMainStatus', '-p', 'ActiveState'], text=True).splitlines())


def switch(target):
    pending = LINK.with_name(UNIT + '.queue32-pending')
    assert not pending.exists() and not pending.is_symlink()
    pending.symlink_to(target)
    os.replace(pending, LINK)
    subprocess.run(['systemctl', '--user', 'daemon-reload'], check=True)


if __name__ == '__main__':
    os.umask(0o077)
    assert LINK.resolve() == OLD
    assert (R / 'logs/queue32-build-r1.exit-code').read_text().strip() == '0'
    assert sha(R / 'target/release/marketcow-discovery-collector') == SHA
    others = ('marketcow-paper-read-api.service', 'marketcow-polymarket-discovery.service',
              'marketcow-polymarket-proxy.service')
    before = {name: status(name)['MainPID'] for name in others}
    assert all(pid != '0' for pid in before.values())
    RELEASE.mkdir()
    (RELEASE / 'units').mkdir()
    binary = RELEASE / 'marketcow-discovery-collector'
    shutil.copyfile(R / 'target/release/marketcow-discovery-collector', binary)
    binary.chmod(0o500)
    assert sha(binary) == SHA
    text = OLD.read_text()
    old_binary = str(R / 'releases/queue6-7b19dd2/marketcow-discovery-collector')
    assert text.count(old_binary) == 1 and text.count('MemoryMax=4G') == 1
    assert '--market-workers 6' in text
    text = text.replace(old_binary, str(binary)).replace('MemoryMax=3072M', 'MemoryMax=4G')
    text = text.replace('logs/queue6-7b19dd2-marketcow-polymarket-collector.log',
                        'logs/queue32-8042e80-marketcow-polymarket-collector.log')
    (RELEASE / 'units' / UNIT).write_text(text)
    manifest = {'commit': '8042e80', 'binary_sha256': SHA, 'queue_capacity': 32,
                'workers': 6, 'rollback_unit': str(OLD), 'unit_sha256': sha(RELEASE / 'units' / UNIT)}
    (RELEASE / 'manifest.json').write_text(json.dumps(manifest, indent=2))
    subprocess.run(['systemctl', '--user', 'stop', UNIT], check=True)
    stopped = status(UNIT)
    assert stopped['MainPID'] == '0' and stopped['Result'] == 'success' and stopped['ExecMainStatus'] == '0', stopped
    try:
        switch(RELEASE / 'units' / UNIT)
        subprocess.run(['systemctl', '--user', 'start', UNIT], check=True)
        assert status(UNIT)['MainPID'] != '0'
    except BaseException:
        subprocess.run(['systemctl', '--user', 'stop', UNIT], check=True)
        switch(OLD)
        subprocess.run(['systemctl', '--user', 'start', UNIT], check=True)
        raise
    after = {name: status(name)['MainPID'] for name in others}
    assert before == after, (before, after)
    report = {'collector': status(UNIT), 'unchanged_pids': after,
              'manifest_sha256': sha(RELEASE / 'manifest.json'), 'ready_verified': False}
    (R / 'logs/queue32-8042e80-activation.json').write_text(json.dumps(report, indent=2))
    print(json.dumps(report))
