"""Prepare immutable U1 release and rollback units. Never activate services."""
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess

R = Path('/mnt/p44pro/marketcow-shadow-v3-runtime/linux')
RELEASE = R / 'releases/direct-rust-0424396'
CONFIG = Path('/home/czx/.config/systemd/user')
LIVE = 'marketcow-polymarket-collector.service'
DISCOVERY = 'marketcow-polymarket-discovery.service'
API = 'marketcow-paper-read-api.service'
SHA = '2ef588a11ff540d3cbc014323770246351ff65ffcef50141cccc30cb31ceada2'


def sha(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def replace_one(text, old, new):
    if text.count(old) != 1:
        raise ValueError(f'expected exactly one configuration occurrence: {old}')
    return text.replace(old, new, 1)


def main():
    os.umask(0o077)
    assert not RELEASE.exists()
    assert (R/'logs/public-validation-r5.exit-code').read_text().strip() == '0'
    for prefix in ('public-rust-smoke-r3', 'public-discovery-smoke-r3'):
        report = json.loads((R/f'logs/{prefix}.report.json').read_text())
        assert report['passed'] and report['binary_sha256'] == SHA
        assert report['unit']['MainPID'] == '0'
    assert sha(R/'target/release/marketcow-discovery-collector') == SHA
    seed = R/'bounded-discovery-candidate-r1/discovery-public-seed.json'
    seed_body = json.loads(seed.read_bytes())
    assert seed_body['catalog_manifest_sha256'] == sha(seed.parent/'catalog.json')
    assert len(seed_body['markets']) == 1000
    before = {}
    for name in (LIVE, DISCOVERY, API):
        path = (CONFIG/name).resolve(strict=True)
        assert path.is_relative_to(R/'releases')
        before[name] = {'path': str(path), 'sha256': sha(path)}
    RELEASE.mkdir()
    (RELEASE/'units').mkdir()
    (RELEASE/'previous-units').mkdir()
    binary = RELEASE/'marketcow-discovery-collector'
    shutil.copyfile(R/'target/release/marketcow-discovery-collector', binary)
    binary.chmod(0o500)
    assert sha(binary) == SHA
    for name, binding in before.items():
        shutil.copyfile(binding['path'], RELEASE/'previous-units'/name)
        if name == API:
            continue
        text = Path(binding['path']).read_text()
        old_binary = str(R/('releases/queue32-8042e80/marketcow-discovery-collector' if name == LIVE
            else 'releases/main-cd7cf396624b/marketcow-discovery-collector'))
        text = replace_one(text, old_binary, str(binary))
        old_log = 'queue32-8042e80' if name == LIVE else 'main-cd7cf396624b'
        text = replace_one(text, str(R/f'logs/{old_log}-{name.removesuffix(".service")}.log'),
            str(R/f'logs/direct-rust-0424396-{name.removesuffix(".service")}.log'))
        if name == LIVE:
            text = replace_one(text, '--live-listen 127.0.0.1:18896 --live-frame-bytes 67108864 --live-maximum-clients 2',
                '--public-listen 192.168.124.3:8793 --public-full-sync-bytes 134217728 '
                '--public-snapshot-concurrency 1 --public-frame-bytes 67108864 '
                '--public-replay-bytes 8388608 --public-maximum-clients 2 --public-send-timeout-seconds 5')
        else:
            text = replace_one(text, 'MemoryMax=512M', 'MemoryMax=4G')
            text = replace_one(text, '--persistence-queue-bytes 67108864', '--persistence-queue-bytes 134217728')
            text = replace_one(text, '--bounded-history-bytes 67108864',
                '--bounded-history-bytes 67108864 --discovery-listen 192.168.124.3:8795 '
                f'--discovery-seed {seed} --discovery-seed-sha256 {sha(seed)} '
                '--discovery-state-bytes 134217728 --discovery-full-sync-bytes 67108864 '
                '--discovery-frame-bytes 16777216 --discovery-replay-bytes 8388608 '
                '--discovery-clients 2 --discovery-baselines 2 --discovery-send-timeout-seconds 5')
        output = RELEASE/'units'/name
        output.write_text(text)
        before[name]['new_sha256'] = sha(output)
    subprocess.run(['systemd-analyze', '--user', 'verify',
        str(RELEASE/'units'/LIVE), str(RELEASE/'units'/DISCOVERY)], check=True)
    manifest = {'prepared': True, 'activated': False, 'source_commit': '0424396',
        'binary_sha256': SHA, 'units': before, 'retire_python_api': API,
        'seed_sha256': sha(seed), 'live_base': 'http://192.168.124.3:8793',
        'discovery_base': 'http://192.168.124.3:8795', 'roots_unchanged': True}
    (RELEASE/'manifest.json').write_text(json.dumps(manifest, indent=2))
    for path in RELEASE.rglob('*'):
        if path.is_file() and path != binary:
            path.chmod(0o400)
    print(json.dumps({'prepared': True, 'manifest': str(RELEASE/'manifest.json'),
        'manifest_sha256': sha(RELEASE/'manifest.json'), 'services_changed': False}))


if __name__ == '__main__':
    main()
