"""Freeze and activate API-only instrumentation; leave all source units running."""
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess


def sha(path):
    with path.open('rb') as file:
        return hashlib.file_digest(file, 'sha256').hexdigest()


def main():
    runtime = Path('/mnt/p44pro/marketcow-shadow-v3-runtime/linux')
    old = runtime / 'releases/gap-index-aa14360-r1'
    release = runtime / 'releases/slow-timeline-4a20c34'
    project = Path('/mnt/p44pro/projects/marketcow-shadow-v3')
    name = 'marketcow-paper-read-api.service'
    active = Path('/home/czx/.config/systemd/user') / name
    assert active.resolve() == old / 'units' / name
    assert not release.exists(), 'inspect existing candidate, never overwrite'
    def pid(unit):
        return subprocess.check_output(['systemctl', '--user', 'show', unit, '-p', 'MainPID', '--value'], text=True).strip()
    untouched = {unit: pid(unit) for unit in ('marketcow-polymarket-collector',
        'marketcow-polymarket-discovery', 'marketcow-polymarket-proxy')}
    assert all(value != '0' for value in untouched.values())
    os.umask(0o077)
    release.mkdir()
    shutil.copytree(old / 'src', release / 'src', ignore=shutil.ignore_patterns('__pycache__', '*.pyc'))
    shutil.copytree(old / 'scripts', release / 'scripts')
    for filename in ('polymarket_live_stream.py', 'polymarket_stream_metrics.py'):
        shutil.copy2(project / 'src/marketcow' / filename, release / 'src/marketcow' / filename)
    (release / 'units').mkdir()
    original = active.read_text()
    (release / 'previous-api.service').write_text(original)
    updated = original.replace(str(old), str(release)).replace('/logs/gap-index-aa14360-r1-', '/logs/slow-timeline-4a20c34-')
    (release / 'units' / name).write_text(updated)
    manifest = {str(p.relative_to(release)): sha(p) for p in sorted(release.rglob('*')) if p.is_file()}
    (release / 'manifest.json').write_text(json.dumps(manifest, indent=2))
    subprocess.run(['systemd-analyze', '--user', 'verify', str(release / 'units' / name)], check=True)
    subprocess.run(['systemctl', '--user', 'stop', name], check=True)
    assert pid(name) == '0'
    status = subprocess.check_output(['systemctl', '--user', 'show', name, '-p', 'Result', '-p', 'ExecMainStatus'], text=True)
    assert 'Result=success' in status and 'ExecMainStatus=0' in status, status
    pending = active.with_name(name + '.decode-pending')
    assert not pending.exists() and not pending.is_symlink()
    pending.symlink_to(release / 'units' / name)
    os.replace(pending, active)
    subprocess.run(['systemctl', '--user', 'daemon-reload'], check=True)
    subprocess.run(['systemctl', '--user', 'start', name], check=True)
    assert untouched == {unit: pid(unit) for unit in untouched}
    report = {'activated': True, 'release': str(release), 'manifest_sha256': sha(release / 'manifest.json'),
              'api_pid': pid(name), 'untouched_pids': untouched, 'health_not_yet_verified': True}
    with (runtime / 'logs/slow-timeline-4a20c34-activation.json').open('x') as file:
        json.dump(report, file, indent=2)
    print(json.dumps(report))


if __name__ == '__main__':
    main()
