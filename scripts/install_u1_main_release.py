"""Publish merged U1 Paper data service; preserve data, budgets and rollback units."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--commit', required=True)
    args = parser.parse_args()
    assert len(args.commit) == 40 and all(c in '0123456789abcdef' for c in args.commit)
    runtime = Path('/mnt/p44pro/marketcow-shadow-v3-runtime/linux')
    project = Path('/mnt/p44pro/projects/marketcow-shadow-v3')
    release = runtime / 'releases' / ('main-' + args.commit[:12])
    assert not release.exists(), 'inspect existing release; never overwrite'
    os.umask(0o077)
    release.mkdir()
    shutil.copytree(project / 'src', release / 'src', ignore=shutil.ignore_patterns('__pycache__', '*.pyc'))
    (release / 'scripts').mkdir()
    for filename in ('run_polymarket_live_read_api.py', 'run_with_bounded_log.py'):
        shutil.copy2(project / 'scripts' / filename, release / 'scripts' / filename)
    binary = runtime / 'target/release/marketcow-discovery-collector'
    with binary.open('rb') as file:
        binary_sha = hashlib.file_digest(file, 'sha256').hexdigest()
    assert binary_sha == 'c9f5f242f38fcb30a727d68c2dc075e4a2f85fabb9bf92d5782fd51c9ae21018'
    shutil.copy2(binary, release / binary.name)
    names = ('marketcow-paper-read-api.service', 'marketcow-polymarket-collector.service',
             'marketcow-polymarket-discovery.service')
    config = Path('/home/czx/.config/systemd/user')
    (release / 'units').mkdir()
    (release / 'previous-units').mkdir()
    for name in names:
        active = (config / name).resolve()
        assert active.parent.name == 'units' and active.parent.parent.parent == runtime / 'releases'
        old_release = active.parent.parent
        original = active.read_text()
        (release / 'previous-units' / name).write_text(original)
        updated = original.replace(str(old_release), str(release))
        # Diagnostic outputs remain bounded and separate from rollback logs.
        updated = updated.replace('/logs/' + old_release.name + '-', '/logs/' + release.name + '-')
        (release / 'units' / name).write_text(updated)
    subprocess.run(['systemd-analyze', '--user', 'verify', *[str(release / 'units' / n) for n in names]], check=True)
    hashes = {}
    for path in sorted(release.rglob('*')):
        if path.is_file():
            with path.open('rb') as file:
                hashes[str(path.relative_to(release))] = hashlib.file_digest(file, 'sha256').hexdigest()
    (release / 'manifest.json').write_text(json.dumps({'commit': args.commit, 'files': hashes}, indent=2))
    for name in names:
        subprocess.run(['systemctl', '--user', 'stop', name], check=True)
        state = subprocess.check_output(['systemctl', '--user', 'show', name, '-p', 'MainPID', '-p', 'Result', '-p', 'ExecMainStatus'], text=True)
        assert 'MainPID=0' in state and 'Result=success' in state and 'ExecMainStatus=0' in state, state
    for name in names:
        pending = config / (name + '.main-pending')
        assert not pending.exists() and not pending.is_symlink()
        pending.symlink_to(release / 'units' / name)
        os.replace(pending, config / name)
    subprocess.run(['systemctl', '--user', 'daemon-reload'], check=True)
    for name in reversed(names):
        subprocess.run(['systemctl', '--user', 'start', name], check=True)
    report = {'commit': args.commit, 'release': str(release), 'activated': True,
              'binary_sha256': binary_sha, 'units': list(names), 'ready_not_yet_verified': True,
              'rollback': str(release / 'previous-units')}
    with (runtime / 'logs' / (release.name + '-install.json')).open('x') as file:
        json.dump(report, file, indent=2)
    print(json.dumps(report))


if __name__ == '__main__':
    main()
