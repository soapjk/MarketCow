"""Create exact preheat/public unit artifacts; never replace the formal unit."""
import hashlib
import json
import os
from pathlib import Path
import shlex
import socket

R = Path('/mnt/p44pro/marketcow-shadow-v3-runtime/linux')
RELEASE = R/'releases/universe-runtime-r4'
ROOT = R/'universe-live-candidate-r1'
UNITS = Path('/home/czx/.config/systemd/user')


def sha(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def main():
    os.umask(0o077)
    with socket.socket() as check:
        check.bind(('127.0.0.1', 18899))
    binary = RELEASE/'marketcow-discovery-collector'
    assert sha(binary) == 'a3ef2127a46c61b322c25875d77036ed27f854ab1422b3d9134b36b45aa6b45b'
    incumbent = (UNITS/'marketcow-polymarket-collector.service').resolve(strict=True)
    text = incumbent.read_text()
    starts = [line for line in text.splitlines() if line.startswith('ExecStart=')]
    assert len(starts) == 1
    args = shlex.split(starts[0].removeprefix('ExecStart='))
    binaries = [i for i, value in enumerate(args) if value.endswith('/marketcow-discovery-collector')]
    assert len(binaries) == 1
    args[binaries[0]] = str(binary)
    report = json.loads((ROOT/'candidate-preparation.json').read_bytes())
    updates = {'--root': str(ROOT), '--plan': str(ROOT/'rust-scoped-plan-r1.json'),
        '--plan-sha256': report['plan_sha256'], '--configured-scope': str(ROOT/'configured-scope.json'),
        '--configured-scope-sha256': report['configured_scope_sha256'],
        '--dependency-plan': str(ROOT/'live-bridge-plan-r1.json'), '--dependency-plan-sha256': report['dependency_plan_sha256']}
    for flag, value in updates.items():
        assert args.count(flag) == 1
        args[args.index(flag)+1] = value
    unit_dir = RELEASE/'units'
    unit_dir.mkdir(mode=0o700)
    artifacts = {}
    for kind, name, endpoint in [('preheat','marketcow-universe-live-preheat-r3.service','127.0.0.1:18899'),
                                 ('public','marketcow-polymarket-collector.service','192.168.124.3:8793')]:
        argv = list(args)
        argv[argv.index('--public-listen')+1] = endpoint
        argv[argv.index('--log')+1] = str(R/'logs'/f'universe-live-{kind}-r3.log')
        # Operator paths/values are safe tokens here; do not accept client shell syntax.
        assert all(not any(c in value for c in '\n\r\t "\'\\$%') for value in argv)
        body = text.replace(starts[0], 'ExecStart='+' '.join(argv))
        if kind == 'preheat':
            body = body.replace('Restart=on-failure', 'Restart=no\nRuntimeMaxSec=240')
        path = unit_dir/name
        with path.open('x') as stream:
            stream.write(body); stream.flush(); os.fsync(stream.fileno())
        path.chmod(0o400)
        artifacts[kind] = {'name': name, 'path': str(path), 'sha256': sha(path)}
    preheat = artifacts['preheat']
    # Exclusive creation: no existing unit is replaced by preparation.
    (UNITS/preheat['name']).symlink_to(preheat['path'])
    result = {'incumbent': {'name': 'marketcow-polymarket-collector.service', 'path': str(incumbent),
                          'sha256': sha(incumbent)}, 'artifacts': artifacts, 'formal_changed': False}
    with (RELEASE/'unit-artifacts.json').open('x') as stream:
        json.dump(result, stream, sort_keys=True); stream.flush(); os.fsync(stream.fileno())
    print(json.dumps(result, sort_keys=True))


if __name__ == '__main__':
    main()
