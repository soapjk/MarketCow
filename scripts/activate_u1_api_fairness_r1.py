"""Activate the hash-verified API candidate, preserving source processes."""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import argparse


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--downstream-diagnostics', action='store_true')
    parser.add_argument('--membership-cache', action='store_true')
    args = parser.parse_args()
    runtime = Path('/mnt/p44pro/marketcow-shadow-v3-runtime/linux')
    release_name = 'bounded-v2-downstream-diag-r1' if args.downstream_diagnostics else 'bounded-v2-api-fairness-r1'
    previous_name = 'bounded-v2-api-fairness-r1' if args.downstream_diagnostics else 'bounded-v2-decode-r1'
    if args.membership_cache:
        release_name, previous_name = 'bounded-v2-membership-r1', 'bounded-v2-downstream-diag-r1'
    release = runtime / 'releases' / release_name
    name = 'marketcow-paper-read-api.service'
    active = Path('/home/czx/.config/systemd/user') / name
    assert active.resolve() == runtime / 'releases' / previous_name / 'units' / name
    manifest = (release / 'manifest.json').read_bytes()
    expected = ('02d580e3ddaa4ddb22a605d612602454b9be606f2e553c40fc7b2fd2c0abc6c7'
                if args.downstream_diagnostics else
                '6c1b75412728dc56226b0ae5110415cff0823042727875c5be1d29039707cefc')
    if args.membership_cache:
        expected = '3c2df18febbbd68c1d401825373a43c3118b181e446791f85bce77a0d97871a4'
    assert hashlib.sha256(manifest).hexdigest() == expected
    for relative, digest in json.loads(manifest).items():
        with (release / relative).open('rb') as file:
            assert hashlib.file_digest(file, 'sha256').hexdigest() == digest, relative
    def pid(unit):
        return subprocess.check_output(['systemctl', '--user', 'show', unit, '-p', 'MainPID', '--value'], text=True).strip()
    untouched = {unit: pid(unit) for unit in ('marketcow-polymarket-collector',
        'marketcow-polymarket-discovery', 'marketcow-polymarket-proxy')}
    assert all(value != '0' for value in untouched.values())
    subprocess.run(['systemctl', '--user', 'stop', name], check=True)
    assert pid(name) == '0'
    status = subprocess.check_output(['systemctl', '--user', 'show', name, '-p', 'Result', '-p', 'ExecMainStatus'], text=True)
    assert 'Result=success' in status and 'ExecMainStatus=0' in status, status
    pending = active.with_name(name + '.fairness-pending')
    assert not pending.exists() and not pending.is_symlink()
    pending.symlink_to(release / 'units' / name)
    os.replace(pending, active)
    subprocess.run(['systemctl', '--user', 'daemon-reload'], check=True)
    subprocess.run(['systemctl', '--user', 'start', name], check=True)
    assert untouched == {unit: pid(unit) for unit in untouched}
    report = {'activated': True, 'release': str(release), 'manifest_sha256': expected,
              'api_pid': pid(name), 'untouched_pids': untouched, 'health_not_yet_verified': True}
    with (runtime / 'logs' / (release_name + '-activation.json')).open('x') as file:
        json.dump(report, file, indent=2)
    print(json.dumps(report))


if __name__ == '__main__':
    main()
