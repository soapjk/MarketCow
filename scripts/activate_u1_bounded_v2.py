"""Activate only the frozen scoped collector and read API; preserve other units."""
import hashlib
import json
import os
from pathlib import Path
import subprocess


def main():
    runtime = Path('/mnt/p44pro/marketcow-shadow-v3-runtime/linux')
    release = runtime / 'releases/bounded-v2'
    config = Path('/home/czx/.config/systemd/user')
    names = ('marketcow-paper-read-api.service', 'marketcow-polymarket-collector.service')
    untouched = ('marketcow-polymarket-proxy.service', 'marketcow-polymarket-discovery.service')
    def pid(name):
        return subprocess.check_output(['systemctl', '--user', 'show', name, '-p', 'MainPID', '--value'], text=True).strip()
    before = {name: pid(name) for name in untouched}
    assert all(value != '0' for value in before.values())
    for relative, expected in json.loads((release / 'manifest.json').read_text()).items():
        with (release / relative).open('rb') as file:
            assert hashlib.file_digest(file, 'sha256').hexdigest() == expected, relative
    for name in names:
        assert (config / name).resolve() == runtime / 'releases/bounded-v1/units' / name
    for name in names:
        subprocess.run(['systemctl', '--user', 'stop', name], check=True)
        assert pid(name) == '0'
        status = subprocess.check_output(['systemctl', '--user', 'show', name, '-p', 'Result', '-p', 'ExecMainStatus'], text=True)
        assert 'Result=success' in status and 'ExecMainStatus=0' in status, status
    for name in names:
        pending = config / (name + '.bounded-v2-pending')
        assert not pending.exists() and not pending.is_symlink()
        pending.symlink_to(release / 'units' / name)
        os.replace(pending, config / name)
    subprocess.run(['systemctl', '--user', 'daemon-reload'], check=True)
    for name in reversed(names):
        subprocess.run(['systemctl', '--user', 'start', name], check=True)
    assert before == {name: pid(name) for name in untouched}
    report = {'activated': True, 'ready_not_yet_verified': True,
              'new_pids': {name: pid(name) for name in names}, 'unchanged_pids': before}
    with (runtime / 'logs/bounded-v2-activation-report.json').open('x') as file:
        json.dump(report, file, indent=2)
    print(json.dumps(report))


if __name__ == '__main__':
    main()
