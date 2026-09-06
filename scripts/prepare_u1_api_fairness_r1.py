"""Freeze an API-only candidate; do not change units or restart services."""
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import argparse


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--downstream-diagnostics', action='store_true')
    parser.add_argument('--membership-cache', action='store_true')
    args = parser.parse_args()
    runtime = Path('/mnt/p44pro/marketcow-shadow-v3-runtime/linux')
    old_name = 'bounded-v2-api-fairness-r1' if args.downstream_diagnostics else 'bounded-v2-decode-r1'
    new_name = 'bounded-v2-downstream-diag-r1' if args.downstream_diagnostics else 'bounded-v2-api-fairness-r1'
    if args.membership_cache:
        old_name, new_name = 'bounded-v2-downstream-diag-r1', 'bounded-v2-membership-r1'
    old = runtime / 'releases' / old_name
    new = runtime / 'releases' / new_name
    project = Path('/mnt/p44pro/projects/marketcow-shadow-v3')
    assert not new.exists()
    new.mkdir(mode=0o700)
    for directory in ('src', 'scripts'):
        shutil.copytree(old / directory, new / directory, ignore=shutil.ignore_patterns('__pycache__', '*.pyc'))
    for name in ('polymarket_live_stream.py', 'polymarket_live_read_api.py', 'polymarket_stream_metrics.py'):
        shutil.copy2(project / 'src/marketcow' / name, new / 'src/marketcow' / name)
    (new / 'units').mkdir()
    name = 'marketcow-paper-read-api.service'
    original = (old / 'units' / name).read_text()
    (new / 'previous-api.service').write_text(original)
    (new / 'units' / name).write_text(original.replace(str(old), str(new)).replace(
        '/logs/' + old_name + '-', '/logs/' + new_name + '-'))
    subprocess.run(['systemd-analyze', '--user', 'verify', str(new / 'units' / name)], check=True)
    hashes = {}
    for path in sorted(new.rglob('*')):
        if path.is_file():
            with path.open('rb') as file:
                hashes[str(path.relative_to(new))] = hashlib.file_digest(file, 'sha256').hexdigest()
    manifest = json.dumps(hashes, indent=2)
    (new / 'manifest.json').write_text(manifest)
    print(json.dumps({'prepared': True, 'activated': False, 'release': str(new),
                      'manifest_sha256': hashlib.sha256(manifest.encode()).hexdigest()}))


if __name__ == '__main__':
    main()
