"""Freeze the tested fairness release without changing running units or data."""
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess


def sha(path):
    with path.open('rb') as source:
        return hashlib.file_digest(source, 'sha256').hexdigest()


def main():
    runtime = Path('/mnt/p44pro/marketcow-shadow-v3-runtime/linux')
    project = Path('/mnt/p44pro/projects/marketcow-shadow-v3')
    old = runtime / 'releases/bounded-v1'
    final = runtime / 'releases/bounded-v2'
    staging = runtime / 'releases/bounded-v2.preparing'
    binary = runtime / 'target/release/marketcow-discovery-collector'
    expected = 'c9f5f242f38fcb30a727d68c2dc075e4a2f85fabb9bf92d5782fd51c9ae21018'
    assert (runtime / 'logs/stream-fairness-build-r1.exit-code').read_text().strip() == '0'
    assert sha(binary) == expected
    assert not final.exists() and not staging.exists(), 'inspect existing release; never overwrite'
    os.umask(0o077)
    staging.mkdir()
    # Only copy the immutable prior read API, then the exact tested changes.
    shutil.copytree(old / 'src', staging / 'src', ignore=shutil.ignore_patterns('__pycache__', '*.pyc'))
    shutil.copytree(old / 'scripts', staging / 'scripts', ignore=shutil.ignore_patterns('__pycache__', '*.pyc'))
    for name in ('polymarket_stream_metrics.py', 'polymarket_live_stream.py', 'polymarket_live_read_api.py'):
        shutil.copy2(project / 'src/marketcow' / name, staging / 'src/marketcow' / name)
    shutil.copy2(binary, staging / binary.name)
    (staging / 'units').mkdir()
    (staging / 'previous-units').mkdir()
    names = ('marketcow-polymarket-collector.service', 'marketcow-paper-read-api.service')
    for name in names:
        active = Path('/home/czx/.config/systemd/user') / name
        assert active.resolve() == old / 'units' / name, 'unexpected active release'
        original = active.read_text()
        assert str(old) in original
        (staging / 'previous-units' / name).write_text(original)
        updated = original.replace(str(old), str(final))
        updated = updated.replace('/logs/bounded-v1-', '/logs/bounded-v2-')
        (staging / 'units' / name).write_text(updated)
    manifest = {str(p.relative_to(staging)): sha(p) for p in sorted(staging.rglob('*')) if p.is_file()}
    (staging / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
    staging.rename(final)
    subprocess.run(['systemd-analyze', '--user', 'verify', *[str(final / 'units' / n) for n in names]], check=True)
    report = {'prepared': True, 'activated': False, 'release': str(final),
              'binary_sha256': expected, 'manifest_sha256': sha(final / 'manifest.json'),
              'units': list(names), 'buffers_and_freshness_unchanged': True}
    with (runtime / 'logs/bounded-v2-prepare-report.json').open('x') as out:
        json.dump(report, out, indent=2)
    print(json.dumps(report))


if __name__ == '__main__':
    main()
