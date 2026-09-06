"""Offline, hash-verified relocation into a new self-contained live generation.

Does not fetch markets, rebuild catalog indexes, or alter source facts/scope IDs.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil


def digest(path):
    h = hashlib.sha256()
    with path.open('rb') as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b''):
            h.update(chunk)
    return h.hexdigest()


def relocate(source, target, original, scope, *, resume=False):
    source = source.resolve(strict=True)
    scope = scope.resolve(strict=True)
    target = target.resolve()
    staging = target.with_name(target.name + '.preparing')
    if target.exists() or (staging.exists() and not resume) or target.is_relative_to(source):
        raise ValueError('target must be a new independent root')
    if staging.is_symlink() or any(p.is_symlink() for p in staging.rglob('*')):
        raise ValueError('staging contains a symlink')
    paths = list(source.rglob('*'))
    if any(p.is_symlink() for p in paths):
        raise ValueError('source contains a symlink')
    manifest = json.loads((source / 'catalog.json').read_bytes())
    verified = []
    for field in ['normalized_catalog', 'catalog_index', 'candidate_snapshot']:
        entry = manifest[field]
        relative = Path(entry['path']).relative_to(original)
        path = (source / relative).resolve(strict=True)
        if not path.is_relative_to(source) or digest(path) != entry['sha256']:
            raise ValueError('catalog artifact integrity failed: ' + field)
        verified.append(field)
    scope_hash = digest(scope)
    runtime = json.loads((source / 'scope-runtime.json').read_bytes())
    configured = json.loads(scope.read_bytes())
    if runtime['manifest_sha256'] != scope_hash or runtime['scope_id'] != configured['active_scope_id']:
        raise ValueError('configured scope binding differs')
    if configured['catalog_revision'] != manifest['catalog_revision'] or configured['mode'] != 'shadow':
        raise ValueError('scope is not this Shadow catalog')
    os.umask(0o077)
    staging.mkdir(mode=0o700, exist_ok=resume)
    copied = []
    for path in sorted(paths):
        relative = path.relative_to(source)
        dest = staging / relative
        if path.is_dir():
            dest.mkdir(mode=0o700, exist_ok=resume)
            continue
        before = digest(path)
        if not (resume and dest.is_file() and digest(dest) == before):
            shutil.copyfile(path, dest)
        dest.chmod(0o600)
        if digest(dest) != before or digest(path) != before:
            raise ValueError('copy mismatch or source changed: ' + str(relative))
        with dest.open('rb') as stream:
            os.fsync(stream.fileno())
        copied.append({'path': str(relative), 'sha256': before, 'bytes': dest.stat().st_size})

    def rebase(value):
        if isinstance(value, dict):
            return {k: rebase(v) for k, v in value.items()}
        if isinstance(value, list):
            return [rebase(v) for v in value]
        if isinstance(value, str) and value.startswith('/'):
            relative = Path(value).relative_to(original)
            if not (staging / relative).resolve(strict=True).is_relative_to(staging):
                raise ValueError('runtime reference escapes root')
            return str(target / relative)
        return value

    for name in ['catalog.json', 'state-index.json']:
        path = staging / name
        with path.open('w') as stream:
            json.dump(rebase(json.loads((source / name).read_bytes())), stream)
            stream.flush()
            os.fsync(stream.fileno())
    shutil.copyfile(scope, staging / 'configured-scope.json')
    if digest(staging / 'configured-scope.json') != scope_hash:
        raise ValueError('scope copy hash differs')
    report = {'complete': True, 'source_root': str(source), 'target_root': str(target),
              'catalog_revision': manifest['catalog_revision'], 'scope_id': runtime['scope_id'],
              'configured_market_count': len(configured['configured_markets']),
              'configured_scope_sha256': scope_hash, 'verified_catalog_artifacts': verified,
              'copied': copied, 'remote_fetches': 0}
    # A relocated root may itself be relocated again. Retain its old receipt.
    previous_report = staging / 'relocation-report.json'
    if previous_report.exists():
        provenance = staging / 'relocation-provenance'
        provenance.mkdir(exist_ok=True)
        archived = provenance / (digest(previous_report) + '.json')
        if archived.exists() and digest(archived) != digest(previous_report):
            raise ValueError('relocation provenance differs')
        previous_report.rename(archived)
    with (staging / 'relocation-report.json').open('x') as stream:
        json.dump(report, stream, indent=2)
        stream.flush()
        os.fsync(stream.fileno())
    for directory in [p for p in staging.rglob('*') if p.is_dir()] + [staging]:
        fd = os.open(directory, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    staging.rename(target)
    fd = os.open(target.parent, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
    print(json.dumps({k: v for k, v in report.items() if k != 'copied'}))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    for name in ['source', 'target', 'original', 'scope']:
        parser.add_argument('--' + name, required=True, type=Path)
    parser.add_argument('--resume', action='store_true')
    args = parser.parse_args()
    relocate(args.source, args.target, args.original, args.scope, resume=args.resume)
