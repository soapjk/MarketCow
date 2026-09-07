"""Offline candidate clone from a live bounded SQLite snapshot; never copies JSONL."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import sqlite3
import time


def clone(source: Path, target: Path, *, kind: str):
    if kind not in ('scoped', 'discovery'):
        raise ValueError('explicit scoped/discovery kind required')
    source = source.resolve(strict=True)
    target = target.resolve()
    stage = target.with_name(target.name + '.preparing')
    if target.exists() or stage.exists() or target.is_relative_to(source):
        raise ValueError('new isolated target required')
    os.umask(0o077)
    names = (['catalog.json', 'configured-scope.json', 'scope-runtime.json',
              'rust-scoped-plan-r1.json', 'live-bridge-plan-r1.json'] if kind == 'scoped'
             else ['catalog.json', 'rust-source-plan.json'])
    files = [source / name for name in names]
    for name in ['catalogs', 'catalog-indexes', 'candidate-snapshots', 'raw']:
        files.extend(p for p in (source / name).rglob('*') if not p.is_dir())
    for path in files:
        if path.is_symlink() or not path.is_file() or not path.resolve().is_relative_to(source):
            raise ValueError(f'unsafe source path {path}')
    required = sum(p.stat().st_size for p in files if p.stat().st_mode & 0o222 or p.parent == source)
    db_path = source / 'indexes/latest-state.sqlite3'
    if shutil.disk_usage(target.parent).free < required + db_path.stat().st_size + 2 * 1024**3:
        raise ValueError('insufficient candidate and rollback reserve')
    stage.mkdir(mode=0o700)
    (stage / 'indexes').mkdir()
    copied = []
    for path in files:
        dest = stage / path.relative_to(source)
        dest.parent.mkdir(parents=True, exist_ok=True)
        if not path.stat().st_mode & 0o222 and path.parent != source:
            os.link(path, dest)
            method = 'readonly_hardlink'
        else:
            shutil.copyfile(path, dest)
            method = 'copy'
        copied.append({'path': str(path.relative_to(source)), 'bytes': dest.stat().st_size, 'method': method})
    db = sqlite3.connect('file:' + str(db_path) + '?mode=ro', uri=True)
    destination = sqlite3.connect(stage / 'indexes/latest-state.sqlite3')
    deadline = time.monotonic() + 60
    def progress(status, remaining, total):
        if time.monotonic() > deadline:
            raise TimeoutError('bounded backup deadline exceeded')
    db.backup(destination, pages=256, progress=progress, sleep=0.01)
    db.close()
    metadata = dict(destination.execute('SELECT * FROM metadata'))
    if int(metadata['bounded_history_bytes']) <= 0:
        raise ValueError('source is not bounded history')
    if destination.execute('PRAGMA integrity_check').fetchone()[0] != 'ok':
        raise ValueError('candidate SQLite integrity')
    count = 0
    used = 0
    previous = None
    for cursor, payload, digest in destination.execute('SELECT cursor,payload,sha256 FROM recent_events ORDER BY cursor'):
        body = payload.encode() if isinstance(payload, str) else payload
        if hashlib.sha256(body).hexdigest() != digest or (previous is not None and cursor != previous + 1):
            raise ValueError('recent event hash or continuity')
        previous = cursor
        used += len(body)
        count += 1
    if previous != int(metadata['latest_cursor']) or used != int(metadata['recent_event_bytes']):
        raise ValueError('bounded boundary differs')
    destination.close()
    def rebase(value):
        if isinstance(value, dict):
            return {k: rebase(v) for k, v in value.items()}
        if isinstance(value, list):
            return [rebase(v) for v in value]
        if isinstance(value, str) and value.startswith(str(source) + '/'):
            return str(target / Path(value).relative_to(source))
        return value
    catalog = stage / 'catalog.json'
    catalog.write_text(json.dumps(rebase(json.loads(catalog.read_text())), separators=(',', ':')))
    plan_path = stage / ('live-bridge-plan-r1.json' if kind == 'scoped' else 'rust-source-plan.json')
    if kind == 'scoped':
        plan = rebase(json.loads(plan_path.read_text()))
        plan['catalog_manifest_sha256'] = hashlib.sha256(catalog.read_bytes()).hexdigest()
        plan_path.write_text(json.dumps(plan, separators=(',', ':')))
    state = dict(metadata)
    state['path'] = str(target / 'indexes/latest-state.sqlite3')
    (stage / 'state-index.json').write_text(json.dumps(state, separators=(',', ':')))
    report = {'complete': True, 'kind': kind, 'source': str(source), 'target': str(target),
              'cursor': metadata['latest_cursor'], 'floor': metadata['history_floor_cursor'],
              'gaps': metadata['unresolved_gap_count'], 'recent_events': count,
              'recent_event_bytes': used, 'no_jsonl': True, 'files': copied,
              'plan_sha256': hashlib.sha256(plan_path.read_bytes()).hexdigest()}
    (stage / 'public-candidate-report.json').write_text(json.dumps(report, indent=2))
    for path in stage.rglob('*'):
        if path.is_file():
            with path.open('rb') as stream:
                os.fsync(stream.fileno())
    for path in [p for p in stage.rglob('*') if p.is_dir()] + [stage]:
        fd = os.open(path, os.O_RDONLY)
        os.fsync(fd)
        os.close(fd)
    stage.rename(target)
    fd = os.open(target.parent, os.O_RDONLY)
    os.fsync(fd)
    os.close(fd)
    print(json.dumps(report), flush=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--source', required=True, type=Path)
    parser.add_argument('--target', required=True, type=Path)
    parser.add_argument('--kind', required=True, choices=['scoped', 'discovery'])
    args = parser.parse_args()
    clone(args.source, args.target, kind=args.kind)
