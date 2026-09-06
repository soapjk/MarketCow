"""Clone only a committed source boundary; retain the original torn tail intact."""
import hashlib
import json
import os
from pathlib import Path
import shutil
import sqlite3


def prepare(source, target):
    source = source.resolve(strict=True)
    target = target.resolve()
    stage = target.with_name(target.name + '.preparing')
    if target.exists() or stage.exists() or target.is_relative_to(source):
        raise ValueError('new isolated target required')
    os.umask(0o077)
    db = sqlite3.connect('file:' + str(source / 'indexes/latest-state.sqlite3') + '?mode=ro', uri=True)
    db.execute('BEGIN')
    metadata = dict(db.execute('SELECT * FROM metadata'))
    boundary = int(metadata['event_log_size'])
    if shutil.disk_usage(target.parent).free < boundary + 2 * 1024**3:
        raise ValueError('insufficient copy and rollback reserve')
    stage.mkdir(mode=0o700)
    skipped = {'events.jsonl', 'indexes/latest-state.sqlite3',
               'indexes/latest-state.sqlite3-wal', 'indexes/latest-state.sqlite3-shm'}
    for path in sorted(source.rglob('*')):
        relative = path.relative_to(source)
        if path.is_symlink():
            raise ValueError('source symlink rejected')
        if path.is_dir():
            (stage / relative).mkdir(exist_ok=True)
        elif str(relative) not in skipped and not path.name.endswith('.lock'):
            dest = stage / relative
            if (str(relative).startswith('catalogs/') or str(relative).startswith('raw/gamma-catalog/')) and path.stat().st_mode & 0o222 == 0:
                os.link(path, dest)
            else:
                shutil.copyfile(path, dest)
    copy = sqlite3.connect(stage / 'indexes/latest-state.sqlite3')
    db.backup(copy)
    assert copy.execute('PRAGMA integrity_check').fetchone()[0] == 'ok'
    last = copy.execute('SELECT byte_offset,byte_length,line_sha256 FROM event_offsets ORDER BY cursor DESC LIMIT 1').fetchone()
    assert last[0] + last[1] == boundary
    with (source / 'events.jsonl').open('rb') as src, (stage / 'events.jsonl').open('xb') as dst:
        remaining = boundary
        while remaining:
            chunk = src.read(min(1024**2, remaining))
            if not chunk:
                raise ValueError('committed log truncated')
            dst.write(chunk)
            remaining -= len(chunk)
        dst.flush()
        os.fsync(dst.fileno())
        tail_hash = hashlib.file_digest(src, 'sha256').hexdigest()
    with (stage / 'events.jsonl').open('rb') as stream:
        stream.seek(last[0])
        assert hashlib.sha256(stream.read(last[1])).hexdigest() == last[2]
    copy.close()
    db.close()
    def rebase(value):
        if isinstance(value, dict):
            return {k: rebase(v) for k, v in value.items()}
        if isinstance(value, list):
            return [rebase(v) for v in value]
        if isinstance(value, str) and value.startswith(str(source) + '/'):
            return str(target / Path(value).relative_to(source))
        return value
    for name in ('catalog.json', 'state-index.json', 'discovery-materialized-v3/current.json'):
        path = stage / name
        if not path.exists():
            continue
        path.write_text(json.dumps(rebase(json.loads(path.read_text())), separators=(',', ':')))
    plan_path = stage / 'live-bridge-plan-r1.json'
    if plan_path.exists():
        plan = rebase(json.loads(plan_path.read_text()))
        plan['catalog_manifest_sha256'] = hashlib.sha256((stage / 'catalog.json').read_bytes()).hexdigest()
        plan_path.write_text(json.dumps(plan, separators=(',', ':')))
    report = {'complete': True, 'source_unchanged': str(source), 'target': str(target),
              'cursor': metadata['latest_cursor'], 'unresolved_gap_count': metadata['unresolved_gap_count'],
              'committed_log_bytes': boundary, 'original_uncommitted_tail_sha256': tail_hash,
              'semantics': 'committed-boundary clone, not recovery of uncommitted events'}
    (stage / 'bounded-preparation-report.json').write_text(json.dumps(report, indent=2))
    for path in stage.rglob('*'):
        if path.is_file():
            with path.open('rb') as stream:
                os.fsync(stream.fileno())
    for directory in [p for p in stage.rglob('*') if p.is_dir()] + [stage]:
        fd = os.open(directory, os.O_RDONLY)
        os.fsync(fd)
        os.close(fd)
    stage.rename(target)
    fd = os.open(target.parent, os.O_RDONLY)
    os.fsync(fd)
    os.close(fd)
    print(json.dumps(report), flush=True)


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--source', required=True, type=Path)
    parser.add_argument('--target', required=True, type=Path)
    args = parser.parse_args()
    prepare(args.source, args.target)
