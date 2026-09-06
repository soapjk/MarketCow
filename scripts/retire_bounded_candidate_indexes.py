"""Retire two audited, unpublished candidate copies; preserve original recovery copies."""
import fcntl
import hashlib
import json
from pathlib import Path
import sqlite3

runtime = Path('/mnt/p44pro/marketcow-shadow-v3-runtime/linux')
root = runtime / 'bounded-discovery-candidate-r1'
directory = root / 'discovery-materialized-v3'
source = runtime / 'discovery-source-r1/discovery-materialized-v3'
names = [
 'catalog-7d05e5113ee8ac6eb01d94371feb1df6bb7dd8a89348b555e01cd5af17fffa5c-daf50f5bbe9c4a9fad5cbe8557c05046.sqlite3',
 'catalog-7d05e5113ee8ac6eb01d94371feb1df6bb7dd8a89348b555e01cd5af17fffa5c-569a970ef1b148fcb488d4a89310875d.sqlite3',
]

def sha(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()

with (directory / '.materialization.lock').open('a+b') as lock:
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    manifest = json.loads((directory / 'current.json').read_text())
    published = Path(manifest['database_path']).resolve(strict=True)
    assert published.parent == directory.resolve()
    with sqlite3.connect(f'file:{published}?mode=ro', uri=True) as db:
        assert db.execute('PRAGMA integrity_check').fetchone()[0] == 'ok'
        assert db.execute('SELECT 1 FROM snapshots WHERE snapshot_id=? AND boundary_cursor=?',
                          (manifest['snapshot_id'],manifest['boundary_cursor'])).fetchone()
    report = []
    for name in names:
        candidate = directory / name
        original = source / name
        assert candidate.resolve(strict=True) != published
        assert not candidate.is_symlink() and not original.is_symlink()
        assert not Path(str(candidate) + '-wal').exists()
        digest = sha(candidate)
        assert digest == sha(original), 'original recovery copy differs'
        report.append({'removed_candidate': str(candidate), 'original_preserved': str(original),
                       'bytes':candidate.stat().st_size, 'sha256':digest})
    # All original copies and the active publication verified before any deletion.
    for item in report:
        Path(item['removed_candidate']).unlink()
    (runtime / 'logs/bounded-index-retirement-r1-report.json').write_text(json.dumps(report,indent=2))
    print(json.dumps({'removed_bytes':sum(item['bytes'] for item in report),'files':report}))
