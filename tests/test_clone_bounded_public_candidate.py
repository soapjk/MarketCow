import hashlib
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest

from scripts.clone_bounded_public_candidate import clone


class CloneTests(unittest.TestCase):
    def seed(self, root):
        (root / 'indexes').mkdir(parents=True)
        for name in ['catalogs', 'catalog-indexes', 'candidate-snapshots', 'raw']:
            (root / name).mkdir()
        for name in ['configured-scope.json', 'scope-runtime.json', 'rust-scoped-plan-r1.json']:
            (root / name).write_text('{}')
        (root / 'catalog.json').write_text(json.dumps({'path': str(root / 'catalogs/data')}))
        (root / 'catalog.json').chmod(0o444)
        (root / 'catalogs/data').write_bytes(b'immutable')
        (root / 'catalogs/data').chmod(0o444)
        (root / 'live-bridge-plan-r1.json').write_text('{}')
        body = b'{"cursor":1}'
        db = sqlite3.connect(root / 'indexes/latest-state.sqlite3')
        db.executescript('CREATE TABLE metadata(key TEXT PRIMARY KEY,value TEXT); CREATE TABLE recent_events(cursor INTEGER PRIMARY KEY,payload BLOB,sha256 TEXT);')
        db.executemany('INSERT INTO metadata VALUES (?,?)', [('bounded_history_bytes','1024'),('latest_cursor','1'),
            ('history_floor_cursor','1'),('recent_event_bytes',str(len(body))),('unresolved_gap_count','0')])
        db.execute('INSERT INTO recent_events VALUES (1,?,?)',(body,hashlib.sha256(body).hexdigest()))
        db.commit()
        db.close()

    def test_atomic_bounded_clone_preserves_readonly_source_manifest(self):
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / 'source'
            target = Path(directory) / 'candidate'
            self.seed(source)
            before = (source / 'catalog.json').read_bytes()
            clone(source,target)
            self.assertEqual(before,(source / 'catalog.json').read_bytes())
            self.assertNotEqual((source / 'catalog.json').stat().st_ino,(target / 'catalog.json').stat().st_ino)
            self.assertEqual((source / 'catalogs/data').stat().st_ino,(target / 'catalogs/data').stat().st_ino)
            self.assertFalse((target / 'events.jsonl').exists())
            self.assertTrue(json.loads((target / 'public-candidate-report.json').read_text())['complete'])
            with self.assertRaises(ValueError):
                clone(source,target)

    def test_corruption_never_publishes_candidate(self):
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / 'source'
            target = Path(directory) / 'candidate'
            self.seed(source)
            db = sqlite3.connect(source / 'indexes/latest-state.sqlite3')
            db.execute("UPDATE recent_events SET sha256='invalid'")
            db.commit()
            db.close()
            with self.assertRaisesRegex(ValueError,'hash'):
                clone(source,target)
            self.assertFalse(target.exists())


if __name__ == '__main__':
    unittest.main()
