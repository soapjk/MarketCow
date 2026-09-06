"""Synthetic filesystem/systemd tests; no real service or network calls."""
import json
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]/'scripts'))
import install_u1_send_diagnostics as installer


class InstallTests(unittest.TestCase):
    def run_case(self, failure=None):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            config = root/'config'
            prior = root/'releases/direct-rust-resume-ac50b7e'
            release = root/'releases/direct-rust-send-diag-6b23ab9'
            for path in (config, prior/'units', root/'logs', root/'target/release',
                         root/'bounded-scoped-candidate-r1/indexes'):
                path.mkdir(parents=True)
            names = (installer.LIVE, installer.DISCOVERY, installer.API, 'marketcow-polymarket-proxy.service')
            for name in names:
                old = prior/'units'/name
                old.write_text(str(prior/'marketcow-discovery-collector')+
                    ' direct-rust-resume-ac50b7e-marketcow-polymarket-collector.log')
                (config/name).symlink_to(old)
            binary = root/'target/release/marketcow-discovery-collector'
            binary.write_bytes(b'synthetic never executed')
            (root/'logs/public-send-diag-build-r1.exit-code').write_text('0')
            database = root/'bounded-scoped-candidate-r1/indexes/latest-state.sqlite3'
            with sqlite3.connect(database) as db:
                db.execute('CREATE TABLE metadata(key TEXT,value TEXT)')
                db.executemany('INSERT INTO metadata VALUES (?,?)', [
                    ('latest_cursor','10000000'), ('recent_event_bytes','8'), ('bounded_history_bytes','64')])
            calls = []
            original_sha = installer.sha
            def sha(path):
                return ('c55462ae4ebcabb1b39e02d8fe02f45f685421d60d1bb9419f4e028fbd8a4a88'
                        if path == binary else original_sha(path))
            def switch(name, target):
                calls.append(['switch', name])
                (config/name).unlink()
                (config/name).symlink_to(target)
            def baseline(*args):
                if failure == 'startup':
                    raise RuntimeError('synthetic failed full-sync')
                return {'cursor':10000001}
            minimum = 10000002 if failure == 'cursor' else 9999999
            with patch.multiple(installer, R=root, CONFIG=config), \
                 patch.object(installer,'sha',side_effect=sha), \
                 patch.object(installer,'status',return_value={'MainPID':'123','ActiveState':'active'}), \
                 patch.object(installer,'stop',side_effect=lambda name:calls.append(['stop',name])), \
                 patch.object(installer,'switch',side_effect=switch), \
                 patch.object(installer,'baseline',side_effect=baseline), \
                 patch.object(installer.subprocess,'run',side_effect=lambda cmd,**kw:calls.append(cmd)), \
                 patch.object(sys,'argv',['install','--minimum-cursor',str(minimum),
                    '--paper-pause-receipt','channel_message:synthetic-pause']):
                if failure:
                    with self.assertRaises((AssertionError,RuntimeError)):
                        installer.main()
                else:
                    installer.main()
            report=json.loads((release/'manifest.json').read_text())
            self.assertEqual(report['activated'],failure is None)
            self.assertEqual((config/installer.LIVE).resolve(),
                (prior if failure else release)/'units'/installer.LIVE)
            for name in names[1:]:
                self.assertEqual((config/name).resolve(),prior/'units'/name)
                self.assertFalse(any(name in call for call in calls))
            with sqlite3.connect(database) as db:
                self.assertEqual(dict(db.execute('SELECT * FROM metadata'))['latest_cursor'],'10000000')

    def test_only_live_switches(self):
        self.run_case()

    def test_failed_baseline_rolls_back_live_only(self):
        self.run_case('startup')

    def test_durable_cursor_below_paper_rejects_candidate(self):
        self.run_case('cursor')
