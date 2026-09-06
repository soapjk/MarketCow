"""Hermetic service switch tests; no SSH or actual systemd invocation."""
import json
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest
from unittest.mock import patch

SCRIPTS = Path(__file__).resolve().parents[1]/'scripts'
sys.path.insert(0, str(SCRIPTS))
import activate_u1_direct_rust as activation


class ActivationTests(unittest.TestCase):
    def run_case(self, fail, tamper=False):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            release = root/'release'
            config = root/'config'
            for path in (release/'units', config, root/'logs'):
                path.mkdir(parents=True)
            names = (activation.LIVE, activation.DISCOVERY, activation.API)
            bindings = {}
            original_paths = {}
            for name in names:
                old = root/('old-'+name)
                old.write_text('old '+name)
                original_paths[name] = old
                (config/name).symlink_to(old)
                bindings[name] = {'path': str(old), 'sha256': activation.sha(old)}
                if name != activation.API:
                    new = release/'units'/name
                    new.write_text('new '+name)
                    bindings[name]['new_sha256'] = activation.sha(new)
            binary = release/'marketcow-discovery-collector'
            binary.write_bytes(b'test binary, never executed')
            for kind in ('scoped', 'discovery'):
                data = root/f'bounded-{kind}-candidate-r1'
                (data/'indexes').mkdir(parents=True)
                with sqlite3.connect(data/'indexes/latest-state.sqlite3') as db:
                    db.execute('CREATE TABLE metadata(key TEXT PRIMARY KEY,value TEXT)')
                    db.executemany('INSERT INTO metadata VALUES (?,?)',
                        [('latest_cursor','10'),('recent_event_bytes','5'),('bounded_history_bytes','10')])
            seed = root/'bounded-discovery-candidate-r1/discovery-public-seed.json'
            seed.write_text('{}')
            manifest = {'prepared': True, 'activated': False, 'binary_sha256': activation.sha(binary),
                'seed_sha256': activation.sha(seed), 'units': bindings,
                'live_base': 'http://test', 'discovery_base': 'http://test'}
            manifest_path = release/'manifest.json'
            manifest_path.write_text(json.dumps(manifest))
            if tamper:
                original_paths[activation.LIVE].write_text('changed since preparation')
            states = {name: True for name in names}
            calls = []
            def status(name):
                active = states.get(name, True)
                return {'MainPID': '9' if active else '0', 'Result': 'success',
                    'ExecMainStatus': '0', 'ActiveState': 'active' if active else 'inactive'}
            def run(cmd, **kwargs):
                calls.append(cmd)
                if cmd[2] in ('stop', 'start'):
                    for name in cmd[3:]:
                        states[name] = cmd[2] == 'start'
            def baseline(*args):
                if tamper:
                    with self.assertRaises(AssertionError):
                        activation.main()
                    self.assertEqual(calls, [])
                    self.assertFalse((root/'logs/direct-rust-0424396-activation.json').exists())
                    return
                elif fail:
                    raise RuntimeError('injected startup failure')
                return {'cursor': 11}
            with patch.multiple(activation, R=root, RELEASE=release, CONFIG=config), \
                 patch.object(activation, 'status', side_effect=status), \
                 patch.object(activation.subprocess, 'run', side_effect=run), \
                 patch.object(activation, 'baseline', side_effect=baseline), \
                 patch.object(sys, 'argv', ['activate', '--manifest-sha256', activation.sha(manifest_path),
                    '--paper-pause-receipt', 'channel_message:synthetic-test']):
                if fail:
                    with self.assertRaisesRegex(RuntimeError, 'injected'):
                        activation.main()
                else:
                    activation.main()
            report = json.loads((root/'logs/direct-rust-0424396-activation.json').read_text())
            self.assertEqual(report['activated'], not fail)
            if fail:
                self.assertTrue(report['rollback_units_restored'])
                for name in names:
                    self.assertEqual((config/name).resolve(), original_paths[name])
            else:
                self.assertEqual((config/activation.API).resolve(), Path('/dev/null'))
                self.assertFalse(states[activation.API])
            self.assertFalse(any('marketcow-polymarket-proxy.service' in cmd for cmd in calls))
            for kind in ('scoped', 'discovery'):
                with sqlite3.connect(root/f'bounded-{kind}-candidate-r1/indexes/latest-state.sqlite3') as db:
                    self.assertEqual(db.execute("SELECT value FROM metadata WHERE key='latest_cursor'").fetchone()[0], '10')

    def test_failed_activation_restores_units_without_state_rewind(self):
        self.run_case(True)

    def test_success_retires_python_api_without_proxy_restart(self):
        self.run_case(False)

    def test_changed_unit_fails_before_any_service_operation(self):
        self.run_case(False, tamper=True)
