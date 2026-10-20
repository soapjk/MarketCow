import base64
import importlib.util
import json
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import unittest

spec = importlib.util.spec_from_file_location('migration', Path(__file__).parents[1] / 'scripts/migrate_u1_legacy_rules.py')
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)


class MigrationTests(unittest.TestCase):
    def original(self):
        g = dict(id='1', conditionId='condition', slug='btc-updown-5m-1792080600',
                 outcomes=json.dumps(['Up', 'Down']), clobTokenIds=json.dumps(['11', '22']),
                 eventStartTime='2026-10-15T16:10:00Z', endDate='2026-10-15T16:15:00Z',
                 description='original rule', resolutionSource='https://data.chain.link/streams/btc-usd-twap-60s-streams')
        e = dict(c='condition', t=[dict(o='Up', t='11'), dict(o='Down', t='22')])
        wrap = lambda x: dict(raw_base64=base64.b64encode(json.dumps(x).encode()).decode(), bytes=len(json.dumps(x).encode()))
        return dict(schema_version='marketcow.polymarket.btc-5m-manager-evidence.v1', gamma=wrap(g), execution=wrap(e),
                    slug=g['slug'], rule_sha256='opaque', record=dict(start_at=g['eventStartTime'], end_at=g['endDate'],
                    identity=dict(market_id='1', condition_id='condition', outcomes=[dict(outcome='Up', token_id='11'), dict(outcome='Down', token_id='22')]), rules=dict(rule_version='opaque')))

    def check(self, obj):
        return m.checked_original(json.dumps(obj).encode(), '1', 'condition', 'btc-updown-5m-1792080600', 'opaque')

    def test_exact_original(self):
        self.assertEqual(self.check(self.original())['up_token_id'], '11')

    def test_changed_opaque_id_rejected(self):
        obj = self.original(); obj['rule_sha256'] = 'other'
        with self.assertRaisesRegex(ValueError, 'identity_conflict'): self.check(obj)

    def test_token_order_rejected(self):
        obj = self.original(); obj['record']['identity']['outcomes'].reverse()
        with self.assertRaisesRegex(ValueError, 'identity_conflict'): self.check(obj)

    def test_window_mismatch_rejected(self):
        obj = self.original(); obj['record']['end_at'] = '2026-10-15T16:20:00Z'
        with self.assertRaisesRegex(ValueError, 'rule_binding_conflict'): self.check(obj)

    def test_length_mismatch_rejected(self):
        obj = self.original(); obj['gamma']['bytes'] += 1
        with self.assertRaisesRegex(ValueError, 'original_length'): self.check(obj)

    def test_atomic_idempotent_mapping_and_conflict(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp); folder = root/'btc-5m-manager-r1'; folder.mkdir()
            original = folder/'original.json'; original.write_bytes(b'original')
            db = sqlite3.connect(root/'typed-identities.sqlite3')
            db.executescript('CREATE TABLE typed_rules(rules_id TEXT,revision INTEGER,market_id TEXT,condition_id TEXT,window_id TEXT,start_at TEXT,end_at TEXT,up_token_id TEXT,down_token_id TEXT,description TEXT,resolution_source TEXT,resolution_proto BLOB,evidence_proto BLOB,PRIMARY KEY(rules_id,revision)); CREATE TABLE typed_legacy_identity(namespace TEXT,legacy_id TEXT,record_id TEXT,revision INTEGER,PRIMARY KEY(namespace,legacy_id));')
            payload = dict(root=tmp, original=str(original), original_bytes=base64.b64encode(b'original').decode(),
                           namespace=m.NAMESPACE, old_id='opaque', fields=self.check(self.original()), resolution='', evidence='')
            run = lambda: subprocess.run([sys.executable, '-c', m.APPLY], input=json.dumps(payload), text=True, capture_output=True)
            first = run(); self.assertEqual(first.returncode, 0, first.stderr)
            second = run(); self.assertEqual(json.loads(first.stdout), json.loads(second.stdout))
            payload['fields']['description'] = 'conflict'
            self.assertNotEqual(run().returncode, 0)
            self.assertEqual(db.execute('SELECT count(*) FROM typed_rules').fetchone()[0], 1)
            self.assertEqual(db.execute('SELECT count(*) FROM typed_legacy_identity').fetchone()[0], 1)
            db.close()


if __name__ == '__main__':
    unittest.main()
