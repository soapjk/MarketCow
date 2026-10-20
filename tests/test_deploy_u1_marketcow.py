import hashlib
import importlib.util
import io
import json
import sqlite3
import shutil
from pathlib import Path
import tarfile
import tempfile
import tomllib
import unittest
from unittest.mock import patch
from types import SimpleNamespace


spec = importlib.util.spec_from_file_location(
    'deploy_u1', Path(__file__).parents[1] / 'scripts/deploy_u1_marketcow.py')
deploy = importlib.util.module_from_spec(spec)
spec.loader.exec_module(deploy)


class DeploymentTests(unittest.TestCase):
    def test_activate_local_does_not_upload_or_rebuild(self):
        with patch.object(deploy, 'run') as run:
            deploy.local(SimpleNamespace(activate_prepared=True, host='czx@192.168.124.3'))
        argv=run.call_args.args[0]
        self.assertEqual(argv[0],'ssh')
        self.assertIn('--activate-prepared',argv[-1])
        self.assertEqual(run.call_count,1)

    def test_prepared_unit_drift_refuses_before_stopping_source(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory); d=root/'deploy';d.mkdir()
            home=root/'home';unit=home/'.config/systemd/user'/deploy.UNIT
            unit.parent.mkdir(parents=True);unit.write_text('concurrent edit')
            (d/'receipt.json').write_text(json.dumps({'status':'prepared'}))
            (d/'prepared-unit').write_text('new unit')
            (d/'prepared-unit-original').write_text('original unit')
            with patch.object(deploy,'DEPLOY',d),patch.object(deploy.Path,'home',return_value=home),patch.object(deploy,'run') as run:
                with self.assertRaisesRegex(RuntimeError,'unit changed'):
                    deploy.activate_prepared()
                run.assert_not_called()

    def test_sdk_listener_preserves_internal_address_and_other_arguments(self):
        unit = '[Service]\nExecStart=/collector --public-listen 192.168.124.3:8793 --other unchanged\n'
        policy = str(deploy.DEPLOY / 'private/sdk-access.pb')
        updated = deploy.with_sdk_listener(unit, '127.0.0.1:18793', policy)
        self.assertIn('--public-listen 192.168.124.3:8793', updated)
        self.assertIn('--other unchanged', updated)
        self.assertEqual(updated.count('--sdk-listen '), 1)
        again = deploy.with_sdk_listener(updated, '127.0.0.1:18793', policy)
        self.assertEqual(again.count('--sdk-listen '), 1)
        self.assertEqual(again, updated)
        for listen, path in [('0.0.0.0:18793', policy), ('127.0.0.1:18793', '/tmp/public.pb')]:
            with self.assertRaises(RuntimeError):
                deploy.with_sdk_listener(unit, listen, path)

    def test_source_archive_contains_wire_but_not_sdk_runtime(self):
        repo = Path(__file__).resolve().parents[1]
        files = set(deploy.source_files(repo))
        members = tomllib.loads((repo / 'Cargo.toml').read_text())['workspace']['members']
        self.assertTrue({member + '/Cargo.toml' for member in members} <= files)
        self.assertIn('sdk/crates/marketcow-wire/Cargo.toml', files)
        self.assertIn('sdk/crates/marketcow-wire/proto/marketcow/btc/v3/price.proto', files)
        self.assertFalse(any(name.startswith(('sdk/python', 'sdk/crates/marketcow-sdk')) for name in files))
        self.assertNotIn('crates/marketcow-contracts/schema.bin', files)
        self.assertFalse(any('/.venv/' in name or '/target/' in name for name in files))

    def test_service_archive_local_dependencies_are_self_contained(self):
        repo = Path(__file__).resolve().parents[1]
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory)
            for name in deploy.source_files(repo):
                output = target / name
                output.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(repo / name, output)
            self.assertFalse((target / 'sdk/Cargo.toml').exists())
            for manifest_path in target.rglob('Cargo.toml'):
                manifest = tomllib.loads(manifest_path.read_text())
                self.assertNotEqual(manifest.get('package', {}).get('name'), 'marketcow-python')
                for section in ('dependencies', 'dev-dependencies', 'build-dependencies'):
                    for dependency in manifest.get(section, {}).values():
                        if isinstance(dependency, dict) and 'path' in dependency:
                            local = (manifest_path.parent / dependency['path']).resolve()
                            self.assertTrue(local.is_relative_to(target.resolve()))
                            self.assertTrue((local / 'Cargo.toml').is_file())

    def test_locked_cargo_seed_uses_public_package_files_not_configuration(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            repo, cargo, source, target = (root / name for name in ('repo', 'cargo', 'source', 'target'))
            repo.mkdir()
            (repo / 'Cargo.lock').write_text('''version = 4
[[package]]
name = "im"
version = "15.1.0"
source = "registry+https://github.com/rust-lang/crates.io-index"
''')
            archive = cargo / 'registry/cache/local-public/im-15.1.0.crate'
            index = cargo / 'registry/index/local-public/.cache/2/im'
            for path, body in ((archive, b'public crate archive'), (index, b'public index record')):
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(body)
            (cargo / 'credentials.toml').write_text('not a seed input')
            entries = deploy.cargo_seed_entries(repo, set(), cargo)
            self.assertEqual(set(entries), {'dependency-seed/cache/im-15.1.0.crate', 'dependency-seed/index/2/im'})
            self.assertEqual(deploy.cargo_seed_entries(repo, {'im-15.1.0.crate'}, cargo), {})
            for name, path in entries.items():
                destination = source / name
                destination.parent.mkdir(parents=True, exist_ok=True)
                destination.write_bytes(path.read_bytes())
            target.mkdir()
            config = target / 'config.toml'
            config.write_text('preserve existing configuration')
            with patch.dict(deploy.os.environ, {'CARGO_HOME': str(target)}):
                self.assertEqual(deploy.install_cargo_seed(source), 2)
            self.assertEqual(config.read_text(), 'preserve existing configuration')
            self.assertFalse((target / 'credentials.toml').exists())
            self.assertEqual((target / 'registry/cache/index.crates.io-1949cf8c6b5b557f/im-15.1.0.crate').read_bytes(), b'public crate archive')

    def test_typed_migration_root_rejects_ambiguous_or_external_paths(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            data = root / 'btc'
            data.mkdir()
            with patch.object(deploy, 'ROOT', root):
                self.assertEqual(deploy.typed_migration_root(f'--btc-research-root {data}'), data)
                for unit in ('', f'--btc-research-root {root}',
                             f'--btc-research-root {data} --btc-research-root {data}',
                             '--btc-research-root /tmp'):
                    with self.assertRaises(RuntimeError):
                        deploy.typed_migration_root(unit)

    def test_explicit_typed_migration_receipts_do_not_claim_coverage(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            data = root / 'btc'
            staging = root / 'deployment'
            staging.mkdir()
            (staging / 'build.log').touch()
            for relative, table, columns, row in (
                ('binance-trades/trades.sqlite3', 'binance_trades',
                 'trade_id INTEGER PRIMARY KEY,trade_time_ms INTEGER', (10, 1792000000000)),
                ('chainlink-twap/observations.sqlite3', 'chainlink_observations',
                 'kind TEXT,source_time_ms INTEGER', ('twap60', 1792000000000)),
            ):
                path = data / relative
                path.parent.mkdir(parents=True, exist_ok=True)
                with sqlite3.connect(path) as db:
                    db.execute(f'CREATE TABLE {table} ({columns})')
                    db.execute(f'INSERT INTO {table} VALUES (?,?)', row)
                db.close()
            commands = []
            def fake_run(args, **kwargs):
                commands.append(args)
                return SimpleNamespace(stdout=json.dumps({
                    'schema_version': 'marketcow.typed-db-import.v1',
                    'source_database': str(Path(args[3]).resolve()), 'destination_database': args[3],
                    'start_time_ms': int(args[4].split('=')[1]),
                    'end_time_ms': int(args[5].split('=')[1]),
                    'coverage_verified': False,
                }))
            receipt = {}
            with patch.object(deploy, 'ROOT', root), patch.object(deploy, 'DEPLOY', staging), \
                    patch.object(deploy, 'run', side_effect=fake_run), \
                    patch.object(deploy.time, 'time_ns', return_value=1792000001000000000), \
                    patch.object(deploy.shutil, 'disk_usage', return_value=SimpleNamespace(free=10 * 1024**3)):
                deploy.run_typed_migrations(f'--btc-research-root {data}', root / 'migrator', receipt)
            self.assertEqual(len(commands), 2)
            self.assertEqual(commands[0][2], 'binance-typed')
            self.assertEqual(commands[1][2], 'chainlink-typed')
            self.assertTrue(all(i['coverage_verified'] is False for i in receipt['typed_database_imports']))
            self.assertTrue((staging / 'typed-migration-handoff.json').is_file())

    def test_external_build_keeps_separate_space_guards(self):
        gib = 1024**3
        deploy.check_build_space(2 * gib, 6 * gib, True)
        with self.assertRaisesRegex(RuntimeError, 'runtime volume'):
            deploy.check_build_space(2 * gib - 1, 100 * gib, True)
        with self.assertRaisesRegex(RuntimeError, 'isolated build volume'):
            deploy.check_build_space(3 * gib, 6 * gib - 1, True)
        with self.assertRaisesRegex(RuntimeError, 'isolated compilation'):
            deploy.check_build_space(3 * gib, 100 * gib, False)

    def test_btc_research_config_changes_only_client_budget(self):
        unit = ('ExecStart=/formal/collector --btc-research-clients 4 '
                '--btc-5m-clients 12 --root /data/live\nMemoryMax=17179869184\n')
        updated = deploy.rewrite_btc_research_clients(unit, 8)
        self.assertEqual(updated, unit.replace('--btc-research-clients 4',
                                               '--btc-research-clients 8'))
        self.assertEqual(updated, deploy.rewrite_btc_research_clients(updated, 8))
        for invalid in (unit.replace('--btc-research-clients 4 ', ''),
                        unit + '--btc-research-clients 4'):
            with self.assertRaises(RuntimeError):
                deploy.rewrite_btc_research_clients(invalid, 8)
        for invalid in (0, 65):
            with self.assertRaises(ValueError):
                deploy.rewrite_btc_research_clients(unit, invalid)

    def test_unit_rewrite_is_repeatable_and_preserves_data_arguments(self):
        unit = ('ExecStart=/usr/bin/python3 /old/scripts/run_with_bounded_log.py '
                '--log /data/current.log -- /old/release/marketcow-discovery-collector '
                '--root /data/live --configured-scope /data/config.json\n')
        current = Path('/data/deployment/current')
        updated = deploy.rewrite_unit(unit, current)
        self.assertEqual(updated, deploy.rewrite_unit(updated, current))
        self.assertIn('--root /data/live --configured-scope /data/config.json', updated)
        self.assertNotIn('/old/', updated)

    def test_ambiguous_formal_unit_is_rejected(self):
        with self.assertRaises(RuntimeError):
            deploy.rewrite_unit('/a/marketcow-discovery-collector /b/marketcow-discovery-collector', Path('/data/current'))

    def test_persistence_queue_rewrite_is_explicit_and_repeatable(self):
        unit = ('ExecStart=/old/run_with_bounded_log.py -- '
                '/old/marketcow-discovery-collector --persistence-queue-batches 256 '
                '--persistence-queue-bytes 67108864 --memory-replay-bytes 2147483648\n')
        current = Path('/data/deployment/current')
        updated = deploy.rewrite_unit(unit, current, None, 16384, 4 * 1024**3)
        self.assertIn('--persistence-queue-batches 16384', updated)
        self.assertIn('--persistence-queue-bytes 4294967296', updated)
        self.assertIn('--memory-replay-bytes 2147483648', updated)
        self.assertEqual(updated, deploy.rewrite_unit(updated, current, None, 16384, 4 * 1024**3))

    def test_btc_5m_ws_budget_rewrite_is_explicit_and_repeatable(self):
        unit = ('ExecStart=/old/run_with_bounded_log.py -- '
                '/old/marketcow-discovery-collector '
                '--btc-5m-clients 4 --btc-5m-send-timeout-seconds 5\n')
        current = Path('/data/deployment/current')
        updated = deploy.rewrite_unit(unit, current, btc_5m_clients=12)
        self.assertIn('--btc-5m-clients 12 --btc-5m-send-timeout-seconds 5', updated)
        self.assertEqual(updated, deploy.rewrite_unit(updated, current, btc_5m_clients=12))
        with self.assertRaisesRegex(RuntimeError, 'exactly one BTC 5m WS client limit'):
            deploy.rewrite_unit(unit.replace('--btc-5m-clients 4 ', ''),
                                current, btc_5m_clients=12)

    def test_chainlink_credentials_rewrite_only_references_existing_u1_path(self):
        unit = ('ExecStart=/old/run_with_bounded_log.py -- '
                '/old/marketcow-discovery-collector --btc-research-root /data/btc\n')
        current = Path('/data/deployment/current')
        credentials = '/home/czx/.config/marketcow/secrets/clob-polybolt.json'
        self.assertEqual(deploy.validate_chainlink_credentials_path(credentials), Path(credentials))
        updated = deploy.rewrite_unit(unit, current, None, None, None, credentials)
        self.assertIn('--btc-chainlink-credentials ' + credentials, updated)
        self.assertEqual(updated, deploy.rewrite_unit(updated, current,
                                                       None, None, None, credentials))
        for invalid in ('/tmp/clob.json',
                        '/home/czx/.config/marketcow/secrets/../../other.json',
                        '/home/czx/.config/marketcow/secrets/has space.json'):
            with self.assertRaises(RuntimeError):
                deploy.validate_chainlink_credentials_path(invalid)

    def test_disable_polynode_removes_only_subscription_argument(self):
        unit = ('ExecStart=/old/run_with_bounded_log.py -- '
                '/old/marketcow-discovery-collector --root /data/live '
                '--btc-polynode-key-file /data/private/api-key '
                '--btc-5m-manager-root /data/manager\n')
        current = Path('/data/deployment/current')
        updated = deploy.rewrite_unit(unit, current, disable_polynode=True)
        self.assertNotIn('--btc-polynode-key-file', updated)
        self.assertIn('--root /data/live', updated)
        self.assertIn('--btc-5m-manager-root /data/manager', updated)
        self.assertEqual(updated, deploy.rewrite_unit(updated, current, disable_polynode=True))

    def test_cleanup_rejects_outside_target_and_symlink(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with self.assertRaises(RuntimeError):
                deploy.remove_owned(root, root)
            link = root / 'link'
            link.symlink_to(root)
            with self.assertRaises(RuntimeError):
                deploy.remove_owned(link, root)
            self.assertTrue(root.exists())

    def test_verified_unpack_and_checksum_rejection(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            archive = root / 'source.tar.gz'
            body = b'fn main() {}'
            with tarfile.open(archive, 'w:gz') as tar:
                item = tarfile.TarInfo('crates/app/src/main.rs')
                item.size = len(body)
                tar.addfile(item, io.BytesIO(body))
            name = 'crates/app/src/main.rs'
            deploy.unpack_verified(archive, {name: hashlib.sha256(body).hexdigest()}, root / 'valid')
            self.assertEqual((root / 'valid' / name).read_bytes(), body)
            with self.assertRaises(RuntimeError):
                deploy.unpack_verified(archive, {name: '0' * 64}, root / 'invalid')

    def test_archive_link_and_traversal_are_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for index, name in enumerate(('../outside', 'safe.rs')):
                archive = root / f'{index}.tar'
                with tarfile.open(archive, 'w') as tar:
                    item = tarfile.TarInfo(name)
                    if name == 'safe.rs':
                        item.type = tarfile.SYMTYPE
                        item.linkname = '/etc/passwd'
                    tar.addfile(item)
                with self.assertRaises(RuntimeError):
                    deploy.unpack_verified(archive, {name: '0' * 64}, root / str(index))


if __name__ == '__main__':
    unittest.main()
