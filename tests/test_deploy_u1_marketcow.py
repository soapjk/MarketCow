import hashlib
import importlib.util
import io
from pathlib import Path
import tarfile
import tempfile
import unittest


spec = importlib.util.spec_from_file_location(
    'deploy_u1', Path(__file__).parents[1] / 'scripts/deploy_u1_marketcow.py')
deploy = importlib.util.module_from_spec(spec)
spec.loader.exec_module(deploy)


class DeploymentTests(unittest.TestCase):
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
