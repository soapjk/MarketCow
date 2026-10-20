import importlib.util
from pathlib import Path
import unittest
import json
import sqlite3
import tempfile

proxy_spec = importlib.util.spec_from_file_location('provision_binance', Path(__file__).parents[1] / 'scripts/configure_u1_binance_proxy.py')
provision = importlib.util.module_from_spec(proxy_spec)
proxy_spec.loader.exec_module(provision)


spec = importlib.util.spec_from_file_location('deploy_binance', Path(__file__).parents[1] / 'scripts/deploy_u1_marketcow.py')
deploy = importlib.util.module_from_spec(spec)
spec.loader.exec_module(deploy)


class BinanceProxyTests(unittest.TestCase):
    def test_only_selected_node_and_dns_are_copied_without_global_tunnel(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config = {'proxies': [{'name': 'selected', 'password': 'test-only'},
                                  {'name': 'unrelated', 'password': 'never-copy'}],
                      'dns': {'nameserver': ['https://resolver.example/dns-query'],
                              'default-nameserver': ['192.0.2.1']}}
            (root / 'config.yaml').write_text(json.dumps(config))
            with sqlite3.connect(root / 'database.sqlite') as db:
                db.execute('CREATE TABLE profiles(selected_map TEXT)')
                db.execute('INSERT INTO profiles VALUES (?)', (json.dumps({'手动选择': 'selected'}),))
            result, name = provision.configuration(root)
            self.assertEqual(name, 'selected')
            self.assertEqual(len(result['proxies']), 1)
            self.assertNotIn('never-copy', json.dumps(result))
            self.assertFalse(result['tun']['enable'])
            self.assertFalse(result['allow-lan'])
            self.assertEqual(result['bind-address'], '127.0.0.1')
            self.assertEqual(result['rules'][-1], 'MATCH,REJECT')
            self.assertEqual(result['dns']['nameserver'], config['dns']['nameserver'])
            with sqlite3.connect(root / 'database.sqlite') as db:
                db.execute('INSERT INTO profiles VALUES (?)', (json.dumps({'手动选择': 'different'}),))
            with self.assertRaises(RuntimeError):
                provision.configuration(root)

    def test_unit_change_is_isolated_idempotent_and_keeps_existing_proxy(self):
        unit = '[Unit]\nDescription=collector\n[Service]\nEnvironment="HTTPS_PROXY=http://127.0.0.1:17890"\nExecStart=/bin/collector\n'
        updated = deploy.with_binance_proxy(unit, 'http://127.0.0.1:17892')
        self.assertIn('HTTPS_PROXY=http://127.0.0.1:17890', updated)
        self.assertIn('MARKETCOW_BINANCE_PROXY=http://127.0.0.1:17892', updated)
        self.assertEqual(deploy.with_binance_proxy(updated, 'http://127.0.0.1:17892'), updated)

    def test_rejects_global_remote_or_wrong_service_configuration(self):
        for endpoint in ['http://0.0.0.0:17892', 'http://127.0.0.1:17890', 'http://remote:7890']:
            with self.assertRaises(RuntimeError):
                deploy.with_binance_proxy('[Service]\n', endpoint)
        with self.assertRaises(RuntimeError):
            deploy.with_binance_proxy('[Unit]\n', 'http://127.0.0.1:17892')
