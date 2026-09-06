import unittest

from scripts.run_polymarket_dynamic_live_api import validated_listener


class DynamicLiveListenerTests(unittest.TestCase):
    def test_loopback_is_allowed_without_lan_flag(self):
        self.assertEqual(
            validated_listener({'host': '127.0.0.1', 'port': 8793}),
            ('127.0.0.1', 8793),
        )

    def test_exact_private_lan_requires_explicit_flag(self):
        with self.assertRaisesRegex(ValueError, 'private-LAN'):
            validated_listener({'host': '192.168.124.3', 'port': 8793})
        self.assertEqual(
            validated_listener({
                'host': '192.168.124.3',
                'port': 8793,
                'allow_private_lan': True,
            }),
            ('192.168.124.3', 8793),
        )

    def test_wildcard_and_production_port_are_rejected(self):
        for config in (
            {'host': '0.0.0.0', 'port': 8793, 'allow_private_lan': True},
            {'host': '127.0.0.1', 'port': 8790},
        ):
            with self.assertRaisesRegex(ValueError, 'private-LAN'):
                validated_listener(config)
