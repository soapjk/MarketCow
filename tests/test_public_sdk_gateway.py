import unittest
from scripts.prepare_public_sdk_gateway import render


class PublicGatewayTest(unittest.TestCase):
    def files(self, **kw):
        args = dict(domain="marketcow.llmay.cn", cloud_host="47.94.144.23",
                    ssh_user="marketcow-sdk", identity="/private/id",
                    known_hosts="/private/known_hosts")
        args.update(kw)
        return render(**args)

    def test_distinct_encrypted_channels_and_loopback_only(self):
        files = self.files()
        for name, port in (("price", 28791), ("book", 28792), ("query", 28793)):
            unit = files[f"marketcow-sdk-tunnel-{name}.service"]
            self.assertIn(f"-R 127.0.0.1:{port}:127.0.0.1:18793", unit)
            self.assertIn("StrictHostKeyChecking=yes", unit)
            self.assertIn("ExitOnForwardFailure=yes", unit)
            self.assertIn("ControlMaster=no", unit)
            self.assertNotIn(":127.0.0.1:8793 ", unit)
        sshd = files["sshd-marketcow-sdk.conf"]
        self.assertIn("GatewayPorts no", sshd)
        self.assertIn("MaxSessions 0", sshd)

    def test_only_public_market_routes_no_legacy_or_trading_relays(self):
        caddy = self.files()["marketcow-public.caddy"]
        self.assertIn("response_header_timeout 150s", caddy)
        self.assertIn("flush_interval -1", caddy)
        self.assertIn("respond 403", caddy)
        for unwanted in ("official-book", "/execution", "/rest", "evidence/", "log {", "encode ", "Authorization "):
            self.assertNotIn(unwanted, caddy)

    def test_configuration_injection_and_root_user_rejected(self):
        for kw in ({"domain": "evil\nadmin off"}, {"ssh_user": "root"},
                   {"identity": "/path -o ProxyCommand=evil"},
                   {"known_hosts": "/private/../known_hosts"}, {"local_port": 80}):
            with self.assertRaises(ValueError):
                self.files(**kw)


if __name__ == "__main__":
    unittest.main()
