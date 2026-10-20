from datetime import datetime
import importlib.util
import json
from pathlib import Path
import sys
import tempfile
import unittest
from zoneinfo import ZoneInfo


SCRIPT = Path(__file__).parents[1] / "scripts/u1_external_traffic_monitor.py"
SPEC = importlib.util.spec_from_file_location("u1_traffic", SCRIPT)
traffic = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
sys.modules[SPEC.name] = traffic
SPEC.loader.exec_module(traffic)


def snapshot(ingress_v4=0, egress_v4=0, ingress_v6=0, egress_v6=0):
    return traffic.Snapshot(ingress_v4, egress_v4, ingress_v6, egress_v6)


class TrafficMonitorTests(unittest.TestCase):
    def test_rules_classify_only_remote_endpoint_and_never_drop(self):
        rules = traffic.nft_ruleset()
        self.assertIn("ip saddr != {", rules)
        self.assertIn("ip daddr != {", rules)
        self.assertIn("ip6 saddr != {", rules)
        self.assertIn("ip6 daddr != {", rules)
        self.assertIn("10.0.0.0/8", rules)
        self.assertIn("192.168.0.0/16", rules)
        self.assertIn("fc00::/7", rules)
        self.assertIn("fe80::/10", rules)
        self.assertIn("ff00::/8", rules)
        self.assertEqual(rules.count("policy accept"), 2)
        self.assertNotIn(" drop", rules)
        self.assertNotIn(" reject", rules)

    def test_parse_nft_snapshot(self):
        entries = [
            {"counter": {"family": "inet", "table": traffic.TABLE,
                         "name": name, "packets": 1, "bytes": index}}
            for index, name in enumerate(traffic.COUNTERS, 10)
        ]
        actual = traffic.parse_nft_json(json.dumps({"nftables": entries}))
        self.assertEqual(actual, snapshot(10, 11, 12, 13))
        self.assertEqual(actual.ingress, 22)
        self.assertEqual(actual.egress, 24)

    def test_increment_and_counter_recreation(self):
        state = traffic.update_state(None, snapshot(100, 200, 300, 400), "2026-10-16")
        state = traffic.update_state(state, snapshot(110, 220, 330, 440), "2026-10-16")
        self.assertEqual((state.ingress_bytes, state.egress_bytes), (40, 60))
        # All kernel counters were recreated. Existing daily totals remain and
        # only bytes since recreation are added.
        state = traffic.update_state(state, snapshot(3, 5, 7, 11), "2026-10-16")
        self.assertEqual((state.ingress_bytes, state.egress_bytes), (50, 76))

    def test_rollover_starts_new_day_without_cross_day_delta(self):
        state = traffic.update_state(None, snapshot(100, 200), "2026-10-15")
        state = traffic.update_state(state, snapshot(150, 250), "2026-10-15")
        state.notified = True
        state.notification_attempts = 1
        state = traffic.update_state(state, snapshot(175, 275), "2026-10-16")
        self.assertEqual(state.day, "2026-10-16")
        self.assertEqual((state.ingress_bytes, state.egress_bytes), (0, 0))
        self.assertFalse(state.notified)
        self.assertEqual(state.notification_attempts, 0)

    def test_threshold_notifies_once(self):
        calls = []
        now = datetime(2026, 10, 16, 12, tzinfo=ZoneInfo("Asia/Shanghai"))
        with tempfile.TemporaryDirectory() as directory:
            state_path = Path(directory) / "state.json"
            traffic.sample_once(state_path, 100, calls.append, now=now,
                                snapshot=snapshot(1000, 1000))
            state = traffic.sample_once(state_path, 100, calls.append, now=now,
                                        snapshot=snapshot(1060, 1050))
            self.assertTrue(state.notified)
            self.assertEqual(len(calls), 1)
            state = traffic.sample_once(state_path, 100, calls.append, now=now,
                                        snapshot=snapshot(2000, 2000))
            self.assertEqual(len(calls), 1)
            self.assertEqual(state.notification_attempts, 1)

    def test_failed_notification_is_retried_without_losing_usage(self):
        attempts = []

        def failing(message):
            attempts.append(message)
            raise RuntimeError("offline")

        now = datetime(2026, 10, 16, 12, tzinfo=ZoneInfo("Asia/Shanghai"))
        with tempfile.TemporaryDirectory() as directory:
            state_path = Path(directory) / "state.json"
            traffic.sample_once(state_path, 100, failing, now=now,
                                snapshot=snapshot(1000, 1000))
            with self.assertRaisesRegex(RuntimeError, "offline"):
                traffic.sample_once(state_path, 100, failing, now=now,
                                    snapshot=snapshot(1060, 1050))
            failed = traffic.load_state(state_path)
            self.assertIsNotNone(failed)
            self.assertEqual(failed.total_bytes, 110)
            self.assertFalse(failed.notified)
            recovered = traffic.sample_once(
                state_path, 100, lambda message: attempts.append(message),
                now=now, snapshot=snapshot(1070, 1060),
            )
            self.assertEqual(recovered.total_bytes, 130)
            self.assertTrue(recovered.notified)
            self.assertEqual(recovered.notification_attempts, 2)

    def test_state_file_is_private(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "nested" / "state.json"
            state = traffic.update_state(None, snapshot(), "2026-10-16")
            traffic.save_state(path, state)
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)


if __name__ == "__main__":
    unittest.main()
