import unittest
from scripts.measure_sdk_public_latency import Matcher, QuoteAges, distribution, iso_ns
from types import SimpleNamespace


class MatcherTests(unittest.TestCase):
    def test_quote_age_does_not_refresh_on_receipt_or_confirmation(self):
        q=QuoteAges()
        m=SimpleNamespace(data_valid=True,token_id='up',quote_source_at='2026-10-20T00:00:00Z')
        event=SimpleNamespace(kind='price_change',stream_id='s',frame=SimpleNamespace(
            published_at='2026-10-20T00:00:00.010Z',price_change=SimpleNamespace(post_metadata=[m])))
        q.add('public',event,iso_ns('2026-10-20T00:00:00.020Z'))
        q.add('public',event,iso_ns('2026-10-20T00:00:00.030Z'))
        self.assertEqual(len(q.quotes['public']),1)
        self.assertEqual(len(q.published['public']),2)
        with self.assertRaises(ValueError):iso_ns('2026-10-20T00:00:00')
    def test_both_arrival_orders_preserve_signed_difference(self):
        m=Matcher()
        m.add("lan",("a",),1_000_000)
        m.add("public",("a",),3_000_000)
        m.add("public",("b",),4_000_000)
        m.add("lan",("b",),5_000_000)
        self.assertEqual(m.deltas,[2_000_000,-1_000_000])
        m.add("public",("a",),6_000_000)
        self.assertEqual(len(m.deltas),2)
        self.assertEqual(m.report()["public_minus_lan_arrival"]["negative_count"],1)

    def test_unmatched_eviction_never_invents_zero_and_memory_is_bounded(self):
        m=Matcher(maximum_pending=1,maximum_pairs=1)
        m.add("lan",("a",),1)
        m.add("lan",("b",),2)
        self.assertEqual(len(m.pending),1)
        self.assertEqual(m.report()["events"]["lan_unmatched_evicted"],1)
        self.assertEqual(distribution(m.deltas),{"count":0})
        m.add("public",("b",),3)
        m.add("lan",("c",),4)
        with self.assertRaisesRegex(RuntimeError,"measurement_pair_capacity"):
            m.add("public",("c",),5)


if __name__ == "__main__":
    unittest.main()
