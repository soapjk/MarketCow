import unittest
from marketcow.polymarket_stream_metrics import SlowTimeline, StreamMetrics, DECODE_STAGES


class TimelineTest(unittest.TestCase):
    def test_nested_overlap_union_and_missing_coverage(self):
        t = SlowTimeline()
        t.add('json_encode', 'b', 2, 3)
        t.add('filter_convert', 'a', 1, 4)
        x = t.overlap(0, 5)
        self.assertEqual(x['union_seconds'], 3)
        self.assertEqual(x['unexplained_seconds'], 2)
        self.assertFalse(x['coverage_incomplete'])
        for i in range(2000):
            t.add('json_encode', 'a', 10+i, 11+i)
        self.assertEqual(len(t.spans), 1024)
        self.assertTrue(t.overlap(0, 5)['coverage_incomplete'])

    def test_slow_samples_bounded_no_payload(self):
        m = StreamMetrics('test')
        detail = dict.fromkeys(DECODE_STAGES, 0.0)
        detail['_trace'] = {'finish': 1, 'resume': 1.002}
        for i in range(100):
            m.record_decode(detail, {'type':'event', 'cursor':i, 'secret_payload':'not retained'})
        self.assertEqual(len(m.slow_frames), 8)
        self.assertEqual(m.slow_seen, 100)
        self.assertNotIn('secret_payload', str(m.slow_frames))


if __name__ == '__main__':
    unittest.main()
