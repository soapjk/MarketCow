import unittest
from datetime import date

from marketcow.normalize import instrument_id, latest_broad_report_period, normalize_report_period


class NormalizeTest(unittest.TestCase):
    def test_instrument_ids(self):
        self.assertEqual(instrument_id("600298"), "600298.XSHG")
        self.assertEqual(instrument_id("000001"), "000001.XSHE")
        self.assertEqual(instrument_id("920992"), "920992.XBSE")

    def test_latest_broad_period(self):
        self.assertEqual(latest_broad_report_period(date(2026, 7, 15)), "20260331")
        self.assertEqual(latest_broad_report_period(date(2026, 9, 1)), "20260630")
        self.assertEqual(latest_broad_report_period(date(2026, 2, 1)), "20250930")

    def test_report_period_validation(self):
        self.assertEqual(normalize_report_period("2026-03-31"), "20260331")
        with self.assertRaises(ValueError):
            normalize_report_period("20260430")


if __name__ == "__main__":
    unittest.main()
