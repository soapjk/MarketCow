import unittest

from marketcow.instruments import canonical_instrument


class CanonicalInstrumentTest(unittest.TestCase):
    def test_a_share_aliases(self):
        self.assertEqual(canonical_instrument("600519.ss").symbol, "600519.SH")
        self.assertEqual(canonical_instrument("000001.sz").instrument_id, "CN:SZSE:000001")

    def test_hong_kong_codes_are_padded(self):
        self.assertEqual(canonical_instrument("700.hk").symbol, "00700.HK")
        self.assertEqual(canonical_instrument("00700").instrument_id, "HK:HKEX:00700")

    def test_us_share_class_is_canonical(self):
        self.assertEqual(canonical_instrument("brk.b").symbol, "BRK-B")


if __name__ == "__main__":
    unittest.main()
