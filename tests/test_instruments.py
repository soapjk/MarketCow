import unittest

from marketcow.instruments import canonical_instrument, external_instrument


class CanonicalInstrumentTest(unittest.TestCase):
    def test_canonical_ids_are_symbol_dot_mic(self):
        self.assertEqual(
            canonical_instrument("600519.xshg").instrument_id,
            "600519.XSHG",
        )
        self.assertEqual(
            canonical_instrument("000001.XSHE").mic,
            "XSHE",
        )
        self.assertEqual(
            canonical_instrument("700.XHKG").symbol,
            "700",
        )

    def test_provider_symbols_require_explicit_namespace(self):
        instrument = external_instrument("provider:tushare", "600519.SH")
        self.assertEqual(instrument.instrument_id, "600519.XSHG")
        self.assertEqual(
            instrument.provider_symbol("provider:tushare"),
            "600519.SH",
        )
        hk = external_instrument("provider:longport", "00700.HK")
        self.assertEqual(hk.instrument_id, "700.XHKG")
        self.assertEqual(
            hk.provider_symbol("provider:longport"),
            "700.HK",
        )

    def test_us_requires_explicit_mic_and_never_guesses(self):
        for value in ("AAPL", "AAPL.US", "BRK.B"):
            with self.subTest(value=value), self.assertRaisesRegex(
                ValueError, "SYMBOL.MIC"
            ):
                canonical_instrument(value)
        with self.assertRaisesRegex(ValueError, "explicit MIC"):
            external_instrument("broker:longport", "AAPL.US")
        resolved = external_instrument(
            "broker:longport", "AAPL.US", mic="XNAS"
        )
        self.assertEqual(resolved.instrument_id, "AAPL.XNAS")
        self.assertEqual(
            resolved.provider_symbol("broker:longport"),
            "AAPL.US",
        )

    def test_legacy_internal_formats_are_rejected(self):
        for value in (
            "CN:SSE:600519", "CN:SZSE:000001", "HK:HKEX:00700",
            "US:US:AAPL", "CN.XSHG.600519",
        ):
            with self.subTest(value=value), self.assertRaises(ValueError):
                canonical_instrument(value)


if __name__ == "__main__":
    unittest.main()
