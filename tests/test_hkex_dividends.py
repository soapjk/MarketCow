import unittest

from marketcow.providers.hkex_dividends import parse_hkex_dividend_form


class HkexDividendParserTest(unittest.TestCase):
    def test_standard_announcement_form(self):
        rows = parse_hkex_dividend_form(
            "Cash Dividend Announcement for Equity Issuer "
            "Announcement date 18 March 2026 Status New announcement "
            "Dividend type Final Dividend nature Ordinary "
            "For the financial year end 31 December 2025 "
            "Dividend declared HKD 4.50 per share "
            "Payment date 01 June 2026",
            "00700.HK", "https://www1.hkexnews.hk/a.pdf", "a.pdf",
        )
        self.assertEqual(rows[0]["amount_per_share"], "4.50")
        self.assertEqual(rows[0]["fiscal_year"], 2025)
        self.assertEqual(rows[0]["expected_payment_date"], "2026-06-01")

    def test_missing_payment_date_is_not_confirmed(self):
        self.assertEqual(parse_hkex_dividend_form(
            "Dividend declared HKD 4.50 per share", "00700.HK",
            "https://www1.hkexnews.hk/a.pdf", "a.pdf",
        ), [])


if __name__ == "__main__":
    unittest.main()
