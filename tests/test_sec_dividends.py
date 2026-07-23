import unittest

from marketcow.providers.sec_dividends import SecDividendProvider, parse_sec_dividend_filing


class SecDividendProviderTest(unittest.TestCase):
    def test_parser_requires_amount_and_payment_date(self):
        rows = parse_sec_dividend_filing(
            "declared a cash dividend of $0.25 per share, payable on August 8, 2026 "
            "to shareholders of record on August 1, 2026; "
            "the ex-dividend date is July 31, 2026",
            "AAPL", "2026-07-20", "https://www.sec.gov/x", "0001",
        )
        self.assertEqual(rows[0]["amount_per_share"], "0.25")
        self.assertEqual(rows[0]["expected_payment_date"], "2026-08-08")
        self.assertEqual(rows[0]["record_date"], "2026-08-01")
        self.assertEqual(rows[0]["ex_date"], "2026-07-31")
        self.assertEqual(rows[0]["payment_date"], "2026-08-08")
        self.assertEqual(parse_sec_dividend_filing(
            "cash dividend of $0.25 per share", "AAPL", "2026-07-20",
            "https://www.sec.gov/x", "0001",
        ), [])

    def test_provider_uses_configured_sec_identity(self):
        calls = []

        def get_json(url, headers):
            calls.append(headers["User-Agent"])
            if "company_tickers" in url:
                return {"0": {"ticker": "AAPL", "cik_str": 320193}}
            return {"filings": {"recent": {
                "form": ["8-K"], "filingDate": ["2026-07-20"],
                "accessionNumber": ["0000320193-26-000001"],
                "primaryDocument": ["aapl.htm"],
            }}}

        provider = SecDividendProvider(
            "MarketCow toczx@outlook.com", get_json,
            lambda _url, _headers: (
                "declared a cash dividend of $0.25 per share, "
                "payable on August 8, 2026"
            ),
        )
        self.assertEqual(len(provider.fetch("AAPL", 2026)), 1)
        self.assertTrue(all(item == "MarketCow toczx@outlook.com" for item in calls))


if __name__ == "__main__":
    unittest.main()
