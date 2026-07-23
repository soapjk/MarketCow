import unittest

from marketcow.providers.cn_dividends import parse_cn_implementation_announcement


class CnDividendParserTest(unittest.TestCase):
    def test_implementation_announcement_is_confirmed(self):
        rows = parse_cn_implementation_announcement(
            "2025 年度权益分派实施公告 本次方案为每 10 股派发现金红利 3.00 元。"
            "股权登记日：2026年5月26日 除权除息日：2026年5月27日 "
            "现金红利发放日：2026年5月28日",
            "002568.SZ", "2026-05-21", "https://www.szse.cn/a.pdf",
            "2026-042", "Shenzhen Stock Exchange",
        )
        self.assertEqual(rows[0]["amount_per_share"], "0.30")
        self.assertEqual(rows[0]["expected_payment_date"], "2026-05-28")
        self.assertEqual(rows[0]["record_date"], "2026-05-26")
        self.assertEqual(rows[0]["ex_date"], "2026-05-27")
        self.assertEqual(rows[0]["payment_date"], "2026-05-28")
        self.assertEqual(rows[0]["fiscal_year"], 2025)

    def test_proposal_or_missing_payment_date_is_not_confirmed(self):
        self.assertEqual(parse_cn_implementation_announcement(
            "2025年度利润分配预案 每股派发现金红利1元",
            "600519.SH", "2026-03-01", "https://www.sse.com.cn/a.pdf",
            "x", "Shanghai Stock Exchange",
        ), [])


if __name__ == "__main__":
    unittest.main()
