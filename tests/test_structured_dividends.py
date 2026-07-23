from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import Mock

from marketcow.providers.structured_dividends import (
    CnStructuredDividendProvider,
    LongPortDividendProvider,
    TushareDividendProvider,
    UsStructuredDividendProvider,
)


class StructuredDividendProviderTest(unittest.TestCase):
    def test_tushare_maps_report_period_and_payment_fields(self):
        provider = Mock()
        provider.name = "tushare"
        provider.base_url = "https://example.invalid"
        provider.call.return_value = {"payload": "fixture"}
        provider.rows.return_value = [{
            "ts_code": "600519.SH",
            "end_date": "20251231",
            "ann_date": "20260403",
            "cash_div_tax": "27.6",
            "pay_date": "20260626",
            "record_date": "20260625",
            "div_proc": "实施",
        }]

        rows = TushareDividendProvider(provider).fetch("600519.SH", 2026)

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["fiscal_year"], 2026)
        self.assertEqual(rows[0]["announcement_date"], "2026-04-03")
        self.assertEqual(rows[0]["expected_payment_date"], "2026-06-26")
        self.assertEqual(rows[0]["record_date"], "2026-06-25")
        self.assertIsNone(rows[0]["ex_date"])
        self.assertEqual(rows[0]["payment_date"], "2026-06-26")
        self.assertEqual(rows[0]["confirmation_status"], "unverified")

    def test_tushare_uses_fund_div_for_cn_etf_payment_history(self):
        provider = Mock()
        provider.name = "tushare"
        provider.base_url = "https://example.invalid"
        provider.call.return_value = {"payload": "fixture"}
        provider.rows.return_value = [{
            "ts_code": "159583.SZ",
            "ann_date": "20251016",
            "imp_anndate": "20251016",
            "base_date": "20251020",
            "record_date": "20251020",
            "ex_date": "20251021",
            "pay_date": "20251022",
            "div_cash": "0.15",
            "base_unit": "10",
            "div_proc": "实施",
        }]

        rows = TushareDividendProvider(provider).fetch("159583.SZ", 2025)

        self.assertEqual(rows[0]["amount_per_share"], "0.015")
        self.assertEqual(rows[0]["record_date"], "2025-10-20")
        self.assertEqual(rows[0]["payment_date"], "2025-10-22")
        self.assertEqual(provider.call.call_args.args[0], "fund_div")

    def test_longport_maps_hk_payment_year_without_pdf(self):
        item = SimpleNamespace(
            desc="Cash dividend 5.3 HKD",
            ex_date="05/15/2026",
            record_date="05/18/2026",
            payment_date="06/01/2026",
            id="615061",
        )
        context = SimpleNamespace(
            dividend=Mock(return_value=SimpleNamespace(list=[item])),
            dividend_detail=Mock(return_value=SimpleNamespace(list=[])),
        )
        provider = LongPortDividendProvider(
            "key", "secret", "token", context_factory=lambda: context
        )

        rows = provider.fetch("0700.HK", 2026)

        context.dividend.assert_called_once_with("700.HK")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["symbol"], "00700.HK")
        self.assertEqual(rows[0]["amount_per_share"], "5.3")
        self.assertEqual(rows[0]["expected_payment_date"], "2026-06-01")
        self.assertEqual(rows[0]["record_date"], "2026-05-18")
        self.assertEqual(rows[0]["ex_date"], "2026-05-15")
        self.assertEqual(rows[0]["payment_date"], "2026-06-01")
        self.assertEqual(rows[0]["payload"]["fiscal_year_basis"], "payment_year")

    def test_longport_combines_detail_records_missing_from_history(self):
        detail = SimpleNamespace(
            desc="Cash Dividend: \n28.02423CNY",
            ex_date="2026-06-26",
            record_date="2026-06-25",
            payment_date="2026-06-26",
            id="",
        )
        context = SimpleNamespace(
            dividend=Mock(return_value=SimpleNamespace(list=[])),
            dividend_detail=Mock(return_value=SimpleNamespace(list=[detail])),
        )
        provider = LongPortDividendProvider(
            "key", "secret", "token", context_factory=lambda: context
        )

        rows = provider.fetch("600519.SH", 2026)

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["amount_per_share"], "28.02423")
        self.assertEqual(rows[0]["record_date"], "2026-06-25")

    def test_longport_supports_us_etf_history_dates(self):
        item = SimpleNamespace(
            desc="Cash dividend 0.52 USD",
            ex_date="03/20/2025",
            record_date="03/20/2025",
            payment_date="03/27/2025",
            id="soxx-2025-q1",
        )
        context = SimpleNamespace(
            dividend=Mock(return_value=SimpleNamespace(list=[item])),
            dividend_detail=Mock(return_value=SimpleNamespace(list=[])),
        )
        provider = LongPortDividendProvider(
            "key", "secret", "token", context_factory=lambda: context
        )

        rows = provider.fetch("SOXX", 2025)

        context.dividend.assert_called_once_with("SOXX.US")
        self.assertEqual(rows[0]["record_date"], "2025-03-20")
        self.assertEqual(rows[0]["ex_date"], "2025-03-20")
        self.assertEqual(rows[0]["payment_date"], "2025-03-27")

    def test_longport_distinguishes_unparseable_payload_from_true_empty(self):
        item = SimpleNamespace(
            desc="unexpected upstream schema",
            ex_date="03/20/2025",
            record_date="03/20/2025",
            payment_date="03/27/2025",
            id="changed-schema",
        )
        context = SimpleNamespace(
            dividend=Mock(return_value=SimpleNamespace(list=[item])),
            dividend_detail=Mock(return_value=SimpleNamespace(list=[])),
        )
        provider = LongPortDividendProvider(
            "key", "secret", "token", context_factory=lambda: context
        )

        with self.assertRaisesRegex(ValueError, "parse produced no usable events"):
            provider.fetch("SOXX", 2025)

    def test_longport_retries_rate_limit_under_global_request_lock(self):
        item = SimpleNamespace(
            desc="Cash dividend 0.25 USD",
            ex_date="03/20/2025",
            record_date="03/20/2025",
            payment_date="03/27/2025",
            id="event",
        )
        context = SimpleNamespace(
            dividend=Mock(side_effect=[
                RuntimeError("429002 api request is limited"),
                SimpleNamespace(list=[item]),
            ]),
            dividend_detail=Mock(return_value=SimpleNamespace(list=[])),
        )
        provider = LongPortDividendProvider(
            "key", "secret", "token", context_factory=lambda: context,
            min_interval_seconds=0, max_attempts=2,
        )

        rows = provider.fetch("MCD", 2025)

        self.assertEqual(len(rows), 1)
        self.assertEqual(context.dividend.call_count, 2)

    def test_cn_provider_falls_back_to_longport_when_tushare_is_empty(self):
        tushare = SimpleNamespace(fetch=Mock(return_value=[]))
        longport = SimpleNamespace(fetch=Mock(return_value=[{"source": "longport"}]))

        rows = CnStructuredDividendProvider(tushare, longport).fetch(
            "600519.SH", 2026
        )

        self.assertEqual(rows, [{"source": "longport"}])
        longport.fetch.assert_called_once_with("600519.SH", 2026)

    def test_us_provider_prefers_longport_and_falls_back_to_sec(self):
        longport = SimpleNamespace(
            configured=True, fetch=Mock(return_value=[])
        )
        sec = SimpleNamespace(fetch=Mock(return_value=[{"source": "sec"}]))

        rows = UsStructuredDividendProvider(longport, sec).fetch("MCD", 2025)

        self.assertEqual(rows, [{"source": "sec"}])
        longport.fetch.assert_called_once_with("MCD", 2025)
        sec.fetch.assert_called_once_with("MCD", 2025)

    def test_us_provider_does_not_hide_primary_failure_as_sec_empty(self):
        error = RuntimeError("429002 api request is limited")
        longport = SimpleNamespace(
            configured=True, fetch=Mock(side_effect=error)
        )
        sec = SimpleNamespace(fetch=Mock(return_value=[]))

        with self.assertRaisesRegex(RuntimeError, "429002"):
            UsStructuredDividendProvider(longport, sec).fetch("MCD", 2025)


if __name__ == "__main__":
    unittest.main()
