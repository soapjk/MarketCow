import unittest
from datetime import datetime, timedelta, timezone

from marketcow.exposure_facts import ExposureFactsService, evidence


NOW = datetime(2026, 7, 23, 4, 0, tzinfo=timezone.utc)


def ev(source="exchange_master", effective="2026-07-22T20:00:00+00:00"):
    return evidence(
        source,
        source_url="https://example.test/source",
        fetched_at="2026-07-23T03:00:00+00:00",
        effective_at=effective,
        source_tier="primary",
    )


class Source:
    source_id = "exchange_master"
    source_tier = "primary"

    def __init__(self, result=None, error=None):
        self.result = result
        self.error = error
        self.calls = 0

    def fetch(self, symbol, market):
        self.calls += 1
        if self.error:
            raise self.error
        return self.result


class ExposureFactsTest(unittest.TestCase):
    def test_stock_basic_facts_and_multiple_classifications(self):
        source = Source({
            "asset_type": "equity",
            "listing_market": "US",
            "currency": "USD",
            "as_of": "2026-07-22T20:00:00+00:00",
            "classifications": [
                {"scheme": "GICS", "value": "Semiconductors", "level": "industry",
                 "evidence": ev()},
                {"scheme": "SIC", "value": "3674", "level": "industry",
                 "evidence": ev()},
            ],
            "company_materials": [{
                "kind": "business_description",
                "summary": "Designs and manufactures memory products.",
                "document_id": "issuer-10k-2025",
                "evidence": ev("sec_filing"),
            }],
            "evidence": ev(),
        })
        result = ExposureFactsService([source], clock=lambda: NOW).get("MU")
        self.assertEqual(result["symbol"], "MU")
        self.assertEqual(result["asset_type"], "equity")
        self.assertEqual(result["currency"], "USD")
        self.assertEqual(len(result["classifications"]), 2)
        self.assertEqual(result["company_materials_status"], "available")
        self.assertIn("not a complete risk exposure", result["classification_notice"])

    def test_etf_holdings_preserve_weights_and_provenance(self):
        source = Source({
            "asset_type": "etf",
            "listing_market": "US",
            "currency": "USD",
            "as_of": "2026-07-22T20:00:00+00:00",
            "classifications": [],
            "company_materials": [],
            "holdings": {
                "status": "available",
                "as_of": "2026-07-22",
                "constituents": [
                    {"symbol": "NVDA", "weight": 0.12},
                    {"symbol": "AVGO", "weight": 0.08},
                ],
                "evidence": [ev("fund_holdings")],
            },
            "evidence": ev("fund_master"),
        })
        result = ExposureFactsService([source], clock=lambda: NOW).get("SOXX")
        self.assertEqual(result["holdings"]["status"], "available")
        self.assertEqual(result["holdings"]["constituents"][0]["weight"], 0.12)
        self.assertEqual(result["classifications"], [])

    def test_etf_without_holdings_is_explicitly_unavailable(self):
        source = Source({
            "asset_type": "etf", "listing_market": "CN", "currency": "CNY",
            "as_of": "2026-07-22T07:00:00+00:00",
            "classifications": [], "company_materials": [], "evidence": ev(),
        })
        result = ExposureFactsService([source], clock=lambda: NOW).get("513180.SH")
        self.assertEqual(result["holdings"]["status"], "unavailable")
        self.assertEqual(result["holdings"]["reason"], "no_constituent_source")
        self.assertEqual(result["classifications"], [])

    def test_no_data_returns_stable_unavailable_contract(self):
        result = ExposureFactsService([Source(None)], clock=lambda: NOW).get("0700.HK")
        self.assertEqual(result["status"], "unavailable")
        self.assertEqual(result["holdings"]["status"], "unavailable")
        self.assertEqual(result["cache_status"], "empty")
        self.assertEqual(result["classifications"], [])

    def test_stale_cache_is_bounded_and_labeled(self):
        clock = [NOW]
        source = Source({
            "asset_type": "equity", "listing_market": "US", "currency": "USD",
            "as_of": "2026-07-22T20:00:00+00:00",
            "classifications": [], "company_materials": [], "evidence": ev(),
        })
        service = ExposureFactsService(
            [source], ttl_seconds=60, stale_max_seconds=3600, clock=lambda: clock[0]
        )
        service.get("MU")
        source.error = TimeoutError("upstream down")
        clock[0] += timedelta(seconds=120)
        result = service.get("MU", refresh=True)
        self.assertEqual(result["cache_status"], "stale")
        self.assertEqual(result["degradations"][0]["code"], "source_unavailable")
        self.assertNotIn("upstream down", str(result))

    def test_source_degradation_uses_lower_priority_facts(self):
        primary = Source(error=ConnectionError("primary down"))
        fallback = Source({
            "asset_type": "equity", "listing_market": "US", "currency": "USD",
            "as_of": "2026-07-22T20:00:00+00:00",
            "classifications": [], "company_materials": [],
            "evidence": ev("fallback_master"),
        })
        result = ExposureFactsService([primary, fallback], clock=lambda: NOW).get("MU")
        self.assertEqual(result["status"], "available")
        self.assertEqual(result["degradations"][0]["source_id"], "exchange_master")
        self.assertEqual(result["evidence"][0]["source_id"], "fallback_master")


if __name__ == "__main__":
    unittest.main()
