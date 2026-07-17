import unittest
from unittest.mock import Mock

from marketcow.providers.instrument_search import InstrumentSearchProvider


class InstrumentSearchProviderTest(unittest.TestCase):
    def test_finds_meituan_and_filters_hk_derivatives(self):
        response = Mock()
        response.raise_for_status.return_value = None
        response.json.return_value = {"QuotationCodeTable": {"Data": [
            {"Code": "83690", "Name": "美团-WR", "Classify": "HK", "JYS": "HK", "TypeUS": "3"},
            {"Code": "03690", "Name": "美团-W", "Classify": "HK", "JYS": "HK", "TypeUS": "3"},
            {"Code": "13002", "Name": "美团认购证", "Classify": "HK", "JYS": "HK", "TypeUS": "6"},
        ]}}
        provider = InstrumentSearchProvider()
        provider.session.get = Mock(return_value=response)

        items = provider.search("美团")

        self.assertEqual([item["symbol"] for item in items], ["3690.HK"])
        self.assertEqual(items[0]["name"], "美团-W")
        self.assertEqual(items[0]["currency"], "HKD")

    def test_includes_shanghai_etf(self):
        response = Mock()
        response.raise_for_status.return_value = None
        response.json.return_value = {"QuotationCodeTable": {"Data": [
            {"Code": "513180", "Name": "恒生科技ETF华夏", "Classify": "Fund", "JYS": "9", "TypeUS": "9"},
        ]}}
        provider = InstrumentSearchProvider()
        provider.session.get = Mock(return_value=response)

        items = provider.search("513180.HK")

        self.assertEqual(items[0]["symbol"], "513180.SH")
        self.assertEqual(items[0]["currency"], "CNY")
        self.assertEqual(provider.session.get.call_args.kwargs["params"]["input"], "513180")


if __name__ == "__main__":
    unittest.main()
