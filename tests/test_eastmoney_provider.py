import unittest

from marketcow.providers.eastmoney import EastmoneySpotProvider


class FakeEastmoneyProvider(EastmoneySpotProvider):
    def _fetch_page(self, page):
        if page == 1:
            return {
                "data": {
                    "total": 2,
                    "diff": [
                        {"f12": "600298", "f14": "安琪酵母", "f2": 33.0, "f9": 20.5, "f23": 2.8, "f20": 100.0, "f21": 90.0, "f3": 1.2},
                        {"f12": "000001", "f14": "平安银行", "f2": 10.0, "f9": 5.5, "f23": 0.6, "f20": 200.0, "f21": 200.0, "f3": -0.2},
                    ],
                }
            }
        return {"data": {"total": 2, "diff": []}}


class EastmoneyProviderTest(unittest.TestCase):
    def test_fetch_all_maps_fields(self):
        rows = FakeEastmoneyProvider().fetch_all()
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["symbol"], "600298")
        self.assertEqual(rows[0]["pe_dynamic"], 20.5)
        self.assertEqual(rows[0]["pb"], 2.8)


if __name__ == "__main__":
    unittest.main()
