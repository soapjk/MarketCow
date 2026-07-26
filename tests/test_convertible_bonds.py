import unittest

from marketcow.convertible_bonds import (
    SCORER_FIELDS,
    ConvertibleBondCatalog,
    build_market_snapshot,
    market_view,
    normalize_rating,
    percentile,
    records_from_tushare,
)


OBSERVED = "2026-07-26T13:20:00+00:00"


def fixture_records():
    basic = []
    issues = []
    stocks = []
    specifications = [
        ("123272.SZ", "中汽转债", "301215.SZ", "中汽股份", "中汽研汽车试验场股份有限公司", 5.99, "20260630"),
        ("118070.SH", "南芯转债", "688484.SH", "南芯科技", "上海南芯半导体科技股份有限公司", 43.84, "20260710"),
        ("113052.SH", "兴业转债", "601166.SH", "兴业银行", "兴业银行股份有限公司", 23.51, "20210114"),
        ("110059.SH", "浦发转债", "600000.SH", "浦发银行", "上海浦东发展银行股份有限公司", 13.74, "20191115"),
        ("127012.SZ", "招路转债", "001965.SZ", "招商公路", "招商局公路网络科技控股股份有限公司", 8.66, "20190430"),
    ]
    for index, (bond, name, stock, stock_name, issuer, conv, list_date) in enumerate(
        specifications
    ):
        basic.append({
            "ts_code": bond, "cb_code": bond.split(".")[0],
            "bond_short_name": name, "bond_full_name": name + "可转换公司债券",
            "stk_code": stock, "stk_short_name": stock_name,
            "issue_price": 100, "par": 100, "first_conv_price": conv,
            "conv_price": conv, "maturity": 6, "maturity_date": "20320617",
            "call_clause": "赎回条款", "put_clause": "回售条款",
            "reset_clause": "下修条款", "newest_rating": "AA+ sti",
            "issue_size": 10 + index, "remain_size": 8 + index,
            "list_date": list_date,
        })
        issues.append({
            "ts_code": bond, "ann_date": "20260616",
            "res_ann_date": "20260620", "issue_price": 100,
            "issue_size": (10 + index) * 100_000_000,
            "shd_ration_price": 100,
            "shd_ration_vol": (5 + index / 10) * 1_000_000,
            "shd_ration_record_date": "20260617", "onl_date": "20260618",
            "shd_ration_pay_date": "20260620", "onl_name": name,
        })
        stocks.append({
            "ts_code": stock, "name": stock_name, "fullname": issuer,
            "list_status": "L",
        })
    return records_from_tushare(basic, issues, stocks, OBSERVED)


class ConvertibleBondCatalogTest(unittest.TestCase):
    def setUp(self):
        self.catalog = ConvertibleBondCatalog(records=fixture_records())

    def test_production_catalog_requires_a_source(self):
        catalog = ConvertibleBondCatalog()
        result = catalog.search("123272")
        self.assertEqual(result["catalog_status"], "data_missing")
        self.assertIn("not configured", result["catalog_error"])

    def test_search_normalizes_name_alias_and_all_code_forms(self):
        for query in ("中汽转债", "123272", "123272.SZ", "123272.XSHE"):
            with self.subTest(query=query):
                result = self.catalog.search(query)
                self.assertEqual(result["catalog_size"], 5)
                self.assertEqual(result["items"][0]["bond_id"], "123272.XSHE")
                self.assertEqual(
                    result["items"][0]["underlying_instrument_id"], "301215.XSHE"
                )
                self.assertEqual(
                    result["items"][0]["issuer"],
                    "中汽研汽车试验场股份有限公司",
                )

    def test_empty_search_does_not_claim_nonexistence(self):
        result = self.catalog.search("不存在的转债")
        self.assertEqual(result["items"], [])
        self.assertIn("does not prove", result["empty_result_semantics"])

    def test_terms_have_independent_provenance_and_missing_semantics(self):
        result = self.catalog.get("123272")
        self.assertEqual(result["facts"]["initial_conversion_price"]["value"], 5.99)
        self.assertEqual(result["facts"]["audit_opinion"]["status"], "data_missing")
        self.assertEqual(
            result["facts"]["issue_size_billion"]["published_at"], "2026-06-16"
        )
        self.assertEqual(result["facts"]["issue_size_billion"]["value"], 10)
        self.assertEqual(result["facts"]["shareholder_placement_pct"]["value"], 50)
        self.assertIsNone(result["facts"]["listing_date"]["published_at"])
        self.assertFalse(result["facts"]["listing_date"]["point_in_time"])
        for fact in result["facts"].values():
            for field in (
                "source", "source_url", "observed_at", "published_at",
                "ingested_at", "quality_status", "cache_status",
            ):
                self.assertIn(field, fact)

    def test_point_in_time_filters_each_fact_instead_of_record(self):
        result = self.catalog.get("123272", "2026-06-18T23:59:59+08:00")
        self.assertEqual(result["status"], "available")
        self.assertEqual(result["facts"]["issue_size_billion"]["value"], 10.0)
        self.assertEqual(result["facts"]["listing_date"]["status"], "data_missing")
        self.assertEqual(
            result["facts"]["listing_date"]["missing_reason"],
            "point_in_time_unavailable",
        )
        self.assertEqual(result["facts"]["winning_date"]["status"], "data_missing")
        self.assertEqual(
            result["facts"]["winning_date"]["missing_reason"],
            "not_published_as_of_cutoff",
        )

    def test_rating_normalization_preserves_raw_and_machine_map_is_complete(self):
        result = self.catalog.get("南芯转债")
        rating = result["facts"]["bond_rating"]
        self.assertEqual(rating["value"], "AA+")
        self.assertEqual(rating["raw_value"], "AA+ sti")
        self.assertEqual(normalize_rating("AA+ sti"), "AA+")
        scorer_map = result["scorer_input_map"]
        self.assertEqual(set(scorer_map), set(SCORER_FIELDS))
        self.assertEqual(scorer_map["credit_rating"]["value"], "AA+")
        self.assertEqual(scorer_map["stock_quality_score"]["missing_reason"],
                         "scorer_judgment_required")
        self.assertEqual(
            scorer_map["expected_market_premium_pct"]["source_path"],
            "get_convertible_bond_market.comparable_premium_median_pct",
        )
        self.assertEqual(
            scorer_map["cb_valuation_percentile"]["missing_reason"],
            "historical_market_valuation_percentile_not_available",
        )

    def test_broad_same_date_sample_produces_percentiles(self):
        bond_rows = []
        stock_rows = []
        for index, record in enumerate(self.catalog.records):
            bond_rows.append({
                "ts_code": record["provider_code"], "trade_date": "20260724",
                "close": 110 + index * 5,
            })
            stock_code = record["underlying_instrument_id"].replace(
                ".XSHG", ".SH"
            ).replace(".XSHE", ".SZ")
            stock_rows.append({
                "ts_code": stock_code, "trade_date": "20260724",
                "close": record["facts"]["latest_conversion_price"]["value"] * 1.1,
            })
        snapshot = build_market_snapshot(
            self.catalog, bond_rows, stock_rows, OBSERVED
        )
        result = market_view(self.catalog.get("123272"), snapshot)
        self.assertEqual(result["cross_sectional_percentile_status"], "available")
        self.assertEqual(result["sample_size"], 4)
        self.assertAlmostEqual(result["conversion_value"], 110)
        self.assertAlmostEqual(
            result["comparable_premium_median_pct"], 11.363636, places=6
        )
        self.assertEqual(result["comparable_premium_statistic_status"], "available")
        self.assertEqual(len(result["sample_bond_ids"]), result["sample_size"])
        self.assertEqual(percentile([115, 120, 125, 130], 110), 0)
        self.assertIn("same-date", result["comparable_method"])
        self.assertEqual(
            result["historical_market_valuation_percentile_status"],
            "data_missing",
        )
        self.assertEqual(
            result["scorer_input_map"]["cb_valuation_percentile"]["missing_reason"],
            "historical_market_valuation_percentile_not_available",
        )

    def test_unlisted_bond_uses_comparable_median_not_own_premium(self):
        stock_rows = []
        bond_rows = []
        for index, record in enumerate(self.catalog.records):
            if record["bond_id"] != "123272.XSHE":
                bond_rows.append({
                    "ts_code": record["provider_code"], "trade_date": "20260724",
                    "close": 115 + index * 5,
                })
            stock_rows.append({
                "ts_code": record["underlying_instrument_id"]
                .replace(".XSHG", ".SH").replace(".XSHE", ".SZ"),
                "trade_date": "20260724",
                "close": record["facts"]["latest_conversion_price"]["value"] * 1.1,
            })
        result = market_view(
            self.catalog.get("123272"),
            build_market_snapshot(self.catalog, bond_rows, stock_rows, OBSERVED),
        )
        self.assertIsNone(result["bond_quote"])
        self.assertIsNone(result["conversion_premium_pct"])
        self.assertEqual(result["comparable_premium_statistic_status"], "available")
        self.assertIsNotNone(result["comparable_premium_median_pct"])
        expected = result["scorer_input_map"]["expected_market_premium_pct"]
        self.assertEqual(expected["status"], "available")
        self.assertEqual(expected["value"], result["comparable_premium_median_pct"])
        self.assertEqual(result["cross_sectional_percentile_status"], "data_missing")


if __name__ == "__main__":
    unittest.main()
