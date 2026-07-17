import tempfile
import unittest
from pathlib import Path

import pandas as pd

from marketcow.providers.tdx_financial import TdxFinancialProvider


class TdxFinancialProviderTest(unittest.TestCase):
    def test_normalizes_curated_fields_and_units(self):
        columns = [
            "report_date", "财报公告日期", "净资产收益率", "基本每股收益",
            "营业总收入(万元)", "营业总收入TTM(万元)",
            "归属于母公司所有者的净利润", "经营活动产生的现金流量净额",
            "购建固定资产、无形资产和其他长期资产支付的现金", "资产总计", "负债合计",
        ]
        frame = pd.DataFrame(
            [[20260331, 260428, 3.48, 0.5, 453372.0, 1746838.5, 425925053.0, -370479249.0, 423321056.0, 25655250653.0, 12296285682.0]],
            index=pd.Index(["600298"], name="code"),
            columns=columns,
        )
        with tempfile.TemporaryDirectory() as tmp:
            provider = TdxFinancialProvider(Path(tmp))
            rows = provider.normalize(frame, "gpcw20260331.zip")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["symbol"], "600298")
        self.assertEqual(rows[0]["published_at"], "2026-04-28")
        self.assertEqual(rows[0]["revenue"], 4_533_720_000.0)
        self.assertEqual(rows[0]["capex"], 423_321_056.0)


if __name__ == "__main__":
    unittest.main()
