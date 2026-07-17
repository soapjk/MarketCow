from __future__ import annotations

from typing import Dict

import pandas as pd


class AkshareFinancialProvider:
    name = "akshare_eastmoney_financials"

    def _ak(self):
        import akshare as ak

        return ak

    def fetch_market_summaries(self, report_period: str) -> Dict[str, pd.DataFrame]:
        ak = self._ak()
        return {
            "performance": ak.stock_yjbb_em(date=report_period),
            "balance": ak.stock_zcfz_em(date=report_period),
            "income": ak.stock_lrb_em(date=report_period),
            "cashflow": ak.stock_xjll_em(date=report_period),
        }

    def fetch_company_statements(self, eastmoney_symbol: str) -> Dict[str, pd.DataFrame]:
        ak = self._ak()
        return {
            "income": ak.stock_profit_sheet_by_report_em(symbol=eastmoney_symbol),
            "balance": ak.stock_balance_sheet_by_report_em(symbol=eastmoney_symbol),
            "cashflow": ak.stock_cash_flow_sheet_by_report_em(symbol=eastmoney_symbol),
        }
