from __future__ import annotations

import threading
from datetime import date, timedelta
from typing import Any, Dict, List, Optional


class BaoStockProvider:
    name = "baostock"
    _lock = threading.RLock()

    @staticmethod
    def provider_code(symbol: str) -> str:
        code = "".join(ch for ch in str(symbol) if ch.isdigit()).zfill(6)
        prefix = "sh" if code.startswith(("5", "6", "9")) else "bj" if code.startswith(("4", "8")) else "sz"
        return prefix + "." + code

    @staticmethod
    def _rows(result: Any) -> List[Dict[str, Any]]:
        if result.error_code != "0":
            raise RuntimeError("BaoStock query failed: {0} {1}".format(result.error_code, result.error_msg))
        rows: List[Dict[str, Any]] = []
        while result.next():
            rows.append(dict(zip(result.fields, result.get_row_data())))
        return rows

    def _session(self):
        import baostock as bs

        login = bs.login()
        if login.error_code != "0":
            raise RuntimeError("BaoStock login failed: {0} {1}".format(login.error_code, login.error_msg))
        return bs

    def fetch_valuation(self, symbol: str, trade_date: str = "") -> Dict[str, Any]:
        with self._lock:
            bs = self._session()
            try:
                end = date.fromisoformat(trade_date) if trade_date else date.today()
                start = end - timedelta(days=10)
                result = bs.query_history_k_data_plus(
                    self.provider_code(symbol),
                    "date,code,close,tradestatus,peTTM,pbMRQ,psTTM,pcfNcfTTM,isST",
                    start_date=start.isoformat(),
                    end_date=end.isoformat(),
                    frequency="d",
                    adjustflag="3",
                )
                rows = self._rows(result)
                if not rows:
                    raise RuntimeError("BaoStock returned no recent valuation row")
                return rows[-1]
            finally:
                bs.logout()

    def fetch_financials(self, symbol: str, report_period: str) -> Dict[str, Any]:
        report_period = "".join(ch for ch in str(report_period) if ch.isdigit())
        year = int(report_period[:4])
        quarter = {"0331": 1, "0630": 2, "0930": 3, "1231": 4}[report_period[4:]]
        code = self.provider_code(symbol)
        with self._lock:
            bs = self._session()
            try:
                queries = {
                    "profit": bs.query_profit_data,
                    "operation": bs.query_operation_data,
                    "growth": bs.query_growth_data,
                    "balance": bs.query_balance_data,
                    "cashflow": bs.query_cash_flow_data,
                    "dupont": bs.query_dupont_data,
                }
                payload: Dict[str, Any] = {}
                for name, query in queries.items():
                    rows = self._rows(query(code=code, year=year, quarter=quarter))
                    payload[name] = rows[0] if rows else {}
                return payload
            finally:
                bs.logout()


def optional_float(value: Any, scale: float = 1.0) -> Optional[float]:
    if value in (None, "", "-"):
        return None
    try:
        return float(value) * scale
    except (TypeError, ValueError):
        return None
