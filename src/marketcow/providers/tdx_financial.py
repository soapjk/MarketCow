from __future__ import annotations

import math
import re
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

import pandas as pd


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _number(value: Any, scale: float = 1.0) -> Optional[float]:
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(number) or math.isinf(number):
        return None
    return number * scale


class TdxFinancialProvider:
    name = "tdx_financial_via_mootdx"

    def __init__(self, download_dir: Path):
        self.download_dir = Path(download_dir)
        self.download_dir.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def list_files() -> List[Dict[str, Any]]:
        from mootdx.affair import Affair

        files = Affair.files()
        valid: List[Dict[str, Any]] = []
        today_key = int(date.today().strftime("%Y%m%d"))
        for item in files:
            filename = str(item.get("filename") or "")
            match = re.fullmatch(r"gpcw(\d{8})\.zip", filename)
            size = int(item.get("filesize") or 0)
            if match and size > 1000 and int(match.group(1)) <= today_key:
                valid.append({**item, "report_period": match.group(1)})
        return sorted(valid, key=lambda item: item["report_period"], reverse=True)

    def fetch_and_parse(self, filename: str) -> pd.DataFrame:
        from mootdx.affair import Affair

        path = self.download_dir / filename
        if not path.exists() or path.stat().st_size <= 1000:
            Affair.fetch(downdir=str(self.download_dir), filename=filename)
        frame = Affair.parse(downdir=str(self.download_dir), filename=filename)
        if frame is None or frame.empty:
            raise RuntimeError("Mootdx parsed no rows from {0}".format(filename))
        return frame

    @staticmethod
    def _column_value(row: pd.Series, columns: Sequence[Any], name: str) -> Any:
        for position, column in enumerate(columns):
            if str(column) != name:
                continue
            value = row.iloc[position]
            try:
                if pd.isna(value):
                    continue
            except Exception:
                pass
            return value
        return None

    def normalize(self, frame: pd.DataFrame, filename: str) -> List[Dict[str, Any]]:
        match = re.search(r"(\d{8})", filename)
        if not match:
            raise ValueError("cannot infer report period from {0}".format(filename))
        report_period = match.group(1)
        rows: List[Dict[str, Any]] = []
        fetched_at = _utc_now()
        for position in range(len(frame)):
            row = frame.iloc[position]
            symbol = str(frame.index[position]).zfill(6)
            if len(symbol) != 6 or not symbol.isdigit():
                continue
            value = lambda name: self._column_value(row, frame.columns, name)
            publish_raw = _number(value("财报公告日期"))
            published_at = None
            if publish_raw:
                text = str(int(publish_raw))
                if len(text) == 6:
                    century = "19" if int(text[:2]) >= 80 else "20"
                    text = century + text
                if len(text) == 8:
                    published_at = "{0}-{1}-{2}".format(text[:4], text[4:6], text[6:8])
            rows.append(
                {
                    "symbol": symbol,
                    "report_period": report_period,
                    "published_at": published_at,
                    "roe_weighted": _number(value("净资产收益率")),
                    "eps": _number(value("基本每股收益")),
                    "eps_adjusted": _number(value("扣除非经常性损益每股收益")),
                    "book_value_per_share": _number(value("每股净资产")),
                    "ocf_per_share": _number(value("每股经营现金流量")),
                    "cash": _number(value("货币资金")),
                    "accounts_receivable": _number(value("应收账款")),
                    "inventory": _number(value("存货")),
                    "total_assets": _number(value("资产总计")),
                    "total_liabilities": _number(value("负债合计")),
                    "total_equity": _number(value("所有者权益（或股东权益）合计")),
                    "revenue": _number(value("营业总收入(万元)"), 10000.0),
                    "revenue_ttm": _number(value("营业总收入TTM(万元)"), 10000.0),
                    "net_profit_parent": _number(value("归属于母公司所有者的净利润")),
                    "net_profit_parent_ttm": _number(value("近一年归母净利润（万元）"), 10000.0),
                    "operating_cashflow": _number(value("经营活动产生的现金流量净额")),
                    "capex": _number(value("购建固定资产、无形资产和其他长期资产支付的现金")),
                    "source_file": filename,
                    "fetched_at": fetched_at,
                }
            )
        # Some TDX packages contain duplicate codes. Keep the most complete row
        # so the canonical (symbol, report_period) key remains deterministic.
        best: Dict[str, Dict[str, Any]] = {}
        for item in rows:
            score = sum(value is not None for value in item.values())
            previous = best.get(item["symbol"])
            previous_score = sum(value is not None for value in previous.values()) if previous else -1
            if score > previous_score:
                best[item["symbol"]] = item
        return [best[symbol] for symbol in sorted(best)]
