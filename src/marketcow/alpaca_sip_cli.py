from __future__ import annotations

import argparse
import json
from datetime import date, datetime, time, timezone
from pathlib import Path
from typing import Sequence

from .providers.alpaca_sip import (
    PANEL_INSTRUMENTS,
    AlpacaBarsRequest,
    AlpacaBudgets,
    AlpacaCredentials,
    AlpacaSipProvider,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="marketcow-alpaca-sip")
    parser.add_argument("--output", required=True)
    parser.add_argument("--start", required=True, help="inclusive YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="inclusive YYYY-MM-DD")
    parser.add_argument("--timeframe", default="1Day")
    parser.add_argument(
        "--adjustments", default="raw,split,dividend,all",
        help="comma-separated Alpaca adjustments",
    )
    parser.add_argument("--asof", default="")
    parser.add_argument("--credentials-file", default="")
    parser.add_argument("--maximum-requests", type=int, default=200)
    parser.add_argument("--maximum-bytes", type=int, default=256 * 1024 * 1024)
    parser.add_argument("--maximum-retries", type=int, default=3)
    parser.add_argument("--timeout-seconds", type=float, default=15.0)
    parser.add_argument("--total-seconds", type=float, default=1800.0)
    parser.add_argument("--page-limit", type=int, default=10_000)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    start_date = date.fromisoformat(args.start)
    end_date = date.fromisoformat(args.end)
    if start_date > end_date:
        raise ValueError("start must not be after end")
    start = datetime.combine(start_date, time.min, timezone.utc)
    end = datetime.combine(end_date, time.max, timezone.utc)
    if args.timeframe != "1Day" and (end_date - start_date).days > 7:
        raise ValueError(
            "minute/hour capture is limited to seven days per run; "
            "this command will not download unbounded intraday history"
        )
    asof = date.fromisoformat(args.asof) if args.asof else end_date
    adjustments = tuple(item.strip() for item in args.adjustments.split(",") if item.strip())
    if not adjustments or len(set(adjustments)) != len(adjustments):
        raise ValueError("adjustments must be non-empty and unique")
    symbols = tuple(sorted(PANEL_INSTRUMENTS))
    budgets = AlpacaBudgets(
        maximum_requests=args.maximum_requests,
        maximum_bytes=args.maximum_bytes,
        maximum_retries=args.maximum_retries,
        timeout_seconds=args.timeout_seconds,
        total_seconds=args.total_seconds,
        page_limit=args.page_limit,
    )
    credentials = AlpacaCredentials.load(
        Path(args.credentials_file) if args.credentials_file else None
    )
    provider = AlpacaSipProvider(credentials, budgets=budgets)
    requests_ = tuple(
        AlpacaBarsRequest(
            symbols=symbols, start=start, end=end, timeframe=args.timeframe,
            adjustment=adjustment, asof=asof, sort="asc", limit=args.page_limit,
        )
        for adjustment in adjustments
    )
    manifest = provider.capture_dataset(
        Path(args.output), requests_, corporate_start=start_date,
        corporate_end=end_date, provider_neutral_symbols=PANEL_INSTRUMENTS,
    )
    print(json.dumps({
        "complete": manifest["complete"],
        "dataset_id": manifest["dataset_id"],
        "manifest": manifest["manifest"],
        "coverage": manifest["coverage"],
        "usage": manifest["usage"],
    }, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
