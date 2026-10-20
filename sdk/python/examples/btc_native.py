"""Explicit, bounded native-SDK examples; importing this file performs no I/O."""

import argparse
import asyncio
import os

from marketcow_sdk import AsyncClient, Source


async def run(args: argparse.Namespace) -> None:
    async with AsyncClient(args.endpoint, api_key=os.environ.get("MARKETCOW_API_KEY")) as client:
        if args.mode == "history":
            if args.start_ms is None:
                raise ValueError("history requires --start-ms; no automatic history download")
            async with client.history.trades(
                start_time_ms=args.start_ms, end_time_ms=args.end_ms
            ) as pages:
                async for page in pages:
                    print(
                        "segment",
                        page.segment_index,
                        "page",
                        page.page_index,
                        "rows",
                        len(page.trades),
                        "segment_complete",
                        page.segment_complete,
                        "query_complete",
                        page.complete,
                        "database",
                        page.database_state,
                    )
            return

        stream = (
            client.prices.subscribe(Source.BINANCE_BTCUSDT)
            if args.mode == "prices"
            else client.markets.subscribe_current()
        )
        async with stream:
            count = 0
            async for event in stream:
                print(event.kind, "available", event.available)
                if args.mode == "prices" and event.kind == "trade":
                    print(event.trade.trade_id, event.trade.trade_time_ms, event.trade.price)
                elif args.mode == "market" and event.kind == "snapshot":
                    # Explicit stable view of the CURRENT owner, not an as-of
                    # reconstruction of a previously dequeued event sequence.
                    view = await stream.snapshot(depth=5)
                    if view is not None:
                        print(view.market_id, view.state_sequence, view.source_status)
                        for book in view.books:
                            print(book.outcome, book.bids, book.asks, "truncated", book.truncated)
                count += 1
                if count >= args.events:
                    break


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--endpoint", required=True)
    parser.add_argument("--mode", choices=("market", "prices", "history"), default="market")
    parser.add_argument("--events", type=int, default=10)
    parser.add_argument("--start-ms", type=int)
    parser.add_argument("--end-ms", type=int)
    options = parser.parse_args()
    if options.events <= 0:
        parser.error("--events must be positive")
    asyncio.run(run(options))
