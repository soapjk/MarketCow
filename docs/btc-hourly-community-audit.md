# Historical L2 source audit — 2026-09-10

Read-only documentation review; no unknown code executed, paid subscription or
identity submission. None of these sources has yet supplied a verified pair of
the selected September 7 hourly Up/Down token books.

| Source | Actual published claim | Decision / missing evidence |
|---|---|---|
| [pmxt archive](https://archive.pmxt.dev/Polymarket) | Landing page explicitly grants CC BY 4.0, with attribution to pmxt; hourly Parquet and v1/v2 directories | First candidate; v2 directory fetch timed out in this review. Target-day/token coverage and continuity remain unverified; no large archive downloaded |
| [Rocklabs](https://github.com/rocklabs-io/polymarket-dataset) | Raw WS book/price-change JSONL, receive timestamps, hour partitions; research/academic access by contacting operator | Not an anonymous ready download. No affiliation or user details submitted; license/access terms must be satisfied before fetching |
| [ibold](https://github.com/ibold-dev/polymarket-orderbook-history) | Repository reachable | No license match in retrieved page; cannot infer permission or full target L2 coverage; raw sample still needed |
| [Pancake](https://github.com/usepancake/polymarket-history) | Trades explicitly synthetic last-price observations; quotes/book_l2 absent pending license review | Exclude as executable L2/trade evidence. Its resolution table would require independent identity/finality verification |
| [LuciferForge](https://github.com/LuciferForge/polymarket-historical-data) | March–June 2026, 15-minute prices, mostly placeholder top-of-book; explicitly no resolution labels | Does not meet September hourly L2/settlement requirement; no purchase |

[Official SDK issue 216](https://github.com/Polymarket/py-clob-client/issues/216)
reports resolved-market price-history granularity problems. It is an issue report,
not proof all current resolved price requests fail; price history is not L2 anyway.

[Project-authored Reddit announcement](https://www.reddit.com/r/DataHoarder/comments/1rdhx3j/pmxt_is_opensourcing_a_terabyte_sized_dataset_of/)
provides the pmxt archive lead, not independent assurance of every token's coverage.
Its all-platform daily size is not a budget to download wholesale.

Next bounded examination must first list day/hour file sizes and schema/license,
then select only a file or supported filtered query fitting the 64MiB/file and
256MiB aggregate research budget. If the smallest indivisible file exceeds this,
report the precise limit and request a different export; do not silently fetch TBs.
Captured book deltas without a trustworthy starting snapshot or gap evidence are
partial observations, not guaranteed reconstructable historical depth.

## Bounded pmxt v2 sample — 2026-09-12 follow-up

The current v2 index and data overview were re-read before downloading. The
publisher documents one Parquet object per UTC hour, sourced from the official
Polymarket market-channel WebSocket, with `book`, `price_change`,
`last_trade_price`, and `tick_size_change` events. It also states that v2 starts
on 2026-04-13, and that files are event-stream archives rather than independently
verified reconstructable snapshots.

Direct HEAD requests for the target settlement hour and its adjacent hours,
`2026-09-10T05`, `T06`, and `T07`, returned 404 from both the v1 and v2 object
hosts. No body was downloaded and absence is reported as an archive availability
gap, not as proof that the market had no activity.

The smallest nearby listed v2 object fitting the agreed 64 MiB bound was
`polymarket_orderbook_2026-09-09T15.parquet`: 24,037,341 bytes, ETag
`8df62f76c748a16eb8d2294710b7b916-3`, file SHA-256
`4c6c322a68b22cb3638fe0ea7c9ef8c576bdc86e388ab61951fe7b7560a61270`.
It contains 2,930,050 rows, 16 columns and 3 row groups. Its Arrow schema matches
the published current v2 shape. Predicate reads for both tokens of market
4358214 returned zero rows. This does not establish target-token continuity:
the stream is event-driven, and zero matching rows in one hour cannot distinguish
no change, missing subscription, or missing coverage. Therefore no historical L2
sample is promoted into the paired dataset, and executable historical PnL remains
unavailable.

The retained local sample is
`/Volumes/T9/data/marketcow/research/btc-hourly/community/pmxt-v2-20260909T15-r1/`.

Nautilus local checkout uses LGPL-3.0 notices. The adapter wrapper imports the
installed library without copying/modifying its source. Redistribution packaging
and dependency-lock audit remains outstanding; this note is not legal clearance.
