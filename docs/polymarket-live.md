# Polymarket free full-market live data

Status: local candidate. Contract: `marketcow.prediction_market.v1` with live schema
`marketcow.polymarket.live.v2`.

## Source and trust boundary

MarketCow uses only these public, free Polymarket interfaces:

- Gamma `GET /markets/keyset` for complete active catalog discovery;
- CLOB V2 public `POST /books` for full-book bootstrap and recovery;
- the unauthenticated CLOB market WebSocket for `book`, `price_change`,
  `best_bid_ask`, `last_trade_price`, `tick_size_change`, `new_market`, and
  `market_resolved`;
- public Data API trades, activity, positions, and holders; and
- reviewed official contracts/subgraphs or public Polygon facts when on-chain
  reconciliation is needed.

The implementation contains no PMData, Dome, PolymarketData, paid RPC, trial quota,
or synthetic/interpolated L2 path. Market and Data APIs require no credentials. The
collector does not connect to the authenticated user channel and cannot observe
unfilled-order owners or private orders.

Official contract references:

- <https://docs.polymarket.com/api-reference/markets/list-markets-keyset-pagination>
- <https://docs.polymarket.com/market-data/websocket/overview>
- <https://docs.polymarket.com/market-data/websocket/market-channel>
- <https://docs.polymarket.com/api-reference/market-data/get-order-books-request-body>
- <https://docs.polymarket.com/api-reference/core/get-trades-for-a-user-or-markets>
- <https://docs.polymarket.com/api-reference/core/get-user-activity>
- <https://docs.polymarket.com/api-reference/core/get-positions-for-a-market>
- <https://docs.polymarket.com/api-reference/core/get-top-holders-for-markets>

## Catalog and lifecycle

`GammaKeysetCatalog` uses `/markets/keyset`, a maximum page size of 100, and only
the opaque `next_cursor` as the next `after_cursor`. It never supplies an offset.
There is no static page-count ceiling: only a response without `next_cursor` marks the
snapshot complete. It detects cursor loops, unchanged cursors, repeated page content,
duplicate market IDs, empty nonterminal pages, and malformed responses. HTTP 429/5xx
uses bounded per-page `Retry-After`/exponential backoff, so retries on an early page do
not consume the budget of a later page.

The default client owns one persistent `requests.Session`, reusing TLS connections for
the entire traversal. Raw rows stream to a temporary canonical JSONL snapshot while a
temporary SQLite uniqueness ledger tracks cursors, page hashes, and market IDs. Fetch
memory is therefore bounded by one server page rather than total catalog size. Every
25 pages (configurable) the collector logs pages, accumulated market count, elapsed
seconds, retry count, and current cursor; final evidence also includes bytes and raw
SHA-256.

Normalization begins only after the server terminates the cursor. The complete JSONL
snapshot is content-addressed and atomically copied to raw evidence; the normalized
catalog itself is streamed to immutable content-addressed JSONL. The small atomic
`catalog.json` manifest binds its path, row count, SHA-256, catalog revision, and raw
source evidence. Restart verifies both JSONL hashes and rebuilds typed markets one row
at a time, avoiding a second giant parsed raw catalog. Catalog revision, source
hash/count/format, token map, and the catalog-revision event are swapped only after all
validation succeeds. An exception or process restart during pagination sees the
previous complete revision, never a mixture with the temporary spool.
The revision event carries added/removed token counts and content hashes plus
`requires_bootstrap=true`, not a hundreds-of-thousands-element token array; consumers
reload the atomic bootstrap while the collector computes its subscription diff locally.

`GammaLiveNormalizer` publishes reversible event, market, condition, outcome token,
and `POLY:{condition_id}:{token_id}` instrument identities. A content revision covers
each metadata payload. The breaking `live.v2` shape embeds a versioned
`LiveInstrumentFacts` object with settlement currency, activation/expiration, price
increment, size increment, minimum order size, lifecycle/accepting state, and the
source revision for every fact. Current `pUSD` is tied to the official collateral
documentation (modified 2026-04-17) and `0.01` size increment to the official CLOB SDK
rounding config pinned at commit `b076b04d61135657e25dccc1bbd6866a96bd8c6e`;
Gamma supplies the per-market time, tick, and minimum-size fields. A missing source
field is published in `missing_fields`, never replaced by a business default, and
causes `instrument_facts_incomplete`.

Binary complement and standard negative-risk relations are explicit, versioned, and
source-backed. A standard negative-risk relation's `members` is only the
mutually-exclusive YES instrument set. `outcome_pairs` separately maps every explicit
Gamma event-outcome label to its market/condition and YES/NO token/instrument pair.
The consumer never parses a title, slug, or unconstrained outcome string. All markets
sharing a Gamma negative-risk ID receive the complete pair set from one full keyset
traversal. Missing labels, ambiguous YES/NO sides, or partial group expansion sets
`negative_risk_relation_incomplete`. New-market/resolution WebSocket events are
catalog invalidations: the collector refreshes Gamma and dynamically updates token
subscriptions. It does not infer relations from title or slug.

Every complete Gamma traversal is stored as immutable canonical JSON under its
SHA-256. The live catalog records the raw path/hash and refuses restart when either the
canonical catalog revision or raw source evidence has been altered.

The fee contract is a typed `LiveFeeSchedule`: schedule ID/version, currency,
maker/taker rates, formula, exponent, quantum, rounding mode, tie semantics,
calculation status, effective interval, and source provenance. MarketCow does not
conflate current `pUSD` settlement collateral with the fee denomination: the official
fee page states `USDC`, a zero maker rate, `C × feeRate × p × (1-p)`, and a
`0.00001` quantum. A Gamma/CLOB per-token rate in bps is converted exactly by dividing
by 10,000 and retains its source revision. Officially unspecified fee tie-breaking
remains `UNSPECIFIED / unspecified / informational_only`; it is not zero and cannot be
used for executable PnL. Missing per-market rate or schedule facts are named in
`missing_fields` and cause `fee_schedule_incomplete`.

## Book semantics and recovery

Polymarket does not expose one continuous exchange sequence on the public market
channel. MarketCow therefore declares `sequence_semantics=deterministic_normalized`.
The sequence is the applied receive order within one `book_epoch`; it is not presented
as an exchange sequence.

On startup and every reconnect:

1. open a recovery gap and a new recovery ID;
2. fetch full snapshots for every active token through batched `POST /books`;
3. require every requested token to be present;
4. validate decimal strings, tick alignment, non-crossed books, and nonnegative size;
5. start a new content-addressed `book_epoch`; and
6. resolve the gap only after all token snapshots are installed and checkpointed.

`POST /books` uses one persistent HTTP session, at most 500 token IDs per request,
bounded per-batch 429/5xx retry/backoff, and progress evidence every 25 batches. The
evidence reports requested token count, received books, elapsed time, retries, and
batch coverage. A response traversal may finish, but recovery is not marked complete
unless every active token appears; partial coverage therefore remains fail closed.

`price_change.size` is an absolute level size. Zero removes the level. A full `book`
replaces the state. MarketCow never manufactures a cancel, delta, or queue position.
Duplicate payload hashes are ignored idempotently; older exchange timestamps,
crossed/locked books, unticked prices, missing snapshots, and invalid tick changes are
ledgered and not applied. Each applied event carries the complete canonical book,
provider-independent decimal-string levels, canonical/raw SHA-256 hashes, receive and
exchange times, epoch, sequence, tick version, and state checksum.

The state checksum is SHA-256 of canonical JSON (sorted keys, no insignificant
whitespace) over:

```json
{
  "asks": [{"price": "0.42", "size": "11"}],
  "bids": [{"price": "0.40", "size": "10"}],
  "tick_size": "0.01",
  "token_id": "..."
}
```

Bids sort numerically descending and asks ascending. Price and size are always JSON
strings in the normalized payload.

## Tradude read contract

All endpoints are local MarketCow reads and appear in OpenAPI:

```text
GET /v1/prediction-markets/polymarket/live/bootstrap
GET /v1/prediction-markets/polymarket/live/snapshot?market_id=m1&market_id=m2
GET /v1/prediction-markets/polymarket/live/events?after_cursor=123&limit=1000
GET /v1/prediction-markets/polymarket/live/checkpoint
GET /v1/prediction-markets/polymarket/live/health
GET /v1/prediction-markets/polymarket/live/gaps?unresolved_only=true
GET /v1/prediction-markets/polymarket/live/public-data/{kind}
```

Bootstrap contains the complete canonical catalog, typed instrument/fee facts,
relations/pairs, catalog revision, active token list, current cursor, sequence
semantics, and recovery contract. Snapshot contains one `MarketFrame` per requested
market. Every frame returns exactly the two books for its binary complete set. A
standard negative-risk frame additionally returns every YES-member book and the exact
pair metadata needed to verify its corresponding NO instrument. Frame revisions bind
the instrument facts and fee schedule.

Stable fail-closed reasons include `missing_outcome_book`,
`instrument_facts_incomplete`, `fee_schedule_incomplete`, `unresolved_gap`,
`token_frame_skew`, `stale_book`, `negative_risk_relation_incomplete`,
`negative_risk_member_missing`, `negative_risk_frame_skew`,
`negative_risk_member_stale`, and `negative_risk_member_gap`.
Group-wide metadata failures additionally use `negative_risk_member_catalog_missing`,
`negative_risk_member_instrument_facts_incomplete`, and
`negative_risk_member_fee_schedule_incomplete`.

Events are ordered by a process-independent monotonically increasing cursor and contain
canonical and raw payloads plus separate hashes. A retained cursor resumes exactly;
HTTP 409 `polymarket_live_resume_cursor_expired` requires a fresh bootstrap and snapshot.
Checkpoint is content-addressed and restart recovery replays every complete canonical
book event after the checkpoint. Health reports catalog/token/book coverage, ready frame
count, lag, gap count, and latest cursor.

### Producer/consumer synchronization

The collector is the single writer. The FastAPI process is a read-side durable tailer;
both must use the exact same local root:

```text
<MarketCow storage_root>/prediction-markets/polymarket-live
```

Before every bootstrap, snapshot, events, checkpoint, health, or gaps response, FastAPI
checks the atomic catalog/checkpoint identities and tails only complete newly fsynced
JSONL records. A checkpoint change triggers deterministic rebuild from that checkpoint
plus its verified successor events. Thus a collector that writes after FastAPI startup
becomes visible without process restart, shared memory, or polling an upstream source.
Multiple concurrent collector writers are not supported.

Every event ID covers the entire envelope, including applied/failure semantics and gap
facts. Recovery separately recomputes canonical and raw payload hashes, enforces a
continuous cursor, and cross-checks checkpoint books/unresolved gaps against event-log
replay. Invalid, missing-snapshot, out-of-order, duplicate, and recovery gaps therefore
survive process restarts. A truncated, reordered, or hash/identity-tampered log produces
`polymarket_live_integrity_failed` rather than a partially updated API view.

## Public wallet facts

`DataApiPublicNormalizer` preserves wallet, condition, token, outcome, side, decimal
price/size/value fields, timestamp, transaction hash, and public profile provenance.
Raw provider payload and hash remain attached. The semantic boundary is explicit:
these rows are public trade/activity/position/holder facts, not owners of unfilled
orders and not verified real-world identities. Binary floats in normalized financial
fields are rejected. Each fact is an envelope with separately hashed
`canonical_payload` and `raw_payload`; consumers never need provider field names.

## Running locally

The command below is intentionally separate from the MarketCow web process. It changes
only the supplied local storage directory.

```bash
PYTHONPATH=src .venv/bin/python scripts/run_polymarket_live.py \
  --root '<MarketCow storage_root>/prediction-markets/polymarket-live' \
  --catalog-progress-pages 25
```

Use `--catalog-only` to validate discovery or `--bootstrap-only` to stop after complete
REST recovery. Every REST `/books` request and WebSocket subscription message is
bounded to 500 tokens. Up to 32 WebSocket connections are used by default; all shards
are distributed evenly across those connections and additional shards use the official
dynamic subscribe operation. This bounds connection count without dropping catalog
coverage. Every socket sends `custom_feature_enabled=true`, sends `PING` every ten
seconds, handles `PONG`, and supports dynamic subscribe/unsubscribe messages. Both the
message size and connection ceiling are configurable locally.

On 2026-08-04, a local read-only traversal of the real `closed=false` keyset completed
only after 1,270 pages and 126,981 markets. It took 700.262 seconds with zero retries,
reused one HTTP session, and produced a 903,205,293-byte canonical JSONL snapshot with
SHA-256 `12642d93b5f8d774715431a8e3dc0ab151fa12ea84f5f50ca7925173efbe8bed`.
The spool was iterated to the same 126,981 count and then deleted without publication.
At the two-token upper bound (253,962 tokens), planning uses 508 bounded REST/WS
messages and 32 balanced WebSocket connection groups.

No production service is started or restarted by this script. Operators should size
file retention and disk monitoring before running an indefinite capture.

Public wallet fact pages are captured explicitly with endpoint-specific parameters;
for example, a market holder page can be captured without credentials as follows:

```bash
PYTHONPATH=src .venv/bin/python scripts/capture_polymarket_public_data.py \
  --root /absolute/local/path/prediction-markets/polymarket-live \
  --kind holders \
  --params-json '{"market":["0x..."],"limit":20}'
```

## Operational failure policy

- Gamma pagination incomplete or cursor loop: no catalog publication.
- Gamma repeated/no-progress page or duplicate market ID: no catalog publication.
- `/books` missing one active token: recovery remains failed and frames remain closed.
- WebSocket disconnect: record coverage gap and require a new epoch/full recovery.
- Rate limit or transient upstream failure: bounded retry/backoff; never synthesize.
- Cursor outside retention: HTTP 409 and full consumer resynchronization.
- Durable tail/hash/cursor failure: HTTP 409; do not serve a partially replayed state.
- Corrupt checkpoint/public fact file: explicit integrity error; never serve silently.
- Missing/ambiguous fee or relation metadata: frame remains `fail_closed`.

## Provider-neutral consumer fixture

`tests/fixtures/polymarket-live-provider-neutral-v2.json` covers one ordinary binary
market and one three-outcome standard negative-risk group across bootstrap, current
snapshot, event resume, checkpoint, and expired-cursor recovery. It deliberately
contains no Gamma/CLOB field names. Provider evidence remains available separately in
the live event raw payload/hash fields.
