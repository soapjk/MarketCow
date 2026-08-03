# Polymarket free full-market live data

Status: local candidate. Contract: `marketcow.prediction_market.v1` with live schema
`marketcow.polymarket.live.v1`.

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
It detects cursor loops, fails on an incomplete `max_pages` traversal, and retries
HTTP 429/5xx with bounded `Retry-After`/exponential backoff.

`GammaLiveNormalizer` publishes reversible event, market, condition, outcome token,
and `POLY:{condition_id}:{token_id}` instrument identities. A content revision covers
each metadata payload. Binary complement and standard negative-risk relations are
explicit, versioned, and source-backed; all markets sharing a Gamma negative-risk ID
receive the complete relation member set. New-market/resolution WebSocket events are
catalog invalidations: the collector refreshes Gamma and dynamically updates token
subscriptions. It does not infer relations from title or slug.

Every complete Gamma traversal is stored as immutable canonical JSON under its
SHA-256. The live catalog records the raw path/hash and refuses restart when either the
canonical catalog revision or raw source evidence has been altered.

Tick size, minimum order size, lifecycle, accepting-orders state, and the source fee
form are retained: either explicit legacy maker/taker bps or current
rate/exponent/rebate/taker-only fields. Officially unspecified fee rounding remains
`UNSPECIFIED / informational_only`. Missing rule or fee facts are represented by
completeness flags and make the Tradude market frame fail closed.

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

Bootstrap contains the complete canonical catalog, catalog revision, active token list,
current cursor, sequence semantics, and recovery contract. Snapshot contains one
`MarketFrame` per requested market. Each frame includes both binary token books and all
standard negative-risk relation token books. It is `fail_closed` when a member is
missing, stale, skewed beyond the configured bound, affected by an unresolved gap, or
has incomplete rules/fees.

Events are ordered by a process-independent monotonically increasing cursor and contain
canonical and raw payloads plus separate hashes. A retained cursor resumes exactly;
HTTP 409 `polymarket_live_resume_cursor_expired` requires a fresh bootstrap and snapshot.
Checkpoint is content-addressed and restart recovery replays every complete canonical
book event after the checkpoint. Health reports catalog/token/book coverage, ready frame
count, lag, gap count, and latest cursor.

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
  --root /absolute/local/path/prediction-markets/polymarket-live
```

Use `--catalog-only` to validate discovery or `--bootstrap-only` to stop after complete
REST recovery. Default WebSocket shards contain 500 tokens. Every socket immediately
sends its subscription with `custom_feature_enabled=true`, sends `PING` every ten
seconds, handles `PONG`, and supports official dynamic subscribe/unsubscribe messages.

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
- `/books` missing one active token: recovery remains failed and frames remain closed.
- WebSocket disconnect: record coverage gap and require a new epoch/full recovery.
- Rate limit or transient upstream failure: bounded retry/backoff; never synthesize.
- Cursor outside retention: HTTP 409 and full consumer resynchronization.
- Corrupt checkpoint/public fact file: explicit integrity error; never serve silently.
- Missing/ambiguous fee or relation metadata: frame remains `fail_closed`.
