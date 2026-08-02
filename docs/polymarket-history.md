# Polymarket free historical data

Status: local candidate. Contract: `marketcow.prediction_market.v1`.

This subsystem implements MC-PM-HIST-001 through MC-PM-HIST-006. MarketCow owns
acquisition, raw retention, identity, lifecycle revisions, replay, reconciliation,
certification, and immutable publication. Tradude reads only certified manifests and
their Parquet parts.

## Source policy

Allowed sources are deliberately closed:

- official Gamma, CLOB, Data API, and public market WebSocket;
- official Polymarket subgraph endpoints and public Polygon logs;
- fixed-revision, hash-pinned Hugging Face datasets with an approved license:
  `kinzikdza/polymarket-updown-microstructure`,
  `Alezanello/polymarket-arena-capture`,
  `moose-code/polymarket-onchain-v1`, and
  `od2961/polymarket-full-market-dataset`.

PMData, Dome, PolymarketData, paid sources, and trial quotas are rejected by policy.
The dataset adapter requires a 40-character commit, a URL containing that commit, an
approved license, and a SHA-256. A moving `main` revision cannot be acquired.

Official price history and on-chain fills are truth sources for prices/trades and
reconciliation. They are never expanded, interpolated, or presented as historical L2.

## Local-first acquisition

`LocalFirstRawCache.get()` hashes the complete source request. If both the immutable
payload and its manifest exist, MarketCow verifies the payload hash and returns it
without touching the network. On a miss it invokes the injected free-source adapter,
checks required coverage and any expected hash, then fsyncs and atomically renames the
payload and manifest.

```python
request = SourceRequest(
    dataset_key="kinzikdza/polymarket-updown-microstructure",
    source="huggingface_fixed_revision",
    source_url="https://huggingface.co/datasets/.../resolve/<40-char-commit>/part.parquet",
    revision="<40-char-commit>",
    license="apache-2.0",
    expected_sha256="<64 hex characters>",
)
cached = LocalFirstRawCache(root / "raw-cache").get(
    request, FixedRevisionDatasetAdapter()
)
```

Missing license/revision/hash, a prohibited host, a hash mismatch, or insufficient
coverage fails explicitly. No empty or synthetic part is substituted.

## Canonical identity and lifecycle

`GammaCatalogNormalizer` archives Gamma raw responses through the cache boundary and
emits:

- reversible event, market, condition, token, outcome, and instrument identities;
- exactly two outcome tokens for a certifiable binary market;
- negative-risk relation when applicable;
- point-in-time lifecycle state, resolution evidence, tick, minimum size, and fees;
- source revision, raw path, observed/ingested time, and payload hash.

Missing tick/minimum size or an indeterminate fee schedule is a contract failure.

`TradeTruthNormalizer` produces decimal-safe Data API trades and Polygon/subgraph
truth events (`fill`, `split`, `merge`, `convert`, `redemption`, `resolution`). JSON
adapters parse numbers as strings so price and size never pass through binary float.

## WebSocket recorder and recovery

`PolymarketWebSocketRecorder` writes every public market-channel message as fsynced
append-only JSONL with exchange and receive timestamps, raw payload, normalized event,
and SHA-256. It maintains per-token books and content-addressed checkpoints.

It handles:

- duplicate event IDs idempotently;
- sequence gaps and out-of-order events through an explicit gap ledger;
- missing snapshots without applying deltas;
- reconnect only through fresh full snapshots;
- tick-size changes and zero-size level deletion;
- invalid/crossed/unticked books as retained but rejected raw messages;
- optional expected state hashes for snapshot/delta consistency checks.

`PolymarketWebSocketCollector` connects only to the public market endpoint, subscribes
to both canonical token IDs, and invokes a caller-supplied official snapshot fetcher
after reconnect before accepting a new delta epoch.

## Immutable Parquet and certification

`PredictionMarketMaterializer` writes Zstandard-compressed Parquet with an explicit
schema. Decimal price/size remain strings; every row also retains canonical raw JSON
and its hash. Part names are content-addressed and never overwritten. The draft
manifest records source revisions, identity, parts, coverage, and the complete gap
ledger.

`PredictionMarketCertifier` publishes only when all gates pass:

1. exactly two unique outcome tokens;
2. both books observed, non-crossed, nonnegative, and tick aligned;
3. no unresolved snapshot/delta hash mismatch;
4. every official trade key has matching on-chain truth;
5. coverage bounds exist and no unresolved gap remains.

A failed gate creates a `rejected` manifest with evidence. It cannot be published.
Certification creates a new content-addressed manifest and attestation; it never
mutates the draft.

## Tradude read contract

Only `PublishedPredictionMarketStore.publish(certified_manifest)` changes the local
published pointer. The HTTP read boundary is:

```http
GET /v1/prediction-markets/polymarket/datasets/{dataset_id}/manifest
GET /v1/prediction-markets/polymarket/datasets/{dataset_id}/bootstrap
GET /v1/prediction-markets/polymarket/datasets/{dataset_id}/parts/{table}
```

`table` is one of `catalog`, `lifecycle`, `books`, `trades`, or `onchain`. Manifest
responses conform to the OpenAPI `PredictionMarketManifest` schema. Parts use media
type `application/vnd.apache.parquet`. Each read re-verifies the part SHA-256 and
refuses drafts, missing tables, path escape, or modified files.

The bootstrap response is certified-only and is bound by both `dataset_id` and
`manifest_id`. It carries typed, reversible event/market/condition/token/outcome/
instrument identities plus the fields required to construct a Nautilus
`BinaryOption`: title/question, USDC.e settlement currency, activation and
expiration, price and size increments, minimum order size, lifecycle/resolution,
structural relations, rule facts, and a complete versioned fee schedule. Missing
fee or rule evidence makes the bootstrap invalid; a missing or mismatched bootstrap
makes certification fail.

Fee rounding is an enum, not prose. `ROUND_DOWN`, `ROUND_HALF_EVEN`, and
`ROUND_HALF_UP` carry matching tie semantics and may be used for executable PnL;
`UNSPECIFIED` must be paired with `calculation_status=informational_only` and makes
fee calculation fail closed. The pinned free sample uses `UNSPECIFIED`: official
fee documentation defines the five-decimal quantum, while the fixed-revision public
FeeModule accepts an operator-selected integer fee and does not expose the
operator's decimal tie-breaking implementation.

Book rows expose `record_type`, `book_epoch`, deterministic `sequence`, optional
`source_sequence`, absolute-size update semantics, exchange and receive timestamps,
tick version, and state checksum. `replay.mode=snapshot_only` explicitly means no
delta, cancellation, or queue-position semantics may be inferred. Consumers order
rows by `(exchange_at, received_at, book_epoch, sequence, record_id)`.

For book rows, `payload_json` is the provider-independent
`marketcow.polymarket.book.v1` payload. A full snapshot contains `record_id`,
`token_id`, `snapshot_type=full`, epoch and sequence fields, exchange/receive
timestamps, tick version and tick size, absolute-size semantics, and `bids`/`asks`.
Every level encodes `price` and `size` as JSON strings; a JSON float anywhere in the
normalized payload makes certification fail. Provider input is retained separately
as `raw_payload_json` plus `raw_payload_sha256`, and never replaces the public
payload.

The `state_checksum` algorithm is SHA-256 over UTF-8 canonical JSON (object keys
sorted lexicographically, no insignificant whitespace) of exactly this object:

```json
{
  "token_id": "...",
  "tick_size": "0.01",
  "bids": [{"price": "0.54", "size": "96.73"}],
  "asks": [{"price": "0.5500", "size": "106.87"}]
}
```

Bids are sorted by numeric price descending and asks ascending before hashing.
Duplicate prices, non-string decimals, invalid ticks, crossed books, payload/hash
mismatches, or top-level/payload identity mismatches fail certification. Consumers
can use `marketcow.polymarket_history.book_state_checksum` as the reference
implementation without parsing any provider-specific field.

Example:

```sh
curl -fsS \
  http://127.0.0.1:8790/v1/prediction-markets/polymarket/datasets/sample/manifest
curl -fsS -o books.parquet \
  http://127.0.0.1:8790/v1/prediction-markets/polymarket/datasets/sample/parts/books
```

## Operating notes

- Storage root: `<MARKETCOW_HOME>/prediction-markets/polymarket`.
- Run collectors under a supervised local process and preserve the raw/checkpoint/gap
  folders together.
- Pin dataset revisions and licenses in reviewed local configuration. Credentials or
  paid API keys are neither required nor accepted by these adapters.
- Do not publish when the requested historical window exceeds source coverage.
- A production dataset must meet the design document's sample gate (at least 20
  resolved binary markets and a complete lifecycle/24-hour window). The code does not
  waive this gate when no real pinned dataset configuration is supplied.

## Pinned free Nautilus sample

`scripts/materialize_polymarket_sample.py` builds the first local certified sample
from `kinzikdza/polymarket-updown-microstructure` at immutable Hugging Face revision
`eb4e9fc794c059dd9bef69c98eb4d34e70a5bd83` under CC BY 4.0. The three source
Parquet SHA-256 values are pinned in `marketcow.polymarket_sample`; MarketCow refuses
to read a mismatch. The builder also stores and hashes one official CLOB market
response per condition and a local hash-pinned copy of the official fee contract.

```bash
uv run python scripts/materialize_polymarket_sample.py \
  --source-root /path/to/pinned/source \
  --store-root /path/to/immutable/store \
  --dataset-id polymarket-updown-nautilus-sample-eb4e9fc
```

The upstream capture provides sampled books, not deltas. Source snapshots which are
crossed or violate the declared tick are never repaired: immutable raw files remain
the evidence, excluded counts enter a resolved gap ledger, and only valid snapshots
are materialized for the declared snapshot-only replay. The sample has complete
activation, expiration, and resolution facts for each included market; it does not
claim a continuous 24-hour L2 window.
