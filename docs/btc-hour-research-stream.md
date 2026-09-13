# BTC hour isolated research stream

`btc_research_stream` is a direct Rust Polymarket WebSocket capture for one to
three already-reviewed markets. It does not use or mutate the formal Discovery
1000 or Live 250 scopes, and it has no account, order, or activation API.

Build and run locally:

```text
cargo build --release -p marketcowd --example btc_research_stream
target/release/examples/btc_research_stream /absolute/package /absolute/new-output
```

The config schema is
`marketcow.btc-hour.rust-research-stream-config.v1`. It requires:

- `markets`: 1–3 entries with exact `market_id`, 32-byte `condition_id`, two
  decimal token IDs, an absolute reviewed `market-evidence.v1` file path, and
  its file SHA-256. Startup re-reads the bounded file and binds all identities;
- `seconds`: 1–1800;
- `maximum_batches`, `maximum_frames`, `maximum_total_bytes`, and
  `maximum_batch_bytes`;
- `maximum_pending_batches` and `maximum_pending_bytes` for the independent
  persistence queue.

The official upstream transport is shared Rust code. It has an 8 MiB wire-frame
limit, bounded reconnect attempts, heartbeat handling, deterministic token
subscription, and emits a token-local `source_gap` for every connection
boundary. One transport batch may wait before the application moves it to the
separately count-and-byte bounded persistence queue. Disk writes are performed
by the persistence task and are not performed in the transport callback.

Output is a new immutable directory containing the exact config, `frames.jsonl`,
and `report.json`. Each row records the application receive time and parsed raw
source payload. The current shared transport parses JSON before this boundary,
so `raw_wire_bytes_preserved=false` is explicit; the archive must not be called
byte-identical WebSocket wire evidence. A complete observation window is not an
execution-ready order book and does not establish historical coverage.

Initial production resource proposal (not active): three markets/six tokens,
30 minutes, 250,000 frames, 512 MiB total output, 8 MiB batch, eight queued
batches, and 64 MiB queued encoded bytes. Run it as an independent process/root;
do not add its tokens to the shared 1000/250 pools. Formal deployment or starting
the new upstream subscription remains a separate explicit operation.

Local verification:

```text
cargo test --offline -p marketcowd --example btc_research_stream --no-default-features
```

The tests cover exact identity validation, duplicate token rejection, queue
resource bounds, and the parsed-payload/raw-wire distinction. They do not open a
network connection.

## Reviewable 30-minute startup package

Generate a new package immediately before capture. This calls the existing Rust
market-evidence read route exactly three times (current UTC hour and the next two),
retains each full rule response, verifies the Binance finalized 1H BTC/USDT rule,
and writes a content-bound Rust config. It does not start a subscription.

```text
python -m marketcow.btc_research_package --endpoint http://192.168.124.3:8793 \
  --output /ABSOLUTE/NEW/PACKAGE
target/release/examples/btc_research_stream \
  /ABSOLUTE/NEW/PACKAGE /ABSOLUTE/NEW/CAPTURE
```

The generated config fixes the observation at 1,800 seconds, at most three
markets/six tokens, 250,000 application frames and 512 MiB of encoded archive
data. Persistence is a separate 8-batch/64-MiB bounded queue. A budget breach
exits nonzero and produces no `complete` report. This is an application archive
bound, not a hard bound on TLS/WebSocket headers or heartbeat traffic; a NIC-byte
cap requires OS accounting and is not claimed.

The Rust executable does not accept a bare config. Before any WebSocket is
opened it requires `manifest.json`, requires its source endpoint to be exactly
`http://192.168.124.3:8793`, binds the manifest to the config and all evidence,
review and binding files, checks ordered Up/Down identities and exact three-hour
windows, and rejects packages more than five minutes old. A partial package whose
manifest publication failed is therefore not startable.

Renewal is explicit: after the process ends, generate a fresh package, review
the new current/next-two identities, and use a new output directory. Evidence
and config hashes prevent silent identity reuse. There is no hidden daemon.

## Offline L2 projection

```text
python -m marketcow.btc_research_l2 --capture /ABSOLUTE/CAPTURE \
  --output /ABSOLUTE/NEW/L2
```

Rows bind capture config/report/archive hashes, exact identities, input line,
book epoch and local sequence. `source_gap` invalidates that token; subsequent
changes are rejected until a new full `book`, so epochs are never joined across
a disconnect. A market is two-sided only while both Up and Down states are valid.
The venue feed exposes no authoritative per-book sequence here and the transport
archives parsed JSON, not original WebSocket bytes. `local_sequence` is therefore
capture order within an epoch, and neither output is described as wire-exact.
