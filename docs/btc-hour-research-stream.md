# BTC hour isolated research stream

`btc_research_stream` is a direct Rust Polymarket WebSocket capture for one to
three already-reviewed markets. It does not use or mutate the formal Discovery
1000 or Live 250 scopes, and it has no account, order, or activation API.

Build and run locally:

```text
cargo build --release -p marketcowd --example btc_research_stream
target/release/examples/btc_research_stream /absolute/config.json /absolute/new-output
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
