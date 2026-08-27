# MarketCow Rust + Python migration: Phase 0 decisions

Status: accepted for shadow implementation, 2026-08-27.

Authoritative input: `objective-marketcow-rust-python-migration-technical-plan.md`, SHA-256
`62d1a0418eedf6dbf00c37dabce827e0cf1b4d3c8fc43536ffe3e0421d3742d4`.

## Scope and safety invariants

The target users are MarketCow operators and consumers of its HTTP, WebSocket and MCP
contracts. Rust owns the public boundary, realtime state, persistence watermarks and
control plane. Python is an untrusted, non-realtime provider executor over a local UDS.
Real-order submission remains disabled. Tradude is a consumer and never starts, stops,
restarts or rolls back MarketCow. Migration is shadow-first and preserves one writer per
domain. These invariants are release gates, not configuration conveniences.

Money, price, quantity and rates use decimal strings backed by `rust_decimal::Decimal`.
The canonical scale is field-specific and rounding is explicit (banker's rounding unless
the source contract says otherwise). Time is UTC RFC 3339 plus integer epoch milliseconds
where ordering is required. Every provider result carries source, observation time,
revision, update cadence and raw SHA-256.

## ADR decisions

1. **WAL encoding:** versioned canonical JSON records. Keys are produced from typed
   structures and bytes are hashed exactly as written. This is inspectable and provides
   a low-risk bridge to legacy JSONL. A later protobuf payload version can coexist.
2. **Segments/durability:** 256 MiB production target; tests may lower it. Each record has
   CRC32C and payload SHA-256; segments form a SHA-256 chain. Required realtime events call
   `sync_data` before projection publication. No authoritative record is rewritten.
3. **Watermarks:** `published_cursor` never leads `persisted_cursor` for Polymarket. A WAL
   failure leaves committed state unchanged and makes readiness fail closed.
4. **ClickHouse:** typed `clickhouse` client behind narrow repository traits; bounded pool,
   deadlines and idempotent insert tokens. It is not linked into the realtime core.
5. **LongPort:** remains a Python bridge until its Rust client passes sequence, reconnect,
   source-evidence and replay tests. It is out of the Polymarket sample cutover.
6. **MCP:** Rust owns transport/auth first; existing tool implementations are proxied until
   golden tests pass. SDK choice is deferred without delaying HTTP/realtime ownership.
7. **Worker protocol:** length-prefixed versioned JSON over UDS for Phase 1. This avoids a
   build-time `protoc` dependency while retaining typed schemas, deadlines and hashes.
   UDS mode is `0600`; no TCP fallback exists. gRPC is a compatible future transport ADR.
8. **Worker isolation:** dedicated OS processes, minimal environment allow-list, per-task
   staging lease, deadline and restart budget. Provider secrets are passed only to the
   relevant process by reference/FD and are redacted from logs.
9. **Ports:** 8791/8794 remain shadow-only through a 24-hour observation; removal requires
   Phase 7 approval. 8790 cutover cannot occur during this implementation work item.
10. **Load/SLO:** exact 100 markets/200 books, 200-token stream, four concurrent scoped
    readers and two independent consumers. Gates: publish p99 <=20 ms, WAL p99 <=20 ms,
    health p99 <=50 ms, queue bounded, hot SQLite queries zero, gaps/disconnect delta zero,
    and no health/bootstrap 503. This work item requires a reproducible >=45 minute run;
    60 minute, 24 hour and 7 day evidence remain later release gates.

## Permission and compliance boundary

Public reads may be anonymous only where the frozen Python contract allows it. Mutations
require authenticated capability scopes, idempotency keys and audit records. Worker input
and output are untrusted and schema/hash validated. No legal, regulatory or investment
conclusion is produced by the migration.

## Acceptance mapping

Each completed phase writes machine-readable results below
`artifacts/rust-python-migration/results/`. A result contains the command, commit, timestamps,
input hashes and measured assertions. A phase without such evidence is incomplete.

