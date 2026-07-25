# MCVI completion audit — 2026-07-25

Authoritative scope: Plane project `MCVI`, parent `MCVI-1`, stages 1–11.

## Verified stages

1. CSV contract — `docs/csv-import-contract-v1.schema.json`,
   `CsvImportRequest`, valid/invalid contract tests, strict unknown-field policy.
2. Schema Profile — versioned columns, encoding, delimiter, timezone, units,
   precision, defaults, sessions and reusable US supplier example.
3. Instrument mapping — explicit `provider:`/`broker:` mapping to `SYMBOL.MIC`;
   missing/unsupported US MIC and conflicting normalized mappings fail.
4. Streaming dry-run — disk-backed exact key index, bounded error samples,
   OHLC/finite/negative checks, DST, ordering, duplicates, sessions and gaps.
   A 1,000,000-row run completed with maximum RSS 27,688,960 bytes.
5. Manifest/archive — SHA-256, size, profile, mappings, adjustment, time range,
   creator, source proof, retention, atomic hash-verified archive and reverse
   manifest-to-job lookup. Public APIs omit server paths.
6. Recoverable jobs — durable job/shard state, atomic claims, leases, heartbeat,
   fencing, bounded retries, cooperative cancellation, restart takeover and
   monotonic live progress. Terminal job updates are repository-guarded.
7. ClickHouse reliability — authoritative acknowledged+verified write path,
   WAL/replay tests, stable per-shard/per-Instrument ingestion IDs, receipt
   reconciliation and safe partial multi-Instrument replay.
8. Canonical quality — exact raw/canonical key coverage, receipt/artifact/count
   checks, Manifest range checks, canonical OHLC/finite checks, abnormal-price
   warnings, gap/calendar policy and per-shard diagnostics. Failure blocks
   `succeeded`.
9. CLI/API — shared service layer, dry-run/create/list/detail/cancel/retry,
   Manifest/quality/error endpoints, allowed-root, size and path protections,
   admin endpoint boundary and US example documentation.
10. Management page — dry-run/start, two-second durable polling, live progress,
    Manifest/quality/error evidence, confirmed cancel and confirmed retry.

## Stage 11 evidence

- Full suite: `Ran 423 tests ... OK (skipped=19)`.
- Static gate: `ruff check src tests` passed.
- JSON contract parses successfully.
- Fault tests cover invalid rows, duplicate file/archive, bounded-memory scale,
  DST, adjusted/split semantics, process takeover, ClickHouse/WAL failure,
  partial writes, cancellation, canonical failure and retry.
- Runbook: `docs/csv-import-recovery-runbook.md`.
- Release checklist: `docs/csv-import-release-gate.md`.

## Remaining external evidence

Plane stage 11 explicitly requires a smoke test using an authorized real
supplier sample. No CSV exists under the configured allowed root
`/Volumes/T9/projects/marketcow` as of this audit. Completion therefore requires
the operator to provide an authorized sample path plus its matching declaration.
Purchased CSV contents must not be committed to the repository.

Plane state/comment synchronization is also pending explicit authorization for
that remote write.
