# MarketCow exact-scope runtime evidence

Observed 2026-08-24 in Asia/Shanghai.

## Runtime identity and ownership

- MarketCow implementation commit: `334f0ac42fd781364b4a2d262cf4fda463393043`.
- Required Tradude commit: `1d509944134cf3e181782ff2acbe2d907ccdde1b`; the artifact-producing Tradude worktree HEAD was exactly this commit at collector startup.
- Exact scope: `bc7f527f6ac04b7ef6a5c53aba2b592c01a49b50cdc3164c0aa0e4389585ff0a`.
- Collector and 8794 live-stream PID: `2989` (one combined MarketCow process), launchd label `com.marketcow.polymarket.scoped`.
- 8790 PID: `14727`, launchd label `com.marketcow.events-soak`.
- 8791 PID: `14729`, launchd label `com.marketcow.polymarket.read-api`.
- All three jobs report `state = running`, PPID 1, and this WorkItem worktree as their working directory. `lsof` reports the PIDs listening on 8794, 8790, and 8791 respectively.
- `launchctl list` contains no running Tradude shadow PID, and process inspection found no Tradude shadow process. No Tradude shadow or real-order process was started.

Reproduce:

```sh
for label in com.marketcow.polymarket.scoped com.marketcow.events-soak com.marketcow.polymarket.read-api; do
  launchctl print "gui/$(id -u)/$label" | rg 'state = running|pid =|working directory =|program ='
done
lsof -nP -iTCP:8790 -iTCP:8791 -iTCP:8794 -sTCP:LISTEN
ps -p 2989,14727,14729 -o pid,ppid,lstart,etime,command
```

## Immutable scope provenance

The supervised collector requires the manifest, selection report, candidate snapshot, and Tradude worktree together. Startup fails closed unless the report embeds the byte-equivalent manifest, the candidate revision/ID match, all 100 markets remain eligible for at least one hour, every market's lock duration is at most 30 days, the final CLOB attempt covers exactly the same 200 tokens with complete two-sided books, and the Tradude worktree contains the required commit.

- `manifest.json` SHA-256: `6e111e4e2652d2256ec1e0fd6ebef6ed3ab8c48e80e270240c0589de61be7f33`.
- `selection-report.json` SHA-256: `c35c62ac95bd76d62a3d142b9baf55cc376a149f99b91bd0963f5dc472edee20`.
- candidate snapshot SHA-256: `0d049ac4799ec1f57f7b0f561c42368362624c5a1b98e6e7ecc41684d8dd5683`.
- Maximum selected capital-lock duration: `2377809000000000 ns` (less than `2592000000000000 ns`, 30 days).
- Final selection coverage: 100 markets, 200 requested/received/two-sided tokens, zero missing and zero non-two-sided tokens.
- Collector stdout line `scope_selection_validated` records these hashes, exact scope, 200-token count, max lock, and Tradude commit before `exec` into the live collector.

Reproduce:

```sh
rg 'scope_selection_validated' '/Users/androidjk/Library/Logs/MarketCow/polymarket-scoped.log' | tail -1
```

## Dual-endpoint verification

`exact-scope-dual-endpoint.json` is the authoritative acceptance Artifact. It contains two full-sync rounds for both ports and passed every strict check without ignored fields or relaxed validation.

- 8790 cursor: `22686483 -> 22687823`.
- 8791 cursor: `22686483 -> 22687824`.
- Every round/port: HTTP 200, `index_ready`, exact 100 markets, 200 books, 100 complete markets, gap 0, disconnect 0.
- Every round/port: all 200 tokens have `instrument.price_increment == book.tick_size`; no mismatch token IDs.
- Every round/port: outer, health, bootstrap, and snapshot catalog revision/cursor/projection generation/scope/freshness boundary agree; no mixed-revision component.
- Every round/port: `events_read_source=memory_projection`, `realtime_sqlite_query_ms=0`.
- Every round/port also reports `derived_index_error=DatabaseError:database disk image is malformed`, proving that the actually damaged derived SQLite does not block the real-time memory path.

Reproduce:

```sh
PYTHONPATH=src '/Users/androidjk/Library/Application Support/MarketCow/atomic-freshness-venv/bin/python' scripts/verify_polymarket_exact_scope.py \
  --scope-manifest '/Volumes/T9/projects/trade/tradude-worktrees/polymarket-memory-stream-contract/.tradude-local/polymarket-live/v81-thirty-day-capital-lock-20260824T193000CST/scopes/bc7f527f6ac04b7ef6a5c53aba2b592c01a49b50cdc3164c0aa0e4389585ff0a/manifest.json' \
  --expected-scope-id bc7f527f6ac04b7ef6a5c53aba2b592c01a49b50cdc3164c0aa0e4389585ff0a \
  --port 8790 --port 8791 --rounds 2 --round-interval-seconds 5 \
  --output artifacts/polymarket-scope-bc7f-20260824/exact-scope-dual-endpoint.json
```

## Append-only authority preservation

Before switch, `events.jsonl` had inode `30826139`, size `88110173685`, and first/then-current-last 1 MiB SHA-256 values `9f643f8292bd842a76919a4ba3dae168c36890f8593db5c2699c55dc83d13c98` and `5d05a50365017fc02f0a3da1ca689bf4926d7e5b57cde0b214661fd272f0eb4f`.

After switch, it retained inode `30826139`, grew to `88225526246` bytes, retained the same first 1 MiB hash, and the exact pre-switch final 1 MiB byte range still hashes to `5d05a50365017fc02f0a3da1ca689bf4926d7e5b57cde0b214661fd272f0eb4f`. This proves the authority file was appended to rather than deleted, replaced, truncated, overwritten, or rewritten. The malformed derived SQLite retained inode `30825606`, size `6451847168`, and its old modification timestamp while the event log and cursors continued forward.

## Verification commands and results

- `ruff check src scripts tests`: passed.
- `python -m compileall -q src scripts`: passed.
- Focused Polymarket suite: 146 passed.
- Full suite reached 384 passed, 2 skipped before an unrelated macOS `/private/var` vs `/var` temporary executable path alias caused `test_stdio_absolute_entry_starts_from_arbitrary_workspace` to fail. No Polymarket test failed.
- The additional 200x2 high-frequency health stress report is retained as a failed non-acceptance artifact: both ports correctly failed closed with HTTP 503 during freshness-budget pressure. It is not cited as passing evidence and did not replace the successful two-round atomic full-sync Artifact.
