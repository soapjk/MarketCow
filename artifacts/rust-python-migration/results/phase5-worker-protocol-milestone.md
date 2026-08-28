# Phase 5 worker protocol milestone

Status: verified milestone; Phase 5 remains incomplete.

Local implementation commit: `61f9252a53477d2529d9b7414b1f9b2fdd83152f`.

Implemented evidence:

- Rust owns the typed `marketcow.worker.v1` job state machine and UDS protocol.
- The UDS has no TCP fallback, is mode `0600`, and uses bounded 1 MiB length-prefixed JSON.
- Handshake binds protocol version, worker ID/revision, nonce and explicit capabilities.
- Jobs carry idempotency key, request schema/hash, deadline, revision, lease, attempts,
  audit actor, classified error and content-addressed result metadata.
- Only the unexpired lease holder can start, fail or complete a job. Late results are
  rejected, retry attempts are bounded, and canceled/terminal jobs cannot be advanced.
- Each task receives a mode `0700` staging directory. Rust validates regular-file type,
  canonical parent containment, byte length and streaming SHA-256 before accepting a result.
- The Python package opens only the UDS client connection and has no public listener or DB
  client dependency.

Reproducible commands and observed result:

```text
PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 python3 -m pytest -q \
  tests/test_rust_python_worker.py tests/test_rust_migration_architecture.py
# 9 passed

cargo test --workspace
# 47 passed, 0 failed

cargo clippy --workspace --all-targets -- -D warnings
# passed, 0 warnings
```

Remaining Phase 5 work: persistent PostgreSQL-backed JobRepository, process pool supervision,
provider handlers, restart recovery, timeout/cancel integration at the public control plane,
and verified removal of Python DB credentials/public API dependencies. This file does not claim
Phase 5 or overall migration completion.
