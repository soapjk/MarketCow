#!/bin/bash
set -eu
umask 077
runtime=/mnt/p44pro/marketcow-shadow-v3-runtime/linux
export CARGO_HOME=$runtime/cargo CARGO_TARGET_DIR=$runtime/target TMPDIR=$runtime/tmp
export CARGO_INCREMENTAL=0
cd /mnt/p44pro/projects/marketcow-shadow-v3
finish() { status=$?; printf '%s\n' "$status" > "$runtime/logs/bounded-history-r1.exit-code"; }
trap finish EXIT
# Release profile avoids recreating the deleted multi-GB debug cache.
cargo test --offline --locked --release -j2 -p marketcow-runtime discovery_source::tests
cargo test --offline --locked --release -j2 -p marketcowd --bin marketcow-discovery-collector
cargo test --offline --locked --release -j2 -p marketcowd --bin marketcow-live-source-bridge
PYTHONPATH="$runtime/read-api-packages:$PWD/src" PYTHONDONTWRITEBYTECODE=1 \
 python3 -m unittest tests.test_polymarket_partial_scope tests.test_polymarket_live_projection_freshness -q
