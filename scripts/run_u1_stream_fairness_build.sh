#!/bin/bash
set -eu
umask 077
runtime=/mnt/p44pro/marketcow-shadow-v3-runtime/linux
project=/mnt/p44pro/projects/marketcow-shadow-v3
export CARGO_HOME=$runtime/cargo CARGO_TARGET_DIR=$runtime/target TMPDIR=$runtime/tmp CARGO_INCREMENTAL=0
export PYTHONPATH=$runtime/read-api-packages:$project/src PYTHONDONTWRITEBYTECODE=1
finish() { status=$?; printf '%s\n' "$status" > "$runtime/logs/stream-fairness-build-r1.exit-code"; }
trap finish EXIT
cd "$project"
cargo test --offline --locked --release -j2 -p marketcowd --bin marketcow-discovery-collector
python3 -m unittest tests.test_polymarket_stream_metrics tests.test_polymarket_partial_scope tests.test_polymarket_live_projection_freshness -q
cargo build --offline --locked --release -j2 --bin marketcow-discovery-collector
sha256sum "$runtime/target/release/marketcow-discovery-collector"
