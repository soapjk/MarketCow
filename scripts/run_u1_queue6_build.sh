#!/bin/bash
set -eu
umask 077
runtime=/mnt/p44pro/marketcow-shadow-v3-runtime/linux
export CARGO_HOME=$runtime/cargo CARGO_TARGET_DIR=$runtime/target TMPDIR=$runtime/tmp CARGO_INCREMENTAL=0
test ! -e "$runtime/logs/queue6-7b19dd2-build.exit-code"
finish() { status=$?; printf '%s\n' "$status" > "$runtime/logs/queue6-7b19dd2-build.exit-code"; }
trap finish EXIT
cd /mnt/p44pro/projects/marketcow-shadow-v3
touch crates/marketcowd/src/source_dispatch.rs crates/marketcowd/src/source_websocket_pipeline.rs
cargo test --offline --locked --release -j4 -p marketcowd --bin marketcow-discovery-collector
cargo build --offline --locked --release -j4 --bin marketcow-discovery-collector
sha256sum "$runtime/target/release/marketcow-discovery-collector"
