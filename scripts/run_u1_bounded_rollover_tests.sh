#!/bin/bash
set -eu
umask 077
runtime=/mnt/p44pro/marketcow-shadow-v3-runtime/linux
export CARGO_HOME=$runtime/cargo CARGO_TARGET_DIR=$runtime/target TMPDIR=$runtime/tmp CARGO_INCREMENTAL=0
finish() { status=$?; printf '%s\n' "$status" > "$runtime/logs/bounded-rollover-r1.exit-code"; }
trap finish EXIT
cd /mnt/p44pro/projects/marketcow-shadow-v3
# Synthetic temporary roots only: no proxy, authoritative feeds or actual services.
cargo test --offline --locked --release -j2 -p marketcow-runtime bounded_history
cargo test --offline --locked --release -j2 -p marketcowd --bin marketcow-discovery-collector
cargo test --offline --locked --release -j2 -p marketcowd --bin marketcow-live-source-bridge
