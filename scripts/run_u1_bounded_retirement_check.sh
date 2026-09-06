#!/bin/bash
set -eu
umask 077
runtime=/mnt/p44pro/marketcow-shadow-v3-runtime/linux
run_id=${1:-r1}
export CARGO_HOME=$runtime/cargo CARGO_TARGET_DIR=$runtime/target TMPDIR=$runtime/tmp CARGO_INCREMENTAL=0
finish() { status=$?; printf '%s\n' "$status" > "$runtime/logs/bounded-retirement-$run_id.exit-code"; }
trap finish EXIT
cd /mnt/p44pro/projects/marketcow-shadow-v3
cargo test --offline --locked --release -j2 -p marketcow-runtime bounded_history
cargo test --offline --locked --release -j2 -p marketcowd --bin marketcow-live-source-bridge
cargo build --offline --locked --release -j2 --bin marketcow-source-maintenance
"$runtime/target/release/marketcow-source-maintenance" \
 --root "$runtime/bounded-scoped-candidate-r1" --maximum-batch-bytes 16777216 \
 --bounded-history-bytes 67108864 --retire-candidate-legacy-log
df -h "$runtime"
