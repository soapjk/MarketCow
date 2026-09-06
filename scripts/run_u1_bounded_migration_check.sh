#!/bin/bash
set -eu
umask 077
runtime=/mnt/p44pro/marketcow-shadow-v3-runtime/linux
project=/mnt/p44pro/projects/marketcow-shadow-v3
export CARGO_HOME=$runtime/cargo CARGO_TARGET_DIR=$runtime/target TMPDIR=$runtime/tmp CARGO_INCREMENTAL=0
finish() { status=$?; printf '%s\n' "$status" > "$runtime/logs/bounded-migration-r1.exit-code"; }
trap finish EXIT
cd "$project"
cargo test --offline --locked --release -j2 -p marketcow-runtime bounded_history
cargo build --offline --locked --release -j2 --bin marketcow-source-maintenance
python3 "$project/scripts/prepare_bounded_source_candidate.py" \
  --source "$runtime/scoped-live-source-r1" --target "$runtime/bounded-scoped-candidate-r1"
"$runtime/target/release/marketcow-source-maintenance" \
  --root "$runtime/bounded-scoped-candidate-r1" --maximum-batch-bytes 16777216 --bounded-history-bytes 67108864
df -h "$runtime"
