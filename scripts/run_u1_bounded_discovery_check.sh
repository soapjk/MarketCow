#!/bin/bash
set -eu
umask 077
runtime=/mnt/p44pro/marketcow-shadow-v3-runtime/linux
project=/mnt/p44pro/projects/marketcow-shadow-v3
export CARGO_HOME=$runtime/cargo CARGO_TARGET_DIR=$runtime/target TMPDIR=$runtime/tmp CARGO_INCREMENTAL=0
export PYTHONPATH=$runtime/read-api-packages:$project/src PYTHONDONTWRITEBYTECODE=1
finish() { status=$?; printf '%s\n' "$status" > "$runtime/logs/bounded-discovery-r1.exit-code"; }
trap finish EXIT
cd "$project"
cargo test --offline --locked --release -j2 -p marketcowd --bin marketcow-discovery-collector
cargo build --offline --locked --release -j2 --bin marketcow-source-maintenance --bin marketcow-discovery-collector
python3 scripts/prepare_bounded_source_candidate.py --source "$runtime/discovery-source-r1" --target "$runtime/bounded-discovery-candidate-r1"
"$runtime/target/release/marketcow-source-maintenance" --root "$runtime/bounded-discovery-candidate-r1" \
 --maximum-batch-bytes 16777216 --bounded-history-bytes 67108864 --retire-candidate-legacy-log
python3 scripts/verify_bounded_discovery_candidate.py
df -h "$runtime"
