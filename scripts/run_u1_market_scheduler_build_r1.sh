#!/bin/bash
set -eu
umask 077
runtime=/mnt/p44pro/marketcow-shadow-v3-runtime/linux
project=/mnt/p44pro/projects/marketcow-shadow-v3
export CARGO_HOME=$runtime/cargo CARGO_TARGET_DIR=$runtime/target TMPDIR=$runtime/tmp CARGO_INCREMENTAL=0
test ! -e "$runtime/logs/market-scheduler-build-r1.exit-code"
finish() { status=$?; printf '%s\n' "$status" > "$runtime/logs/market-scheduler-build-r1.exit-code"; }
trap finish EXIT
cd "$project"
cargo test --offline --locked --release -j4 -p marketcowd --bin marketcow-discovery-collector
cargo build --offline --locked --release -j4 --bin marketcow-discovery-collector
sha256sum "$runtime/target/release/marketcow-discovery-collector"
