#!/bin/bash
set -eu
umask 077
runtime=/mnt/p44pro/marketcow-shadow-v3-runtime/linux
export CARGO_HOME=$runtime/cargo CARGO_TARGET_DIR=$runtime/target TMPDIR=$runtime/tmp CARGO_INCREMENTAL=0
run=${1:?explicit run identity required}
[[ "$run" =~ ^r[1-9][0-9]*$ ]]
test ! -e "$runtime/logs/universe-runtime-build-$run.exit-code"
finish() { result=$?; printf '%s\n' "$result" > "$runtime/logs/universe-runtime-build-$run.exit-code"; }
trap finish EXIT
manifest=$runtime/tmp/universe-rust-source-r1/Cargo.toml
cd "$runtime/tmp/universe-rust-source-r1"
timeout 900 cargo test --manifest-path "$manifest" --offline --locked --release --workspace -j4
timeout 900 cargo build --manifest-path "$manifest" --offline --locked --release -j4 --bin marketcow-discovery-collector
sha256sum "$runtime/target/release/marketcow-discovery-collector"
