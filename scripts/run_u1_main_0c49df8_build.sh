#!/bin/bash
set -eu
umask 077
runtime=/mnt/p44pro/marketcow-shadow-v3-runtime/linux
export CARGO_HOME=$runtime/cargo CARGO_TARGET_DIR=$runtime/target TMPDIR=$runtime/tmp CARGO_INCREMENTAL=0
test ! -e "$runtime/logs/main-0c49df8-build-r2.exit-code"
finish() { result=$?; printf '%s\n' "$result" > "$runtime/logs/main-0c49df8-build-r2.exit-code"; }
trap finish EXIT
cd "$runtime/tmp"
manifest=/mnt/p44pro/projects/marketcow-main-0c49df887ed9/Cargo.toml
cargo test --manifest-path "$manifest" --offline --locked --release --workspace -j4
cargo build --manifest-path "$manifest" --offline --locked --release -j4 --bin marketcow-discovery-collector
sha256sum "$runtime/target/release/marketcow-discovery-collector"
