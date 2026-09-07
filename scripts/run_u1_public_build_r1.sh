#!/bin/bash
set -eu
umask 077
runtime=/mnt/p44pro/marketcow-shadow-v3-runtime/linux
export CARGO_HOME=$runtime/cargo CARGO_TARGET_DIR=$runtime/target TMPDIR=$runtime/tmp CARGO_INCREMENTAL=0
test ! -e "$runtime/logs/public-api-build-r1.exit-code"
finish() { status=$?; printf '%s\n' "$status" > "$runtime/logs/public-api-build-r1.exit-code"; }
trap finish EXIT
cd /mnt/p44pro/projects/marketcow-shadow-v3
touch crates/marketcowd/src/discovery_collector.rs crates/marketcowd/src/source_publication.rs crates/marketcowd/src/source_public_api.rs crates/marketcowd/src/source_public_frame.rs crates/marketcowd/src/source_public_binding.rs
cargo test --offline --locked --release -j4 --workspace
cargo build --offline --locked --release -j4 --bin marketcow-discovery-collector
sha256sum "$runtime/target/release/marketcow-discovery-collector"
