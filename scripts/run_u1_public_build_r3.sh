#!/bin/bash
set -eu
umask 077
runtime=/mnt/p44pro/marketcow-shadow-v3-runtime/linux
export CARGO_HOME=$runtime/cargo CARGO_TARGET_DIR=$runtime/target TMPDIR=$runtime/tmp CARGO_INCREMENTAL=0
test ! -e "$runtime/logs/public-api-build-r3.exit-code"
finish() { status=$?; printf '%s\n' "$status" > "$runtime/logs/public-api-build-r3.exit-code"; }
trap finish EXIT
cd /mnt/p44pro/projects/marketcow-shadow-v3
touch crates/marketcowd/src/discovery_collector.rs crates/marketcowd/src/source_publication.rs crates/marketcowd/src/source_public_api.rs crates/marketcowd/src/source_public_frame.rs crates/marketcowd/src/source_discovery_history.rs crates/marketcowd/src/source_discovery_projection.rs crates/marketcowd/src/source_discovery_api.rs crates/marketcowd/src/source_discovery_startup.rs crates/marketcowd/src/live_source_bridge.rs
cargo test --offline --locked --release -j4 --workspace
cargo build --offline --locked --release -j4 --bin marketcow-discovery-collector
sha256sum "$runtime/target/release/marketcow-discovery-collector"
