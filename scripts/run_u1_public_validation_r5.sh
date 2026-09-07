#!/bin/bash
set -eu
umask 077
runtime=/mnt/p44pro/marketcow-shadow-v3-runtime/linux
export CARGO_HOME=$runtime/cargo CARGO_TARGET_DIR=$runtime/target TMPDIR=$runtime/tmp CARGO_INCREMENTAL=0
export PYTHONPATH=$runtime/read-api-packages
test ! -e "$runtime/logs/public-validation-r5.exit-code"
finish() { status=$?; printf '%s\n' "$status" > "$runtime/logs/public-validation-r5.exit-code"; }
trap finish EXIT
cd /mnt/p44pro/projects/marketcow-shadow-v3
touch crates/marketcowd/src/source_public_api.rs crates/marketcowd/src/source_discovery_api.rs
cargo test --offline --locked --release -j4 --workspace
cargo build --offline --locked --release -j4 --bin marketcow-discovery-collector
binary_sha=$(sha256sum "$runtime/target/release/marketcow-discovery-collector" | cut -d ' ' -f 1)
printf '%s\n' "$binary_sha"
/usr/bin/python3 scripts/check_u1_public_rust_r1.py --run r3 --binary-sha256 "$binary_sha" --seconds 30 --wire-bytes 67108864
/usr/bin/python3 scripts/check_u1_rust_discovery.py --run r3 --binary-sha256 "$binary_sha"
