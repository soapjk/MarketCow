#!/bin/bash
set -eu
umask 077
runtime=/mnt/p44pro/marketcow-shadow-v3-runtime/linux
[[ $(findmnt -n -o FSTYPE --target "$runtime") == ext4 ]]
cd /mnt/p44pro/projects/marketcow-shadow-v3
export CARGO_HOME="$runtime/cargo" CARGO_TARGET_DIR="$runtime/target" TMPDIR="$runtime/tmp"
export PYTHONDONTWRITEBYTECODE=1 PYTHONPATH="$runtime/read-api-packages:/mnt/p44pro/projects/marketcow-shadow-v3/src"
[[ ! -e "$runtime/logs/async-retry-r4.exit-code" ]]
trap 'result=$?; printf "%s\n" "$result" > "$runtime/logs/async-retry-r4.exit-code"' EXIT
# rsync-preserved source mtimes predate the baseline build. Invalidate exactly
# these changed inputs, preserving contents, baseline binary and all build caches.
touch crates/marketcow-polymarket/src/discovery_source.rs crates/marketcow-runtime/src/discovery_source.rs crates/marketcowd/src/discovery_collector.rs crates/marketcowd/src/source_publication.rs
cargo build --release --offline --locked -j2 --bin marketcow-discovery-collector > "$runtime/logs/async-load-r4-build.log" 2>&1
sha256sum "$runtime/target/release/marketcow-discovery-collector" "$runtime/target/release/marketcow-discovery-collector-baseline-r2" > "$runtime/logs/async-load-r4-binaries.sha256"
if cmp -s "$runtime/target/release/marketcow-discovery-collector" "$runtime/target/release/marketcow-discovery-collector-baseline-r2"; then
    echo 'optimized binary unexpectedly equals baseline' >&2
    exit 1
fi
python3 scripts/check_u1_async_load.py --run r4 --profile release --input-mode rest-poll > "$runtime/logs/async-load-r4.log" 2>&1
