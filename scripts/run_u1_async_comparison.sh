#!/bin/bash
set -eu
umask 077
runtime=/mnt/p44pro/marketcow-shadow-v3-runtime/linux
[[ $(findmnt -n -o FSTYPE --target "$runtime") == ext4 ]]
cd /mnt/p44pro/projects/marketcow-shadow-v3
export CARGO_HOME="$runtime/cargo" CARGO_TARGET_DIR="$runtime/target" TMPDIR="$runtime/tmp"
export PYTHONDONTWRITEBYTECODE=1 PYTHONPATH="$runtime/read-api-packages:/mnt/p44pro/projects/marketcow-shadow-v3/src"
[[ ! -e "$runtime/logs/async-comparison-r2.exit-code" ]]
trap 'result=$?; printf "%s\n" "$result" > "$runtime/logs/async-comparison-r2.exit-code"' EXIT
# Old deployed source is the release baseline. Do not replace it while building/running it.
sha256sum crates/marketcowd/src/discovery_collector.rs crates/marketcowd/src/source_publication.rs > "$runtime/logs/async-load-r2-source.sha256"
cargo build --release --offline --locked -j2 --bin marketcow-discovery-collector > "$runtime/logs/async-load-r2-build.log" 2>&1
cp "$runtime/target/release/marketcow-discovery-collector" "$runtime/target/release/marketcow-discovery-collector-baseline-r2"
python3 scripts/check_u1_async_load.py --run r2 --profile release --input-mode rest-poll > "$runtime/logs/async-load-r2.log" 2>&1
# All r2 services are stopped by the audit before the optimized source is installed.
rsync -a async-optimized-r3/crates/ crates/
cargo test --offline --locked -j2 -p marketcowd --bin marketcow-discovery-collector > "$runtime/logs/async-load-r3-tests.log" 2>&1
cargo test --offline --locked -j2 -p marketcow-polymarket -p marketcow-runtime discovery_source >> "$runtime/logs/async-load-r3-tests.log" 2>&1
cargo build --release --offline --locked -j2 --bin marketcow-discovery-collector > "$runtime/logs/async-load-r3-build.log" 2>&1
sha256sum crates/marketcowd/src/discovery_collector.rs crates/marketcowd/src/source_publication.rs > "$runtime/logs/async-load-r3-source.sha256"
python3 scripts/check_u1_async_load.py --run r3 --profile release --input-mode rest-poll > "$runtime/logs/async-load-r3.log" 2>&1
