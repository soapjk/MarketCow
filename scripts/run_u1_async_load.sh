#!/bin/bash
set -eu
umask 077
runtime=/mnt/p44pro/marketcow-shadow-v3-runtime/linux
[[ $(findmnt -n -o FSTYPE --target "$runtime") == ext4 ]]
cd /mnt/p44pro/projects/marketcow-shadow-v3
export CARGO_HOME="$runtime/cargo" CARGO_TARGET_DIR="$runtime/target" TMPDIR="$runtime/tmp"
export PYTHONDONTWRITEBYTECODE=1 PYTHONPATH="$runtime/read-api-packages:/mnt/p44pro/projects/marketcow-shadow-v3/src"
[[ ! -e "$runtime/logs/async-load-r1.exit-code" ]]
trap 'result=$?; printf "%s\n" "$result" > "$runtime/logs/async-load-r1.exit-code"' EXIT
cargo test --offline --locked -j2 -p marketcowd --bin marketcow-discovery-collector -- --nocapture > "$runtime/logs/async-load-r1-tests.log" 2>&1
cargo test --offline --locked -j2 -p marketcow-runtime discovery_source >> "$runtime/logs/async-load-r1-tests.log" 2>&1
cargo build --offline --locked -j2 --bin marketcow-discovery-collector > "$runtime/logs/async-load-r1-build.log" 2>&1
python3 scripts/check_u1_async_load.py > "$runtime/logs/async-load-r1.log" 2>&1
