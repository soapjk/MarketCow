#!/bin/bash
set -eu
umask 077
runtime=/mnt/p44pro/marketcow-shadow-v3-runtime/linux
[[ $(findmnt -n -o FSTYPE --target "$runtime") == ext4 ]]
cd /mnt/p44pro/projects/marketcow-shadow-v3
export CARGO_HOME="$runtime/cargo" CARGO_TARGET_DIR="$runtime/target" TMPDIR="$runtime/tmp"
export PYTHONDONTWRITEBYTECODE=1
export PYTHONPATH="$runtime/read-api-packages:/mnt/p44pro/projects/marketcow-shadow-v3/src"
[[ ! -e "$runtime/logs/bridge-regression-r2.exit-code" ]]
trap 'result=$?; printf "%s\n" "$result" > "$runtime/logs/bridge-regression-r2.exit-code"' EXIT
# Update only local workspace dependency edges; no network resolution.
cargo test --offline -j2 --bin marketcow-live-source-bridge > "$runtime/logs/bridge-r2-test.log" 2>&1
bash scripts/run_u1_rust_tests.sh r8
cargo build --offline --locked -j2 --bin marketcow-live-source-bridge > "$runtime/logs/bridge-r2-build.log" 2>&1
python3 scripts/prepare_u1_live_bridge.py > "$runtime/logs/bridge-r2-preparation.log" 2>&1
