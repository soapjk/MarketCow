#!/bin/bash
set -eu
umask 077
runtime=/mnt/p44pro/marketcow-shadow-v3-runtime/linux
[[ $(findmnt -n -o FSTYPE --target "$runtime") == ext4 ]]
[[ $(df --output=avail -B1 "$runtime" | tail -n 1) -gt 7000000000 ]]
cd /mnt/p44pro/projects/marketcow-shadow-v3
export CARGO_HOME="$runtime/cargo" CARGO_TARGET_DIR="$runtime/target" TMPDIR="$runtime/tmp"
export PYTHONDONTWRITEBYTECODE=1 PYTHONPATH="$runtime/read-api-packages:/mnt/p44pro/projects/marketcow-shadow-v3/src"
[[ ! -e "$runtime/logs/dynamic-check-r1.exit-code" ]]
trap 'result=$?; printf "%s\n" "$result" > "$runtime/logs/dynamic-check-r1.exit-code"' EXIT
python3 -m unittest tests.test_polymarket_scope_activation -v > "$runtime/logs/dynamic-check-r1-tests.log" 2>&1
bash scripts/run_u1_rust_tests.sh r13
cargo build --offline --locked -j2 --bin marketcow-discovery-collector --bin marketcow-live-source-bridge > "$runtime/logs/dynamic-check-r1-build.log" 2>&1
python3 scripts/check_u1_dynamic_scope.py --run r1 > "$runtime/logs/dynamic-check-r1.log" 2>&1
