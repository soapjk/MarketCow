#!/bin/bash
set -eu
umask 077
runtime=/mnt/p44pro/marketcow-shadow-v3-runtime/linux
[[ $(findmnt -n -o FSTYPE --target "$runtime") == ext4 ]]
cd /mnt/p44pro/projects/marketcow-shadow-v3
export CARGO_HOME="$runtime/cargo" CARGO_TARGET_DIR="$runtime/target" TMPDIR="$runtime/tmp"
export PYTHONDONTWRITEBYTECODE=1 PYTHONPATH="$runtime/read-api-packages:/mnt/p44pro/projects/marketcow-shadow-v3/src"
[[ ! -e "$runtime/logs/terminal-read-r1.exit-code" ]]
trap 'result=$?; printf "%s\n" "$result" > "$runtime/logs/terminal-read-r1.exit-code"' EXIT
python3 -m unittest tests.test_polymarket_terminal_read tests.test_polymarket_scope_activation -v > "$runtime/logs/terminal-read-r1-tests.log" 2>&1
bash scripts/run_u1_rust_tests.sh r14
cargo build --offline --locked -j2 --bin marketcow-discovery-collector --bin marketcow-live-source-bridge > "$runtime/logs/terminal-read-r1-build.log" 2>&1
python3 scripts/check_u1_terminal_read.py --run r1 > "$runtime/logs/terminal-read-r1.log" 2>&1
