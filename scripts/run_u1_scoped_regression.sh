#!/bin/bash
set -eu
umask 077
runtime=/mnt/p44pro/marketcow-shadow-v3-runtime/linux
[[ $(findmnt -n -o FSTYPE --target "$runtime") == ext4 ]]
cd /mnt/p44pro/projects/marketcow-shadow-v3
export CARGO_HOME="$runtime/cargo" CARGO_TARGET_DIR="$runtime/target" TMPDIR="$runtime/tmp"
[[ ! -e "$runtime/logs/scoped-regression-r1.exit-code" ]]
trap 'result=$?; printf "%s\n" "$result" > "$runtime/logs/scoped-regression-r1.exit-code"' EXIT
bash scripts/run_u1_rust_tests.sh r7
cargo build --offline --locked -j2 --bin marketcow-discovery-collector > "$runtime/logs/scoped-r1-build.log" 2>&1
python3 scripts/run_u1_scoped_rust_probe.py
