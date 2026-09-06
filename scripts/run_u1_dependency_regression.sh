#!/bin/bash
set -eu
umask 077
runtime=/mnt/p44pro/marketcow-shadow-v3-runtime/linux
[[ $(findmnt -n -o FSTYPE --target "$runtime") == ext4 ]]
cd /mnt/p44pro/projects/marketcow-shadow-v3
export CARGO_HOME="$runtime/cargo" CARGO_TARGET_DIR="$runtime/target" TMPDIR="$runtime/tmp"
export PYTHONDONTWRITEBYTECODE=1
export PYTHONPATH="$runtime/read-api-packages:/mnt/p44pro/projects/marketcow-shadow-v3/src"
[[ ! -e "$runtime/logs/dependency-regression-r1.exit-code" ]]
trap 'result=$?; printf "%s\n" "$result" > "$runtime/logs/dependency-regression-r1.exit-code"' EXIT
bash scripts/run_u1_rust_tests.sh r10
cargo build --offline --locked -j2 --bin marketcow-discovery-collector > "$runtime/logs/dependency-r1-build.log" 2>&1
python3 scripts/run_u1_batched_scope_probe.py --run r2 --dependencies --audit-runs r4 r5 > "$runtime/logs/dependency-r1-verification.log" 2>&1
