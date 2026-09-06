#!/bin/bash
set -eu
umask 077
runtime=/mnt/p44pro/marketcow-shadow-v3-runtime/linux
[[ $(findmnt -n -o FSTYPE --target "$runtime") == ext4 ]]
cd /mnt/p44pro/projects/marketcow-shadow-v3
export CARGO_HOME="$runtime/cargo" CARGO_TARGET_DIR="$runtime/target" TMPDIR="$runtime/tmp"
export PYTHONDONTWRITEBYTECODE=1
export PYTHONPATH="$runtime/read-api-packages:/mnt/p44pro/projects/marketcow-shadow-v3/src"
[[ ! -e "$runtime/logs/batch-regression-r1.exit-code" ]]
trap 'result=$?; printf "%s\n" "$result" > "$runtime/logs/batch-regression-r1.exit-code"' EXIT
bash scripts/run_u1_rust_tests.sh r9
cargo build --offline --locked -j2 --bin marketcow-discovery-collector > "$runtime/logs/batch-r1-build.log" 2>&1
if systemctl --user is-active --quiet marketcow-scoped-live-observe-r2; then
    exit 1
fi
python3 scripts/run_u1_batched_scope_probe.py --run r1 --audit-runs r2 r3 > "$runtime/logs/batch-r1-verification.log" 2>&1
