#!/bin/bash
set -eu
umask 077

run=${1:-r1}
runtime=/mnt/p44pro/marketcow-shadow-v3-runtime/linux
project=/mnt/p44pro/projects/marketcow-shadow-v3
log="$runtime/logs/paper-250-build-$run.log"
exit_file="$runtime/logs/paper-250-build-$run.exit-code"

[[ $(findmnt -n -o FSTYPE --target "$runtime") == ext4 ]]
[[ ! -e "$exit_file" ]]
trap 'status=$?; trap - EXIT; printf "%s\n" "$status" > "$exit_file"; exit "$status"' EXIT

export CARGO_HOME="$runtime/cargo"
export CARGO_TARGET_DIR="$runtime/target"
export TMPDIR="$runtime/tmp"
export PYTHONDONTWRITEBYTECODE=1
export PYTHONPATH="$runtime/read-api-packages:$project/src"

cd "$project"
{
  python3 -m unittest \
    tests.test_prepare_polymarket_scoped_live \
    tests.test_polymarket_configured_scope_read_api \
    tests.test_run_polymarket_dynamic_live_api -v
  cargo test --offline --locked -j2 -p marketcowd \
    --bin marketcow-discovery-collector \
    --bin marketcow-live-source-bridge
  cargo build --offline --locked --release -j2 \
    --bin marketcow-discovery-collector \
    --bin marketcow-live-source-bridge
  sha256sum \
    "$runtime/target/release/marketcow-discovery-collector" \
    "$runtime/target/release/marketcow-live-source-bridge"
} > "$log" 2>&1
