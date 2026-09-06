#!/bin/bash
# Run the U1 baseline with all generated files on the approved ext4 volume.
set -eu
run_id=${1:?Supply a unique run id such as r4}
[[ $run_id =~ ^r[0-9]+$ ]]
runtime=/mnt/p44pro/marketcow-shadow-v3-runtime/linux
[[ $(findmnt -n -o FSTYPE --target "$runtime") == ext4 ]]
cd /mnt/p44pro/projects/marketcow-shadow-v3
export CARGO_HOME="$runtime/cargo"
export CARGO_TARGET_DIR="$runtime/target"
export TMPDIR="$runtime/tmp"
umask 077
[[ ! -e "$runtime/logs/rust-baseline-test-$run_id.log" ]]
trap 'result=$?; printf "%s\n" "$result" > "$runtime/logs/rust-baseline-test-$run_id.exit-code"' EXIT
cargo test --offline --locked --workspace -j 2 > "$runtime/logs/rust-baseline-test-$run_id.log" 2>&1
