#!/bin/bash
set -eu
umask 077
runtime=/mnt/p44pro/marketcow-shadow-v3-runtime/linux
run=${1:?explicit run required}
[[ "$run" == r48 ]]
[[ $(findmnt -n -o FSTYPE --target "$runtime") == ext4 ]]
cd /mnt/p44pro/projects/marketcow-shadow-v3
export CARGO_HOME="$runtime/cargo" CARGO_TARGET_DIR="$runtime/target" TMPDIR="$runtime/tmp"
export PYTHONDONTWRITEBYTECODE=1 PYTHONPATH="$runtime/read-api-packages:/mnt/p44pro/projects/marketcow-shadow-v3/src"
[[ ! -e "$runtime/logs/ws-load-$run.exit-code" ]]
trap 'result=$?; printf "%s\n" "$result" > "$runtime/logs/ws-load-$run.exit-code"' EXIT
trap 'exit 143' HUP INT TERM
touch crates/marketcow-runtime/src/discovery_source.rs crates/marketcowd/src/discovery_collector.rs crates/marketcowd/src/source_dispatch.rs crates/marketcowd/src/source_publication.rs crates/marketcowd/src/source_websocket.rs crates/marketcowd/src/source_websocket_pipeline.rs
cargo test --offline --locked -j2 -p marketcowd --bin marketcow-discovery-collector -p marketcow-runtime --lib > "$runtime/logs/ws-load-$run-tests.log" 2>&1
cargo test --offline --locked -j2 -p marketcow-polymarket >> "$runtime/logs/ws-load-$run-tests.log" 2>&1
python3 -m unittest tests.test_polymarket_terminal_read >> "$runtime/logs/ws-load-$run-tests.log" 2>&1
python3 -m unittest tests.test_polymarket_live_projection_freshness >> "$runtime/logs/ws-load-$run-tests.log" 2>&1
cargo build --release --offline --locked -j2 --bin marketcow-discovery-collector > "$runtime/logs/ws-load-$run-build.log" 2>&1
python3 scripts/check_u1_async_load.py --run "$run" --profile release --input-mode websocket > "$runtime/logs/async-load-$run.log" 2>&1
