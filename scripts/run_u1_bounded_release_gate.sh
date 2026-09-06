#!/bin/bash
set -eu
umask 077
runtime=/mnt/p44pro/marketcow-shadow-v3-runtime/linux
project=/mnt/p44pro/projects/marketcow-shadow-v3
export CARGO_HOME=$runtime/cargo CARGO_TARGET_DIR=$runtime/target TMPDIR=$runtime/tmp CARGO_INCREMENTAL=0
export PYTHONPATH=$runtime/read-api-packages:$project/src PYTHONDONTWRITEBYTECODE=1
export BOUNDED_GATE_RUN=${1:-r2}
api=marketcow-bounded-http-api-$BOUNDED_GATE_RUN
proxy=marketcow-bounded-http-proxy-$BOUNDED_GATE_RUN
cleanup() {
 result=$?
 trap - EXIT
 systemctl --user stop "$api" || true
 systemctl --user stop "$proxy" || true
 printf '%s\n' "$result" > "$runtime/logs/bounded-release-gate-$BOUNDED_GATE_RUN.exit-code"
 exit "$result"
}
trap cleanup EXIT
cd "$project"
cargo test --offline --locked --release -j2 -p marketcow-runtime bounded_history -- --test-threads=1
cargo test --offline --locked --release -j2 -p marketcow-runtime bounded_history_sigkill -- --ignored --test-threads=1
cargo test --offline --locked --release -j2 -p marketcowd --bin marketcow-discovery-collector
cargo build --offline --locked --release -j2 --bin marketcow-discovery-collector
sha256sum "$runtime/target/release/marketcow-discovery-collector"
python3 -m unittest tests.test_bounded_diagnostic_log -q
systemd-run --user --unit="$proxy" -p RuntimeMaxSec=180 -p StandardOutput=null -p StandardError=null \
 "$runtime/polymarket-proxy/mihomo" -d "$runtime/polymarket-proxy" -f "$runtime/polymarket-proxy/config.yaml"
systemd-run --user --unit="$api" -p RuntimeMaxSec=180 -p MemoryMax=768M -p KillSignal=SIGINT \
 -p StandardOutput=append:$runtime/logs/bounded-http-api-$BOUNDED_GATE_RUN.log -p StandardError=append:$runtime/logs/bounded-http-api-$BOUNDED_GATE_RUN.log \
 --setenv=PYTHONPATH="$PYTHONPATH" --setenv=PYTHONDONTWRITEBYTECODE=1 \
 /usr/bin/python3 "$project/scripts/run_polymarket_live_read_api.py" \
 --root "$runtime/bounded-scoped-candidate-r1" --discovery-root "$runtime/bounded-discovery-candidate-r1" \
 --host 127.0.0.1 --port 8794 --stable-snapshot-max-book-age-seconds 5 --consumer-maximum-book-age-seconds 5 --minimum-delivery-headroom-seconds 0 \
 --stable-read-wait-seconds 6 --stable-read-poll-seconds .025 --executor-workers 4 \
 --discovery-maximum-book-age-ms 5000 --discovery-maximum-full-sync-bytes 268435456 \
 --discovery-depth-notional 10 --discovery-depth-notional 50 --discovery-depth-notional 100 --discovery-depth-notional 500 --no-access-log
sleep 5
python3 scripts/audit_bounded_discovery_http_ws.py
