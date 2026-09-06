#!/bin/bash
set -eu
umask 077
runtime=/mnt/p44pro/marketcow-shadow-v3-runtime/linux
project=/mnt/p44pro/projects/marketcow-shadow-v3
root=$runtime/scoped-live-source-r1
export PARTIAL_SCOPE_RUN=${1:-r1}
mode=${2:-audit}
collector_seconds=150
api_seconds=120
api_host=127.0.0.1
if [ "$mode" = joint ]; then
  collector_seconds=720
  api_seconds=660
  api_host=192.168.124.3
elif [ "$mode" != audit ]; then
  exit 2
fi
collector=marketcow-partial-scope-collector-$PARTIAL_SCOPE_RUN
api=marketcow-partial-scope-api-$PARTIAL_SCOPE_RUN
export CARGO_HOME=$runtime/cargo CARGO_TARGET_DIR=$runtime/target TMPDIR=$runtime/tmp
export PYTHONPATH=$runtime/read-api-packages:$project/src PYTHONDONTWRITEBYTECODE=1
cleanup() {
  status=$?
  trap - EXIT
  systemctl --user show "$collector" "$api" -p Id -p MainPID -p MemoryPeak -p MemoryCurrent || true
  systemctl --user stop "$api" "$collector" || true
  printf '%s\n' "$status" > "$runtime/logs/partial-scope-$PARTIAL_SCOPE_RUN.exit-code"
  exit "$status"
}
trap cleanup EXIT
cd "$project"
cargo test --offline --locked -j2 -p marketcow-core -p marketcowd --lib --bin marketcow-discovery-collector
python3 -m unittest tests.test_polymarket_partial_scope tests.test_polymarket_scope_activation -q
cargo build --offline --locked --release -j2 --bin marketcow-discovery-collector
sha256sum "$runtime/target/release/marketcow-discovery-collector"
systemd-run --user --unit="$collector" -p MemoryMax=3072M -p MemorySwapMax=0 -p RuntimeMaxSec="$collector_seconds" -p TimeoutStopSec=30 \
  -p StandardOutput=append:$runtime/logs/partial-scope-collector-$PARTIAL_SCOPE_RUN.log \
  -p StandardError=append:$runtime/logs/partial-scope-collector-$PARTIAL_SCOPE_RUN.log \
  --setenv=HTTPS_PROXY=http://127.0.0.1:17890 --setenv=HTTP_PROXY=http://127.0.0.1:17890 --setenv=NO_PROXY=localhost,127.0.0.1 \
  "$runtime/target/release/marketcow-discovery-collector" --input-mode websocket --root "$root" \
  --plan "$root/rust-scoped-plan-r1.json" --plan-sha256 3461f8b75ad6078e5e1199c4ea1d67c62e29872b17856603bc64764d297bbdff \
  --configured-scope "$root/configured-scope.json" --configured-scope-sha256 4443ccdc6896ed451d9be94e467d8deca2594f69d3f4d6d5f19e63a437acf7df \
  --dependency-plan "$root/live-bridge-plan-r1.json" --dependency-plan-sha256 d449f3aa8ed82b81b6ec66be7e4bef1602219ef0eda55abd41a82d38e219ca45 \
  --expected-market-count 250 --concurrency 16 --request-market-batch-size 20 --response-byte-limit 2097152 --batch-byte-limit 16777216 \
  --persistence-queue-batches 256 --persistence-queue-bytes 67108864 --websocket-shard-tokens 500 --websocket-recovery-concurrency 16 \
  --websocket-confirmation-seconds 1 --poll-seconds 1 --request-timeout-seconds 10 --lifecycle-refresh-seconds 300 \
  --live-listen 127.0.0.1:18897 --live-frame-bytes 67108864 --live-maximum-clients 2
sleep 10
systemctl --user is-active --quiet "$collector"
systemd-run --user --unit="$api" -p MemoryMax=768M -p RuntimeMaxSec="$api_seconds" -p KillSignal=SIGINT \
  -p StandardOutput=append:$runtime/logs/partial-scope-api-$PARTIAL_SCOPE_RUN.log -p StandardError=append:$runtime/logs/partial-scope-api-$PARTIAL_SCOPE_RUN.log \
  --setenv=PYTHONPATH="$PYTHONPATH" --setenv=PYTHONDONTWRITEBYTECODE=1 \
  /usr/bin/python3 "$project/scripts/run_polymarket_live_read_api.py" --root "$root" --discovery-root "$runtime/discovery-source-r1" \
  --configured-scope "$root/configured-scope.json" --host "$api_host" --port 8794 --live-stream-uri ws://127.0.0.1:18897 \
  --stable-snapshot-max-book-age-seconds 5 --consumer-maximum-book-age-seconds 5 --minimum-delivery-headroom-seconds 0 \
  --stable-read-wait-seconds 6 --stable-read-poll-seconds .025 --executor-workers 4 --discovery-maximum-book-age-ms 5000 \
  --discovery-maximum-full-sync-bytes 268435456 --live-stream-replay-capacity 4096 --discovery-depth-notional 10 --no-access-log
sleep 10
systemctl --user is-active --quiet "$collector" "$api"
if [ "$mode" = joint ]; then
  sleep 600
else
  timeout 90 python3 "$project/scripts/audit_u1_partial_scope.py"
fi
