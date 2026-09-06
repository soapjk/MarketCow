#!/usr/bin/env bash
set -euo pipefail

runtime=/mnt/p44pro/marketcow-shadow-v3-runtime/linux
source_root=/mnt/p44pro/projects/marketcow-shadow-v3
root="$runtime/dynamic-live-candidate-r1"
logs="$runtime/logs"
mkdir -p "$logs"

exec 9>"$runtime/paper-data-plane.lock"
flock -n 9 || { echo "paper data plane is already running" >&2; exit 73; }
printf '%s\n' "$$" >"$runtime/paper-data-plane.pid"

proxy_pid=
collector_pid=
api_pid=
cleanup() {
    trap - EXIT INT TERM
    for pid in "$api_pid" "$collector_pid" "$proxy_pid"; do
        if [[ -n "$pid" ]]; then kill -INT "$pid" 2>/dev/null || true; fi
    done
    for pid in "$api_pid" "$collector_pid" "$proxy_pid"; do
        if [[ -n "$pid" ]]; then wait "$pid" 2>/dev/null || true; fi
    done
    rm -f "$runtime/paper-data-plane.pid"
}
trap cleanup EXIT INT TERM

"$runtime/polymarket-proxy/mihomo" \
    -d "$runtime/polymarket-proxy" \
    -f "$runtime/polymarket-proxy/config.yaml" \
    >>"$logs/paper-proxy.log" 2>&1 &
proxy_pid=$!

for _ in {1..100}; do
    if (exec 8<>/dev/tcp/127.0.0.1/17890) 2>/dev/null; then
        exec 8>&-
        break
    fi
    kill -0 "$proxy_pid"
    sleep .1
done

env -u ALL_PROXY -u all_proxy -u HTTPS_PROXY -u https_proxy \
    -u HTTP_PROXY -u http_proxy \
    HTTPS_PROXY=http://127.0.0.1:17890 \
    HTTP_PROXY=http://127.0.0.1:17890 \
    NO_PROXY=127.0.0.1,localhost,::1 \
    "$runtime/target/release/marketcow-discovery-collector" \
    --input-mode websocket \
    --root "$root" \
    --plan "$root/rust-scoped-plan-r1.json" \
    --plan-sha256 603396c05129df8c598ee941fccd9b6894081719af67057c762f8cade1035389 \
    --configured-scope "$root/configured-scope.json" \
    --configured-scope-sha256 1f95a4d8cbe99b40a1a24571f47994ce020640a9e12c03f13ff13f796eb1e7f6 \
    --dependency-plan "$root/live-bridge-plan-r1.json" \
    --dependency-plan-sha256 a6802894c3d12c4d42e881e0af1d7442d7af4df59c7219e348429422b6169dfd \
    --expected-market-count 100 \
    --concurrency 16 --request-market-batch-size 20 \
    --response-byte-limit 2097152 --batch-byte-limit 16777216 \
    --persistence-queue-batches 256 --persistence-queue-bytes 67108864 \
    --websocket-shard-tokens 50 --websocket-recovery-concurrency 16 \
    --websocket-confirmation-seconds 1 \
    --poll-seconds 1 --request-timeout-seconds 10 \
    --lifecycle-refresh-seconds 300 \
    --live-listen 127.0.0.1:18896 \
    --live-frame-bytes 33554432 --live-maximum-clients 2 \
    >>"$logs/paper-collector.log" 2>&1 &
collector_pid=$!

env PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH="$runtime/read-api-packages:$source_root/src" \
    /usr/bin/python3 "$source_root/scripts/run_polymarket_live_read_api.py" \
    --root "$root" --discovery-root "$runtime/discovery-source-r1" \
    --configured-scope "$root/configured-scope.json" \
    --host 192.168.124.3 --port 8793 \
    --live-stream-uri ws://127.0.0.1:18896 \
    --stable-snapshot-max-book-age-seconds 5 \
    --consumer-maximum-book-age-seconds 5 \
    --minimum-delivery-headroom-seconds 0 \
    --stable-read-wait-seconds 6 --stable-read-poll-seconds .025 \
    --executor-workers 2 --discovery-maximum-book-age-ms 5000 \
    --discovery-maximum-full-sync-bytes 268435456 \
    --live-stream-replay-capacity 2048 \
    --discovery-depth-notional 10 --discovery-depth-notional 50 \
    --discovery-depth-notional 100 --discovery-depth-notional 500 \
    --no-access-log >>"$logs/paper-read-api.log" 2>&1 &
api_pid=$!

wait -n "$proxy_pid" "$collector_pid" "$api_pid"
exit 1
