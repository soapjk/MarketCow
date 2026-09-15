#!/bin/bash
set -eu
umask 077
runtime=/mnt/p44pro/marketcow-shadow-v3-runtime/linux
project=/mnt/p44pro/projects/marketcow-shadow-v3
root=$runtime/bounded-scoped-candidate-r1
export BOUNDED_LIVE_RUN=${1:-r2}
collector=marketcow-bounded-live-collector-$BOUNDED_LIVE_RUN
api=marketcow-bounded-live-api-$BOUNDED_LIVE_RUN
proxy=marketcow-bounded-live-proxy-$BOUNDED_LIVE_RUN
export TMPDIR=$runtime/tmp PYTHONPATH=$runtime/read-api-packages:$project/src PYTHONDONTWRITEBYTECODE=1
cleanup() {
 status=$?
 trap - EXIT
 # Keep the proxy available until collector shutdown recovery and disk drain finish.
 systemctl --user stop "$api" || status=1
 systemctl --user stop "$collector" || status=1
 systemctl --user stop "$proxy" || status=1
 for unit in "$api" "$collector" "$proxy"; do
   [ "$(systemctl --user show "$unit" -p ExecMainStatus --value)" = 0 ] || status=1
   [ "$(systemctl --user show "$unit" -p MainPID --value)" = 0 ] || status=1
   [ "$(systemctl --user show "$unit" -p Result --value)" = success ] || status=1
 done
 systemctl --user show "$api" "$collector" "$proxy" -p Id -p Result -p ExecMainStatus -p ActiveState -p MainPID -p MemoryPeak || true
 python3 -c 'from scripts.audit_bounded_live import durable; import json; print("final_durable="+json.dumps(durable()))' || status=1
 printf '%s\n' "$status" > "$runtime/logs/bounded-live-$BOUNDED_LIVE_RUN.exit-code"
 exit "$status"
}
cd "$project"
# Never take over an active listener or a stable unit.
python3 -c 'import socket; sockets=[]
for port in (17890,18897,8794):
 s=socket.socket(); s.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1); s.bind(("127.0.0.1",port)); sockets.append(s)'
trap cleanup EXIT
systemd-run --user --unit="$proxy" -p RuntimeMaxSec=330 -p StandardOutput=null -p StandardError=null \
 "$runtime/polymarket-proxy/mihomo" -d "$runtime/polymarket-proxy" -f "$runtime/polymarket-proxy/config.yaml"
sleep 2
systemd-run --user --unit="$collector" -p RuntimeMaxSec=300 -p TimeoutStopSec=30 -p MemoryMax=3072M -p MemorySwapMax=0 \
 -p StandardOutput=append:$runtime/logs/bounded-live-collector-$BOUNDED_LIVE_RUN.log -p StandardError=append:$runtime/logs/bounded-live-collector-$BOUNDED_LIVE_RUN.log \
 --setenv=HTTPS_PROXY=http://127.0.0.1:17890 --setenv=HTTP_PROXY=http://127.0.0.1:17890 --setenv=NO_PROXY=localhost,127.0.0.1 \
 "$runtime/target/release/marketcow-discovery-collector" --input-mode websocket --root "$root" \
 --plan "$root/rust-scoped-plan-r1.json" --plan-sha256 3461f8b75ad6078e5e1199c4ea1d67c62e29872b17856603bc64764d297bbdff \
 --configured-scope "$root/configured-scope.json" --configured-scope-sha256 4443ccdc6896ed451d9be94e467d8deca2594f69d3f4d6d5f19e63a437acf7df \
 --dependency-plan "$root/live-bridge-plan-r1.json" --dependency-plan-sha256 b108d8795ba17892f6f2b361db341048cae3c2687aa1277dad0be9089d9019ce \
 --bounded-history-bytes 67108864 --expected-market-count 250 --concurrency 16 --request-market-batch-size 20 --response-byte-limit 2097152 --batch-byte-limit 16777216 \
 --persistence-queue-batches 256 --persistence-queue-bytes 67108864 --websocket-shard-tokens 500 --websocket-recovery-concurrency 16 \
 --websocket-confirmation-seconds 0 --poll-seconds 1 --request-timeout-seconds 10 --lifecycle-refresh-seconds 300 \
 --live-listen 127.0.0.1:18897 --live-frame-bytes 67108864 --live-maximum-clients 2
sleep 10
systemctl --user is-active --quiet "$collector"
systemd-run --user --unit="$api" -p RuntimeMaxSec=270 -p MemoryMax=768M -p KillSignal=SIGINT \
 -p StandardOutput=append:$runtime/logs/bounded-live-api-$BOUNDED_LIVE_RUN.log -p StandardError=append:$runtime/logs/bounded-live-api-$BOUNDED_LIVE_RUN.log \
 --setenv=PYTHONPATH="$PYTHONPATH" --setenv=PYTHONDONTWRITEBYTECODE=1 \
 /usr/bin/python3 "$project/scripts/run_polymarket_live_read_api.py" --root "$root" --discovery-root "$runtime/bounded-discovery-candidate-r1" \
 --configured-scope "$root/configured-scope.json" --host 127.0.0.1 --port 8794 --live-stream-uri ws://127.0.0.1:18897 \
 --stable-snapshot-max-book-age-seconds 5 --consumer-maximum-book-age-seconds 5 --minimum-delivery-headroom-seconds 0 \
 --stable-read-wait-seconds 6 --stable-read-poll-seconds .025 --executor-workers 4 --discovery-maximum-book-age-ms 5000 \
 --discovery-maximum-full-sync-bytes 268435456 --live-stream-replay-capacity 4096 --discovery-depth-notional 10 --no-access-log
sleep 10
python3 scripts/audit_bounded_live.py &
observer=$!
PARTIAL_SCOPE_RUN=bounded-live-$BOUNDED_LIVE_RUN timeout 90 python3 scripts/audit_u1_partial_scope.py || { kill "$observer" || true; wait "$observer" || true; exit 1; }
wait "$observer"
