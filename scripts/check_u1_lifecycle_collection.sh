#!/bin/bash
# Two finite cycles. No API/bridge/controller; proxy is stopped on exit.
set -eu
umask 077
runtime=/mnt/p44pro/marketcow-shadow-v3-runtime/linux
root="$runtime/scoped-live-source-r1"
[[ $(findmnt -n -o FSTYPE --target "$runtime") == ext4 ]]
cd /mnt/p44pro/projects/marketcow-shadow-v3
export CARGO_HOME="$runtime/cargo" CARGO_TARGET_DIR="$runtime/target" TMPDIR="$runtime/tmp"
[[ ! -e "$runtime/logs/lifecycle-check-r1.exit-code" ]]
trap 'result=$?; systemctl --user stop marketcow-polymarket-proxy-lifecycle-r1 >/dev/null 2>&1 || true; printf "%s\n" "$result" > "$runtime/logs/lifecycle-check-r1.exit-code"' EXIT
bash scripts/run_u1_rust_tests.sh r12
cargo build --offline --locked -j2 --bin marketcow-discovery-collector > "$runtime/logs/lifecycle-check-r1-build.log" 2>&1
systemd-run --user --unit=marketcow-polymarket-proxy-lifecycle-r1 --property=UMask=0077 --property=MemoryMax=256M --property=RuntimeMaxSec=180 --property=StandardOutput=append:"$runtime/logs/proxy-lifecycle-r1.log" --property=StandardError=append:"$runtime/logs/proxy-lifecycle-r1.log" "$runtime/polymarket-proxy/mihomo" -d "$runtime/polymarket-proxy" -f "$runtime/polymarket-proxy/config.yaml"
env -u ALL_PROXY -u all_proxy HTTPS_PROXY=http://127.0.0.1:17890 HTTP_PROXY=http://127.0.0.1:17890 NO_PROXY=127.0.0.1,localhost,::1 \
    /usr/bin/time -v "$runtime/target/debug/marketcow-discovery-collector" \
    --root "$root" --plan "$root/rust-scoped-plan-r1.json" \
    --plan-sha256 603396c05129df8c598ee941fccd9b6894081719af67057c762f8cade1035389 \
    --configured-scope "$root/configured-scope.json" \
    --configured-scope-sha256 1f95a4d8cbe99b40a1a24571f47994ce020640a9e12c03f13ff13f796eb1e7f6 \
    --dependency-plan "$root/live-bridge-plan-r1.json" \
    --dependency-plan-sha256 0958672fbb466a17803779479567fa1efceb889d38749496421630bf060cfb85 \
    --input-mode rest-poll --expected-market-count 100 --concurrency 10 --request-market-batch-size 10 \
    --response-byte-limit 2097152 --persistence-queue-batches 256 --persistence-queue-bytes 67108864 --batch-byte-limit 16777216 \
    --lifecycle-refresh-seconds 300 \
    --poll-seconds 1 --request-timeout-seconds 10 --cycles 2 \
    > "$runtime/logs/lifecycle-check-r1.log" 2>&1
