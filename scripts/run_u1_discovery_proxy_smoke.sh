#!/bin/bash
# Only this Polymarket process receives proxy environment variables.
set -eu
runtime=/mnt/p44pro/marketcow-shadow-v3-runtime/linux
[[ $(findmnt -n -o FSTYPE --target "$runtime") == ext4 ]]
umask 077
run=${1:?explicit unique run name required}
[[ "$run" =~ ^[a-z0-9-]+$ ]]
log="$runtime/logs/rust-discovery-proxy-$run.log"
result_file="$runtime/logs/rust-discovery-proxy-$run.exit-code"
[[ ! -e "$log" && ! -e "$result_file" ]]
trap 'result=$?; printf "%s\n" "$result" > "$result_file"' EXIT
env -u ALL_PROXY -u all_proxy \
    HTTP_PROXY=http://127.0.0.1:17890 HTTPS_PROXY=http://127.0.0.1:17890 \
    http_proxy=http://127.0.0.1:17890 https_proxy=http://127.0.0.1:17890 \
    NO_PROXY=127.0.0.1,localhost,::1 no_proxy=127.0.0.1,localhost,::1 \
    /usr/bin/time -v "$runtime/target/debug/marketcow-discovery-collector" \
    --root "$runtime/discovery-source-r1" \
    --plan "$runtime/discovery-source-r1/rust-source-plan.json" \
    --plan-sha256 655f5536841baa52a7c5680432aa621d2a14f786145a9ae0f1aae70a18f9a807 \
    --input-mode rest-poll --expected-market-count 1000 --concurrency 16 \
    --response-byte-limit 2097152 --persistence-queue-batches 256 --persistence-queue-bytes 67108864 --batch-byte-limit 16777216 \
    --poll-seconds 30 --request-timeout-seconds 10 --request-market-batch-size 1 --cycles 2 \
    > "$log" 2>&1
