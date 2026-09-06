#!/bin/bash
set -eu
runtime=/mnt/p44pro/marketcow-shadow-v3-runtime/linux
[[ $(findmnt -n -o FSTYPE --target "$runtime") == ext4 ]]
umask 077
trap 'result=$?; printf "%s\n" "$result" > "$runtime/logs/rust-discovery-smoke-r1.exit-code"' EXIT
"$runtime/target/debug/marketcow-discovery-collector" \
    --root "$runtime/discovery-source-r1" \
    --plan "$runtime/discovery-source-r1/rust-source-plan.json" \
    --plan-sha256 655f5536841baa52a7c5680432aa621d2a14f786145a9ae0f1aae70a18f9a807 \
    --input-mode rest-poll --expected-market-count 1000 --concurrency 16 \
    --response-byte-limit 2097152 --persistence-queue-batches 256 --persistence-queue-bytes 67108864 --batch-byte-limit 16777216 \
    --poll-seconds 30 --request-timeout-seconds 10 --request-market-batch-size 1 --cycles 2 \
    > "$runtime/logs/rust-discovery-smoke-r1.log" 2>&1
