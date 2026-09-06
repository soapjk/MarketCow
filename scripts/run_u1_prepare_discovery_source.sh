#!/bin/bash
set -eu
runtime=/mnt/p44pro/marketcow-shadow-v3-runtime/linux
[[ $(findmnt -n -o FSTYPE --target "$runtime") == ext4 ]]
umask 077
export TMPDIR="$runtime/tmp"
trap 'result=$?; printf "%s\n" "$result" > "$runtime/logs/prepare-discovery-source-r1.exit-code"' EXIT
python3 /mnt/p44pro/projects/marketcow-shadow-v3/scripts/prepare_rust_discovery_source.py \
    --source /mnt/p44pro/data/marketcow/shadow-v3/prediction-markets/polymarket-discovery \
    --target "$runtime/discovery-source-r1" \
    --original-root /Volumes/T9/data/marketcow/shadow-v3-isolated-20260905/prediction-markets/polymarket-discovery \
    > "$runtime/logs/prepare-discovery-source-r1.log" 2>&1
