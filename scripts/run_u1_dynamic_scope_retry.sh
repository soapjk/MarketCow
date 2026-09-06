#!/bin/bash
set -eu
umask 077
runtime=/mnt/p44pro/marketcow-shadow-v3-runtime/linux
[[ $(findmnt -n -o FSTYPE --target "$runtime") == ext4 ]]
cd /mnt/p44pro/projects/marketcow-shadow-v3
export PYTHONDONTWRITEBYTECODE=1 PYTHONPATH="$runtime/read-api-packages:/mnt/p44pro/projects/marketcow-shadow-v3/src"
[[ ! -e "$runtime/logs/dynamic-check-r2.exit-code" ]]
trap 'result=$?; printf "%s\n" "$result" > "$runtime/logs/dynamic-check-r2.exit-code"' EXIT
python3 -m unittest tests.test_polymarket_scope_activation tests.test_relocate_polymarket_live_source -v > "$runtime/logs/dynamic-check-r2-tests.log" 2>&1
python3 scripts/check_u1_dynamic_scope.py --run r2 --resume > "$runtime/logs/dynamic-check-r2.log" 2>&1
