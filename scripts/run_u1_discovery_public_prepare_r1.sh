#!/bin/bash
set -eu
umask 077
runtime=/mnt/p44pro/marketcow-shadow-v3-runtime/linux
project=/mnt/p44pro/projects/marketcow-shadow-v3
test ! -e "$runtime/logs/discovery-public-prepare-r1.exit-code"
finish() { status=$?; printf '%s\n' "$status" > "$runtime/logs/discovery-public-prepare-r1.exit-code"; }
trap finish EXIT
export PYTHONPATH=$runtime/read-api-packages:$project/src TMPDIR=$runtime/tmp
python3 "$project/scripts/clone_bounded_public_candidate.py" --kind discovery \
  --source "$runtime/bounded-discovery-candidate-r1" --target "$runtime/public-discovery-candidate-r1"
python3 "$project/scripts/prepare_discovery_public_seed.py" \
  --source-root "$runtime/bounded-discovery-candidate-r1" --target-root "$runtime/public-discovery-candidate-r1" \
  --depth-quantities 10 50 100 500 --maximum-book-age-ms 5000
