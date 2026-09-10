#!/bin/sh
set -eu

project_dir="${MARKETCOW_PROJECT_DIR:-/Volumes/T9/projects/marketcow}"
script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
env_file="${MARKETCOW_ENV_FILE:-$script_dir/production.env}"
export MARKETCOW_ENV_FILE="$env_file"

python="${MARKETCOW_PYTHON:-$project_dir/.venv/bin/python}"
if [ ! -x "$python" ]; then
    managed_python="$script_dir/atomic-freshness-venv/bin/python"
    [ -x "$managed_python" ] || {
        echo "Missing production Python executable: $python" >&2
        exit 1
    }
    python="$managed_python"
fi

# Resolve the same CLI/environment selection as the supervisor before starting
# any storage processes. Binance-only operation has no stock database dependency.
storage_required=$("$python" "$project_dir/ops/launchd/run-production.py" --storage-required "$@")
if [ "$storage_required" = yes ]; then
    "$script_dir/ensure-production-storage.sh"
elif [ "$storage_required" != no ]; then
    echo "Invalid storage prerequisite response" >&2
    exit 1
fi

exec "$python" "$project_dir/ops/launchd/run-production.py" "$@"
