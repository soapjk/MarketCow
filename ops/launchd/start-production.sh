#!/bin/sh
set -eu

project_dir="${MARKETCOW_PROJECT_DIR:-/Volumes/T9/projects/marketcow}"
script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
env_file="${MARKETCOW_ENV_FILE:-$script_dir/production.env}"
export MARKETCOW_ENV_FILE="$env_file"

"$script_dir/ensure-production-storage.sh"

python="${MARKETCOW_PYTHON:-$project_dir/.venv/bin/python}"
if [ ! -x "$python" ]; then
    managed_python="$script_dir/atomic-freshness-venv/bin/python"
    [ -x "$managed_python" ] || {
        echo "Missing production Python executable: $python" >&2
        exit 1
    }
    python="$managed_python"
fi

exec "$python" "$project_dir/ops/launchd/run-production.py"
