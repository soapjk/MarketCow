#!/bin/sh
set -eu

project_dir="${MARKETCOW_PROJECT_DIR:-/Volumes/T9/projects/marketcow}"
script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
env_file="${MARKETCOW_ENV_FILE:-$script_dir/production.env}"
export MARKETCOW_ENV_FILE="$env_file"

"$script_dir/ensure-production-storage.sh"

exec "$project_dir/.venv/bin/python" "$script_dir/run-production.py"
