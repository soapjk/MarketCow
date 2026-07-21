#!/bin/sh
set -eu

project_dir="/Volumes/T9/projects/marketcow"
cd "$project_dir"
profile="${MARKETCOW_PROFILE:-production}"
exec "$project_dir/.venv/bin/python" -m marketcow \
    --profile "$profile" start --host 127.0.0.1 --port 8790
