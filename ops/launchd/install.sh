#!/bin/sh
set -eu

label="com.marketcow.production"
project_dir="/Volumes/T9/projects/marketcow"
script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
source_plist="$script_dir/$label.plist"
target_plist="$HOME/Library/LaunchAgents/$label.plist"
support_dir="$HOME/Library/Application Support/MarketCow"
log_dir="$HOME/Library/Logs/MarketCow"
target_launcher="$support_dir/start-production.sh"
target_storage_launcher="$support_dir/ensure-production-storage.sh"
target_clickhouse_config="$support_dir/clickhouse-production.xml"
target_env="$support_dir/production.env"
target_runner="$support_dir/run-production.py"
domain="gui/$(id -u)"

production_python="${MARKETCOW_PYTHON:-$project_dir/.venv/bin/python}"
if [ ! -x "$production_python" ]; then
    managed_python="$support_dir/atomic-freshness-venv/bin/python"
    [ -x "$managed_python" ] || {
        echo "Missing MarketCow production Python executable" >&2
        exit 1
    }
    production_python="$managed_python"
fi
PYTHONPATH="$project_dir/src" "$production_python" -c 'import marketcow'
if [ ! -f "$project_dir/.env.production" ]; then
    echo "Missing production configuration: $project_dir/.env.production" >&2
    exit 1
fi

mkdir -p "$HOME/Library/LaunchAgents" "$support_dir" "$log_dir"
plutil -lint "$source_plist"
cp "$source_plist" "$target_plist"
cp "$script_dir/start-production.sh" "$target_launcher"
cp "$script_dir/ensure-production-storage.sh" "$target_storage_launcher"
cp "$script_dir/clickhouse-production.xml" "$target_clickhouse_config"
cp "$project_dir/.env.production" "$target_env"
cp "$script_dir/run-production.py" "$target_runner"
chmod 700 "$target_launcher" "$target_storage_launcher" "$target_runner"
chmod 600 "$target_clickhouse_config" "$target_env"

launchctl bootout "$domain/$label" 2>/dev/null || true
launchctl enable "$domain/$label"
attempt=0
until launchctl bootstrap "$domain" "$target_plist"; do
    attempt=$((attempt + 1))
    [ "$attempt" -lt 5 ] || {
        echo "Unable to bootstrap $label after $attempt attempts" >&2
        exit 1
    }
    sleep 1
done
launchctl kickstart -k "$domain/$label"

echo "Installed $label from $target_plist"
