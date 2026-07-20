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
domain="gui/$(id -u)"

if [ ! -x "$project_dir/.venv/bin/marketcow" ]; then
    echo "Missing executable: $project_dir/.venv/bin/marketcow" >&2
    exit 1
fi
if [ ! -f "$project_dir/.env.production" ]; then
    echo "Missing production configuration: $project_dir/.env.production" >&2
    exit 1
fi

mkdir -p "$HOME/Library/LaunchAgents" "$support_dir" "$log_dir"
plutil -lint "$source_plist"
cp "$source_plist" "$target_plist"
cp "$script_dir/start-production.sh" "$target_launcher"
chmod 700 "$target_launcher"

launchctl bootout "$domain/$label" 2>/dev/null || true
launchctl bootstrap "$domain" "$target_plist"
launchctl enable "$domain/$label"
launchctl kickstart -k "$domain/$label"

echo "Installed $label from $target_plist"
