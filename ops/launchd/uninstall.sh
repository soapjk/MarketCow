#!/bin/sh
set -eu

label="com.marketcow.production"
target_plist="$HOME/Library/LaunchAgents/$label.plist"
target_launcher="$HOME/Library/Application Support/MarketCow/start-production.sh"
target_storage_launcher="$HOME/Library/Application Support/MarketCow/ensure-production-storage.sh"
target_clickhouse_config="$HOME/Library/Application Support/MarketCow/clickhouse-production.xml"
target_env="$HOME/Library/Application Support/MarketCow/production.env"
domain="gui/$(id -u)"

launchctl bootout "$domain/$label" 2>/dev/null || true
rm -f "$target_plist"
rm -f "$target_launcher"
rm -f "$target_storage_launcher"
rm -f "$target_clickhouse_config"
rm -f "$target_env"
echo "Uninstalled $label"
