#!/bin/sh
set -eu

label="com.marketcow.production"
target_plist="$HOME/Library/LaunchAgents/$label.plist"
target_launcher="$HOME/Library/Application Support/MarketCow/start-production.sh"
domain="gui/$(id -u)"

launchctl bootout "$domain/$label" 2>/dev/null || true
rm -f "$target_plist"
rm -f "$target_launcher"
echo "Uninstalled $label"
