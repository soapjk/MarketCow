#!/bin/sh
set -eu

RUNTIME_ROOT='/Users/androidjk/Library/Application Support/MarketCow/polymarket-universe-refresh/t9-external-7f33700'
EXTERNAL_ROOT='/Volumes/T9/MarketCow'

test -d "$EXTERNAL_ROOT/runtime/storage"
test -d "$EXTERNAL_ROOT/runtime/registry"
test -w "$EXTERNAL_ROOT/refresh/work"
export MARKETCOW_RUST_ADMIN_TOKEN='marketcow-rust-clean-generation18-local'
export HTTPS_PROXY='http://127.0.0.1:7890'
export PYTHONPATH="$RUNTIME_ROOT/runtime"
cd "$RUNTIME_ROOT/runtime"
exec /usr/bin/python3 -m scripts.migration.auto_refresh_polymarket_dynamic_universe \
  --config "$EXTERNAL_ROOT/config/auto-refresh-config.json"
