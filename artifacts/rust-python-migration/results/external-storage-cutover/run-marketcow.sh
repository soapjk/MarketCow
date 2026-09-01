#!/bin/sh
set -eu

EXTERNAL_ROOT='/Volumes/T9/MarketCow'
BINARY='/Users/androidjk/Library/Application Support/MarketCow/runtime/7f33700-external-storage/marketcow'
LOG="$EXTERNAL_ROOT/logs/marketcow-7f33700-18872.log"

test -d "$EXTERNAL_ROOT/runtime/storage"
test -d "$EXTERNAL_ROOT/runtime/registry"
test -w "$EXTERNAL_ROOT/runtime/storage"
test -x "$BINARY"
export HTTPS_PROXY='http://127.0.0.1:7890'
export MARKETCOW_BINARY_COMMIT='7f3370028551b1524446d60cef2a7e1eb81359c9'
export MARKETCOW_POLYMARKET_LIVE_ENABLED='true'
export MARKETCOW_POLYMARKET_SCOPE_FILE="$EXTERNAL_ROOT/runtime/registry/cb61239bf22515153f8bdb8864d401de2197c58f3eb964264ed2eae2861d3d34.json"
export MARKETCOW_POLYMARKET_SCOPE_REGISTRY_ROOT="$EXTERNAL_ROOT/runtime/registry"
export MARKETCOW_REAL_ORDER_SUBMISSION_ENABLED='false'
export MARKETCOW_RUST_ADMIN_TOKEN='marketcow-rust-clean-generation18-local'
export MARKETCOW_RUST_BIND='127.0.0.1:18872'
export MARKETCOW_RUST_MAX_BOOK_AGE_MS='3600000'
export MARKETCOW_RUST_PROFILE='test'
export MARKETCOW_RUST_SCOPE_ID='cb61239bf22515153f8bdb8864d401de2197c58f3eb964264ed2eae2861d3d34'
export MARKETCOW_RUST_SHADOW='true'
export MARKETCOW_RUST_STORAGE_ROOT="$EXTERNAL_ROOT/runtime/storage"
export RUST_LOG='info'
exec "$BINARY" serve >>"$LOG" 2>&1
