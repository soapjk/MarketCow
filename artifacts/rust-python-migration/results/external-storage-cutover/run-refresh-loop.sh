#!/bin/sh
set -u

RUN_ONCE='/Users/androidjk/Library/Application Support/MarketCow/polymarket-universe-refresh/t9-external-7f33700/run-one-refresh.sh'
LOG='/Volumes/T9/MarketCow/logs/universe-auto-refresh.log'
ERROR_LOG='/Volumes/T9/MarketCow/logs/universe-auto-refresh.error.log'

while :; do
  if ! "$RUN_ONCE" >>"$LOG" 2>>"$ERROR_LOG"; then
    printf '%s refresh_failed_closed\n' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" >>"$ERROR_LOG"
  fi
  sleep 300
done
