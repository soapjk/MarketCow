#!/bin/sh
set -eu

if [ "${MARKETCOW_ROLLBACK_CONFIRMATION:-}" != "I_UNDERSTAND_SINGLE_WRITER_ROLLBACK" ]; then
  echo "refusing rollback: explicit confirmation is absent" >&2
  exit 64
fi
echo "Rollback requires a recorded WAL boundary and MarketCow operator; no state was changed." >&2
exit 69

