#!/bin/sh
set -eu

if [ "${MARKETCOW_CUTOVER_CONFIRMATION:-}" != "I_UNDERSTAND_SINGLE_WRITER_CUTOVER" ]; then
  echo "refusing cutover: explicit confirmation is absent" >&2
  exit 64
fi
if [ "${MARKETCOW_REAL_ORDER_SUBMISSION_ENABLED:-false}" = "true" ]; then
  echo "refusing cutover: real orders must remain disabled" >&2
  exit 65
fi
echo "Cutover is intentionally disabled in the shadow milestone; follow docs/architecture/migration/runbook.md" >&2
exit 69

