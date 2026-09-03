#!/bin/sh
set -eu

if [ "$#" -ne 1 ]; then
  echo "usage: $0 /absolute/path/to/result.json" >&2
  exit 64
fi

output=$1
case "$output" in
  /*) ;;
  *) echo "result path must be absolute" >&2; exit 64 ;;
esac

command -v cargo >/dev/null
command -v initdb >/dev/null
command -v pg_ctl >/dev/null
command -v createdb >/dev/null
command -v jq >/dev/null

root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
pg_root=$(mktemp -d /tmp/marketcow-instrument-pg.XXXXXX)
pg_port=${MARKETCOW_EPHEMERAL_INSTRUMENT_POSTGRES_PORT:-55433}
pg_started=false

cleanup() {
  if [ "$pg_started" = true ]; then
    pg_ctl -D "$pg_root/data" stop >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT INT TERM

cd "$root"
initdb -D "$pg_root/data" --auth=trust --username=marketcow_test >/dev/null
pg_ctl -D "$pg_root/data" -l "$pg_root/postgres.log" \
  -o "-h 127.0.0.1 -p $pg_port -k $pg_root" start >/dev/null
pg_started=true
createdb -h 127.0.0.1 -p "$pg_port" -U marketcow_test marketcow_test
MARKETCOW_TEST_POSTGRES_DSN="postgresql://marketcow_test@127.0.0.1:$pg_port/marketcow_test" \
  cargo test -p marketcow-storage \
  postgres_instrument_repository_round_trip_when_test_dsn_is_configured \
  -- --ignored --nocapture
pg_ctl -D "$pg_root/data" stop >/dev/null
pg_started=false

mkdir -p "$(dirname -- "$output")"
temporary="$output.tmp"
jq -n \
  --arg schema_version "marketcow.instrument-postgres-integration.v1" \
  --arg git_commit "$(git rev-parse HEAD)" \
  --arg postgres_version "$(postgres --version)" \
  --arg pg_root "$pg_root" \
  '{schema_version:$schema_version,git_commit:$git_commit,passed:true,
    postgres:{version:$postgres_version,
      test:"postgres_instrument_repository_round_trip_when_test_dsn_is_configured",
      passed:true,ephemeral_root:$pg_root,
      assertions:["safe_forward_migration_idempotent","exact_decimal_round_trip",
        "provider_mapping_resolves","stale_mapping_removed",
        "cross_instrument_mapping_conflict_rejected"]},
    tradude_manages_marketcow:false}' >"$temporary"
mv "$temporary" "$output"
cat "$output"
