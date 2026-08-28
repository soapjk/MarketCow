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
command -v docker >/dev/null
command -v curl >/dev/null
command -v jq >/dev/null

root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
pg_root=$(mktemp -d /tmp/marketcow-storage-pg.XXXXXX)
pg_port=${MARKETCOW_EPHEMERAL_POSTGRES_PORT:-55432}
clickhouse_port=${MARKETCOW_EPHEMERAL_CLICKHOUSE_PORT:-58123}
container_name="marketcow-storage-test-$$"
clickhouse_image=${MARKETCOW_TEST_CLICKHOUSE_IMAGE:-clickhouse/clickhouse-server:25.8-alpine}
pg_started=false
clickhouse_started=false

cleanup() {
  if [ "$pg_started" = true ]; then
    pg_ctl -D "$pg_root/data" stop >/dev/null 2>&1 || true
  fi
  if [ "$clickhouse_started" = true ]; then
    docker stop "$container_name" >/dev/null 2>&1 || true
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
  postgres_job_repository_round_trip_when_test_dsn_is_configured -- --ignored --nocapture
pg_ctl -D "$pg_root/data" stop >/dev/null
pg_started=false

docker image inspect "$clickhouse_image" >/dev/null
docker run --detach --rm --name "$container_name" \
  -p "127.0.0.1:$clickhouse_port:8123" \
  -e CLICKHOUSE_DB=marketcow_test \
  -e CLICKHOUSE_USER=marketcow_test \
  -e CLICKHOUSE_PASSWORD=marketcow_test_password \
  -e CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1 \
  "$clickhouse_image" >/dev/null
clickhouse_started=true

ready=false
attempt=0
while [ "$attempt" -lt 20 ]; do
  if curl --silent --fail --max-time 1 \
    -u marketcow_test:marketcow_test_password \
    "http://127.0.0.1:$clickhouse_port/ping" >/dev/null; then
    ready=true
    break
  fi
  attempt=$((attempt + 1))
  sleep 1
done
if [ "$ready" != true ]; then
  echo "ephemeral ClickHouse did not become ready" >&2
  exit 1
fi

MARKETCOW_TEST_CLICKHOUSE_URL="http://127.0.0.1:$clickhouse_port" \
MARKETCOW_TEST_CLICKHOUSE_DATABASE=marketcow_test \
MARKETCOW_TEST_CLICKHOUSE_USERNAME=marketcow_test \
MARKETCOW_TEST_CLICKHOUSE_PASSWORD=marketcow_test_password \
  cargo test -p marketcow-storage \
  clickhouse_quote_round_trip_when_test_endpoint_is_configured -- --ignored --nocapture
docker stop "$container_name" >/dev/null
clickhouse_started=false

mkdir -p "$(dirname -- "$output")"
temporary="$output.tmp"
jq -n \
  --arg schema_version "marketcow.storage-integration.v1" \
  --arg git_commit "$(git rev-parse HEAD)" \
  --arg postgres_version "$(postgres --version)" \
  --arg clickhouse_image "$clickhouse_image" \
  --arg clickhouse_image_id "$(docker image inspect --format '{{.Id}}' "$clickhouse_image")" \
  --arg pg_root "$pg_root" \
  '{schema_version:$schema_version,git_commit:$git_commit,passed:true,
    postgres:{version:$postgres_version,test:"postgres_job_repository_round_trip_when_test_dsn_is_configured",passed:true,ephemeral_root:$pg_root},
    clickhouse:{image:$clickhouse_image,image_id:$clickhouse_image_id,test:"clickhouse_quote_round_trip_when_test_endpoint_is_configured",passed:true},
    real_order_submission_enabled:false,tradude_manages_marketcow:false}' >"$temporary"
mv "$temporary" "$output"
cat "$output"
