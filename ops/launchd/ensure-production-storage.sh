#!/bin/sh
set -eu

project_dir="/Volumes/T9/projects/marketcow"
script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
runtime_dir="$project_dir/data-production/runtime"
postgres_bin="/opt/homebrew/opt/postgresql@17/bin"
clickhouse_bin="/Volumes/T9/posthog-native/bin/clickhouse"
postgres_dir="$runtime_dir/postgres"
clickhouse_dir="$runtime_dir/clickhouse"

set -a
. "$project_dir/.env.production"
set +a

mkdir -p "$postgres_dir" "$clickhouse_dir/data" "$clickhouse_dir/tmp" \
    "$clickhouse_dir/user_files" "$clickhouse_dir/format_schemas"

if [ ! -f "$postgres_dir/PG_VERSION" ]; then
    "$postgres_bin/initdb" -D "$postgres_dir" -U marketcow --auth=trust --no-locale
fi
if ! "$postgres_bin/pg_isready" -h 127.0.0.1 -p 55492 -q; then
    "$postgres_bin/pg_ctl" -D "$postgres_dir" -l "$postgres_dir/server.log" \
        -o "-h 127.0.0.1 -p 55492" start
fi
if ! "$postgres_bin/psql" -h 127.0.0.1 -p 55492 -U marketcow -d postgres \
    -tAc "SELECT 1 FROM pg_database WHERE datname='marketcow_production'" | grep -q 1; then
    "$postgres_bin/createdb" -h 127.0.0.1 -p 55492 -U marketcow marketcow_production
fi

if ! curl -fsS --max-time 2 http://127.0.0.1:18192/ping >/dev/null 2>&1; then
    "$clickhouse_bin" server \
        --config-file="$script_dir/clickhouse-production.xml" \
        --pid-file="$clickhouse_dir/clickhouse.pid" --daemon
fi

i=0
until curl -fsS --max-time 2 http://127.0.0.1:18192/ping >/dev/null 2>&1; do
    i=$((i + 1))
    [ "$i" -lt 30 ] || { echo "ClickHouse did not become ready" >&2; exit 1; }
    sleep 1
done
