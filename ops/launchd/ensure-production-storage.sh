#!/bin/sh
set -eu

project_dir="${MARKETCOW_PROJECT_DIR:-/Volumes/T9/projects/marketcow}"
script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
runtime_dir="${MARKETCOW_RUNTIME_DIR:-/Volumes/T9/data/marketcow/production/runtime}"
postgres_bin="${MARKETCOW_POSTGRES_BIN:-/opt/homebrew/opt/postgresql@17/bin}"
clickhouse_bin="${MARKETCOW_CLICKHOUSE_BIN:-/Volumes/T9/posthog-native/bin/clickhouse}"
postgres_dir="$runtime_dir/postgres"
clickhouse_dir="$runtime_dir/clickhouse"
postgres_host="${MARKETCOW_POSTGRES_HOST:-127.0.0.1}"
postgres_port="${MARKETCOW_POSTGRES_PORT:-55492}"
postgres_user="${MARKETCOW_POSTGRES_USER:-marketcow}"
postgres_database="${MARKETCOW_POSTGRES_DATABASE:-marketcow_production}"
clickhouse_ping_url="${MARKETCOW_CLICKHOUSE_PING_URL:-http://127.0.0.1:18192/ping}"
ready_attempts="${MARKETCOW_STORAGE_READY_ATTEMPTS:-30}"
ready_interval="${MARKETCOW_STORAGE_READY_INTERVAL:-1}"
env_file="${MARKETCOW_ENV_FILE:-$project_dir/.env.production}"
clickhouse_config="${MARKETCOW_CLICKHOUSE_CONFIG:-$script_dir/clickhouse-production.xml}"

read_env_value() {
    env_key="$1"
    env_value=""
    while IFS= read -r env_line || [ -n "$env_line" ]; do
        case "$env_line" in
            "$env_key="*) env_value=${env_line#*=}; break ;;
        esac
    done < "$env_file"
    case "$env_value" in
        \'*\') env_value=${env_value#\'}; env_value=${env_value%\'} ;;
        \"*\") env_value=${env_value#\"}; env_value=${env_value%\"} ;;
    esac
    printf '%s' "$env_value"
}

require_executable() {
    [ -x "$1" ] || { echo "Missing executable: $1" >&2; exit 1; }
}

wait_for_postgres() {
    attempt=0
    until "$postgres_bin/pg_isready" -h "$postgres_host" -p "$postgres_port" -q; do
        attempt=$((attempt + 1))
        [ "$attempt" -lt "$ready_attempts" ] || {
            echo "PostgreSQL did not become ready at $postgres_host:$postgres_port" >&2
            exit 1
        }
        sleep "$ready_interval"
    done
}

wait_for_clickhouse() {
    attempt=0
    until clickhouse_is_ready; do
        attempt=$((attempt + 1))
        [ "$attempt" -lt "$ready_attempts" ] || {
            echo "ClickHouse did not become ready at $clickhouse_ping_url" >&2
            exit 1
        }
        sleep "$ready_interval"
    done
}

clickhouse_is_ready() {
    curl -fsS --max-time 2 --user \
        "$clickhouse_username:$clickhouse_password" \
        "$clickhouse_ping_url" >/dev/null 2>&1
}

[ -f "$env_file" ] || { echo "Missing production configuration: $env_file" >&2; exit 1; }
[ -f "$clickhouse_config" ] || { echo "Missing ClickHouse configuration: $clickhouse_config" >&2; exit 1; }
for executable in initdb pg_isready pg_ctl psql createdb; do
    require_executable "$postgres_bin/$executable"
done
require_executable "$clickhouse_bin"
command -v curl >/dev/null 2>&1 || { echo "Missing executable: curl" >&2; exit 1; }

clickhouse_username="${MARKETCOW_CLICKHOUSE_USERNAME:-$(read_env_value MARKETCOW_CLICKHOUSE_USERNAME)}"
clickhouse_password="${MARKETCOW_CLICKHOUSE_PASSWORD:-$(read_env_value MARKETCOW_CLICKHOUSE_PASSWORD)}"
[ -n "$clickhouse_username" ] || { echo "Missing ClickHouse username" >&2; exit 1; }
[ -n "$clickhouse_password" ] || { echo "Missing ClickHouse password" >&2; exit 1; }
export MARKETCOW_CLICKHOUSE_PASSWORD="$clickhouse_password"

mkdir -p "$postgres_dir" "$clickhouse_dir/data" "$clickhouse_dir/tmp" \
    "$clickhouse_dir/user_files" "$clickhouse_dir/format_schemas"

if [ ! -f "$postgres_dir/PG_VERSION" ]; then
    echo "Initializing PostgreSQL storage at $postgres_dir"
    "$postgres_bin/initdb" -D "$postgres_dir" -U "$postgres_user" --auth=trust --no-locale
fi
if ! "$postgres_bin/pg_isready" -h "$postgres_host" -p "$postgres_port" -q; then
    echo "Starting PostgreSQL at $postgres_host:$postgres_port"
    "$postgres_bin/pg_ctl" -D "$postgres_dir" -l "$postgres_dir/server.log" \
        -o "-h $postgres_host -p $postgres_port" start
fi
wait_for_postgres
if ! "$postgres_bin/psql" -h "$postgres_host" -p "$postgres_port" -U "$postgres_user" -d postgres \
    -tAc "SELECT 1 FROM pg_database WHERE datname='$postgres_database'" | grep -q 1; then
    echo "Creating PostgreSQL database $postgres_database"
    "$postgres_bin/createdb" -h "$postgres_host" -p "$postgres_port" \
        -U "$postgres_user" "$postgres_database"
fi

if ! clickhouse_is_ready; then
    if curl -fsS --max-time 2 "$clickhouse_ping_url" >/dev/null 2>&1; then
        echo "ClickHouse is running but MarketCow authentication failed" >&2
        exit 1
    fi
    echo "Starting ClickHouse for MarketCow"
    "$clickhouse_bin" server \
        --config-file="$clickhouse_config" \
        --pid-file="$clickhouse_dir/clickhouse.pid" --daemon
fi
wait_for_clickhouse

echo "MarketCow storage dependencies are ready"
