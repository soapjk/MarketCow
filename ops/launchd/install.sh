#!/bin/sh
set -eu

label="com.marketcow.production"
project_dir="/Volumes/T9/projects/marketcow"
script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
source_plist="$script_dir/$label.plist"
target_plist="$HOME/Library/LaunchAgents/$label.plist"
support_dir="$HOME/Library/Application Support/MarketCow"
log_dir="$HOME/Library/Logs/MarketCow"
target_launcher="$support_dir/start-production.sh"
target_storage_launcher="$support_dir/ensure-production-storage.sh"
target_clickhouse_config="$support_dir/clickhouse-production.xml"
target_env="$support_dir/production.env"
domain="gui/$(id -u)"
rust_build_root="$support_dir/rust-build"
rust_binary="$support_dir/bin/marketcowd"

production_python="${MARKETCOW_PYTHON:-$project_dir/.venv/bin/python}"
if [ ! -x "$production_python" ]; then
    managed_python="$support_dir/atomic-freshness-venv/bin/python"
    [ -x "$managed_python" ] || {
        echo "Missing MarketCow production Python executable" >&2
        exit 1
    }
    production_python="$managed_python"
fi
PYTHONPATH="$project_dir/src" "$production_python" -c 'import marketcow'
if [ ! -f "$target_env" ] && [ ! -f "$project_dir/.env.production" ]; then
    echo "Missing production configuration: $target_env or $project_dir/.env.production" >&2
    exit 1
fi

mkdir -p "$HOME/Library/LaunchAgents" "$support_dir" "$log_dir"
retired_launch_agents="$support_dir/retired-launch-agents"
mkdir -p "$retired_launch_agents"

# Production has exactly one launchd owner. Retire historical MarketCow jobs
# so a login or reboot cannot start a partial Polymarket-only stack beside it.
for legacy_plist in "$HOME/Library/LaunchAgents"/com.marketcow.*.plist; do
    [ -e "$legacy_plist" ] || continue
    [ "$legacy_plist" != "$target_plist" ] || continue
    legacy_label=$(basename "$legacy_plist" .plist)
    launchctl bootout "$domain/$legacy_label" 2>/dev/null || true
    launchctl disable "$domain/$legacy_label" 2>/dev/null || true
    mv "$legacy_plist" "$retired_launch_agents/$legacy_label.plist"
done

plutil -lint "$source_plist"
cp "$source_plist" "$target_plist"
cp "$script_dir/start-production.sh" "$target_launcher"
cp "$script_dir/ensure-production-storage.sh" "$target_storage_launcher"
cp "$script_dir/clickhouse-production.xml" "$target_clickhouse_config"
[ -f "$target_env" ] || cp "$project_dir/.env.production" "$target_env"
chmod 700 "$target_launcher" "$target_storage_launcher"
chmod 600 "$target_clickhouse_config" "$target_env"

# Build and pin the authoritative Rust data plane locally. The launch job never
# compiles code and always executes this stable binary path.
cargo_bin="${CARGO:-$(command -v cargo || true)}"
[ -x "$cargo_bin" ] || {
    echo "Missing cargo executable required to build marketcowd" >&2
    exit 1
}
mkdir -p "$rust_build_root" "$support_dir/bin"
CARGO_TARGET_DIR="$rust_build_root" "$cargo_bin" build \
    --locked --release -p marketcowd --manifest-path "$project_dir/Cargo.toml"
rust_temporary="$rust_binary.tmp.$$"
cp "$rust_build_root/release/marketcowd" "$rust_temporary"
chmod 700 "$rust_temporary"
mv "$rust_temporary" "$rust_binary"

PYTHONPATH="$project_dir/src" "$production_python" \
    "$project_dir/scripts/configure_polymarket_final_architecture.py" \
    --project-dir "$project_dir" \
    --support-dir "$support_dir" \
    --data-root "/Volumes/T9/data/marketcow/production" \
    --env-file "$target_env" \
    --rust-binary "$rust_binary"

launchctl bootout "$domain/$label" 2>/dev/null || true
launchctl enable "$domain/$label"
attempt=0
until launchctl bootstrap "$domain" "$target_plist"; do
    attempt=$((attempt + 1))
    [ "$attempt" -lt 5 ] || {
        echo "Unable to bootstrap $label after $attempt attempts" >&2
        exit 1
    }
    sleep 1
done
launchctl kickstart -k "$domain/$label"

echo "Installed $label from $target_plist"
