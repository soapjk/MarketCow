#!/usr/bin/env bash
# Verify a local source archive without unpacking or building on the system disk.
# Usage: scripts/verify_source_archive_t9.sh --check
#        scripts/verify_source_archive_t9.sh ARCHIVE SUMS [CARGO_ARGS...]
set -euo pipefail

fail() { printf 'T9 source verification: %s\n' "$*" >&2; exit 1; }

t9_mount=/Volumes/T9
[[ -d "$t9_mount" && -w "$t9_mount" ]] || fail 'T9 is not mounted and writable'
t9_device=$(df -P "$t9_mount" | awk 'NR == 2 { print $1 }')
system_device=$(df -P /private/tmp | awk 'NR == 2 { print $1 }')
[[ -n "$t9_device" && -n "$system_device" && "$t9_device" != "$system_device" ]] ||
  fail 'T9 resolves to the system disk; refusing to fall back to /private/tmp'
[[ $(cd -P "$t9_mount" && pwd) == "$t9_mount" ]] || fail 'T9 mount path is redirected'
available_kib=$(df -Pk "$t9_mount" | awk 'NR == 2 { print $4 }')
[[ "$available_kib" =~ ^[0-9]+$ && "$available_kib" -ge 8388608 ]] ||
  fail 'less than 8 GiB free on T9'

scratch="$t9_mount/.cache/marketcow/source-archive-verify"
if [[ ${1-} == --check ]]; then
  printf 'T9 mounted: %s; free KiB: %s\nScratch: %s\nCargo target: %s/target\n' \
    "$t9_device" "$available_kib" "$scratch" "$scratch"
  exit 0
fi
[[ $# -ge 2 ]] || fail 'usage: verify_source_archive_t9.sh ARCHIVE SUMS [CARGO_ARGS...]'
archive=$1
sums=$2
shift 2
[[ -f "$archive" && -f "$sums" ]] || fail 'archive and checksum manifest must exist'
archive=$(cd "$(dirname "$archive")" && printf '%s/%s' "$(pwd -P)" "$(basename "$archive")")
sums=$(cd "$(dirname "$sums")" && printf '%s/%s' "$(pwd -P)" "$(basename "$sums")")
[[ $(df -P "$archive" | awk 'NR == 2 { print $1 }') == "$t9_device" ]] ||
  fail 'source archive is not stored on T9'

mkdir -p "$scratch/runs" "$scratch/tmp" "$scratch/target" "$scratch/cargo-home"
[[ $(cd -P "$scratch" && pwd) == "$t9_mount"/* ]] || fail 'scratch escaped T9'
run_dir=$(mktemp -d "$scratch/runs/verify.XXXXXX")
export TMPDIR="$scratch/tmp/"
export CARGO_TARGET_DIR="$scratch/target"
export CARGO_HOME="$scratch/cargo-home"
export CARGO_INCREMENTAL=0

# Seed the new T9 Cargo cache locally so --offline verification remains usable.
# Do not copy native Cargo credentials/configuration or mutate the original cache.
native_cache=${HOME:?}/.cargo
if [[ ! -d "$CARGO_HOME/registry" && -d "$native_cache/registry" ]]; then
  rsync -a "$native_cache/registry/" "$CARGO_HOME/registry/"
fi
if [[ ! -d "$CARGO_HOME/git" && -d "$native_cache/git" ]]; then
  rsync -a "$native_cache/git/" "$CARGO_HOME/git/"
fi

tar -xzf "$archive" -C "$run_dir"
(
  cd "$run_dir"
  shasum -a 256 -c "$sums"
  if [[ $# -eq 0 ]]; then
    cargo test --offline --locked -p marketcowd --bin marketcow-discovery-collector --quiet
  else
    cargo "$@"
  fi
)
printf 'Verified source: %s\nShared Cargo target: %s\n' "$run_dir" "$CARGO_TARGET_DIR"
