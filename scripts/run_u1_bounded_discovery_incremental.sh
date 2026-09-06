#!/bin/bash
set -eu
umask 077
runtime=/mnt/p44pro/marketcow-shadow-v3-runtime/linux
project=/mnt/p44pro/projects/marketcow-shadow-v3
export TMPDIR=$runtime/tmp PYTHONPATH=$runtime/read-api-packages:$project/src PYTHONDONTWRITEBYTECODE=1
proxy=marketcow-bounded-discovery-incremental-proxy-r1
python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",17890))'
cleanup() {
 result=$?
 trap - EXIT
 systemctl --user stop "$proxy" || result=1
 printf '%s\n' "$result" > "$runtime/logs/bounded-discovery-incremental-r1.exit-code"
 exit "$result"
}
trap cleanup EXIT
systemd-run --user --unit="$proxy" -p RuntimeMaxSec=330 -p StandardOutput=null -p StandardError=null \
 "$runtime/polymarket-proxy/mihomo" -d "$runtime/polymarket-proxy" -f "$runtime/polymarket-proxy/config.yaml"
sleep 2
python3 "$project/scripts/audit_bounded_discovery_incremental.py"
