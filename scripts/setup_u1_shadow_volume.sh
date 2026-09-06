#!/bin/bash
# Explicitly authorized U1-only volume creation. Never format a block device.
set -euo pipefail
base=/mnt/p44pro/marketcow-shadow-v3-runtime
image=/mnt/p44pro/marketcow-shadow-v3-runtime/runtime.ext4
target=/mnt/p44pro/marketcow-shadow-v3-runtime/linux
[[ $(id -u) -eq 0 ]] || { echo 'Run with sudo.' >&2; exit 1; }
[[ -d "$base" && ! -L "$base" && $(realpath "$base") == "$base" ]]
[[ ! -e "$image" && ! -L "$image" && ! -e "$target" && ! -L "$target" ]] || {
    echo 'Image or mountpoint already exists; refusing to overwrite. Ask for inspection.' >&2
    exit 1
}
available=$(df --output=avail -B1 "$base" | tail -1)
[[ $available -gt 53687091200 ]] || { echo 'Need over 50 GiB free.' >&2; exit 1; }
id czx >/dev/null
command -v mkfs.ext4 >/dev/null
command -v mount >/dev/null
# Exclusive creation; the only formatted target is this new regular image file.
( set -o noclobber; : > "$image" )
[[ -f "$image" && ! -L "$image" ]]
truncate -s 40G "$image"
mkfs.ext4 -m 0 -L mc-shadow-v3 "$image"
mkdir "$target"
mount -o loop,nosuid,nodev "$image" "$target"
[[ $(findmnt -n -o FSTYPE --target "$target") == ext4 ]]
chown czx:czx "$target"
chmod 0700 "$target"
findmnt --target "$target"
stat -c 'mode=%a owner=%U path=%n' "$target"
echo 'Created and mounted. No fstab entry or service autostart was added.'
