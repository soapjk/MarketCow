"""Offline relocation of a stopped, hash-verified Shadow source; never fetch Gamma.

Creates a fresh self-contained target. Existing source and target are never overwritten.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import sqlite3


def digest(path):
    checksum = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            checksum.update(chunk)
    return checksum.hexdigest()


def encoded(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()


def write_new(path, value):
    with path.open("xb") as stream:
        stream.write(encoded(value))
        stream.flush()
        os.fsync(stream.fileno())


def prepare(source, target, original):
    source = source.resolve(strict=True)
    target = target.absolute()
    if target.exists() or target.is_symlink() or target.is_relative_to(source):
        raise ValueError("target must be a new independent generation")
    if any(path.is_symlink() for path in source.rglob("*")):
        raise ValueError("source contains symlinks")
    shutil.copytree(source, target)
    for path in target.rglob("*"):
        path.chmod(0o700 if path.is_dir() else 0o600)
    target.chmod(0o700)
    copied = []
    for path in source.rglob("*"):
        if path.is_file():
            relative = path.relative_to(source)
            checksum = digest(path)
            if checksum != digest(target / relative):
                raise ValueError(f"copy mismatch: {relative}")
            copied.append({"path":str(relative), "sha256":checksum})
    def relocate(value):
        if isinstance(value, dict):
            return {key:relocate(item) for key,item in value.items()}
        if isinstance(value, list):
            return [relocate(item) for item in value]
        if isinstance(value, str) and value.startswith(str(original) + "/"):
            path = target / Path(value).relative_to(original)
            if not path.resolve(strict=True).is_relative_to(target.resolve()):
                raise ValueError("relocated reference escapes target")
            return str(path)
        return value
    # Only location-bearing manifests change. Source facts, event logs, immutable
    # catalog/index bytes, revisions and gap ledgers remain untouched.
    for name in ["catalog.json", "state-index.json", "discovery-materialized-v3/current.json"]:
        path = target / name
        if not path.is_file():
            raise ValueError(f"missing prepared manifest: {name}")
        value = relocate(json.loads(path.read_bytes()))
        candidate = path.with_name(path.name + ".relocating")
        write_new(candidate, value)
        candidate.replace(path)
    catalog = json.loads((target / "catalog.json").read_bytes())
    selected = catalog["realtime_universe"]["market_ids"]
    if len(set(selected)) != len(selected):
        raise ValueError("duplicate universe ids")
    index = Path(catalog["catalog_index"]["path"])
    normalized = Path(catalog["normalized_catalog"]["path"])
    for path, checksum in [(index,catalog["catalog_index"]["sha256"]),
                           (normalized,catalog["normalized_catalog"]["sha256"])]:
        if not path.resolve().is_relative_to(target) or digest(path) != checksum:
            raise ValueError("catalog artifact binding failed")
    markets = []
    with sqlite3.connect(f"file:{index}?mode=ro", uri=True) as db, normalized.open("rb") as stream:
        for market_id in selected:
            row = db.execute("SELECT byte_offset,byte_length,row_sha256 FROM markets WHERE market_id=?", (market_id,)).fetchone()
            if row is None:
                raise ValueError(f"missing catalog market {market_id}")
            offset, length, checksum = row
            stream.seek(offset)
            body = stream.read(length)
            if stream.read(1) != b"\n" or hashlib.sha256(body).hexdigest() != checksum:
                raise ValueError("catalog row hash mismatch")
            identity = json.loads(body)["identity"]
            if identity["market_id"] != market_id or len(identity["outcomes"]) != 2:
                raise ValueError("invalid binary market identity")
            markets.append({"market_id":market_id,"condition_id":identity["condition_id"],
                            "token_ids":[item["token_id"] for item in identity["outcomes"]]})
    plan = {"schema_version":"marketcow.polymarket.rust-discovery-source-plan.v1",
            "catalog_revision":catalog["catalog_revision"],"markets":markets}
    write_new(target / "rust-source-plan.json", plan)
    report = {"source":str(source),"target":str(target),"market_count":len(markets),
              "copied_artifacts":copied,"plan_sha256":digest(target / "rust-source-plan.json"),
              "catalog_revision":catalog["catalog_revision"],"complete":True}
    write_new(target / "rust-source-preparation.json", report)
    directory = os.open(target, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)
    print(json.dumps({key:value for key,value in report.items() if key != "copied_artifacts"}))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", required=True, type=Path)
    parser.add_argument("--target", required=True, type=Path)
    parser.add_argument("--original-root", required=True, type=Path)
    args = parser.parse_args()
    prepare(args.source, args.target, args.original_root)
