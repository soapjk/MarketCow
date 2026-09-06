"""Probe a chosen deployment volume without touching existing service data.

This checks local filesystem primitives, not power-loss durability or load capacity.
"""

import argparse
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile


def probe(root: Path) -> dict:
    root = root.resolve(strict=True)
    with tempfile.TemporaryDirectory(prefix="marketcow-storage-probe-", dir=root) as name:
        directory = Path(name)
        lease = directory / "writer.lock"
        lease_fd = os.open(lease, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        try:
            os.fchmod(lease_fd, 0o600)
            lease_mode = os.fstat(lease_fd).st_mode & 0o777
        finally:
            os.close(lease_fd)
        database = directory / "probe.sqlite3"
        connection = sqlite3.connect(database)
        try:
            mode = connection.execute("PRAGMA journal_mode=WAL").fetchone()[0]
            if mode != "wal":
                raise RuntimeError(f"WAL unavailable: {mode}")
            connection.execute("PRAGMA synchronous=FULL")
            connection.execute("CREATE TABLE probe (value INTEGER NOT NULL)")
            connection.execute("INSERT INTO probe VALUES (1)")
            connection.commit()
            connection.execute("BEGIN IMMEDIATE")
            child = subprocess.run(
                [sys.executable, "-c", """
import sqlite3, sys
c = sqlite3.connect(sys.argv[1], timeout=0.1)
try:
    c.execute('BEGIN IMMEDIATE')
except sqlite3.OperationalError as error:
    if 'locked' not in str(error):
        raise
else:
    raise RuntimeError('cross-process writer lock was not enforced')
assert c.execute('SELECT value FROM probe').fetchall() == [(1,)]
""", str(database)],
                capture_output=True, text=True, timeout=10, check=True,
            )
            connection.rollback()
            connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            if connection.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
                raise RuntimeError("SQLite integrity check failed")
        finally:
            connection.close()
        candidate = directory / "candidate"
        with candidate.open("xb") as stream:
            stream.write(b"verified-generation\n")
            stream.flush()
            os.fsync(stream.fileno())
        candidate.replace(directory / "published")
        fd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
        if (directory / "published").read_bytes() != b"verified-generation\n":
            raise RuntimeError("published contents differ")
        return {
            "root": str(root), "sqlite_wal": True,
            "writer_lease_mode": oct(lease_mode),
            "rust_private_writer_lease_compatible": lease_mode & 0o077 == 0,
            "cross_process_writer_exclusion": child.returncode == 0,
            "concurrent_reader": True, "integrity_check": "ok",
            "file_and_directory_fsync": True, "rename_readback": True,
            "power_loss_durability_verified": False,
        }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", required=True, type=Path)
    print(json.dumps(probe(parser.parse_args().root), sort_keys=True))
