"""Offline empty candidate initialization, never a reset or history migration.

An entirely new state directory avoids importing unrelated recovery tokens from
an incumbent pool. The new runtime gets a fresh stream identity and must acquire
real books. Cursor zero is not comparable with a different incumbent instance.
"""
import json
import os
import re
from pathlib import Path

from marketcow.polymarket_live import LiveStateIndex


def initialize_empty_candidate(root: Path, *, catalog_revision: str):
    root = root.resolve(strict=True)
    if not re.fullmatch(r"[0-9a-f]{64}", catalog_revision):
        raise ValueError("invalid catalog revision")
    with (root/"catalog.json").open("rb") as stream:
        raw = stream.read(2097153)
    if len(raw) > 2097152 or json.loads(raw)["catalog_revision"] != catalog_revision:
        raise ValueError("candidate catalog mismatch")
    for name in ("indexes", "events.jsonl", "state-index.json", ".collector.lock"):
        path = root/name
        if path.exists() or path.is_symlink():
            raise ValueError("candidate state already exists; reset prohibited")
    # Atomic mkdir reserves this new candidate state before any writes. A failed
    # preparation stays explicit and cannot be silently retried as an empty root.
    (root/"indexes").mkdir(mode=0o700)
    event_path = root/"events.jsonl"
    with event_path.open("xb") as stream:
        stream.flush()
        os.fsync(stream.fileno())
    index = LiveStateIndex(root)
    try:
        index.rebuild(event_path=event_path, books={}, gaps=[], catalog_revision=catalog_revision,
                      token_to_market={}, verified_event_offsets=[])
    finally:
        index.close()
    for path in (root/"indexes/latest-state.sqlite3", root/"state-index.json"):
        with path.open("rb") as stream:
            os.fsync(stream.fileno())
    for path in (root/"indexes", root):
        fd = os.open(path, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    return {"latest_cursor": 0, "books": 0, "requires_real_preheat": True,
            "new_stream_required": True, "history_mode": "empty_pending_bounded_enable"}
