"""Read existing immutable normalized catalog/index without Discovery pool filters.

This is the source reader, not the phase-1 HTTP record mapper. It preserves raw
normalized records; absent source facts must not be fabricated by the mapper.
No source download, full-table materialization, or source database writes.
"""
from __future__ import annotations

import hashlib
import json
import os
import sqlite3
from pathlib import Path


class CatalogReadError(ValueError):
    pass


class IndexedCatalogReader:
    def __init__(self, catalog: Path, index: Path, *, revision: str,
                 expected_sha256: str, max_row_bytes: int):
        if type(max_row_bytes) is not int or max_row_bytes <= 0:
            raise ValueError("explicit positive row byte budget required")
        self.max_row_bytes = max_row_bytes
        self.stream = catalog.open("rb")
        self.db = None
        try:
            # Pin descriptors before verification: path replacement cannot switch
            # this reader to another generation half way through traversal.
            self.db = sqlite3.connect(index.resolve().as_uri() + "?mode=ro", uri=True)
            self.db.execute("PRAGMA query_only=ON")
            self.db.execute("PRAGMA cache_size=-8192")
            self.db.execute("BEGIN")
            metadata = dict(self.db.execute("SELECT key,value FROM metadata"))
            if (metadata.get("catalog_revision") != revision
                    or metadata.get("catalog_sha256") != expected_sha256):
                raise CatalogReadError("catalog_revision_mismatch")
            if hashlib.file_digest(self.stream, "sha256").hexdigest() != expected_sha256:
                raise CatalogReadError("catalog_hash_mismatch")
            self.size = os.fstat(self.stream.fileno()).st_size
            self.count = self.db.execute("SELECT count(*) FROM markets").fetchone()[0]
            if str(self.count) != metadata.get("market_count"):
                raise CatalogReadError("catalog_count_mismatch")
            self.revision = revision
        except BaseException:
            self.close()
            raise

    def close(self):
        if self.db is not None:
            self.db.close()
        self.stream.close()

    def page(self, *, after: str | None, limit: int, byte_budget: int):
        """Keyset page over ALL identities; byte budget covers normalized bodies.

        Transport envelope bytes need a separate outer budget. A too-large first
        record fails explicitly; it is not skipped. Returned cursor is last ID,
        to be wrapped by the snapshot layer, not exposed as an opaque wire token.
        """
        if type(limit) is not int or limit <= 0 or type(byte_budget) is not int or byte_budget <= 0:
            raise ValueError("explicit positive page budgets required")
        select = "SELECT market_id,byte_offset,byte_length,row_sha256,catalog_revision FROM markets "
        # Do not use (? IS NULL OR market_id>?): that can scan from the start
        # on every page instead of seeking directly through the primary index.
        rows = self.db.execute(
            select + ("ORDER BY market_id LIMIT ?" if after is None else
                      "WHERE market_id>? ORDER BY market_id LIMIT ?"),
            (limit + 1,) if after is None else (after, limit + 1),
        )
        records = []
        used = 0
        last = after
        ended = True
        for market_id, offset, length, digest, revision in rows:
            if len(records) == limit:
                ended = False
                break
            if (revision != self.revision or offset < 0 or length <= 0
                    or offset + length > self.size):
                raise CatalogReadError("catalog_index_invalid")
            if length > self.max_row_bytes:
                raise CatalogReadError("response_size_exceeded")
            if used + length > byte_budget:
                if not records:
                    raise CatalogReadError("response_size_exceeded")
                ended = False
                break
            body = os.pread(self.stream.fileno(), length, offset)
            if hashlib.sha256(body).hexdigest() != digest:
                raise CatalogReadError("catalog_row_hash_mismatch")
            record = json.loads(body)
            if record.get("identity", {}).get("market_id") != market_id:
                raise CatalogReadError("catalog_identity_mismatch")
            records.append(record)
            used += length
            last = market_id
        return records, None if ended else last, used
