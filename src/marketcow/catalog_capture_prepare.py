"""Verify a Rust capture, then reuse the disk-backed production normalizer."""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import time
from pathlib import Path

from marketcow.catalog_incremental import encoded, timestamp
from marketcow.polymarket_live import GammaCatalogRows, GammaLiveNormalizer
from marketcow.universe_catalog_prepare import prepare_catalog
from marketcow import catalog_normalization_cache


def strict_json(body):
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError("duplicate JSON key")
            result[key] = value
        return result

    def invalid(value):
        raise ValueError("nonfinite JSON")

    return json.loads(body, object_pairs_hook=pairs, parse_float=str, parse_constant=invalid)


def bounded_read(path, maximum):
    with path.open("rb") as stream:
        body = stream.read(maximum + 1)
    if len(body) > maximum:
        raise ValueError("input byte budget")
    return body


def prepare_capture(
    capture: Path,
    output: Path,
    *,
    max_pages: int,
    max_bytes: int,
    max_page_bytes: int,
    max_records: int,
    max_row_bytes: int,
    max_database_bytes: int,
    metric_unit: str | None,
    inventory_path: Path | None = None,
):
    began = time.monotonic()
    for limit in (max_pages, max_bytes, max_page_bytes, max_records, max_row_bytes, max_database_bytes):
        if type(limit) is not int or limit <= 0:
            raise ValueError("explicit positive limits required")
    report = strict_json(bounded_read(capture / "report.json", 65536))
    if (
        report.get("schema_version") != "marketcow.catalog-capture.v1"
        or report.get("complete") is not True
        or report.get("error") is not None
        or report.get("terminal_cursor") is not None
        or type(report.get("closed_filter")) is not bool
    ):
        raise ValueError("incomplete capture")
    start, end = timestamp(report["capture_started_at"]), timestamp(report["capture_completed_at"])
    requested = report.get("requested_market_ids", [])
    if (
        not isinstance(requested, list)
        or len(requested) > max_pages
        or any(not isinstance(mid, str) or not mid.isascii() or not mid.isdigit() for mid in requested)
        or len(requested) != len(set(requested))
    ):
        raise ValueError("requested identities invalid")
    if end < start:
        raise ValueError("capture time regression")
    output.mkdir()  # Never reuse a previous run or overwrite an artifact.
    db = sqlite3.connect(output / "capture-index.sqlite")
    try:
        db.executescript(
            "CREATE TABLE ids(id TEXT PRIMARY KEY, observed TEXT); CREATE TABLE cursors(id TEXT PRIMARY KEY);"
        )
        total = count = pages = 0
        cursor = None
        raw_sha = hashlib.sha256()
        raw_path = output / "raw.jsonl"
        with raw_path.open("xb") as raw_file, (capture / "pages.jsonl").open("rb") as ledger:
            while line := ledger.readline(65537):
                if len(line) > 65536:
                    raise ValueError("page ledger budget")
                page = strict_json(line)
                pages += 1
                if pages > max_pages or page["page"] != pages:
                    raise ValueError("page sequence/budget")
                if pages > 1 and cursor is None and not requested:
                    raise ValueError("page after terminal")
                expected = [
                    ["limit", "100"],
                    ["closed", str(report["closed_filter"]).lower()],
                    ["order", "id"],
                    ["ascending", "true"],
                ]
                if cursor is not None:
                    expected.append(["after_cursor", cursor])
                if requested:
                    if (
                        pages > len(requested)
                        or page.get("url") != "https://gamma-api.polymarket.com/markets/" + requested[pages - 1]
                    ):
                        raise ValueError("targeted request binding")
                    expected = []
                if page["params"] != expected or page["status"] != 200 or page["truncated"] is not False:
                    raise ValueError("page request/status binding")
                name = f"page-{pages:06}.json"
                if page["file"] != name:
                    raise ValueError("page path binding")
                body = bounded_read(capture / name, min(max_page_bytes, max_bytes - total))
                total += len(body)
                if len(body) != page["raw_bytes"] or hashlib.sha256(body).hexdigest() != page["raw_sha256"]:
                    raise ValueError("page hash mismatch")
                observed = timestamp(page["received_at"])
                if not start <= observed <= end:
                    raise ValueError("page capture time")
                value = strict_json(body)
                markets = [value] if requested else value["markets"]
                if requested and value.get("id") != requested[pages - 1]:
                    raise ValueError("targeted response identity")
                if not isinstance(markets, list) or len(markets) > 100:
                    raise ValueError("page markets")
                cursor = None if requested else value.get("next_cursor")
                if cursor is not None:
                    if not isinstance(cursor, str) or not cursor or not markets:
                        raise ValueError("page cursor")
                    db.execute("INSERT INTO cursors VALUES(?)", (cursor,))
                for market in markets:
                    mid = market.get("id")
                    if not isinstance(mid, str) or not mid:
                        raise ValueError("market identity")
                    count += 1
                    if count > max_records:
                        raise ValueError("record budget")
                    db.execute("INSERT INTO ids VALUES(?,?)", (mid, page["received_at"]))
                    row = encoded(market) + b"\n"
                    if len(row) > max_row_bytes:
                        raise ValueError("raw row budget")
                    raw_file.write(row)
                    raw_sha.update(row)
                db.commit()
            raw_file.flush()
            os.fsync(raw_file.fileno())
        if (
            not pages
            or cursor is not None
            or pages != report["pages"]
            or total != report["retained_raw_bytes"]
            or count != report["market_count"]
        ):
            raise ValueError("capture final count/boundary mismatch")
        if requested and pages != len(requested):
            raise ValueError("targeted capture incomplete")
        verified_at = time.monotonic()
        composite_start = report["capture_started_at"]
        normalized = None
        normalized_groups = None
        if inventory_path is not None:
            # Catalog-only inventory. Source updates are staged here even if a
            # later normalization fails; publication remains separately atomic.
            inventory = sqlite3.connect(inventory_path)
            try:
                inventory.execute(f"PRAGMA max_page_count={max_database_bytes // 4096}")
                inventory.execute("CREATE TABLE IF NOT EXISTS raw(id TEXT PRIMARY KEY, payload BLOB, observed TEXT)")
                catalog_normalization_cache.initialize(inventory)
                with inventory, raw_path.open("rb") as raw_file:
                    for line in raw_file:
                        mid = strict_json(line)["id"]
                        observed = db.execute("SELECT observed FROM ids WHERE id=?", (mid,)).fetchone()[0]
                        old = inventory.execute("SELECT observed,payload FROM raw WHERE id=?", (mid,)).fetchone()
                        if old and timestamp(old[0]) > timestamp(observed):
                            raise ValueError("raw observation regression")
                        inventory.execute("INSERT OR REPLACE INTO raw VALUES(?,?,?)", (mid, line, observed))
                        if old is None or old[1] != line:
                            catalog_normalization_cache.changed(inventory, mid, strict_json(line))
                    if inventory.execute("SELECT count(*) FROM raw").fetchone()[0] > max_records:
                        raise ValueError("inventory record budget")
                raw_path = output / "inventory.jsonl"
                raw_sha = hashlib.sha256()
                count = 0
                db.execute("DELETE FROM ids")
                with raw_path.open("xb") as stream:
                    for mid, line, observed in inventory.execute("SELECT id,payload,observed FROM raw ORDER BY id"):
                        if len(line) > max_row_bytes:
                            raise ValueError("inventory row budget")
                        stream.write(line)
                        raw_sha.update(line)
                        count += 1
                        db.execute("INSERT INTO ids VALUES(?,?)", (mid, observed))
                        if timestamp(observed) < timestamp(composite_start):
                            composite_start = observed
                    stream.flush()
                    os.fsync(stream.fileno())
                db.commit()
                normalized, normalized_groups = catalog_normalization_cache.normalize(
                    inventory,
                    output / "normalized-cache.jsonl",
                    end,
                    max_group_bytes=max_page_bytes,
                    max_group_records=max_records,
                )
            finally:
                inventory.close()
        if normalized is None:
            normalized = GammaLiveNormalizer.normalize_to_disk(
                GammaCatalogRows(raw_path, row_count=count, sha256=raw_sha.hexdigest()),
                end,
                output_root=output / "normalized",
            )
        normalized_at = time.monotonic()
        prepared = prepare_catalog(
            raw_path=raw_path,
            normalized_path=normalized.path,
            output=output / "prepared.sqlite",
            raw_sha256=raw_sha.hexdigest(),
            normalized_sha256=normalized.sha256,
            revision=normalized.revision,
            raw_count=count,
            normalized_count=normalized.market_count,
            observed_at=report["capture_completed_at"],
            source_predicate=(
                "Gamma observed inventory; non-atomic mixed capture times"
                if inventory_path
                else f"Gamma keyset closed={str(report['closed_filter']).lower()}; non-atomic traversal"
            ),
            traversal_verified=not bool(requested) and inventory_path is None,
            metric_unit=metric_unit,
            max_input_row_bytes=max_row_bytes,
            max_output_row_bytes=max_row_bytes,
            max_records=max_records,
            max_database_bytes=max_database_bytes,
            observed_by_id=lambda mid: db.execute("SELECT observed FROM ids WHERE id=?", (mid,)).fetchone()[0],
        )
        result = dict(
            prepared,
            capture_started_at=report["capture_started_at"],
            capture_completed_at=report["capture_completed_at"],
            pages=pages,
            inventory_observation_start=composite_start,
            source_atomic_snapshot=False,
            normalized_groups=normalized_groups,
            phase_wall_seconds={
                "capture_verify": verified_at - began,
                "inventory_and_normalize": normalized_at - verified_at,
                "map_and_index": time.monotonic() - normalized_at,
            },
        )
        with (output / "manifest.json").open("xb") as stream:
            stream.write(encoded(result))
            stream.flush()
            os.fsync(stream.fileno())
        return result
    finally:
        db.close()
