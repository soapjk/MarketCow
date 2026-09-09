"""Bounded GET-only verification of the installed catalog publication.

Run through an existing authorized loopback/SSH path. No admissions or pool
operations. The bearer is read from a file and never included in the report.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import time
from urllib.parse import urlsplit

import httpx


def audit(client, *, maximum_bytes, maximum_pages, maximum_seconds):
    started = time.monotonic()
    used = 0
    prefix = "/v1/prediction-markets/polymarket/catalog"

    def get(path, **params):
        nonlocal used
        remaining = maximum_seconds - (time.monotonic() - started)
        if remaining <= 0:
            raise ValueError("audit deadline")
        body = bytearray()
        with client.stream("GET", prefix + path, params=params, timeout=min(15, remaining)) as response:
            response.raise_for_status()
            for chunk in response.iter_bytes():
                used += len(chunk)
                if used > maximum_bytes or len(body) + len(chunk) > 2097152:
                    raise ValueError("audit byte budget")
                if time.monotonic() - started >= maximum_seconds:
                    raise ValueError("audit deadline")
                body.extend(chunk)
        return json.loads(body), hashlib.sha256(body).hexdigest()

    before, before_sha = get("/status")
    snapshot, snapshot_sha = get("/snapshot", page_size=100)
    if snapshot["schema_version"] != "marketcow.polymarket.catalog-snapshot.v1":
        raise ValueError("v1 compatibility")
    cursor = snapshot["first_page_token"]
    ids, tokens, relations, pages = set(), set(), {}, []
    while cursor is not None:
        if len(pages) >= maximum_pages or cursor in tokens:
            raise ValueError("page budget or repeated token")
        tokens.add(cursor)
        page, sha = get("/page", snapshot_id=snapshot["snapshot_id"], page_token=cursor, limit=100)
        if (page["snapshot_id"] != snapshot["snapshot_id"]
                or page["catalog_revision"] != snapshot["catalog_revision"]
                or page["page_token"] != cursor):
            raise ValueError("page identity mismatch")
        if page["end_of_snapshot"] != (page["next_page_token"] is None):
            raise ValueError("terminal marker")
        for row in page["records"]:
            if row["market_id"] in ids:
                raise ValueError("duplicate market")
            ids.add(row["market_id"])
            for relation in row["relations"]:
                prior = relations.setdefault(relation["relation_id"], relation)
                if prior != relation:
                    raise ValueError("inconsistent relation")
        pages.append({"sha256": sha, "count": len(page["records"])})
        cursor = page["next_page_token"]
    if len(ids) != snapshot["unique_count"]:
        raise ValueError("unique count mismatch")
    for relation in relations.values():
        if sorted(set(relation["member_market_ids"]) - ids) != relation["missing_market_ids"]:
            raise ValueError("relation missing-member mismatch")
    v2, v2_sha = get("/snapshot-v2", page_size=100)
    if v2["schema_version"] != "marketcow.polymarket.catalog-snapshot.v2":
        raise ValueError("v2 schema")
    changes, changes_sha = get("/changes", after_sequence=v2["change_sequence"], limit=100)
    if changes["next_sequence"] < v2["change_sequence"]:
        raise ValueError("changes cursor regression")
    after, after_sha = get("/status")
    return dict(passed=True, snapshot=snapshot, snapshot_sha256=snapshot_sha,
                v2=v2, v2_sha256=v2_sha, changes=changes, changes_sha256=changes_sha,
                status_before=before, status_before_sha256=before_sha,
                status_after=after, status_after_sha256=after_sha,
                pages=pages, unique_count=len(ids), relation_count=len(relations),
                response_bytes=used, elapsed_seconds=time.monotonic() - started)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", required=True)
    parser.add_argument("--secret-file", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--maximum-bytes", type=int, required=True)
    parser.add_argument("--maximum-pages", type=int, required=True)
    parser.add_argument("--maximum-seconds", type=int, required=True)
    args = parser.parse_args()
    url = urlsplit(args.base)
    if url.scheme != "http" or url.hostname not in ("127.0.0.1", "localhost") or url.username:
        parser.error("explicit loopback base required")
    if min(args.maximum_bytes, args.maximum_pages, args.maximum_seconds) <= 0:
        parser.error("positive budgets required")
    with args.secret_file.open("r") as stream:
        secret = stream.read(4097).strip()
    if not secret or len(secret) > 4096:
        parser.error("invalid secret file")
    fd = os.open(args.output, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    report = {"passed": False}
    try:
        with httpx.Client(base_url=args.base, trust_env=False, follow_redirects=False,
                          headers={"Authorization": "Bearer " + secret}) as client:
            report = audit(client, maximum_bytes=args.maximum_bytes, maximum_pages=args.maximum_pages,
                           maximum_seconds=args.maximum_seconds)
    except Exception as error:
        report["error_type"] = type(error).__name__
        raise
    finally:
        with os.fdopen(fd, "w") as output:
            json.dump(report, output, sort_keys=True, separators=(",", ":"))
            output.flush()
            os.fsync(output.fileno())
    print(json.dumps({key: report[key] for key in ("passed", "unique_count", "response_bytes", "elapsed_seconds")}))


if __name__ == "__main__":
    main()
