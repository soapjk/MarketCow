"""Finite real control-plane audit. No activation, orders or account calls."""
import hashlib
import json
import os
from pathlib import Path
import resource
import secrets
import sqlite3
import time

import httpx

from marketcow.universe_control import instant, wire_bytes
from marketcow.universe_phase1 import selection_sha256


def main():
    root = Path("/mnt/p44pro/marketcow-shadow-v3-runtime/linux/phase1")
    output = root/"audit-r1.json"
    if output.exists():
        raise FileExistsError(output)
    config = json.loads((root/"operator.json").read_bytes())
    profile = json.loads(Path(config["profile_path"]).read_bytes())
    token = (root/"tradude.secret").read_text()
    prefix = "/v1/prediction-markets/polymarket"
    started = time.monotonic()
    total_bytes = pages = 0
    ids, seen_tokens, relations = set(), set(), {}
    admission_ids = []
    report = {"passed": False}

    def read(client, path, **kwargs):
        nonlocal total_bytes
        with client.stream(kwargs.pop("method", "GET"), path, **kwargs) as response:
            body = bytearray()
            for chunk in response.iter_bytes():
                if len(body)+len(chunk) > 2097152:
                    raise AssertionError("response size exceeded")
                body.extend(chunk)
            total_bytes += len(body)
            return response.status_code, bytes(body), json.loads(body)

    try:
        with httpx.Client(base_url="http://127.0.0.1:18898", timeout=15, trust_env=False,
                          headers={"Authorization": "Bearer "+token}) as client:
            status, raw_snapshot, snapshot = read(client, prefix+"/catalog/snapshot?page_size=100")
            assert status == 200
            report["snapshot"] = snapshot
            report["snapshot_sha256"] = hashlib.sha256(raw_snapshot).hexdigest()
            cursor = snapshot["first_page_token"]
            first_params = first_body = None
            while cursor is not None:
                assert cursor not in seen_tokens
                seen_tokens.add(cursor)
                assert pages < 2500 and total_bytes < 1073741824 and time.monotonic()-started < 700
                params = {"snapshot_id": snapshot["snapshot_id"], "page_token": cursor, "limit": 100}
                status, body, page = read(client, prefix+"/catalog/page", params=params)
                assert status == 200 and page["catalog_revision"] == snapshot["catalog_revision"]
                assert page["snapshot_id"] == snapshot["snapshot_id"] and page["page_token"] == cursor
                if first_params is None:
                    first_params, first_body = params, body
                for row in page["records"]:
                    mid = row["market_id"]
                    assert mid not in ids
                    ids.add(mid)
                    for rel in row["relations"]:
                        encoded = wire_bytes(rel)
                        assert rel["relation_id"] not in relations or relations[rel["relation_id"]] == encoded
                        relations[rel["relation_id"]] = encoded
                    # A deterministic audit list, not a production strategy selection.
                    if len(admission_ids) < 1000 and all(not rel["missing_market_ids"] for rel in row["relations"]):
                        admission_ids.append(mid)
                assert page["end_of_snapshot"] == (page["next_page_token"] is None)
                cursor = page["next_page_token"]
                pages += 1
            assert len(ids) == snapshot["unique_count"]
            for encoded in relations.values():
                rel = json.loads(encoded)
                assert sorted(set(rel["member_market_ids"])-ids) == rel["missing_market_ids"]
            assert read(client, prefix+"/catalog/page", params=first_params)[1] == first_body
            assert read(client, prefix+"/catalog/page", params=dict(first_params, limit=99))[0] == 400
            results = []
            for count in (10, 100, 1000):
                selected = sorted(admission_ids[:count])
                assert len(selected) == count
                now = time.time_ns()//1000000
                selection = {"catalog_revision": snapshot["catalog_revision"], "market_ids": selected,
                    "tradude_policy_version": "synthetic-audit-not-strategy", "protected_markets": [],
                    "expected_active_selection_id": config["expected_active_selection_id"],
                    "resource_profile_id": profile["profile_id"], "resource_profile_sha256": config["profile_sha256"]}
                request = {"schema_version": "tradude.marketcow.discovery-selection-request.v1",
                    "request_id": f"{now}:"+secrets.token_hex(16), "created_at": instant(now),
                    "expires_at": instant(now+900000), "selection": selection,
                    "selection_sha256": selection_sha256(selection)}
                status, body, response = read(client, prefix+"/discovery-selections/admit", method="POST", json=request)
                assert status in (200, 422), response
                retry = read(client, prefix+"/discovery-selections/admit", method="POST", json=request)
                assert retry[0] == status and retry[1] == body
                altered = dict(request, expires_at=instant(now+899999))
                assert read(client, prefix+"/discovery-selections/admit", method="POST", json=altered)[0] == 409
                results.append({"requested_count": count, "http_status": status, "response": response,
                                "response_sha256": hashlib.sha256(body).hexdigest(), "idempotent": True})
            assert read(client, prefix+"/catalog/snapshot?page_size=100", headers={"Authorization": "Bearer invalid"})[0] == 401
            report.update(passed=True, pages=pages, unique_count=len(ids), relation_count=len(relations),
                          admission=results, total_http_bytes=total_bytes)
        with sqlite3.connect((root/"catalog-r1.sqlite").as_uri()+"?mode=ro", uri=True) as db:
            report["prepared_manifest"] = json.loads(db.execute("SELECT value FROM metadata WHERE key='manifest'").fetchone()[0])
            report["omitted"] = db.execute("SELECT id,reason FROM omitted ORDER BY id").fetchall()
    except BaseException as error:
        report["error"] = type(error).__name__+": "+str(error)
        raise
    finally:
        report.update(elapsed_seconds=time.monotonic()-started, peak_rss_kib=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
        with output.open("xb") as stream:
            stream.write(wire_bytes(report))
            stream.flush()
            os.fsync(stream.fileno())
        print(json.dumps({"passed": report["passed"], "output": str(output),
                          "elapsed_seconds": report["elapsed_seconds"]}))


if __name__ == "__main__":
    main()
