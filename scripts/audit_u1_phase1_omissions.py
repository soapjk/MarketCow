"""Read-only source diagnosis; writes only a new bounded omission report."""
import hashlib
import json
import os
from pathlib import Path
import sqlite3


def main():
    root = Path("/mnt/p44pro/marketcow-shadow-v3-runtime/linux")
    phase = root/"phase1"
    manifest = json.loads((root/"bounded-discovery-candidate-r1/catalog.json").read_bytes())
    with sqlite3.connect((phase/"catalog-r1.sqlite").as_uri()+"?mode=ro", uri=True) as db:
        omitted = dict(db.execute("SELECT id,reason FROM omitted"))
    evidence = {}
    sha = hashlib.sha256()

    def values(value):
        if isinstance(value, str):
            value = json.loads(value)
        return value if isinstance(value, list) else []

    with Path(manifest["catalog_source"]["raw_path"]).open("rb") as stream:
        for body in stream:
            sha.update(body)
            raw = json.loads(body)
            mid = str(raw.get("id", ""))
            if mid not in omitted:
                continue
            tokens = values(raw.get("clobTokenIds") or raw.get("clob_token_ids"))
            outcomes = values(raw.get("outcomes"))
            event = (raw.get("events") or [{}])[0]
            condition = raw.get("conditionId") or raw.get("condition_id")
            reasons = []
            if len(tokens) != 2 or len(outcomes) != 2:
                reasons.append("normalizer_requires_two_tokens_and_two_outcomes")
            if not condition:
                reasons.append("normalizer_requires_condition_id")
            if not (raw.get("event_id") or event.get("id")):
                reasons.append("source_event_identity_missing")
            evidence[mid] = {"prepared_reason": omitted[mid], "source_row_sha256": hashlib.sha256(body.rstrip(b"\n")).hexdigest(),
                "token_count": len(tokens), "outcome_count": len(outcomes), "condition_present": bool(condition),
                "event_id_present": bool(raw.get("event_id") or event.get("id")), "source_reasons": reasons}
    assert sha.hexdigest() == manifest["catalog_source"]["raw_payload_sha256"]
    assert set(evidence) == set(omitted)
    result = {"raw_sha256": sha.hexdigest(), "count": len(evidence), "records": evidence}
    target = phase/"omissions-r1.json"
    with target.open("xb") as stream:
        stream.write(json.dumps(result, sort_keys=True, separators=(",", ":")).encode())
        stream.flush()
        os.fsync(stream.fileno())
    print(json.dumps({"count": len(evidence), "unexplained": [mid for mid, row in evidence.items() if not row["source_reasons"]]}))


if __name__ == "__main__":
    main()
