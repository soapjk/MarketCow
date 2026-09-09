import hashlib
import json

import pytest

from marketcow.universe_catalog_artifacts import clone_catalog_artifacts


def test_verified_catalog_sharing_excludes_state(tmp_path):
    source, stage, final = (tmp_path/x for x in ("source", "stage", "final"))
    source.mkdir(); stage.mkdir()
    manifest = {}
    for key in ("normalized_catalog", "catalog_index", "candidate_snapshot", "catalog_source"):
        path = source/key
        path.write_bytes(key.encode()); path.chmod(0o400)
        pk, hk = ("raw_path", "raw_payload_sha256") if key == "catalog_source" else ("path", "sha256")
        manifest[key] = {pk: str(path), hk: hashlib.sha256(key.encode()).hexdigest()}
    (source/"catalog.json").write_text(json.dumps(manifest))
    (source/"events.jsonl").write_text("must not migrate")
    rewritten, report = clone_catalog_artifacts(source, stage, final, maximum_copy_bytes=0)
    assert len(report) == 4
    assert not (stage/"events.jsonl").exists()
    assert rewritten["catalog_index"]["path"] == str(final/"catalog_index")
    assert (source/"catalog_index").stat().st_ino == (stage/"catalog_index").stat().st_ino
    other = tmp_path/"other"; other.mkdir()
    (source/"normalized_catalog").chmod(0o600)
    with pytest.raises(ValueError, match="mutable"):
        clone_catalog_artifacts(source, other, final, maximum_copy_bytes=0)
    rewritten, report = clone_catalog_artifacts(source, other, final, maximum_copy_bytes=100)
    assert report[0]["method"] == "verified_copy"
    assert (other/"normalized_catalog").stat().st_ino != (source/"normalized_catalog").stat().st_ino
