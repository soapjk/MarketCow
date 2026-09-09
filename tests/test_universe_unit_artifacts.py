"""Real local files, no systemd or network; templates are labelled synthetic."""
import hashlib

import pytest

from marketcow.universe_systemd import UnitArtifact
from marketcow.universe_unit_artifacts import build_unit_pair


def test_pinned_unit_pair_and_binary_integrity(tmp_path):
    binary = tmp_path/"marketcow-discovery-collector"; binary.write_bytes(b"synthetic binary")
    template = tmp_path/"template"
    template.write_text("[Service]\nExecStart=/usr/bin/python3 /synthetic/logger --log /synthetic/log -- /synthetic/marketcow-discovery-collector --root /old --expected-market-count 1 --plan /old/plan --plan-sha256 old --discovery-seed /old/seed --discovery-seed-sha256 old --discovery-listen 127.0.0.1:8795\nRestart=on-failure\n")
    artifact = UnitArtifact("marketcow-discovery.service", template, hashlib.sha256(template.read_bytes()).hexdigest())
    args = dict(pool="discovery", template=artifact, binary=binary, binary_sha256=hashlib.sha256(binary.read_bytes()).hexdigest(),
        candidate_root=tmp_path/"candidate", preparation=dict(selected=2, plan_sha256="a"*64, seed_sha256="b"*64),
        directory=tmp_path/"units", preheat_name="marketcow-universe-discovery-test.service",
        preheat_listener="127.0.0.1:18900", public_listener="192.168.124.3:8795",
        preheat_log=tmp_path/"preheat.log", public_log=tmp_path/"public.log", maximum_runtime_seconds=120)
    result = build_unit_pair(**args)
    assert "RuntimeMaxSec=120" in result["preheat"].path.read_text()
    assert "RuntimeMaxSec" not in result["candidate"].path.read_text()
    assert "--expected-market-count 2" in result["candidate"].path.read_text()
    assert result["candidate"].verify(tmp_path) == result["candidate"].path
    binary.write_bytes(b"tampered")
    with pytest.raises(ValueError, match="binary hash mismatch"):
        result["candidate"].verify(tmp_path)
    with pytest.raises(ValueError, match="existing unit"):
        build_unit_pair(**args)
