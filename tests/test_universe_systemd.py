"""Unit filesystem is real; systemctl is fault-injected, not U1 evidence."""
import asyncio
import hashlib
from dataclasses import replace

import pytest

from marketcow.universe_systemd import SystemdUnits, UnitArtifact, LiveUnitBinding


@pytest.fixture
def setup(tmp_path):
    releases, units = tmp_path/"releases", tmp_path/"units"
    releases.mkdir()
    units.mkdir()
    artifacts = []
    for version in ("old", "new"):
        path = releases/version
        path.write_text("[Service]\nExecStart=/bin/true\n#"+version)
        artifacts.append(UnitArtifact("marketcow-test.service", path, hashlib.sha256(path.read_bytes()).hexdigest()))
    (units/artifacts[0].name).symlink_to(artifacts[0].path)
    driver = SystemdUnits(release_root=releases, user_unit_root=units, command_timeout_seconds=1)
    calls = []
    running = True

    async def command(*args):
        nonlocal running
        calls.append(args[0])
        if args[0] == "start":
            running = True
        if args[0] == "stop":
            running = False
        return f"MainPID={123 if running else 0}\nActiveState={'active' if running else 'inactive'}\nResult=success\nExecMainStatus=0\n"
    driver._command = command
    return driver, artifacts[0], artifacts[1], calls


def test_publication_requires_probe(setup):
    driver, old, new, calls = setup
    async def verify(artifact):
        assert artifact == new
        assert driver._current(new.name) == new.path
        calls.append("probe")
        return "actual-receipt-placeholder-for-synthetic-test"
    result = asyncio.run(driver.publish(new, incumbent=old, verify=verify))
    assert result.startswith("actual-receipt")
    assert calls == ["stop", "show", "daemon-reload", "start", "show", "probe"]


def test_probe_failure_restores_old_unit_and_starts_it(setup):
    driver, old, new, calls = setup
    async def fail(_):
        raise ValueError("fullsync binding mismatch")
    with pytest.raises(ValueError, match="fullsync"):
        asyncio.run(driver.publish(new, incumbent=old, verify=fail))
    assert driver._current(old.name) == old.path
    assert calls[-5:] == ["stop", "show", "daemon-reload", "start", "show"]


def test_bad_candidate_hash_never_stops_incumbent(setup):
    driver, old, new, calls = setup
    new.path.write_text("tampered")
    with pytest.raises(ValueError, match="hash mismatch"):
        asyncio.run(driver.publish(new, incumbent=old, verify=None))
    assert calls == []
    assert driver._current(old.name) == old.path


def test_stale_link_never_stops_another_version(setup):
    driver, old, new, calls = setup
    driver.replace_link(new, expected=old)
    with pytest.raises(ValueError, match="incumbent conflict"):
        asyncio.run(driver.publish(new, incumbent=old, verify=None))
    assert calls == []


def test_cancelled_probe_restores_previous_unit(setup):
    driver, old, new, calls = setup
    async def cancelled(_):
        raise asyncio.CancelledError()
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(driver.publish(new, incumbent=old, verify=cancelled))
    assert driver._current(old.name) == old.path
    assert calls[-2:] == ["start", "show"]


def test_deployment_binding_hash_has_no_generation_cycle(setup):
    _, old, new, _ = setup
    binding = LiveUnitBinding("pending", "scope", old, new, old,
        "http://127.0.0.1:18899", "http://127.0.0.1:8793", 10000, 5000, 10, 50000)
    digest = binding.artifact_digest()
    assert replace(binding, generation_id="registered").artifact_digest() == digest
    assert replace(binding, incumbent=new).artifact_digest() == digest
    assert replace(binding, public_endpoint="http://127.0.0.1:9999").artifact_digest() != digest
    assert replace(binding, candidate=replace(new, sha256="0"*64)).artifact_digest() != digest


def test_retained_generation_can_return_without_aba_overwrite(setup):
    driver, old, new, calls = setup
    async def verify(artifact):
        assert driver._current(artifact.name) == artifact.path
        return artifact.sha256
    assert asyncio.run(driver.publish(new, incumbent=old, verify=verify)) == new.sha256
    assert asyncio.run(driver.publish(old, incumbent=new, verify=verify)) == old.sha256
    before = len(calls)
    with pytest.raises(ValueError, match="incumbent conflict"):
        asyncio.run(driver.publish(old, incumbent=new, verify=verify))
    assert len(calls) == before
