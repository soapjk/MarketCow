"""Concrete bounded systemd process/atomic unit-link adapter for U1.

Only operator-registered immutable unit files are accepted. No client-provided
commands, shell execution, secret arguments or account APIs. Link publication
is control state; HTTP/WS verification remains mandatory before acknowledgement.
"""
from __future__ import annotations

import asyncio
import hashlib
import os
import re
import shlex
import tempfile
from dataclasses import asdict, dataclass
from pathlib import Path

from marketcow.universe_live_probe import probe_live
from marketcow.universe_discovery_probe import probe_discovery
from marketcow.universe_phase1 import selection_sha256


@dataclass(frozen=True)
class UnitArtifact:
    name: str
    path: Path
    sha256: str

    def verify(self, release_root: Path):
        if not re.fullmatch(r"marketcow-[a-zA-Z0-9_-]+\.service", self.name):
            raise ValueError("invalid registered service name")
        root = release_root.resolve(strict=True)
        path = self.path.resolve(strict=True)
        if not path.is_relative_to(root) or not path.is_file() or path.stat().st_size > 65536:
            raise ValueError("unit outside release or too large")
        raw = path.read_bytes()
        if hashlib.sha256(raw).hexdigest() != self.sha256:
            raise ValueError("unit artifact hash mismatch")
        text = raw.decode()
        markers = [line.removeprefix("# Verified Rust binary SHA256=") for line in text.splitlines()
                   if line.startswith("# Verified Rust binary SHA256=")]
        if markers:
            starts = [line.removeprefix("ExecStart=") for line in text.splitlines() if line.startswith("ExecStart=")]
            if len(markers) != 1 or len(starts) != 1 or not re.fullmatch("[0-9a-f]{64}", markers[0]):
                raise ValueError("invalid binary attestation")
            binaries = [Path(arg).resolve(strict=True) for arg in shlex.split(starts[0])
                        if arg.endswith("/marketcow-discovery-collector")]
            if len(binaries) != 1 or not binaries[0].is_relative_to(root) or binaries[0].stat().st_size > 67108864:
                raise ValueError("attested binary outside release budget")
            with binaries[0].open("rb") as stream:
                if hashlib.file_digest(stream, "sha256").hexdigest() != markers[0]:
                    raise ValueError("runtime binary hash mismatch")
        return path


@dataclass(frozen=True)
class LiveUnitBinding:
    """Operator-built artifacts; never accepted directly from admission JSON."""
    generation_id: str
    scope_id: str
    preheat: UnitArtifact
    candidate: UnitArtifact
    incumbent: UnitArtifact
    preheat_endpoint: str
    public_endpoint: str
    full_sync_bytes: int
    frame_bytes: int
    maximum_frames: int
    maximum_stream_bytes: int

    def artifact_digest(self):
        # Avoid the generation_id -> artifact hash -> generation_id cycle.
        # The incumbent is an operation CAS precondition, not candidate content:
        # including it would make returning to a retained generation impossible
        # after A->B (A would still demand its original predecessor). The pinned
        # operator operation config and actual unit link verify it separately.
        value = asdict(self)
        del value["generation_id"]
        del value["incumbent"]
        for key in ("preheat", "candidate"):
            value[key]["path"] = str(value[key]["path"])
        return selection_sha256(value)


class SystemdLiveRuntime:
    """Concrete Live process adapter. Discovery has a different wire protocol.

    Both units must already be built/registered from the same candidate root.
    Never run preheat and public units concurrently against its writable DB.
    The caller holds its supervisor owner lock and coordinates Paper maintenance.
    """
    def __init__(self, units, binding: LiveUnitBinding):
        self.units, self.binding = units, binding
        self.boundary = None

    def _check(self, generation):
        b = self.binding
        if generation.pool != "live" or generation.generation_id != b.generation_id:
            raise ValueError("runtime generation binding mismatch")
        if generation.artifact_sha256 != b.artifact_digest():
            raise ValueError("runtime artifact binding mismatch")
        if b.preheat.name in (b.candidate.name, b.incumbent.name):
            raise ValueError("preheat must have independent unit identity")
        for artifact in (b.preheat, b.candidate, b.incumbent):
            artifact.verify(self.units.releases)

    async def _probe(self, generation, endpoint):
        b = self.binding
        await self._wait_listener(endpoint)
        return await probe_live(endpoint, scope_id=b.scope_id,
                                catalog_revision=generation.catalog_revision, market_ids=generation.market_ids,
                                timeout_seconds=self.units.timeout, full_sync_bytes=b.full_sync_bytes,
                                frame_bytes=b.frame_bytes, maximum_frames=b.maximum_frames,
                                maximum_stream_bytes=b.maximum_stream_bytes)

    async def _wait_listener(self, endpoint):
        """Type=simple start is not listener readiness; bounded TCP wait only.

        Avoid creating repeated full-sync leases while waiting for startup.
        Protocol/scope validation still occurs once in the actual probe.
        """
        from urllib.parse import urlsplit
        address = urlsplit(endpoint)
        artifact = self.binding.preheat if endpoint == self.binding.preheat_endpoint else self.binding.candidate
        async with asyncio.timeout(self.units.timeout):
            while True:
                state = await self.units.status(artifact)
                if state.get("ActiveState") != "active" or state.get("MainPID") in (None, "0"):
                    raise RuntimeError("candidate exited before listener readiness")
                try:
                    _, stream = await asyncio.wait_for(asyncio.open_connection(address.hostname,
                        address.port or (443 if address.scheme == "https" else 80)), 1)
                except (OSError, TimeoutError):
                    await asyncio.sleep(.1)
                    continue
                stream.close()
                await stream.wait_closed()
                return

    async def prepare(self, generation):
        self._check(generation)
        b = self.binding
        await self.units.start(b.preheat)
        try:
            self.boundary = await self._probe(generation, b.preheat_endpoint)
        except BaseException:
            cleanup = asyncio.create_task(self.units.stop(b.preheat))
            try:
                await asyncio.shield(cleanup)
            except asyncio.CancelledError:
                await cleanup
                raise
            raise

    async def publish(self, generation, epoch):
        self._check(generation)
        if type(epoch) is not int or epoch <= 0 or self.boundary is None:
            raise ValueError("preheat and epoch required")
        b = self.binding
        stopped = await self.units.stop(b.preheat)
        if stopped.get("Result") != "success" or stopped.get("ExecMainStatus") != "0":
            raise ValueError("preheat did not stop cleanly")
        async def verify(_):
            observed = await self._probe(generation, b.public_endpoint)
            # Final endpoint starts a fresh process/instance; never reuse the
            # preheat stream cursor as a client resume cursor.
            if observed.stream_instance_id == self.boundary.stream_instance_id:
                raise ValueError("restarted runtime reused preheat instance")
            return observed
        self.boundary = await self.units.publish(b.candidate, incumbent=b.incumbent, verify=verify)

    async def probe(self, generation, epoch):
        self._check(generation)
        if type(epoch) is not int or epoch <= 0:
            raise ValueError("runtime epoch required")
        if self.units._current(self.binding.candidate.name) != self.binding.candidate.path.resolve():
            raise ValueError("candidate not published")
        state = await self.units.status(self.binding.candidate)
        if state.get("ActiveState") != "active" or state.get("MainPID") in (None, "0"):
            raise ValueError("candidate not active")
        return await self._probe(generation, self.binding.public_endpoint)


@dataclass(frozen=True)
class DiscoveryUnitBinding(LiveUnitBinding):
    projection_id: str
    universe_revision: str


class SystemdDiscoveryRuntime(SystemdLiveRuntime):
    """Real Discovery units share process mechanics, not Live wire semantics."""
    def _check(self, generation):
        b = self.binding
        if generation.pool != "discovery" or generation.generation_id != b.generation_id:
            raise ValueError("runtime generation binding mismatch")
        if generation.artifact_sha256 != b.artifact_digest():
            raise ValueError("runtime artifact binding mismatch")
        if b.preheat.name in (b.candidate.name, b.incumbent.name):
            raise ValueError("preheat must have independent unit identity")
        for artifact in (b.preheat, b.candidate, b.incumbent):
            artifact.verify(self.units.releases)

    async def _probe(self, generation, endpoint):
        b = self.binding
        await self._wait_listener(endpoint)
        return await probe_discovery(endpoint, projection_id=None,
                                     universe_revision=b.universe_revision,
                                     catalog_revision=generation.catalog_revision, market_ids=generation.market_ids,
                                     timeout_seconds=self.units.timeout, full_sync_bytes=b.full_sync_bytes,
                                     frame_bytes=b.frame_bytes, maximum_frames=b.maximum_frames,
                                     maximum_stream_bytes=b.maximum_stream_bytes)

    async def publish(self, generation, epoch):
        self._check(generation)
        if type(epoch) is not int or epoch <= 0 or self.boundary is None:
            raise ValueError("preheat and epoch required")
        b = self.binding
        stopped = await self.units.stop(b.preheat)
        if stopped.get("Result") != "success" or stopped.get("ExecMainStatus") != "0":
            raise ValueError("preheat did not stop cleanly")
        async def verify(_):
            observed = await self._probe(generation, b.public_endpoint)
            if observed.stream_instance_id == self.boundary.stream_instance_id:
                raise ValueError("restarted Discovery reused preheat projection")
            return observed
        self.boundary = await self.units.publish(b.candidate, incumbent=b.incumbent, verify=verify)


class SystemdUnits:
    """Caller must hold one exclusive lifetime supervisor lock.

    Timeout means service state is uncertain: terminating systemctl does not
    cancel a systemd job. Re-read status before recovery; never report success.
    """
    def __init__(self, *, release_root: Path, user_unit_root: Path, command_timeout_seconds: float):
        self.releases = release_root.resolve(strict=True)
        self.units = user_unit_root.resolve(strict=True)
        if not 0 < command_timeout_seconds < float("inf"):
            raise ValueError("explicit command deadline required")
        self.timeout = command_timeout_seconds

    async def _command(self, *args):
        # Only controlled callers below can construct systemctl arguments.
        process = await asyncio.create_subprocess_exec(
            "systemctl", "--user", *args, stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL)
        try:
            async with asyncio.timeout(self.timeout):
                output = bytearray()
                while chunk := await process.stdout.read(4096):
                    if len(output)+len(chunk) > 16384:
                        raise ValueError("systemctl output budget")
                    output.extend(chunk)
                code = await process.wait()
                if code != 0:
                    raise RuntimeError(f"systemctl failed exit={code}")
                return output.decode("utf-8")
        except BaseException:
            if process.returncode is None:
                process.kill()
            await process.wait()
            raise

    async def status(self, artifact: UnitArtifact):
        artifact.verify(self.releases)
        output = await self._command("show", artifact.name, "-p", "MainPID", "-p", "ActiveState",
                                     "-p", "Result", "-p", "ExecMainStatus", "-p", "FragmentPath")
        return dict(line.split("=", 1) for line in output.splitlines() if "=" in line)

    def _current(self, name):
        path = self.units/name
        if not path.is_symlink():
            raise ValueError("managed unit must be explicit release symlink")
        resolved = path.resolve(strict=True)
        if not resolved.is_relative_to(self.releases):
            raise ValueError("incumbent unit outside releases")
        return resolved

    def replace_link(self, candidate: UnitArtifact, *, expected: UnitArtifact):
        target = candidate.verify(self.releases)
        incumbent = expected.verify(self.releases)
        if candidate.name != expected.name or self._current(candidate.name) != incumbent:
            raise ValueError("unit incumbent conflict")
        # Exclusive owner lock prevents competing supervisors between check/replace.
        temporary = Path(tempfile.mkdtemp(prefix=".marketcow-unit-", dir=self.units))
        link = temporary/candidate.name
        try:
            link.symlink_to(target)
            os.replace(link, self.units/candidate.name)
            descriptor = os.open(self.units, os.O_RDONLY)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
        finally:
            if link.is_symlink():
                link.unlink()
            temporary.rmdir()

    async def start(self, artifact: UnitArtifact):
        if self._current(artifact.name) != artifact.verify(self.releases):
            raise ValueError("start artifact not installed")
        await self._command("start", artifact.name)
        state = await self.status(artifact)
        if state.get("ActiveState") != "active" or state.get("MainPID") in (None, "0"):
            raise RuntimeError("service did not start")
        return state  # Not a ready receipt.

    async def stop(self, artifact: UnitArtifact):
        if self._current(artifact.name) != artifact.verify(self.releases):
            raise ValueError("stop artifact not installed")
        await self._command("stop", artifact.name)
        state = await self.status(artifact)
        if state.get("MainPID") != "0" or state.get("ActiveState") not in ("inactive", "failed"):
            raise RuntimeError("service still running")
        return state  # Failed exit remains explicit in Result/ExecMainStatus.

    async def publish(self, candidate: UnitArtifact, *, incumbent: UnitArtifact, verify):
        """Coordinated stop/switch/start/real-probe, with restoration on failure.

        No automatic market selection. Probe is passed the candidate and must
        bind HTTP+WS to its artifact. Caller provides a real Paper pause before
        invoking this on the formal Live service.
        """
        candidate.verify(self.releases)
        incumbent.verify(self.releases)
        if candidate.name != incumbent.name or self._current(incumbent.name) != incumbent.path.resolve():
            raise ValueError("unit incumbent conflict")
        switched = False
        try:
            await self.stop(incumbent)
            self.replace_link(candidate, expected=incumbent)
            switched = True
            await self._command("daemon-reload")
            await self.start(candidate)
            async with asyncio.timeout(self.timeout):
                return await verify(candidate)
        except BaseException:
            async def restore():
                # Re-evaluate actual link even if fsync failed after replace.
                current = self._current(candidate.name)
                if switched or current == candidate.path.resolve():
                    await self.stop(candidate)
                    self.replace_link(incumbent, expected=candidate)
                await self._command("daemon-reload")
                await self.start(incumbent)
            recovery = asyncio.create_task(restore())
            try:
                await asyncio.shield(recovery)
            except asyncio.CancelledError:
                await recovery
                raise
            raise
