"""Generate bounded direct-Rust unit artifacts from a hash-pinned operator unit.

No unit is installed or started here. All paths, listeners and process budgets
come from the operator, never an admission request.
"""
import hashlib
import ipaddress
import os
from pathlib import Path
import re
import shlex
from urllib.parse import urlsplit

from marketcow.universe_systemd import UnitArtifact


def build_unit_pair(*, pool, template: UnitArtifact, binary: Path, binary_sha256,
        candidate_root: Path, preparation, directory: Path, preheat_name,
        preheat_listener, public_listener, preheat_log: Path, public_log: Path,
        maximum_runtime_seconds):
    if pool not in ("live", "discovery") or type(maximum_runtime_seconds) is not int or not 1 <= maximum_runtime_seconds <= 600:
        raise ValueError("explicit bounded preheat profile required")
    if any(not p.is_absolute() for p in (binary, candidate_root, directory, preheat_log, public_log)):
        raise ValueError("absolute operator artifact paths required")
    if (preheat_name == template.name or not re.fullmatch(r"marketcow-universe-[a-zA-Z0-9_-]+\.service", preheat_name)
            or not re.fullmatch(r"marketcow-[a-zA-Z0-9_-]+\.service", template.name)):
        raise ValueError("dedicated preheat unit name required")
    for value in (preheat_listener, public_listener):
        address = urlsplit("//"+value)
        if address.username or address.password or address.path or address.query or address.fragment or not address.port:
            raise ValueError("explicit listener address required")
        ip = ipaddress.ip_address(address.hostname)
        if value == preheat_listener and not ip.is_loopback:
            raise ValueError("preheat listener must remain loopback")
    if directory.exists():
        raise ValueError("refuse existing unit artifact directory")
    with template.path.open("rb") as stream:
        raw = stream.read(65537)
    if len(raw) > 65536 or hashlib.sha256(raw).hexdigest() != template.sha256:
        raise ValueError("unit template hash mismatch")
    with binary.open("rb") as stream:
        if hashlib.file_digest(stream, "sha256").hexdigest() != binary_sha256:
            raise ValueError("runtime binary hash mismatch")
    text = raw.decode()
    starts = [line for line in text.splitlines() if line.startswith("ExecStart=")]
    if len(starts) != 1 or "Restart=on-failure" not in text or "RuntimeMaxSec=" in text:
        raise ValueError("unsupported bounded operator template")
    args = shlex.split(starts[0].removeprefix("ExecStart="))
    indexes = [i for i, value in enumerate(args) if value.endswith("/marketcow-discovery-collector")]
    if len(indexes) != 1:
        raise ValueError("explicit Rust executable required")
    args[indexes[0]] = str(binary)
    updates = {"--root": str(candidate_root), "--expected-market-count": str(preparation["selected"]),
               "--plan-sha256": preparation["plan_sha256"]}
    if pool == "live":
        updates.update({"--plan": str(candidate_root/"rust-scoped-plan-r1.json"),
            "--configured-scope": str(candidate_root/"configured-scope.json"),
            "--configured-scope-sha256": preparation["configured_scope_sha256"],
            "--dependency-plan": str(candidate_root/"live-bridge-plan-r1.json"),
            "--dependency-plan-sha256": preparation["dependency_plan_sha256"]})
        listen_flag = "--public-listen"
    else:
        updates.update({"--plan": str(candidate_root/"rust-discovery-plan.json"),
            "--discovery-seed": str(candidate_root/"discovery-public-seed.json"),
            "--discovery-seed-sha256": preparation["seed_sha256"]})
        listen_flag = "--discovery-listen"
    for flag, value in updates.items():
        if args.count(flag) != 1:
            raise ValueError("operator template flag missing or duplicated")
        args[args.index(flag)+1] = value
    directory.mkdir(mode=0o700)
    result = {}
    for kind, name, listener, log in (("preheat", preheat_name, preheat_listener, preheat_log),
                                     ("candidate", template.name, public_listener, public_log)):
        argv = list(args)
        for flag, value in ((listen_flag, listener), ("--log", str(log))):
            if argv.count(flag) != 1:
                raise ValueError("operator listener/logger flag missing or duplicated")
            argv[argv.index(flag)+1] = value
        if any(any(c in value for c in '\n\r\t "\'\\$%') for value in argv):
            raise ValueError("unsupported systemd argument encoding")
        body = text.replace(starts[0], "ExecStart="+" ".join(argv))
        if kind == "preheat":
            body = body.replace("Restart=on-failure", f"Restart=no\nRuntimeMaxSec={maximum_runtime_seconds}")
        body += "\n# Verified Rust binary SHA256="+binary_sha256+"\n"
        path = directory/name
        with path.open("x") as stream:
            stream.write(body); stream.flush(); os.fsync(stream.fileno())
        path.chmod(0o400)
        result[kind] = UnitArtifact(name, path, hashlib.sha256(body.encode()).hexdigest())
    descriptor = os.open(directory, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    return result
