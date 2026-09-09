"""Freeze an operator release and unit artifacts; never install or restart units."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shlex
import shutil

from marketcow.polymarket_contracts import canonical_json, content_sha256

R = Path("/mnt/p44pro/marketcow-shadow-v3-runtime/linux")
UNITS = Path("/home/czx/.config/systemd/user")


def sha(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def write(path, value):
    raw = value.encode() if isinstance(value, str) else canonical_json(value)
    with path.open("xb") as stream:
        stream.write(raw); stream.flush(); os.fsync(stream.fileno())
    path.chmod(0o400)
    return sha(path)


def argv_line(text):
    lines = [line for line in text.splitlines() if line.startswith("ExecStart=")]
    if len(lines) != 1:
        raise ValueError("exact incumbent ExecStart required")
    return lines[0], shlex.split(lines[0].split("=", 1)[1])


def safe_start(argv):
    if any(any(c in item for c in '\n\r\t "\'\\$%') for item in argv):
        raise ValueError("unsafe operator unit token")
    return "ExecStart="+" ".join(argv)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--release", required=True)
    p.add_argument("--binary-sha256", required=True)
    p.add_argument("--caller-identity", required=True)
    args = p.parse_args()
    if not args.release.isascii() or not args.release.replace("-", "").isalnum():
        raise ValueError("release name")
    os.umask(0o077)
    source_binary = R/"target/release/marketcow-discovery-collector"
    assert sha(source_binary) == args.binary_sha256
    release = R/"releases"/args.release
    release.mkdir(mode=0o700)  # Never overwrite or activate an existing release.
    (release/"units").mkdir()
    binary = release/source_binary.name
    shutil.copyfile(source_binary, binary); binary.chmod(0o500)
    assert sha(binary) == args.binary_sha256
    shutil.copytree(R/"tmp/universe-python-r1/marketcow", release/"src/marketcow",
                    ignore=shutil.ignore_patterns("__pycache__", "*.pyc"))
    report = dict(formal_changed=False, binary_sha256=sha(binary), units={}, files={})
    runtime_profiles = {}
    source_root = None
    for pool, name, sockets in (("live", "marketcow-polymarket-collector.service", 16),
                                ("discovery", "marketcow-polymarket-discovery.service", 128)):
        incumbent = (UNITS/name).resolve(strict=True)
        text = incumbent.read_text(); start, argv = argv_line(text)
        indexes = [i for i, item in enumerate(argv) if item.endswith("/marketcow-discovery-collector")]
        assert len(indexes) == 1
        argv[indexes[0]] = str(binary)
        assert argv.count("--root") == 1 and "--scope-control-socket" not in argv
        root = Path(argv[argv.index("--root")+1]).resolve(strict=True)
        assert root.parent == R
        private = R/(args.release+"-"+pool+"-ctl")
        private.mkdir(mode=0o700)
        socket_path = private/"scope.sock"
        assert len(str(socket_path).encode()) < 108
        argv += ["--scope-control-socket", str(socket_path), "--scope-control-bytes", "16777216",
            "--scope-control-timeout-seconds", "20", "--scope-state-file", str(private/"state.json"),
            "--scope-state-bytes", "134217728", "--scope-retire-grace-seconds", "30",
            "--acquisition-token-budget", "4096", "--acquisition-socket-budget", str(sockets)]
        assert argv.count("--log") == 1
        argv[argv.index("--log")+1] = str(R/"logs"/(args.release+"-"+pool+".log"))
        memory = [line for line in text.splitlines() if line.startswith("MemoryMax=")]
        assert len(memory) == 1
        updated = text.replace(start, safe_start(argv)).replace(memory[0], "MemoryMax=6G")
        unit_path = release/"units"/name
        report["units"][name] = dict(incumbent_path=str(incumbent), incumbent_sha256=sha(incumbent),
            candidate_path=str(unit_path), candidate_sha256=write(unit_path, updated), root=str(root))
        runtime_profiles[pool] = dict(socket_path=str(socket_path), maximum_bytes=16777216, timeout_seconds=20)
        if pool == "discovery":
            source_root = root
            seed = json.loads(Path(argv[argv.index("--discovery-seed")+1]).read_bytes())
    current = json.loads((R/"phase1/operator.json").read_bytes())
    hot = dict(store_path=str(R/"phase1/hot-scope.sqlite"), owner_lock=str(R/"phase1/hot-owner.lock"),
        source_root=str(source_root), maximum_candidates=4, maximum_artifact_bytes=134217728,
        maximum_row_bytes=2097152, maximum_source_bytes=17179869184, maximum_relation_members=16384,
        maximum_metadata_tokens=16384, maximum_dependency_markets=4096,
        depth_quantities=seed["depth_quantities"], maximum_book_age_ms=seed["maximum_book_age_ms"],
        candidate_ttl_seconds=900, legacy_discovery_universe=current["legacy_incumbent"]["binding"]["universe_revision"],
        legacy_selection_id=current["expected_active_selection_id"], runtimes=runtime_profiles)
    write(release/"hot-operator.json", hot)
    current["hot_operations"] = dict(path=str(release/"hot-operator.json"), sha256=content_sha256(hot))
    # Reuse existing dedicated bearer digests, never copy or print secrets.
    callers = json.loads(Path(current["callers_path"]).read_bytes())
    assert sum(caller["identity"] == args.caller_identity for caller in callers) == 1
    for caller in callers:
        if caller["identity"] == args.caller_identity:
            caller["scopes"] = sorted(set(caller["scopes"]) | {"hot.read", "hot.prepare", "hot.activate", "hot.retire"})
    write(release/"callers.json", callers)
    current["callers_path"] = str(release/"callers.json")
    write(release/"operator.json", current)
    name = "marketcow-phase1-control.service"
    incumbent = (UNITS/name).resolve(strict=True)
    text = incumbent.read_text(); start, argv = argv_line(text)
    assert argv.count("--config") == 1 and argv.count("--log") == 1
    argv[argv.index("--config")+1] = str(release/"operator.json")
    argv[argv.index("--log")+1] = str(R/"logs"/(args.release+"-control.log"))
    paths = [line for line in text.splitlines() if line.startswith("Environment=PYTHONPATH=")]
    assert len(paths) == 1
    updated = text.replace(start, safe_start(argv)).replace(paths[0],
        "Environment=PYTHONPATH="+str(release/"src")+":"+str(R/"read-api-packages"))
    memory = [line for line in updated.splitlines() if line.startswith("MemoryMax=")]
    assert len(memory) == 1
    updated = updated.replace(memory[0], "MemoryMax=1536M")
    unit_path = release/"units"/name
    report["units"][name] = dict(incumbent_path=str(incumbent), incumbent_sha256=sha(incumbent),
        candidate_path=str(unit_path), candidate_sha256=write(unit_path, updated))
    for path in sorted(release.rglob("*")):
        if path.is_file():
            report["files"][str(path.relative_to(release))] = sha(path)
    write(release/"manifest.json", report)
    print(json.dumps(dict(release=str(release), manifest_sha256=sha(release/"manifest.json"),
        binary_sha256=sha(binary), formal_changed=False)))


if __name__ == "__main__":
    main()
