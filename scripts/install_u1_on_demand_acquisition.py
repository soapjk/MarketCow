"""Install demand-driven acquisition into the current U1 formal services.

This is a forward-only software update. It preserves the existing Live and
Discovery roots, scopes, private control sockets, catalog data and caller
secret digests. It does not access Paper accounts or activate a market scope.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shlex
import shutil
import subprocess
import time


RUNTIME = Path("/mnt/p44pro/marketcow-shadow-v3-runtime/linux")
UNITS = Path("/home/czx/.config/systemd/user")
SERVICES = {
    "live": "marketcow-polymarket-collector.service",
    "discovery": "marketcow-polymarket-discovery.service",
}
CONTROL = "marketcow-phase1-control.service"


def sha(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def write_new(path: Path, body: bytes | str) -> str:
    raw = body.encode() if isinstance(body, str) else body
    with path.open("xb") as stream:
        stream.write(raw)
        stream.flush()
        os.fsync(stream.fileno())
    path.chmod(0o400)
    return sha(path)


def argv_line(text: str) -> tuple[str, list[str]]:
    lines = [line for line in text.splitlines() if line.startswith("ExecStart=")]
    if len(lines) != 1:
        raise ValueError("one ExecStart required")
    return lines[0], shlex.split(lines[0].split("=", 1)[1])


def safe_start(argv: list[str]) -> str:
    if any(any(char in item for char in '\n\r\t "\'\\$%') for item in argv):
        raise ValueError("unsafe systemd argument")
    return "ExecStart=" + " ".join(argv)


def state(name: str) -> dict[str, str]:
    output = subprocess.check_output(
        ["systemctl", "--user", "show", name, "-p", "MainPID",
         "-p", "ActiveState", "-p", "Result", "-p", "ExecMainStatus"],
        text=True,
        timeout=10,
    )
    return dict(line.split("=", 1) for line in output.splitlines())


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--commit", required=True)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--python-source", type=Path, required=True)
    parser.add_argument("--release", required=True)
    parser.add_argument("--lease-capacity", type=int, default=8)
    parser.add_argument("--lease-max-seconds", type=int, default=120)
    args = parser.parse_args()
    if (len(args.commit) != 40 or any(c not in "0123456789abcdef" for c in args.commit)
            or not args.release.isascii()
            or not args.release.replace("-", "").isalnum()
            or not 1 <= args.lease_capacity <= 64
            or not 1 <= args.lease_max_seconds <= 3600):
        raise ValueError("invalid release profile")
    binary = args.binary.resolve(strict=True)
    python_source = args.python_source.resolve(strict=True)
    if not (python_source / "universe_control_http.py").is_file():
        raise ValueError("MarketCow Python source required")

    os.umask(0o077)
    release = RUNTIME / "releases" / args.release
    release.mkdir(mode=0o700)
    (release / "units").mkdir()
    shutil.copy2(binary, release / "marketcow-discovery-collector")
    (release / "marketcow-discovery-collector").chmod(0o500)
    shutil.copytree(
        python_source,
        release / "src" / "marketcow",
        ignore=shutil.ignore_patterns("__pycache__", "*.pyc"),
    )

    manifest: dict = {
        "schema_version": "marketcow.u1-on-demand-release.v1",
        "commit": args.commit,
        "created_at_ns": time.time_ns(),
        "binary_sha256": sha(release / "marketcow-discovery-collector"),
        "lease_profile": {
            "required": True,
            "maximum_leases": args.lease_capacity,
            "maximum_ttl_seconds": args.lease_max_seconds,
        },
        "services": {},
        "data_roots_unchanged": True,
        "scope_activation_performed": False,
        "paper_account_accessed": False,
    }
    lease_args = [
        "--acquisition-lease-required",
        "--acquisition-lease-capacity", str(args.lease_capacity),
        "--acquisition-lease-max-seconds", str(args.lease_max_seconds),
    ]
    for pool, name in SERVICES.items():
        incumbent = (UNITS / name).resolve(strict=True)
        text = incumbent.read_text()
        start, argv = argv_line(text)
        indexes = [i for i, value in enumerate(argv)
                   if value.endswith("/marketcow-discovery-collector")]
        if len(indexes) != 1 or "--input-mode" not in argv or argv[argv.index("--input-mode") + 1] != "websocket":
            raise ValueError(f"unexpected {name} command")
        if "--scope-control-socket" not in argv:
            raise ValueError(f"unexpected {name} control profile")
        root = Path(argv[argv.index("--root") + 1]).resolve(strict=True)
        argv[indexes[0]] = str(release / "marketcow-discovery-collector")
        if "--acquisition-lease-required" in argv:
            if (argv.count("--acquisition-lease-required") != 1
                    or argv.count("--acquisition-lease-capacity") != 1
                    or argv.count("--acquisition-lease-max-seconds") != 1):
                raise ValueError(f"ambiguous {name} lease profile")
            argv[argv.index("--acquisition-lease-capacity") + 1] = str(args.lease_capacity)
            argv[argv.index("--acquisition-lease-max-seconds") + 1] = str(args.lease_max_seconds)
        else:
            argv.extend(lease_args)
        if argv.count("--log") != 1:
            raise ValueError(f"unexpected {name} log")
        argv[argv.index("--log") + 1] = str(RUNTIME / "logs" / f"{args.release}-{pool}.log")
        updated = text.replace(start, safe_start(argv))
        unit = release / "units" / name
        digest = write_new(unit, updated)
        manifest["services"][name] = {
            "unit_sha256": digest,
            "root": str(root),
            "scope_control_socket": argv[argv.index("--scope-control-socket") + 1],
        }

    incumbent = (UNITS / CONTROL).resolve(strict=True)
    text = incumbent.read_text()
    start, argv = argv_line(text)
    if argv.count("--config") != 1 or argv.count("--log") != 1:
        raise ValueError("unexpected control command")
    old_config = Path(argv[argv.index("--config") + 1]).resolve(strict=True)
    config = json.loads(old_config.read_bytes())
    old_callers = Path(config["callers_path"]).resolve(strict=True)
    callers = json.loads(old_callers.read_bytes())
    if len(callers) != 1 or callers[0].get("identity") != "tradude-phase1":
        raise ValueError("dedicated Tradude caller required")
    callers[0]["scopes"] = sorted(set(callers[0]["scopes"]) | {"acquisition.lease"})
    callers_path = release / "callers.json"
    write_new(callers_path, json.dumps(callers, sort_keys=True, separators=(",", ":")))
    config["callers_path"] = str(callers_path)
    config_path = release / "operator.json"
    write_new(config_path, json.dumps(config, sort_keys=True, separators=(",", ":")))
    argv[argv.index("--config") + 1] = str(config_path)
    argv[argv.index("--log") + 1] = str(RUNTIME / "logs" / f"{args.release}-control.log")
    paths = [line for line in text.splitlines() if line.startswith("Environment=PYTHONPATH=")]
    if len(paths) != 1:
        raise ValueError("one control PYTHONPATH required")
    suffix = paths[0].split("=", 2)[2].split(":", 1)[1]
    updated = text.replace(start, safe_start(argv)).replace(
        paths[0], f"Environment=PYTHONPATH={release / 'src'}:{suffix}"
    )
    unit = release / "units" / CONTROL
    digest = write_new(unit, updated)
    manifest["services"][CONTROL] = {"unit_sha256": digest}

    subprocess.run(
        ["systemd-analyze", "--user", "verify",
         *[str(release / "units" / name) for name in (*SERVICES.values(), CONTROL)]],
        check=True,
        timeout=30,
    )
    files = {}
    for path in sorted(release.rglob("*")):
        if path.is_file():
            files[str(path.relative_to(release))] = sha(path)
    manifest["files"] = files
    manifest_path = release / "manifest.json"
    write_new(manifest_path, json.dumps(manifest, indent=2, sort_keys=True))

    for name in (CONTROL, *SERVICES.values()):
        subprocess.run(["systemctl", "--user", "stop", name], check=True, timeout=90)
        stopped = state(name)
        if stopped["MainPID"] != "0":
            raise RuntimeError(f"failed to stop {name}: {stopped}")
    for name in (*SERVICES.values(), CONTROL):
        pending = UNITS / ("." + name + ".on-demand")
        pending.symlink_to(release / "units" / name)
        os.replace(pending, UNITS / name)
    directory = os.open(UNITS, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)
    subprocess.run(["systemctl", "--user", "daemon-reload"], check=True, timeout=20)
    for name in (*SERVICES.values(), CONTROL):
        subprocess.run(["systemctl", "--user", "start", name], check=True, timeout=60)
    states = {name: state(name) for name in (*SERVICES.values(), CONTROL)}
    if any(value["ActiveState"] != "active" or value["MainPID"] == "0" for value in states.values()):
        raise RuntimeError(f"formal service start failed: {states}")
    receipt = {
        "schema_version": "marketcow.u1-on-demand-installation.v1",
        "release": str(release),
        "manifest_sha256": sha(manifest_path),
        "installed_at_ns": time.time_ns(),
        "states": states,
        "data_roots_unchanged": True,
        "scope_activation_performed": False,
        "paper_account_accessed": False,
        "network_readiness_not_inferred": True,
    }
    receipt_path = release / "installation.json"
    write_new(receipt_path, json.dumps(receipt, indent=2, sort_keys=True))
    print(json.dumps(receipt, sort_keys=True))


if __name__ == "__main__":
    main()
