"""Authorized U1-only control configuration. Never prints secret material."""
import hashlib
import json
import os
from pathlib import Path
import secrets

from marketcow.universe_control import wire_bytes
from marketcow.universe_legacy_binding import legacy_binding, legacy_incumbent_id
from marketcow.universe_phase1 import selection_sha256


def main():
    root = Path("/mnt/p44pro/marketcow-shadow-v3-runtime/linux")
    release = root / "releases/phase1-control-r1"
    phase = root / "phase1"
    targets = [phase/name for name in ("tradude.secret", "callers.json", "operator.json", "deployment.json")]
    if any(path.exists() for path in targets):
        raise FileExistsError("phase1 config already exists; inspect before resuming")
    manifest_path = root / "bounded-discovery-candidate-r1/catalog.json"
    binding = legacy_binding(json.loads(manifest_path.read_bytes()))
    catalog = phase / "catalog-r1.sqlite"
    with catalog.open("rb") as stream:
        catalog_hash = hashlib.file_digest(stream, "sha256").hexdigest()
    profile_path = release / "tests/contracts/phase1/profile.json"
    profile = json.loads(profile_path.read_bytes())
    token = secrets.token_urlsafe(48)
    config = {"host": "127.0.0.1", "port": 18898, "profile_path": str(profile_path),
              "profile_sha256": selection_sha256(profile), "catalog_path": str(catalog),
              "catalog_sha256": catalog_hash, "admission_path": str(phase/"admission.sqlite3"),
              "callers_path": str(phase/"callers.json"),
              "expected_active_selection_id": legacy_incumbent_id(binding),
              "legacy_incumbent": {"manifest_path": str(manifest_path), "binding": binding},
              "body_timeout_seconds": 10, "maximum_catalog_row_bytes": 2097152}
    callers = [{"identity": "tradude-phase1", "bearer_sha256": hashlib.sha256(token.encode()).hexdigest(),
                "scopes": ["catalog.read", "discovery.admit"]}]
    code_hashes = {str(path.relative_to(release)): hashlib.sha256(path.read_bytes()).hexdigest()
                   for path in sorted((release/"src/marketcow").glob("*.py"))}
    deployment = {"profile_sha256": config["profile_sha256"], "catalog_sha256": catalog_hash,
                  "expected_active_selection_id": config["expected_active_selection_id"], "files": code_hashes}
    for path, body in zip(targets, [token.encode(), wire_bytes(callers), wire_bytes(config), wire_bytes(deployment)]):
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(body)
            stream.flush()
            os.fsync(stream.fileno())
    directory = os.open(phase, os.O_RDONLY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)
    print(json.dumps(deployment, sort_keys=True))


if __name__ == "__main__":
    main()
