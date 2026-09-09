"""Explicit operator entrypoint for the phase-1 loopback control plane.

No default paths, secret, port, profile or incumbent identity. This executable
does not prepare catalogs or install/modify systemd units. Never serves quotes.
"""
import argparse
import json
from pathlib import Path

import uvicorn

from marketcow.universe_control import UniverseControl
from marketcow.universe_control_http import Caller, create_control_app
from marketcow.universe_phase1 import selection_sha256
from marketcow.universe_prepared_source import PreparedCatalogSource
from marketcow.universe_legacy_binding import verify_legacy_binding


def read_json(path: Path, maximum: int):
    if not path.is_absolute():
        raise ValueError("operator paths must be absolute")
    with path.open("rb") as stream:
        body = stream.read(maximum+1)
    if len(body) > maximum:
        raise ValueError("operator config byte budget exceeded")

    def unique(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("duplicate config key")
            result[key] = value
        return result

    return json.loads(body, object_pairs_hook=unique)


def load_application(config_path: Path):
    config = read_json(config_path, 65536)
    fields = {"host", "port", "profile_path", "profile_sha256", "catalog_path", "catalog_sha256",
              "admission_path", "callers_path", "expected_active_selection_id", "body_timeout_seconds",
              "maximum_catalog_row_bytes", "legacy_incumbent"}
    if not fields <= set(config) or not set(config) <= fields | {"runtime_operations", "discovery_preparation", "hot_operations", "catalog_publication_root"} or config["host"] not in {"127.0.0.1", "::1"}:
        raise ValueError("invalid explicit loopback configuration")
    if config.get("hot_operations") is not None and any(config.get(k) is not None for k in ("runtime_operations", "discovery_preparation")):
        raise ValueError("hot scope and cold replacement controls cannot share an application")
    if type(config["port"]) is not int or not 1024 <= config["port"] <= 65535:
        raise ValueError("invalid explicit port")
    for field in ("profile_path", "catalog_path", "admission_path", "callers_path"):
        if not Path(config[field]).is_absolute():
            raise ValueError("operator paths must be absolute")
    profile = read_json(Path(config["profile_path"]), 65536)
    if selection_sha256(profile) != config["profile_sha256"]:
        raise ValueError("operator profile hash mismatch")
    caller_rows = read_json(Path(config["callers_path"]), 65536)
    if not isinstance(caller_rows, list) or len(caller_rows) > 32:
        raise ValueError("bounded caller list required")
    callers = []
    for item in caller_rows:
        if set(item) != {"identity", "bearer_sha256", "scopes"}:
            raise ValueError("invalid caller binding")
        if not isinstance(item["scopes"], list) or len(item["scopes"]) != len(set(item["scopes"])):
            raise ValueError("invalid caller scopes")
        callers.append(Caller(item["identity"], item["bearer_sha256"], frozenset(item["scopes"])))
    source = PreparedCatalogSource(Path(config["catalog_path"]), expected_sha256=config["catalog_sha256"],
                                   max_row_bytes=config["maximum_catalog_row_bytes"])
    try:
        guard = None
        legacy = config["legacy_incumbent"]
        if legacy is not None:
            if set(legacy) != {"manifest_path", "binding"}:
                raise ValueError("invalid explicit legacy incumbent")
            if legacy["binding"]["catalog_revision"] != source.revision:
                raise ValueError("legacy catalog revision mismatch")

            def guard():
                return verify_legacy_binding(read_json(Path(legacy["manifest_path"]), 2097152),
                                             legacy["binding"], config["expected_active_selection_id"])

            guard()
        elif config["expected_active_selection_id"] is not None:
            raise ValueError("nonempty incumbent requires verifiable binding in phase 1")
        control = UniverseControl(source, profile, Path(config["admission_path"]),
                                  expected_active_selection_id=config["expected_active_selection_id"],
                                  incumbent_guard=guard)
        if config.get('catalog_publication_root') is not None:
            from marketcow.catalog_publication import CatalogPublication
            root = Path(config['catalog_publication_root'])
            if not root.is_absolute():
                raise ValueError('absolute catalog publication root required')
            control.catalog_provider = CatalogPublication(root, max_row_bytes=config['maximum_catalog_row_bytes'])
            control.catalog_provider.current()  # Verify before accepting requests.
        operations = preparation = hot = None
        if config.get("hot_operations") is not None:
            from marketcow.universe_hot_operations import HotScopeOperations
            reference = config["hot_operations"]
            if set(reference) != {"path", "sha256"}:
                raise ValueError("invalid hot scope profile reference")
            hot_config = read_json(Path(reference["path"]), 65536)
            if selection_sha256(hot_config) != reference["sha256"]:
                raise ValueError("hot scope profile hash mismatch")
            hot = HotScopeOperations(control, hot_config)
            control.incumbent_provider = hot.current_discovery_selection
        if config.get("discovery_preparation") is not None:
            from marketcow.universe_discovery_preparation import DiscoveryPreparation
            reference = config["discovery_preparation"]
            if set(reference) != {"path", "sha256"}:
                raise ValueError("invalid preparation profile reference")
            preparation_config = read_json(Path(reference["path"]), 65536)
            if selection_sha256(preparation_config) != reference["sha256"]:
                raise ValueError("preparation profile hash mismatch")
            preparation = DiscoveryPreparation(control, preparation_config)
        if config.get("runtime_operations") is not None:
            from marketcow.universe_generation_operations import GenerationOperations
            reference = config["runtime_operations"]
            if set(reference) != {"path", "sha256"}:
                raise ValueError("invalid runtime registry reference")
            registry = read_json(Path(reference["path"]), 65536)
            if selection_sha256(registry) != reference["sha256"]:
                raise ValueError("runtime registry hash mismatch")
            operations = GenerationOperations(**registry)
            if preparation is not None:
                operations.registration_provider = preparation.registration
        app = create_control_app(control, callers=tuple(callers), body_timeout_seconds=config["body_timeout_seconds"],
                                 runtime_operations=operations, discovery_preparation=preparation, hot_operations=hot)
    except BaseException:
        source.close()
        raise
    return app, source, config, profile


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    args = parser.parse_args()
    app, source, config, profile = load_application(args.config)
    try:
        uvicorn.run(app, host=config["host"], port=config["port"], proxy_headers=False,
                    access_log=False, backlog=16,
                    limit_concurrency=profile["catalog"]["concurrent_readers"]+1)
    finally:
        source.close()


if __name__ == "__main__":
    main()
