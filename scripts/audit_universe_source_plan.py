"""Read-only bounded source-plan audit against the actual prepared catalog.

Uses an operator-supplied existing Rust plan only as a labelled test selection;
does not claim these identities were newly ranked by Tradude. No listener or
state file is changed. Output can be retained by the operator as audit evidence.
"""
import argparse
import hashlib
import json
import time
from pathlib import Path

from marketcow.universe_prepared_source import PreparedCatalogSource
from marketcow.universe_source_plan import build_source_plan


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", type=Path, required=True)
    parser.add_argument("--catalog-sha256", required=True)
    parser.add_argument("--existing-plan", type=Path, required=True)
    parser.add_argument("--maximum-plan-bytes", type=int, required=True)
    parser.add_argument("--maximum-row-bytes", type=int, required=True)
    parser.add_argument("--maximum-dependencies", type=int, required=True)
    parser.add_argument("--maximum-tokens", type=int, required=True)
    parser.add_argument("--pool", choices=("discovery", "live"), required=True)
    args = parser.parse_args()
    if args.maximum_plan_bytes <= 0:
        raise ValueError("explicit plan byte budget required")
    started = time.monotonic()
    with args.existing_plan.open("rb") as stream:
        raw = stream.read(args.maximum_plan_bytes+1)
    if len(raw) > args.maximum_plan_bytes:
        raise ValueError("plan byte budget exceeded")
    original = json.loads(raw)
    source = PreparedCatalogSource(args.catalog, expected_sha256=args.catalog_sha256,
                                   max_row_bytes=args.maximum_row_bytes)
    try:
        result = build_source_plan(source, catalog_revision=original["catalog_revision"], pool=args.pool,
            market_ids=tuple(sorted(x["market_id"] for x in original["markets"])),
            maximum_dependency_markets=args.maximum_dependencies, maximum_total_tokens=args.maximum_tokens)
    finally:
        source.close()
    assert result["plan"]["markets"] == sorted(original["markets"], key=lambda x: x["market_id"])
    encoded = json.dumps(result, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
    print(json.dumps({"purpose": "existing_selection_readonly_plan_parity", "pool": args.pool,
        "original_plan_sha256": hashlib.sha256(raw).hexdigest(), "result_sha256": hashlib.sha256(encoded).hexdigest(),
        "selected": len(result["plan"]["markets"]), "dependencies": len(result["dependency_markets"]),
        "missing_dependency_market_ids": result["missing_dependency_market_ids"],
        "tokens": result["total_token_count"], "relations": len(result["relations"]),
        "elapsed_seconds": time.monotonic()-started, "passed": True}))


if __name__ == "__main__":
    main()
