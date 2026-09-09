"""Prepare a NEW phase-1 metadata database from an existing immutable manifest.

No network requests, service operations or runtime root mutations. Coverage is
explicitly unverified unless source traversal evidence is separately audited;
this command intentionally has no flag to silently promote it to complete.
"""
import argparse
import json
from pathlib import Path

from marketcow.universe_catalog_prepare import prepare_catalog
from marketcow.universe_control_server import read_json


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--source-predicate", required=True)
    parser.add_argument("--metric-unit", required=True, help="explicit source unit, or 'unknown'")
    parser.add_argument("--max-input-row-bytes", type=int, required=True)
    parser.add_argument("--max-output-row-bytes", type=int, required=True)
    parser.add_argument("--max-records", type=int, required=True)
    parser.add_argument("--max-database-bytes", type=int, required=True)
    args = parser.parse_args()
    if not args.output.is_absolute():
        parser.error("output must be absolute")
    manifest = read_json(args.manifest, 2097152)
    source, normal = manifest["catalog_source"], manifest["normalized_catalog"]
    if source["source"] != "polymarket_gamma" or source["raw_format"] != "canonical_jsonl":
        raise ValueError("unsupported source binding")
    root = args.manifest.parent.resolve()
    raw_path, normalized_path = Path(source["raw_path"]).resolve(), Path(normal["path"]).resolve()
    if not raw_path.is_relative_to(root / "raw") or not normalized_path.is_relative_to(root / "catalogs"):
        raise ValueError("source paths outside declared data root")
    report = prepare_catalog(
        raw_path=raw_path, normalized_path=normalized_path, output=args.output,
        raw_sha256=source["raw_payload_sha256"], normalized_sha256=normal["sha256"],
        revision=manifest["catalog_revision"], raw_count=source["market_count"],
        normalized_count=normal["market_count"], observed_at=source["observed_at"],
        source_predicate=args.source_predicate, traversal_verified=False,
        metric_unit=None if args.metric_unit == "unknown" else args.metric_unit,
        max_input_row_bytes=args.max_input_row_bytes, max_output_row_bytes=args.max_output_row_bytes,
        max_records=args.max_records, max_database_bytes=args.max_database_bytes,
    )
    print(json.dumps(report, sort_keys=True))


if __name__ == "__main__":
    main()
