"""Local catalog-only import/read commands. No official GETs or pool operations.

Input JSONL must come from the validated phase-1 mapper, not raw Gamma responses.
Freshness bounds are explicit; no existing spool is automatically reused.
"""

import argparse
import json
from pathlib import Path

from marketcow.catalog_incremental import CatalogChanges


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--database", type=Path, required=True)
    p.add_argument("--max-database-bytes", type=int, required=True)
    p.add_argument("--max-record-bytes", type=int, required=True)
    p.add_argument("--max-batch-records", type=int, required=True)
    commands = p.add_subparsers(dest="operation", required=True)
    put = commands.add_parser("import-validated")
    put.add_argument("--records", type=Path, required=True)
    put.add_argument("--capture-id", required=True)
    put.add_argument("--started-at", required=True)
    put.add_argument("--completed-at", required=True)
    put.add_argument("--expected-revision", required=True)
    put.add_argument("--coverage-json", required=True)
    get = commands.add_parser("changes")
    get.add_argument("--after-sequence", type=int, required=True)
    get.add_argument("--limit", type=int, required=True)
    get.add_argument("--byte-budget", type=int, required=True)
    commands.add_parser("head")
    args = p.parse_args()
    if not args.database.is_absolute():
        p.error("absolute database path required")
    store = CatalogChanges(
        args.database,
        max_database_bytes=args.max_database_bytes,
        max_record_bytes=args.max_record_bytes,
        max_batch_records=args.max_batch_records,
    )
    try:
        if args.operation == "import-validated":

            def records():
                with args.records.open("rb") as f:
                    while True:
                        line = f.readline(args.max_record_bytes + 1)
                        if not line:
                            return
                        if len(line) > args.max_record_bytes:
                            raise ValueError("input row byte budget exceeded")
                        yield json.loads(line)

            result = store.publish(
                records(),
                capture_id=args.capture_id,
                started_at=args.started_at,
                completed_at=args.completed_at,
                expected_revision=args.expected_revision,
                coverage=json.loads(args.coverage_json),
            )
        elif args.operation == "changes":
            result = store.changes(after_sequence=args.after_sequence, limit=args.limit, byte_budget=args.byte_budget)
        else:
            result = store.head()
        print(json.dumps(result, ensure_ascii=False))
    finally:
        store.close()


if __name__ == "__main__":
    main()
