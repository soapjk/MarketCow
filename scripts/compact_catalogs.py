"""Local-only lossless catalog compaction and explicit historical restoration."""
import argparse
import json
from pathlib import Path
import resource
import shutil
import time

from marketcow.catalog_archive import CatalogArchive, compact


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=Path)
    parser.add_argument("--prune", action="store_true", help="delete verified unpinned full JSONL files")
    parser.add_argument("--restore", metavar="REVISION")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.restore:
        if not args.output or args.prune:
            parser.error("restore requires --output and cannot use --prune")
        CatalogArchive(args.root).restore(args.restore, args.output)
        print(json.dumps({"restored": args.restore, "path": str(args.output)}), flush=True)
    else:
        if args.output:
            parser.error("--output requires --restore")
        files = list((args.root / "catalogs").glob("*.jsonl"))
        free_before = shutil.disk_usage(args.root).free
        started = time.monotonic()
        print(json.dumps({"event": "start", "full_files": len(files),
                          "full_bytes": sum(p.stat().st_size for p in files),
                          "free_bytes": free_before}), flush=True)
        compact(args.root, prune=args.prune, progress=lambda meta: print(json.dumps(meta), flush=True))
        remaining = list((args.root / "catalogs").glob("*.jsonl"))
        print(json.dumps({"event": "complete", "remaining_full_files": len(remaining),
                          "remaining_full_bytes": sum(p.stat().st_size for p in remaining),
                          "archive_bytes": sum(p.stat().st_size for p in (args.root / "catalog-deltas").iterdir()),
                          "free_before_bytes": free_before, "free_after_bytes": shutil.disk_usage(args.root).free,
                          "elapsed_seconds": round(time.monotonic() - started, 2),
                          "max_rss": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss}), flush=True)


if __name__ == "__main__":
    main()
