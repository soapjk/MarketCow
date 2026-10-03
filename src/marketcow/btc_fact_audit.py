"""Offline full-history audit for a stopped BTC fact archive."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from .btc_fact_log import audit_committed


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("root", type=Path)
    parser.add_argument("--maximum-record-bytes", type=int, default=8 * 1024 * 1024)
    args = parser.parse_args()
    print(json.dumps(audit_committed(args.root, maximum_record_bytes=args.maximum_record_bytes),
                     sort_keys=True, separators=(",", ":")))


if __name__ == "__main__":
    main()
