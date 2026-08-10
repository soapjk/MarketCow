#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

from marketcow.polymarket_live import build_live_candidate_snapshot


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Build and atomically publish the checksum-bound Polymarket "
            "candidate catalog snapshot."
        )
    )
    parser.add_argument(
        "--root",
        required=True,
        type=Path,
        help="Polymarket live storage root containing catalog.json.",
    )
    args = parser.parse_args()
    print(json.dumps(build_live_candidate_snapshot(args.root), sort_keys=True))


if __name__ == "__main__":
    main()
