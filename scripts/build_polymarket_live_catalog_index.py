#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

from marketcow.polymarket_live import build_live_catalog_index


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build and publish a verified Polymarket live catalog offset index."
    )
    parser.add_argument(
        "--root",
        required=True,
        type=Path,
        help="Polymarket live storage root containing catalog.json and catalogs/.",
    )
    args = parser.parse_args()
    print(json.dumps(build_live_catalog_index(args.root), sort_keys=True))


if __name__ == "__main__":
    main()
