from __future__ import annotations

import argparse
import json
from pathlib import Path

from marketcow.polymarket_sample import FixedFreeSampleBuilder


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Materialize the pinned free Polymarket Nautilus sample"
    )
    parser.add_argument("--source-root", type=Path, required=True)
    parser.add_argument("--store-root", type=Path, required=True)
    parser.add_argument(
        "--dataset-id", default="polymarket-updown-nautilus-sample-eb4e9fc"
    )
    parser.add_argument("--market-count", type=int, default=20)
    args = parser.parse_args()
    result = FixedFreeSampleBuilder(
        args.source_root, args.store_root
    ).build(args.dataset_id, market_count=args.market_count)
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
