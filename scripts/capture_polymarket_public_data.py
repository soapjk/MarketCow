#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

from marketcow.polymarket_live import (
    PUBLIC_DATA_KINDS,
    DataApiPublicClient,
    DataApiPublicNormalizer,
    atomic_write_public_facts,
)
from marketcow.polymarket_sources import utc_now


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Capture one free public Polymarket Data API fact page locally"
    )
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--kind", choices=sorted(PUBLIC_DATA_KINDS), required=True)
    parser.add_argument(
        "--params-json", default="{}",
        help="Official endpoint query parameters as a JSON object",
    )
    arguments = parser.parse_args()
    parameters = json.loads(arguments.params_json)
    if not isinstance(parameters, dict):
        raise SystemExit("--params-json must decode to an object")
    rows = DataApiPublicClient().fetch(arguments.kind, params=parameters)
    normalized = DataApiPublicNormalizer.normalize(
        arguments.kind, rows, utc_now()
    )
    path = atomic_write_public_facts(
        arguments.root, arguments.kind, normalized
    )
    print({"kind": arguments.kind, "count": len(normalized), "path": str(path)})


if __name__ == "__main__":
    main()
