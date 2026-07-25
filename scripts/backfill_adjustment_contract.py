from __future__ import annotations

import argparse
import json

from marketcow.adjustment_backfill import AdjustmentBackfillService
from marketcow.clickhouse_repositories import (
    ClickHouseDatabase,
    ClickHouseMarketBarRepository,
)
from marketcow.config import Settings


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Audit or conservatively backfill explicit adjustment fields"
    )
    parser.add_argument(
        "--profile", choices=("development", "production", "test"),
        default="development",
    )
    parser.add_argument("--limit", type=int, default=10000)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument(
        "--confirm", default="",
        help="Production apply requires APPLY_ADJUSTMENT_BACKFILL",
    )
    args = parser.parse_args()
    if args.apply and args.profile == "production" and (
        args.confirm != "APPLY_ADJUSTMENT_BACKFILL"
    ):
        parser.error(
            "production apply requires --confirm APPLY_ADJUSTMENT_BACKFILL"
        )

    settings = Settings.from_env(args.profile)
    database = ClickHouseDatabase(
        settings.clickhouse_host,
        settings.clickhouse_port,
        settings.clickhouse_database,
        settings.clickhouse_username,
        settings.clickhouse_password,
        settings.clickhouse_secure,
    )
    database.open()
    try:
        repository = ClickHouseMarketBarRepository(database)
        result = AdjustmentBackfillService(repository).run(
            limit=args.limit, apply=args.apply
        )
        safe = {
            key: value for key, value in result.items()
            if key != "rows"
        }
        print(json.dumps(safe, ensure_ascii=False, default=str, sort_keys=True))
    finally:
        database.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

