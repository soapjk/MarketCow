from __future__ import annotations

import unittest
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from marketcow.history_shards import (
    freeze_history_range,
    history_ingestion_identity,
    plan_history_shards,
)


class HistoryShardPlanningTest(unittest.TestCase):
    def test_relative_range_is_frozen_and_plan_is_deterministic(self):
        request = {
            "provider": "yahoo", "range": "3mo", "interval": "1m",
            "adjustment": "raw",
        }
        observed = datetime(2026, 7, 25, 12, 30, tzinfo=timezone.utc)
        frozen = freeze_history_range(request, observed)

        first = plan_history_shards(frozen)
        second = plan_history_shards(dict(frozen))

        self.assertEqual(frozen["range_end"], "2026-07-25T12:30:00+00:00")
        self.assertEqual(frozen["range_start"], "2026-04-23T12:30:00+00:00")
        self.assertEqual(first, second)
        self.assertEqual(len(first), 14)
        self.assertEqual(first[0]["range_start"], frozen["range_start"])
        self.assertEqual(first[-1]["range_end"], frozen["range_end"])
        for left, right in zip(first, first[1:]):
            self.assertEqual(left["range_end"], right["range_start"])

    def test_ytd_starts_at_utc_year_boundary(self):
        frozen = freeze_history_range(
            {
                "provider": "yahoo", "range": "ytd", "interval": "1d",
                "adjustment": "adjusted",
            },
            datetime(2026, 7, 25, 12, 30, tzinfo=timezone.utc),
        )
        self.assertEqual(frozen["range_start"], "2026-01-01T00:00:00+00:00")

    def test_provider_interval_limits_choose_stable_shard_sizes(self):
        base = {
            "range": "1y", "adjustment": "raw",
            "range_start": "2025-01-01T00:00:00+00:00",
            "range_end": "2026-01-01T00:00:00+00:00",
        }
        yahoo = plan_history_shards({
            **base, "provider": "yahoo", "interval": "5m",
        })
        tushare = plan_history_shards({
            **base, "provider": "tushare", "interval": "5m",
        })
        hyperliquid = plan_history_shards({
            **base, "provider": "hyperliquid", "interval": "1h",
        })

        self.assertEqual(len(yahoo), 7)
        self.assertEqual(len(tushare), 12)
        self.assertEqual(len(hyperliquid), 3)

    def test_naive_or_reversed_boundaries_are_rejected(self):
        with self.assertRaisesRegex(ValueError, "timezone-aware"):
            freeze_history_range(
                {
                    "provider": "yahoo", "range": "1d", "interval": "1d",
                    "adjustment": "raw", "range_start": "2026-01-01T00:00:00",
                    "range_end": "2026-01-02T00:00:00",
                },
                datetime.now(timezone.utc),
            )
        with self.assertRaisesRegex(ValueError, "must precede"):
            freeze_history_range(
                {
                    "provider": "yahoo", "range": "1d", "interval": "1d",
                    "adjustment": "raw",
                    "range_start": "2026-01-02T00:00:00+00:00",
                    "range_end": "2026-01-01T00:00:00+00:00",
                },
                datetime.now(timezone.utc),
            )

    def test_dst_observation_is_normalized_to_utc_before_planning(self):
        frozen = freeze_history_range(
            {
                "provider": "yahoo", "range": "1d", "interval": "1m",
                "adjustment": "raw",
            },
            datetime(
                2026, 11, 1, 1, 30,
                tzinfo=ZoneInfo("America/New_York"), fold=1,
            ),
        )
        shards = plan_history_shards(frozen)

        self.assertEqual(frozen["range_end"], "2026-11-01T06:30:00+00:00")
        self.assertEqual(shards[0]["range_start"], frozen["range_start"])
        self.assertEqual(shards[-1]["range_end"], frozen["range_end"])
        boundaries = [
            value
            for shard in shards
            for value in (shard["range_start"], shard["range_end"])
        ]
        self.assertEqual(len(set(boundaries)), len(shards) + 1)

    def test_ingestion_identity_is_stable_across_jobs(self):
        request = freeze_history_range(
            {
                "provider": "yahoo", "range": "5d", "interval": "1d",
                "adjustment": "raw",
            },
            datetime(2026, 7, 25, tzinfo=timezone.utc),
        )
        shard = plan_history_shards(request)[0]

        first = history_ingestion_identity("aapl", request, shard)
        second = history_ingestion_identity(" AAPL ", dict(request), dict(shard))

        self.assertEqual(first, second)
        self.assertEqual(len(first), 64)


if __name__ == "__main__":
    unittest.main()
