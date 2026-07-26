from __future__ import annotations

import csv
import random
import re
from datetime import datetime, time, timezone
from pathlib import Path
from typing import Any, Mapping
from zoneinfo import ZoneInfo


INFERENCE_SCHEMA = "marketcow.csv-import-inference.v1"
MIC_TIMEZONES = {
    "XNAS": "America/New_York",
    "XNYS": "America/New_York",
    "ARCX": "America/New_York",
    "XHKG": "Asia/Hong_Kong",
    "XSHG": "Asia/Shanghai",
    "XSHE": "Asia/Shanghai",
    "XBSE": "Asia/Shanghai",
}
MIC_SESSIONS = {
    "XNAS": (time(4), time(20)),
    "XNYS": (time(4), time(20)),
    "ARCX": (time(4), time(20)),
    "XHKG": (time(9), time(16, 30)),
    "XSHG": (time(9, 15), time(15, 30)),
    "XSHE": (time(9, 15), time(15, 30)),
    "XBSE": (time(9, 15), time(15, 30)),
}


def _timestamp(value: str, timestamp_format: str) -> datetime:
    text = value.strip()
    if timestamp_format == "iso8601":
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    return datetime.strptime(text, timestamp_format)


def _normalized(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", value.strip().lower()).strip("_")


def _reservoir_add(
    values: list[datetime], value: datetime, seen: int, limit: int,
    generator: random.Random,
) -> None:
    if len(values) < limit:
        values.append(value)
        return
    index = generator.randrange(seen)
    if index < limit:
        values[index] = value


def _session_score(
    timestamps: list[datetime], source_zone: str, market_zone: str,
    session: tuple[time, time],
) -> float:
    source = ZoneInfo(source_zone)
    market = ZoneInfo(market_zone)
    start, end = session
    matched = 0
    for observed in timestamps:
        aware = (
            observed.astimezone(timezone.utc)
            if observed.tzinfo is not None
            else observed.replace(tzinfo=source).astimezone(timezone.utc)
        )
        local = aware.astimezone(market)
        if local.weekday() < 5 and start <= local.time().replace(tzinfo=None) < end:
            matched += 1
    return matched / len(timestamps) if timestamps else 0.0


def infer_csv_semantics(
    path: Path,
    *,
    mic: str,
    columns: Mapping[str, str],
    timestamp_format: str = "iso8601",
    encoding: str = "utf-8-sig",
    delimiter: str = ",",
    sample_limit: int = 20_000,
) -> dict[str, Any]:
    """Infer semantic hints without silently converting them into facts."""
    market_zone = MIC_TIMEZONES.get(mic)
    session = MIC_SESSIONS.get(mic)
    timestamp_column = columns.get("timestamp", "")
    close_column = columns.get("close", "")
    timestamps: list[datetime] = []
    parse_failures = 0
    seen = 0
    generator = random.Random(0)
    header: list[str] = []
    with path.open("r", encoding=encoding, errors="replace", newline="") as stream:
        reader = csv.DictReader(stream, delimiter=delimiter)
        header = list(reader.fieldnames or ())
        for row in reader:
            raw = str(row.get(timestamp_column) or "")
            try:
                observed = _timestamp(raw, timestamp_format)
            except (TypeError, ValueError):
                parse_failures += 1
                continue
            seen += 1
            _reservoir_add(
                timestamps, observed, seen, sample_limit, generator
            )

    timezone_result = _infer_timezone(
        timestamps, market_zone, session, parse_failures
    )
    adjustment_result = _infer_adjustment(header, close_column)
    return {
        "schema": INFERENCE_SCHEMA,
        "sample": {
            "rows_seen": seen + parse_failures,
            "timestamps_sampled": len(timestamps),
            "timestamp_parse_failures": parse_failures,
        },
        "timezone": timezone_result,
        "adjustment": adjustment_result,
    }


def _infer_timezone(
    timestamps: list[datetime],
    market_zone: str | None,
    session: tuple[time, time] | None,
    parse_failures: int,
) -> dict[str, Any]:
    if not timestamps:
        return {
            "value": None, "confidence": "none", "score": 0.0,
            "evidence": ["No parseable timestamps were available."],
            "alternatives": [],
        }
    if all(value.tzinfo is not None for value in timestamps):
        if all(value.utcoffset() == timezone.utc.utcoffset(value) for value in timestamps):
            return {
                "value": "UTC", "confidence": "high", "score": 1.0,
                "evidence": ["Every sampled timestamp carries an explicit UTC offset."],
                "alternatives": [],
            }
        if market_zone and session:
            score = _session_score(
                timestamps, market_zone, market_zone, session
            )
            return {
                "value": market_zone,
                "confidence": "high" if score >= 0.9 else "medium",
                "score": round(score, 4),
                "evidence": [
                    "Timestamps include explicit offsets.",
                    f"{score:.1%} align with the {market_zone} market session.",
                ],
                "alternatives": [],
            }
    if not market_zone or not session:
        return {
            "value": None, "confidence": "none", "score": 0.0,
            "evidence": ["The selected MIC has no configured timezone model."],
            "alternatives": [],
        }
    candidates = list(dict.fromkeys([
        market_zone, "UTC", "Asia/Shanghai", "Asia/Hong_Kong",
    ]))
    scored = sorted(
        (
            {
                "value": candidate,
                "score": round(_session_score(
                    timestamps, candidate, market_zone, session
                ), 4),
            }
            for candidate in candidates
        ),
        key=lambda item: (-item["score"], item["value"]),
    )
    best = scored[0]
    runner_up = scored[1]["score"] if len(scored) > 1 else 0.0
    margin = best["score"] - runner_up
    if best["score"] >= 0.9 and margin >= 0.15:
        confidence = "high"
    elif best["score"] >= 0.75 and margin >= 0.08:
        confidence = "medium"
    else:
        confidence = "low"
    evidence = [
        f"{best['score']:.1%} of sampled timestamps align with the "
        f"{market_zone} session when interpreted as {best['value']}.",
        f"The next-best timezone score is {runner_up:.1%}.",
    ]
    if parse_failures:
        evidence.append(
            f"{parse_failures} timestamp rows could not be parsed."
        )
    return {
        "value": best["value"],
        "confidence": confidence,
        "score": best["score"],
        "evidence": evidence,
        "alternatives": scored[1:],
    }


def _infer_adjustment(
    header: list[str], selected_close_column: str
) -> dict[str, Any]:
    selected = _normalized(selected_close_column)
    normalized_header = {_normalized(value): value for value in header}
    if "adj" in selected.split("_") or "adjusted" in selected.split("_"):
        return {
            "value": None, "confidence": "none", "score": 0.0,
            "evidence": [
                f"The selected close column {selected_close_column!r} "
                "is explicitly named as adjusted, but the column name cannot "
                "distinguish qfq from hfq.",
                "Select qfq or hfq from the vendor's documented methodology.",
            ],
        }
    adjusted_columns = [
        original for normalized, original in normalized_header.items()
        if (
            ("adj" in normalized.split("_") or "adjusted" in normalized.split("_"))
            and "close" in normalized.split("_")
        )
    ]
    if adjusted_columns and selected_close_column not in adjusted_columns:
        return {
            "value": "raw", "confidence": "high", "score": 0.95,
            "evidence": [
                f"The file contains separate adjusted column(s): "
                f"{', '.join(adjusted_columns)}.",
                f"The selected close column is {selected_close_column!r}.",
            ],
        }
    return {
        "value": "raw", "confidence": "low", "score": 0.35,
        "evidence": [
            "No explicit adjusted-price column or adjustment metadata was found.",
            "Price values alone cannot prove adjustment status without a "
            "corporate-action or reference-price comparison.",
        ],
    }
