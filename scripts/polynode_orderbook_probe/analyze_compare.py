#!/usr/bin/env python3
"""Offline summary of compare_rest JSONL; never contacts an API."""
import collections
import bisect
import json
import statistics
import sys
from decimal import Decimal


def dec(value):
    return Decimal(str(value))


def levels(book, side):
    return {dec(row["price"]): dec(row["size"]) for row in book.get(side, [])}


def bbo(book):
    bids = levels(book, "bids")
    asks = levels(book, "asks")
    bid = max(bids) if bids else None
    ask = min(asks) if asks else None
    return (bid, bids.get(bid), ask, asks.get(ask))


def fingerprint(book):
    return (tuple(sorted(levels(book, "bids").items())),
            tuple(sorted(levels(book, "asks").items())))


def percentile(values, pct):
    if not values:
        return None
    ordered = sorted(values)
    return round(ordered[min(len(ordered) - 1, int((len(ordered) - 1) * pct))], 2)


def main(path):
    counts = collections.Counter()
    bucket_counts = collections.defaultdict(collections.Counter)
    temporal_counts = collections.defaultdict(collections.Counter)
    pn_states = []
    rest_states = collections.defaultdict(list)
    rest_latency = []
    ws_batch_age = []
    rest_minus_batch = []
    first_ns = None
    last_ns = None
    for line in open(path, encoding="utf-8"):
        row = json.loads(line)
        counts["rows"] += 1
        counts[f"outcome:{row['outcome']}"] += 1
        first_ns = row["ws_received_at_ns"] if first_ns is None else min(first_ns, row["ws_received_at_ns"])
        last_ns = row["ws_received_at_ns"] if last_ns is None else max(last_ns, row["ws_received_at_ns"])
        if row["rest_request_started_at_ns"] and row["rest_response_received_at_ns"]:
            rest_latency.append((row["rest_response_received_at_ns"] - row["rest_request_started_at_ns"]) / 1e6)
        if row["polynode_batch_ts_ms"]:
            ws_batch_age.append(row["ws_received_at_ns"] / 1e6 - row["polynode_batch_ts_ms"])
        counts["changed_levels"] += len(row["levels"])
        counts["changed_levels_match"] += sum(level["matches"] for level in row["levels"])
        if not row["rest_raw"]:
            continue
        try:
            official = {book["asset_id"]: book for book in json.loads(row["rest_raw"])}
            pn = json.loads(row["polynode_raw"])
        except (ValueError, TypeError, KeyError):
            counts["decode_error"] += 1
            continue
        for token, book in official.items():
            try:
                rest_states[(token, fingerprint(book))].append(int(book["timestamp"]))
            except (ValueError, TypeError, KeyError):
                pass
        if pn.get("type") == "snapshot_batch":
            updates = pn.get("snapshots", [])
        elif pn.get("type") == "batch":
            updates = pn.get("updates", [])
        elif pn.get("type") == "book_snapshot":
            updates = [pn]
        else:
            updates = []
        bucket = (row["ws_received_at_ns"] - first_ns) // 30_000_000_000
        for update in updates:
            if update.get("type") == "book_snapshot":
                token = update.get("asset_id")
                rest = official.get(token)
                if rest is None:
                    counts["snapshot_missing_rest"] += 1
                    continue
                counts["snapshots_compared"] += 1
                if row["polynode_batch_ts_ms"]:
                    pn_states.append((token, row["polynode_batch_ts_ms"], fingerprint(update)))
                bucket_counts[bucket]["snapshots"] += 1
                full_match = levels(update, "bids") == levels(rest, "bids") and levels(update, "asks") == levels(rest, "asks")
                pn_bbo = bbo(update)
                rest_bbo = bbo(rest)
                bbo_match = pn_bbo == rest_bbo
                bbo_price_match = (pn_bbo[0], pn_bbo[2]) == (rest_bbo[0], rest_bbo[2])
                counts["snapshot_full_match"] += full_match
                counts["snapshot_bbo_match"] += bbo_match
                counts["snapshot_bbo_price_match"] += bbo_price_match
                bucket_counts[bucket]["full_match"] += full_match
                bucket_counts[bucket]["bbo_match"] += bbo_match
                bucket_counts[bucket]["bbo_price_match"] += bbo_price_match
                try:
                    offset = int(rest["timestamp"]) - int(row["polynode_batch_ts_ms"])
                    rest_minus_batch.append(offset)
                    time_class = ("rest_older_100ms_plus" if offset < -100 else
                                  "within_100ms" if offset <= 100 else
                                  "rest_newer_100_to_500ms" if offset <= 500 else
                                  "rest_newer_500ms_plus")
                    temporal_counts[time_class]["snapshots"] += 1
                    temporal_counts[time_class]["full_match"] += full_match
                    temporal_counts[time_class]["bbo_price_match"] += bbo_price_match
                except (ValueError, TypeError, KeyError):
                    pass
            elif update.get("type") == "price_change":
                for asset in update.get("assets", []):
                    rest = official.get(asset.get("asset_id"))
                    if not rest:
                        continue
                    p_bid, _, p_ask, _ = bbo(rest)
                    try:
                        bid_match = dec(asset["best_bid"]) == p_bid
                        ask_match = dec(asset["best_ask"]) == p_ask
                    except (KeyError, ValueError):
                        continue
                    counts["price_change_bbo_compared"] += 1
                    counts["price_change_bbo_match"] += bid_match and ask_match
    for timestamps in rest_states.values():
        timestamps.sort()
    nearest_matching_ms = []
    for token, batch_ts, state in pn_states:
        timestamps = rest_states.get((token, state), [])
        position = bisect.bisect_left(timestamps, batch_ts)
        choices = timestamps[max(0, position - 1):position + 1]
        if choices:
            difference = min(abs(t - batch_ts) for t in choices)
            nearest_matching_ms.append(difference)
            counts["snapshot_exact_state_seen_in_rest_within_1s"] += difference <= 1000
            counts["snapshot_exact_state_seen_in_rest_within_5s"] += difference <= 5000
    print(json.dumps({
        "span_between_first_last_ws_seconds": round((last_ns - first_ns) / 1e9, 3) if first_ns else 0,
        "counts": dict(counts),
        "rest_latency_ms": {"p50": percentile(rest_latency, .5), "p95": percentile(rest_latency, .95), "max": percentile(rest_latency, 1)},
        "pn_batch_age_at_receive_ms": {"p50": percentile(ws_batch_age, .5), "p95": percentile(ws_batch_age, .95), "max": percentile(ws_batch_age, 1)},
        "rest_timestamp_minus_pn_batch_ms": {"p50": percentile(rest_minus_batch, .5), "p95": percentile(rest_minus_batch, .95), "min": percentile(rest_minus_batch, 0), "max": percentile(rest_minus_batch, 1)},
        "by_rest_timestamp_vs_pn_batch": {k: dict(v) for k, v in sorted(temporal_counts.items())},
        "cross_request_state_alignment": {"pn_batch_snapshots": len(pn_states), "any_exact_rest_state": len(nearest_matching_ms), "nearest_match_time_distance_ms_p50": percentile(nearest_matching_ms, .5)},
        "per_30s": {str(k): dict(v) for k, v in sorted(bucket_counts.items())},
    }, ensure_ascii=False))


if __name__ == "__main__":
    main(sys.argv[1])
