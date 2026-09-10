"""Bounded current/next-hour discovery via MarketCow's Rust source reader.

Slug convention is a lookup hypothesis, not proof of a contract's time/rules.
Full source evidence is retained for bind_hour's separate rule review.
"""
import asyncio
import base64
import hashlib
from datetime import timedelta, timezone
from zoneinfo import ZoneInfo

import httpx

from .universe_live_probe import strict_json
from .btc_polymarket_binding import bind_hour, review_binance_hour


MONTHS = "january february march april may june july august september october november december".split()


def hour_queries(now):
    if now.tzinfo is None:
        raise ValueError("aware_clock_required")
    start = now.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0)
    result = []
    for index in range(3):
        hour = start + timedelta(hours=index)
        local = hour.astimezone(ZoneInfo("America/New_York"))
        slug = (f"bitcoin-up-or-down-{MONTHS[local.month-1]}-{local.day}-{local.year}-"
                f"{local.hour % 12 or 12}{'am' if local.hour < 12 else 'pm'}-et")
        result.append(dict(slug=slug, proposed_start_utc=hour.isoformat(),
                           proposed_end_utc=(hour+timedelta(hours=1)).isoformat(),
                           hour_binding_verified=False))
    if len({r["slug"] for r in result}) != len(result):
        raise ValueError("ambiguous_dst_slug_requires_explicit_identity")
    return result


async def discover(endpoint, now, publish, *, maximum_bytes, seconds, transport=None):
    from urllib.parse import urlsplit
    url = urlsplit(endpoint)
    if url.scheme not in ("http", "https") or not url.hostname or url.username or url.password or url.query or url.fragment:
        raise ValueError("invalid_endpoint")
    if type(maximum_bytes) is not int or not 0 < maximum_bytes <= 2*1024*1024 or not 0 < seconds <= 60:
        raise ValueError("invalid_discovery_budget")
    queries = hour_queries(now)
    results, used = [], 0
    async with asyncio.timeout(seconds):
        async with httpx.AsyncClient(trust_env=False, follow_redirects=False, timeout=min(seconds, 15), transport=transport) as client:
            for index, query in enumerate(queries):
                if index:
                    await asyncio.sleep(1)
                async with client.stream("GET", endpoint.rstrip("/")+"/v1/prediction-markets/polymarket/research/btc-hour-evidence",
                                         params={"slug": query["slug"]}) as response:
                    response.raise_for_status()
                    body = bytearray()
                    async for chunk in response.aiter_bytes(chunk_size=16384):
                        used += len(chunk)
                        if used > maximum_bytes or len(body)+len(chunk) > 524288:
                            raise ValueError("discovery_response_capacity")
                        body.extend(chunk)
                value = strict_json(body)
                evidence = base64.b64decode(value["raw_base64"], validate=True)
                if (value.get("raw_complete") is not True or len(evidence) != value["raw_bytes"]
                        or hashlib.sha256(evidence).hexdigest() != value["raw_sha256"]):
                    raise ValueError("discovery_evidence_mismatch")
                # Archive rejections as evidence before stopping; a 404 does not
                # establish absence across the whole hourly market universe.
                publish(dict(schema_version="marketcow.btc-hour.discovery.v1", query=query,
                             response=value, response_sha256=hashlib.sha256(body).hexdigest(),
                             execution_eligible=False))
                if value.get("schema_version") != "marketcow.polymarket.market-evidence.v1":
                    raise ValueError("discovery_source_rejected")
                raw = strict_json(evidence)
                if raw.get("slug") != query["slug"] or raw.get("id") != value.get("market_id") or raw.get("conditionId") != value.get("condition_id"):
                    raise ValueError("discovery_identity_mismatch")
                if any(v["evidence"]["market_id"] == value["market_id"]
                       or v["evidence"]["condition_id"] == value["condition_id"] for v in results):
                    raise ValueError("duplicate_hour_identity")
                review = review_binance_hour(value, query["proposed_start_utc"])
                binding = bind_hour(value, review)
                publish(dict(schema_version="marketcow.btc-hour.reviewed-discovery.v1",
                             market_id=binding["market_id"], review_sha256=binding["review_sha256"],
                             raw_sha256=binding["raw_sha256"], start_utc=binding["start_utc"],
                             end_utc=binding["end_utc"], execution_eligible=False,
                             activation_performed=False))
                results.append(dict(query=query, evidence=value, review=review, binding=binding))
    return results


async def monitor_discovery(endpoint, publish, *, maximum_bytes, seconds,
                            interval_seconds, maximum_consecutive_failures,
                            stop, discover_function=discover, registry=None):
    """One round in flight; bounded failure streak; cancellation never starts a new GET."""
    from datetime import datetime
    if (type(interval_seconds) is not int or interval_seconds < 60
            or type(maximum_consecutive_failures) is not int
            or not 1 <= maximum_consecutive_failures <= 10):
        raise ValueError("explicit_monitor_budget_required")
    failures = 0
    loop = asyncio.get_running_loop()
    while not stop.is_set():
        started = loop.time()
        try:
            values = await discover_function(endpoint, datetime.now(timezone.utc), publish,
                                             maximum_bytes=maximum_bytes, seconds=seconds)
            if registry is not None:
                # A complete bounded source round is registered on the control
                # path, never from a realtime callback.
                await asyncio.to_thread(registry.register, [row["binding"] for row in values])
            failures = 0
            publish(dict(event_type="hour_discovery_round_complete", count=len(values),
                         rule_review_required=True, activation_performed=False))
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            failures += 1
            publish(dict(event_type="hour_discovery_round_failed", consecutive_failures=failures,
                         error_type=type(exc).__name__, activation_performed=False))
            if failures >= maximum_consecutive_failures:
                raise RuntimeError("hour_discovery_failure_budget") from exc
        try:
            await asyncio.wait_for(stop.wait(), max(0, interval_seconds-(loop.time()-started)))
        except TimeoutError:
            pass


def main():
    import argparse
    from datetime import datetime
    from pathlib import Path
    from .btc_fact_log import FactLog
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--endpoint", required=True)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--maximum-response-bytes", required=True, type=int)
    parser.add_argument("--maximum-disk-bytes", required=True, type=int)
    parser.add_argument("--seconds", required=True, type=int)
    parser.add_argument("--continuous", action="store_true")
    parser.add_argument("--interval-seconds", type=int)
    parser.add_argument("--maximum-consecutive-failures", type=int)
    parser.add_argument("--lifecycle-db", type=Path)
    parser.add_argument("--maximum-lifecycle-markets", type=int)
    parser.add_argument("--maximum-lifecycle-payload-bytes", type=int)
    args = parser.parse_args()
    if args.continuous and (args.interval_seconds is None or args.interval_seconds < 60
                           or args.maximum_consecutive_failures is None
                           or not 1 <= args.maximum_consecutive_failures <= 10):
        parser.error("continuous discovery requires explicit interval >=60 and failure budget 1..10")
    lifecycle = (args.lifecycle_db, args.maximum_lifecycle_markets,
                 args.maximum_lifecycle_payload_bytes)
    if any(item is not None for item in lifecycle) and any(item is None for item in lifecycle):
        parser.error("lifecycle registry requires path, market and payload budgets")
    registry = None
    if args.lifecycle_db is not None:
        from .btc_lifecycle import HourRegistry
        registry = HourRegistry(args.lifecycle_db,
                                maximum_markets=args.maximum_lifecycle_markets,
                                maximum_bytes=args.maximum_lifecycle_payload_bytes)
    # Three responses only. No hidden recurring job, subscription or activation.
    log = FactLog(args.output, maximum_pending=3,
                  maximum_pending_bytes=args.maximum_response_bytes * 2,
                  maximum_disk_bytes=args.maximum_disk_bytes, maximum_subscribers=1)
    try:
        async def run():
            if not args.continuous:
                values = await discover(args.endpoint, datetime.now(timezone.utc), log.publish,
                                        maximum_bytes=args.maximum_response_bytes, seconds=args.seconds)
                if registry is not None:
                    await asyncio.to_thread(registry.register, [row["binding"] for row in values])
                return values
            import signal
            stop = asyncio.Event()
            loop = asyncio.get_running_loop()
            installed = []
            try:
                for signum in (signal.SIGTERM, signal.SIGINT):
                    loop.add_signal_handler(signum, stop.set)
                    installed.append(signum)
                return await monitor_discovery(args.endpoint, log.publish,
                    maximum_bytes=args.maximum_response_bytes, seconds=args.seconds,
                    interval_seconds=args.interval_seconds,
                    maximum_consecutive_failures=args.maximum_consecutive_failures, stop=stop,
                    registry=registry)
            finally:
                for signum in installed:
                    loop.remove_signal_handler(signum)
        asyncio.run(run())
    finally:
        log.close(10)


if __name__ == "__main__":
    main()
