"""Bounded read-only live/history overlap verification against MarketCow."""
import asyncio
import base64
import hashlib
import json
import time
import sys
import urllib.request
import urllib.parse
from datetime import datetime
from decimal import Decimal
from pathlib import Path

import websockets

BASE = "http://192.168.124.3:8793"
OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def get(path):
    with OPENER.open(BASE + path, timeout=35) as response:
        raw = response.read(4194305)
        assert len(raw) <= 4194304, "page budget"
        return json.loads(raw), len(raw), hashlib.sha256(raw).hexdigest()


def identity(row):
    return (row["trade_id"], row["trade_time_ms"], Decimal(row["price"]),
            Decimal(row["quantity"]), row["buyer_is_maker"])


async def main():
    started = time.monotonic()
    output = Path(sys.argv[1]) if len(sys.argv) > 1 else None
    if output:
        output.mkdir(parents=True, exist_ok=False)
    status, _, _ = await asyncio.to_thread(get, "/v1/prediction-markets/polymarket/research/btc-5m/status")
    market = next(m for m in status["profile"]["markets"] if m["role"] == "current")
    start = int(datetime.fromisoformat(market["start_at"].replace("Z", "+00:00")).timestamp()*1000)-660000
    async with websockets.connect(BASE.replace("http:", "ws:") + "/v1/btc-research/price-stream?source=binance_spot_trade",
                                  subprotocols=["marketcow.btc-price.v2"], max_size=4194304) as ws:
        snapshot = json.loads(await asyncio.wait_for(ws.recv(), 15))
        if output:
            (output / "live-snapshot.json").write_text(json.dumps(snapshot))
        assert snapshot["type"] == "snapshot" and snapshot["trades"], "live snapshot missing"
        anchor = snapshot["trades"][-1]["trade_id"]
        live_count = 0
        async def drain():
            nonlocal live_count
            async for _ in ws:
                live_count += 1
        reader = asyncio.create_task(drain())
        try:
            query = dict(source="binance_spot_trade", start_time_ms=start, through_trade_id=anchor, limit=1000)
            rows, hashes, sizes = [], [], []
            predecessor, session = None, None
            for index in range(64):
                page, size, sha = await asyncio.to_thread(get, "/v1/btc-research/price-history?" + urllib.parse.urlencode(query))
                if output:
                    (output / f"page-{index}.json").write_text(json.dumps(page))
                assert page["schema_version"] == "marketcow.binance-trade-history.v1"
                assert page["page_index"] == index and page["through_trade_id"] == anchor and page["start_time_ms"] == start
                if session is None:
                    session = page["session_id"]
                    predecessor = page["predecessor"]
                assert page["session_id"] == session
                assert index == 0 or page["predecessor"] is None
                evidence = {}
                for item in page["evidence"]:
                    raw = base64.b64decode(item["raw_base64"], validate=True)
                    assert len(raw) == item["raw_bytes"]
                    assert hashlib.sha256(raw).hexdigest() == item["raw_sha256"] == item["evidence_ref"]
                    evidence[item["evidence_ref"]] = json.loads(raw)
                for row in page["trades"] + ([predecessor] if index == 0 and predecessor else []):
                    raw = evidence[row["evidence_ref"]]
                    raw = raw[row["source_row_index"]] if isinstance(raw, list) else raw
                    if "t" in raw:
                        original = (raw["t"], raw["T"], Decimal(raw["p"]), Decimal(raw["q"]), raw["m"])
                        assert row["event_time_ms"] == raw["E"]
                    else:
                        original = (raw["id"], raw["time"], Decimal(raw["price"]), Decimal(raw["qty"]), raw["isBuyerMaker"])
                        assert row["event_time_ms"] is None
                    assert identity(row) == original, "supplier row mismatch"
                rows.extend(page["trades"])
                sizes.append(size)
                hashes.append(sha)
                assert sum(sizes) <= 67108864
                if page["next_page_token"] is None:
                    break
                assert not page["complete"]
                query["page_token"] = page["next_page_token"]
            else:
                raise AssertionError("page count exceeded")
            by_id = {r["trade_id"]: r for r in rows}
            overlap = [r for r in snapshot["trades"] if r["trade_id"] in by_id]
            assert overlap and all(identity(r) == identity(by_id[r["trade_id"]]) for r in overlap)
            if page["complete"]:
                assert predecessor and rows[-1]["trade_id"] == anchor
                assert len(rows) == page["total_trade_count"]
                chain = [predecessor] + rows
                assert all(b["trade_id"] == a["trade_id"]+1 and b["trade_time_ms"] >= a["trade_time_ms"] for a,b in zip(chain, chain[1:]))
            report = dict(market_id=market["market_id"], start_time_ms=start, through_trade_id=anchor,
                                  pages=len(sizes), trades=len(rows), predecessor=predecessor, complete=page["complete"],
                                  code=page["code"], missing_ranges=page["missing_ranges"], overlap=len(overlap),
                                  live_frames_received=live_count, elapsed_seconds=time.monotonic()-started,
                                  page_bytes=sizes, page_sha256=hashes, raw_verification="passed",
                                  saved_page_format="parsed JSON; page_sha256 identifies original HTTP bytes")
            if output:
                (output / "report.json").write_text(json.dumps(report))
            print(json.dumps(report))
        finally:
            reader.cancel()
            await asyncio.gather(reader, return_exceptions=True)


if __name__ == "__main__":
    asyncio.run(main())
