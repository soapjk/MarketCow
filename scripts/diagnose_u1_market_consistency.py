"""Finite read-only comparison of one market's initial WS books and REST books."""

import asyncio
from decimal import Decimal
import json
import os
from pathlib import Path
import socket
import ssl
import sys
import time

import httpx
import websockets


RUNTIME = Path("/mnt/p44pro/marketcow-shadow-v3-runtime/linux")
HOST = "ws-subscriptions-clob.polymarket.com"


def proxy_tunnel() -> socket.socket:
    sock = socket.create_connection(("127.0.0.1", 17890), timeout=5)
    try:
        sock.sendall(f"CONNECT {HOST}:443 HTTP/1.1\r\nHost: {HOST}:443\r\n\r\n".encode())
        header = b""
        while not header.endswith(b"\r\n\r\n"):
            part = sock.recv(1)
            if not part or len(header) >= 8192:
                raise RuntimeError("invalid CONNECT response")
            header += part
        if header.split(b"\r\n")[0].split()[1] != b"200":
            raise RuntimeError("CONNECT rejected")
        sock.setblocking(False)
        return sock
    except BaseException:
        sock.close()
        raise


def levels(book: dict, side: str) -> list[tuple[str, str]]:
    return sorted(
        (str(Decimal(row["price"])), str(Decimal(row["size"])))
        for row in book.get(side, [])
    )


async def main() -> None:
    os.umask(0o077)
    market_id = sys.argv[1]
    plan = json.loads(
        (RUNTIME / "dynamic-live-candidate-r1/rust-scoped-plan-r1.json").read_text()
    )
    market = next(row for row in plan["markets"] if row["market_id"] == market_id)
    proxy = await asyncio.create_subprocess_exec(
        str(RUNTIME / "polymarket-proxy/mihomo"),
        "-d",
        str(RUNTIME / "polymarket-proxy"),
        "-f",
        str(RUNTIME / "polymarket-proxy/config.yaml"),
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL,
    )
    try:
        await asyncio.sleep(0.5)
        sock = await asyncio.to_thread(proxy_tunnel)
        ws_books: dict[str, dict] = {}
        async with websockets.connect(
            f"wss://{HOST}/ws/market",
            sock=sock,
            ssl=ssl.create_default_context(),
            server_hostname=HOST,
            open_timeout=10,
            max_size=2_097_152,
        ) as ws:
            await ws.send(json.dumps({"assets_ids": market["token_ids"], "type": "market"}))
            deadline = time.monotonic() + 10
            while set(ws_books) != set(market["token_ids"]):
                raw = await asyncio.wait_for(ws.recv(), deadline - time.monotonic())
                if raw in ("PING", "PONG"):
                    continue
                decoded = json.loads(raw)
                for item in decoded if isinstance(decoded, list) else [decoded]:
                    if item.get("event_type") == "book":
                        ws_books[item["asset_id"]] = item
        async with httpx.AsyncClient(
            proxies="http://127.0.0.1:17890", trust_env=False, timeout=10
        ) as client:
            response = await client.post(
                "https://clob.polymarket.com/books",
                json=[{"token_id": token} for token in market["token_ids"]],
            )
            response.raise_for_status()
            rest_books = {row["asset_id"]: row for row in response.json()}
        result = {"market_id": market_id, "tokens": []}
        for token in market["token_ids"]:
            ws_book, rest_book = ws_books[token], rest_books[token]
            row = {
                "token_id": token,
                "ws_timestamp": ws_book.get("timestamp"),
                "rest_timestamp": rest_book.get("timestamp"),
                "tick_equal": ws_book.get("tick_size") == rest_book.get("tick_size"),
                "last_trade": {
                    "ws": ws_book.get("last_trade_price"),
                    "rest": rest_book.get("last_trade_price"),
                    "decimal_equal": (
                        Decimal(ws_book["last_trade_price"] or "0")
                        == Decimal(rest_book["last_trade_price"] or "0")
                    ),
                },
                "bids_equal": levels(ws_book, "bids") == levels(rest_book, "bids"),
                "asks_equal": levels(ws_book, "asks") == levels(rest_book, "asks"),
            }
            for side in ("bids", "asks"):
                ws_levels, rest_levels = set(levels(ws_book, side)), set(levels(rest_book, side))
                row[f"{side}_ws_only"] = sorted(ws_levels - rest_levels)[:5]
                row[f"{side}_rest_only"] = sorted(rest_levels - ws_levels)[:5]
            result["tokens"].append(row)
        print(json.dumps(result, ensure_ascii=False, indent=2))
    finally:
        if proxy.returncode is None:
            proxy.terminate()
        try:
            await asyncio.wait_for(proxy.wait(), 3)
        except asyncio.TimeoutError:
            proxy.kill()
            await proxy.wait()


if __name__ == "__main__":
    asyncio.run(main())
