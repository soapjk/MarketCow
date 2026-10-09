"""Capture five minutes of raw Polymarket market-channel messages in memory."""

import argparse
import asyncio
from dataclasses import asdict, dataclass
import json
import os
from pathlib import Path
import socket
import ssl
import time

import websockets


HOST = "ws-subscriptions-clob.polymarket.com"
DURATION_SECONDS = 300


@dataclass(slots=True)
class ReceivedMessage:
    raw_data: str
    received_at_ns: int


def tunnel(proxy_host: str, proxy_port: int) -> socket.socket:
    sock = socket.create_connection((proxy_host, proxy_port), timeout=5)
    try:
        sock.sendall(f"CONNECT {HOST}:443 HTTP/1.1\r\nHost: {HOST}:443\r\n\r\n".encode())
        header = b""
        while not header.endswith(b"\r\n\r\n"):
            piece = sock.recv(1)
            if not piece or len(header) >= 8192:
                raise RuntimeError("invalid CONNECT response")
            header += piece
        if header.split(b"\r\n")[0].split()[1] != b"200":
            raise RuntimeError("CONNECT rejected")
        sock.setblocking(False)
        return sock
    except BaseException:
        sock.close()
        raise


async def capture_messages(ws, messages: list[ReceivedMessage], duration_seconds: float = DURATION_SECONDS) -> None:
    deadline = time.monotonic() + duration_seconds
    try:
        async with asyncio.timeout_at(deadline):
            while True:
                raw = await ws.recv()
                received_at_ns = time.time_ns()
                # Market messages are text. If a binary message arrives, Latin-1
                # maps each byte to one code point without losing its contents.
                raw_data = raw if isinstance(raw, str) else raw.decode("latin-1")
                messages.append(ReceivedMessage(raw_data, received_at_ns))
    except asyncio.TimeoutError:
        pass


async def heartbeat(ws) -> None:
    while True:
        await asyncio.sleep(10)
        await ws.send("PING")


async def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--asset-id", action="append", required=True, dest="asset_ids")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--proxy-host", default="127.0.0.1")
    parser.add_argument("--proxy-port", type=int, default=17890)
    parser.add_argument("--duration-seconds", type=float, default=DURATION_SECONDS)
    args = parser.parse_args()

    os.umask(0o077)
    output: Path = args.output
    if output.exists():
        parser.error(f"output already exists: {output}")
    if not 0 < args.duration_seconds <= DURATION_SECONDS:
        parser.error("duration must be between 0 and 300 seconds")

    messages: list[ReceivedMessage] = []
    started_at_ns = time.time_ns()
    error: BaseException | None = None
    try:
        sock = await asyncio.to_thread(tunnel, args.proxy_host, args.proxy_port)
        async with websockets.connect(
            f"wss://{HOST}/ws/market",
            sock=sock,
            ssl=ssl.create_default_context(),
            server_hostname=HOST,
            open_timeout=10,
            max_size=None,
            ping_interval=None,
        ) as ws:
            await ws.send(json.dumps({"assets_ids": args.asset_ids, "type": "market"}))
            ping = asyncio.create_task(heartbeat(ws))
            try:
                await capture_messages(ws, messages, args.duration_seconds)
            finally:
                ping.cancel()
                await asyncio.gather(ping, return_exceptions=True)
    except BaseException as exc:
        error = exc

    ended_at_ns = time.time_ns()
    # No JSON parsing or disk writes occur in the receive loop. Every output
    # row has exactly the two fields captured for that WebSocket message.
    with output.open("x", encoding="utf-8") as out:
        for message in messages:
            out.write(json.dumps(asdict(message), ensure_ascii=False, separators=(",", ":")))
            out.write("\n")

    print(json.dumps({
        "output": str(output),
        "messages": len(messages),
        "started_at_ns": started_at_ns,
        "ended_at_ns": ended_at_ns,
        "error": None if error is None else f"{type(error).__name__}: {error}",
    }))
    if error is not None:
        raise error


if __name__ == "__main__":
    asyncio.run(main())
