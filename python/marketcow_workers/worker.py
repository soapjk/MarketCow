from __future__ import annotations

import argparse
import asyncio
import json
import os
import secrets
import struct
from pathlib import Path

PROTOCOL_VERSION = "marketcow.worker.v1"
MAX_FRAME_BYTES = 1_048_576


async def handshake(socket_path: Path, revision: str) -> dict[str, object]:
    if not socket_path.is_absolute():
        raise ValueError("worker socket path must be absolute")
    reader, writer = await asyncio.open_unix_connection(socket_path)
    nonce = secrets.token_hex(16)
    payload = json.dumps(
        {
            "protocol_version": PROTOCOL_VERSION,
            "message_id": secrets.token_hex(16),
            "message_type": "hello",
            "worker_id": f"python-{os.getpid()}",
            "worker_revision": revision,
            "nonce": nonce,
            "capabilities": [],
        },
        separators=(",", ":"),
    ).encode()
    if len(payload) > MAX_FRAME_BYTES:
        raise ValueError("worker frame too large")
    writer.write(struct.pack(">I", len(payload)) + payload)
    await writer.drain()
    length = struct.unpack(">I", await reader.readexactly(4))[0]
    if not 0 < length <= MAX_FRAME_BYTES:
        raise ValueError("invalid server frame")
    response = json.loads(await reader.readexactly(length))
    writer.close()
    await writer.wait_closed()
    if response.get("protocol_version") != PROTOCOL_VERSION:
        raise ValueError("worker protocol version mismatch")
    if response.get("message_type") != "hello_ack" or response.get("nonce") != nonce:
        raise ValueError("worker handshake authentication failed")
    if response.get("maximum_frame_bytes") != MAX_FRAME_BYTES:
        raise ValueError("worker frame limit mismatch")
    return response


def safe_staging_path(root: Path, task_id: str, filename: str) -> Path:
    if not root.is_absolute() or not task_id.replace("-", "").isalnum():
        raise ValueError("invalid staging lease")
    target = (root / task_id / filename).resolve()
    lease = (root / task_id).resolve()
    if target.parent != lease or target.name != filename:
        raise ValueError("staging path escape rejected")
    return target


async def _main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--socket", type=Path, required=True)
    parser.add_argument("--revision", default=os.environ.get("MARKETCOW_WORKER_REVISION", "development"))
    args = parser.parse_args()
    print(json.dumps(await handshake(args.socket, args.revision), sort_keys=True))


if __name__ == "__main__":
    asyncio.run(_main())
