import asyncio
import json
import struct
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest

from python.marketcow_workers.worker import (
    MAX_FRAME_BYTES,
    PROTOCOL_VERSION,
    handshake,
    safe_staging_path,
)


def test_staging_path_is_contained(tmp_path: Path) -> None:
    path = safe_staging_path(tmp_path.resolve(), "task-123", "result.json")
    assert path == tmp_path.resolve() / "task-123" / "result.json"


@pytest.mark.parametrize("filename", ["../secret", "/etc/passwd", "nested/file"])
def test_staging_path_escape_is_rejected(tmp_path: Path, filename: str) -> None:
    with pytest.raises(ValueError):
        safe_staging_path(tmp_path.resolve(), "task-123", filename)


def test_python_worker_performs_versioned_nonce_bound_uds_handshake() -> None:
    async def scenario() -> None:
        temporary = TemporaryDirectory(prefix="mc-worker-", dir="/tmp")
        socket_path = Path(temporary.name) / "worker.sock"

        async def server(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            length = struct.unpack(">I", await reader.readexactly(4))[0]
            assert 0 < length <= MAX_FRAME_BYTES
            request = json.loads(await reader.readexactly(length))
            assert request["protocol_version"] == PROTOCOL_VERSION
            assert request["message_type"] == "hello"
            assert request["worker_id"].startswith("python-")
            assert request["worker_revision"] == "python-test-v1"
            response = json.dumps(
                {
                    "protocol_version": PROTOCOL_VERSION,
                    "message_id": request["message_id"],
                    "message_type": "hello_ack",
                    "daemon_revision": "rust-test-v1",
                    "nonce": request["nonce"],
                    "maximum_frame_bytes": MAX_FRAME_BYTES,
                },
                separators=(",", ":"),
            ).encode()
            writer.write(struct.pack(">I", len(response)) + response)
            await writer.drain()
            writer.close()
            await writer.wait_closed()

        listener = await asyncio.start_unix_server(server, socket_path)
        try:
            response = await handshake(socket_path, "python-test-v1")
            assert response["daemon_revision"] == "rust-test-v1"
        finally:
            listener.close()
            await listener.wait_closed()
            temporary.cleanup()

    asyncio.run(scenario())
