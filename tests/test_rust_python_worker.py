import asyncio
import hashlib
import json
import struct
from datetime import UTC, datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest

from python.marketcow_workers.worker import (
    MAX_FRAME_BYTES,
    PROTOCOL_VERSION,
    SEC_DIVIDEND_REQUEST_SCHEMA,
    SEC_DIVIDEND_RESULT_SCHEMA,
    SEC_DIVIDEND_TASK,
    handle_sec_dividend_filing,
    handshake,
    run_worker,
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


def test_sec_dividend_handler_preserves_exact_decimal_and_provenance() -> None:
    result = handle_sec_dividend_filing({
        "text": (
            "The board declared a cash dividend of $0.250000000000000001 per share, "
            "payable on September 30, 2026 to shareholders of record on September 10, 2026."
        ),
        "symbol": "AAPL.XNAS",
        "filed_at": "2026-08-28T00:00:00Z",
        "source_url": "https://www.sec.gov/Archives/fixture.htm",
        "accession": "0000000000-26-000001",
    })
    assert result["schema_version"] == SEC_DIVIDEND_RESULT_SCHEMA
    assert result["rows"][0]["amount_per_share"] == "0.250000000000000001"
    assert result["rows"][0]["currency"] == "USD"
    assert result["rows"][0]["source_type"] == "regulatory_filing"


def test_python_worker_executes_task_only_through_leased_uds_protocol() -> None:
    async def scenario() -> None:
        temporary = TemporaryDirectory(prefix="mc-worker-task-", dir="/tmp")
        root = Path(temporary.name)
        socket_path = root / "worker.sock"
        staging_path = root / "staging" / "job-1"
        staging_path.mkdir(parents=True)
        request = {
            "text": (
                "Dividend of $0.125 per share payable October 1, 2026 "
                "to holders of record September 15, 2026."
            ),
            "symbol": "AAPL.XNAS",
            "filed_at": "2026-08-28T00:00:00Z",
            "source_url": "https://www.sec.gov/Archives/fixture.htm",
            "accession": "0000000000-26-000002",
        }
        request_sha256 = hashlib.sha256(
            json.dumps(request, allow_nan=False, separators=(",", ":"), sort_keys=True).encode()
        ).hexdigest()

        async def read_request(reader: asyncio.StreamReader) -> dict:
            length = struct.unpack(">I", await reader.readexactly(4))[0]
            assert 0 < length <= MAX_FRAME_BYTES
            return json.loads(await reader.readexactly(length))

        async def respond(writer: asyncio.StreamWriter, request_frame: dict, **payload: object) -> None:
            encoded = json.dumps({
                "protocol_version": PROTOCOL_VERSION,
                "message_id": request_frame["message_id"],
                **payload,
            }, separators=(",", ":")).encode()
            writer.write(struct.pack(">I", len(encoded)) + encoded)
            await writer.drain()

        async def server(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            hello = await read_request(reader)
            assert hello["message_type"] == "hello"
            assert hello["capabilities"] == [SEC_DIVIDEND_TASK]
            await respond(
                writer,
                hello,
                message_type="hello_ack",
                daemon_revision="rust-test-v1",
                nonce=hello["nonce"],
                maximum_frame_bytes=MAX_FRAME_BYTES,
            )
            poll = await read_request(reader)
            assert poll["message_type"] == "poll"
            await respond(
                writer,
                poll,
                message_type="task",
                job_id="job-1",
                lease_token="lease-1",
                deadline=(datetime.now(UTC) + timedelta(minutes=1)).isoformat(),
                job_type=SEC_DIVIDEND_TASK,
                request_schema=SEC_DIVIDEND_REQUEST_SCHEMA,
                request_sha256=request_sha256,
                request=request,
                staging_path=str(staging_path),
            )
            start = await read_request(reader)
            assert start["message_type"] == "start"
            assert start["lease_token"] == "lease-1"
            await respond(
                writer, start, message_type="job_state", job_id="job-1", status="running", revision=3
            )
            complete = await read_request(reader)
            assert complete["message_type"] == "complete"
            assert complete["relative_path"] == "result.json"
            body = (staging_path / "result.json").read_bytes()
            assert complete["sha256"] == hashlib.sha256(body).hexdigest()
            assert complete["size_bytes"] == len(body)
            parsed = json.loads(body)
            assert parsed["rows"][0]["amount_per_share"] == "0.125"
            await respond(
                writer,
                complete,
                message_type="job_state",
                job_id="job-1",
                status="succeeded",
                revision=4,
            )
            writer.close()
            await writer.wait_closed()

        listener = await asyncio.start_unix_server(server, socket_path)
        try:
            await run_worker(socket_path, "python-test-v2", once=True)
        finally:
            listener.close()
            await listener.wait_closed()
            temporary.cleanup()

    asyncio.run(scenario())
