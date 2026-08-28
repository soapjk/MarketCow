from __future__ import annotations

import argparse
import asyncio
import csv
import hashlib
import io
import json
import os
import secrets
import struct
from functools import partial
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable

from marketcow.providers.sec_dividends import parse_sec_dividend_filing

PROTOCOL_VERSION = "marketcow.worker.v1"
MAX_FRAME_BYTES = 1_048_576
SEC_DIVIDEND_TASK = "transform.sec_dividend_filing"
SEC_DIVIDEND_REQUEST_SCHEMA = "marketcow.worker.transform.sec-dividend-filing.v1"
SEC_DIVIDEND_RESULT_SCHEMA = "marketcow.worker.transform.sec-dividend-filing-result.v1"
CSV_INFERENCE_TASK = "transform.csv_inference"
CSV_INFERENCE_REQUEST_SCHEMA = "marketcow.worker.transform.csv-inference.v1"
CSV_INFERENCE_RESULT_SCHEMA = "marketcow.worker.transform.csv-inference-result.v1"
LONGPORT_RESOLVE_TASK = "provider.longport.resolve_instruments"
LONGPORT_RESOLVE_REQUEST_SCHEMA = "marketcow.worker.provider.longport-resolve.v1"
LONGPORT_RESOLVE_RESULT_SCHEMA = "marketcow.worker.provider.longport-resolve-result.v1"
POLL_INTERVAL_SECONDS = 1.0
MAX_CSV_ROWS = 10_000
MAX_CSV_COLUMNS = 256
ALLOWED_CSV_DELIMITERS = (",", "\t", ";", "|")

Handler = Callable[[dict[str, Any]], dict[str, Any]]


async def read_frame(reader: asyncio.StreamReader) -> dict[str, Any]:
    length = struct.unpack(">I", await reader.readexactly(4))[0]
    if not 0 < length <= MAX_FRAME_BYTES:
        raise ValueError("invalid server frame")
    value = json.loads(await reader.readexactly(length))
    if not isinstance(value, dict) or value.get("protocol_version") != PROTOCOL_VERSION:
        raise ValueError("worker protocol version mismatch")
    return value


async def write_frame(writer: asyncio.StreamWriter, value: dict[str, Any]) -> None:
    encoded = json.dumps(value, allow_nan=False, separators=(",", ":"), sort_keys=True).encode()
    if not 0 < len(encoded) <= MAX_FRAME_BYTES:
        raise ValueError("worker frame too large")
    writer.write(struct.pack(">I", len(encoded)) + encoded)
    await writer.drain()


async def open_session(
    socket_path: Path,
    revision: str,
    *,
    capabilities: list[str],
    worker_id: str | None = None,
) -> tuple[asyncio.StreamReader, asyncio.StreamWriter, dict[str, Any]]:
    if not socket_path.is_absolute():
        raise ValueError("worker socket path must be absolute")
    if not revision or len(revision) > 256:
        raise ValueError("worker revision is invalid")
    if not capabilities or len(capabilities) > 64 or any(not item for item in capabilities):
        raise ValueError("worker capabilities are invalid")
    reader, writer = await asyncio.open_unix_connection(socket_path)
    nonce = secrets.token_hex(16)
    message_id = secrets.token_hex(16)
    await write_frame(
        writer,
        {
            "protocol_version": PROTOCOL_VERSION,
            "message_id": message_id,
            "message_type": "hello",
            "worker_id": worker_id or f"python-{os.getpid()}",
            "worker_revision": revision,
            "nonce": nonce,
            "capabilities": capabilities,
        },
    )
    response = await read_frame(reader)
    if response.get("message_id") != message_id:
        writer.close()
        await writer.wait_closed()
        raise ValueError("worker handshake response correlation failed")
    if response.get("message_type") != "hello_ack" or response.get("nonce") != nonce:
        writer.close()
        await writer.wait_closed()
        raise ValueError("worker handshake authentication failed")
    if response.get("maximum_frame_bytes") != MAX_FRAME_BYTES:
        writer.close()
        await writer.wait_closed()
        raise ValueError("worker frame limit mismatch")
    return reader, writer, response


async def handshake(socket_path: Path, revision: str) -> dict[str, object]:
    reader, writer, response = await open_session(
        socket_path,
        revision,
        capabilities=[SEC_DIVIDEND_TASK],
    )
    # The handshake compatibility helper intentionally performs no work.
    writer.close()
    await writer.wait_closed()
    return response


def safe_staging_path(root: Path, task_id: str, filename: str) -> Path:
    if not root.is_absolute() or not task_id.replace("-", "").isalnum():
        raise ValueError("invalid staging lease")
    target = (root / task_id / filename).resolve()
    lease = (root / task_id).resolve()
    if target.parent != lease or target.name != filename:
        raise ValueError("staging path escape rejected")
    return target


def handle_sec_dividend_filing(request: dict[str, Any]) -> dict[str, Any]:
    required = {"text", "symbol", "filed_at", "source_url", "accession"}
    if set(request) != required or not all(isinstance(request[key], str) for key in required):
        raise ValueError("SEC dividend request fields are invalid")
    filed_at = datetime.fromisoformat(request["filed_at"].replace("Z", "+00:00"))
    if filed_at.tzinfo is None:
        raise ValueError("filed_at must be timezone-aware")
    rows = parse_sec_dividend_filing(
        request["text"],
        request["symbol"],
        filed_at.astimezone(UTC).isoformat(),
        request["source_url"],
        request["accession"],
    )
    return {"schema_version": SEC_DIVIDEND_RESULT_SCHEMA, "rows": rows}


def handle_csv_inference(request: dict[str, Any]) -> dict[str, Any]:
    required = {"content", "source", "observed_at"}
    optional = {"delimiter"}
    if not required.issubset(request) or not set(request).issubset(required | optional):
        raise ValueError("CSV inference request fields are invalid")
    if not all(isinstance(request[key], str) for key in required):
        raise ValueError("CSV inference request fields are invalid")
    content = request["content"]
    if not content or "\x00" in content or len(content.encode()) > MAX_FRAME_BYTES // 2:
        raise ValueError("CSV content is empty, contains NUL, or is too large")
    source = request["source"]
    if not source or len(source) > 2048:
        raise ValueError("CSV source is invalid")
    observed_at = datetime.fromisoformat(request["observed_at"].replace("Z", "+00:00"))
    if observed_at.tzinfo is None:
        raise ValueError("observed_at must be timezone-aware")
    requested_delimiter = request.get("delimiter")
    if requested_delimiter is not None and requested_delimiter not in ALLOWED_CSV_DELIMITERS:
        raise ValueError("CSV delimiter is unsupported")
    if requested_delimiter is None:
        try:
            delimiter = csv.Sniffer().sniff(
                content[:8192], delimiters="".join(ALLOWED_CSV_DELIMITERS)
            ).delimiter
        except csv.Error as error:
            raise ValueError("CSV delimiter could not be inferred") from error
    else:
        delimiter = requested_delimiter
    reader = csv.reader(io.StringIO(content, newline=""), delimiter=delimiter, strict=True)
    try:
        columns = next(reader)
        if not columns or len(columns) > MAX_CSV_COLUMNS:
            raise ValueError("CSV column count is invalid")
        if any(not column or len(column) > 512 for column in columns):
            raise ValueError("CSV header is invalid")
        if len(set(columns)) != len(columns):
            raise ValueError("CSV headers must be unique")
        rows: list[list[str]] = []
        for row in reader:
            if len(rows) >= MAX_CSV_ROWS:
                raise ValueError("CSV row count exceeds limit")
            if len(row) != len(columns):
                raise ValueError("CSV row width does not match header")
            rows.append(row)
    except csv.Error as error:
        raise ValueError("CSV syntax is invalid") from error
    return {
        "schema_version": CSV_INFERENCE_RESULT_SCHEMA,
        "source": source,
        "observed_at": observed_at.astimezone(UTC).isoformat(),
        "content_sha256": hashlib.sha256(content.encode()).hexdigest(),
        "delimiter": delimiter,
        "columns": columns,
        "rows": rows,
        "row_count": len(rows),
    }


def handle_longport_resolution(request: dict[str, Any], *, provider: Any) -> dict[str, Any]:
    if set(request) != {"namespace", "symbols"}:
        raise ValueError("LongPort resolution request fields are invalid")
    if request["namespace"] != "provider:longport":
        raise ValueError("LongPort resolution namespace is invalid")
    symbols = request["symbols"]
    if (
        not isinstance(symbols, list)
        or not 1 <= len(symbols) <= 20
        or any(
            not isinstance(symbol, str)
            or not symbol
            or len(symbol) > 128
            or symbol != symbol.strip().upper().replace(" ", "")
            for symbol in symbols
        )
    ):
        raise ValueError("LongPort resolution symbols are invalid")
    raw_items = provider.resolve_instruments(symbols)
    if not isinstance(raw_items, list) or len(raw_items) != len(symbols):
        raise RuntimeError("LongPort returned an incomplete resolution batch")
    items = []
    for expected_symbol, raw in zip(symbols, raw_items, strict=True):
        if not isinstance(raw, dict) or raw.get("external_symbol") != expected_symbol:
            raise RuntimeError("LongPort resolution order or identity changed")
        status = raw.get("status")
        if status == "error":
            error = raw.get("error")
            if (
                not isinstance(error, dict)
                or error.get("code") not in {"not_found", "ambiguous", "provider_unavailable"}
                or not isinstance(error.get("message"), str)
                or not 1 <= len(error["message"]) <= 1000
            ):
                raise RuntimeError("LongPort resolution error contract is invalid")
            items.append({
                "external_symbol": expected_symbol,
                "status": "error",
                "error": {"code": error["code"], "message": error["message"]},
            })
            continue
        required = {
            "instrument_id", "symbol", "mic", "market", "currency", "lot_size",
            "source", "source_exchange",
        }
        if status != "resolved" or any(key not in raw for key in required):
            raise RuntimeError("LongPort resolved instrument contract is incomplete")
        lot_size = str(raw["lot_size"])
        if not lot_size.isdigit() or int(lot_size) <= 0:
            raise RuntimeError("LongPort lot size is invalid")
        item = {
            "external_symbol": expected_symbol,
            "status": "resolved",
            "instrument_id": str(raw["instrument_id"]),
            "symbol": str(raw["symbol"]),
            "mic": str(raw["mic"]),
            "market": str(raw["market"]),
            "currency": str(raw["currency"]),
            "lot_size": lot_size,
            "source": str(raw["source"]),
            "source_exchange": str(raw["source_exchange"]),
        }
        if any(not value or len(value) > 256 for key, value in item.items() if key != "status"):
            raise RuntimeError("LongPort resolved instrument text is invalid")
        items.append(item)
    return {
        "schema_version": LONGPORT_RESOLVE_RESULT_SCHEMA,
        "namespace": "provider:longport",
        "observed_at": datetime.now(UTC).isoformat(),
        "items": items,
    }


def _longport_handler_from_secret_fd() -> Handler:
    if os.environ.get("MARKETCOW_PROVIDER_SECRET_FD") != "3":
        raise ValueError("LongPort provider secret descriptor is unavailable")
    with os.fdopen(os.dup(3), "rb", closefd=True) as handle:
        encoded = handle.read(65_537)
    if not encoded or len(encoded) > 65_536:
        raise ValueError("LongPort provider secret is invalid")
    secret = json.loads(encoded)
    required = {"app_key", "app_secret", "access_token"}
    optional = {"enable_overnight"}
    if (
        not isinstance(secret, dict)
        or not required.issubset(secret)
        or not set(secret).issubset(required | optional)
        or any(not isinstance(secret[key], str) or not secret[key] for key in required)
        or not isinstance(secret.get("enable_overnight", False), bool)
    ):
        raise ValueError("LongPort provider secret fields are invalid")
    from marketcow.providers.longport_quote import LongPortQuoteProvider

    provider = LongPortQuoteProvider(
        secret["app_key"],
        secret["app_secret"],
        secret["access_token"],
        enable_overnight=secret.get("enable_overnight", False),
    )
    return partial(handle_longport_resolution, provider=provider)


HANDLERS: dict[tuple[str, str], Handler] = {
    (SEC_DIVIDEND_TASK, SEC_DIVIDEND_REQUEST_SCHEMA): handle_sec_dividend_filing,
    (CSV_INFERENCE_TASK, CSV_INFERENCE_REQUEST_SCHEMA): handle_csv_inference,
}
REGISTERED_CAPABILITIES = {
    SEC_DIVIDEND_TASK,
    CSV_INFERENCE_TASK,
    LONGPORT_RESOLVE_TASK,
}


def write_staging_result(staging_path: Path, job_id: str, payload: dict[str, Any]) -> tuple[str, int]:
    target = safe_staging_path(staging_path.parent.resolve(), job_id, "result.json")
    if target.parent != staging_path.resolve():
        raise ValueError("daemon staging lease does not match job")
    encoded = json.dumps(
        payload, ensure_ascii=False, allow_nan=False, separators=(",", ":"), sort_keys=True
    ).encode()
    temporary = safe_staging_path(
        staging_path.parent.resolve(), job_id, f"result-{secrets.token_hex(8)}.tmp"
    )
    with temporary.open("xb") as handle:
        handle.write(encoded)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, target)
    return hashlib.sha256(encoded).hexdigest(), len(encoded)


async def exchange(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    message_type: str,
    **fields: Any,
) -> dict[str, Any]:
    message_id = secrets.token_hex(16)
    await write_frame(
        writer,
        {
            "protocol_version": PROTOCOL_VERSION,
            "message_id": message_id,
            "message_type": message_type,
            **fields,
        },
    )
    response = await read_frame(reader)
    if response.get("message_id") != message_id:
        raise ValueError("worker response correlation failed")
    return response


async def run_one_task(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    handlers: dict[tuple[str, str], Handler] | None = None,
) -> bool:
    handlers = HANDLERS if handlers is None else handlers
    task = await exchange(reader, writer, "poll")
    if task.get("message_type") == "no_work":
        return False
    if task.get("message_type") != "task":
        raise ValueError("daemon returned an invalid poll response")
    required = {
        "job_id", "lease_token", "deadline", "job_type", "request_schema",
        "request_sha256", "request", "staging_path",
    }
    if not required.issubset(task):
        raise ValueError("daemon task is incomplete")
    job_id = str(task["job_id"])
    lease_token = str(task["lease_token"])
    handler = handlers.get((str(task["job_type"]), str(task["request_schema"])))
    if handler is None:
        raise ValueError("daemon assigned an unsupported task")
    request = task["request"]
    if not isinstance(request, dict):
        raise ValueError("worker request must be an object")
    request_sha256 = hashlib.sha256(
        json.dumps(request, allow_nan=False, separators=(",", ":"), sort_keys=True).encode()
    ).hexdigest()
    if request_sha256 != str(task["request_sha256"]).lower():
        raise ValueError("worker request SHA-256 mismatch")
    started = await exchange(
        reader,
        writer,
        "start",
        job_id=job_id,
        lease_token=lease_token,
    )
    if started.get("message_type") != "job_state" or started.get("status") != "running":
        raise RuntimeError("daemon rejected job start")
    deadline = datetime.fromisoformat(str(task["deadline"]).replace("Z", "+00:00"))
    remaining = (deadline.astimezone(UTC) - datetime.now(UTC)).total_seconds()
    if remaining <= 0:
        raise TimeoutError("job deadline elapsed")
    try:
        async with asyncio.timeout(remaining):
            payload = await asyncio.to_thread(handler, request)
            sha256, size_bytes = await asyncio.to_thread(
                write_staging_result, Path(str(task["staging_path"])), job_id, payload
            )
    except (ValueError, TypeError) as error:
        response = await exchange(
            reader,
            writer,
            "fail",
            job_id=job_id,
            lease_token=lease_token,
            code="invalid_provider_request",
            classification="input_validation",
            redacted_message=type(error).__name__,
            retryable=False,
        )
        if response.get("message_type") != "job_state":
            raise RuntimeError("daemon rejected terminal failure") from error
        return True
    except TimeoutError as error:
        response = await exchange(
            reader,
            writer,
            "fail",
            job_id=job_id,
            lease_token=lease_token,
            code="provider_timeout",
            classification="timeout",
            redacted_message=type(error).__name__,
            retryable=True,
        )
        if response.get("message_type") != "job_state":
            raise RuntimeError("daemon rejected retryable timeout") from error
        return True
    except Exception as error:
        response = await exchange(
            reader,
            writer,
            "fail",
            job_id=job_id,
            lease_token=lease_token,
            code="provider_execution_failed",
            classification="provider_failure",
            redacted_message=type(error).__name__,
            retryable=True,
        )
        if response.get("message_type") != "job_state":
            raise RuntimeError("daemon rejected retryable provider failure") from error
        return True
    completed = await exchange(
        reader,
        writer,
        "complete",
        job_id=job_id,
        lease_token=lease_token,
        relative_path="result.json",
        sha256=sha256,
        size_bytes=size_bytes,
        media_type="application/json",
    )
    if completed.get("message_type") != "job_state" or completed.get("status") != "succeeded":
        raise RuntimeError("daemon rejected worker result")
    return True


async def run_worker(
    socket_path: Path,
    revision: str,
    *,
    once: bool = False,
    capabilities: list[str] | None = None,
    handler_overrides: dict[tuple[str, str], Handler] | None = None,
) -> None:
    selected = sorted(REGISTERED_CAPABILITIES if capabilities is None else set(capabilities))
    if not selected or any(capability not in REGISTERED_CAPABILITIES for capability in selected):
        raise ValueError("configured worker capability is unsupported")
    handlers = {
        key: handler for key, handler in HANDLERS.items() if key[0] in selected
    }
    overrides = {} if handler_overrides is None else dict(handler_overrides)
    if any(
        key[0] not in selected or key[0] not in REGISTERED_CAPABILITIES
        for key in overrides
    ):
        raise ValueError("worker handler override capability is invalid")
    handlers.update(overrides)
    longport_key = (LONGPORT_RESOLVE_TASK, LONGPORT_RESOLVE_REQUEST_SCHEMA)
    if LONGPORT_RESOLVE_TASK in selected and longport_key not in handlers:
        handlers[longport_key] = _longport_handler_from_secret_fd()
    reader, writer, _ = await open_session(socket_path, revision, capabilities=selected)
    try:
        while True:
            handled = await run_one_task(reader, writer, handlers)
            if once:
                return
            if not handled:
                await asyncio.sleep(POLL_INTERVAL_SECONDS)
    finally:
        writer.close()
        await writer.wait_closed()


async def _main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--socket", type=Path, required=True)
    parser.add_argument("--revision", default=os.environ.get("MARKETCOW_WORKER_REVISION", "development"))
    parser.add_argument("--capability", action="append", dest="capabilities")
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()
    await run_worker(
        args.socket,
        args.revision,
        once=args.once,
        capabilities=args.capabilities,
    )


if __name__ == "__main__":
    asyncio.run(_main())
