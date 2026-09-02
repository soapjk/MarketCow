from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from urllib.parse import parse_qs, urlencode, urlsplit, urlunsplit

import httpx
import websockets


AsgiReceive = Callable[[], Awaitable[dict]]
AsgiSend = Callable[[dict], Awaitable[None]]


_HTTP_PATHS = frozenset({
    "/v1/prediction-markets/polymarket/live/health",
    "/v1/prediction-markets/polymarket/live/scope",
    "/v1/prediction-markets/polymarket/live/snapshot",
    "/v1/prediction-markets/polymarket/live/full-sync",
    "/v1/prediction-markets/polymarket/live/events",
    "/v1/prediction-markets/polymarket/live/checkpoint",
})
_MARKET_SNAPSHOT_PREFIX = (
    "/v1/prediction-markets/polymarket/live/markets/"
)
_POLYMARKET_LIVE_PREFIX = "/v1/prediction-markets/polymarket/live/"
_HOP_HEADERS = frozenset({
    b"connection", b"keep-alive", b"proxy-authenticate",
    b"proxy-authorization", b"te", b"trailers", b"transfer-encoding",
    b"upgrade",
})


def _is_polymarket_stream(scope: dict) -> bool:
    path = scope.get("path", "")
    if path == "/v1/prediction-markets/polymarket/live/stream":
        return True
    if path != "/v1/market-data/stream":
        return False
    query = parse_qs(scope.get("query_string", b"").decode("ascii"))
    return query.get("provider", ["polymarket"])[0] == "polymarket"


class RustPolymarketGatewayMiddleware:
    """Route the authoritative hot Scope to the internal Rust data plane.

    Discovery and non-Polymarket APIs remain local to the unified gateway. Once
    configured, no Python scoped projection is consulted for these routes.
    """

    def __init__(
        self,
        app,
        *,
        base_url: str,
        client_factory: Callable[[], httpx.AsyncClient] | None = None,
        maximum_response_bytes: int = 256 * 1024 * 1024,
    ) -> None:
        self.app = app
        self.base_url = base_url.rstrip("/")
        self.client_factory = client_factory or (
            lambda: httpx.AsyncClient(
                trust_env=False,
                follow_redirects=False,
                timeout=httpx.Timeout(60, connect=5),
            )
        )
        self.maximum_response_bytes = maximum_response_bytes

    async def __call__(self, scope: dict, receive: AsgiReceive, send: AsgiSend):
        path = scope.get("path", "")
        if scope["type"] == "http" and (
            path in _HTTP_PATHS
            or path.startswith(_MARKET_SNAPSHOT_PREFIX)
            or path.startswith(_POLYMARKET_LIVE_PREFIX)
        ):
            await self._http(scope, receive, send)
            return
        if scope["type"] == "websocket" and _is_polymarket_stream(scope):
            await self._websocket(scope, receive, send)
            return
        await self.app(scope, receive, send)

    async def _http(
        self, scope: dict, receive: AsgiReceive, send: AsgiSend,
    ) -> None:
        body = bytearray()
        while True:
            message = await receive()
            if message["type"] == "http.disconnect":
                return
            body.extend(message.get("body", b""))
            if len(body) > 1_048_576:
                await self._error(send, 413, b"request body exceeds proxy limit")
                return
            if not message.get("more_body", False):
                break
        query = scope.get("query_string", b"")
        url = f"{self.base_url}{scope['path']}"
        if query:
            url += "?" + query.decode("ascii")
        headers = {
            key.decode("latin-1"): value.decode("latin-1")
            for key, value in scope.get("headers", [])
            if key.lower() not in _HOP_HEADERS and key.lower() != b"host"
        }
        try:
            async with self.client_factory() as client:
                response = await client.request(
                    scope["method"], url, content=bytes(body), headers=headers,
                )
                content = response.content
            if len(content) > self.maximum_response_bytes:
                await self._error(send, 502, b"Rust data-plane response exceeds proxy limit")
                return
            response_headers = [
                (key.lower(), value)
                for key, value in response.headers.raw
                if key.lower() not in _HOP_HEADERS and key.lower() != b"content-length"
            ]
            response_headers.append((b"content-length", str(len(content)).encode()))
            await send({
                "type": "http.response.start",
                "status": response.status_code,
                "headers": response_headers,
            })
            await send({"type": "http.response.body", "body": content})
        except (httpx.HTTPError, OSError) as exc:
            await self._error(
                send,
                503,
                f"Rust Polymarket data plane unavailable: {type(exc).__name__}".encode(),
            )

    async def _websocket(
        self, scope: dict, receive: AsgiReceive, send: AsgiSend,
    ) -> None:
        first = await receive()
        if first["type"] != "websocket.connect":
            return
        source_path = scope.get("path", "")
        target_path = (
            "/v1/market-data/stream"
            if source_path.endswith("/live/stream") else source_path
        )
        parsed = urlsplit(self.base_url)
        query = scope.get("query_string", b"").decode("ascii")
        if source_path.endswith("/live/stream"):
            values = parse_qs(query, keep_blank_values=True)
            values["provider"] = ["polymarket"]
            query = urlencode(values, doseq=True)
        target = urlunsplit((
            "wss" if parsed.scheme == "https" else "ws",
            parsed.netloc,
            target_path,
            query,
            "",
        ))
        try:
            async with websockets.connect(
                target,
                open_timeout=5,
                close_timeout=5,
                max_size=64 * 1024 * 1024,
                proxy=None,
            ) as upstream:
                await send({"type": "websocket.accept"})

                async def client_to_upstream() -> None:
                    while True:
                        message = await receive()
                        if message["type"] == "websocket.disconnect":
                            await upstream.close()
                            return
                        if message.get("text") is not None:
                            await upstream.send(message["text"])
                        elif message.get("bytes") is not None:
                            await upstream.send(message["bytes"])

                async def upstream_to_client() -> None:
                    async for message in upstream:
                        key = "text" if isinstance(message, str) else "bytes"
                        await send({"type": "websocket.send", key: message})

                tasks = {
                    asyncio.create_task(client_to_upstream()),
                    asyncio.create_task(upstream_to_client()),
                }
                done, pending = await asyncio.wait(
                    tasks, return_when=asyncio.FIRST_COMPLETED,
                )
                for task in pending:
                    task.cancel()
                await asyncio.gather(*done, *pending, return_exceptions=True)
        except Exception:
            await send({"type": "websocket.close", "code": 1013})

    @staticmethod
    async def _error(send: AsgiSend, status: int, message: bytes) -> None:
        body = b'{"detail":{"code":"polymarket_rust_data_plane_unavailable","message":"' + message.replace(b'"', b"'") + b'"}}'
        await send({
            "type": "http.response.start",
            "status": status,
            "headers": [(b"content-type", b"application/json"), (b"content-length", str(len(body)).encode())],
        })
        await send({"type": "http.response.body", "body": body})
