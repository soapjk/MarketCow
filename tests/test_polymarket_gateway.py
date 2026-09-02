from __future__ import annotations

import httpx
import unittest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from marketcow.polymarket_gateway import RustPolymarketGatewayMiddleware


class RustPolymarketGatewayTest(unittest.TestCase):
    def test_authoritative_live_routes_are_forwarded_to_rust(self) -> None:
        observed: list[httpx.Request] = []

        def handler(request: httpx.Request) -> httpx.Response:
            observed.append(request)
            return httpx.Response(
                200,
                json={"owner": "rust", "query": request.url.query.decode()},
                headers={"x-rust-scope": "active"},
            )

        app = FastAPI()
        transport = httpx.MockTransport(handler)
        app.add_middleware(
            RustPolymarketGatewayMiddleware,
            base_url="http://127.0.0.1:8796",
            client_factory=lambda: httpx.AsyncClient(transport=transport),
        )

        @app.get("/v1/prediction-markets/polymarket/live/scope")
        def legacy_scope() -> dict[str, str]:
            return {"owner": "python"}

        @app.get("/v1/health")
        def health() -> dict[str, str]:
            return {"owner": "gateway"}

        @app.get("/v1/prediction-markets/polymarket/live/gaps")
        def legacy_gaps() -> dict[str, str]:
            return {"owner": "python"}

        @app.get("/v1/prediction-markets/polymarket/live/discovery/snapshot")
        def discovery_snapshot() -> dict[str, str]:
            return {"owner": "python-discovery"}

        with TestClient(app) as client:
            response = client.get(
                "/v1/prediction-markets/polymarket/live/scope?generation=7"
            )
            retired_route_response = client.get(
                "/v1/prediction-markets/polymarket/live/gaps"
            )
            discovery_response = client.get(
                "/v1/prediction-markets/polymarket/live/discovery/snapshot"
            )
            health_response = client.get("/v1/health")

        self.assertEqual(
            response.json(), {"owner": "rust", "query": "generation=7"},
        )
        self.assertEqual(response.headers["x-rust-scope"], "active")
        self.assertEqual(
            str(observed[0].url),
            "http://127.0.0.1:8796/v1/prediction-markets/polymarket/live/scope?generation=7",
        )
        self.assertEqual(health_response.json(), {"owner": "gateway"})
        self.assertEqual(retired_route_response.json()["owner"], "rust")
        self.assertEqual(discovery_response.json()["owner"], "python-discovery")
        self.assertEqual(
            str(observed[1].url),
            "http://127.0.0.1:8796/v1/prediction-markets/polymarket/live/gaps",
        )

    def test_unavailable_rust_scope_fails_closed_without_python_fallback(self) -> None:
        def handler(_request: httpx.Request) -> httpx.Response:
            raise httpx.ConnectError("offline")

        app = FastAPI()
        transport = httpx.MockTransport(handler)
        app.add_middleware(
            RustPolymarketGatewayMiddleware,
            base_url="http://127.0.0.1:8796",
            client_factory=lambda: httpx.AsyncClient(transport=transport),
        )

        @app.get("/v1/prediction-markets/polymarket/live/scope")
        def legacy_scope() -> dict[str, str]:
            return {"owner": "python"}

        with TestClient(app) as client:
            response = client.get("/v1/prediction-markets/polymarket/live/scope")

        self.assertEqual(response.status_code, 503)
        self.assertEqual(
            response.json()["detail"]["code"],
            "polymarket_rust_data_plane_unavailable",
        )
