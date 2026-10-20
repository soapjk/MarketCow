"""Native wheel integration through bounded loopback Proto HTTP, no suppliers."""

import asyncio
from collections import Counter
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread

import pytest

from marketcow_sdk import AsyncClient, Client, Finality, Market, Reference, Rules, Source


def varint(value):
    out = bytearray()
    while value > 127:
        out.append((value & 127) | 128)
        value >>= 7
    out.append(value)
    return bytes(out)


def integer(number, value):
    return varint(number << 3) + varint(value)


def blob(number, value):
    if isinstance(value, str):
        value = value.encode()
    return varint((number << 3) | 2) + varint(len(value)) + value


def fields(data):
    def read(at):
        value = shift = 0
        while True:
            byte = data[at]
            at += 1
            value |= (byte & 127) << shift
            if byte < 128:
                return value, at
            shift += 7

    out = {}
    at = 0
    while at < len(data):
        tag, at = read(at)
        if tag & 7 == 0:
            value, at = read(at)
        else:
            assert tag & 7 == 2
            size, at = read(at)
            value, at = data[at : at + size], at + size
        out[tag >> 3] = value
    return out


@pytest.fixture
def endpoint():
    calls = Counter()
    session = "0d711d81-c50f-42aa-83bc-08504d81ccca"
    long_query = [False]
    identity = blob(1, "rules-a") + integer(2, 7)
    key = blob(1, "market-a") + blob(2, "condition-a") + blob(3, "window-a")
    key += blob(4, "start") + blob(5, "end") + blob(6, "up") + blob(7, "down")
    key += blob(9, identity)

    def trade(i):
        return (
            integer(1, i)
            + integer(2, 900 + i * 100)
            + integer(3, 905 + i * 100)
            + blob(4, "100.00")
            + blob(5, "0.0100")
            + integer(6, 0)
            + blob(8, blob(1, "evidence-a") + integer(2, 0))
        )

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *_):
            pass

        def respond(self, body, status=200):
            self.send_response(status)
            self.send_header("Content-Type", "application/x-protobuf")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            calls[self.path] += 1
            if self.headers.get("Authorization") != "Bearer local-test":
                return self.respond(blob(1, "authentication_required"), 401)
            if self.path == "/v3/btc/capabilities":
                return self.respond(
                    blob(1, "marketcow.btc-capabilities.v3")
                    + integer(3, 1000)
                    + integer(4, 1024)
                    + integer(5, 1_024_000)
                    + integer(6, 1 << 30)
                    + integer(7, 960000)
                    + integer(8, 600)
                    + integer(9, 1)
                    + integer(10, 1)
                )
            if self.path == "/v3/btc/health":
                source = integer(1, 2) + blob(2, "BTCUSD.CHAINLINK") + blob(3, "not_collected")
                return self.respond(
                    blob(1, "marketcow.btc-health.v3") + integer(2, 1) + blob(3, source)
                )
            if self.path == "/v7/polymarket/btc-5m/market-streams":
                if calls["directory_error"]:
                    return self.respond(blob(1, "manager_unavailable") + integer(2, 1), 503)
                if calls["directory_empty"]:
                    return self.respond(blob(1, "marketcow.polymarket.btc5m.market-streams.v7"))
                binding = blob(1, key) + blob(2, "current") + blob(3, "stream-a")
                budget = integer(1, 8388608) + integer(2, 8388608)
                budget += integer(3, 10000) + integer(4, 10000)
                return self.respond(
                    blob(1, "marketcow.polymarket.btc5m.market-streams.v7")
                    + blob(2, binding)
                    + blob(3, budget)
                )
            if self.path == "/v3/btc/bars/1s":
                return self.respond(
                    blob(1, "marketcow.binance-finalized-window.v3")
                    + blob(2, "BTCUSDT.BINANCE")
                    + integer(3, 1000)
                    + blob(4, "bars-a")
                    + blob(6, "warming")
                )
            self.respond(blob(1, "not_found"), 404)

        def do_POST(self):
            calls[self.path] += 1
            body = self.rfile.read(int(self.headers["Content-Length"]))
            request = fields(body)
            if self.path in ("/v1/facts/polymarket/reference", "/v1/facts/polymarket/finality"):
                meta = blob(1, blob(1, "fact-a") + integer(2, 3))
                meta += blob(2, request[2]) + integer(3, 1234)
                response = blob(2, request[1]) + blob(3, meta)
                if self.path.endswith("reference"):
                    reading = blob(1, blob(1, "reading-a") + integer(2, 4))
                    reading += blob(2, "binance_preopen_proxy") + blob(3, "BTCUSDT.BINANCE")
                    reading += blob(4, "USDT") + blob(5, "twap") + blob(6, "100.00")
                    reading += integer(7, 60000) + integer(8, 0) + integer(9, 60000)
                    reading += integer(10, 60000) + blob(11, "preopen_60s")
                    reading += blob(12, "verified") + blob(15, "epoch-a") + blob(15, "epoch-b")
                    reading += blob(16, blob(1, "evidence-a") + integer(2, 0))
                    reading += blob(17, "baseline-a")
                    return self.respond(
                        blob(1, "marketcow.facts.reference.v1")
                        + response
                        + blob(5, integer(1, 1) + blob(3, reading))
                    )
                payout = blob(1, "up") + blob(2, "1")
                provider = blob(1, "rpc-a") + blob(2, "operator-a") + blob(3, "domain-a")
                provider += blob(4, identity) + blob(5, "valid_resolved")
                provider += blob(7, blob(1, "external-block-id") + blob(2, "42"))
                provider += blob(9, "137") + blob(10, "ctf-a") + blob(11, "PUSD")
                provider += blob(12, payout) + blob(13, "1")
                provider += integer(14, 1) + integer(15, 1) + integer(16, 1)
                return self.respond(
                    blob(1, "marketcow.facts.finality.v1")
                    + response
                    + blob(4, "verified_final")
                    + blob(5, "one_valid_no_conflict")
                    + integer(6, 2)
                    + integer(7, 1)
                    + integer(8, 1)
                    + blob(9, "wait_both")
                    + blob(10, provider)
                    + blob(10, blob(1, "rpc-b") + blob(5, "timed_out"))
                    + blob(11, payout)
                    + blob(12, "1")
                    + integer(13, 1)
                    + integer(14, 1)
                    + integer(15, 91)
                    + blob(17, "single_valid")
                    + integer(18, 15000)
                )
            if self.path == "/v3/btc/trade-history":
                first = 2 not in request
                result = blob(1, "marketcow.btc-trade-history.v3") + blob(2, session)
                result += blob(3, request[1]) + integer(4, 0 if first else 1)
                result += integer(5, 2) + integer(6, 600)
                if first:
                    result += blob(7, trade(1)) + blob(9, session + ":1")
                else:
                    result += integer(10, 1)
                missing = fields(request[1])[4] == 4
                result += blob(8, trade(2 if first else 3)) + integer(11, int(not missing))
                if missing:
                    result += blob(12, "trade_id_gap")
                    result += blob(13, integer(1, 4) + integer(2, 4))
                result += integer(14, 1100) + integer(15, 1200) + integer(16, 2)
                result += blob(17, blob(1, "writing") + integer(3, 3))
                result += integer(19, 7) + blob(20, request[3])
                return self.respond(result)
            if self.path == "/v3/btc/observation-history/resolve":
                long_query[0] = request[4] - request[3] > 960000
                return self.respond(
                    blob(1, "marketcow.btc-observation-range.v3")
                    + blob(2, body)
                    + integer(3, 3)
                    + integer(4, 2)
                )
            if self.path != "/v3/btc/observation-history":
                return self.respond(blob(1, "not_found"), 404)
            first = 2 not in request
            bounds = fields(request[1])
            points = [(1, 1500), (3, 1_801_500 if long_query[0] else 1500)]
            selected = [(i, t) for i, t in points if bounds[3] <= t < bounds[4]]
            record = b""
            if selected:
                row_id, at = selected[0 if first else -1]
            else:
                row_id, at = 0, 1500
            evidence = blob(1, "original-report") + integer(2, 0)
            observation = integer(1, at) + blob(2, "100.00") + integer(3, 7)
            observation += integer(4, 0) + blob(6, evidence)
            if selected:
                record = integer(1, row_id) + blob(2, "source-a") + blob(3, observation)
            page = blob(1, "marketcow.btc-observation-history.v3") + blob(2, request[1])
            if selected:
                page += blob(3, record)
            if first and len(selected) > 1:
                page += blob(4, f"{session}:1")
            else:
                page += integer(5, 1)
            page += blob(7, "observation_prefix_coverage_unverified") + integer(8, 3)
            page += blob(10, session) + blob(11, request[3]) + integer(12, 600)
            self.respond(page)

        def do_DELETE(self):
            calls[self.path] += 1
            request = fields(self.rfile.read(int(self.headers["Content-Length"])))
            if self.path == "/v3/btc/trade-history/session":
                return self.respond(
                    blob(1, "marketcow.btc-history-release.v3")
                    + blob(2, request[1])
                    + blob(3, request[2])
                    + integer(4, 1)
                    + blob(5, request[3])
                )
            self.respond(
                blob(1, "marketcow.btc-observation-history-release.v3")
                + blob(2, request[1])
                + blob(3, request[2])
                + blob(4, request[3])
                + integer(5, 1)
            )

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}", calls
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def test_native_sync_initialization_health_bars_and_history_release(endpoint):
    url, calls = endpoint
    with Client(url, api_key="local-test") as client:
        assert client.btc_health().sources[0].phase == "not_collected"
        assert not client.bars.latest().ready
        with client.history.observations(
            Source.CHAINLINK_TWAP60, start_time_ms=1000, end_time_ms=2000
        ) as pages:
            items = list(pages)
        assert len(items) == 2
        assert items[-1].complete and not items[-1].coverage_verified
        assert items[0].observations[0].observation.price == "100.00"
        assert calls["/v3/btc/observation-history/session"] == 1
        assert client.usage().http_response_bytes > 0
    assert calls["/v3/btc/capabilities"] == 1


@pytest.mark.asyncio
async def test_native_async_uses_same_core_and_does_not_block_event_loop(endpoint):
    url, calls = endpoint
    ticks = []

    async def heartbeat():
        for _ in range(5):
            await asyncio.sleep(0)
            ticks.append(1)

    async with AsyncClient(url, api_key="local-test") as client:
        health, _ = await asyncio.gather(client.btc_health(), heartbeat())
        assert health.sources[0].source == 2
        async with client.history.observations(
            Source.CHAINLINK_TWAP60, start_time_ms=1000, end_time_ms=2000
        ) as pages:
            rows = [record.observation_id async for page in pages for record in page.observations]
        assert rows == [1, 3]
        assert ticks == [1] * 5
    assert calls["/v3/btc/observation-history/session"] == 1


def test_native_errors_keep_http_classification(endpoint):
    url, _ = endpoint
    from marketcow_sdk._native import NativeError

    with pytest.raises(NativeError) as failure:
        with Client(url, api_key="wrong"):
            pass
    assert failure.value.status == 401
    assert failure.value.code == "authentication_required"


def test_native_long_observations_segment_and_release_without_upgrading_coverage(endpoint):
    url, calls = endpoint
    with Client(url, api_key="local-test") as client:
        with client.history.observations(
            Source.CHAINLINK_TWAP60, start_time_ms=1, end_time_ms=2_000_000
        ) as stream:
            pages = list(stream)
        assert [r.observation_id for p in pages for r in p.observations] == [1, 3]
        assert len(pages) == 3
        assert [p.segment_index for p in pages] == [0, 1, 2]
        assert [p.complete for p in pages] == [False, False, True]
        assert not any(p.coverage_verified for p in pages)
        assert calls["/v3/btc/observation-history/session"] == 3


def test_native_full_facts_preserve_binding_provider_and_reading_identity(endpoint):
    url, _ = endpoint
    with Client(url, api_key="local-test") as client:
        market = client.markets.list()[0]
        assert market.binding.key.rules.revision == 7
        reference = client.reference(market).fact
        assert reference.schema_version == "marketcow.facts.reference.v1"
        assert reference.meta.key.rules.id == "rules-a"
        assert reference.meta.identity.id == "fact-a" and reference.meta.identity.revision == 3
        reading = reference.binance_opening.reading
        assert reading.identity.id == "reading-a" and reading.identity.revision == 4
        assert reading.interval_start_ms == 0  # Explicit zero is not absent.
        assert reading.supplier_report_time_ms is None
        assert reading.source_generations == ["epoch-a", "epoch-b"]
        assert reading.evidence[0].row_index == 0
        assert reading.selection_rule == "preopen_60s" and reading.verification == "verified"
        finality = client.finality(market).fact
        assert finality.meta.key.rules.id == "rules-a"
        assert finality.meta.key.condition_id == "condition-a"
        assert finality.durable_sequence == 91 and finality.request_id
        assert finality.provider_reads_requested == 2
        assert finality.provider_reads_completed_before_decision
        provider = finality.providers[0]
        assert (provider.provider_id, provider.chain_id, provider.collateral) == (
            "rpc-a",
            "137",
            "PUSD",
        )
        assert provider.ctf_address == "ctf-a" and provider.configuration.revision == 7
        assert provider.token_binding_verified and provider.deployment_code_verified
        assert provider.endpoint_identity_verified and provider.block.number == "42"
        assert finality.providers[1].status == "timed_out"
        assert finality.payouts[0].token_id == "up"
        with pytest.raises(AttributeError):
            provider.collateral = "invented"


def test_native_history_complete_is_not_install_and_release_can_follow_install(endpoint):
    url, calls = endpoint
    with Client(url, api_key="local-test") as client:
        job = client.history.trades_through(
            start_time_ms=1100, through_trade_id=3, limit=1, release_on_complete=False
        )
        with job:
            pages = list(job)
            page = pages[-1].fact
            assert page.complete and page.coverage_verified and page.missing_ranges == []
            assert page.range.through_trade_id == 3 and page.database_revision == 7
            assert pages[0].fact.predecessor.evidence.row_index == 0
            assert page.trades[0].buyer_is_maker is False
            assert page.database_health_at_freeze.last_success_sequence == 3
            before = job.lifecycle()
            assert before.retrieval_complete and not before.released
            assert before.release_receipt is None
            assert calls["/v3/btc/trade-history/session"] == 0
            # Consumer's complete live join + atomic install belongs HERE.
        after = job.lifecycle()
        assert after.released and after.release_error is None
        assert after.release_receipt.range.through_trade_id == 3
        assert after.release_receipt.request_id == page.request_id
        assert calls["/v3/btc/trade-history/session"] == 1


def test_native_failed_install_abandons_and_releases_without_claiming_complete(endpoint):
    url, calls = endpoint
    with Client(url, api_key="local-test") as client:
        job = client.history.trades_through(
            start_time_ms=1100, through_trade_id=3, limit=1, release_on_complete=False
        )
        with pytest.raises(ValueError, match="install rejected"):
            with job:
                next(job)
                raise ValueError("install rejected")
        state = job.lifecycle()
        assert not state.retrieval_complete and state.released
        assert calls["/v3/btc/trade-history/session"] == 1


@pytest.mark.asyncio
async def test_native_cancelled_context_releases_known_freeze(endpoint):
    url, calls = endpoint
    async with AsyncClient(url, api_key="local-test") as client:
        job = client.history.trades_through(
            start_time_ms=1100, through_trade_id=3, limit=1, release_on_complete=False
        )
        entered = asyncio.Event()

        async def consume():
            async with job:
                await anext(job)
                entered.set()
                await asyncio.Event().wait()

        task = asyncio.create_task(consume())
        await asyncio.wait_for(entered.wait(), timeout=2)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        state = await job.lifecycle()
        assert not state.retrieval_complete and state.released
        assert state.release_receipt.range.through_trade_id == 3
        assert calls["/v3/btc/trade-history/session"] == 1


def test_native_incomplete_history_exposes_original_typed_failure_and_releases(endpoint):
    from marketcow_sdk._native import NativeError

    url, calls = endpoint
    with Client(url, api_key="local-test") as client:
        job = client.history.trades_through(
            start_time_ms=1100, through_trade_id=4, limit=1, release_on_complete=False
        )
        with pytest.raises(NativeError) as failure:
            with job:
                next(job)
        error = failure.value
        assert error.code == "history_gap" and error.reason == "trade_id_gap"
        assert error.missing_ranges == [(4, 4)]
        assert error.page.range.through_trade_id == 4
        assert not error.page.coverage_verified
        assert error.page.database_health_at_freeze.last_success_sequence == 3
        assert error.page.request_id
        assert job.lifecycle().released and not job.lifecycle().retrieval_complete
        assert calls["/v3/btc/trade-history/session"] == 1


def test_archived_key_requests_do_not_require_current_discovery_or_open_feed(endpoint):
    from marketcow_sdk import NativeError

    url, calls = endpoint
    key = dict(
        market_id="ended-market",
        condition_id="condition-ended",
        window_id="old-window",
        start_at="2026-10-01T00:00:00Z",
        end_at="2026-10-01T00:05:00Z",
        up_token_id="old-up",
        down_token_id="old-down",
        rules_id="old-rules",
        rules_revision=9,
    )
    archived = Market.from_key(**key)
    assert archived.read_only and archived.stream_id == ""
    assert not calls  # Constructor is pure data, no implicit discovery/feed.
    restored = Market.from_proto_bytes(archived.proto_bytes())
    copied = Market.from_binding(archived.binding)
    assert restored.read_only and copied.read_only
    assert restored.binding.key.rules.id == "old-rules"
    with Client(url, api_key="local-test") as client:
        assert client.reference(restored).fact.meta.key.market_id == "ended-market"
        assert client.finality(copied).fact.meta.key.rules.revision == 9
        with pytest.raises(NativeError, match="data-only archived identity"):
            next(client.markets.subscribe(restored))
    assert calls["/v7/polymarket/btc-5m/market-streams"] == 0
    assert calls["/v7/polymarket/btc-5m/market-stream"] == 0
    with pytest.raises(NativeError, match="incomplete archived"):
        Market.from_key(**{**key, "rules_revision": 0})


def test_explicit_cold_proto_roundtrip_preserves_facts_and_does_not_imply_validation(endpoint):
    import sqlite3

    from marketcow_sdk import NativeError

    url, _ = endpoint
    with Client(url, api_key="local-test") as client:
        market = client.markets.list()[0]
        reference = client.reference(market)
        finality = client.finality(market)
    with sqlite3.connect(":memory:") as db:
        db.execute("CREATE TABLE facts (kind TEXT PRIMARY KEY, body BLOB NOT NULL)")
        db.executemany(
            "INSERT INTO facts VALUES (?, ?)",
            [("reference", reference.proto_bytes()), ("finality", finality.proto_bytes())],
        )
        saved_reference = db.execute("SELECT body FROM facts WHERE kind='reference'").fetchone()[0]
        saved_finality = db.execute("SELECT body FROM facts WHERE kind='finality'").fetchone()[0]
    restored = Reference.from_proto_bytes(saved_reference).fact
    assert restored.meta.key.rules.revision == 7
    assert restored.binance_opening.reading.price == "100.00"
    assert restored.binance_opening.reading.interval_start_ms == 0
    assert restored.binance_opening.reading.supplier_report_time_ms is None
    assert restored.binance_opening.reading.evidence[0].row_index == 0
    assert Finality.from_proto_bytes(saved_finality).fact.providers[0].collateral == "PUSD"
    view = type(restored).from_proto_bytes(restored.proto_bytes())
    assert view.request_id == restored.request_id
    assert type(restored).from_proto_bytes(b"").meta is None  # Decode is NOT verification.
    rules = Rules.from_proto_bytes(
        blob(1, blob(1, "rules-a") + integer(2, 7))
        + blob(2, "market-a")
        + blob(11, blob(1, "verified") + blob(3, "USD") + integer(5, 60))
    )
    assert Rules.from_proto_bytes(rules.proto_bytes()).fact.resolution.averaging_seconds == 60
    with pytest.raises(NativeError):
        Finality.from_proto_bytes(saved_reference)
    with pytest.raises(NativeError, match="wrong type"):
        Finality.from_proto_bytes(
            blob(1, "wrong.schema") + blob(3, blob(2, market.binding.key.proto_bytes()))
        )
    with pytest.raises(NativeError):
        Reference.from_proto_bytes(b"\xff")
    with pytest.raises(NativeError, match="8MiB"):
        Reference.from_proto_bytes(b"x" * (8 * 1024 * 1024 + 1))


def test_full_directory_keeps_source_budget_and_reuses_list_discovery(endpoint):
    url, calls = endpoint
    with Client(url, api_key="local-test") as client:
        directory = client.markets.directory()
        original = directory.fact
        assert directory.schema_version == original.schema_version
        assert original.markets[0].key.rules.revision == 7
        assert original.markets[0].role == "current"
        assert original.markets[0].stream_id == "stream-a"
        budget = directory.delivery_budget
        assert budget.maximum_frame_bytes == 8388608
        assert budget.maximum_state_bytes == original.delivery_budget.maximum_state_bytes
        assert budget.maximum_levels_per_side == 10000 and budget.maximum_changes == 10000
        assert client.markets.list()[0].rules_id == "rules-a"
        assert calls["/v7/polymarket/btc-5m/market-streams"] == 1


@pytest.mark.asyncio
async def test_async_full_directory_shares_discovery_request_with_list(endpoint):
    url, calls = endpoint
    async with AsyncClient(url, api_key="local-test") as client:
        a, b, markets = await asyncio.gather(
            client.markets.directory(), client.markets.directory(), client.markets.list()
        )
        assert a.fact.markets[0].key.rules.id == markets[0].rules_id
        assert a.delivery_budget.maximum_changes == b.fact.delivery_budget.maximum_changes
        assert calls["/v7/polymarket/btc-5m/market-streams"] == 1


def test_directory_failure_is_not_an_empty_list_and_absent_budget_is_not_fabricated(endpoint):
    from marketcow_sdk import NativeError

    url, calls = endpoint
    calls["directory_error"] = 1
    with Client(url, api_key="local-test") as client:
        with pytest.raises(NativeError) as failure:
            client.markets.directory()
        assert failure.value.status == 503 and failure.value.code == "manager_unavailable"
    calls["directory_error"] = 0
    calls["directory_empty"] = 1
    with Client(url, api_key="local-test") as client:
        directory = client.markets.directory()
        assert directory.fact.markets == [] and directory.delivery_budget is None
        assert directory.fact.delivery_budget is None
        with pytest.raises(NativeError, match="delivery budget missing"):
            client.markets.list()
