"""Opt-in, bounded Binance SPOT data-only worker for Nautilus 1.231.0.

The isolated process installs a scoped raw callback wrapper; no third-party
source is changed and no execution client is registered.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import time
import threading
from datetime import datetime, timezone
from pathlib import Path

from .btc_fact_log import FactLog
from .btc_continuity import Continuity
from .btc_backfill import KlineBackfill


def decode_raw(raw: bytes, *, processed_at: str, monotonic_ns: int, maximum_bytes: int) -> dict | None:
    if len(raw) > maximum_bytes:
        raise ValueError("raw_size_exceeded")
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError("duplicate_json_key")
            result[key] = value
        return result
    msg = json.loads(raw, object_pairs_hook=pairs,
                     parse_constant=lambda _: (_ for _ in ()).throw(ValueError("invalid_json_number")))
    if not isinstance(msg, dict):
        raise ValueError("invalid_wire")
    data = msg.get("data")
    if data is None and "result" in msg:
        return None
    if not isinstance(data, dict) or data.get("s") != "BTCUSDT":
        raise ValueError("wrong_instrument")
    event = data.get("e")
    if event not in {"trade", "aggTrade", "kline"}:
        return None
    if type(data.get("E")) is not int:
        raise ValueError("missing_event_time")
    final = None
    if event == "kline":
        kline = data.get("k", {})
        if kline.get("s") != "BTCUSDT" or kline.get("i") not in {"1m", "1h"} or type(kline.get("x")) is not bool:
            raise ValueError("invalid_kline_identity")
        final = kline["x"]
    return {
        "schema_version": "marketcow.btc-hourly.raw-event.v1",
        "source": "binance_global_spot", "source_version": "nautilus_1.231.0_raw_callback",
        "instrument_id": "BTCUSDT.BINANCE", "event_type": event,
        "exchange_at_ms": data["E"], "final": final,
        # Nautilus invokes this wrapper at the first application-owned raw
        # callback boundary.  This is not the kernel/socket receive time, but
        # it is an observed receive boundary and must not be erased as unknown.
        "first_received_at": processed_at,
        "received_monotonic_ns": monotonic_ns,
        "adapter_processed_at": processed_at,
        "adapter_monotonic_ns": monotonic_ns,
        "raw_sha256": hashlib.sha256(raw).hexdigest(), "raw_utf8": raw.decode("utf-8"),
        "missing_reasons": ["socket_kernel_receive_time_unknown", "source_continuity_not_yet_verified"],
    }


def run(args) -> None:
    import nautilus_trader
    if nautilus_trader.__version__ != "1.231.0":
        raise ValueError("unsupported_nautilus_version")
    from nautilus_trader.adapters.binance import BinanceAccountType, BinanceDataClientConfig, BinanceLiveDataClientFactory
    from nautilus_trader.adapters.binance.common.enums import BinanceEnvironment
    from nautilus_trader.adapters.binance.data import BinanceCommonDataClient
    from nautilus_trader.common.actor import Actor
    from nautilus_trader.config import InstrumentProviderConfig, LoggingConfig, TradingNodeConfig
    from nautilus_trader.live.node import TradingNode
    from nautilus_trader.model.data import BarType
    from nautilus_trader.model.identifiers import InstrumentId, TraderId

    instrument = InstrumentId.from_str("BTCUSDT.BINANCE")
    config = TradingNodeConfig(
        trader_id=TraderId("MC-DATA-001"), logging=LoggingConfig(log_level="ERROR"),
        data_clients={"BINANCE": BinanceDataClientConfig(
            api_key=None, api_secret=None, account_type=BinanceAccountType.SPOT,
            environment=BinanceEnvironment.LIVE, us=False, proxy_url=args.proxy_url,
            instrument_provider=InstrumentProviderConfig(load_ids=frozenset([instrument])),
        )}, exec_clients={}, timeout_connection=20.0, timeout_disconnection=10.0,
    )
    if args.check_only:
        print(json.dumps({"version": nautilus_trader.__version__, "module": nautilus_trader.__file__,
                          "execution_clients": len(config.exec_clients), "product": "SPOT",
                          "instrument": str(instrument), "network_started": False}))
        return
    continuous = getattr(args, "continuous", False)
    if not continuous and (args.seconds is None or args.seconds <= 0 or args.seconds > 1800):
        raise ValueError("seconds_must_be_1_to_1800")
    if getattr(args, "read_port", None) is not None and not 1 <= args.read_port <= 65535:
        raise ValueError("invalid_read_port")
    log = FactLog(args.output, maximum_pending=10000, maximum_pending_bytes=64 * 1024 * 1024,
                  maximum_disk_bytes=args.maximum_disk_bytes, maximum_subscribers=4)
    node = TradingNode(config=config)
    failure = []
    original = BinanceCommonDataClient._handle_ws_message
    server = None
    server_thread = None
    continuity = Continuity()
    if log.recovered_continuity is not None:
        continuity.restore(log.recovered_continuity)

    def repair_completed(entity, start, end):
        repaired = continuity.repair_completed(entity, start, end)
        log.publish({"schema_version": "marketcow.binance-continuity-repair.v1",
                     "entity": entity, "repaired": repaired,
                     "continuity_checkpoint": continuity.checkpoint()})
        return repaired

    repairs = KlineBackfill(log.publish, completed=repair_completed)

    def capture(client, raw):
        try:
            fact = decode_raw(raw, processed_at=datetime.now(timezone.utc).isoformat(),
                              monotonic_ns=time.monotonic_ns(), maximum_bytes=8 * 1024 * 1024)
            if fact is not None:
                try:
                    fact["stream_quality"] = continuity.observe(json.loads(raw)["data"])
                except (ValueError, KeyError, TypeError) as exc:
                    fact["stream_quality"] = {"apply_to_live": False,
                                              "reason": "invalid_entity_data",
                                              "error_type": type(exc).__name__}
                fact["continuity_checkpoint"] = continuity.checkpoint()
                log.publish(fact)
                if not fact["stream_quality"]["apply_to_live"]:
                    return
                quality = fact["stream_quality"]
                if quality.get("gap") and quality.get("entity") in ("1m", "1h"):
                    async def fetch(interval, start, end, limit):
                        import msgspec
                        from nautilus_trader.adapters.binance.common.enums import BinanceKlineInterval
                        rows = await client._http_market.query_klines(
                            symbol="BTCUSDT", interval=BinanceKlineInterval(interval),
                            start_time=start, end_time=end, limit=limit)
                        return msgspec.to_builtins(rows)
                    repairs.submit(quality["entity"], quality["gap"], fetch)
        except Exception as exc:
            if not failure:
                failure.append(type(exc).__name__)
            node.stop()
            return
        original(client, raw)

    class Collector(Actor):
        def on_start(self):
            self.subscribe_trade_ticks(instrument)
            for interval in ("1-MINUTE", "1-HOUR"):
                self.subscribe_bars(BarType.from_str(f"{instrument}-{interval}-LAST-EXTERNAL"))

        def on_bar(self, bar):
            pass  # Raw callback archives both final and in-progress exchange updates.

        def on_trade_tick(self, tick):
            pass

    BinanceCommonDataClient._handle_ws_message = capture
    try:
        if getattr(args, "read_port", None) is not None:
            import uvicorn
            from .btc_research_read import create_read_app
            app = create_read_app(args.output, maximum_records=1000,
                                  maximum_bytes=8 * 1024 * 1024, live_log=log)
            server = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1",
                                                   port=args.read_port, log_level="warning"))
            server_thread = threading.Thread(target=server.run, name="btc-read-api", daemon=True)
            server_thread.start()
            ready_deadline = time.monotonic() + 5
            while not server.started:
                if not server_thread.is_alive() or time.monotonic() >= ready_deadline:
                    raise RuntimeError("read_api_start_failed")
                time.sleep(.01)
        node.trader.add_actor(Collector())
        node.add_data_client_factory("BINANCE", BinanceLiveDataClientFactory)
        node.build()
        loop = node.get_event_loop()
        if not continuous:
            loop.call_later(args.seconds, node.stop)
        def monitor():
            error = log.status()["error"]
            if server_thread is not None and not server_thread.is_alive():
                error = error or "read_api_exited"
            if error:
                if not failure:
                    failure.append(error)
                node.stop()
            elif not log.stopped:
                loop.call_later(1, monitor)
        loop.call_later(1, monitor)
        node.run()
    finally:
        BinanceCommonDataClient._handle_ws_message = original
        repairs.closed = True
        repairs.pending.clear()
        for task in list(repairs.tasks.values()):
            task.cancel()
        node.dispose()
        if server is not None:
            server.should_exit = True
            server_thread.join(10)
        log.close(10)
        if server_thread is not None and server_thread.is_alive():
            raise RuntimeError("read_api_shutdown_timeout")
    if failure:
        raise RuntimeError("capture_failed:" + failure[0])
    print(json.dumps(log.status()))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    duration = parser.add_mutually_exclusive_group(required=True)
    duration.add_argument("--seconds", type=int)
    duration.add_argument("--continuous", action="store_true", help="Run until stopped or a bounded resource fails")
    parser.add_argument("--maximum-disk-bytes", type=int, required=True)
    parser.add_argument("--proxy-url")
    parser.add_argument("--check-only", action="store_true")
    parser.add_argument("--read-port", type=int, help="Optional loopback HTTP/WS read listener")
    run(parser.parse_args())


if __name__ == "__main__":
    main()
