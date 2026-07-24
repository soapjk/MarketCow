# Hyperliquid 公共市场数据

MarketCow 的 Hyperliquid 接入只读取公共市场数据，不持有钱包私钥，不读取账户，
也不调用下单接口。默认主网地址为 `https://api.hyperliquid.xyz`。

## 标的身份

为避免将 `BTC` 误识别成美股代码，所有调用必须使用带 venue 的标准代码：

```text
BTC-PERP.HYPL
ETH-PERP.HYPL
HYPE-USDC.HYPL
```

`HYPL` 是 MarketCow 内部 venue code，不表示正式 ISO MIC。永续的 provider symbol
是 coin 名称，例如 `BTC`；Spot 的 provider symbol 使用 HyperCore 稳定索引，例如
HYPE/USDC 当前为 `@107`。instrument master 同时保留 token ID、spot index 和显示名，
不以可能变化的 UI 名称作为唯一身份。

同步公开 instrument master：

```bash
curl -X POST http://127.0.0.1:8790/v1/admin/instruments/hyperliquid/refresh
```

## 报价与历史 K 线

```bash
curl 'http://127.0.0.1:8790/v1/quotes/BTC-PERP.HYPL?refresh=true&provider=hyperliquid'
```

后台拉取最近一个月的 BTC 永续小时线：

```bash
curl -X POST http://127.0.0.1:8790/v1/admin/history-jobs \
  -H 'content-type: application/json' -d '{
    "symbols":["BTC-PERP.HYPL"],
    "provider":"hyperliquid",
    "range":"1mo",
    "interval":"1h",
    "adjustment":"raw",
    "allow_fallback":false,
    "max_concurrency":1,
    "max_attempts":3,
    "retry_backoff_seconds":1,
    "canonical_wait_seconds":5,
    "idempotency_key":"hyperliquid-btc-1h-20260724"
  }'
```

支持 interval：`1m`、`3m`、`5m`、`15m`、`30m`、`1h`、`2h`、`4h`、
`8h`、`12h`、`1d`、`3d`、`1w`、`1M`。Crypto 不存在复权语义，
`adjustment` 必须为 `raw`。

Hyperliquid API 每次时间范围查询最多返回有限批次，且只保留最近 5000 根 candle。
MarketCow 会持久化已取得的数据，但完整分钟历史必须靠定时后台任务持续采集，不能在
很久以后仅靠 API 临时回补。

## 实时流

现有 `/v1/market-data/stream` 契约同时支持 LongPort 和 Hyperliquid。instrument
带 `.HYPL` 时自动走 Hyperliquid WebSocket：

- `quote` / `order_book` 使用公开 `bbo`；
- `trade` 使用公开 `trades`；
- `bar` 由 MarketCow 对 trade 聚合为一分钟 bar。

同一 WebSocket 客户端可以继续订阅股票；路由层按 instrument venue 分发，不会把
Hyperliquid symbol 发送给 LongPort。

永续 quote 快照同时保存 `mark_price`、`oracle_price`、`funding_rate` 和
`open_interest`；quote 的完整 payload 会进入 ClickHouse。历史资金费率通过专用
接口读取，并把原始响应登记到 raw artifact：

```bash
curl 'http://127.0.0.1:8790/v1/hyperliquid/BTC-PERP.HYPL/funding-history?start=2026-07-01T00:00:00Z&end=2026-07-24T00:00:00Z'
```

## 配置

```text
MARKETCOW_HYPERLIQUID_BASE_URL=https://api.hyperliquid.xyz
MARKETCOW_HYPERLIQUID_TIMEOUT_SECONDS=3
MARKETCOW_HYPERLIQUID_REQUEST_BUDGET_SECONDS=10
```

这些配置不包含凭据。测试网可通过显式覆盖 base URL 使用，生产环境不做隐式切换。
