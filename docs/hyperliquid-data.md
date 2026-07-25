# Hyperliquid 公共市场数据

MarketCow 的 Hyperliquid 接入只读取公共市场数据，不持有钱包私钥，不读取账户，
也不调用下单接口。默认主网地址为 `https://api.hyperliquid.xyz`。

## 标的身份

为避免将 `BTC` 误识别成美股代码，所有调用必须使用带 venue 的标准代码：

```text
BTC-PERP.HYPL
ETH-PERP.HYPL
HYPE-USDC.HYPL
AAPL-PERP.XYZH
```

`HYPL` 是 MarketCow 内部 venue code，不表示正式 ISO MIC。永续的 provider symbol
是 coin 名称，例如 `BTC`；Spot 的 provider symbol 使用 HyperCore 稳定索引，例如
HYPE/USDC 当前为 `@107`。instrument master 同时保留 token ID、spot index 和显示名，
不以可能变化的 UI 名称作为唯一身份。

HIP-3 builder DEX 使用独立四位 MarketCow venue code。比如 provider symbol
`xyz:AAPL` 映射为 `AAPL-PERP.XYZH`；`XYZH` 不是 ISO MIC，也不掩盖真实的
`dex=xyz` 与 provider symbol。股票永续使用 `equity_perpetual` /
`equity_derivative`，不会被错误归类为 crypto。目录刷新会查询 `perpDexs` 及
逐 DEX `metaAndAssetCtxs`，退市标的保留身份并标记状态。HIP-3 目录本身不提供
可靠的股票/商品分类；MarketCow 只把经过明确核验的合约标成
`equity_perpetual`，其余使用 `hip3_perpetual/other_derivative`，不得仅凭同名代码
自动建立股票关系。

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
    "retry_max_backoff_seconds":30,
    "retry_jitter_seconds":0.5,
    "retry_budget_seconds":120,
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

- `quote` / `order_book` 使用公开 `l2Book`，标准深度为
  1、5、10 或 20 档；
- `trade` 使用公开 `trades`；
- `asset_context` 使用 `activeAssetCtx`，包含 mark、oracle、external oracle、
  funding、open interest 和可审计的 oracle 状态；
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

## 跨市场比较

MarketCow 只提供客观行情和基差，不返回交易方向、建议仓位或预期利润。严格同标的
映射可查询：

```bash
curl 'http://127.0.0.1:8790/v1/instrument-relationships?derivative_instrument_id=AAPL-PERP.XYZH&underlying_instrument_id=AAPL.XNAS'
```

批量同步快照：

```bash
curl -X POST http://127.0.0.1:8790/v1/cross-market/snapshots/query \
  -H 'content-type: application/json' -d '{
    "pairs":[{
      "derivative_instrument_id":"AAPL-PERP.XYZH",
      "underlying_instrument_id":"AAPL.XNAS"
    }],
    "book_depth":20,
    "max_age_ms":1000,
    "max_skew_ms":250
  }'
```

响应保留两边盘口、mark/oracle/funding、顶层容量、数据年龄、时间偏差及
`gross_basis`。它不扣除账户手续费、借券费或资金成本。

LongPort Depth 没有交易所事件时间，因此 MarketCow 分别记录：

- `book_received_at` / `underlying_book_age_ms`：盘口到达 MarketCow 的时间；
- `quote_event_at` / `underlying_status_age_ms`：LongPort Quote 的 provider 时间；
- `trade_status` / `session`：Quote 提供的证券状态与交易时段。

Quote 状态为 active、session 可交易、盘口与状态均未过期、跨市场偏差未超限且
Hyperliquid/oracle 正常时，`usable_for_immediate_hedge=true`。Quote 状态不可用或
过期时明确返回 `underlying_session_unverified`、`underlying_not_tradable` 或
`stale_market_data`。休市时 LongPort 可能没有双边 Depth；此时仍返回带 Quote
状态的受限快照，基差为 null，并标记 `insufficient_depth`，而不是伪装成 provider
故障。这个字段只表示行情数据足以进行即时对冲评估，不代表账户、
借券或交易权限可用。
