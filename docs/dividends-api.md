# 股息数据 API

`GET /v1/dividends/{symbol}?fiscal_year=2026` 返回指定年度已公告的逐笔每股股息、
股权登记日、除息日、实际派发日、确认状态，以及上一完整年度的每股股息合计。

每个 `announcements[]` 统一包含：

- `record_date`：股权登记日。
- `ex_date`：除息/除权日。
- `payment_date`：来源明确给出的实际派发日。
- `expected_payment_date`：兼容旧客户端的字段。来源给出 `payment_date` 时两者相同；
  旧记录如果只有预计日期，`payment_date` 仍为 `null`，不会把预计日期冒充为实际日期。
- `date_evidence`：按上述三个日期分别记录 `value`、`source_name`、`source_url`、
  `source_document_id`、`verification_status`、`source_priority`、
  `selection_policy` 和 `missing_reason`。

日期均为 ISO `YYYY-MM-DD` 或 `null`。服务不会用派发日推断登记日或除息日，也不会
用相邻交易日猜测。日期缺失时，`date_evidence.<field>.missing_reason` 明确记录
`<field>_not_provided_by_source`。

## 批量查询

持仓类调用方应使用 `POST /v1/dividends/query`，一次提交最多 50 个代码和一个财年：

```bash
curl -X POST 'http://127.0.0.1:8790/v1/dividends/query' \
  -H 'Content-Type: application/json' \
  -d '{
    "symbols": ["600519.SH", "0700.HK", "AAPL"],
    "fiscal_year": 2026
  }'
```

```json
{
  "fiscal_year": 2026,
  "requested_count": 3,
  "completed_count": 2,
  "items": [
    {
      "requested_symbol": "600519.SH",
      "symbol": "600519.SH",
      "status": "available",
      "cache_status": "fresh",
      "error": null,
      "data": {
        "symbol": "600519.SH",
        "fiscal_year": 2026,
        "announcements": [{
          "amount_per_share": 28.02423,
          "currency": "CNY",
          "record_date": "2026-06-25",
          "ex_date": "2026-06-26",
          "payment_date": "2026-06-26",
          "expected_payment_date": "2026-06-26",
          "date_evidence": {
            "record_date": {
              "value": "2026-06-25",
              "source_name": "LongPort OpenAPI",
              "verification_status": "unverified",
              "missing_reason": null
            }
          }
        }],
        "announced_count": 1,
        "amount_per_share_total": 28.02423,
        "data_status": "fresh"
      }
    },
    {
      "requested_symbol": "0700.HK",
      "symbol": "00700.HK",
      "status": "refreshing",
      "cache_status": "refreshing",
      "error": null,
      "data": {
        "symbol": "00700.HK",
        "fiscal_year": 2026,
        "data_status": "refreshing"
      }
    },
    {
      "requested_symbol": "AAPL",
      "symbol": "AAPL",
      "status": "error",
      "cache_status": null,
      "error": "upstream temporarily unavailable",
      "data": null
    }
  ]
}
```

批量结果严格保持请求顺序；等价代码（例如 `0700.HK` 和 `00700.HK`）只执行一次
服务查询，但各自保留一个响应项。单项 `data` 与相同参数的单标的 GET 完全相同。

`status` 取值：

- `available`：缓存新鲜且存在股息记录。
- `unavailable`：查询成功，但当前没有记录。
- `refreshing`：返回现有数据，同时刷新正在进行。
- `stale`：返回过期数据，刷新处于失败退避或尚未重新调度。
- `error`：代码无效、上游异常或超过批量截止时间；不会导致整批 HTTP 失败。

服务默认最多并发处理 8 个证券，整个批次截止时间为 15 秒；分别通过
`MARKETCOW_DIVIDEND_BATCH_WORKERS` 和
`MARKETCOW_DIVIDEND_BATCH_TIMEOUT_SECONDS` 配置。服务端硬限制为 32 个 worker、
60 秒截止时间。建议 Investrace 为该批量请求配置 20 秒客户端超时，不再为每个证券
设置独立的 8 秒串行超时。

接口版本为 `v1`，实现位于 `src/marketcow/api.py` 的 `dividends_query`；
业务数据和缓存语义继续复用 `FundamentalService.get_dividends`。原有
`GET /v1/dividends/{symbol}` 保持兼容。

调用方只需使用该 GET 接口，无需先刷新数据。服务采用缓存优先策略：

- 缓存新鲜时直接返回，`data_status=fresh`。
- 已有缓存过期时立即返回旧数据并在后台触发单次刷新，
  `data_status=refreshing`；刷新失败时继续返回旧数据并标记为 `stale`。
- 首次查询没有任何缓存时，服务会在当前请求中获取结构化快速数据并保存后返回。
- `last_refreshed_at` 表示最近一次成功刷新时间；即使官方来源没有返回任何公告，
  成功的空结果也会被缓存，避免每次请求重复访问上游。

A 股和港股的首次查询使用结构化快速源，不再等待 PDF：

- A 股优先读取 Tushare `dividend`，以 `end_date` 作为报告财年；Tushare 未配置、
  请求失败或没有记录时回退到 Longport。
- 港股读取 Longport `FundamentalContext.dividend`。
- 美股股票和 ETF 在 Longport 可用时也可取得结构化历史事件的三类日期；SEC
  申报解析仅保存正文明确出现的日期，缺少的日期保持 `null`。
- Longport 没有提供发行人报告财年和公告日期，因此按支付年度匹配请求年份，
  并以除权日作为排序日期；这些推导依据会保存在 `payload_json`。
- 所有结构化第三方记录写为 `unverified`。线上服务当前不下载或解析交易所 PDF，
  也不启动后台 PDF 核验。

缓存有效期、失败重试间隔和后台刷新并发数分别由
`MARKETCOW_DIVIDEND_CACHE_TTL_SECONDS`、
`MARKETCOW_DIVIDEND_EMPTY_CACHE_TTL_SECONDS`、
`MARKETCOW_DIVIDEND_REFRESH_RETRY_SECONDS` 和
`MARKETCOW_DIVIDEND_REFRESH_WORKERS` 配置。

有数据成功和空结果成功使用不同缓存状态：

- `success_data`：来源查询完成并取得至少一个事件，使用普通缓存 TTL（默认 6 小时）。
- `success_empty`：来源查询完成但没有事件，使用独立负缓存 TTL（默认 15 分钟）。
- `failed_rate_limited`、`failed_timeout`、`failed_parse`、`failed_source`：
  分别表示限流、超时、解析失败和其他来源失败。失败不会更新 `last_success_at`，
  也不会作为 `fresh/unavailable` 返回；如有旧数据只能标记为 `stale`。

响应额外提供 `refresh_status`、`query_source`、`refresh_completed_at`、
`cache_schema_version` 和 `parser_version`，便于调用方审计来源、完成状态和缓存版本。
数据库状态采用 `dividend-cache-v2` 与 `structured-v6-payment-year`。migration 10 会把旧
`success` 标记迁移为旧版本 `success_empty`、旧 `failed` 标记为
`failed_source`，但保留原始时间和错误；由于版本不匹配，这些记录不会命中新缓存。
旧版本空缓存会在下一次普通 GET 或批量查询时同步重新抓取，旧版本有数据缓存会作为
stale 返回并触发后台刷新。因此 Investrace 无需逐标的调用管理员强刷。

`structured-v6-payment-year` 还包含以下完整度修复：

- Longport Fundamental 请求在进程内全局串行限速，遇到 429 做有上限退避重试；
  可通过 `MARKETCOW_DIVIDEND_LONGPORT_MIN_INTERVAL_SECONDS`（默认 0.65 秒）和
  `MARKETCOW_DIVIDEND_LONGPORT_MAX_ATTEMPTS`（默认 3）调整。
- Longport history/detail 对同一支付日返回不同小数精度时，优先保留带稳定事件 ID
  的历史记录，API 汇总也会兼容折叠数据库里已经存在的旧重复行。
- 美股 Longport 失败且 SEC 没有取得事件时保留原始失败，不再降级成成功空结果。
- A 股场内基金使用 Tushare `fund_div`，按支付年度映射登记日、除息日和派发日；
  A 股股票继续使用 `dividend`。
- JSONB 使用原始 UTF-8 编码，兼容生产 SQL_ASCII 数据库，避免中文状态或来源证据
  被 `\\uXXXX` 转义拒绝。

版本升级后，既有状态会自动失效并按普通查询刷新，因此 0700.HK、
1024.HK、2400.HK、600036.SH、600519.SH 等旧事件可用 Longport 稳定事件 ID
原位补齐三日期，不要求调用方执行人工强刷。

所有 provider 的 API 年度统一采用 `payment_date` 所在自然年。Tushare 的
`end_date` 只作为发行人报告期资料保留在原始 payload，不再决定事件归属年度；
这避免报告期为 2025、实际在 2026 支付的修订记录被重复计入 2025。

`confirmed_amount_per_share_total` 只汇总 `confirmed` 记录。
`amount_per_share_total` 汇总当前最佳可用记录，包括结构化接口返回的 `unverified`
记录；`total_is_fully_confirmed` 表示该合计是否全部经过官方确认。调用方需要及时
可用的股息合计时应使用 `amount_per_share_total`。
`previous_complete_year` 明确标记为 `is_estimate_basis=true`，供调用方作为估算基线；
它不是当前年度已公告股息。

请求字段 `fiscal_year` 保留兼容命名：Longport 历史路径按 `payment_date` 所在年份
筛选，等价于 `payment_year`；Tushare A 股路径按 `end_date`（发行人报告期）筛选；
SEC 路径按申报年份筛选。调用方若要计算某自然年已入账现金，应以事件
`payment_date` 筛选，不能只依赖不同来源含义不完全相同的 `fiscal_year`。

Investrace 应使用 `record_date` 当日或之前的最近一份不可变持仓快照计算权益数量，
税费当前按 0。若 `record_date` 为 `null`，或者历史快照晚于 `record_date`，必须
降级为“无法计算/历史不足”，不得使用当前持仓或 `payment_date` 代替。

本地导入使用 `POST /v1/admin/dividends/ingest`。每条记录必须提供：

- `symbol`、`fiscal_year`、`amount_per_share`、`currency`
- `announcement_date`；可选 `record_date`、`ex_date`、`payment_date`、
  `expected_payment_date` 和 `date_evidence`
- `confirmation_status`：`confirmed` 或 `unverified`
- `source_type`：`fund_manager`、`issuer_announcement`、`exchange_announcement`、
  `ir_filing`、`regulatory_filing` 或 `third_party`
- 已确认记录必须包含 `source_url` 和 `source_document_id`

数据库约束和服务校验都会拒绝将 `third_party` 记录标记为 `confirmed`。第三方行情只适合
发现或交叉验证，核验前必须保持 `unverified`。

同一证券、财年、公告日、每股金额、币种和实际/预计派发日组成同一分红事件。重复来源按以下
优先级保留：基金管理人/上市公司公告、交易所公告、IR/监管申报、第三方。低优先级来源
不能覆盖高优先级来源；同优先级取较新的 observation，已确认记录优先于未核验记录。
最终胜出的来源、优先级和选择规则保存在每个日期的 `date_evidence` 中，可供审计。

证券代码会标准化为稳定标识，例如 `600519.SS` → `600519.SH`、
`700.HK` → `00700.HK`、`BRK.B` → `BRK-B`。

管理员仍可在排障或紧急更新时强制刷新：

```text
POST /v1/admin/dividends/AAPL/refresh?fiscal_year=2026
```

管理员刷新始终绕过 fresh、stale、负缓存和失败退避。在同一 symbol/year 的 single-flight
锁内先完成事件入库，再一次写入完整的成功状态（状态、来源、结果数、版本及完成时间）；
上游失败则只写失败状态，不写成功空缓存。

该流程查询 SEC 官方 ticker/submissions 数据并读取 8-K、8-K/A、6-K、6-K/A
原始申报。只有同一申报中同时解析到每股金额和明确派发日时才写入 `confirmed`；
缺少派发日的 Company Facts 聚合值不会被冒充为确定性公告。SEC 请求身份由
`MARKETCOW_SEC_USER_AGENT` 配置。

同一个刷新接口也支持 A 股和港股；它只强制刷新结构化快速源：

```text
POST /v1/admin/dividends/600519.SH/refresh?fiscal_year=2025
POST /v1/admin/dividends/000001.SZ/refresh?fiscal_year=2025
POST /v1/admin/dividends/00700.HK/refresh?fiscal_year=2025
```

原有 A 股和港股官方 PDF provider 暂时保留为离线核验代码，但不接入线上请求或
后台刷新链路。

A 股第三方候选发现：

```text
POST /v1/admin/dividends/600519.SH/discover?fiscal_year=2025
```

该接口读取 Tushare `dividend` 候选，但无条件写为 `unverified`，只用于发现和
交叉验证，不能进入已确认合计。
