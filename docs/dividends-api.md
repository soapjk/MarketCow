# 股息数据 API

`GET /v1/dividends/{symbol}?fiscal_year=2026` 返回指定财年已公告的逐笔每股股息、
预计派发日、确认状态，以及上一完整财年的已确认每股股息合计。

`confirmed_amount_per_share_total` 只汇总 `confirmed` 记录。
`previous_complete_year` 明确标记为 `is_estimate_basis=true`，供调用方作为估算基线；
它不是当前年度已公告股息。

本地导入使用 `POST /v1/admin/dividends/ingest`。每条记录必须提供：

- `symbol`、`fiscal_year`、`amount_per_share`、`currency`
- `announcement_date`，可选 `expected_payment_date`
- `confirmation_status`：`confirmed` 或 `unverified`
- `source_type`：`fund_manager`、`issuer_announcement`、`exchange_announcement`、
  `ir_filing`、`regulatory_filing` 或 `third_party`
- 已确认记录必须包含 `source_url` 和 `source_document_id`

数据库约束和服务校验都会拒绝将 `third_party` 记录标记为 `confirmed`。第三方行情只适合
发现或交叉验证，核验前必须保持 `unverified`。

同一证券、财年、公告日、每股金额、币种和预计派发日组成同一分红事件。重复来源按以下
优先级保留：基金管理人/上市公司公告、交易所公告、IR/监管申报、第三方。低优先级来源
不能覆盖高优先级来源。

证券代码会标准化为稳定标识，例如 `600519.SS` → `600519.SH`、
`700.HK` → `00700.HK`、`BRK.B` → `BRK-B`。

美股官方刷新可调用：

```text
POST /v1/admin/dividends/AAPL/refresh?fiscal_year=2026
```

该流程查询 SEC 官方 ticker/submissions 数据并读取 8-K、8-K/A、6-K、6-K/A
原始申报。只有同一申报中同时解析到每股金额和明确派发日时才写入 `confirmed`；
缺少派发日的 Company Facts 聚合值不会被冒充为确定性公告。SEC 请求身份由
`MARKETCOW_SEC_USER_AGENT` 配置。

同一个刷新接口也支持 A 股和港股：

```text
POST /v1/admin/dividends/600519.SH/refresh?fiscal_year=2025
POST /v1/admin/dividends/000001.SZ/refresh?fiscal_year=2025
POST /v1/admin/dividends/00700.HK/refresh?fiscal_year=2025
```

- 上交所：查询官方公司/基金公告并读取交易所 PDF；针对上交所静态站的机器人
  限制使用其官方 Big5 镜像读取同一公告。
- 深交所：查询官方 `announcement/annList` 并读取 `disc.static.szse.cn` PDF。
- 港交所：通过 HKEXnews 股票代码前缀查询取得 `stockId`，筛选
  `Dividend or Distribution (Announcement Form)` 并解析标准公告表。
- A 股只有权益分派实施公告，或带明确发放日的基金利润分配公告，才会确认为
  `confirmed`。预案、股东会方案及缺少发放日的公告不会确认。
- HKEX Revised 公告按同一报告期和股息类型覆盖旧版本；Cancelled 版本从合计中排除。

官方 PDF 或 SEC HTML 会按 SHA-256 保存到本地 raw artifact，并把
`raw_artifact_id` 写回股息事件。

A 股第三方候选发现：

```text
POST /v1/admin/dividends/600519.SH/discover?fiscal_year=2025
```

该接口读取 Tushare `dividend` 候选，但无条件写为 `unverified`，只用于发现和
交叉验证，不能进入已确认合计。
