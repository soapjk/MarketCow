# 初始架构方案

## 一、设计原则

1. 数据服务只负责“事实、来源、版本和质量”，不负责投资结论。
2. 批量数据优先于逐标的网页抓取。
3. 原始层不可变，标准化层可重建，派生层可版本化。
4. 所有历史查询必须支持 `as_of`，财务数据必须按公告时间可见。
5. 任一免费来源都可能失效，provider 是可替换部件。

## 二、逻辑架构

```text
上游数据源
  ├─ 批量包：TDX 日线、Mootdx 财务 ZIP
  ├─ API/网页：BaoStock、AKShare、Sina、Tencent、Yahoo
  └─ 一手披露：CNInfo、SSE/SZSE/HKEX/SEC、BLS/BEA/Census
          │
          ▼
Provider adapters
  ├─ 请求、限速、重试、熔断
  ├─ 原始响应落档
  └─ schema 版本与来源健康度
          │
          ▼
Raw layer（JSON/ZIP/原始 Parquet，不可变）
          │
          ▼
Normalize + validate
  ├─ 统一证券 ID、币种、单位、复权口径
  ├─ 公告时间与修订版本
  └─ 双源比对、异常标记
          │
          ▼
Canonical Parquet + DuckDB
  ├─ instruments / aliases
  ├─ prices / corporate actions
  ├─ financial facts / statements
  ├─ estimates / events / macro
  └─ derived metrics / quality flags
          │
          ▼
Local API + maintenance CLI
  ├─ 全市场筛选器
  ├─ 本地行情监控应用
  └─ 其他研究、看板和回测项目
```

## 三、建议目录

```text
marketcow/
  src/marketcow/
    api/
    providers/
    normalize/
    quality/
    storage/
    jobs/
  schemas/
  migrations/
  tests/
    fixtures/
    contract/
  docs/
  data/                 # gitignore，本地运行时生成
    raw/
    canonical/
    warehouse/
```

## 四、核心数据模型

### 1. Instrument

- `instrument_id`：稳定内部 ID，例如 `CN.XSHG.600298`。
- `asset_type`、`exchange`、`currency`、`list_date`、`delist_date`。
- `symbol`、`name`、`status`。
- 多个 `instrument_alias`：新浪、东方财富、Yahoo、旧机器人 ID、中文简称和拼音。

### 2. PriceBar / QuoteObservation

- `instrument_id`、`interval`、`trade_date/time`。
- `open/high/low/close/volume/amount`。
- `adjustment`：`raw`、`forward`、`backward_total_return`。
- `source`、`observed_at`、`ingested_at`、`quality_status`。

历史日线与机器人采集型报价必须分表或分 observation type，不能把不连续采样误当成完整 tick 数据。

### 3. FinancialFact

- `instrument_id`、`statement`、`concept`、`value`、`unit/currency`。
- `report_period`、`period_type`、`consolidation_scope`。
- `published_at`、`observed_at`、`ingested_at`、`restated_at`。
- `source`、`source_record_id`、`raw_artifact_id`。

标准层保留来源原值；跨来源“共识值”应作为派生结果，并附差异率和选择理由。

### 4. Event / Estimate / Macro

事件允许日期修订；一致预期按采集日保留快照；宏观指标需要 release/vintage 概念，避免用后续修订值污染历史判断。

## 五、API 草案

只读查询：

```text
GET /v1/instruments
GET /v1/instruments/{id}
GET /v1/prices/bars
GET /v1/quotes/latest
GET /v1/financials/facts
GET /v1/financials/statements
GET /v1/estimates
GET /v1/events
GET /v1/macro/series
GET /v1/quality/issues
GET /v1/screen
```

运维写操作：

```text
POST /v1/admin/jobs/{job_name}/run
GET  /v1/admin/jobs
GET  /v1/admin/providers/health
```

所有时间序列查询都应支持 `as_of`、`source` 和 `adjustment`。默认绑定 `127.0.0.1`；管理接口使用本地令牌，并与公共查询路由分离。

## 六、存储选择

首期采用 Parquet + DuckDB：

- 适合 5,000 多只股票的列式扫描和全市场筛选。
- 文件可分区、易备份、无需常驻数据库服务。
- DuckDB 可直接查询 Parquet，也能给本地 API 提供 SQL 层。

SQLite 仅用于任务状态、provider 健康度或 legacy 导入，不作为长期全市场分析仓库。若未来出现多机写入、高并发或远程团队访问，再评估 PostgreSQL/对象存储，不在首期引入。

## 七、采集分层

### 每日全市场轻量层

- 证券状态、日线、估值、核心财务指标、分红股本事件。
- 用于漏斗召回和估值触发。

### 入围公司深度层

- 完整三张表、主营构成、非经常性损益、公告原文索引、一致预期。
- 只对漏斗入围公司或观察池执行，控制抓取量和失败面。

## 八、质量与可观测性

- provider 成功率、延迟、最近成功时间和字段漂移告警。
- 数据新鲜度、重复、缺失、单位异常、价格跳变、财务恒等式校验。
- 双源差异表：`source_a_value`、`source_b_value`、`difference_pct`、`resolution`。
- 每次任务生成 manifest：输入、适配器版本、行数、时间范围、异常和输出分区。
