# ADR-002：拆分事务型数据与大规模行情时序存储

状态：已采纳

日期：2026-07-20

## 背景

ADR-001 选择 Parquet + DuckDB 作为首期本地分析存储。该选择仍适用于单机研究、批量扫描和早期数据量，但 MarketCow 的目标已经扩展为持续接收全 A 股分钟行情、保存多个来源并服务多个本地下游。

基本面和行情数据具有不同的生命周期与访问模式：

- 基本面、财务报表、证券主数据、任务和溯源记录总量相对有限，需要事务、唯一约束、版本修订、JSON 动态字段和并发更新。
- 分钟 K 线、行情观测以及未来的 Tick/盘口数据为追加型时序数据，需要高吞吐批量写入、按时间分区、列式压缩、长区间扫描和全市场横截面聚合。

按 5,500 个证券、每日 240 根一分钟 K 线、每年 250 个交易日估算，十年单来源上限约为 33 亿行。保存多个来源、在线副本和备份后将进入 TB 级规模。DuckDB 原生文件模式也不适合作为多进程服务的长期唯一写库。

## 决策

长期目标架构采用 PostgreSQL + ClickHouse + 原始文件/Parquet，并保留 DuckDB 作为离线分析引擎：

```text
上游 Provider
     │
     ▼
有界缓冲 / 微批写入
     │
     ├──────────────► PostgreSQL
     │                 基本面 / 财务 / 主数据 / 任务 / 溯源
     │
     ├──────────────► ClickHouse
     │                 多源原始行情 / 统一 K 线 / 聚合行情
     │
     └──────────────► 原始文件与 Parquet
                       完整响应 / 冷数据 / 可重放归档
                              │
                              ▼
                           DuckDB
                       本地研究 / 回测 / 质量检查
```

这是一项目标架构决策，不要求一次性替换当前 DuckDB。迁移必须通过存储接口逐步完成，并保持现有 HTTP API 契约稳定。

## 数据库职责

### PostgreSQL

PostgreSQL 是关系型事实、控制面和溯源元数据的权威存储，承载：

- 证券、交易所、代码别名和公司主数据；
- 基本面快照、完整财务报表、公告与修订历史；
- point-in-time 版本和跨来源校验结果；
- 经济日历、财报日历和宏观指标；
- Provider 健康状态、采集任务、请求记录和 schema migration；
- 原始 Artifact manifest、权限和服务配置；
- Tushare 等动态接口的完整行字段，使用 JSONB 保存未标准化字段。

基本面记录必须保留 `published_at`、`observed_at`、`ingested_at`、`source` 和 `raw_artifact_id`。

### ClickHouse

ClickHouse 是在线行情时序存储，承载：

- 一分钟 K 线以及必要的 Tick、逐笔和盘口数据；
- 不同来源的原始标准化行情；
- 对外默认使用的 canonical 行情；
- 从一分钟数据生成的多周期聚合；
- 大时间范围和全市场横截面分析。

采集器不得逐行提交。分钟行情按全市场每分钟一个批次或按 1～5 秒微批写入；单批建议为数千到数万行。写入失败必须进入本地 WAL/spool 并可重放。

### 原始文件与 Parquet

完整上游响应按请求批次保存，数据库行通过 `raw_artifact_id` 定位原始数据。不得为每根 K 线重复复制大型响应 JSON。

Parquet 用于：

- 不可变原始归档；
- 半年以上或两年以上的次要来源冷数据；
- ClickHouse 导出与灾难恢复；
- DuckDB 本地研究和回测。

推荐分区：

```text
market=CN/interval=1m/source=tushare/year=2026/month=07/*.parquet
```

### DuckDB

DuckDB 不再作为未来多进程在线行情服务的唯一主库，但继续用于：

- 当前阶段的本地兼容存储；
- 直接查询 Parquet；
- 离线研究、回测和数据质量检查；
- PostgreSQL/ClickHouse 数据快照分析；
- 迁移期间的对账和双写验证。

## 行情数据模型

### 多源原始标准化表

`market_bar_raw` 的业务身份为：

```text
(symbol, interval, adjustment, bar_time, source)
```

至少保存：

```text
symbol, market, interval, adjustment, bar_time
open, high, low, close, volume, amount
source, source_sequence, observed_at, ingested_at
raw_artifact_id
```

ClickHouse 建议按 `toYYYYMM(bar_time)` 分区，按 `(symbol, interval, source, bar_time)` 排序。相同时间点的多个来源必须并存，不能在原始层互相覆盖。

### 统一行情表

`market_bar_canonical` 保存对外默认结果，业务身份为：

```text
(symbol, interval, adjustment, bar_time)
```

除 OHLCVA 外还必须保存：

```text
selected_source, source_count, quality_status, version, updated_at
```

原始层负责事实保留，canonical 层负责选源和质量判断。调用方可以显式请求某个来源，默认接口返回 canonical 结果。

## 周期与保留策略

- 一分钟数据是分钟行情的永久基础粒度。
- `5m/15m/30m/60m` 原则上由一分钟数据聚合，不重复永久保存全部来源的所有周期。
- 最近 3～6 个月可在线保留全部来源。
- 较老的次要来源可归档为 Parquet；canonical 一分钟数据继续在线或按实际查询热度分层。
- Tick 和盘口必须配置独立保留周期，不默认永久在线保存。
- ClickHouse 需要为后台合并保留至少 30% 的空闲磁盘空间。

## 候选方案

### InfluxDB 3

InfluxDB 3 适合持续实时写入、最新值、近期窗口和 Dashboard，也支持 SQL、tag/field 模型及高基数序列。它适合作为短期热行情层。

本项目不选择它作为十年行情主库，原因是当前 InfluxDB 3 Core 对单次查询时间范围存在约 72 小时限制，而 MarketCow 的主要需求包含跨月、跨年、全市场横截面和十年回测。若同时引入 InfluxDB 和 ClickHouse，会产生两个时序数据库的职责重叠和额外运维成本。

只有在未来明确需要独立的超低延迟热数据层，且近期窗口查询与历史研究可以彻底分离时，才重新评估 InfluxDB。

### TimescaleDB

TimescaleDB 能复用 PostgreSQL 事务、约束和生态，是减少数据库种类的优选方案。若实际规模停留在数千万到低亿级、团队更重视简单运维，可以改用 PostgreSQL + TimescaleDB。

本次未将其作为十年多源分钟行情的首选，因为目标规模可能达到数十亿行，并包含大量全市场聚合；ClickHouse 在该模式下更匹配长期容量和查询特征。

### 继续只使用 DuckDB

运维最简单，适合当前单机阶段，但不满足长期多进程并发写入、持续实时摄取和在线 TB 级行情服务目标。

## 迁移阶段

### 阶段 0：接口隔离

- 提取 `MetadataRepository`、`FundamentalRepository`、`MarketBarRepository` 和 `ArtifactStore`。
- API 和 Service 不直接依赖 DuckDB SQL。
- 明确写入幂等键、来源、版本和错误语义。

### 阶段 1：PostgreSQL

- 迁移任务、Provider 状态、Artifact manifest、证券主数据和基本面。
- DuckDB 保持只读对账副本。
- 验证 point-in-time 查询和唯一约束。

### 阶段 2：ClickHouse 影子写入

- 分钟行情同时写入 DuckDB 与 ClickHouse。
- 对比行数、OHLCVA、缺失率、来源和时间边界。
- 建立批量写入、spool、重放和延迟监控。

### 阶段 3：查询切换

- history 和批量行情查询切换到 ClickHouse。
- DuckDB 不再承担在线行情写入。
- 保留回滚开关，直到双写对账稳定。

### 阶段 4：冷热分层

- 次要来源历史数据归档为 Parquet。
- 建立备份、恢复和重新构建 canonical 表的演练。
- 根据实际查询成本调整在线保留期限。

## 后果

正面影响：

- 基本面与行情分别采用适合其访问模式的数据库；
- 支持数十亿行行情、长区间分析和多来源并存；
- 在线写入不再受单个 DuckDB 文件锁约束；
- 原始、标准化和 canonical 三层边界清晰；
- API 契约可以在底层迁移期间保持稳定。

成本与约束：

- 需要维护 PostgreSQL 和 ClickHouse 两套服务；
- 需要实现批量写入、spool、重放、双写和对账；
- 跨基本面和行情的查询不能依赖在线跨库 Join，需要由服务层、ETL 或研究快照完成；
- ClickHouse 的版本合并是最终一致模型，不能当作 PostgreSQL 的即时唯一约束；
- 生产切换前必须完成容量压测和故障恢复演练。

## 容量基线

以十年全 A 股一分钟数据为规划基线：

- 单来源约 20～33 亿行；
- 单来源在线压缩数据初步按 150～350 GB 规划；
- 三来源、双副本和备份后按 1.5～3 TB 起步评估；
- 不将逐行完整 JSON 纳入上述估算；完整响应必须按批次归档；
- 最终采购或部署前必须使用真实一个月数据完成压缩率、写入、合并和查询基准测试。

容量数字是架构规划假设，不是硬性承诺。真实结果取决于证券历史数量、字段类型、来源数量、压缩编码、副本和保留策略。

## 评审触发条件

出现以下任一情况时重新评审：

- 实际分钟数据规模显著低于预期，TimescaleDB 可以明显降低运维复杂度；
- 主要产品需求变为仅查询最近数小时或数天，需要独立热数据层；
- 引入 Tick/逐笔导致写入量超过单节点规划；
- ClickHouse 与 PostgreSQL 的运维成本超过团队承受能力；
- InfluxDB Core 的长时间范围限制和集群能力发生实质变化。
