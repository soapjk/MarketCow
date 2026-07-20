# MarketCow Storage V2 开发交接

最后更新：2026-07-20

本文是清空上下文后开始新会话的首要入口。新会话应先完整阅读本文，再阅读文末列出的 ADR 和架构文档。

## 一、当前目标

在不影响现有正式服务的前提下，将 MarketCow 从单一 DuckDB 在线存储逐步演进为：

```text
PostgreSQL
  └── 基本面、财务、证券主数据、任务、请求和溯源元数据

ClickHouse
  └── 多来源分钟 K 线、统一行情、未来 Tick/逐笔/盘口

原始文件 / Parquet
  └── 完整响应、冷数据、可重放归档

DuckDB
  └── 当前兼容层、离线研究、回测、质量检查和迁移对账
```

当前阶段不是立即替换生产数据库，而是先建立存储接口、测试环境和可回滚的双写能力。

### 2026-07-20 Stage 1 进展

- 已完成 `MetadataRepository` 、`FundamentalRepository`、`MarketBarRepository`、
  `ArtifactStore` 接口隔离和 DuckDB 兼容 backend；
- PostgreSQL metadata backend 已实现连接池、独立 schema、事务化版本 migration、
  migration advisory lock；
- 已迁移任务记录、Provider 健康、Artifact manifest、日历和 Tushare 请求控制面；
- 已迁移基本面当前快照、不可变版本历史、完整财务报表 JSONB 和严格
  point-in-time 查询；
- BaoStock 快照、TDX 当前快照、TDX 不可变版本历史与 `as_of` 查询已迁移
  PostgreSQL；
- `validation_result` 与 `funnel_metrics` 已迁移 PostgreSQL，包括复合键幂等更新、
  七项跨来源校验重建、漏斗重建/筛选及严格 `as_of` 查询；Stage 1 路由仅切换
  这些已迁移的基本面数据域；
- PostgreSQL 目前只允许 development profile 显式启用，schema 必须以
  `_development` 或 `_test` 结尾；
- 开发 profile 的 Stage 1 基本面关系数据可由 PostgreSQL 承担；DuckDB 仍是行情主写
  和默认读取 backend，只有 development 显式 opt-in 时，历史 `get_price_bars` 才读取
  ClickHouse canonical。未连接或迁移正式 PostgreSQL/ClickHouse。
- ClickHouse development/test 存储基础已建立：默认关闭且只允许显式连接本机回环
  地址及 `_development`/`_test` database；包含客户端生命周期、幂等 migration、
  健康诊断，以及 `market_bar_raw`/`market_bar_canonical` 基础表；
- 已实现独立的可靠影子写入原语：1000～50000 行可配置微批、字段和 UTC 时间
  规范化、稳定批次 ID、development 本地原子 WAL、显式有界重放与诊断。该 writer
  已用于 development raw 影子双写和显式 canonical 重建；DuckDB 主写不变，读取仅有
  development opt-in 的 canonical history 例外。
- WAL 隔离采用显式 development storage root 允许边界：配置层要求 root 名称明确包含
  development/test 且 spool resolve 后位于其中；`LocalClickHouseSpool` 构造层再次要求
  显式 allowed root 并在创建目录前校验。兄弟正式目录、`..` 穿越及 symlink 逃逸均拒绝。
- development 且显式启用 ClickHouse 时，`ShadowMarketBarRepository` 已接入 raw 行情
  影子双写：DuckDB 必须先成功，shadow 失败只落 spool 并保持 primary 成功；quotes
  始终委托 DuckDB。默认 history 也委托 DuckDB，只有额外显式启用 canonical read backend
  时才读取 ClickHouse 并在失败时回退 DuckDB。disabled 时不连接 ClickHouse、不创建 spool。
- 提供最后一个有界 raw 批次的显式只读对账：按业务键比较 DuckDB 与 ClickHouse
  `FINAL` 的行数、时间边界、OHLCVA、source 和 ingestion lag，结构化输出有限数量的
  mismatch；对账错误不会写数据或阻断主路径。
- canonical 构建是显式、有界的 `rebuild_canonical(symbol, interval, adjustment,
  start, end, limit)` 操作：只扫描 `market_bar_raw FINAL` 的指定闭区间，不在 raw 写入
  时隐式重建，也不启动后台线程。达到限制时整次返回 `truncated` 且不写入，避免使用
  不完整来源集合生成 canonical。
- 来源选择使用配置的显式优先级，其后依次是最新 `observed_at`、最新 `ingested_at`，
  最后使用 source、artifact 和 sequence 的稳定字典序 tie-break，不依赖查询返回顺序。
  默认优先级为 tushare、sina、eastmoney、yahoo_chart、baostock。
- 多来源 OHLCVA 使用 `rel_tol=1e-6`、`abs_tol=1e-9` 的数值容差；质量状态区分
  `single_source`、`multi_source_consistent` 和
  `multi_source_ohlcva_difference`，并保留来源数量、选中来源及其 provenance。
- canonical 输入 fingerprint 相同则复用 version，输入变化才递增；
  ReplacingMergeTree 按业务键和 version 收敛。写入复用可靠 writer，失败进入
  development spool 并可显式 replay；诊断包含扫描行/组、写入/spool、质量与来源计数、
  截断和有界错误。默认应用读取仍走 DuckDB。
- 历史读取现在支持 development-only 的显式切换：
  `MARKETCOW_MARKET_BAR_READ_BACKEND=clickhouse_canonical` 必须与
  `MARKETCOW_CLICKHOUSE_ENABLED=true` 同时设置；默认值和立即回滚值均为 `duckdb`，
  production 会拒绝 canonical 读取配置。quotes、DuckDB-primary 写入和 shadow 写入
  路由不变。
- canonical 历史查询使用 `FINAL`，按 symbol/interval/adjustment 过滤，倒序取最近 limit
  条后按时间升序返回。结果映射为原 `get_price_bars` 字段，包括 timestamp、bar_at、
  OHLC、raw_close、adjustment_factor、volume、amount、source、ingested_at 与
  source_payload；migration 3 将两个调整契约字段以 Nullable(Float64) 贯穿 raw 与
  canonical。ClickHouse 读取失败会有界记录 error/backend/fallback 并同步回退 DuckDB。
- 历史范围契约为 `get_price_bars_range(symbol, interval, adjustment, start, end, limit)`：
  start/end 必须是带时区 ISO-8601，统一为 UTC，使用闭区间并稳定升序返回。超过 limit
  返回前 limit 条并显式 `truncated=true`，不得把部分结果伪装为完整结果。DuckDB 与
  development opt-in ClickHouse canonical 实现字段和空值语义一致；ClickHouse 失败按
  同一范围回退 DuckDB，并在 read diagnostics 记录 backend/fallback/count/truncated/error。
- `/v1/quotes/{symbol}/history` 在同时提供 start/end 时执行本地范围读取并返回
  `cached=true` 与 `truncated`；只提供一个端点或非法范围返回 400。未提供 start/end 时，
  原有 refresh/cache 与最近 N 条行为保持不变。
- canonical 横截面契约为 `get_price_bars_cross_section(interval, adjustment, bar_at,
  limit, symbols=None)`：bar_at 必须是带时区 ISO-8601，规范化为 UTC 整数秒，只精确
  匹配该 bar 时间点，不回填陈旧值。每个 symbol 最多一条并按 symbol 升序；symbols
  可选、去重且最多 5000 个，显式空列表返回空；limit 1..5000，部分结果显式
  `truncated=true`。DuckDB 对同 symbol/time 的多来源用最新 ingested_at、再按 source
  稳定选择；ClickHouse 查询 canonical `FINAL`，失败按同一查询回退 DuckDB。
- 只读 API `GET /v1/quotes/cross-section` 返回规范化 bar_at、count、bars、cached=true 和
  truncated；不触发 refresh 或写入，语义参数错误返回 400。
- raw 多来源范围读取契约为 `get_raw_price_bars_range(symbol, interval, adjustment,
  start, end, limit, sources=None)`：带时区端点统一为 UTC 整数秒并使用闭区间；同一
  bar_time 的各 source 分别保留，同一 raw 业务键只返回最新逻辑版本，按 bar_time、source
  稳定升序并显式报告 truncated。sources 去重且最多 100 个，limit 为 1..5000。
  DuckDB 主写按规范化 UTC ingested_at 保留同一业务键的最新版本；时间相同时使用共享的
  逻辑内容 SHA-256 派生的 208-bit rank 决定唯一胜者。rank 将时间统一为 UTC 毫秒、数值统一为 Decimal
  规范字符串，并统一 null/string，因而不受 `1`/`1.0`、Z/offset 或到达顺序影响。
  ClickHouse 查询不依赖等版本 `FINAL` 胜者，而用窗口按 ingested_at、content_rank 降序
  选择，与 DuckDB 使用同一 rank；普通重复及最新 ingestion 仍收敛。ClickHouse provenance 通过
  DateTime64(3) 毫秒 epoch 映射为 UTC ISO，不截断 observed_at/ingested_at 毫秒。
- raw 读取 backend 与 canonical 读取独立配置：默认
  `MARKETCOW_RAW_MARKET_BAR_READ_BACKEND=duckdb`；只有 development、ClickHouse 已显式
  启用时才允许 `clickhouse_raw`，production 拒绝。ClickHouse 查询使用 raw `FINAL`，失败
  以同一查询回退 DuckDB，并记录 raw_multisource/backend/fallback/count/truncated/error。
- 只读 API `GET /v1/quotes/{symbol}/raw-history` 返回规范化范围、全部 provenance、count、
  bars、cached=true 和 truncated；不触发 refresh 或写入，语义参数错误返回 400。
- automatic canonical 衔接默认关闭，仅 development 可通过
  `MARKETCOW_CLICKHOUSE_AUTO_CANONICAL=true` 显式启用。raw shadow 成功后按当前批次精确
  min/max 闭区间同步执行有界 rebuild；raw 仅落 spool 时不执行，只有该 raw 项成功 replay
  后才执行。canonical spool 不触发回调；所有截断、写入失败和异常均 fail-open，并在
  `auto_canonical` diagnostics 中有界记录，不改变 DuckDB primary 返回。
  分块 raw 写入会持久化逻辑批次 intent（完整规范化 rows、完整范围和 pending chunk ID）；
  只有所有失败块均成功 replay 后才用完整逻辑批次触发一次 rebuild。回调异常保留 intent、
  记录有界 `replay_callback` 错误，不会被静默丢弃。

## 二、仓库、分支和 worktree

GitHub 仓库：

```text
https://github.com/soapjk/MarketCow
```

仓库是公开仓库。任何后续 `git push`、PR、Release、上传或其他远程写入都必须先按项目规则披露目标、内容、可见性和存储后果，并在后续消息得到用户明确确认。

### 正式工作区

```text
路径：/Volumes/T9/projects/marketcow
分支：main
提交：739e2d8
远端：本文不声明当前同步状态；任何 push 前须重新只读核验
用途：正式服务，只维护稳定版本
```

不要在这个工作区进行 Storage V2 开发，不要切换它的分支，不要重置或清理它。

### Storage V2 开发工作区

```text
路径：/Volumes/T9/projects/marketcow-storage-v2
分支：feature/storage-v2
基线提交：71f5b26
远端状态：该分支目前仅存在于本地，尚未推送
用途：PostgreSQL、ClickHouse、Repository 接口和迁移开发
```

新会话应将工作目录设为：

```bash
cd /Volumes/T9/projects/marketcow-storage-v2
```

## 三、正式版与开发版隔离

### 正式服务

```text
地址：http://127.0.0.1:8790
数据：/Volumes/T9/projects/marketcow/data/
数据库：data/warehouse/market_data.duckdb
服务管理：launchd（com.marketcow.production）
工作区：marketcow
```

正式服务正在被 Investrace 等本机消费者使用。禁止为开发测试执行以下操作：

- 停止或重启 `com.marketcow.production`；
- 占用 8790；
- 修改或删除正式 `data/`；
- 对正式 DuckDB 执行 schema 实验；
- 让开发测试连接正式 PostgreSQL/ClickHouse database 或 schema；
- 在未备份和明确确认前执行正式迁移。

### 开发服务

```text
地址：http://127.0.0.1:8791
数据：/Volumes/T9/projects/marketcow-storage-v2/data-development/
数据库：data-development/warehouse/market_data.duckdb
日志：logs-development/marketcow.log
服务管理：本文不声明当前存在常驻 development 服务
工作区：marketcow-storage-v2
```

如需临时启动开发服务，可使用独立的 development profile；本文不声明当前存在持久运行的
development 服务或 tmux session：

```bash
cd /Volumes/T9/projects/marketcow-storage-v2
.venv/bin/marketcow --profile development start
```

检查两个环境：

```bash
curl http://127.0.0.1:8790/v1/health
curl http://127.0.0.1:8791/v1/health
```

开发版健康响应必须包含：

```json
{
  "profile": "development",
  "database": "data-development/warehouse/market_data.duckdb"
}
```

配置保护会拒绝开发版使用 8790 或默认正式数据目录。

## 四、本地配置与凭证

以下真实配置文件均被 Git 忽略：

```text
.env
.env.production
.env.development
```

仓库只跟踪：

```text
.env.production.example
.env.development.example
```

Storage V2 worktree 已有本地 `.env.development`，配置 8791 和 `data-development/`，但没有在跟踪文件中保存 Tushare key。

Tushare key 不得写入源码、测试、日志、ADR、提交信息或任何被 Git 跟踪的文件。如果开发环境需要真实调用，应将 key 写入该 worktree 被忽略的 `.env.development` 或 `.env`，并优先使用独立测试凭证，避免消耗正式配额。

## 五、当前已实现能力

基线提交 `71f5b26` 包含：

### Tushare 数据源

- 通用接口：`POST /v1/tushare/{api_name}`；
- 接受任意 `api_name`、`params` 和 `fields`；
- 特殊实时接口：`POST /v1/tushare/realtime-quote`；
- 通用 Pro 中转地址：`https://fastapic.stockai888.top`；
- 实时校验地址：`https://realtime.stockai888.top`；
- gzip 请求头；
- 默认最小间隔 0.5 秒，即每分钟不超过 120 次；
- SDK 实时接口从内存读取 token，不写 `~/.tushare.csv`。

### 请求即落库

所有成功的 Tushare 请求保存到：

```text
tushare_request
tushare_data_row
data/raw/tushare/
raw_artifact_manifest
```

`tushare_data_row.payload_json` 保留每一行的完整动态字段，`source` 明确记录数据来源。

### 统一分钟行情

A 股分钟 K 线已经接入：

```text
GET /v1/quotes/{symbol}/history
```

支持：

```text
1m / 5m / 15m / 30m / 60m / 1h
```

当前 Tushare HTTP 分钟行情只按未复权接入，因此必须使用：

```text
adjustment=raw
```

统一表 `market_price_bar` 的业务主键为：

```text
(symbol, interval, adjustment, timestamp, source)
```

同一根 K 线的多个来源可以并存。表中还保留 `amount`、`payload_json`、`source_url` 和 `raw_artifact_id`。

### 有界上游超时

曾发生上游多层重试占满 FastAPI 同步线程池，导致行情和纯本地 snapshot 同时卡死。现已修复：

- 新浪全部回退共享 1.8 秒墙钟预算；
- 东方财富请求与 curl 回退共享 1.8 秒预算；
- Yahoo 请求与 curl 回退共享 3.5 秒预算；
- 上游失败时有缓存则返回可识别的缓存降级响应；
- 没有缓存时在有界时间内返回错误；
- 模拟上游超时测试验证 snapshot 不会被饿死。

正式环境独立验收结果曾达到：

```text
/v1/quotes/600519.SH：HTTP 200，约 0.24～0.40 秒
/v1/snapshot?limit=10：HTTP 200，约 0.025～0.027 秒
当时完整测试：47/47 通过（随后环境隔离阶段曾增长到 51 项；最新数量见第十一节）
```

## 六、已确定的数据库决策

必须先阅读：

- `docs/decisions/ADR-001-local-first-data-platform.md`
- `docs/decisions/ADR-002-split-transactional-and-market-time-series-storage.md`
- `docs/architecture/initial-architecture.md`

ADR-002 已采纳以下长期职责：

### PostgreSQL

保存：

- instrument、exchange、symbol alias；
- 基本面、完整财务报表和 point-in-time 历史；
- 经济日历、财报日历和宏观数据；
- Tushare 请求及动态 JSONB 行；
- Provider 健康、任务、schema migration；
- Artifact manifest 和跨来源校验结果。

### ClickHouse

保存：

- 多来源原始标准化分钟行情；
- canonical 统一行情；
- 未来 Tick、逐笔和盘口；
- 从一分钟数据生成的聚合周期；
- 长时间范围和全市场横截面查询。

建议原始行情业务键：

```text
(symbol, interval, adjustment, bar_time, source)
```

建议 canonical 业务键：

```text
(symbol, interval, adjustment, bar_time)
```

canonical 必须额外记录：

```text
selected_source, source_count, quality_status, version, updated_at
```

### 为什么不是 InfluxDB 主库

InfluxDB 适合最新值、近期窗口和 Dashboard，但当前 InfluxDB 3 Core 的长查询时间范围限制不适合十年分钟数据、跨年回测和全市场横截面。现阶段不同时维护 InfluxDB 与 ClickHouse两个职责重叠的时序数据库。

### 为什么保留 DuckDB

DuckDB 继续用于本地兼容、Parquet 查询、离线分析、回测、质量检查和迁移对账，但不作为未来多进程在线行情服务的唯一主写库。

## 七、容量与保留假设

规划基线：

```text
5,500 个证券 × 240 分钟 × 250 交易日 × 10 年
≈ 33 亿行/数据源
```

考虑历史上市数量后，单来源约 20～33 亿行。初步容量假设：

- 单来源在线压缩数据约 150～350 GB；
- 三来源、双副本和备份后按 1.5～3 TB 起步评估；
- 上述数字不包含逐行大型 JSON；
- 完整响应必须按请求批次归档，而不是在每根 K 线上复制；
- 最终部署前必须使用真实一个月数据进行压缩率、写入、合并和查询基准测试；
- ClickHouse 长期保持至少 30% 空闲空间供后台合并。

只永久保存一分钟基础粒度；`5m/15m/30m/60m` 原则上由一分钟聚合，不为每个来源永久重复保存所有周期。

## 八、Storage V2 实施顺序

新会话不要直接开始迁移正式数据库。按以下顺序推进。

### 第 1 步：存储接口隔离

先在应用内部建立：

```text
MetadataRepository
FundamentalRepository
MarketBarRepository
ArtifactStore
```

目标：

- Service/API 不再直接依赖 DuckDB SQL；
- 当前 DuckDB 实现成为这些接口的一个 backend；
- 保持所有现有 HTTP 响应兼容；
- 为 PostgreSQL/ClickHouse 实现建立稳定契约；
- 明确幂等键、版本、来源和错误语义。

本步骤已经完成；新会话不得重复实施，应以长期协作任务的最新检查点指令为准。

### 第 2 步：开发环境基础设施

- 只在 development 环境加入 PostgreSQL 和 ClickHouse；
- database、user、schema、端口和数据卷必须与正式环境隔离；
- 凭证放 `.env.development`；
- 增加 readiness、连接诊断和禁用开关；
- 测试默认不能依赖外部数据库，集成测试需要显式启用。

在引入 Docker Compose 或其他容器写操作前，检查用户当前授权和项目规则。禁止 ChatGPT Sites。

### 第 3 步：PostgreSQL 迁移

优先迁移控制面和关系数据：

```text
provider_health
ingestion_runs
raw_artifact_manifest
instrument / aliases
tushare_request / tushare_data_row
fundamental / statements / PIT history
```

先影子写入并对账，不切换正式读取。

当前单步状态：控制面、核心基本面/PIT、财务报表、BaoStock、TDX、
`validation_result` 和 `funnel_metrics` 的 PostgreSQL repository 与 development 路由
均已实现。集成测试须通过 `MARKETCOW_TEST_POSTGRES_DSN` 显式启用；默认测试不依赖
外部 PostgreSQL。正式环境尚未切换。

### 第 4 步：ClickHouse 影子写入

本步骤的 development 范围已经完成：

- 已建立 `market_bar_raw`；
- 已建立 `market_bar_canonical`；
- 已实现数千到数万行微批写入原语；
- 已实现写入失败进入 development 本地 WAL/spool；
- 已实现显式重放、稳定幂等标识和有界延迟诊断；
- 已完成 development raw 的 DuckDB-primary/ClickHouse-shadow 双写，并可显式对比
  行数、OHLCVA、时间边界、来源和 lag；
- 已实现确定性 canonical 来源选择、质量分类、稳定/单调 version 与显式有界范围重建；
- canonical 写入复用可靠 writer，失败进入同一 development spool 并可重放；
- 已实现 development opt-in canonical history 读取与 DuckDB 故障回退，但默认读取和
  所有主写仍为 DuckDB。

注意：普通 `ReplacingMergeTree` 允许物理重复行，逻辑读取必须使用合并后语义（例如
`FINAL`）；稳定 `insert_deduplication_token` 同时为未来 ReplicatedMergeTree 保留批次
幂等标识。WAL 成功重放后原子写入 `replayed/` 再移除 pending，崩溃后重复重放仍以
相同批次 ID 和业务排序键收敛为一个逻辑版本。

### 第 5 步：查询切换

- 已完成第一段：development 可显式让现有 `get_price_bars` 从 ClickHouse canonical
  读取最近 N 条，保留 backend 开关、DuckDB 默认值与失败回退路径；production 拒绝启用；
- 已完成带时区 ISO-8601 起止时间的闭区间历史范围契约、API 扩展、limit/truncated 语义、
  DuckDB/ClickHouse canonical 实现及同范围故障回退；
- 已完成精确单一 bar 时间点的 canonical 全市场横截面契约、可选有界 symbols 过滤、
  DuckDB/ClickHouse canonical 实现、只读 API 与同查询故障回退；
- 尚未完成大规模长时间范围基准与分页/游标契约；
- 尚未完成非精确时间（如最近有效 bar）或多时间点横截面，以及多来源 raw 查询契约；
- 尚未完成缓存命中、失效、新鲜度和 ClickHouse/DuckDB 回退一致性契约测试；
- 尚未实现自动或后台 canonical 调度；当前只能显式、有界重建；
- 尚未对正式读取进行连接、迁移或切换；
- 未经用户明确批准，不切换 8790 正式服务。

### 第 6 步：冷热分层

- 最近 3～6 个月保留全部在线来源；
- 较老次要来源归档 Parquet；
- canonical 一分钟数据长期在线或按查询热度分层；
- Tick/盘口设置独立保留周期；
- 演练备份、恢复和 canonical 重建。

## 九、开发约束

### Git 与 worktree

- 只在 `/Volumes/T9/projects/marketcow-storage-v2` 编辑 Storage V2；
- 不在正式工作区切分支；
- 不 reset、checkout、rebase 或清理正式 worktree；
- 不删除其他 worktree；
- 每次分支/worktree 操作前检查 `git status`、当前分支和 `git worktree list`；
- 当前 `feature/storage-v2` 尚未推送，推送前仍需远程写入确认。

### 文件和数据安全

- 保留用户已有修改；
- 使用 `apply_patch` 编辑源码和文档；
- 不把 `.env`、token、数据库文件、原始行情或日志加入 Git；
- 不使用正式数据库跑测试；
- 不执行破坏性清理；
- 删除任何实质数据前必须确认精确目标和可恢复性。

### 外部行为

- 默认只做本地开发；
- 不 push、开 PR、发 Release、部署、上传、发布包或写外部服务，除非用户针对确切目标再次确认；
- 只读互联网研究允许，但技术结论优先使用官方文档；
- 不安装、启用、推荐或使用 ChatGPT Sites。

## 十、测试与验收

完整测试：

```bash
cd /Volumes/T9/projects/marketcow-storage-v2
test_root=$(mktemp -d)
MARKETCOW_HOME="$test_root" uv run python -m unittest discover -s tests -v
uv run ruff check src tests
git diff --check
```

使用临时 `MARKETCOW_HOME` 是为了避免测试进程与正在运行的 DuckDB 文件锁冲突。

双环境验证：

```bash
curl --max-time 5 http://127.0.0.1:8790/v1/health
curl --max-time 5 http://127.0.0.1:8791/v1/health
curl --max-time 5 http://127.0.0.1:8791/v1/snapshot?limit=10
curl --max-time 5 http://127.0.0.1:8791/v1/quotes/600519.SH
```

任何 Storage V2 变更至少满足：

- 8790 不被停止、重启或修改；
- development 只写独立存储；
- 现有默认测试继续通过（检查点 8 的数量见第十一节）；
- 新 backend 有单元测试和显式集成测试；
- 上游失败仍有界，不占满服务线程；
- 双写失败不能阻断现有 DuckDB 主路径，除非测试明确验证 fail-closed；
- 数据记录保留 `source`、`observed_at`、`ingested_at` 和 `raw_artifact_id`；
- 接口兼容性变化必须先形成文档并获得用户确认。

## 十一、当前运行检查结果

最近一次 Storage V2 检查：

```text
feature/storage-v2 检查点 8 基线：f3e642c；本检查点为其上的 canonical 横截面改动
默认测试：发现 97 项且整体通过；7 项 PostgreSQL、7 项 ClickHouse 集成测试因未
显式配置本地服务而跳过
PostgreSQL 集成测试：7 项通过（显式启用，使用独立 UTF-8 临时数据库）
ClickHouse 测试：9 项通过（2 项隔离边界测试、7 项使用一次性 ClickHouse 25.8
本地容器的集成测试；容器测试后停止并删除）
本检查点已完成精确单一 bar 时间点的 canonical 全市场横截面、只读 API、显式截断和
DuckDB/ClickHouse canonical 等价回退；默认仍为 DuckDB。非精确/多时间点横截面、
大规模范围分页/基准、多来源 raw、缓存契约、自动 canonical 调度及后续阶段尚未完成，
必须等待验收方指定。
```

正式服务由 `com.marketcow.production` 管理；最近已验证 health 包含
`profile=production` 和相对 database 路径。Storage V2 文档核验不得为此停止或重启正式服务。

## 十二、新会话建议的第一条指令

可以直接使用：

```text
请先完整阅读 docs/development-handoff-storage-v2.md、ADR-001、ADR-002 和 AGENTS.md。
只在 /Volumes/T9/projects/marketcow-storage-v2 的 feature/storage-v2 分支工作，
不要影响 127.0.0.1:8790 正式服务。先核对 feature/storage-v2 当前 HEAD、本文的已完成
检查点和长期协作任务的最新指令；不要重复实现已完成的 Repository/PostgreSQL/ClickHouse
步骤。只执行验收方指定的下一单步，保持现有 API 契约和全部测试通过；不要连接或迁移
正式 PostgreSQL/ClickHouse，也不要远程推送。
```
