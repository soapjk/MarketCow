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
- 最近 N 条、闭区间、精确横截面和 raw-history 响应统一提供 `cache_status`、
  `newest_ingested_at`、`cache_age_seconds`、`served_at` 和
  `cache_freshness_seconds`。状态确定为 fresh、stale 或 empty；时间统一为 UTC，age 非负。
  阈值由 `MARKETCOW_MARKET_BAR_CACHE_FRESHNESS_SECONDS` 配置，范围 1..86400 秒，默认
  900 秒。refresh 失败但缓存存在时返回 `cache_degraded=true` 和有界 cache_reason；缓存
  为空则保持有界错误。ClickHouse 成功读取与同查询 DuckDB fallback 使用同一响应层算法，
  backend diagnostics 不参与缓存语义。
- canonical 单 symbol 闭区间支持显式 keyset 分页：同时提供 start/end/page_size（1..5000）
  后返回 `next_cursor`，按 bar_time 稳定升序前进，末页为 null。游标为版本化、URL-safe、
  HMAC-SHA256 完整性保护的 opaque token，绑定 symbol/interval/adjustment、规范化 UTC 边界
  和 page_size，并受 60..86400 秒 TTL 约束。secret/TTL 分别通过
  `MARKETCOW_MARKET_BAR_CURSOR_SECRET` 和 `MARKETCOW_MARKET_BAR_CURSOR_TTL_SECONDS`
  配置。不存在显式 secret 时，只有首次分页才会在隔离 storage root 内以进程锁串行、
  原子生成并持久化 32-byte 随机密钥；普通启动和非分页 API 无密钥文件副作用。显式密钥
  至少 32 bytes，已知默认值/placeholder 被拒绝；轮换会使旧游标失效，token 最大 2048
  字符。DuckDB 与 ClickHouse 均使用 `bar_time > after` keyset 条件而非 OFFSET；DuckDB
  fallback 与 canonical builder 复用同一 source priority、observed/ingested 时间及稳定
  tie-break 选择规则，ClickHouse 失败以同一 after/边界回退。
- automatic canonical 衔接默认关闭，仅 development 可通过
  `MARKETCOW_CLICKHOUSE_AUTO_CANONICAL=true` 显式启用。raw shadow 成功后按当前批次精确
  min/max 闭区间同步执行有界 rebuild；raw 仅落 spool 时不执行，只有该 raw 项成功 replay
  后才执行。canonical spool 不触发回调；所有截断、写入失败和异常均 fail-open，并在
  `auto_canonical` diagnostics 中有界记录，不改变 DuckDB primary 返回。
  分块 raw 写入会持久化逻辑批次 intent（完整规范化 rows、完整范围和 pending chunk ID）；
  只有所有失败块均成功 replay 后才用完整逻辑批次触发一次 rebuild。回调异常保留 intent、
  记录有界 `replay_callback` 错误，不会被静默丢弃。
  显式 replay 还会有界扫描无 pending 的 ready intents；callback 失败或进程重启后可重试，
  成功即删除 intent。intent 在回调前原子 claim，且会依据 pending/replayed WAL 状态收敛，
  可恢复 WAL 已移走但 intent 尚未更新的崩溃窗口。
  replay 的 WAL attempt 与 ready callback attempt 共用同一 limit 预算，outcome 报告两类
  attempted/ok/failed、remaining、truncated 与 lock_busy。同一 spool 通过进程级文件锁串行
  replay；活跃 processing claim 不会被第二个 writer 回收，进程退出释放锁后才允许恢复。

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
当前已验证基线：ef86ad1（检查点 11）
当前本地候选：51b0c2b（检查点 12 修订，待独立验收）
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

## 八、Storage V2 固定开发清单

本节是从当前状态到“Storage V2 本地可交付完成”的唯一有序清单，取代此前按六个宽泛步骤
描述的待办路线。它不是 MarketCow 全部未来产品功能的路线图。后续开发必须按稳定 ID 的
依赖顺序逐项下发、实现、验证和验收；每次只推进一项。状态定义如下：

- `已验收`：已有独立验收证据，不得重复实现；
- `已实现待验收`：代码已在本地候选提交中，但不能作为后续依赖的已验收事实；
- `未开始`：尚未授权实施；
- `需用户授权`：包含外部或 production 状态变更，默认流程不得执行。

### 完成定义与范围边界

**本清单 in scope：**

- 已有 Repository 边界，以及本文已列明的 PostgreSQL 控制面、基本面、财务、BaoStock、TDX、
  validation 和 funnel 数据域；
- 一分钟 market bar 的 DuckDB primary 兼容、ClickHouse raw/canonical 影子存储、确定性选源、
  对账、可靠 spool/replay 和显式/后台 canonical 衔接；
- 已有行情 API 对应的 recent、闭区间、精确横截面、多来源 raw、非精确读取、缓存新鲜度、
  keyset 分页和跨 backend fallback 一致性；
- 上述存储能力的本地可观测性、冷热 Parquet 归档原语、备份恢复、迁移/切换/回滚演练、
  容量性能基准、production 准备包和最终本地验收。

**本清单 out of scope：**

- 新 provider 或新市场接入，A 股复权/公司行动、公告、宏观、事件、一致预期等产品数据域扩展；
- Tick、逐笔、盘口的实际采集与产品接口，多周期行情产品化，以及新的筛选、估值、研究或交易功能；
- Investrace 或其他下游应用改造、UI、外部监控/告警 SaaS、云存储和多机集群产品化；
- `git push`、PR、Release、部署以及任何 production 连接、备份、迁移、切换或消费者变更。

当 `SV2-012` 至 `SV2-025`（及其正式登记的阻断修订项）全部由验收方独立验收、没有未解决
的 in-scope 阻断缺陷，且 `SV2-025` 的本地总验收通过时，验收方可以判定“Storage V2 本地
开发完成”。这不表示 MarketCow 所有未来产品开发完成，也不要求执行 `SV2-EXT`；后者始终是
获得用户逐项明确授权后才可选择执行的独立阶段。

### 冻结后的变更控制

1. 本清单获验收冻结后，正常开发不得重排、复用、删除或静默改写稳定 ID、目标和范围；状态、
   Artifact 与验证证据可在每次验收后追加。
2. 若某项验收发现会阻断该项或后续依赖的缺陷，先登记稳定 ID
   `SV2-<父项>-BLOCK-<两位序号>`；若是已验收项的回归修订，使用
   `SV2-<父项>-REV-<两位序号>`。登记必须包含父项、发现证据、阻断原因、最小修复范围、依赖、
   验收标准、必要测试、排除项、状态和最终解决提交。该 ID 插在父项之后、下一依赖开始之前，
   不改变既有 ID，也不得借阻断修复扩大产品范围。
3. 阻断项只有在验收方确认其确实阻止既定验收标准或后续依赖时才能加入完成判定；解决后保留
   记录，不删除。后续项必须同时依赖父项及其全部已登记阻断项通过。
4. 非阻断改进、新产品需求和便利性需求登记到 `Storage V2 Backlog`，不插入固定执行序列，
   不影响 `SV2-025` 完成判定。只有用户明确修改 Storage V2 整体目标后，才能通过新的规划验收
   将 backlog 转为稳定清单项；不得由实现者或验收者单方面扩大范围。

当前没有已登记的 `BLOCK/REV` 项；`SV2-012` 中已知的两项缺陷属于该项首次验收收口，已在
该稳定项范围内明示，不另造重复修订 ID。

### 固定依赖顺序

```text
SV2-012 → SV2-013 → SV2-014A → SV2-014B → SV2-015 → SV2-016
→ SV2-017 → SV2-018 → SV2-019A → SV2-019B → SV2-020A → SV2-020B → SV2-021A → SV2-021B
→ SV2-022A → SV2-022B → SV2-023 → SV2-024 → SV2-025

SV2-EXT：仅在 SV2-025 后且用户逐项授权时可选执行，不属于本地完成条件。
```

冻结清单共 20 个稳定执行项：19 个本地完成项和 1 个可选授权项 `SV2-EXT`。正式登记的
`BLOCK/REV` 不计入冻结项数量，但会作为对应父项的强制验收依赖永久保留记录。

### Storage V2 Backlog

当前为空。未来非阻断需求只在此追加 ID `SV2-BACKLOG-<四位序号>`、摘要、来源和日期；除非
用户明确修改整体目标并通过规划验收，不为其补执行依赖，也不计入本地完成条件。

### 已完成检查点摘要

| 稳定 ID | 已完成范围 | 状态 / 基线 |
| --- | --- | --- |
| `DONE-00` | Repository 接口隔离与 DuckDB 兼容 backend | 已验收 |
| `DONE-01` | PostgreSQL development 基础、关系/控制面/基本面路由及 7 项集成测试 | 已验收，至 `c4184b2` |
| `DONE-02` | ClickHouse development 基础、raw/canonical schema、可靠 writer、隔离 spool/replay | 已验收，至 `5078e79` |
| `DONE-03` | DuckDB-primary raw shadow 双写、对账与 canonical 确定性构建 | 已验收，至 `dc98841` |
| `DONE-04` | canonical 最近 N 条、闭区间、精确横截面和 raw 多来源查询及 fallback | 已验收，至 `af10688` |
| `DONE-05` | 同步有界 automatic canonical、完整批次 intent、重放恢复与互斥 | 已验收，至 `4e613bd` |
| `DONE-06` | 四类行情响应统一缓存命中、失效/新鲜度与缓存降级语义 | 已验收，`ef86ad1` |

### `SV2-012`：canonical 范围 keyset 分页收口

- **目标**：完成单 symbol 闭区间 canonical 历史的安全、跨 backend 等价 keyset 分页。
- **范围**：验收 `b975460` 的分页契约，并收口两项已知缺陷：公开固定 HMAC 默认密钥；
  DuckDB fallback 与 canonical 多来源选择规则不一致。当前 `51b0c2b` 已改为显式高熵密钥
  或隔离 storage root 内原子持久化随机密钥，并复用 canonical 选择规则。
- **前置依赖**：`DONE-06`。
- **验收标准**：游标版本化、URL-safe、查询绑定、TTL 和最大长度有界；密钥至少 32-byte
  等效熵，拒绝 placeholder，重启后可验证且轮换后旧游标失效；DuckDB/ClickHouse/fallback
  对冲突多来源返回相同 source、OHLCVA、bars 和 next_cursor；无 OFFSET、无丢失重复。
- **必要测试**：固定数据多页/边界/篡改/跨查询/过期；密钥生成、重启、轮换；正反来源冲突；
  10,001 行有界查询；默认、ClickHouse、PostgreSQL、Ruff、`git diff --check`。
- **排除项**：raw/横截面分页、production 密钥下发、部署和远程写入。
- **状态**：`已验收`；初始候选 `b975460` 的两项缺陷已在功能候选
  `51b0c2b` 修复。以冻结清单 `ef8645d` 为规划基线的 SV2-012 复核确认无需继续改写功能：
  23 项针对性测试、123 项默认测试（17 项 opt-in 跳过）、12 项 ClickHouse 集成、7 项
  PostgreSQL 集成、Ruff 与 diff-check 均通过；10,001 行本地遍历、密钥生命周期、真实多来源
  canonical/DuckDB/fallback 等价和旧 API 兼容均由测试覆盖。验收方已独立确认 Artifact
  `ed0e55f`（功能提交 `51b0c2b`）。

### `SV2-013`：raw 多来源范围 keyset 分页

- **目标**：为 raw-history 提供适合大范围遍历的稳定游标分页，同时保留每个来源。
- **范围**：按 `(bar_time, source)` 业务顺序 keyset 前进，游标绑定全部过滤条件；保持
  provenance、版本收敛、limit/truncated 和 ClickHouse→DuckDB 同页回退等价。
- **前置依赖**：`SV2-012` 已验收。
- **验收标准**：无 OFFSET、无丢失重复；同秒多来源跨页不拆错；篡改和跨查询游标在查询前
  返回 400；旧无游标 API 兼容；空页和末页语义确定。
- **必要测试**：多来源跨页、版本冲突、source 过滤、UTC offset、真实 ClickHouse fallback、
  大样本查询计划/SQL 不含 OFFSET；全套回归与静态检查。
- **排除项**：canonical 横截面、非精确时间、缓存策略改造、production 切换。
- **状态**：`已验收`。raw-history 新增显式 `page_size`/`cursor`，复用
  SV2-012 的高熵、持久化、完整性保护游标；DuckDB 和 ClickHouse 均使用
  `(bar_time, source)` keyset 条件并以 `page_size + 1` 判定后页。目标测试覆盖同秒多来源穿页、
  过滤绑定、UTC offset、版本收敛、空页/末页、查询前拒绝非法游标、10,001 行有界遍历；真实
  ClickHouse 成功页与强制 DuckDB fallback 已逐页等价验证。默认套件运行 130 项（18 项
  opt-in 跳过），13 项 ClickHouse、7 项 PostgreSQL 集成、Ruff 与 diff-check 均通过；验收方
  已独立确认 Artifact `ccda668`。

### `SV2-014A`：精确单时间点横截面 keyset 分页

- **目标**：让现有精确单 bar 时间点横截面可稳定分页，不依赖单次 5000 行响应。
- **范围**：只为精确 `bar_at` 查询增加按 symbol 升序的查询绑定 keyset 游标；保持每个 symbol
  最多一条、exact-time、limit/truncated、缓存和 fallback 语义。
- **前置依赖**：`SV2-013`。
- **验收标准**：不混入陈旧值；跨页无丢失重复；游标篡改/跨查询复用在查询前返回 400；
  DuckDB、ClickHouse canonical 与 fallback 的 bars/next_cursor 完全等价；旧无游标 API 兼容。
- **必要测试**：symbol 边界、整页/尾页/空页、过滤绑定、版本 `FINAL`、故障回退、大样本
  keyset 有界性、默认与真实 ClickHouse 回归。
- **排除项**：多时间点、非精确最近值、后台物化、production 切换。
- **状态**：`已验收`。现有 cross-section API 新增显式
  `page_size`/`cursor`，使用已验收的高熵签名、TTL、最大长度和规范化查询绑定；DuckDB 与
  ClickHouse canonical 均按 `symbol > after` keyset 前进并以 `page_size + 1` 判断后页，不使用
  OFFSET。目标测试覆盖 exact-time、symbol 边界、整页/尾页/空页、symbols 去重与绑定、UTC
  offset、非法游标查询前拒绝、10,001 symbols 有界遍历；真实 ClickHouse `FINAL`、成功读取与
  强制 DuckDB fallback 的完整 API 响应（bars、next_cursor、缓存字段）已等价验证。默认套件
  运行 137 项（19 项 opt-in 跳过），14 项 ClickHouse、7 项 PostgreSQL 集成、Ruff 与
  diff-check 均通过；验收方已独立确认 Artifact `8f33237`。

### `SV2-014B`：有界多时间点横截面查询

- **目标**：提供多个精确 bar 时间点的矩形横截面读取，同时防止无界全市场扫描。
- **范围**：新增独立多时间点契约，使用稳定 `(bar_time, symbol)` 顺序；显式限制时间点数、
  symbols 数和总返回行数，并复用已验收的游标完整性与 backend fallback 规则。
- **前置依赖**：`SV2-014A`。
- **验收标准**：每个请求只返回明确列出的精确时间点；矩形边界和截断可诊断；跨页无丢失重复；
  DuckDB、ClickHouse canonical 与 fallback 等价；超限/非法参数在查询前返回 400。
- **必要测试**：多个时间点与 symbols 的笛卡尔边界、稀疏/空结果、UTC offset、分页、超限、
  `FINAL` 收敛、故障回退和大样本有界性。
- **排除项**：非精确最近值、时间范围聚合、后台物化、production 切换。
- **状态**：`已验收`。新增只读 API
  `GET /v1/quotes/cross-section/matrix`，要求显式列出 1～100 个精确时间点和 1～1000 个
  symbols，规范化为 UTC 整数秒并去重；请求矩形限制为 100,000 cells，单页限制为 5,000。
  DuckDB 与 ClickHouse canonical `FINAL` 均按 `(bar_time, symbol)` keyset 前进，以
  `page_size + 1` 判断后页且不使用 OFFSET；安全游标绑定完整时间点、symbols、interval、
  adjustment 和 page_size。测试覆盖稀疏/空矩形、UTC offset、集合去重与绑定、查询前拒绝非法
  或超限请求、版本收敛、10,100 行有界遍历；真实 ClickHouse 成功页与强制 DuckDB fallback
  的 bars、cursor 和缓存 metadata 完全等价。默认套件运行 144 项（20 项 opt-in 跳过），15 项
  ClickHouse、7 项 PostgreSQL 集成、Ruff 与 diff-check 均通过；验收方已独立确认 Artifact
  `34efd83`。

### `SV2-015`：非精确历史与横截面语义

- **目标**：为“截至某时刻最近有效 bar”建立独立契约，防止把该语义混入精确横截面。
- **范围**：定义最大 lookback、交易时段/停牌处理、陈旧度和结果级 `effective_bar_at`；支持
  单 symbol 和有界横截面，继续保留 exact API。
- **前置依赖**：`SV2-014B`。
- **验收标准**：不跨越 lookback；不使用未来数据；陈旧值可识别；backend/fallback 结果与
  cache freshness 一致；查询规模和超时有界。
- **必要测试**：休市、停牌、缺口、时区/跨日、lookback 边界、无结果、跨 backend 等价、
  API 兼容与性能守卫。
- **排除项**：自动补数、上游 refresh、交易日历重构、production 切换。
- **状态**：`已验收`。新增独立只读 API
  `GET /v1/quotes/{symbol}/as-of` 与 `GET /v1/quotes/cross-section/as-of`；`as_of` 统一为 UTC
  整数秒，`max_lookback_seconds` 限制为 1～31,536,000，横截面限制 1～1000 symbols 和
  1～1000 page_size。返回行显式包含 `effective_bar_at`、`staleness_seconds` 和
  `effective_status`；timestamp 始终不晚于 as_of，下界为闭区间。休市、停牌和缺口不引入交易
  日历推断，统一仅在 elapsed-time lookback 内携带上一条，否则无结果，绝不无限扫描或触发补数。
  DuckDB 与 ClickHouse canonical `FINAL` 的单标的/横截面查询及强制 fallback 完整 API 响应
  等价；横截面按 symbol keyset 分页，游标绑定 as_of、lookback、symbols 和 page_size。
  测试覆盖未来值排除、边界内外、跨日/UTC offset、exact/prior/无结果、canonical 选源与版本
  收敛、1000-symbol 规模守卫和查询前拒绝非法参数/游标。默认套件运行 151 项（21 项 opt-in
  跳过），16 项 ClickHouse、7 项 PostgreSQL 集成、Ruff 与 diff-check 均通过；验收方已独立
  确认 Artifact `de2c70f`。

### `SV2-016`：查询契约一致性与数据对账总闸

- **目标**：把 recent/range/page/cross-section/raw/non-exact 的字段、错误、缓存和回退语义
  固化为可重复的契约矩阵，作为后续运维工作的数据一致性闸门。
- **范围**：共享 fixtures、跨 backend golden comparison、随机边界/property 测试和有界
  差异报告；覆盖 source priority、UTC、null、Decimal/Float、版本和 cursor。
- **前置依赖**：`SV2-015`。
- **验收标准**：所有查询类型的 DuckDB/ClickHouse/fallback 结果一致；已知允许差异被文档化；
  mismatch 非零即失败；不依赖不安全的全局 diagnostics 改变数据语义。
- **必要测试**：固定与生成数据、重复/乱序 ingestion、重启、故障注入、API contract snapshot、
  默认/ClickHouse/PostgreSQL/Ruff/diff-check。
- **排除项**：性能优化、后台调度、production 数据。
- **状态**：`已验收`。新增共享一致性模块
  `marketcow.contract_gate` 和单一默认 gate：

  ```bash
  MARKETCOW_HOME=$(mktemp -d) uv run python -m unittest \
    tests.test_storage_v2_contract_gate -v
  ```

  设置 `MARKETCOW_TEST_CLICKHOUSE_*` 后，`tests.test_clickhouse_repositories` 中的
  `test_sv2_contract_gate_all_query_types_and_fallback` 使用真实 ClickHouse 25.8 复跑相同数据域。
  gate 不读取共享 diagnostics 来决定数据结果，不写外部系统；seed 固定为 `16016`，差异最多报告
  50 项，每个文本值最多 500 字符，因此失败报告有界且可复现。

  | 契约 | DuckDB | ClickHouse | 强制 fallback | 核对重点 |
  | --- | --- | --- | --- | --- |
  | recent | 是 | canonical `FINAL` | 是 | 升序、缓存、选源/OHLCVA |
  | closed range | 是 | canonical `FINAL` | 是 | 闭区间、limit/truncated |
  | canonical page | 是 | canonical `FINAL` | 是 | keyset/cursor、provenance |
  | exact cross-section page | 是 | canonical `FINAL` | 是 | exact-time、symbol 顺序 |
  | matrix | 是 | canonical `FINAL` | 是 | `(bar_time, symbol)`、稀疏结果 |
  | raw range/page | 是 | raw `FINAL` | 是 | 多来源、版本、provenance |
  | single as-of | 是 | canonical `FINAL` | 是 | future 排除、effective time |
  | cross-section as-of | 是 | canonical `FINAL` | 是 | lookback、cursor、cache |

  全部契约逐字段规范化 UTC ISO、整数秒业务时间、Decimal/Float、负零、null、列表顺序、缓存、
  cursor/truncated/effective-time 和错误 schema。固定 fixture 包含多来源冲突、相同时间、乱序输入、
  旧 ingestion 晚到、毫秒 provenance、null 与空结果；有界生成测试覆盖 200 组数值/时间表示。
  ClickHouse 连接故障注入后必须与同查询 DuckDB 页完全相同。

  **允许差异只有两类且均按精确路径匹配**：其一，仅顶层 `$.backend`、
  `$.attempted_backend`、`$.fallback`、`$.error` 及同名 `$.diagnostics.*` 路径是有界路由
  diagnostics，不属于数据响应；bars 或其他数据区域中的同名字段仍必须比较。其二，旧
  recent/range 与 raw 查询的 `source_payload` 在 DuckDB
  中保留任意 provider 原始 JSON，而 ClickHouse schema 只保留结构化 provenance，无法无损重建
  provider 私有字段。该字段只在上述旧/raw 契约的跨后端 gate 中以 bar 路径
  `$[].source_payload`、`$[][].source_payload` 或 `$.bars[].source_payload` 显式排除；其他
  位置的同名字段不得忽略；`source`、
  `observed_at`、`ingested_at`、`raw_artifact_id`、OHLCVA 及 canonical page 的结构化 provenance
  仍必须逐字段相同。除此之外 mismatch 非零立即使 gate 失败。当前本地验证：157 项默认发现
  （22 项显式外部集成跳过）通过；共享 gate 5 项通过；一次性 ClickHouse 25.8 上完整 17 项
  集成通过（其中总闸同时覆盖全部查询类型、API snapshot 和 outage fallback）；临时 UTF-8
  PostgreSQL 上 7 项回归通过；Ruff 与 diff-check 通过。
  验收方已独立确认含路径白名单返修的 Artifact `9766d7b`。

### `SV2-017`：后台 canonical 调度与进程生命周期

- **目标**：将当前同步 default-off 衔接扩展为可长期运行、可关闭和可恢复的有界后台机制。
- **范围**：development-only 先实现单实例 lease、有限队列/扫描窗口、退避、优雅关闭、启动恢复、
  手工暂停/恢复和资源关闭；不得无界全表扫描或无限线程。
- **前置依赖**：`SV2-016`。
- **验收标准**：重复任务幂等；崩溃后不丢 intent；同一范围不并发重建；队列/重试/lag 有界；
  DuckDB primary 始终 fail-open；disabled 零副作用。
- **必要测试**：受控时钟、并发 lease、崩溃窗口、backoff、shutdown、积压上限、ClickHouse
  outage/recovery 集成、长时间 soak 的本地缩短版。
- **排除项**：launchd production 安装、远程协调器、正式连接。
- **状态**：`已验收`（Artifact `6d9fc6e`）。新增 `BackgroundCanonicalScheduler`，仅当
  `MARKETCOW_PROFILE=development`、ClickHouse 显式启用且
  `MARKETCOW_CLICKHOUSE_BACKGROUND_CANONICAL=true` 时组装；它与同步
  `MARKETCOW_CLICKHOUSE_AUTO_CANONICAL` 互斥。默认关闭时不创建 scheduler 目录、不获取 lease、
  不启动线程，也不会增加 ClickHouse 连接。

  scheduler 在已验证 development spool 内保存精确 `(symbol, interval, adjustment, start, end)`
  intent，以内容 SHA-256 去重；同一 spool 使用非阻塞进程 lease，且每实例只有一个非 daemon
  worker。队列上限 1～10,000、每轮扫描 1～1,000 项、poll 0.05～60 秒、重试 1～100 次，指数
  backoff 上限 3,600 秒，全部由显式有界配置控制。目录扫描使用有界 `os.scandir` 窗口，不做
  ClickHouse 全表扫描。成功 raw shadow 写入与成功 raw replay 只进行 durable 入队；队列满、
  scheduler 异常或 ClickHouse outage 均 fail-open，不改变已成功的 DuckDB primary。
  raw replay 回调只有在 scheduler intent 已 durable 入队或确认重复后才成功；队列满会让回调失败，
  从而保留既有 raw replay intent，避免恢复链路静默丢失。

  intent 在 rebuild 前原子移至 processing；启动/下一轮扫描会在持有唯一 lease 后有限恢复崩溃
  窗口中的 processing intent。失败持久化 attempts、下一尝试时间和 4,000 字符内错误；达到
  最大尝试数后留在 scheduler 自身 failed 区，绝不无限重试。`pause()`/`resume()` 只控制新任务
  领取，`close()` 唤醒并 join worker、释放 lease；factory 按 scheduler→ClickHouse 的反向顺序
  关闭资源。diagnostics 有界报告 paused/thread/pending/failed/lag/last，不改变数据语义。

  单元测试覆盖受控时钟、退避、并发重复入队、单实例 lease、精确范围、pause/resume、队列与
  扫描上限、崩溃恢复、shutdown、factory 生命周期、primary fail-open、raw replay hook 和 100 项
  缩短 soak；显式 ClickHouse 集成覆盖 outage 后 durable pending 与恢复构建。排除 SV2-018 的
  operator 清单、清理、dead-letter 管理与其他 spool 运维能力。后台机制使用独立 ClickHouse
  client/session，避免与 API 读取线程并发复用不安全 client；factory 统一管理两个 client。
  当前本地验证：40 项 scheduler/shadow/config 目标测试通过；167 项默认发现（23 项 opt-in
  外部集成跳过）通过；一次性 ClickHouse 25.8 上完整 18 项通过；临时 UTF-8 PostgreSQL 7 项
  通过；Ruff 与 diff-check 通过。

### `SV2-018`：WAL/spool 运维与故障恢复闭环

- **目标**：让 operator 能安全观察、重放、隔离和清理 spool，而不靠直接修改文件。
- **范围**：只读清单/诊断、显式重放/重试、dead-letter、保留期与磁盘配额、校验和、损坏项隔离、
  intent/WAL 一致性审计；所有修改命令须显式且可恢复。
- **前置依赖**：`SV2-017`。
- **验收标准**：损坏或超限不阻塞健康项；配额前有告警且不写入 production root；并发操作串行；
  replay 结果可追溯；清理只针对已确认完成项。
- **必要测试**：文件损坏、磁盘满模拟、权限错误、并发、重启、保留期、symlink/路径逃逸、
  CLI/API 安全边界及 ClickHouse 恢复。
- **排除项**：自动删除原始归档、production spool 操作、远程存储。
- **状态**：`已验收`（Artifact `52ecd0c`）。新增稳定的本地命令入口
  `marketcow --profile development spool <action>`，提供机器可读 JSON 与明确退出状态；支持有界
  `status`、`list`、`audit`、`migrate-legacy`、`replay`、`quarantine-corrupt`、`retry-dead` 和
  `cleanup-replayed`。只读命令在 spool 尚不存在时返回空状态且不创建目录；所有改变状态的操作均
  获取同一 `.operator.lock`，并与 writer replay 和 canonical scheduler 串行。每次 operator
  mutation 追加 fsync 的有界错误审计记录；并发占用返回明确 busy/失败结果。

  spool JSON 新写入均携带基于规范化完整 payload 的 SHA-256 校验和。list/audit/replay 会验证校验和；
  截断 JSON、非对象 JSON、校验和不符或权限错误按单项处理，损坏 WAL 可移入 quarantine，健康项仍
  继续重放。审计将 raw pending WAL 与 raw replay intent 的 pending batch 关联，并报告缺失引用、
  orphan WAL、scheduler pending/processing/failed 与 quarantine 状态。scheduler failed dead-letter
  只能通过显式、有界 retry 重置后移回 pending；replayed 清理仅删除具备有效校验和、明确
  `replayed_at` 且达到保留期的确认完成项，绝不删除 raw archive。

  development spool 配额显式限制在 1 MiB～1 TiB，预警阈值限制为 0.5～小于 1；每次原子写入前
  同时检查逻辑配额和文件系统可用空间，超限时拒绝且不留下半文件。operator、writer 和 scheduler
  继续受已验证 `storage_root` containment 约束；内部目录 resolve 后逃逸或 symlink 目标逃逸也会
  拒绝，production profile 的 spool CLI 始终拒绝。replay 的 replay/operator 锁均在异常路径释放，
  单个损坏项会被隔离且不会阻断后续健康项。

  从 `6d9fc6e` 及更早版本升级时，无 `_checksum` 的既有 WAL、replayed WAL、raw
  intent/processing intent 与 scheduler pending/processing/failed 不会被直接视为损坏。启动组装、
  scheduler 重启和显式 replay 会在统一 operator lock 下执行可重入 legacy migration；operator 也可
  显式执行 `migrate-legacy`。只有字段集合严格匹配旧 schema、类型/长度有界、时间有效，且文件名与
  stable batch/intent/task 业务哈希一致的项目才会通过原子重写补签。未知字段、非法业务键、越界
  payload 或伪造文件名会隔离到 quarantine；配额或权限导致原子补签失败时保留原项，不会先删除。
  每轮迁移记录 checked/migrated/invalid/errors/truncated 与 durable operator audit，崩溃后重复执行会
  跳过已正确签名项并继续未完成项。迁移 `limit` 只计算实际无 checksum 候选，已签文件不消耗
  预算；每个 kind 独立执行最多 10,000 项的硬上限扫描，前置 kind 的已签前缀不会阻断 raw intent
  或后续 scheduler kind。结果另行报告 scanned、remaining、scan_truncated，重复调用或重启后
  remaining 会随已扫描候选处理真实归零；若目录超过扫描边界则 `remaining_exact=false` 且
  `scan_truncated=true`，不会伪称完整。scheduler/writer 在迁移后只消费校验和有效的持久项。

  若有效 legacy 项因权限、配额或磁盘空间无法原子补签，migration 将其计入
  errors/remaining，writer replay 报告 `legacy_blocked` 并保持 remaining/truncated，不会在同轮把
  缺 checksum 的有效项误送 quarantine，也不会调用 ClickHouse。raw processing intent 同样计入
  remaining。scheduler 启动和每轮领取前都在同一 operator lock 下重试补签；补签失败时不移动
  processing intent、不构建，原路径与原字节保持不变；权限或空间恢复后才补签、恢复并执行一次。
  非法 schema 仍隔离，quarantined 只统计实际成功移动的项目。

  当前本地验证：44 项 operator/writer/scheduler/config 目标测试通过；183 项默认发现（23 项 opt-in
  外部集成跳过）通过；一次性 ClickHouse 25.8 上完整 18 项集成通过；临时 UTF-8 PostgreSQL 7 项
  回归通过；Ruff 与 diff-check 通过。范围未进入 `SV2-019A`，未增加 production spool、远程存储、
  自动 raw archive 删除、部署或远程写入。

### `SV2-019A`：有界指标、诊断与结构化日志

- **目标**：提供足以观察写入、重放、canonical、查询和 fallback 的稳定 telemetry 契约。
- **范围**：有界 counters/gauges/histograms、diagnostic snapshot 和 structured logs；覆盖 ingest
  latency、pending/failed/replayed、canonical lag、mismatch、query latency/fallback、cache
  freshness、merge/disk pressure；敏感错误截断和脱敏。
- **前置依赖**：`SV2-018`。
- **验收标准**：指标与 label 基数有界；快照并发安全；日志不泄露凭证/敏感路径；disabled backend
  不产生连接副作用或误导数据；telemetry 不改变读写结果。
- **必要测试**：固定时钟、error 脱敏/长度、label cardinality、并发快照、故障注入、schema 回归。
- **排除项**：health/readiness 状态机、SLO 判定、外部监控或告警发送。
- **状态**：`本地实现完成，待独立验收`。新增 `storage-v2.telemetry.v1` process-local telemetry
  契约，进程重启时明确 reset，不承诺跨进程累计。核心使用单一 `RLock` 保护 counter/gauge/
  histogram 更新、并发 snapshot 与 200 项内存环形结构化日志；无文件、网络、线程或外部 exporter。
  snapshot 固定包含 schema、UTC generated_at、restart_semantics、ClickHouse enabled、metrics、logs、
  dropped_updates 和硬 limits。telemetry 自身异常由 `safe` 吞并计入 dropped，不改变业务返回或异常。

  指标使用固定名称、单位和枚举 label：ingest/write latency；WAL pending/failed/replayed/quarantine；
  canonical pending/processing/failed、lag 与 rebuild outcome；contract mismatch（固定查询契约枚举）；
  query latency、backend/fallback；cache fresh/stale/empty/miss age；ClickHouse merge queue/disk used ratio。
  histogram 桶在代码 schema 中固定，秒数统一使用 seconds；counter 64-bit 饱和、gauge/observation
  数值有界。所有 label 值均按白名单归一化，额外 label、symbol、任意 backend/query/status 会拒绝；
  全 schema 最大 series 数小于 500。ClickHouse disabled 时仍组装纯内存、process-local telemetry 以覆盖
  DuckDB primary write/query/cache，但不组装 ClickHouse backend、不连接、不建 spool 目录、不启线程；
  disabled snapshot 只显示 enabled=false 且不伪造 pressure series。

  Stage1 factory 始终创建同一个纯内存 telemetry；它在保持 Warehouse 对象身份的前提下测量默认 DuckDB
  primary quote/bar write、全部 market-bar query 和 API cache freshness。ClickHouse 显式启用后，同一实例
  才进一步接入 writer/WAL、scheduler/canonical、所有 canonical/raw 查询与 fallback、contract gate 及
  ClickHouse pressure 采样入口；Shadow diagnostics 提供本地 snapshot。所有 wrapper 保留原返回与异常，
  telemetry 失败 fail-open。结构化日志 event/severity 也是固定枚举，字段/列表数量有界；mapping key 与
  value（包括嵌套结构）均统一将凭证、token/API key、DSN URL 与绝对敏感路径替换为 REDACTED，并将所有
  文本限制为 1,000 字符。

  首次独立验收发现默认 DuckDB telemetry 缺口和 mapping key 泄露，本修订仅收口这两项，未进入
  `SV2-019B`。新增默认 DuckDB 真实写入、查询、API cache 路径回归，证明三类指标均出现，同时
  ClickHouse client 未调用、spool 目录未创建且无 pressure series；新增嵌套敏感 mapping key 回归。
  第二次独立验收进一步发现 DuckDB wrapper 在业务调用前直接调用 telemetry clock，clock 异常会阻断
  primary。本修订引入统一 `telemetry_call`/`telemetry_elapsed` fail-open 边界，并核对 DuckDB、ClickHouse
  shadow/writer/scheduler、API cache、contract gate 与 diagnostic snapshot 接入：clock、safe/metric、log、
  snapshot 任一调用异常均只丢弃观测，不能改变业务返回、原始异常或已发生副作用。故障注入同时证明
  正常 DuckDB write/query/API 成功，以及 primary 自身异常的类型与消息保持不变。
  当前本地验证：9 项 telemetry 固定时钟/schema/桶/单位/cardinality/并发/脱敏/故障测试通过；
  contract gate + telemetry + writer/scheduler/operator 合计 41 项通过；190 项默认发现（23 项 opt-in
  外部集成跳过）通过；一次性 ClickHouse 25.8 上 18 项集成通过；临时 UTF-8 PostgreSQL 7 项回归
  通过；Ruff 与 diff-check 通过。Artifact `a0ce718` 已通过独立验收。范围未进入 `SV2-019B`，未实现
  readiness/degraded 状态机、SLO、Prometheus/OTel、外部告警、部署或远程写入。

### `SV2-019B`：健康状态与本地 SLO 判定

- **目标**：基于已验收 telemetry 给出稳定 readiness/degraded 状态和可执行的本地 SLO 闸门。
- **范围**：定义阈值、观察窗口、状态转换、恢复条件和有界原因；扩展本地 health/readiness 契约，
  区分 disabled、healthy、degraded 和 unavailable。
- **前置依赖**：`SV2-019A`。
- **验收标准**：相同指标输入产生确定状态；抖动受窗口/滞回控制；响应不泄露凭证或绝对敏感路径；
  disabled backend 不误报；SLO 与停止条件在文档中可直接引用。
- **必要测试**：固定时钟状态转换、阈值边界、恢复/抖动、缺指标、错误脱敏、API schema 和并发读取。
- **排除项**：外部 dashboard、告警消息发送、production 监控部署。
- **状态**：`本地实现完成，待独立验收`。新增 `storage-v2.health.v1` 本地、process-local、并发安全的
  `StorageHealthEvaluator`，只读取已脱敏 telemetry snapshot，不执行网络、文件或后台线程操作。
  `/v1/health` 保留原 `status=ok`、version/profile/database/backend 字段并向后兼容地增加
  `storage_health`；新增 `/v1/readiness`，disabled/healthy/degraded 返回 200 且 `ready=true`，只有
  unavailable 返回 503。disabled 明确表示 DuckDB primary 可用且 ClickHouse 未启用，不伪称 ClickHouse
  healthy；enabled 但尚无 merge/disk pressure 指标明确为 degraded；无效/不可读 snapshot 为 unavailable。
  为保留旧消费者字段名，`/v1/health.database` 继续存在，但语义从 filesystem path 收紧为
  `storage://<相对 storage_root 的逻辑路径>`；凭证片段继续脱敏，无法安全相对化时返回
  `[REDACTED_PATH]`，绝不输出绝对路径。profile、metadata_backend 和旧顶层 status 语义不变。

  固定阈值与单位：disk used ratio `>=0.85` degraded、`>=0.95` unavailable；merge queue items `>=50`
  degraded、`>=200` unavailable；任一 WAL failed 或 quarantine 项 degraded，quarantine `>=10`
  unavailable；telemetry dropped update degraded。普通 degraded 条件必须连续 30 秒，critical 条件必须
  连续 10 秒；恢复到更轻状态必须连续干净 60 秒。候选状态或输入恢复会重置窗口，形成滞回并阻止阈值
  抖动。disabled、缺 pressure 指标及 telemetry snapshot 不可用是配置/可观测性事实，立即转换而不等待。
  响应包含固定 schema、status/ready/backend、UTC observed_at、candidate 状态/起点、固定 thresholds、
  process-local sustained-condition window 和最多 8 条、每条 240 字符的枚举/脱敏原因；不返回原始异常、
  symbol、query、DSN 或路径。

  **可直接引用的本地 SLO 与停止条件**：DuckDB-only 开发运行的目标状态必须为 disabled 且 readiness
  200；显式 ClickHouse development 运行在预热/采样后必须为 healthy。degraded 可继续 DuckDB-primary
  服务但禁止进入下一迁移/切换步骤，须在 15 分钟内恢复且期间不得出现 unavailable；任一 unavailable、
  readiness 503、quarantine `>=10`、disk `>=0.95` 或 merge queue `>=200` 均立即停止迁移/切换演练并保留
  Artifact 诊断。恢复后必须连续 60 秒 healthy 才可解除停止条件。上述仅为本地 gate，不自动发告警、
  不改变业务读写，也不授权 production 操作。

  本地测试覆盖固定时钟、阈值包含边界、10/30/60 秒状态转换、候选重置与抖动、disabled/缺指标/
  snapshot 不可用、原因脱敏与上限、800 次并发读取以及 health/readiness API schema。范围未进入
  `SV2-020A`，未增加 dashboard、告警发送、production 监控部署或远程写入。Artifact `7d8c646`
  （含完整 health/readiness 响应脱敏修订）已通过独立验收。

### `SV2-020A`：可校验 Parquet 冷归档原语

- **目标**：把隔离 development 行情按确定分区原子导出为可校验、可重放的 Parquet Artifact。
- **范围**：按 market/interval/source/year/month 导出；生成 manifest、checksum、schema/version、
  watermark；提供 DuckDB 离线 round-trip 查询与回填读取接口。
- **前置依赖**：`SV2-019B`。
- **验收标准**：导出前后行数/业务键/校验和一致；重复导出幂等；部分失败可恢复；schema evolution
  明确；损坏 Artifact 在查询/回填前被拒绝。
- **必要测试**：月界/时区、重复导出、崩溃窗口、损坏文件、schema evolution、DuckDB round-trip、
  代表性压缩率。
- **排除项**：retention/删除候选、在线数据删除、云对象存储和 production 数据。
- **状态**：`本地实现完成，待独立验收`。新增 development-only `ParquetColdArchive`，构造时强制
  archive root resolve 后位于显式 storage root 内，production profile 与路径逃逸均拒绝。它只读取
  DuckDB `market_price_bar`，按确定的 `market/interval/source/year/month`（UTC 月界）过滤，并按
  `(symbol, interval, adjustment, timestamp, source)` 稳定排序。目录布局使用 Hive 风格分区，具体
  Artifact 以逻辑内容 SHA-256 前 24 位命名；每个 Artifact 是包含 `data.parquet` 与 `manifest.json`
  的独立目录。

  导出先在 archive 内 `.staging` 写 ZSTD Parquet、fsync 数据，再写/fsync manifest，最后以单次目录
  `os.replace` 原子发布并 fsync 父目录；发布前异常清理 staging，发布后崩溃则下次以相同内容 ID 验证
  并复用已发布 Artifact。重复输入不会生成第二份 Artifact。版本化 manifest 固定记录 manifest/schema
  version、partition、字段名/类型、业务键、行数、逻辑内容 checksum、业务键 checksum、Parquet checksum、
  manifest payload checksum、文件/逻辑字节数，以及 timestamp/ingested_at watermark。

  `verify` 在任何 query/backfill 前依次校验 manifest JSON/version/checksum、schema version、业务键、
  Parquet SHA-256、实际 Parquet schema/row count、逻辑内容与业务键 checksum；损坏或未知 evolution
  均安全拒绝。离线 `query` 只允许固定白名单 predicate，使用 DuckDB `read_parquet` 且显式关闭路径的
  Hive 自动列注入，稳定排序并限制 1..100000 行；`read_for_backfill` 复用完整验证并对超过 100000 行
  的单次读取安全拒绝。当前 schema v1 只允许显式升级版本，不静默接受增删列或类型漂移。

  本地测试覆盖 UTC 月界/offset、两个月独立分区、DuckDB round-trip/backfill、重复导出幂等、发布前与
  发布后崩溃恢复、Parquet/manifest 损坏、未知 schema evolution、development/allowed-root 隔离、
  1000 行代表性 ZSTD 压缩（文件小于逻辑 JSON 的 50%）及业务键完整性。范围未进入 `SV2-020B`，
  未实现 retention、删除候选、在线删除、云对象存储、production 数据处理、上传或远程写入。

  首次独立验收发现重签后的派生 metadata 未与真实内容绑定，以及 rename 前缺 staging directory fsync。
  修订后 `verify` 从无排序修饰的真实 Parquet 行重新推导并严格比对：每行 market/interval/source 与
  UTC year/month 边界、min/max timestamp 与规范化 ingested watermark、artifact ID、内容寻址目录及完整
  Hive 分区路径、Parquet/logical bytes、dataset、schema、row count、业务键唯一性与物理稳定顺序；即使
  攻击者修改 partition/watermark/ID/size 后重新计算 manifest payload hash，也会在 query/backfill 前拒绝。
  artifact 目录及 manifest/data 文件逐层 resolve containment，文件 symlink 明确拒绝。

  发布持久性顺序现固定为：Parquet file fsync → manifest file fsync → staging 及其父目录 fsync → 创建并
  从分区目录到 archive root 逐层 directory fsync → rename → source staging parent 与 final partition
  directory fsync。archive root
  的本地 flock 将同内容并发导出串行化，8 路并发回归确认只产生一个已验证 Artifact。目标测试扩展为
  10 项，并新增所有派生字段重签篡改、目录/manifest 错配、artifact/file symlink 逃逸、目录 fsync 调用
  顺序及并发发布覆盖。
  Artifact `0a34202` 已通过独立验收。

### `SV2-020B`：冷热保留策略与安全删除候选

- **目标**：依据已校验归档定义可审计的在线/冷数据边界，但不实际删除数据。
- **范围**：按数据集、来源和时间定义 retention policy；只有 `SV2-020A` Artifact 校验通过且
  watermark 完整时才生成显式删除候选、估算回收空间并验证冷查询路径。
- **前置依赖**：`SV2-020A`。
- **验收标准**：策略确定且可 dry-run；未归档、未校验、仍在安全窗口或被 hold 的分区绝不成为候选；
  候选可追溯到 manifest；冷查询仍返回等价数据。
- **必要测试**：策略边界、hold、缺 manifest/checksum、重复 dry-run、时间/月界、冷查询等价和空间估算。
- **排除项**：任何实际 DELETE/TTL、production 数据清理、远程归档上传。
- **状态**：`本地实现完成，待独立验收`。新增版本化 `storage-v2.retention-policy.v1` 与纯只读
  `RetentionDryRun`。policy 固定数据集为 `market_price_bar_raw`，定义默认 retain days、1..365 天安全
  窗口及逐 source retain days 覆盖；retain 规则限制在 30..3650 天，`as_of` 必须带时区并统一 UTC。
  月分区只有在 partition end 不晚于 `as_of - max(retain_days, safety_window_days)` 时才通过时间闸门。
  hold 使用完整确定 partition ID（market/interval/source/year/month），命中后无条件排除。

  dry-run 只接收显式 Artifact 列表并稳定排序。每项先执行 SV2-020A 完整 verify；随后从当前 DuckDB
  对应在线月分区重新读取稳定排序行，严格比对 row count、逻辑 checksum 和业务键 checksum，再通过
  已验证 Parquet backfill 读取比较冷热结果。缺 Artifact/manifest/checksum、损坏、重签语义错误、在线
  新增/变化导致 watermark 覆盖不完整、仍在安全/retention 窗口或 hold 均只进入有界 excluded reason，
  绝不成为候选；未提供已归档 Artifact 的在线数据不会凭空成为候选。

  候选含稳定 candidate ID、dataset/partition、artifact ID、archive-root 相对逻辑 URI、manifest payload 与
  Parquet checksum、watermark、row count、cold-query equivalence 以及以完整逻辑行 JSON bytes 为方法的
  `estimated_reclaim_bytes`；report 汇总 policy hash、holds、候选/排除数量、估算空间并固定
  `dry_run=true`、`mutations_performed=0`、`action=candidate_only_no_delete`。相同 policy/as_of/Artifact/hold
  输入逐字段幂等。

  6 项本地测试覆盖 UTC offset 与月界阈值、source-specific retention、hold、缺失/损坏/重签 manifest、
  在线分区新增导致覆盖不完整、空 Artifact 列表、冷热等价、空间估算、policy/version/时区边界和重复
  dry-run。副作用回归在规划期间将 `Path.unlink`、`shutil.rmtree` 与 `os.remove` 设为立即失败，并确认
  dry-run 仍成功、在线 DuckDB 行数以及 Parquet/manifest 字节完全不变。范围未进入 `SV2-021A`，没有
  DELETE/TTL、实际清理、production 数据、远程归档上传、部署或远程写入。

  首次独立验收发现 frozen dataclass 仍持有调用方 source-rule Mapping，以及顶层 action 缺失。本修订
  在构造时严格校验 key/type（显式拒绝 bool/float）、trim 规范化、碰撞/数量/天数边界，再复制、排序并
  以只读 MappingProxyType 冻结；外部字典后续 mutation 不会改变 days_for/document/policy hash。
  report 顶层与 candidate 均固定 `action=candidate_only_no_delete`。完整 policy hash 和由规范化 as_of、
  holds、artifact 输入组成的 input hash 同时绑定 candidate ID 并显式写入候选，消除同版本不同规则歧义。

  artifacts 与 holds 各有 1000 个唯一输入的硬上限，超过时在任何 Artifact 读取前拒绝；excluded 因而
  同样最多 1000 项，limits 在 report 机器可读。单 Artifact verify/在线读取/backfill/锁/权限异常只生成
  `artifact_verification_failed` 或 `artifact_read_failed`，detail 仅保留异常类型，失败路径仅输出 hash
  reference；其余健康 Artifact 继续评估。目标测试扩展为 9 项，新增外部字典 mutation、规则类型边界、
  单项 PermissionError 与健康候选共存、输入上限和完整 no-mutation schema 回归。
  Artifact `134c032` 已通过独立验收。

### `SV2-021A`：备份清单与本地备份 Artifact

- **目标**：为 Storage V2 各持久化组件生成一致、可校验的本地备份 Artifact。
- **范围**：覆盖 PostgreSQL、ClickHouse、DuckDB 兼容数据、Parquet/artifact、spool 与 cursor key；
  定义 RPO/RTO 假设、组件版本、watermark、manifest、checksum、密钥权限和 canonical 可重建边界。
- **前置依赖**：`SV2-020B`。
- **验收标准**：备份集合完整、重复执行确定、组件间 watermark 可解释；故意损坏可被校验发现；
  Artifact 不含明文凭证且仅位于隔离本地目录。
- **必要测试**：全量/增量样例、一致性 watermark、checksum 损坏、权限、缺失组件、重复备份、
  manifest schema 与敏感信息扫描。
- **排除项**：恢复执行、真实 production 数据、远程备份目的地或上传。
- **状态**：`已验收`（Artifact `25f833f`）。新增 development-only `LocalStorageBackup` 与显式
  `BackupComponent` 适配器。固定完整组件集合为 PostgreSQL、ClickHouse、DuckDB compatibility、
  cold Parquet artifacts、spool/WAL/intent 和 cursor key；缺一或多一均在写入前拒绝。PostgreSQL 与
  ClickHouse 提供只读逻辑 JSON extractor（表/行确定排序），其余本地树适配器强制 source resolve
  containment、拒绝 symlink，并限制单组件 10000 文件。调用方显式提供 snapshot/captured watermark，
  所有时间规范化 UTC，组件 watermark 不得晚于 snapshot；manifest 汇总 earliest/latest/snapshot，记录
  每组件 kind/version/watermark、canonical rebuildable 边界、RPO（显式本地 snapshot）与 RTO（本地恢复
  演练目标 60 分钟）假设。

  bundle manifest 版本为 `storage-v2.backup-manifest.v1`，记录 full/incremental 与 base backup ID、
  content-addressed backup ID、manifest payload checksum，以及每个文件的相对路径、SHA-256、bytes 和
  权限。cursor key 不以明文进入 Artifact：使用外部至少 32-byte wrapping key、确定 nonce 的 HMAC-SHA256
  authenticated stream sealing，备份文件权限固定 0600；其他文件固定 0640。创建与验证共用结构感知、
  fail-closed 的敏感信息校验：递归检查所有非 cursor JSON/text payload 与最终 manifest 的字符串 key/value，
  拒绝 password/passwd/secret/token/API/access key、authorization/cookie、DSN 与带凭证数据库 URI；普通
  schema/列名元数据不会仅因名称被误判。cursor ciphertext 仅走认证校验，wrapping key 与 cursor 明文均不
  写入 manifest/bundle。payload 或 manifest 即使被重新计算 checksum，也会在读取前重新执行敏感扫描。

  生成路径只能位于 development storage root 内；所有普通文件 fsync 后写/fsync manifest，再 fsync
  staging 目录层级，通过 archive-root flock 串行并以单次目录 rename 原子发布，随后 fsync source/dest
  parent。相同 inputs/snapshot 复用同一已验证 ID；incremental 样例显式绑定 base ID。`verify` 在任何未来
  恢复前校验 manifest version/hash、backup ID/目录、完整组件集合、逐组件版本/watermark、精确文件库存、
  symlink/containment、逐文件 bytes/checksum/权限及敏感信息，缺失或损坏安全拒绝。本项没有 restore API。
  verify 还会从不含 ID 的 manifest 内容重算 content-addressed backup ID、从六组件 captured_at 重算
  cross-component watermark、拒绝重复组件/文件库存，并以外部 wrapping key 验证 cursor sealed payload
  HMAC tag；仅重签 manifest 或错配目录不能绕过。

  8 项本地测试覆盖 full/incremental、重复幂等、跨组件 watermark、manifest/file 损坏、缺组件、cursor
  权限与明文负向扫描、source/artifact symlink、嵌套 JSON secret、URI/authorization/cookie、manifest
  metadata 与重签注入、正常 schema 列名、publish 崩溃清理、8 路并发互斥及唯一 rename。
  显式 PostgreSQL/ClickHouse 集成套件各增加一项真实 disposable schema 的逻辑导出验证。
  范围未进入 `SV2-021B`，不执行恢复、不连接 production、不上传、部署、push 或远程写入。

### `SV2-021B`：空环境恢复演练

- **目标**：证明 `SV2-021A` 的本地备份可按确定顺序恢复为可用 Storage V2 环境。
- **范围**：仅使用一次性本地实例与合成数据，恢复数据库、归档、spool 和 cursor key，并验证
  migration、canonical 重建边界和失败回退手册。
- **前置依赖**：`SV2-021A`。
- **验收标准**：空环境恢复成功；行数、业务键、PIT、查询契约、cursor 验证与 artifact 引用一致；
  缺失/损坏/版本不兼容时安全停止且给出恢复步骤。
- **必要测试**：跨版本 migration、丢单组件、密钥缺失、checksum 失败、部分恢复重试，以及恢复后
  默认/PG/CH 契约套件。
- **排除项**：生成新备份策略、production restore、远程数据读取。
- **状态**：`已验收`（Artifact `8b6aa74`）。新增 development/test-only `LocalStorageRestore`，只接受名称与
  profile 均明确隔离的空目标根、空 PostgreSQL schema 和空 ClickHouse database。恢复前一次性验证完整
  full→incremental 链：每个 bundle 的 manifest/checksum/文件库存/cursor HMAC、base ID、snapshot 顺序、
  固定六组件集合以及 kind/version allowlist；错误 wrapping key、未知版本、缺失/乱序 base、非空或 symlink
  目标在创建 checkpoint 和写出 cursor 明文前 fail-closed。

  恢复顺序固定为 PostgreSQL migration+逻辑行、ClickHouse migration+raw/canonical 行、DuckDB compatibility
  文件、cold artifacts、spool/WAL/intents、cursor key。目标根内 `.storage-v2-restore/checkpoint.json` 采用临时
  文件 fsync+rename+目录 fsync 原子持久化，带内容 checksum 与严格步骤前缀校验，并以本地 flock 串行；
  每个组件写完和 checkpoint 更新之间的崩溃窗口均允许依据精确内容安全重入。数据库未 checkpoint 的步骤
  通过 migration 幂等与冲突收敛重试，
  文件树只接受与 bundle 完全相同的已发布内容，cursor 原子解封写入且权限固定 0600。恢复完成的旧 cursor
  token 可由恢复密钥继续验证。

  PostgreSQL/ClickHouse 的 schema_migrations 始终由当前代码按版本顺序建立，不覆盖迁移历史；备份业务行按
  原表/列恢复。ClickHouse DateTime/UInt 类型在逻辑 JSON 回填时规范化；无 offset 的 ClickHouse DateTime
  备份值明确按 UTC 恢复，避免 development 主机时区导致偏移。cold 与 spool 保持 bundle 相对路径、
  checksum 和权限，canonical 数据与 manifest 的 verified raw+spool watermark 边界一并记录，绝不越过该
  snapshot 扫描 production 或远程数据。

  最终 `report.json` 只记录 backup IDs、步骤、耗时/RTO、RPO、组件逻辑版本、cross-component watermark、
  canonical boundary、人工 gate 与丢弃 disposable 环境回退步骤；不包含 DSN、凭证或绝对目标路径。5 项
  本地恢复测试覆盖全部六组件边界故障重启、重复恢复、错误密钥零写出、非空/symlink/production 目标、
  full/incremental 顺序、未来版本、cursor 旧 token、报告脱敏；真实一次性 PostgreSQL 16 + ClickHouse 25.8
  联合演练使用可打开的 Warehouse DuckDB、由 `ParquetColdArchive.export_partition` 生成的真实 ZSTD Parquet、
  `ReliableClickHouseWriter` 故障产生且带 checksum/intent 的真实 spool。恢复后直接由目标 Warehouse、
  PostgreSQL repository、ClickHouse repository/writer/builder、ColdArchive 和 FastAPI history route 读取：验证
  PostgreSQL fundamental PIT 与 artifact 引用、ClickHouse raw FINAL、DuckDB range/as-of、两页 cursor/cache、
  DuckDB↔canonical golden 字段、cold verify/query/backfill↔在线对账；恢复 WAL replay 一次后 pending/intent
  清零，重复 replay 零消费，并由 intent callback 仅在 manifest latest watermark 内 rebuild canonical，故意
  插入的 boundary 后 raw 行没有 canonical 结果。`record_verification` 将上述 gate 以最多 50 项、敏感扫描后
  原子追加到本地报告，保留 RPO/RTO、失败续跑与丢弃 disposable 环境回退说明。
  范围未进入 `SV2-022A`，不执行 production 恢复、不读取远程数据、不上传、部署、push 或远程写入。

### `SV2-022A`：迁移回填与增量追平演练

- **目标**：用可丢弃本地数据证明 DuckDB→PostgreSQL/ClickHouse 回填可中断续跑并追平双写。
- **范围**：版本化 preflight、checkpoint/watermark、幂等 backfill、双写增量追平和全域对账；
  所有命令默认指向 development/test 且拒绝 production 标识。
- **前置依赖**：`SV2-021B`。
- **验收标准**：重复迁移幂等；中断续跑无丢失重复；乱序/重复输入收敛；lag 归零且全域对账通过；
  失败注入均有恢复路径。
- **必要测试**：多阶段中断、schema upgrade、乱序/重复、watermark 边界、增量追平、组件恢复组合和
  完整合成数据演练报告。
- **排除项**：读开关切换、回滚执行、真实 production 连接或数据。
- **状态**：`已验收`（Artifact `6b44732`）。新增 development/test-only `LocalStorageBackfill`，仅接受显式
  `allowed_root` 下名称以 `development/test` 结尾的状态根、PostgreSQL schema 与 ClickHouse database；
  profile、任一逻辑标识或 DuckDB 路径含 production、symlink/路径逃逸、迁移版本不完整、目标非空或本地
  容量不足均在写 checkpoint 前拒绝。新 run 将 DuckDB `CHECKPOINT` 后复制冻结快照，source 文件指纹、
  目标逻辑标识、格式版本共同生成稳定 run ID；checkpoint 以 checksum、临时文件 fsync+rename+目录 fsync
  原子持久化，并由同一状态根 flock 串行。

  PostgreSQL 回填采用显式 16 域 allowlist，覆盖 control plane、artifact、calendar、Tushare、fundamental/PIT
  history、BaoStock/TDX、validation 和 funnel；每域按完整业务键 keyset 分批，禁止 OFFSET，并使用目标主键
  幂等 upsert。`market_price_bar` 按 `(symbol, interval, adjustment, timestamp, source)` keyset 规范化为现有
  `market_bar_raw` 契约，通过 `ReliableClickHouseWriter` 写入；完整触及范围持久化后由现有
  `CanonicalMarketBarBuilder` 有界重建，写入 spool 或 rebuild 截断时不推进阶段。每个批次均在 write 前、
  write 后及 checkpoint 后提供故障注入边界；崩溃后从最后 durable key 续跑，canonical 范围不会因进程退出
  丢失。

  冻结 snapshot 完成后，对活动 DuckDB 进行完整有界 keyset catch-up pass。每轮 upsert 继续复用最新
  ingested_at 与相同版本 content-rank 的确定性收敛规则；16 个 PostgreSQL 域和行情域按内容计算的逻辑
  source fingerprint 在一轮前后相同才进入最终 canonical。最终 raw 集合通过共享
  `CanonicalMarketBarBuilder.build_rows(raw, [])` 生成确定性 generation，先安全清空 disposable canonical
  表再完整写入；写入 spool、少写或异常均不推进。随后要求 catch-up 已记录 fingerprint、reconcile 前
  fingerprint、reconcile 后 fingerprint 三者相同，窗口内发生增量则回到下一 catch-up pass；最多 10 轮
  （可显式限制到 1..100），否则以 lag 未归零失败。

  全域闸门逐 PostgreSQL 表比较行数、稳定业务键顺序和规范化内容 checksum；JSON、UTC、Float 表示先
  规范化。raw 与 canonical 均从活动 DuckDB 独立推导期望行，再对 ClickHouse FINAL 的完整业务键、OHLCVA、
  selected source、source_count、observed/ingested、raw_artifact_id、quality、fingerprint、version 与 updated_at
  逐列 checksum；“数量相同但值/来源/version 错误”同样阻止完成。contract gate 必须由调用方绑定实际迁移
  目标；任何非白名单 mismatch 阻止 complete。报告的 lag 由 completion fingerprint 等于最后稳定 fingerprint
  推导，不再硬编码，只保留逻辑标识、watermark、批次/pass、域状态与有界恢复说明，不含 DSN、凭证或
  绝对路径。

  12 项默认单元测试覆盖隔离/未知版本拒绝、checkpoint 篡改、JSON/Float、provenance、同数量 canonical
  负向差异、PG/CH outage、market write 与 checkpoint 崩溃窗口、spool/truncated rebuild 不推进、reconcile
  窗口增量二次追平和脱敏报告。一次性 PostgreSQL 16 + ClickHouse 25.8 联合演练为全部 16 个 PostgreSQL
  域填入非空合成数据，含 JSON、复合键和两版本 PIT history；同时构造乱序 ingestion、同一 bar_time
  多来源冲突与 source priority。演练验证未知 PG/CH migration fail-closed、PG batch 中断、ClickHouse outage
  →spool/replay、活动 DuckDB control-plane update 与行情增量 catch-up，并在真实目标执行 canonical/raw golden
  comparison 与 PostgreSQL as_of 查询；最终 16 域 checksum 全通过，raw=3、canonical=2、优先来源
  `baostock`、version=1、lag=0，重复运行 run ID 稳定且无逻辑重复。当前默认套件 249 项通过（27 项显式
  集成跳过）；独立 PostgreSQL 8 项、
  ClickHouse 19 项、contract gate 5 项、Ruff 与 diff-check 均通过。

  范围未进入 `SV2-022B`：未切换任何读取开关、未执行回滚、未连接 production，也未部署、上传、push 或
  执行其他远程写入。

### `SV2-022B`：本地读切换与回滚演练

- **目标**：在已追平的 disposable 环境中验证 ClickHouse 读取切换、观察停止条件和 DuckDB 回滚。
- **范围**：版本化 dry-run/runbook、验收闸门、逐步 read backend 开关、观察窗口、失败停止和 rollback；
  明确切换期间新写数据的处置。
- **前置依赖**：`SV2-022A`。
- **验收标准**：切换后查询契约和缓存语义一致；故障时在界限内回退；回滚后 DuckDB 可用且无数据
  语义分叉；所有步骤可重复且有审计证据。
- **必要测试**：切换前后 golden contract、ClickHouse outage、lag/mismatch stop condition、回滚、
  重复演练和备份恢复组合。
- **排除项**：production 开关、launchd、消费者改造、部署或远程写入。
- **状态**：`已验收`（Artifacts `094e26d`、`8a376d9`）。新增 development/test-only
  `LocalReadSwitchDrill`，以
  `storage-v2.read-switch.v1` 版本化 checkpoint、配置与报告绑定已完成的 SV2-022A run ID/completion
  fingerprint、目标逻辑标识、源逻辑路径 hash、backfill/restore 报告 hash 和 backup/restore Artifact ID。
  preflight Artifact 必须位于显式 `allowed_root` 内且不得为 symlink；profile、根目录或 Artifact 标识含
  production、backfill 未完成/不稳定、restore 未验证、持久配置或 checkpoint binding 不一致均在切换前
  fail-closed，并保守持久化 DuckDB read backend。

  切换在单一 `flock` 下按 canonical、raw 两阶段将实际 `ShadowMarketBarRepository` 开关原子持久化，重启
  会恢复唯一有效 backend；每阶段在 apply 前后及 checkpoint 后提供故障注入边界，崩溃窗口重启时先回滚
  DuckDB 再恢复演练。闸门逐次要求 lag=0、reconcile/contract=ok、raw spool 与 canonical queue 清零且
  readiness 非 unavailable；每阶段执行有界 observation，并在 canonical 阶段显式注入 DuckDB-primary
  增量写及对应 rebuild。任一 mismatch、lag/backlog、unavailable 或异常均同步 fail-stop 并持久回滚；显式
  rollback 重复执行幂等。

  golden 演练在真实一次性 ClickHouse 25.8 上逐项比较 recent、闭区间、canonical page、exact
  cross-section、cross-section page、matrix、single/cross-section as-of、raw range/page；同一 FastAPI
  target-bound 请求同时核对 bars、排序、分页 cursor、cache freshness/served_at 与 raw provenance。
  ClickHouse client 故障在同一请求内由现有 adapter 回退 DuckDB，随后显式 rollback 验证两个读取开关均
  恢复 DuckDB，cursor/cache 契约保持等价。审计事件最多 100 条、golden 样本最多 50 条，reason 统一
  脱敏截断，报告不包含 DSN、凭证或绝对敏感路径。

  相同 binding 已处于 switched 时，重复 `run()` 只复核真实 gate 与 canonical/raw golden，不重复阶段、
  不再次调用 incremental callback，也不追加审计事件；回滚或真实中断后的新 attempt 则从 durable phase
  明确恢复。真实 outage 期间继续完成 DuckDB-primary 写入，ClickHouse shadow 形成 raw spool；新行在同一
  请求 fallback 中可见，readiness stop condition 持久回滚 DuckDB。ClickHouse 恢复后有界 replay、精确范围
  canonical rebuild 和 target-bound golden 全部收敛，再切换与回滚均无重复或语义分叉。

  另有真实 disposable 组合演练直接调用 `LocalStorageBackup` 生成 bundle、`LocalStorageRestore` 恢复到
  空 PG/CH/本地根，再调用 `LocalStorageBackfill` 产生 signed checkpoint/report；read switch 的 backup ID、
  restore report hash、backfill run/fingerprint/target 均取自这些实际输出。gate 从 backfill checkpoint/report、
  16 域 PG+raw target comparison、spool/canonical pending 与 ClickHouse health 派生；篡改 restore evidence
  会在构造恢复 backend 时 fail-closed 到 DuckDB。

  本项 8 项默认单元测试与 2 项显式真实集成演练通过；完整默认套件、真实 ClickHouse/PostgreSQL 回归、
  Storage V2 contract gate、Ruff 与 diff-check 的最新复跑结果随本项 Artifact 记录。测试容器均为本地
  disposable 实例。

  范围未进入 `SV2-023`：没有修改 production 默认、launchd 或消费者，没有部署、上传、push 或任何
  远程写入。

### `SV2-023`：生产规模性能与容量基准

- **目标**：以 ADR 容量假设为基础，用可复现数据量测写入、合并、查询、分页、rebuild、归档和恢复，
  给出容量与超时配置证据。
- **范围**：至少一个月代表性分钟数据或等价合成分布；单/多来源、并发、冷热查询、磁盘占用、
  压缩率、merge backlog、spool recovery；记录硬件和版本。
- **前置依赖**：`SV2-022B`。
- **验收标准**：预先定义并满足本地 SLO；查询不随页码使用 OFFSET；内存/线程/磁盘有界；保留
  至少 30% ClickHouse 空闲空间的容量模型；不满足时先形成有证据的优化项并复验。
- **必要测试**：可复现 benchmark 命令、warm/cold 多轮、p50/p95/p99、故障/恢复吞吐、查询计划、
  结果正确性抽样和报告 diff。
- **排除项**：采购、云资源创建、production 压测、对外上传数据。
- **状态**：`已验收`（Artifact `9a685e8`）。新增 development/test-only
  `LocalStorageBenchmark`（`storage-v2.benchmark.v1`）和固定 `BenchmarkPlan`。基准根必须位于显式
  `allowed_root`、名称以 development/test 结尾且不得为 symlink；sample 最大 500 万 raw rows、重复轮数
  3～20、线程和 peak memory 均有显式上限。输入必须完整提供 raw write、canonical rebuild、warm/cold
  query、first/deep keyset page、archive、restore、spool recovery、concurrent query 和 merge/disk probe，缺项、
  多项或目标端独立回读的行数/checksum 不一致均 fail-closed。

  每个 operation 统一记录三轮以上 p50/p95/p99、最差 rows/s、每轮目标端 checksum、物理/逻辑 bytes 和
  实际 EXPLAIN。warm 明确定义为复用已经建立的 ClickHouse session；cold 明确定义为每轮创建并关闭新的
  ClickHouse client（本地无法可靠、可移植地清理 OS page cache，因此不把同一调用冒充 cold cache）。
  cold archive 路径另由独立月份 Parquet create/verify/query/backfill 覆盖。page SQL 单独保留并拒绝
  `OFFSET`，deep page 必须包含 `bar_time > cursor` keyset 条件，且 cursor depth 至少达到单来源月样本的
  80%。实际 Repository `after`、EXPLAIN 使用的 UTC predicate 和 ClickHouse `count(bar_time <= after)`
  得到的 depth 必须绑定同一个游标，不接受外部自报深度或不同 predicate 的计划。报告同时记录有界硬件/
  组件版本、操作执行期间 1ms sampler 观测到的 RSS absolute peak、相对基线
  delta 与线程 peak、ClickHouse free ratio 和 merge backlog；内容统一敏感扫描后以临时文件 fsync、
  rename、目录 fsync 原子发布。

  **预先固定的本地 benchmark SLO**：raw write ≥1,000 rows/s；canonical rebuild ≥500 rows/s；warm/cold、
  首/深页和并发查询整体 p95 ≤5s、p99 ≤8s；deep/first p95 比值 ≤5；archive、restore、spool recovery
  各 ≥500 rows/s；Parquet physical/logical ratio ≤0.80；ClickHouse 当前 free ratio ≥30%、merge backlog
  ≤100；peak memory/thread 不超过 plan 上限；first/deep SQL 均不得含 OFFSET。任一失败仍原子保存
  `status=failed` 报告并阻止通过。

  容量模型由实测 raw bytes/row 线性外推 ADR 的 5,500 symbols × 240 bars/day × 250 trading days/year ×
  10 years × sources，并另计 canonical（单来源 raw 的 90% 规划系数）；所需磁盘明确按
  `modeled_online_bytes / 0.70` 计算，以保留 30% merge 空闲。报告明确该结果是本地规划估算，不是
  production 吞吐承诺。

  默认测试使用受控时钟覆盖 percentile、SLO、容量、隔离、目标端 mismatch、OFFSET、瞬时线程超限和不足
  30%/merge backlog 负向闸门；瞬时线程测试证明仅在 operation 中途越界、结束后恢复也会阻断通过。显式
  集成每轮使用一个完整合成月（20 trading days × 240 minute bars × 2 sources，共 9,600 raw rows），三轮
  采用不同月份/业务键/逻辑批次并各自创建独立内容寻址 Parquet Artifact，避免测到 writer 去重或 archive
  reuse 快路径。它连接一次性 ClickHouse 25.8，并使用真实 Warehouse、ReliableClickHouseWriter、canonical
  builder、约第 4,600/4,800 行位置的 keyset query/EXPLAIN、ParquetColdArchive、专用业务键的 spool
  outage/replay 和四线程独立 ClickHouse sessions。raw、canonical、query、archive、restore 与 spool 均从
  ClickHouse/Parquet/DuckDB 目标输出独立回读并核对实际新增行数和 checksum；三轮 warm/cold、真实写入、
  rebuild、独立归档/恢复与故障恢复报告全部满足上述 SLO。

  raw 容量样本不使用全表累计 bytes 充当单轮体积：每轮按其独立 UTC 月分区读取 active part
  `bytes_on_disk`，容量模型使用所有轮次分区 bytes 之和除以所有轮次实际新增 raw rows 之和。不同轮次
  行数不等的单元回归证明 bytes/row 不随 `runs` 线性放大；报告同时保留 measured rows/bytes 便于复核。

  可复跑命令：默认门禁为
  `MARKETCOW_HOME=$(mktemp -d) uv run python -m unittest tests.test_storage_v2_benchmark -v`；真实门禁在本地
  disposable ClickHouse 设置 `MARKETCOW_TEST_CLICKHOUSE_HOST/PORT/USERNAME/PASSWORD` 后运行同一命令。
  范围未进入 `SV2-024`，未连接 production、未部署、上传、push 或执行远程写入。

### `SV2-024`：production 切换准备包

- **目标**：形成可供用户审批的生产就绪包，但不执行任何 production 状态变更。
- **范围**：配置矩阵、secret/权限、网络/端口、服务管理、schema/migration 预检、备份点、容量、
  SLO、观察窗口、逐步读写开关、停止条件、rollback 命令和责任边界。
- **前置依赖**：`SV2-023`。
- **验收标准**：所有命令可 dry-run；production 目标显式且默认拒绝；每个变更有前置检查、成功证据、
  失败停止条件和回滚；不得含明文凭证；列出需用户确认的每个外部动作。
- **必要测试**：配置静态审计、dry-run、在 disposable 环境按 runbook 演练、secret/path 泄漏扫描、
  文档逐项桌面演练。
- **排除项**：连接/迁移/切换 production，launchd 变更，部署，push/PR/Release。
- **状态**：`本地实现完成，返修后待独立验收`。新增 `storage-v2.production-readiness.v1` 本地准备包。
  构造器只允许 development/test 隔离根，必须读取并验证 `SV2-021A`、`SV2-021B`、`SV2-022A`、
  `SV2-022B`、`SV2-023` 的显式本地 acceptance record 与 evidence 文件；两层文件均受 allowed root
  containment、symlink、版本、状态和 checksum 约束。accepted commit 必须存在且为候选 release commit
  的祖先，release commit 必须等于本地 HEAD。package 只记录相对逻辑 URI、两层 evidence hash 和完整 commit，
  不接受调用方手填 Artifact 字符串、容量或任意全 True SLO。

  容量与 SLO 仅从通过完整校验的 `storage-v2.benchmark.v1` 报告派生：benchmark 必须包含固定 14 项
  checks 且全部通过，并保留 measured raw rows/bytes、bytes/row、modeled online bytes、含 30% reserve 的
  required disk 和实测 ClickHouse free ratio。报告缺字段、伪造简化 checks、hash 篡改、证据换包、未知版本、
  非 accepted 状态或 Git ancestry 不成立均在 package 生成和每次 dry-run 前 fail-closed。

  package 固定包含 current/proposed 配置矩阵、launchd/8790 逻辑边界、30 分钟且至少 1,000 请求的观察窗口，
  以及 configuration、backup、schema、backfill、read switch、observation 六个顺序阶段。每阶段明确 precondition、
  success evidence、stop condition、rollback、`authorization_required=true` 和 `apply_command_included=false`；
  package 不包含 production apply command。七类外部动作逐项披露 destination、data、是否包含源码、visibility、
  retention 和 intended action，初始全部 `authorized=false`、`executed=false`，必须由用户分别授权。

  所有 runbook 命令均调用实际存在的
  `uv run python -m marketcow.production_readiness stage ... --dry-run --package ... --allowed-root ...
  --repository-root ...`；CLI 缺少 `--dry-run` 或 package/evidence 绑定参数时直接拒绝。六个 stage 会重新校验
  package、Git ancestry 和对应的本地证据：configuration 校验完整链，backup 校验 021A/021B，schema 校验
  完整链与源码 migration version，backfill/read switch/observation 分别校验 022A/022B/023；输出机器可读的
  checked evidence hash/commit、`production_connection_attempted=false`、`state_changed=false`，错误为非零退出。
  构建使用 package/runbook/manifest
  checksum、临时文件 fsync/rename/目录 fsync 和内容绑定实现确定性原子发布；verify 重新扫描 document、runbook、
  manifest 的凭证文本并拒绝重签篡改或不安全路径。disposable rehearsal 不再接受调用方恒真 probe，而是依次
  调用相同的六个真实 stage checker，任一 evidence mismatch 立即停止。

  `RUNBOOK.md` 逐阶段渲染 preconditions、success evidence、stop conditions 与 rollback，并逐项渲染七类
  external action 的 destination、data、源码披露、visibility/access、retention、proposed action 及
  unauthorized/unexecuted 状态；仍不包含任何 apply command。

  本项测试覆盖确定性重复 build、绑定 package 的真实 dry-run CLI 六阶段、完整 runbook/授权披露、production
  runtime/不完整或伪造 evidence、Git ancestry、完整 benchmark SLO/容量、路径/symlink 拒绝、package/evidence
  重签篡改和凭证扫描。范围未连接或修改
  production，未更改 launchd、消费者或默认 backend，未部署、push、上传或执行其他远程写入，也未开始
  `SV2-025`。

### `SV2-025`：本地最终验收与可交付冻结

- **目标**：证明所有默认授权范围内的 Storage V2 本地开发项完整、可复现且无未登记缺口。
- **范围**：逐项核对 `SV2-012`～`SV2-024` Artifact；全套契约/集成/性能/恢复/安全测试；文档、
  migration、runbook、已知限制和提交范围审计；形成最终本地 Artifact。
- **前置依赖**：`SV2-024`。
- **验收标准**：清单所有前置项已验收；工作树干净；默认、PG、CH、端到端、Ruff、diff-check、
  安全与文档一致性全部通过；限制和 production 待授权动作明确；验收方独立复跑通过。
- **必要测试**：前述套件总集、从空环境初始化、备份恢复、迁移回滚、代表性 benchmark、API contract、
  secret/路径/远程写入审计。
- **排除项**：任何远程写入、production 连接/变更或正式消费者切换。
- **状态**：`未开始`。

### `SV2-EXT`：外部发布与 production 执行

- **目标**：在本地最终验收后，按用户逐项明确授权执行可选的外部发布或正式切换。
- **范围**：可能包括 `git push`、PR、Release、部署、production PostgreSQL/ClickHouse 连接、备份、
  migration、数据回填、读写切换、launchd 更新和消费者验证；每一类动作均须单独披露目标、数据、
  源码是否包含、可见性、保留后果和回滚方式。
- **前置依赖**：`SV2-025`，以及用户在执行前一轮之后对确切目标和动作的明确确认。
- **验收标准**：仅以用户批准的动作清单为准；执行前备份/preflight、执行中观察、执行后验收与回滚
  证据完整；未获批准的动作保持未执行。
- **必要测试**：由获批动作的 runbook 决定；至少包含 health、单实例、数据对账、消费者兼容和回滚验证。
- **排除项**：任何未被用户明确批准的远程写入或 production 状态变更。
- **状态**：`需用户授权`，不属于默认自动开发流程，不得因前置项完成而自动开始。

### 历史六阶段与清单映射

此前“接口隔离、开发基础设施、PostgreSQL、ClickHouse 影子写入、查询切换、冷热分层”六阶段
仍是架构背景，不再作为并行待办：前三项和影子写入已由 `DONE-00`～`DONE-05` 覆盖；查询
收口对应 `SV2-012`～`SV2-019B`；冷热分层及交付准备对应 `SV2-020A`～`SV2-025`。正式执行
只属于 `SV2-EXT`。

历史步骤的详细实现证据已归并到“已完成检查点摘要”和第一节进展；不得据此绕过固定清单顺序。

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
- 现有默认测试继续通过（最新数量见第十一节）；
- 新 backend 有单元测试和显式集成测试；
- 上游失败仍有界，不占满服务线程；
- 双写失败不能阻断现有 DuckDB 主路径，除非测试明确验证 fail-closed；
- 数据记录保留 `source`、`observed_at`、`ingested_at` 和 `raw_artifact_id`；
- 接口兼容性变化必须先形成文档并获得用户确认。

## 十一、当前运行检查结果

最近一次 Storage V2 检查：

```text
feature/storage-v2 检查点 11 已验证基线：ef86ad1；检查点 12 修订候选只扩展 canonical
单 symbol 闭区间 keyset 游标分页及其密钥/多来源 fallback 等价修正
默认测试：123 项通过；17 项显式外部存储集成测试因未配置本地服务而跳过
PostgreSQL 集成测试：7 项通过（显式启用，使用独立 UTF-8 临时数据库）
ClickHouse 集成测试：12 项通过（显式启用，使用一次性 ClickHouse 25.8 本地容器；
容器测试后停止）
检查点 9 已完成单 symbol 多来源 raw 闭区间查询、只读 API、provenance 与 DuckDB/
ClickHouse 等版本确定性收敛。检查点 10 已完成 development-only、default-off 的同步
automatic canonical：raw 逻辑批次完整成功或全部 spool 分块重放成功后按完整精确范围
有界 rebuild，并具备共享 replay 预算、持久化 intent、崩溃恢复和并发互斥；DuckDB
仍为 primary，所有失败保持 fail-open。检查点 11 已验收完成四类只读行情响应的统一
fresh/stale/empty、UTC newest ingestion、age/served_at、refresh 缓存降级和 ClickHouse→
DuckDB fallback 等价契约。检查点 12 初始候选 `b975460` 已完成 canonical 单 symbol
闭区间的 HMAC keyset 游标、无 OFFSET 的 DuckDB/ClickHouse 查询及同游标 fallback，但
独立验收发现公开固定默认密钥可伪造、DuckDB 多来源 fallback 与 canonical priority 不等价。
两项缺陷已在本地修订候选 `51b0c2b` 中修复：增加持久随机密钥、placeholder/长度拒绝和
密钥轮换语义，并让 DuckDB 多来源选择严格复用 canonical priority；该修订仍待独立验收，
不能视为已验证基线。10,001 行本地样本逐页验证有界、无缺失重复。尚未完成
raw/横截面分页、生产容量基准、非精确或多时间点
横截面、后台扫描/调度、冷热分层及正式连接/切换，必须等待验收方指定。
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
步骤。先以第八节固定清单核对唯一下一项，只执行验收方指定的一个清单项，完成后暂停；
保持现有 API 契约和全部测试通过，不要连接或迁移正式 PostgreSQL/ClickHouse，也不要
远程推送。除非验收确认必须插入阻断修复，不得跳项、并行推进或自行改写清单顺序。
```
