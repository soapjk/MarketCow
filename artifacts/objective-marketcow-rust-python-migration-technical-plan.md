# MarketCow Rust + Python 混合架构迁移技术方案

> 文档状态：Proposal / Objective Artifact  
> 版本：v1.0  
> 编制日期：2026-08-27  
> 所属 Objective：MarketCow 金融数据中台  
> 关联背景：MarketCow b16f HTTP 503 长时间运行复发、实时热路径尾延迟与恢复边界问题  
> 目标读者：MarketCow owner、Tradude owner、平台开发、数据工程、运维与验收人员

## 1. 文档目的

本文给出 MarketCow 从当前 Python 单体为主的实现，迁移为“Rust 平台核心 + Python 数据采集插件”的完整技术方案。

本方案重点解决：

1. 实时数据链路在持续负载下的尾延迟、锁竞争、队列膨胀和恢复边界问题。
2. 当前 API、实时采集、数据库访问、任务调度和 Provider 逻辑耦合过深的问题。
3. PostgreSQL、ClickHouse、SQLite、JSONL/WAL 和本地 Artifact 在不同模块中存在多套访问方式的问题。
4. Python 生态不可替代的金融数据 SDK、爬虫、PDF/HTML 解析能力需要继续保留的问题。
5. 服务启动、鉴权、授权、接口契约、审计、可观测性和故障处理需要统一 owner 的问题。

本文是迁移设计，不等同于当前 b16f 故障工作项的验收证据。当前工作项仍须按原验收标准完成持续运行验证，不能用本迁移计划推断其已经通过。

## 2. 结论摘要

MarketCow 的最终目标不是两个并列后端，而是一个由 Rust 主导的统一服务：

- Rust `marketcowd` 是唯一公开服务入口、唯一主进程和统一控制面。
- Rust 负责所有实时数据面、领域契约、公开 API、数据库访问、WAL、任务状态、审计和可观测性。
- Python 只保留依赖 Python 生态的非实时 Provider 抓取、解析和数据转换。
- Python worker 不监听公开端口、不直接运行数据库迁移、不拥有业务任务状态，最终不直接写 PostgreSQL、ClickHouse 或权威 WAL。
- Rust 和 Python 通过本机 Unix Domain Socket 上的版本化内部协议通信。
- PostgreSQL 和 ClickHouse 保持现有职责，不因语言迁移而进行无必要的数据搬迁。
- Polymarket 作为第一个完整迁移的实时域，用 r2 日志和 b16f exact scope 建立可回放、可对比、可回滚的迁移样板。

## 3. 设计原则

### 3.1 单一所有权

每类状态必须有且只有一个 owner：

| 状态 | 最终 owner |
|---|---|
| HTTP/WebSocket/MCP 公开接口 | Rust API Gateway |
| 身份认证、授权、限流、审计 | Rust Security Layer |
| 实时 cursor、sequence、gap、projection | Rust Realtime Core |
| 权威事件 WAL | Rust WAL Service |
| PostgreSQL schema 和事务写入 | Rust Storage Layer |
| ClickHouse schema 和写入 | Rust Storage Layer |
| 历史任务、CSV import、Provider job 状态 | Rust Job Engine |
| Provider 抓取和特定格式解析 | Python Worker |
| Web UI | React/TypeScript，保持不变 |

迁移期间允许 shadow read 和结果比对，不允许同一 domain 长期双写。

### 3.2 fail-closed 不放宽

语言迁移不得改变以下行为：

- cursor gap 必须拒绝增量继续推进，直到建立可信恢复边界。
- 数据源缺失、过期、规则不一致或关联市场不完整时必须返回机器可判定错误。
- 不得将 stale、derived、mark/oracle 数据伪装为可执行 bid/ask。
- 不得为了降低 503 数量而扩大 freshness、gap 或完整性阈值。
- 真实订单保持禁用，除非未来通过独立安全与合规工作项启用。
- Tradude 只消费托管服务，不得启动、重启、回滚或管理 MarketCow。

### 3.3 热路径与派生路径隔离

实时热路径仅包含：

```text
upstream frame
→ authenticate/validate
→ normalize
→ sequence/cursor check
→ apply to single-writer state
→ append authoritative WAL
→ publish immutable projection generation
→ notify downstream clients
```

以下工作不得阻塞实时热路径：

- PostgreSQL 查询。
- ClickHouse merge 或查询。
- SQLite derived index。
- 历史回填。
- Artifact 压缩和归档。
- 大范围 JSON 深拷贝。
- Python worker 任务。
- Grafana、统计聚合或离线质量报告。

### 3.4 金融计算确定性

- 金额、价格、tick、费率、份额、成交量使用 `rust_decimal::Decimal`、定点整数或规范化 decimal string。
- 禁止在关键金额和价格路径使用 `f32`/`f64`。
- 每个金额字段必须同时明确 currency、unit、scale 和 rounding mode。
- 时间统一使用 UTC、RFC 3339 和整数 epoch 纳秒/毫秒之一，禁止隐式本地时区。
- 交易日、session 和 calendar 必须显式携带 source 与 revision。
- 所有 canonical 结果必须可由 raw payload、normalizer version 和配置 revision 重放。

### 3.5 兼容优先、渐进替换

- 外部 URL、字段、错误码、状态码和时序语义先冻结，再迁移实现。
- Rust 可以在早期代理尚未迁移的 Python API。
- 数据库先复用现有 schema，再逐步优化。
- 不进行一次性大爆炸切换。
- 每个 domain 都必须有 shadow、diff、切换、观察和回滚阶段。

## 4. 当前架构问题摘要

当前 MarketCow 主要存在以下结构性问题：

1. `api.py` 同时承担 API、鉴权、领域编排、Provider 调用、数据库读取和部分实时逻辑。
2. `polymarket_live.py` 同时承担采集、规范化、状态、WAL、SQLite、恢复和健康计算，文件规模过大。
3. 进程由 Python supervisor 同时启动 collector、shared API 和 read API，公开能力分布在 8790/8791。
4. 实时投影、权威 JSONL 和派生 SQLite 之间的故障边界经历多次补丁后仍较复杂。
5. 当前事件 JSONL 已达到数十 GB，启动恢复时间随历史日志增长。
6. Python GIL 不是唯一问题；更重要的是锁内复制、队列耦合、非原子发布和恢复状态边界。
7. PostgreSQL 和 ClickHouse repository 接口较大，Provider 与数据库写入路径耦合。
8. Python Provider 生态强，但其运行故障可能影响 API 主进程或共享资源。

Rust 迁移需要解决这些边界，而不仅是逐行翻译 Python。

## 5. 目标运行拓扑

### 5.1 生产进程

```text
launchd
└── marketcow serve --profile production
    ├── Axum HTTP/MCP :8790
    ├── public WebSocket :8790
    ├── realtime provider tasks
    ├── projection/WAL tasks
    ├── storage pools
    ├── durable job engine
    ├── Python worker supervisor
    │   ├── worker-provider-structured
    │   ├── worker-provider-filings
    │   ├── worker-provider-history
    │   └── worker-transform
    └── observability exporter
```

PostgreSQL 和 ClickHouse 是独立的数据库服务，不作为 `marketcowd` 子进程。生产启动器负责先检查依赖，再执行 `marketcow serve`。

### 5.2 端口规划

| 端口/通道 | 用途 | 最终状态 |
|---|---|---|
| 8790 | 唯一公开 HTTP/WebSocket/MCP | 保留，Rust owner |
| 8791 | 当前 Polymarket read API / 迁移 shadow | 观察期后删除 |
| 8794 | 当前内部 live stream | Rust 内部 channel 后删除 |
| UDS | Rust 与 Python worker 通信 | 新增，仅本机 |
| PostgreSQL | 事务与控制面 | 保留 |
| ClickHouse | 市场数据与分析读取 | 保留 |

### 5.3 故障隔离

- 一个 Python worker 崩溃不能使 8790 不可用。
- 一个 Provider 限流不能阻塞其他 Provider。
- PostgreSQL 慢查询不能阻塞实时 projection。
- ClickHouse 故障不能阻止非持久化类型的实时 quote/book 更新；需要权威落盘的 bar 必须 fail-closed。
- derived index 损坏不能停止 WAL append。
- 客户端慢消费只关闭该客户端，不反压整个实时流。

## 6. Rust workspace 设计

建议使用一个 Cargo workspace：

```text
Cargo.toml
crates/
├── marketcowd/
│   ├── main.rs
│   ├── cli.rs
│   ├── bootstrap.rs
│   └── supervisor.rs
├── marketcow-config/
├── marketcow-domain/
├── marketcow-contracts/
├── marketcow-api/
├── marketcow-auth/
├── marketcow-observability/
├── marketcow-realtime/
├── marketcow-wal/
├── marketcow-checkpoint/
├── marketcow-postgres/
├── marketcow-clickhouse/
├── marketcow-artifacts/
├── marketcow-jobs/
├── marketcow-worker-protocol/
├── provider-polymarket/
├── provider-hyperliquid/
├── provider-longport/
└── marketcow-testkit/
proto/
schemas/
python/
└── marketcow_workers/
web/
```

### 6.1 `marketcow-domain`

负责纯领域类型和规则：

- Instrument、symbol mapping、MIC、currency。
- Quote、Trade、Bar、OrderBook。
- MarketState、Session、TradingCalendarRevision。
- PredictionMarket、Outcome、Relation、InstrumentRule、FeeSchedule。
- Decimal、Money、Price、Quantity、Rate。
- Cursor、Sequence、Generation、ScopeId。
- SourceEvidence、Provenance、Revision。

该 crate 不依赖 HTTP、数据库、Tokio runtime 或 Provider SDK。

### 6.2 `marketcow-contracts`

负责：

- API DTO。
- WebSocket frame。
- MCP request/response envelope。
- Python worker protobuf。
- 机器错误码。
- JSON Schema/OpenAPI 生成。
- contract version 与 contract hash。

### 6.3 `marketcow-realtime`

负责通用实时能力：

- 单写者状态机。
- sequence/cursor 验证。
- replay ring buffer。
- immutable projection publication。
- subscription refcount。
- per-client bounded queue。
- slow-consumer 处理。
- heartbeat。
- reconnect 和 recovery barrier。
- 一分钟 bar aggregation。
- freshness 和 delivery headroom。

Provider crate 只负责把上游协议转换成通用事件，不自行实现一套客户端发布系统。

### 6.4 `marketcow-wal`

负责：

- append-only segment。
- fsync policy。
- cursor/hash chain。
- integrity scan。
- sparse cursor index。
- compaction/archival manifest，但不重写权威记录。
- legacy JSONL reader。
- replay iterator。

### 6.5 Storage crates

`marketcow-postgres` 和 `marketcow-clickhouse` 负责唯一数据库访问实现；业务 crate 只依赖 repository trait。

## 7. Python worker 架构

### 7.1 保留在 Python 的能力

建议保留：

- AkShare/EastMoney 财务数据。
- BaoStock。
- mootdx/TDX 财务文件。
- Tushare SDK。
- SEC、HKEX、交易所公告。
- PDF、HTML table 和网页解析。
- pandas/pyarrow 清洗。
- 复杂 CSV 格式推断。
- 低频经济日历、财报日历和分红发现。

### 7.2 Python worker 禁止事项

- 禁止监听公开 HTTP 端口。
- 禁止持有 PostgreSQL/ClickHouse 生产凭据。
- 禁止执行 schema migration。
- 禁止直接推进 job status。
- 禁止写权威 WAL。
- 禁止决定 API 鉴权或授权。
- 禁止未经过 Rust 校验直接发布 canonical 数据。

### 7.3 Worker 类型

```text
provider-structured
  ├── tushare
  ├── longport-non-realtime
  └── baostock

provider-filings
  ├── sec
  ├── hkex
  ├── cn-exchange
  └── pdf/html parser

provider-history
  ├── akshare
  ├── mootdx
  └── calendar sources

transform
  ├── dataframe normalization
  ├── csv inference
  └── parquet/arrow conversion
```

每个 worker pool 独立限流、并发、内存限制和重启预算。

## 8. Rust/Python 内部协议

### 8.1 传输

推荐：Unix Domain Socket + gRPC/protobuf。

理由：

- 强类型和版本化。
- 支持 deadline、cancellation 和 streaming。
- Rust `tonic` 与 Python `grpcio` 成熟。
- 无需引入外部 broker。
- 可以用文件权限限制访问。

### 8.2 请求模型

示意：

```proto
message ProviderTask {
  string protocol_version = 1;
  string task_id = 2;
  string idempotency_key = 3;
  string trace_id = 4;
  string provider = 5;
  string operation = 6;
  bytes request_json = 7;
  int64 deadline_unix_ms = 8;
  string expected_schema = 9;
  string config_revision = 10;
}
```

### 8.3 响应模型

```proto
message ProviderResult {
  string protocol_version = 1;
  string task_id = 2;
  string provider = 3;
  string dataset = 4;
  string schema_version = 5;
  bytes normalized_json = 6;
  ArtifactDescriptor raw_artifact = 7;
  SourceEvidence source = 8;
  repeated DataIssue issues = 9;
  RetryDisposition retry = 10;
}
```

`SourceEvidence` 至少包含：

- source name。
- source URL。
- requested_at、responded_at、observed_at。
- HTTP status 或 SDK result code。
- raw SHA-256。
- update frequency。
- revision/version。
- missing、delayed、duplicate、revised 标记。

### 8.4 大文件处理

对 PDF、CSV、Parquet 和大 JSON：

1. Rust 创建带 task ID 的 staging lease。
2. Rust 传给 Python 一个已验证的绝对路径。
3. Python 只能在该目录写文件。
4. Python 返回文件名、长度、SHA-256 和 media type。
5. Rust 验证路径没有逃逸 allowed root。
6. Rust 重算 hash。
7. Rust 原子移动至 Artifact store。
8. Rust 写 manifest 和审计。
9. Rust 清理 staging lease。

## 9. 统一公开 API 设计

### 9.1 Rust Gateway

Rust Axum gateway 最终负责：

- TLS/loopback binding policy。
- request ID 和 trace ID。
- authentication。
- RBAC 和 capability scope。
- rate limit。
- body size limit。
- input validation。
- idempotency。
- audit。
- error mapping。
- compression。
- Server-Timing。
- OpenAPI/schema。

### 9.2 API 迁移期间代理

阶段性路由：

```text
Rust :8790
├── /v1/health                         → Rust
├── /v1/readiness                      → Rust
├── /v1/prediction-markets/...         → Rust
├── /v1/market-data/stream             → Rust
├── /v1/instruments/...                → Rust after domain migration
├── /v1/quotes/...                     → Rust after storage migration
├── /v1/fundamentals/...               → internal legacy proxy
└── /mcp                               → Rust transport, staged dispatch
```

代理阶段要求：

- Rust 完成外层鉴权后才转发。
- Python legacy 仅监听内部 UDS 或随机 loopback 端口。
- Python 不信任客户端传入的身份头。
- Rust 使用签名内部 identity envelope。
- proxy response 参与 golden diff。

### 9.3 错误契约

统一格式：

```json
{
  "detail": {
    "code": "polymarket_state_index_lagging",
    "message": "...",
    "retryable": true,
    "request_id": "..."
  }
}
```

错误必须区分：

- validation。
- authentication。
- authorization。
- idempotency conflict。
- upstream unavailable。
- upstream invalid data。
- freshness exhausted。
- cursor gap。
- persistence unavailable。
- derived index degraded。
- internal invariant violation。

## 10. 实时核心详细设计

### 10.1 Task 拓扑

每个实时 Provider 建议使用以下任务：

```text
connection manager
  └── frame reader
        └── decoder/validator workers
              └── ordered apply task (single writer)
                    ├── WAL append task
                    ├── projection publisher
                    ├── downstream broadcaster
                    └── derived consumers
```

只有 ordered apply task 可以改变 cursor 和 canonical state。

### 10.2 上游批次

- 保留上游 message/batch 边界。
- 同一个 message 中同一 token 的多 level change 先完整应用，再验证 crossed/locked。
- 一个 market 的多 outcome 规则变化需在 market 原子边界发布。
- invalid delta 可以记入权威事件，但不得让随后同批次已经验证的 full-book 恢复延迟到另一个周期任务。
- recovery completion 必须和恢复 book 在同一个 projection generation 可见。

### 10.3 Immutable Projection

推荐模型：

```rust
struct ProjectionGeneration {
    generation: u64,
    cursor: u64,
    persisted_cursor: u64,
    catalog_revision: Revision,
    markets: Arc<MarketMap>,
    books: Arc<BookMap>,
    unresolved_gaps: Arc<GapSet>,
    published_at: DateTime<Utc>,
}
```

- writer 构造新 generation。
- 通过 `ArcSwap` 一次发布。
- reader 获取 `Arc` 后立即离开共享边界。
- JSON 序列化、过滤和 frame build 在共享锁外完成。
- 大 scope response 可以缓存同 generation 的序列化片段，但缓存必须有明确上限。

### 10.4 Read Path

```text
request
→ validate scope/scope_id
→ load Arc projection
→ validate generation invariants
→ filter references
→ freshness check
→ build response
→ serialize/write
```

禁止：

- 为每次请求深拷贝完整 catalog。
- 在 projection writer lock 内序列化。
- hot read 查询 SQLite/PostgreSQL/ClickHouse。
- 为等待 derived index 而阻塞 HTTP worker。

### 10.5 Persistence Ordering

推荐的事件确认语义：

1. decode/validate。
2. reserve next cursor。
3. apply candidate state。
4. encode canonical event。
5. append WAL 并达到配置的 durability。
6. commit state generation。
7. publish downstream。

如果要求极低延迟，可支持两类字段：

- `published_cursor`：已进入 projection。
- `persisted_cursor`：已达到 WAL durability。

但任何 API 必须明确返回两者，不能把未来 persisted watermark 暴露给尚未应用到本地 projection 的 generation。

### 10.6 Backpressure

所有 channel 必须：

- 有固定 capacity。
- 暴露 depth、oldest age、enqueue/dequeue rate。
- 定义 full policy。
- 不允许无界内存增长。

建议策略：

| Channel | Full 行为 |
|---|---|
| upstream decode | 暂停 socket read，触发连接健康监测 |
| ordered apply | 暂停 decode，不丢事件 |
| WAL append | fail-closed，不发布未持久化要求的事件 |
| derived index | 隔离 derived consumer，权威链路继续 |
| client queue | 关闭该慢客户端，code 1013 |
| Python task dispatch | job 保留在 PostgreSQL，稍后重试 |

## 11. Polymarket Rust 域

### 11.1 模块

```text
provider-polymarket/
├── gamma.rs
├── clob_rest.rs
├── market_ws.rs
├── normalize.rs
├── catalog.rs
├── book.rs
├── relations.rs
├── scope.rs
├── recovery.rs
├── projection.rs
├── contracts.rs
└── observability.rs
```

### 11.2 必须保持的语义

- official free source policy。
- exact scope identity。
- 100 markets/200 outcome books。
- append-only cursor。
- raw/canonical hash。
- duplicate detection。
- out-of-order audit。
- source mismatch gap。
- reconnect recovery。
- dynamic tick/version/provenance。
- negative-risk relation coherence。
- book freshness 与 delivery headroom。
- memory projection hot read。
- real order submission disabled。

### 11.3 r2 Replay Corpus

迁移必须将以下内容纳入固定测试：

- r2 exact scope manifest。
- r2 运行时间窗口的 MarketCow 日志。
- cursor `24906390→25040506` 事件范围。
- 后续 crossed/locked delta 后紧跟 full-book 的事件序列。
- disconnect/reconnect。
- derived SQLite backlog。
- 读请求高并发和大 response serialization。

对同一输入，Python 与 Rust 至少比较：

- cursor。
- event type/applied/fail_closed_reason。
- canonical payload hash。
- book checksum。
- tick version。
- unresolved gap set。
- market instrument revision。
- health status。
- HTTP status/error code。

## 12. 数据库架构

### 12.1 PostgreSQL 职责

继续保存：

- Instrument Master 和 symbol mapping。
- runtime config revision。
- migration checkpoint。
- provider health。
- raw Artifact manifest。
- fundamentals、statements、dividends。
- calendar 和 indicators。
- history jobs/items/shards。
- CSV import jobs/shards。
- admin audit。
- 控制面元数据。

### 12.2 ClickHouse 职责

继续保存：

- raw market bars。
- canonical market bars。
- quote latest/history。
- adjustment factors。
- 高频可分析 observations。
- 大规模时间序列和横截面查询。

### 12.3 SQLite 职责

最终目标：

- 不参与实时 hot read。
- 不作为权威事件存储。
- 只允许用于本地离线工具、临时 index 或兼容读取。
- 可损坏、可删除、可从 WAL/checkpoint 重建。

### 12.4 Repository Traits

业务层只依赖小而明确的 trait，不复制当前超大 repository：

```rust
trait InstrumentRepository { ... }
trait JobRepository { ... }
trait AuditRepository { ... }
trait ArtifactManifestRepository { ... }
trait FundamentalRepository { ... }
trait MarketBarRepository { ... }
trait QuoteRepository { ... }
trait MigrationCheckpointRepository { ... }
```

每个 trait 的事务边界必须明确，禁止一个 repository method 隐式跨多个数据库。

### 12.5 Migration Owner

- Rust 是 migration 唯一 owner。
- PostgreSQL 使用 advisory lock。
- 每条 migration 有 version、checksum、applied_at、binary commit。
- ClickHouse migration 使用独立版本表和幂等 DDL。
- 启动只自动执行标记为 `safe_forward` 的 migration。
- destructive migration 必须通过离线维护命令和备份验证。

### 12.6 数据迁移策略

初期不搬数据：

- Rust 直接读取现有 PostgreSQL/ClickHouse schema。
- 先建立 typed models 和兼容查询。
- 对 Python/Rust 查询做差异比较。
- domain writer 切换后再删除 Python 数据库访问。

## 13. WAL、Checkpoint 与 Artifact

### 13.1 新 WAL 格式

推荐分段格式：

```text
segment header
  magic
  wal_version
  stream_id
  first_cursor
  previous_segment_hash

record
  length
  cursor
  received_at_ns
  schema_version
  flags
  payload
  payload_sha256
  crc32c

segment footer
  last_cursor
  record_count
  segment_sha256
```

建议每 256 MB 或固定时间滚动 segment；最终值由压测决定。

### 13.2 Checkpoint

Checkpoint 内容：

- latest cursor。
- catalog revision。
- scope ID。
- complete immutable books。
- unresolved gaps。
- active recovery。
- instrument/tick provenance。
- checkpoint source WAL position/hash。
- schema version。

写入流程：

1. 写临时文件。
2. fsync 文件。
3. 校验 hash。
4. 原子 rename。
5. fsync 目录。
6. 更新 checkpoint manifest。

### 13.3 启动恢复

```text
load checkpoint
→ verify checkpoint hash
→ locate WAL boundary via sparse index
→ replay only post-checkpoint records
→ validate final cursor/hash
→ bind upstream
→ publish ready
```

启动时间不应随全部历史日志大小线性增长。

### 13.4 Legacy JSONL 连续性

旧 JSONL 不原地改写。切换时生成 `wal-cutover-manifest.v1`：

- legacy path。
- legacy size。
- legacy tail cursor。
- legacy tail record SHA-256。
- legacy last MiB SHA-256。
- Rust first segment path。
- Rust first cursor。
- previous hash。
- runtime commit。
- PID 和切换时间。

### 13.5 Artifact Store

- raw Artifact immutable。
- 路径由 content hash 和 dataset/revision 生成。
- manifest 保存 source、time、schema、hash、size、media type。
- Python 不能直接登记 Artifact。
- Rust 验证后统一登记。
- Artifact retention 与权威 WAL retention 分开配置。

## 14. Job Engine

### 14.1 状态机

```text
pending
→ claimed
→ running
→ succeeded
→ failed_retryable → pending
→ failed_terminal
→ canceled
```

### 14.2 必备字段

- job_id。
- idempotency_key。
- job_type。
- request schema/version/hash。
- status/revision。
- owner_id/lease_token。
- lease_expires_at。
- attempt/max_attempts。
- created/started/finished timestamps。
- error code/classification。
- Artifact/result references。
- audit actor。

### 14.3 Worker 故障处理

- worker 超时后 lease 到期。
- Rust 将 retryable job 重新排队。
- 同一个 lease_token 才能提交结果。
- 过期 worker 的迟到结果必须拒绝。
- idempotency key 防止重复外部请求创建多个业务 job。
- 对可能收费或有副作用的 Provider 调用需要额外确认策略。

## 15. 启动与配置

### 15.1 单一 CLI

```text
marketcow serve
marketcow doctor
marketcow migrate
marketcow storage status
marketcow wal verify
marketcow wal replay
marketcow artifact audit
marketcow worker status
marketcow jobs inspect
marketcow import-bars
```

### 15.2 Preflight

任何副作用前必须验证：

- profile。
- bind host 为允许地址。
- storage root 为绝对路径。
- storage root 位于 allowed root。
- PostgreSQL DSN reference。
- ClickHouse endpoint/credential reference。
- required scope manifest 和 hash。
- Python executable/worker package revision。
- WAL/checkpoint schema compatibility。
- 端口不冲突。
- 真实订单禁用策略。

### 15.3 Secret Handling

- secret 不写日志。
- 配置只记录 secret reference。
- Python worker 只获得其 Provider 所需 credential。
- credential 通过受限环境、FD 或本机 secret broker 传递。
- worker dump 和错误信息必须脱敏。

### 15.4 Shutdown

顺序：

1. readiness 变为 draining。
2. 停止接受新 mutation/job。
3. 通知 WebSocket client server_shutdown。
4. 停止上游订阅。
5. drain ordered apply。
6. flush required WAL。
7. 生成必要 checkpoint。
8. 停止 Python worker。
9. 关闭数据库连接池。
10. 退出主进程。

## 16. 安全与权限

### 16.1 公开边界

Rust gateway 统一：

- session/JWT/service account 认证。
- RBAC。
- capability scopes。
- CSRF（浏览器 mutation）。
- body/query limit。
- rate limit。
- request timeout。
- audit。

### 16.2 内部边界

- UDS `0600`。
- 每个 worker 启动时进行 protocol handshake。
- worker binary/package revision 加入审计。
- Rust 验证 task_id、deadline、schema 和 hash。
- Python 结果视为不可信输入。
- staging path 必须做 canonical path 和 root containment 检查。

### 16.3 敏感操作

下单、转账、支付、账户变更或真实交易未来如需加入，必须另设：

- 独立进程/账户。
- 强认证和最小权限。
- 明确确认步骤。
- idempotency。
- pre-trade risk gate。
- 不可篡改审计。
- kill switch。

本迁移范围内保持禁用。

## 17. 可观测性

### 17.1 Metrics

实时：

- upstream frames/sec。
- decode/apply/WAL/publish latency p50/p95/p99/max。
- cursor published/persisted/indexed。
- cursor lag。
- reconnect/disconnect。
- gap opened/resolved/current。
- maximum book age。
- projection generation。
- per-channel depth/capacity/oldest age。
- per-client queue depth/slow close。

数据库：

- PostgreSQL pool usage、query latency、transaction rollback。
- ClickHouse insert/query latency、merge pressure。
- WAL fsync、segment size、disk usage。
- checkpoint duration/age。

Python worker：

- worker alive/restart count。
- active jobs。
- task latency。
- timeout/cancel/retry。
- provider rate-limit。
- result schema rejection。

### 17.2 Tracing

统一 trace 跨越：

```text
HTTP request
→ job create
→ worker dispatch
→ provider fetch
→ artifact validate
→ DB transaction
→ response/audit
```

实时事件 trace 使用采样，cursor、event_id 和 source timestamp 始终进入结构化日志。

### 17.3 Health 与 Readiness

`/v1/health` 返回组件状态，但不泄露凭据和内部路径。

`/v1/readiness` 仅在以下条件满足时 ready：

- API 已绑定。
- required database 已连接。
- WAL 可写。
- required realtime projection ready。
- scope/config revision 匹配。
- migration 无阻塞。

非关键 Python Provider worker 不应导致整个实时 API unready；对应 capability 单独 degraded。

## 18. 测试策略

### 18.1 单元测试

- Decimal/rounding。
- cursor/sequence。
- duplicate/out-of-order。
- book delta/full snapshot。
- tick transition。
- gap open/resolve。
- job lease/idempotency。
- auth/RBAC。
- migration checksum。
- WAL corruption detection。

### 18.2 Property Tests

- 任意合法事件序列 cursor 单调。
- replay 与在线 apply 得到相同 projection hash。
- 任意 invalid frame 不改变已提交状态。
- 任意 failure injection 不产生 persisted cursor 超前。
- Decimal serialization round-trip 不丢精度。

推荐 `proptest`。

### 18.3 Golden Contract Tests

- 捕获现有 Python API request/response corpus。
- Rust 对相同 request 输出逐字段比较。
- 对有时间字段的响应使用语义 comparator。
- 状态码、header、错误码和 schema 均参与比较。

### 18.4 Differential Replay

同一 WAL 同时送入 Python 和 Rust：

- 每 N 个 cursor 比较 projection hash。
- 每个 gap boundary 比较状态。
- 每个 recovery completion 比较状态。
- 发现首个 divergence 即停止并保存最小复现片段。

### 18.5 Integration Tests

- PostgreSQL real instance。
- ClickHouse real instance。
- Python worker UDS。
- process crash/restart。
- WAL/checkpoint recovery。
- gateway proxy。
- WebSocket slow consumer。

### 18.6 Long-running Tests

- 60 分钟正式负载。
- 24 小时无人值守稳定性。
- 7 天低频耐久运行作为 GA 前门槛。
- 内存 RSS 无持续增长。
- queue depth 无持续增长。
- checkpoint 保持启动恢复时间稳定。

## 19. 性能目标

以下为初始目标，须在 Phase 0 基线后确认：

| 指标 | 初始目标 |
|---|---|
| upstream receive → projection publish | p99 ≤ 20 ms |
| WAL append/fsync | p99 ≤ 20 ms |
| cursor publish lag | p99 ≤ 100 ms |
| scoped health | p99 ≤ 50 ms |
| 100-market bootstrap | p99 ≤ 250 ms |
| 100-market full-sync | p99 ≤ 250 ms，max 单独约束 |
| realtime hot DB query | 0 |
| steady-state health/bootstrap 503 | 0 |
| disconnect delta | 0 |
| max unavailable window | < 30 s |
| checkpoint-based startup | 与总 WAL 大小无关，目标 ≤ 30 s |

负载模型至少复现：

- 200-token Polymarket stream。
- health/bootstrap/snapshot/full-sync/events 并发轮询。
- 双消费者/双端验证负载。
- 4 个并发 exact-scope workflow。
- 慢客户端和取消请求。
- PostgreSQL/ClickHouse 背景压力。

## 20. 故障注入矩阵

| 故障 | 预期行为 |
|---|---|
| PostgreSQL 超时 | 控制面 mutation fail-closed；实时 projection 继续 |
| ClickHouse 不可用 | 需要权威 bar 落盘的发布受阻；book/quote 依契约继续 |
| derived index 损坏 | 标记 degraded，WAL 和 realtime 继续 |
| WAL fsync 超时 | 不发布需要 durability 的新 cursor |
| 磁盘接近满 | readiness degraded，停止产生不可持久化状态 |
| Python worker crash | job lease 到期重试；API 与实时链路继续 |
| Provider 429 | 按 Retry-After/策略重试，记录 source health |
| WebSocket 断线 | 进入 reconnect recovery，严格 cursor barrier |
| out-of-order frame | 审计并拒绝，不污染 book |
| invalid delta 后 full-book | 同一恢复边界原子发布 |
| 慢客户端 | 仅关闭该客户端，1013 |
| checkpoint 损坏 | 回退前一 checkpoint + WAL replay |
| WAL segment 尾部损坏 | 截止到最后完整记录，fail-closed 并报警 |

## 21. 迁移阶段与交付物

### Phase 0：契约冻结与基线

交付：

- 架构 ADR。
- API/WS/MCP 契约清单。
- 数据库 schema snapshot。
- r2/b16f replay corpus。
- Python golden response corpus。
- 当前性能和故障基线。
- domain ownership registry。

退出条件：所有迁移目标均有可自动比较的基线。

### Phase 1：Rust 平台骨架

交付：

- Cargo workspace。
- `marketcow serve/doctor/migrate`。
- config/preflight。
- structured logging/metrics/tracing。
- PostgreSQL/ClickHouse connection foundation。
- Python worker protocol handshake。
- shadow launchd unit。

退出条件：Rust shadow 服务可独立启动、诊断和关闭，不接生产消费者。

### Phase 2：统一 Gateway

交付：

- Axum 8790 shadow gateway。
- auth/RBAC/rate limit/audit。
- legacy FastAPI internal proxy。
- OpenAPI/schema endpoint。
- request/response golden diff。

退出条件：所有现有公开请求可经 Rust gateway 到达正确实现，契约无差异。

### Phase 3：Polymarket Rust Realtime

交付：

- provider adapter。
- normalizer。
- WAL/checkpoint。
- single-writer state。
- projection。
- health/bootstrap/snapshot/events/full-sync/stream。
- scope/recovery/tick/relation。
- r2 replay differential verifier。

退出条件：离线 replay 零 divergence，shadow 60 分钟和 24 小时通过。

### Phase 4：Storage 与主链路

按顺序迁移：

1. Instrument Master。
2. runtime config/migration checkpoint。
3. job/audit。
4. Artifact manifest。
5. quote/bar/adjustment/canonical。
6. fundamentals/dividends/calendar。
7. CSV/history workflows。

退出条件：对应 domain Python writer 被禁用，Rust 成为唯一 owner。

### Phase 5：Python Worker 化

交付：

- Provider handlers。
- staging Artifact protocol。
- worker pool supervision。
- retry/cancel/timeout。
- 删除 Python DB credentials。
- 删除 Python public API 依赖。

退出条件：Python 只执行 Provider/transform task。

### Phase 6：其他 Realtime Provider

顺序：

1. Hyperliquid。
2. generic RealtimeHub。
3. LongPort。
4. EastMoney/Sina polling quote。

退出条件：统一 sequence、replay、subscription 和 downstream contract。

### Phase 7：正式切换

交付：

- launchd 正式指向 Rust。
- WAL cutover manifest。
- 8790 consumer cutover。
- rollback package。
- 24 小时/7 天 Artifact。
- 运维 runbook。

退出条件：观察期完成，legacy FastAPI 和 8791/8794 可移除。

## 22. Domain 切换流程

每个 domain 使用统一模板：

```text
1. inventory
2. contract freeze
3. Rust read implementation
4. shadow read diff
5. Rust write implementation
6. write cutover preparation
7. freeze Python writer
8. record boundary/checkpoint
9. enable Rust writer
10. observe
11. either finalize or rollback
12. remove Python DB access
```

Ownership registry 示例：

```yaml
domains:
  polymarket_realtime:
    owner: rust
    writer_since_cursor: 30000000
    rollback_compatible: true
  fundamentals:
    owner: python
    rust_shadow_read: true
  market_bars:
    owner: rust
```

该 registry 必须版本化、审计并在 readiness 中暴露 hash。

## 23. 正式切换与回滚

### 23.1 切换前

- Rust commit 固定。
- 配置 hash 固定。
- Python worker package revision 固定。
- 数据库 migration 已验证。
- WAL legacy tail 已记录。
- exact scope 双实现对比通过。
- 60 分钟和 24 小时验收通过。
- 回滚命令和 owner 明确。

### 23.2 切换

1. Rust MarketCow owner 将服务置于 draining。
2. 停止旧 writer 接收新状态。
3. flush authoritative WAL。
4. 记录 cursor/hash/boundary。
5. 启用 Rust writer。
6. Rust 8790 ready 后恢复消费者。
7. 保留 Python legacy 但禁止写入。

### 23.3 回滚

1. 停止 Rust writer。
2. flush Rust WAL。
3. 记录 Rust tail cursor/hash。
4. 验证 Python 可读取兼容 schema。
5. 从同一 cursor boundary 恢复 Python writer。
6. 重新验证 exact scope。
7. 记录回滚审计。

禁止：

- 两个 writer 同时运行。
- Tradude 执行切换。
- 用删除 WAL、重置 cursor 或放宽 fail-closed 完成回滚。

## 24. 验收标准

### 24.1 功能

- 全部公开 API 契约一致。
- 全部 WebSocket 序列语义一致。
- MCP 工具结果和权限一致。
- Python Provider 能被 Rust job engine 调用。
- PostgreSQL/ClickHouse 只有 Rust writer。
- Artifact raw→canonical 可追溯。

### 24.2 实时

- b16f 8790 HTTP 200/index_ready。
- 100 markets、200 books、100 complete。
- tick 200/200、gap 0。
- cursor 持续推进。
- memory projection hot path。
- hot DB query 0。
- 无 health/bootstrap 503。
- 无超过 30 秒不可用窗口。
- disconnect delta 0。
- 队列有界。

### 24.3 数据与持久化

- WAL append-only。
- legacy→Rust hash chain 连续。
- checkpoint replay 与在线 projection hash 一致。
- PostgreSQL transaction tests 通过。
- ClickHouse idempotency 和 canonical tests 通过。
- 金额和 decimal 边界测试通过。

### 24.4 运维

- Rust 单一启动入口。
- Python worker 可独立重启。
- readiness 能准确反映能力状态。
- metrics、logs、traces 完整。
- backup/restore 演练通过。
- rollback 演练通过。
- 真实订单禁用证据存在。

## 25. 风险与缓解

### 25.1 大爆炸重写

风险：长期无法交付，语义漂移难定位。

缓解：Gateway + domain ownership + shadow differential 渐进迁移。

### 25.2 Rust 复制原架构问题

风险：把锁内复制、队列耦合和非原子恢复翻译成 Rust。

缓解：先冻结并评审并发模型；使用 single writer 和 immutable generation。

### 25.3 Decimal/JSON 差异

风险：Rust/Python 对 decimal、时间和空值序列化不同。

缓解：golden corpus、canonical serialization 和跨语言 contract tests。

### 25.4 Python Provider 不稳定

风险：第三方库阻塞、泄漏、崩溃或修改全局状态。

缓解：独立 worker process、deadline、memory/restart budget、无 DB 凭据。

### 25.5 数据库双写

风险：同一 domain 出现两套事实。

缓解：ownership registry、单 writer gate、数据库 advisory lock 和审计。

### 25.6 WAL 格式迁移

风险：破坏历史连续性或无法回滚。

缓解：不重写旧日志、cutover manifest、legacy reader 和 hash chain。

### 25.7 LongPort Rust 支持不足

风险：没有可靠官方 Rust SDK。

缓解：先使用 Python bridge 只输出 raw typed events；Rust 仍拥有 sequence/projection/WAL/API。

### 25.8 MCP 生态兼容

风险：Rust MCP SDK 与现有 Python 行为有差异。

缓解：Rust 先拥有 transport/auth，工具实现可阶段性内部代理，使用 golden tests。

## 26. 工程计划与粗略工作量

以人周估算：

| 工作流 | 估算 |
|---|---:|
| Phase 0 契约与基线 | 2–3 |
| Rust 平台、CLI、配置、观测 | 3–5 |
| Gateway、Auth、Legacy Proxy | 3–5 |
| WAL/Checkpoint | 3–5 |
| Polymarket Rust Realtime | 6–10 |
| PostgreSQL/ClickHouse 基础 | 4–6 |
| Domain storage migration | 8–12 |
| Python worker 化 | 4–6 |
| 其他 Realtime Provider | 5–8 |
| 切换、回滚、长期验收 | 3–5 |

总体约 41–65 人周，可并行。首个可证明价值的里程碑应是：

```text
Rust platform
+ segmented WAL/checkpoint
+ Polymarket realtime
+ shadow API
+ r2 differential replay
```

该里程碑约 14–23 人周，具体取决于现有契约测试复用程度和 LongPort 是否进入第一阶段。

## 27. 建议 WorkItems

1. 建立 Rust workspace 与编码规范。
2. 冻结跨语言 domain contracts。
3. 构建 r2/b16f replay corpus。
4. 实现 Rust config/preflight/CLI。
5. 实现 Rust PostgreSQL migration foundation。
6. 实现 Rust ClickHouse repository foundation。
7. 实现 segmented WAL/checkpoint。
8. 实现 Polymarket normalizer。
9. 实现 Polymarket single-writer projection。
10. 实现 Polymarket Rust API/WebSocket。
11. 实现 Python/Rust differential verifier。
12. 实现 Rust Gateway 与 legacy proxy。
13. 实现 Python worker UDS/protobuf。
14. 迁移 Instrument Master domain。
15. 迁移 job/audit domain。
16. 迁移 quote/bar/canonical domain。
17. Python Provider worker 化。
18. Hyperliquid realtime 迁移。
19. LongPort realtime 迁移或 bridge。
20. MCP transport 迁移。
21. launchd 与正式切换。
22. 24 小时/7 天稳定性与回滚验收。

## 28. 待确认技术决策

以下决策应在 Phase 0 形成 ADR：

1. WAL record encoding：protobuf、postcard 或 versioned canonical JSON。
2. WAL segment 大小和 fsync policy。
3. `published_cursor` 是否允许短暂领先 `persisted_cursor`，以及哪些事件必须同步 durability。
4. Rust ClickHouse client 选择和连接池策略。
5. LongPort Rust SDK 是否满足实时订阅契约。
6. MCP Rust SDK 的成熟度与迁移顺序。
7. Rust/Python 协议采用 gRPC/UDS 或自定义 framed protocol。
8. Python worker sandbox、资源限制和 credential 传递方式。
9. 8791/8794 的退役时间和兼容观察期。
10. 60 分钟、24 小时、7 天验收的具体负载参数和性能 SLO。

本文推荐的默认选择是：

- protobuf + UDS/gRPC。
- `sqlx` PostgreSQL。
- typed ClickHouse Rust client。
- segmented WAL + CRC32C + SHA-256 chain。
- `ArcSwap` immutable projection。
- Axum/Tokio。
- Rust 单一公开入口。
- Python 无状态 Provider worker。
- 每个 domain 单 writer。

## 29. Definition of Done

迁移完成必须同时满足：

- Rust 是唯一公开服务入口和主进程。
- Rust 是 PostgreSQL、ClickHouse、WAL、Artifact manifest 和 job state 的唯一 owner。
- Python 只负责 Provider 抓取和转换。
- 所有公开契约通过 golden diff。
- 所有关键金融 decimal/time 测试通过。
- r2 和 b16f replay 零不可解释 divergence。
- exact scope 和长期稳定性验收通过。
- 启动时间不随完整历史 WAL 线性增长。
- derived index 故障不阻塞 realtime。
- Python worker 故障不影响 8790。
- 真实订单保持禁用。
- 切换、回滚、backup/restore 均有可复现 Artifact。
- legacy Python public API、database repository 和 realtime owner 已退役。

## 30. 最终建议

本迁移应按“平台底座 → Polymarket 实时样板 → 数据库所有权 → Python worker 化 → 其他实时 Provider”的顺序推进。

最关键的不是 Rust 代码比例，而是完成以下所有权重构：

1. Rust 统一公开接口和服务生命周期。
2. Rust 统一权威数据主链路和数据库访问。
3. Rust 统一实时状态、WAL 和恢复边界。
4. Python 收敛为可替换、可监督、无状态的数据源执行器。

只有在这些边界真正落实后，Rust 才能带来可预期的尾延迟、内存上限、长时间稳定性和可维护性收益。
