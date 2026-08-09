# Polymarket live：全量 catalog 阻塞主 API 的扩展性修复方案

> 状态：Accepted design v3（Phase 1–3 已实现，Phase 4 按需）
> 日期：2026-08-09
> 关联：`docs/polymarket-live.md`、`docs/polymarket-live-quality-report.md`
> 触发场景：Tradude scoped paper 验证期间，MarketCow `:8790` 在启动阶段被
> 1.8GB catalog 和 588MB event log 阻塞，端口迟迟不绑定。

## 1. 决策摘要

本方案不采用“把全量恢复延迟到第一次请求”作为最终修复，也不以多线程加速全量
Pydantic 加载。最终方向是：

1. **主 API 启动与 Polymarket 数据恢复彻底解耦**，应用创建阶段只读取小型 manifest；
2. **catalog 发布时生成不可变磁盘索引**，scoped bootstrap 通过精确 offset 读取；
3. **collector 维护可重建的 durable latest-state 索引**，scoped snapshot 不扫描全量事件；
4. **全量内存模式默认关闭并与主 API 隔离**，不得由普通 HTTP 请求隐式触发；
5. **主服务健康与 Polymarket 子系统健康分开报告**，所有不完整状态保持 fail closed。

这使股票等既有接口在 Polymarket catalog 损坏、未索引或正在更新时仍可启动和服务，
同时让 Tradude 只加载它实际使用的市场。

## 2. 已确认的现状与根因

当前调用链是同步的：

```text
uvicorn factory startup（主线程）
  -> create_app()
     -> LiveStateStore(...)
        -> LiveStateStore.__init__()
           -> recover()
              -> _load_catalog()
                 -> 逐行 LiveMarket.model_validate_json(...)
              -> _read_all_events()
              -> _rebuild_from_checkpoint()
```

实测数据规模：

| 指标 | 数值 |
|---|---:|
| Gamma keyset 市场 | 129,410 |
| normalized 市场 | 129,366 |
| active token | 258,732 |
| normalized catalog | 1.8GB |
| events.jsonl | 588MB |
| 单市场完整 typed 定义 | 约 14KB |

`LiveStateStore.__init__()` 当前直接调用 `recover()`；`create_app()` 必须等构造返回后
uvicorn 才能完成端口绑定。因此 catalog 验证、对象构造和事件重放全部发生在绑定
`:8790` 之前。

### 2.1 当前加载是不是多线程

**不是。** 这条启动路径是同步、单线程的。`_sync_lock` 是 `threading.RLock`，用途是
互斥和状态一致性，不会创建线程或并行解析。当前 collector 的部分网络调用可能使用
`asyncio.to_thread()`，但它不改变 API 启动时 `recover()` 的执行模型。

即使把 catalog 校验拆成多个线程，也不是正确的主修复：

- Pydantic/Python 对象构造仍产生上十万份常驻对象，内存问题不消失；
- CPU 型 Python 工作受 GIL 和跨线程协调影响，收益不确定；
- 多线程会增加峰值内存，并使确定性顺序、错误归因和 fail-closed 更复杂；
- event replay 有严格 cursor 顺序，本身不能任意并行；
- 最关键的问题是“无关主 API 必须等待全量数据”，而不是线程数不足。

I/O hash、离线索引构建可在独立 worker **进程**中并行，但请求线程不得触发全量恢复。

## 3. 目标与非目标

### 3.1 目标

- launchd 启动后 10 秒内绑定 `:8790`，股票和通用健康接口可用；
- scoped bootstrap/snapshot 的资源消耗与请求市场数相关，而非与全市场规模相关；
- catalog、book、gap、cursor、raw evidence 的可审计和 fail-closed 语义不降低；
- collector 与 API 可独立进程运行，通过 durable contract 同步；
- 旧的未索引数据不会被误认为可提供 scoped 服务。

### 3.2 非目标

- 不减少 collector 对官方免费全市场数据的采集覆盖；
- 不合成盘口、撤单、queue position 或缺失业务事实；
- 不在本方案中实现交易；
- 不承诺主 API 进程内的全市场常驻内存模式。

## 4. 目标架构

```text
Gamma/CLOB/WebSocket
        |
        v
collector（唯一写者）
  - append-only raw/event evidence
  - immutable normalized catalog JSONL
  - immutable catalog index
  - durable latest-state/checkpoint index
        |
        | atomic manifests + cursor/revision binding
        v
MarketCow API（只读）
  - startup: manifest/index metadata only
  - scoped bootstrap: indexed catalog row reads
  - scoped snapshot: indexed latest book/gap reads
  - scoped events: cursor/market index reads
        |
        v
Tradude scoped consumer
```

### 4.1 启动控制面：只加载小型 manifest

`LiveStateStore.__init__()` 不再调用 `recover()`，也不加载 catalog/books/events。
它只初始化路径和状态机。`create_app()` 应在近似常量时间返回。

新增 `PolymarketLiveReadStore`，启动时最多读取并验证：

- `catalog.json`；
- `catalog-index.json` 或 SQLite index header；
- `latest-state.json`/SQLite header；
- 小型 health/coverage manifest。

任何大型文件扫描、Pydantic 全量构造或 event replay 都禁止出现在 `create_app()`、
`/v1/health` 和普通 scoped 请求路径上。

旧提案中的 `_ensure_loaded()` 只允许作为显式、独立 full-universe worker 的内部机制，
不能放在所有 handler 共用的 `sync()` 中，否则首次请求仍会把主 API 拖死。

### 4.2 Catalog 不可变精确索引

collector 发布 normalized JSONL 时，同步生成 content-addressed index。推荐 SQLite
只读索引或等价的紧凑二进制索引：

```text
markets(
  market_id PRIMARY KEY,
  byte_offset,
  byte_length,
  row_sha256,
  active,
  closed,
  catalog_revision
)
tokens(token_id PRIMARY KEY, market_id, outcome_label)
metadata(schema_version, catalog_revision, catalog_sha256, market_count, built_at)
```

发布顺序：

1. 写临时 normalized JSONL 和临时 index；
2. fsync；
3. 完整校验 market count、唯一 ID、每行 SHA-256、index offsets 和 catalog revision；
4. 将两者移动到不可变 content-addressed 路径；
5. 最后原子替换小型 `catalog.json` manifest。

API 启动时完整校验小型 index 和 manifest binding。每次 scoped 读取使用
`byte_offset + byte_length` 精确读取，校验 `row_sha256`，再做单行
`LiveMarket.model_validate_json()`。不得使用字符串 grep：grep 每次仍扫描 1.8GB，且容易
产生模糊匹配和不可预测延迟。

大型 catalog 的完整 SHA-256 在发布时已经确认；运行期可由独立 verifier 复核。选中行
在每次读取时仍有行级 hash，因此单行篡改会 fail closed，而无需每个请求重哈希 1.8GB。

### 4.3 Durable latest-state 索引

仅给 snapshot 增加 catalog 直读不够：当前最新盘口和 gap 需要重放 588MB event log。
collector 必须在追加权威 event 的同时，通过单事务更新一个可重建的 latest-state
索引。推荐 SQLite WAL（单 writer、多 reader）：

```text
books(token_id PRIMARY KEY, market_id, cursor, book_epoch,
      canonical_payload, payload_sha256, raw_payload_sha256,
      exchange_at, received_at, state_checksum)
gaps(gap_id PRIMARY KEY, market_id, token_id, code, detected_at,
     resolved, resolution, cursor)
market_state(market_id PRIMARY KEY, catalog_revision, latest_cursor,
             token_coverage, frame_status)
event_offsets(cursor PRIMARY KEY, byte_offset, byte_length, event_id)
metadata(schema_version, catalog_revision, checkpoint_cursor,
         event_log_size, event_log_prefix_sha256)
```

权威来源仍是 append-only event log；latest-state index 是派生缓存，可从 checkpoint +
events 确定性重建。更新顺序和 crash recovery 必须保证：

- event 先 durable append，再更新 index；
- index cursor 不得超过 durable event cursor；
- 启动时若 index 落后，主 API 返回 503；显式 recovery/migration 从已验证
  checkpoint + 完整 event log 确定性重建，普通请求绝不触发重放；
- index 超前、hash 不匹配或 cursor 不连续时标记 `integrity_failed`，不得返回 ready；
- API 使用只读事务获取同一 cursor 下的 books、gaps 和 market state。

### 4.4 API 契约

#### Scoped bootstrap

```http
GET /v1/prediction-markets/polymarket/live/bootstrap
    ?market_id=1005343&market_id=1007579
```

- `market_id` 数量限制建议 1–100；保持输入去重后的确定性顺序；
- 通过 catalog index 精确读取，不调用全量 `sync()/recover()`；
- 返回选中 markets、对应 active token、catalog revision 和 source binding；
- unknown market 逐项返回机器可读错误，或整体 404，需在 OpenAPI 固定一种语义；
- index 缺失/损坏返回 503/409 和稳定 code，不回退为全量扫描。

#### Scoped snapshot

```http
GET /v1/prediction-markets/polymarket/live/snapshot
    ?market_id=1005343&market_id=1007579
```

- 从 latest-state index 读取指定市场的两个 token、gap 和 typed facts；
- 一个只读事务固定 `catalog_revision + cursor`；
- 缺 book、stale、gap、revision 不一致或 index 落后时该 frame fail closed；
- 不得为了 scoped 请求加载全量 catalog 或读取完整 events.jsonl。

#### Scoped events/resume

现有 events API 使用 1–100 个必填 `market_id` 过滤，通过 `event_offsets` seek 权威
日志。索引当前覆盖完整本地 append-only log；未来若引入保留窗口，窗口外 cursor 必须
返回稳定 resync 语义，要求客户端重新取 scoped bootstrap + snapshot。

#### Full-universe 读取

不带 `market_id` 的全量 bootstrap/snapshot 不再允许在主 API 内隐式构建全市场内存态。
建议默认返回：

```json
{
  "code": "polymarket_full_universe_disabled",
  "message": "Use scoped market_id reads or the isolated full-universe service"
}
```

若确需兼容，必须由显式配置启用独立进程/端口，并设置内存、CPU、超时和并发预算；
不能只靠普通请求触发。

### 4.5 子系统健康状态

通用 `/v1/health` 不触发 Polymarket 加载。响应中新增轻量子状态，例如：

```json
{
  "polymarket_live": {
    "status": "index_ready",
    "catalog_revision": "...",
    "catalog_index_ready": true,
    "latest_state_ready": true,
    "latest_cursor": 123,
    "lag_ms": 420,
    "coverage": "partial",
    "reason_codes": []
  }
}
```

状态至少包含：`not_configured`、`legacy_unindexed`、`index_building`、`index_ready`、
`degraded`、`integrity_failed`。主 API 可以是 `ok`，但 Polymarket 数据状态必须独立、
真实且可观测。

## 5. 并发与资源模型

- API handler 只进行有界 index lookup、pread/SQLite read 和少量 Pydantic 校验；
- 同一个 scoped 请求使用只读 snapshot transaction，避免 catalog/state cursor 撕裂；
- collector 仍是单 writer，按 cursor 顺序写 event 和派生 index；
- catalog/index 构建在 collector 或独立 worker **进程**中完成；
- 全文件 hash/verifier 可后台运行，但未验证的数据不可提升为 certified/ready；
- 每请求限制 market 数、响应字节、执行时间和并发数；
- 不在线程间共享全量 `dict[str, LiveMarket]`。

多线程只适合少量 I/O 并发，不是 catalog 扩展性的基础。若需要并行 hash/index build，
优先使用受控 worker 进程，并确保输出仍按确定性顺序合并和原子发布。

## 6. 迁移与兼容

### Phase 1：立即恢复主 API 可用性（已完成）

1. 移除构造阶段 `recover()`；
2. `/v1/health` 只读小型 manifest；
3. 识别旧 catalog 为 `legacy_unindexed`；
4. scoped/full Polymarket 读取在 index 就绪前返回机器可读 unavailable；
5. 非 Polymarket API 不受影响。

### Phase 2：Catalog index 与 scoped bootstrap（已完成）

1. materializer 生成 index；
2. 提供离线 migration 命令为既有 immutable catalog 建索引；
3. 原子切换 manifest；
4. 上线 scoped bootstrap 和 OpenAPI schema。

### Phase 3：Latest-state index 与 scoped snapshot/events（已完成）

1. collector 双写 append log + derived state index；
2. 从现有 checkpoint/events 构建初始 index；
3. 对账 cursor、books、gaps、state checksum；
4. 开启 scoped snapshot/events；
5. Tradude 切换到完整 scoped 消费链路。

### Phase 4：隔离全市场能力

如仍有全市场扫描需求，将其放到独立进程/端口，配置资源上限和独立 health。主 MarketCow
API 永不因该进程加载或失败而停止服务。

## 7. 失败语义

| 场景 | 结果 |
|---|---|
| catalog manifest/index 缺失 | 503 `polymarket_catalog_index_unavailable` |
| index 与 catalog revision 不一致 | 409 `polymarket_catalog_integrity_failed` |
| selected row hash 失败 | 409 `polymarket_catalog_row_integrity_failed` |
| latest-state cursor 超前或 hash 失败 | 409 `polymarket_state_integrity_failed` |
| state index 落后 | 503 `polymarket_state_index_lagging` |
| market 不存在 | 404 `polymarket_live_market_not_found` |
| market 参数超限 | 400 `polymarket_scope_too_large` |
| 无参全量读取未启用 | 409/403 `polymarket_full_universe_disabled` |

任何失败都不得回退到隐式全量恢复、grep 全文件、空盘口或合成数据。

## 8. 验收标准

1. **启动隔离**：存在 1.8GB catalog 和 588MB events 时，launchd kickstart 后 10 秒内
   `:8790/v1/health` 返回 200；启动 RSS 不随 catalog 大小线性增长。
2. **无隐式全量加载**：health、股票接口和 scoped Polymarket 请求均不调用
   `_load_catalog()` 全量路径；有调用计数/测试证明。
3. **Scoped bootstrap**：2 个 market 返回精确 2 个定义，响应 <1MB、秒级完成，读取
   字节量有界，选中行 hash 和 catalog revision 校验通过。
4. **Scoped snapshot**：无需扫描完整 event log；同一事务返回两个 token、gap、cursor 和
   state checksum；缺失/过期/不一致 fail closed。
5. **Scoped resume**：指定 market 的事件按 cursor 确定性恢复，过期 cursor 明确要求
   resync。
6. **原子发布**：catalog/index 或 state index 任一半成品不可见；重启不混合 revision。
7. **故障注入**：覆盖 catalog 行/index/state/event 篡改、index 落后/超前、发布中断、
   writer crash、reader 并发和旧版未索引数据。
8. **资源上限**：100-market scoped 请求有明确 RSS、读取字节、P95 延迟和并发报告；
   结果不随 12.9 万市场全量规模线性增长。
9. **兼容回归**：MarketCow 全量测试、Ruff、build 通过；Tradude scoped bootstrap →
   snapshot → events/resume 测试通过。
10. **运行证据**：真实 production-like 数据验证启动时间、PID/health、catalog revision、
    scoped market IDs、cursor、coverage、RSS 和未触发全量加载。

## 9. `marketcow-fix-1095` 评估与移植结论

对 detached worktree `/private/tmp/marketcow-fix-1095` 的未提交实现做过逐项评估。
其 47 项 Polymarket live 聚焦测试通过，但内容只部分符合本提案：

| 内容 | 结论 | 处理 |
|---|---|---|
| 构造阶段移除同步 `recover()` | 有价值 | 已移植，作为 Phase 1 启动解耦 |
| `_recover_unlocked()`、显式 `recover()`、锁保护的一次加载 | 有价值 | 已移植，保持现有 writer/full-mode 行为 |
| restart/integrity 测试改为显式恢复 | 有价值 | 已移植 |
| `load_catalog_markets()` 顺序扫描 JSONL | 不满足扩展性 | 不移植；由 offset index 取代 |
| scoped 读取跳过 catalog 全文件 hash | 降低完整性 | 不移植；改为 index hash + row hash |
| `tail_cursor()` 从末尾 1MB 猜最后可解析 cursor | 证据不足 | 不移植；由 durable event-offset index 取代 |
| 无参请求通过 `_ensure_loaded()` 全量恢复 | 仅兼容过渡 | 不作为最终 API；主 API 最终默认禁用 full mode |
| `measure_scoped.py` | 仅临时实验 | 不移植；硬编码 production 路径且 Ruff 失败 |
| 原 Proposal v1 | 已被本方案替代 | 不移植 |

Phase 1 的启动解耦、Phase 2 的 immutable catalog offset index，以及 Phase 3 的
latest-state/event-offset WAL index 均已在专用实现分支完成。主 API 的七条实时读取路径
要求显式 scope；不会调用兼容性 full recovery。`LiveStateStore._ensure_loaded()` 仅保留给
collector、离线迁移与显式 full-state 内部操作。

## 10. 已落地决策

1. Catalog index 使用 immutable SQLite offset 文件；manifest 绑定其 SHA-256。
2. Latest-state index 单独使用 SQLite WAL；event log 仍是权威来源。
3. 无参全量 API 在主服务返回 `polymarket_full_universe_disabled`。
4. scoped 请求上限为 100 个去重 market IDs。
5. 旧数据通过两个显式离线命令依次构建 catalog 与 state index；主 API 不自动迁移。

## 11. 结论

问题本质不是“线程不够”，而是把全市场、全事件的对象化恢复放进了共享 API 的启动和
请求路径。升级后的方案通过 immutable catalog index、durable latest-state index 和
scoped API，把成本从 `O(全市场 + 全日志)` 降为 `O(请求市场 + 尾部事件)`，同时保留
MarketCow 的审计、hash、cursor 和 fail-closed 边界。
