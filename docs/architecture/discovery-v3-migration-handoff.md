# Discovery v3 改造交接文档

> 用途：把 `discovery.v2` → `discovery.v3` 的改造工作交接给下一个会话。
> 状态：**进行中**，改造到一半，剩余工作见 §4。

---

## 1. 任务背景

把 MarketCow 的 Polymarket Discovery 接口从 `discovery.v2` 改造成 `discovery.v3`，严格按
Tradude 冻结的契约实现。

- **契约源（唯一权威，务必先完整读一遍）**：
  `/Volumes/T9/projects/trade/tradude/docs/plans/2026-09-03-marketcow-discovery-contract-freeze.md`（182 行）
- **设计背景**：
  `/Volumes/T9/projects/marketcow-worktrees/polymarket-discovery-hot-scope/docs/architecture/polymarket-fresh-discovery-hot-scope.md`

### 已确认的两个决策

1. **直接改造 v2 → v3**，不做 v2/v3 并存，保持单一接口（Tradude 一次性迁移，不保留旧签名）。
2. **full-sync 为单次原子响应**，超 `maximum_full_sync_bytes` 上限即 `413`，**不做分页**。

### 端口约定

- `8790`：统一网关，Tradude 实际调用的入口（`src/marketcow/api.py`）。
- `8795` / `8796`：内部端口，Tradude 不得调用。

---

## 2. 工作分支与位置

- 分支：`docs/polymarket-discovery-hot-scope`
- worktree：`/Volumes/T9/projects/marketcow-worktrees/polymarket-discovery-hot-scope`
- base commit：`ef74ab5`
- 主仓库 `/Volumes/T9/projects/marketcow` 在 `main`，**不要动**。
- 当前唯一改动文件：`src/marketcow/polymarket_discovery.py`（**未 commit**）。

```bash
git worktree list
# /Volumes/T9/projects/marketcow                                           ef74ab5 [main]
# /Volumes/T9/projects/marketcow-worktrees/polymarket-discovery-hot-scope  ef74ab5 [docs/polymarket-discovery-hot-scope]
```

---

## 3. 已完成（可直接验证）

| 项 | 状态 |
|---|---|
| schema 常量升 v3（4 个常量） | ✅ |
| `DiscoveryFullSync` 模型 + fail-closed validator | ✅ |
| `DiscoveryDeltaItem` / `DiscoveryDeltaFrame` 模型 | ✅ |
| `install_discovery_openapi_extension` 改 v3 | ✅ |
| `_DiscoveryBoundary` 增加 `unresolved_gap_count` | ✅ |
| `snapshots` 表加 `unresolved_gap_count` 列（7 → 8 列） | ✅ |
| `_full_materialize` / `_incremental_materialize` / `_restore_published` 全部适配 8 列 | ✅ |
| 新增 `full_sync()` 方法（原子响应 + 413 字节上限） | ✅ |
| store `__init__` 新增 `maximum_full_sync_bytes` 参数 | ✅ |
| 新增 `DEFAULT_MAXIMUM_FULL_SYNC_BYTES` 常量（256 MiB） | ✅ |
| 物化目录 `discovery-materialized-v2` → `discovery-materialized-v3` | ✅ |
| 清理基类死代码（旧内存版 `capture` / `boundary` / `snapshot_page` / `metadata_page` / `relation` / `events_page` / `_page_offset` / `_load_catalog` / `_relations`） | ✅ |
| `python3 -m py_compile` 通过 | ✅ |

### 关键字段映射（已在代码中落地）

```text
snapshot_id          → projection_id
realtime_universe_id → universe_revision
gap_markets 表 COUNT → unresolved_gap_count
```

`unresolved_gap_count` 必须在 `_full_materialize` 里 `DROP TABLE gap_markets` **之前**取，
否则读到 0。

### `full_sync()` 的 ready 判定（已实现，供参考）

```python
ready = (unresolved_gap_count == 0) and (active_market_count > 0)
fail_closed_reason = (
    None                        if ready else
    "discovery_unresolved_gaps" if unresolved_gap_count > 0 else
    "discovery_empty_universe"
)
```

契约强制：`unresolved_gap_count > 0` 等价于 `ready=false`；`ready=true` 时
`fail_closed_reason` 必须为 `null`。缺失值保持 `null`，**禁止**用 0 / 空串 / 占位值替代。

---

## 4. 待完成（按序）

### 4.1 `events_page` → v3 delta 帧（文件内剩余唯一的 v2 方法）

`src/marketcow/polymarket_discovery.py:1733` 的 `events_page()` 仍返回 `DiscoveryEventPage`
并构造旧的 `DiscoveryEvent` 条目。改为返回 `DiscoveryDeltaFrame`，`items` 用
`DiscoveryDeltaItem`。

三种 `type` 的构造规则：

| type | 来源事件 | payload |
|---|---|---|
| `universe_changed` | universe 变化 | 携带新 `universe_revision`，**不得**带 market/relation |
| `relation_update` | `catalog_revision` 事件的 `relation_changes`（added / removed / changed） | 最新 `DiscoveryRelation` |
| `market_update` | `book` / `price_change` / `best_bid_ask` / `last_trade_price` / `tick_size_change` | 最新 `DiscoveryMarketQuote` |

帧需额外带 `projection_id` 与 `universe_revision`；`resync_required=true` 时 consumer 丢弃
本地投影并重新 `fetch_full_sync()`。

> ⚠️ **已确认风险（契约验收第 2 项，别漏）**
> 契约要求 consumer 侧强校验 `next_cursor == cursor + 1` 严格连续。但源 `event_offsets`
> 索引可能稀疏：上游 `polymarket_live.py:3522` 只在 append 时校验
> `previous_cursor == event.cursor - 1`，存在过滤写入路径。
> **实现时必须显式检测 gap 并置 `resync_required=true`**，否则 Tradude 侧会持续 fail-closed。

### 4.2 路由层改造（两个文件，都必须改）

**`src/marketcow/api.py`（8790 统一网关，Tradude 实际走这个，优先改）**

| 行号 | 现状 |
|---|---|
| 114-117 | import `DiscoveryEventPage` / `DiscoveryMetadataPage` / `DiscoverySnapshotPage` |
| 1768 / 1779 | `/discovery/snapshot` → `discovery.snapshot_page` |
| 1796 / 1806 | `/discovery/events` → `discovery.events_page` |
| 1824 / 1839 | WS 里硬编码 `"marketcow.polymarket.discovery-events.v2"` |
| 1859 / 1870 | `/discovery/metadata` → `discovery.metadata_page` |
| 1879 / 1890 | `/discovery/relations/{id}` → `discovery.relation` |
| 646 | store 实例化，需补 `maximum_full_sync_bytes` |

**`src/marketcow/polymarket_live_read_api.py`**

| 行号 | 现状 |
|---|---|
| 37-40 | import 三个 v2 模型 |
| 324 / 333 | `/discovery/snapshot` → `discovery.snapshot_page` |
| 347 / 354 | `/discovery/events` → `discovery.events_page` |
| 370 | WS 内 `discovery.events_page` |
| 373 | 硬编码 `.v2` 字符串 |
| 393 / 402 | `/discovery/metadata` → `discovery.metadata_page` |
| 411 / 419 | `/discovery/relations/{id}` → `discovery.relation` |
| 105 | store 实例化，需补 `maximum_full_sync_bytes` |

改造目标：

- 新增 `GET /v1/prediction-markets/polymarket/live/discovery/full-sync`
  （`response_model=DiscoveryFullSync`）。
- WS `/v1/prediction-markets/polymarket/live/discovery/stream` 增加 `projection_id` query 参数。
- **删除** `/snapshot`、`/metadata`、`/relations/{id}`、`/events` 四个路由。

> 注意：`polymarket_gateway.py` **不含** discovery handler，不用去那里找。

### 4.3 配置项

`src/marketcow/config.py`：

- 新增 `polymarket_discovery_maximum_full_sync_bytes`（默认值用 `DEFAULT_MAXIMUM_FULL_SYNC_BYTES`）。
- 新增环境变量绑定 `MARKETCOW_POLYMARKET_DISCOVERY_MAXIMUM_FULL_SYNC_BYTES`。

写法参照现有 `polymarket_discovery_depth_notionals`（第 82 行定义，第 251-260 行 env 绑定）。

### 4.4 收尾

- `DiscoveryMetadataPage` 模型（`polymarket_discovery.py:236`）若确认 v3 不再需要则删除。
- 测试：`tests/test_polymarket_discovery.py:139,143` 有 store 实例化，需同步适配。
- 跑通全量测试 + `python3 -m py_compile`。
- 逐条核对契约文档 §5 的 5 条验收标准。
- 确认 `8795` / `8796` 不被 Tradude 调用。

---

## 5. 已知技术债（非阻塞，接手时心里有数）

- `polymarket_discovery.py:572/575` 存在 `_InMemoryPolymarketDiscoveryStore = PolymarketDiscoveryStore`
  + 同名子类的怪结构。语义上现在只是「基类 + 磁盘物化子类」，建议后续合并成单类。
- 基类目前只剩 `__init__` / `_read_events` / `lifecycle_history`，其余均由子类 override。

---

## 6. 编码约定

用户要求：Python dict 取字段**不允许设默认值**。

```python
# ❌ 错误
trader_id = config.get("trader_id", "TRADER-001")

# ✅ 正确
trader_id = config["trader_id"]
```

原因：避免隐藏配置缺失，让配置错误尽早暴露。可选配置项才允许 `config.get("key")`。
新增代码请遵守。
