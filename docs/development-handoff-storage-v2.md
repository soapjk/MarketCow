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
- PostgreSQL 目前只允许 development profile 显式启用，schema 必须以
  `_development` 或 `_test` 结尾；
- 基本面和行情仍由 DuckDB 承担，未连接或迁移正式 PostgreSQL。

## 二、仓库、分支和 worktree

GitHub 仓库：

```text
https://github.com/soapjk/MarketCow
```

仓库是公开仓库。任何后续 `git push`、PR、Release、上传或其他远程写入都必须先按项目规则披露目标、内容、可见性和存储后果，并在后续消息得到用户明确确认。

### 正式工作区

```text
路径：/Volumes/T9/projects/market-data-service
分支：main
提交：71f5b26
远端：origin/main 已包含 71f5b26
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
数据：/Volumes/T9/projects/market-data-service/data/
数据库：data/warehouse/market_data.duckdb
tmux session：marketcow-local
工作区：market-data-service
```

正式服务正在被 `epaper-dashboard` 使用。禁止为开发测试执行以下操作：

- 停止或重启 `marketcow-local`；
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
tmux session：marketcow-development
工作区：marketcow-storage-v2
```

开发服务启动命令：

```bash
tmux kill-session -t marketcow-development 2>/dev/null || true
tmux new-session -d -s marketcow-development \
  'cd /Volumes/T9/projects/marketcow-storage-v2 && exec .venv/bin/marketcow --profile development start >> logs-development/marketcow.log 2>&1'
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
完整测试：47/47 通过（加入环境隔离测试后当前为 51 个）
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

这是新会话首先应该实施的开发任务。

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

### 第 4 步：ClickHouse 影子写入

- 建立 `market_bar_raw`；
- 建立 `market_bar_canonical`；
- 实现数千到数万行微批写入；
- 写入失败进入本地 WAL/spool；
- 支持重放、幂等和延迟监控；
- DuckDB 与 ClickHouse 双写并对比行数、OHLCVA、时间边界和来源。

### 第 5 步：查询切换

- 先让 development 的 history 查询读取 ClickHouse；
- 保留 backend 开关和 DuckDB 回滚路径；
- 完成历史范围、横截面、多来源和缓存契约测试；
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
- 现有 51 个测试继续通过；
- 新 backend 有单元测试和显式集成测试；
- 上游失败仍有界，不占满服务线程；
- 双写失败不能阻断现有 DuckDB 主路径，除非测试明确验证 fail-closed；
- 数据记录保留 `source`、`observed_at`、`ingested_at` 和 `raw_artifact_id`；
- 接口兼容性变化必须先形成文档并获得用户确认。

## 十一、当前运行检查结果

本文写入前确认：

```text
main worktree：71f5b26，正式服务 8790 HTTP 200
feature/storage-v2：71f5b26，开发服务 8791 HTTP 200
tmux：marketcow-local 和 marketcow-development 均存在
开发分支基线测试：51 个测试通过，Ruff 和 git diff --check 通过
```

正式服务的当前进程是在加入 health `profile` 字段前启动的，因此它的 health 响应可能暂时没有 `profile`；这不代表配置错误。不要仅为补这个字段重启正式服务。

## 十二、新会话建议的第一条指令

可以直接使用：

```text
请先完整阅读 docs/development-handoff-storage-v2.md、ADR-001、ADR-002 和 AGENTS.md。
只在 /Volumes/T9/projects/marketcow-storage-v2 的 feature/storage-v2 分支工作，
不要影响 127.0.0.1:8790 正式服务。先实现 Storage Repository 接口隔离及 DuckDB backend，
保持现有 API 契约和全部测试通过；暂时不要连接或迁移正式 PostgreSQL/ClickHouse，也不要远程推送。
```
