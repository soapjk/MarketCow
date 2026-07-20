# ADR-003：PostgreSQL + ClickHouse 独立蓝绿在线架构

状态：已采纳（实现按 BG-001～BG-020 分阶段验收）

日期：2026-07-21

## 背景

ADR-002 确立了 PostgreSQL、ClickHouse、Parquet 和 DuckDB 的长期职责，但其迁移阶段允许同一进程
以 DuckDB 为 primary、向 ClickHouse shadow write，并在 ClickHouse 读取失败时回退 DuckDB。这种
进程内过渡方式已经完成了存储能力验证，却会把旧文件库继续留在 V2 的启动、写入、查询和故障链中，
无法证明新服务能独立运行，也让“回滚”同时包含数据层和进程内路由状态。

本决策取代 ADR-002 中“进程内双写、查询回退和 DuckDB 在线对账副本”的迁移方式；ADR-002 的数据库
选型、数据模型、溯源、冷热分层和容量原则继续有效。

## 决策

MarketCow V2 采用与旧正式服务进程、配置、数据根和生命周期隔离的蓝绿架构：

```text
旧正式服务（蓝）                         V2（绿）
DuckDB + 旧稳定代码                      API / Service
127.0.0.1:8790                           ├─ PostgreSQL
独立继续运行、整体回滚目标               ├─ ClickHouse
                                        └─ local WAL / artifacts / Parquet

只读旧库副本 ──显式离线导入器──► PostgreSQL / ClickHouse
```

### 在线数据库职责

- PostgreSQL 是全部事务型事实、证券主数据、基本面、日历、Provider 状态、采集控制面、任务、
  validation/funnel、Artifact manifest、配置版本和迁移 checkpoint 的权威存储。
- ClickHouse 是全部 raw/canonical 行情、latest quote、历史范围、分页、横截面、matrix 和 as-of 查询的
  权威在线存储。
- 本地文件只保存有界 WAL/spool、原始 Artifact、可校验 Parquet 冷归档和本地密钥；文件元数据由
  PostgreSQL 管理，行情重放结果由 ClickHouse 收敛。
- V2 在线启动、写入、查询、fallback、health/readiness、scheduler 和 operator 不得实例化、打开、
  查询或探测 DuckDB，不得把 `.duckdb` 文件存在性视为 ready 条件。

### DuckDB 边界

DuckDB 只允许用于：

1. 显式调用的离线只读导入器，输入必须是旧正式数据库的隔离副本而非活动文件；
2. test fixture 和 disposable migration drill；
3. 显式离线分析、Parquet 检查或研究工具。

这些模块不得被 `marketcow.api`、`marketcow.service`、在线 Repository 工厂、health/readiness 或后台
scheduler 导入。离线入口必须拥有独立命令、路径 containment、只读打开、schema/version 检查和
production 源授权闸门。真实 production 数据的读取或复制不由本 ADR 授权。

## 一致性模型

- PostgreSQL 内部事务使用数据库事务、唯一约束和单调版本；ClickHouse 使用稳定业务键、显式版本、
  `ReplacingMergeTree`/`FINAL` 与内容 tie-break 实现确定性逻辑收敛。
- 跨库不提供分布式事务。一次采集先形成稳定 ingest/run ID 与持久化 intent，再分别推进数据库步骤；
  每个步骤幂等且有 durable checkpoint。未完成步骤保留在有界 WAL/queue 中。
- API 不进行在线跨库 Join。需要组合的结果由服务层按已记录 watermark 组合；若依赖 watermark 不一致，
  返回有界 degraded/unavailable 诊断，不能回退 DuckDB 或静默返回混合时间点。
- canonical 只处理已经确认持久化的 raw 完整逻辑批次；重放、重复和乱序输入必须得到同一逻辑结果。
- 迁移完成闸门要求全域业务键/内容 checksum、PIT/provenance、API golden 和连续稳定 watermark 全部通过。

## 故障模型

- PostgreSQL 不可用：依赖事务/元数据的写入和读取 fail-closed；health/readiness 按阈值进入 degraded 或
  unavailable。不得改写本地 DuckDB。
- ClickHouse 不可用：行情主写进入 durable WAL 并返回契约定义的明确失败/接受状态；行情读取返回有界
  错误。不得回退 DuckDB。恢复后显式或有界后台重放。
- WAL/spool 不可写、超配额或损坏：新写入在落库确认前失败；健康项继续处理，损坏项隔离，operator
  操作串行、可追溯、可恢复。
- 单库恢复但跨库 watermark 未追平：服务保持 degraded，停止切绿或后续迁移阶段。
- V2 进程崩溃：依赖 durable intent/checkpoint 恢复；不得依赖进程内队列作为唯一事实。

## 蓝绿切换与整体回滚

旧正式服务在全部本地演练期间保持独立运行且不被修改。V2 只从经过授权和校验的旧数据副本执行离线
全量导入与增量追平。切换是消费者目标从蓝服务整体改到绿服务；回滚是消费者目标整体恢复蓝服务，
不是在 V2 进程内把 Repository 切回 DuckDB。切换或回滚期间产生的数据由明确 watermark 和追平流程
处理，禁止两个服务共享活动 DuckDB 文件。

真实副本复制、production PostgreSQL/ClickHouse 连接或迁移、消费者/launchd 修改、部署、push、PR、
上传和发布均属于 BG-EXT，必须由用户对确切目标和动作另行授权。

## 模块依赖规则

机械规则位于 `docs/architecture/storage-v2-online-dependency-policy.json`，由
`tests/test_online_dependency_policy.py` 解析 Python AST 验证。规则固定：

- online entrypoints 不得直接导入 DuckDB driver、`marketcow.storage`、
  `marketcow.duckdb_repositories` 或登记的 offline-only 模块；
- 当前基线中的两条直接依赖债务必须逐条、精确登记，禁止新增或扩大；
- 后续 BG-007/BG-008 删除债务后，exceptions 必须同步缩减至空；
- offline-only 模块不得反向成为在线启动或 readiness 的依赖。

例外清单不是永久豁免，也不表示目标架构允许该依赖；它只是让迁移期间的静态门禁采用“不得新增债务”
语义。任何新增例外都必须通过正式 BG REV/BLOCK 验收，不能在普通实现中静默加入。

## 后果

正面影响：V2 可独立证明 PG/CH 可用性；故障语义不再被 DuckDB fallback 掩盖；回滚边界变成可审计的
服务级动作；旧服务和新服务数据根不会被同一进程同时写入。

成本与约束：V2 启动需要 PostgreSQL 和 ClickHouse 两套必要依赖；跨库一致性必须通过 intent、watermark
和对账管理；ClickHouse 故障期间不能依赖 DuckDB 维持行情读取；切换前必须完成迁移追平、API 兼容、
备份恢复、容量和整体回滚演练。

