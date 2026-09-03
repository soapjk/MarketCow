# MarketCow

MarketCow 是一个本地运行的统一金融数据 API。当前唯一运行架构使用 PostgreSQL 保存事务、元数据、基本面与控制面数据，使用 ClickHouse 保存 raw/canonical 行情。

## 运行架构

- PostgreSQL：元数据、基本面、任务状态、Provider health、Artifact manifest。
- ClickHouse：实时报价缓存、raw/canonical market bars 与全部在线行情查询。
- 本地 WAL/spool：ClickHouse 写入失败后的可靠、有界重放。
- canonical scheduler：从已确认的 raw 数据确定性生成 canonical 数据。

运行时不存在其他存储 backend、shadow write 或进程内 fallback。`production`、`development`、`test` 三个 profile 都要求显式配置 PostgreSQL、ClickHouse 和 allowed root。

## 配置

复制相应模板并填写本地凭证：

```bash
cp .env.development.example .env.development
# 或
cp .env.production.example .env.production
```

配置文件必须保持在本地且不得提交。核心变量包括：

- `MARKETCOW_ALLOWED_ROOT`

See [docs/storage-layout.md](docs/storage-layout.md) for the local source/data
separation and the production runtime directory layout.
- `MARKETCOW_POSTGRES_DSN` / `MARKETCOW_POSTGRES_DSN_REF`
- `MARKETCOW_CLICKHOUSE_PASSWORD` / `MARKETCOW_CLICKHOUSE_PASSWORD_REF`
- `MARKETCOW_CLICKHOUSE_HOST`、`MARKETCOW_CLICKHOUSE_DATABASE`
- Provider 凭证，例如 `TUSHARE_TOKEN` 与 `MARKETCOW_LONGPORT_*`

供 LLMAY 使用的公网只读入口默认关闭；双 JWT、接口白名单、限流、审计和密钥轮换
配置见 [LLMAY 公网只读接入](docs/public-read-api.md)。

## 启动

```bash
uv sync
uv run marketcow --profile development doctor
uv run marketcow --profile development start --host 127.0.0.1 --port 8792
```

开发模式可以直接启动单个 API 进程。正式环境只有一个受支持的启动入口：

```bash
ops/launchd/install.sh
```

该入口一次性启动 PostgreSQL、ClickHouse、股票/加密资产实时能力、Polymarket 热
scope、Polymarket 完整目录/有界实时 discovery 和统一 API。所有业务 HTTP、WebSocket
与 MCP 接口都收口到 `127.0.0.1:8790`；`8795`、`8796` 仅供受管进程在 loopback 上
内部通信，旧端口 `8794` 已停用。不要在生产环境单独运行 `marketcow start` 或任何
Polymarket 脚本。
详细边界见 [统一生产启动](docs/production-startup.md)。

## Provider

HTTP API 不绑定某个 Provider。调用方可在请求中指定 `provider`；不指定时由路由策略按 capability 和市场选择。指定的 Provider 不支持该 capability 时返回结构化错误。新增 Provider 参见 [Provider 开发指南](docs/provider-development.md)。

当前报价 Provider 包括 LongPort、Tushare、Yahoo、Sina 和 Eastmoney。Provider 是否可用取决于凭证、账户权限、市场和对应上游能力。

LongPort 盘口价差按需直连查询，不使用报价缓存：

```bash
curl http://127.0.0.1:8790/v1/quotes/AAPL/spread
```

响应包含 `best_bid`、`best_ask`、`spread`、`spread_bps`、一档挂单量和完整的 `bids`/`asks` 档位。可返回的档位数与实时性取决于账户的 OpenAPI 行情权限。

## 验证

```bash
MARKETCOW_HOME=$(mktemp -d) uv run python -m unittest discover -s tests -q
uv run ruff check src tests
git diff --check
```

更完整的架构边界见 [当前运行架构](docs/architecture/current-runtime.md)。

Polymarket 全市场轻量发现 v2 的原子分页、增量续传、关系和历史事实契约见
[Polymarket discovery v2](docs/polymarket-discovery-v2.md)。该通道与最多 100 个市场的
热 Scope 完整 L2 通道相互独立。

## 管理后台

可视化与管理后台位于 `web/`，使用 React、TypeScript 和 Vite。开发时分别启动
MarketCow API 与前端：

```bash
uv run marketcow --profile development start --host 127.0.0.1 --port 8792
cd web
npm install
npm run dev
```

前端默认监听 `http://127.0.0.1:4173`，并将 `/v1` 代理到本地 API。详细的
Grafana、聚合指标和实时面板边界见
[可视化架构基线](docs/visualization/architecture-baseline.md)。

完整验证、分阶段启用、故障降级和回退方法见
[可视化运维手册](docs/visualization/operations-runbook.md)。

## MCP

MarketCow 服务启动时会默认在同一端口提供只读 MCP 入口。Agent 可通过它搜索标的、
读取缓存报价、历史 K 线、基本面、财务报表、分红及敞口事实：

```bash
uv run marketcow --profile development start --host 127.0.0.1 --port 8792
# MCP endpoint: http://127.0.0.1:8792/mcp
```

MCP 不直连数据库，也不开放刷新、导入或管理写操作。需要让 Agent 只在指定项目中
接入时，使用自带的 `marketcow-mcp-project` 安装/验证/升级/卸载 CLI，并严格遵循
[唯一权威的 Codex 项目级 MCP 安装指南](docs/mcp-project-installation.md)；服务协议、
工具清单和分析时的数据契约注意事项见 [MCP Server 文档](docs/mcp-server.md)。
可转债申购评分所需的发行条款、日历、缺失语义与市场估值契约见
[Convertible-Bond MCP v1](docs/mcp-convertible-bonds.md)。
