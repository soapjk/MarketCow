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
- `MARKETCOW_POSTGRES_DSN` / `MARKETCOW_POSTGRES_DSN_REF`
- `MARKETCOW_CLICKHOUSE_PASSWORD` / `MARKETCOW_CLICKHOUSE_PASSWORD_REF`
- `MARKETCOW_CLICKHOUSE_HOST`、`MARKETCOW_CLICKHOUSE_DATABASE`
- Provider 凭证，例如 `TUSHARE_TOKEN` 与 `MARKETCOW_LONGPORT_*`

## 启动

```bash
uv sync
uv run marketcow --profile development doctor
uv run marketcow --profile development start --host 127.0.0.1 --port 8792
```

正式服务默认监听 `127.0.0.1:8790`：

```bash
uv run marketcow --profile production start --host 127.0.0.1 --port 8790
```

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
