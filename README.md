# MarketCow

一个面向个人投资研究的本地金融数据 API。MarketCow 像一头克制工作的“数据奶牛”：摄取多个免费网页接口、公开数据包和社区数据客户端，经过统一、缓存、溯源和校验后，持续产出可供研究脚本、筛选器、机器人或看板使用的标准化数据。

[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

这不是带图形界面的投资应用，也不是行情供应商或公网数据产品。它是运行在使用者本机上的数据基础服务：下游系统通过 API 读取统一、缓存、可追溯的数据，采集任务则以低频、克制和可恢复的方式访问免费来源。

```text
研究脚本 / 筛选器 / 机器人 / 看板
                 │
                 ▼
        本地 HTTP API（FastAPI）
                 │
          ┌──────┴──────┐
          ▼             ▼
  DuckDB 本地缓存    Provider 适配器
  历史与质量记录     限速 / 回退 / 留档
                        │
                        ▼
                免费接口与公开数据包
```

## 适用场景与限制

适合：

- 个人研究工具按需查询行情、基本面和财务历史；
- 每日、每周或报告期级别的定时更新；
- 优先使用批量数据包完成低频全市场刷新，再从网页接口补充长尾字段；
- 多个本地下游共享同一份缓存、证券代码映射和数据口径；
- 需要来源记录、跨源校验和 point-in-time 约束的研究或回测准备。

不适合：

- tick、逐笔、毫秒级或其他高频行情采集；
- 对免费网页接口进行大规模并发、持续轮询或逐证券高频抓取；
- 对外提供有 SLA 的商业数据 API，或重新分发上游原始数据；
- 绕过登录、验证码、付费墙、限流或其他访问控制；
- 直接暴露到公网。管理接口目前没有身份认证。

这里的“免费数据源”是指当前接入路径通常不要求购买商业 API 密钥，不代表数据没有版权、没有服务条款，也不代表可以无限量访问。全市场任务应优先使用上游批量包；网页类来源只做低频更新、缺口补齐和交叉验证，并尽量读取本地缓存。

## 能做什么

- 提供 A 股、ETF、港股和美股的统一行情查询。
- 提供宏观经济事件、经济指标和中港美财报日历的统一读取接口。
- 保存 A 股基本面、完整三表和通达信历史财务数据。
- 支持带 `published_at`、`observed_at`、`ingested_at` 约束的 point-in-time 查询。
- 保存不可变原始文件 manifest，并记录 `source`、`source_url` 和原始响应定位信息。
- 对关键指标执行跨来源校验，差异超过 1% 时明确标记。
- 通过 FastAPI 提供本地 HTTP 接口，通过 DuckDB 支持本地分析。

免费来源可能随时改变字段、限流或使用条款。本项目不承诺来源持续可用；它提供的是适配器隔离、缓存、回退、原始数据留档和质量审计能力，而不是把不稳定来源包装成无限量的稳定数据。

## 快速开始

要求 Python 3.11 或更高版本，并建议使用 [uv](https://docs.astral.sh/uv/)。中国市场依赖已经包含在默认安装中，不需要额外选择依赖组。

### 一键安装

```bash
uv tool install git+https://github.com/soapjk/MarketCow.git
marketcow start
```

以后升级可运行：

```bash
uv tool upgrade marketcow
```

### 从源码运行

```bash
git clone https://github.com/soapjk/MarketCow.git
cd MarketCow
uv sync --locked
uv run marketcow start
```

服务默认仅监听 `127.0.0.1:8790`，交互式接口文档位于 `http://127.0.0.1:8790/docs`。

### 正式版与开发版隔离

运行配置分为 `production` 和 `development`。正式服务继续使用 8790 和现有 `data/`；
开发服务默认使用 8791 和完全独立的 `data-development/`：

| 配置 | HTTP 端口 | 数据目录 | 配置文件 |
|---|---:|---|---|
| production | 8790 | `data/` | `.env.production`，公共机密可放 `.env` |
| development | 8791 | `data-development/` | `.env.development`，公共机密可放 `.env` |

启动正式版：

```bash
uv run marketcow --profile production start
```

长期后台运行建议使用 macOS `launchd`，不依赖终端或 tmux 会话。项目提供的
LaunchAgent 只启动 production profile，固定监听 `127.0.0.1:8790`：

```bash
./ops/launchd/install.sh
launchctl print gui/$(id -u)/com.marketcow.production
curl http://127.0.0.1:8790/v1/health
```

卸载：

```bash
./ops/launchd/uninstall.sh
```

日志写入 `~/Library/Logs/MarketCow/production.log` 和
`~/Library/Logs/MarketCow/production.error.log`。LaunchAgent 通过安装在
`~/Library/Application Support/MarketCow/` 的包装脚本进入项目目录，避免将
launchd 自身的入口和日志放在外置卷。安装前必须先停止其他占用 8790 的进程，
避免 launchd 与手工启动的服务相互竞争。

启动开发版：

```bash
uv run marketcow --profile development start
```

也可以设置 `MARKETCOW_PROFILE=development`。`--profile` 必须放在子命令之前。
开发配置如果指向 8790 或默认正式数据目录，服务会拒绝启动。两个环境的健康接口会明确返回
`profile`、数据库路径和版本：

```bash
curl http://127.0.0.1:8790/v1/health
curl http://127.0.0.1:8791/v1/health
```

真实 `.env`、`.env.production` 和 `.env.development` 均被 Git 忽略；仓库只提交不含凭证的
`.env.production.example` 与 `.env.development.example`。开发环境应优先使用独立的上游凭证，
避免测试消耗正式环境配额。

启动命令会自动创建本地数据库和数据目录。此时已经可以按需查询 A 股、ETF、港股和美股行情：

```bash
curl 'http://127.0.0.1:8790/v1/quotes/600519'
curl 'http://127.0.0.1:8790/v1/quotes/AAPL'
```

### 首次同步中国市场数据

需要全市场基本面和通达信历史财务时，显式运行一次低频同步：

```bash
uv run marketcow sync-cn
```

默认行为：

- 自动选择当前广泛可用的报告期；
- 刷新一次 A 股全市场基本面和估值快照；
- 同步最近 4 个通达信财务报告期，已下载文件不会重复下载；
- 写入 DuckDB、保存原始响应和来源信息，并重建漏斗指标。

该命令会访问免费上游并可能运行较长时间，只应按日、按周或按报告期人工/定时执行，不应高频重复调用。可以用 `--tdx-periods 1` 缩小首次同步，或用 `--skip-tdx`、`--skip-fundamentals` 只运行其中一步。

### 安装检查

```bash
uv run marketcow init
uv run marketcow doctor
```

`doctor` 不访问网络，只检查 Python、全部数据依赖、本地目录、数据库 schema 和当前数据覆盖情况。

主要命令：

| 命令 | 用途 | 是否访问上游 |
|---|---|---|
| `marketcow start` | 初始化并启动本地 HTTP API | 查询接口按需访问 |
| `marketcow init` | 只创建数据库和数据目录 | 否 |
| `marketcow doctor` | 检查安装和本地数据覆盖率 | 否 |
| `marketcow sync-cn` | 低频同步中国市场基本面和历史财务 | 是 |

## API 使用示例

```bash
curl 'http://127.0.0.1:8790/v1/health'
curl 'http://127.0.0.1:8790/v1/quotes/AAPL'
curl 'http://127.0.0.1:8790/v1/quotes/0700.HK/history?range=1y&interval=1d'
curl 'http://127.0.0.1:8790/v1/fundamentals/600298?as_of=2026-07-17'
curl 'http://127.0.0.1:8790/v1/funnel/metrics?min_roe_median=15&max_pe=25'
curl 'http://127.0.0.1:8790/v1/snapshot?limit=50&days=30'
```

### Tushare 通用接口

在被 Git 忽略的本地 `.env` 中配置 `TUSHARE_TOKEN` 后，可以通过同一个路由调用任意
Tushare Pro 接口。路由中的名字就是官方 `api_name`，请求体原样接受 `params` 和
`fields`，因此上游新增接口时不需要在 MarketCow 中逐个增加适配代码：

```bash
curl -X POST 'http://127.0.0.1:8790/v1/tushare/daily' \
  -H 'Content-Type: application/json' \
  -d '{"params":{"trade_date":"20260717"},"fields":"ts_code,trade_date,open,high,low,close"}'

curl -X POST 'http://127.0.0.1:8790/v1/tushare/income' \
  -H 'Content-Type: application/json' \
  -d '{"params":{"ts_code":"600519.SH","period":"20251231"},"fields":""}'
```

文档中的特殊实时接口单独映射为：

```bash
curl -X POST 'http://127.0.0.1:8790/v1/tushare/realtime-quote' \
  -H 'Content-Type: application/json' \
  -d '{"ts_code":"600000.SH,000001.SZ,000001.SH"}'
```

所有成功的 Tushare 请求都会落库：`tushare_request` 保存接口名、参数、字段、来源及原始
文件定位，`tushare_data_row` 按行保存上游返回的完整动态字段。原始响应同时写入
`data/raw/tushare/`，因此统一字段映射不会造成上游字段丢失。

A 股分钟 K 线已经接入统一 history 路由，支持 `1m/5m/15m/30m/60m/1h`。分钟数据会
同时写入通用 Tushare 表和 `market_price_bar`，后者包含统一 OHLCV、成交额、完整源行及
`source`：

```bash
curl 'http://127.0.0.1:8790/v1/quotes/600000.SH/history?range=5d&interval=5m&adjustment=raw'
```

当前 Tushare HTTP 分钟接口按未复权数据接入，因此分钟请求必须明确使用
`adjustment=raw`；服务不会把未复权数据错误标记成前复权数据。

默认中转地址分别为 `https://fastapic.stockai888.top` 与
`https://realtime.stockai888.top`。Provider 强制发送 gzip 请求头，并以每次至少 0.5 秒
的间隔将频率限制在文档规定的每分钟 120 次以内。可通过
`TUSHARE_BASE_URL`、`TUSHARE_REALTIME_URL` 和 `TUSHARE_MIN_INTERVAL` 覆盖配置。

大多数用户使用 `sync-cn` 即可，不需要理解刷新顺序。需要精细控制时，底层运维接口位于 `/v1/admin/*`，例如：

```bash
curl -X POST 'http://127.0.0.1:8790/v1/admin/fundamentals/refresh?report_period=20260331'
curl -X POST 'http://127.0.0.1:8790/v1/admin/tdx/financials/sync?limit_periods=12'
curl -X POST 'http://127.0.0.1:8790/v1/admin/validation/rebuild?report_period=20260331'
```

这些管理接口目前没有身份认证。请保持默认 loopback 监听，不要把服务直接暴露到公网或不受信任的局域网。

### 日历数据接口

日历读取接口只访问本地 DuckDB，不会在每次读取时抓取上游。默认按 `Asia/Shanghai` 的当天过滤过期事件，并返回未来 30 天的数据：

```bash
curl 'http://127.0.0.1:8790/v1/economic-calendar?country=US&limit=50'
curl 'http://127.0.0.1:8790/v1/economic-indicators?country=US'
curl 'http://127.0.0.1:8790/v1/earnings-calendar?symbols=PDD,600519,00700&limit=50'
curl 'http://127.0.0.1:8790/v1/snapshot?limit=50&days=30'
```

首次使用或需要低频更新时，显式调用管理接口：

```bash
curl -X POST 'http://127.0.0.1:8790/v1/admin/economic-calendar/refresh?days=30'
curl -X POST 'http://127.0.0.1:8790/v1/admin/economic-indicators/refresh'
curl -X POST 'http://127.0.0.1:8790/v1/admin/earnings-calendar/refresh?days=30&symbols=PDD,600519,00700'
```

经济事件来自 BEA 与 Census 官方发布日历，经济指标来自 BLS Public Data API；财报日历使用 Nasdaq、上交所定期报告预约披露和公开的港股业绩公布日历。刷新命令应按日或更低频率运行，不应持续轮询。

`/v1/snapshot` 保留 `economic_calendar`、`economic_indicators` 和 `earnings_calendar` 三个数组，供依赖旧 snapshot 契约的本地消费者迁移。完整的字段、时区、参数和响应示例见[日历 API 契约](docs/calendar-api.md)。

## 数据与查询口径

运行时数据默认写入：

- `data/raw/`：带时间戳与内容哈希的原始响应；
- `data/warehouse/market_data.duckdb`：标准化快照、追加式历史、质量结果和任务状态。

上述目录均被 Git 忽略，不会进入源码包。

可通过 `MARKETCOW_HOME` 整体修改数据目录，或分别使用 `MARKETCOW_DB` 和 `MARKETCOW_RAW`。从已安装的软件包启动时，默认数据目录仍以当前工作目录为基准，不会写入 Python 的安装目录。

财务与漏斗接口支持 `as_of=YYYY-MM-DD`。严格 point-in-time 查询只使用在截止日已经发布、观测并入库的数据；缺少 `published_at` 的财务记录会被排除。今天下载到的历史报告期数据可能包含后续更正，不能冒充历史当时可见的版本。

港美股历史接口默认使用复权 OHLC。Yahoo 只直接提供复权收盘价，服务按 `adjclose/raw_close` 比例同步调整 OHLC，成交量不调整。单一来源结果会标记为 `single_source_unverified`。

## 数据源

当前适配器覆盖通达信/Mootdx、BaoStock、AKShare/东方财富、新浪和 Yahoo 等来源。各来源的服务条款、访问限制和数据权利独立于本项目代码许可证；使用者需要自行确认其场景是否获准。

建议访问方式：

| 来源类型 | 在服务中的用途 | 建议频率与批量策略 |
|---|---|---|
| 通达信财务包 / Mootdx | A 股历史财务底座 | 按报告期或低频计划任务批量同步，避免重复下载 |
| AKShare / 东方财富 | 基本面、完整三表和长尾字段补齐 | 低频刷新，优先读取缓存，不做逐证券高并发扫描 |
| BaoStock | 估值、财务指标和跨源校验 | 串行或受控并发，按日或报告期更新 |
| 新浪 | A 股与 ETF 最新行情 | 小批量按需查询，失败后回退或使用最近缓存 |
| Yahoo | 港美股最新行情与历史 OHLCV | 小批量按需查询；历史区间结果落地缓存 |

API 侧也设置了基础保护，例如批量行情一次最多接受 20 个证券。真正的访问频率仍应由调用方和任务调度器根据来源条款进一步控制。

项目不提供上游数据的授权，不鼓励绕过访问控制，也不建议重新分发抓取到的原始数据。详细规则见[数据源与再分发政策](docs/data-source-policy.md)。

## 开发与测试

```bash
uv sync --locked --group dev
uv run python -m unittest discover -s tests -v
uv run ruff check src tests
uv pip check
```

新增 provider 时必须保留来源、观测时间、入库时间和原始响应定位信息；财务类数据还必须保存公告时间。

架构与决策文档：

- [初始架构](docs/architecture/initial-architecture.md)
- [ADR-001：本地优先、分层存储与统一 API](docs/decisions/ADR-001-local-first-data-platform.md)
- [ADR-002：拆分事务型数据与大规模行情时序存储](docs/decisions/ADR-002-split-transactional-and-market-time-series-storage.md)
- [实施路线](docs/roadmap.md)
- [日历 API 契约](docs/calendar-api.md)

## 安全

不要把凭证写入仓库。凭证只能放在环境变量或被忽略的本地配置中。本服务只支持 loopback 和本地可信环境，管理接口目前没有身份认证。

## 许可证

本项目代码和仓库文档采用 [Apache License 2.0](LICENSE)。第三方数据、网站内容和依赖仍分别受其自身条款与许可证约束。
