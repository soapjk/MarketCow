# MarketCow 统一生产启动

MarketCow 正式环境只有一个受支持的服务所有者和一个调用方入口：

- LaunchAgent：`com.marketcow.production`
- 安装/更新命令：`ops/launchd/install.sh`
- 统一 API：`http://127.0.0.1:8790`

安装命令会编译并固定本地 Rust 二进制、生成最终架构配置、准备 PostgreSQL 与
ClickHouse，并由同一个 supervisor 同时管理：

1. Polymarket 完整目录与有界实时 discovery collector；
2. 权威 Polymarket Rust 数据面；
3. MarketCow 统一 API 网关；
4. Tradude 机会范围控制器；
5. MarketCow 动态 universe 校验与原子激活器。

股票与 Hyperliquid 实时订阅由统一 API 进程内的 realtime hub 管理，不是独立服务。
Discovery 使用内部端口 `8795`，Rust 数据面使用内部端口 `8796`；这些端口
不是调用方接口。旧端口 `8794`、`8791`、`18872` 不属于正式架构。HTTP、WebSocket、
管理后台 API 和 MCP 的调用方入口均为 `8790`。

Polymarket live 的 scope、快照、事件、checkpoint、full-sync、健康状态和 WebSocket
都由 8790 网关转发给 Rust。网关在 Rust 不可用时返回 503，绝不回落到旧 Python
scope/index。范围不是固定 100 个市场：Tradude 按机会选择最多 100 个市场，至少一个
完整且可交易的候选才允许 MarketCow 构建并激活新一代。

Gamma 的完整 `closed=false` 目录只作为元数据保存。生产配置默认从中选择最多 1000
个明确启用 CLOB、已部署并按近期 CLOB 成交量和流动性排序的市场进行盘口 bootstrap
与订阅；当前 Rust Scope 中仍合格的市场优先保留。该边界由
`MARKETCOW_POLYMARKET_DISCOVERY_REALTIME_MARKET_LIMIT` 控制，不能设置为全目录规模。

完整 Gamma 目录刷新不属于服务启动。首次准备或人工刷新必须单独运行：

```bash
PYTHONPATH=src .venv/bin/python scripts/prepare_polymarket_discovery.py \
  --root "$MARKETCOW_HOME/prediction-markets/polymarket-discovery" \
  --realtime-market-limit "$MARKETCOW_POLYMARKET_DISCOVERY_REALTIME_MARKET_LIMIT" \
  --required-realtime-scope "$MARKETCOW_POLYMARKET_RUST_SCOPE_FILE" \
  --fee-semantics-policy "$MARKETCOW_POLYMARKET_FEE_SEMANTICS_POLICY"
```

该工具负责遍历 Gamma、验证候选市场的全部 CLOB token books，并原子发布 catalog、
catalog index 与有界 realtime universe。常驻 collector 启动时只读取并校验这套已发布
边界；边界缺失或损坏时 discovery collector 明确退出，不会联网补建。supervisor 将
其记录为独立模块失败，同时继续启动 Rust 数据面和统一 API；Discovery 状态接口保持
可访问并返回 fail-closed 状态。

`ops/launchd/install.sh` 会停用并把旧的 `com.marketcow.*` 独立 LaunchAgent 移至
`~/Library/Application Support/MarketCow/retired-launch-agents/`。这样旧的 scoped、
read-api、soak 或 refresh job 不会在登录或重启后与正式服务并行启动。
首次安装会从仓库的 `.env.production` 初始化运行配置；更新安装会保留 Application
Support 中现有的 `production.env`，不会用模板或仓库副本覆盖业务凭证。安装器只会
幂等更新最终架构所需的本地路径、端口和 Rust 管理令牌（已有令牌会保留）。每次成功
激活也会原子更新下次重启使用的 scope 文件，重启不会退回旧 generation。

开发和迁移脚本仍可用于隔离测试，但不得注册为正式常驻服务。正式服务也不得通过
`marketcow start`、`run_polymarket_live.py` 或 `run_polymarket_live_read_api.py` 分别启动。

本地检查：

```bash
launchctl list | grep com.marketcow
lsof -nP -iTCP -sTCP:LISTEN | grep -E ':(8790|8795|8796) '
curl http://127.0.0.1:8790/v1/health
curl http://127.0.0.1:8790/v1/prediction-markets/polymarket/live/health
```

预期只有 `com.marketcow.production` 处于运行状态；8795、8796 只绑定 loopback，业务
调用入口只有 `8790`。
