# MarketCow 统一生产启动

MarketCow 正式环境只有一个受支持的服务所有者和一个调用方入口：

- LaunchAgent：`com.marketcow.production`
- 安装/更新命令：`ops/launchd/install.sh`
- 统一 API：`http://127.0.0.1:8790`

安装命令会准备 PostgreSQL 与 ClickHouse，并由同一个 supervisor 同时管理：

1. Polymarket 全市场 discovery collector；
2. Polymarket 热 scope collector；
3. MarketCow 统一 API。

股票与 Hyperliquid 实时订阅由统一 API 进程内的 realtime hub 管理，不是独立服务。
Polymarket collector 使用 `8794`、`8795` loopback WebSocket 向统一 API 提供内部数据流；
这些端口不是调用方接口。HTTP、WebSocket、管理后台 API 和 MCP 均使用 `8790`。

`ops/launchd/install.sh` 会停用并把旧的 `com.marketcow.*` 独立 LaunchAgent 移至
`~/Library/Application Support/MarketCow/retired-launch-agents/`。这样旧的 scoped、
read-api、soak 或 refresh job 不会在登录或重启后与正式服务并行启动。
首次安装会从仓库的 `.env.production` 初始化运行配置；更新安装会保留 Application
Support 中现有的 `production.env`，不会用模板或仓库副本覆盖已验证配置与凭证。

开发和迁移脚本仍可用于隔离测试，但不得注册为正式常驻服务。正式服务也不得通过
`marketcow start`、`run_polymarket_live.py` 或 `run_polymarket_live_read_api.py` 分别启动。

本地检查：

```bash
launchctl list | grep com.marketcow
lsof -nP -iTCP:8790 -sTCP:LISTEN
curl http://127.0.0.1:8790/v1/health
```

预期只有 `com.marketcow.production` 处于运行状态，且业务入口只有 `8790`。
