# Codex 项目级安装 MarketCow MCP

本文是 MarketCow 面向 Codex 新用户的**唯一权威 project-only 安装指南**。安装器只在
一个明确的 Git workspace 根目录内管理 `.codex/config.toml` 和该目录下的本地备份；
不会调用 `codex mcp add`，不会写入 `~/.codex`、`$CODEX_HOME`、用户/系统配置，也不会
安装全局 Skill。

Codex 只有在项目被信任时才加载项目内的 `.codex/config.toml`。官方配置说明见
[Project config files](https://learn.chatgpt.com/docs/config-file/config-advanced#project-config-files-codexconfigtoml)
和 [Model Context Protocol](https://learn.chatgpt.com/docs/extend/mcp#connect-codex-to-an-mcp-server)。

## 1. 选择本地 MarketCow 入口

需要 Python 3.11+、[uv](https://docs.astral.sh/uv/) 和本机可用的 PostgreSQL、
ClickHouse。推荐从可信的本地源码 checkout 使用固定绝对路径：

```bash
export MARKETCOW_ROOT=/absolute/path/to/marketcow
uv sync --project "$MARKETCOW_ROOT"
"$MARKETCOW_ROOT/.venv/bin/marketcow" --help
"$MARKETCOW_ROOT/.venv/bin/marketcow-mcp-project" --help
```

也可以使用已审核的、版本固定且位于本机的 wheel。不要使用仓库里历史遗留的 `dist/`
文件；先核对 wheel 文件名、版本和 SHA-256，再安装到专用本地虚拟环境：

```bash
export MARKETCOW_WHEEL=/absolute/path/to/marketcow-0.2.0-py3-none-any.whl
export MARKETCOW_RUNTIME=/absolute/path/to/local/marketcow-runtime
uv venv --python 3.11 "$MARKETCOW_RUNTIME"
uv pip install --python "$MARKETCOW_RUNTIME/bin/python" "$MARKETCOW_WHEEL"
"$MARKETCOW_RUNTIME/bin/marketcow-mcp-project" --help
```

后文用 `MARKETCOW_BIN` 指向版本化的绝对入口：

```bash
export MARKETCOW_BIN="$MARKETCOW_ROOT/.venv/bin"
# wheel 模式改为：export MARKETCOW_BIN="$MARKETCOW_RUNTIME/bin"
```

不要使用依赖当前目录的 `uv run marketcow-mcp` 作为 Codex stdio 配置。

## 2. 配置并启动 MarketCow 服务

MarketCow 的所有 profile 都要求 PostgreSQL、ClickHouse 和 allowed root。源码模式先复制
对应模板，再只在本机填写：

```bash
cp "$MARKETCOW_ROOT/.env.development.example" "$MARKETCOW_ROOT/.env.development"
# 或 production：.env.production.example -> .env.production
```

至少检查：

- `MARKETCOW_ALLOWED_ROOT`
- `MARKETCOW_POSTGRES_DSN` 或 `MARKETCOW_POSTGRES_DSN_REF`
- `MARKETCOW_CLICKHOUSE_HOST`、`MARKETCOW_CLICKHOUSE_DATABASE`
- `MARKETCOW_CLICKHOUSE_PASSWORD` 或 `MARKETCOW_CLICKHOUSE_PASSWORD_REF`
- 业务需要的 provider 凭据（例如 Tushare、LongPort）

先启动 PostgreSQL/ClickHouse，再执行 doctor。开发实例使用 8792：

```bash
cd "$MARKETCOW_ROOT"
"$MARKETCOW_BIN/marketcow" --profile development doctor
"$MARKETCOW_BIN/marketcow" --profile development start \
  --host 127.0.0.1 --port 8792
```

本机 production 的约定端口是 8790：

```bash
cd "$MARKETCOW_ROOT"
"$MARKETCOW_BIN/marketcow" --profile production doctor
"$MARKETCOW_BIN/marketcow" --profile production start \
  --host 127.0.0.1 --port 8790
```

两者不要混用。下文选择与你实际启动实例一致的 URL：

```bash
export MARKETCOW_SERVICE_URL=http://127.0.0.1:8792/mcp  # development
# export MARKETCOW_SERVICE_URL=http://127.0.0.1:8790/mcp  # production
curl --fail --silent "${MARKETCOW_SERVICE_URL%/mcp}/v1/health"
```

健康响应必须为 JSON 且 `status` 是 `ok` 或 `healthy`。安装器还会校验 MCP initialize、
服务版本、tools/list 最小契约以及 `service_health`。

## 3. 准备目标 Codex workspace

目标必须是现有、可写的**绝对 Git workspace 根目录**：

```bash
export TARGET_WORKSPACE=/absolute/path/to/target-workspace
git -C "$TARGET_WORKSPACE" rev-parse --show-toplevel
```

输出必须与 `TARGET_WORKSPACE` 解析后的路径完全相同。安装器拒绝：

- 相对路径、Git 子目录、不存在或不可写目录；
- `/`、HOME、`CODEX_HOME`，以及包含/位于 `CODEX_HOME` 的目录；
- symlink `.codex`、路径越界的备份或模糊目标。

先审阅目标项目，再在 Codex 的信任提示中把它标记为 trusted；不信任时 Codex会忽略
项目 `.codex/` 配置。信任由 Codex/用户管理，安装器不会替你修改用户级 trust 配置。

## 4. 安装 Streamable HTTP（推荐）

先 dry-run：

```bash
"$MARKETCOW_BIN/marketcow-mcp-project" install \
  --workspace "$TARGET_WORKSPACE" \
  --service-url "$MARKETCOW_SERVICE_URL" \
  --expected-version 0.2.0 \
  --dry-run
```

确认输出后执行安装：

```bash
"$MARKETCOW_BIN/marketcow-mcp-project" install \
  --workspace "$TARGET_WORKSPACE" \
  --service-url "$MARKETCOW_SERVICE_URL" \
  --expected-version 0.2.0
```

安装器默认写入以下受管配置（实际 URL 和观察到的版本按参数生成）：

```toml
# BEGIN MARKETCOW MCP MANAGED v1
# installer_version = 1.0.0
# observed_marketcow_version = 0.2.0
# transport = http
[mcp_servers.marketcow]
url = "http://127.0.0.1:8792/mcp"
enabled = true
# END MARKETCOW MCP MANAGED v1
```

现有 TOML（包括其他 `[mcp_servers.*]`）保持不变。写入采用同目录临时文件、fsync 和
原子 replace。原配置备份位于：

```text
<workspace>/.codex/backups/marketcow-mcp/config.<UTC>.<sha12>.toml
```

原先没有 config 时备份以 `.absent` 结尾。输出会给出确切 `backup` 和
`restore_command`。已有**未受管** `[mcp_servers.marketcow]` 时安装器 fail closed；先
人工审阅、改名或删除冲突表，不能让安装器覆盖它。

安装器不会把工具数量永久写死为 13。它检查版本化最小只读工具集合，报告实际
`tool_count`、`missing_tools` 和 `extra_tools`；缺少最小契约会在写入前失败，额外工具
只报告不阻塞。

## 5. 新会话验证

安装成功后关闭旧会话，从 `TARGET_WORKSPACE` 根目录开启新的 Codex 会话并确认项目已
trusted。使用 `/mcp` 查看 `marketcow`，然后调用 `service_health`。

也可随时独立验证服务，不改配置：

```bash
"$MARKETCOW_BIN/marketcow-mcp-project" verify \
  --workspace "$TARGET_WORKSPACE" \
  --service-url "$MARKETCOW_SERVICE_URL" \
  --expected-version 0.2.0
```

成功输出包含：MarketCow 版本、完整工具名、总数、额外/缺失工具，以及
`service_health.structuredContent`。配置文件存在但工具不可见时，优先检查项目 trust
和是否已经新开会话。

## 6. stdio 兼容模式

只有客户端不能使用 Streamable HTTP 时才选择 stdio。命令必须是绝对、存在且可执行的
版本化入口；MarketCow 源码目录不能作为隐含 cwd：

```bash
"$MARKETCOW_BIN/marketcow-mcp-project" install \
  --workspace "$TARGET_WORKSPACE" \
  --transport stdio \
  --stdio-command "$MARKETCOW_BIN/marketcow-mcp" \
  --service-url "$MARKETCOW_SERVICE_URL" \
  --expected-version 0.2.0
```

生成的 `command` 是绝对路径，`MARKETCOW_MCP_BASE_URL` 明确指向 API base。安装器会从
目标 workspace（不是 MarketCow 源码 cwd）实际启动 stdio 入口，执行 initialize、
tools/list 和 `service_health` 后才写配置。

## 7. 幂等重装与升级

相同参数再次 `install` 为幂等操作：验证照常执行，但 `changed=false`，不创建无意义
备份。改变 URL、transport 或版本化入口时使用 `upgrade`：

```bash
"$MARKETCOW_BIN/marketcow-mcp-project" upgrade \
  --workspace "$TARGET_WORKSPACE" \
  --service-url "$MARKETCOW_SERVICE_URL" \
  --expected-version 0.2.0 \
  --dry-run

"$MARKETCOW_BIN/marketcow-mcp-project" upgrade \
  --workspace "$TARGET_WORKSPACE" \
  --service-url "$MARKETCOW_SERVICE_URL" \
  --expected-version 0.2.0
```

`upgrade` 要求已有 MarketCow 受管块，且同样执行写前/写后验证、备份和原子更新。

## 8. 卸载与恢复

dry-run 后卸载：

```bash
"$MARKETCOW_BIN/marketcow-mcp-project" uninstall \
  --workspace "$TARGET_WORKSPACE" --dry-run

"$MARKETCOW_BIN/marketcow-mcp-project" uninstall \
  --workspace "$TARGET_WORKSPACE"
```

卸载只删除两个 MarketCow marker 之间的受管内容，保留 Investrace 等其他配置，并在
删除前备份。恢复时只能使用该 workspace 安装器报告的绝对备份路径：

```bash
"$MARKETCOW_BIN/marketcow-mcp-project" restore \
  --workspace "$TARGET_WORKSPACE" \
  --backup /absolute/path/reported/by/installer.toml
```

恢复本身也先创建 rollback backup。`.absent` 备份会恢复为“没有 config.toml”。

## 9. 故障排查

- `--workspace must be the exact Git root`：使用 `git ... rev-parse --show-toplevel`
  的绝对输出，不要传子目录。
- `unmanaged [mcp_servers.marketcow] already exists`：安装器不会接管未知配置；人工审阅
  冲突表后再重试。
- `/v1/health is not healthy`：先检查 PostgreSQL、ClickHouse、`.env.<profile>` 和
  MarketCow profile/端口。
- `cannot read JSON`：确认 URL 使用 `http(s)://host:port/mcp`，本机网络命名空间能访问
  服务，且没有把 `/v1/health` 当成 MCP URL。
- `version mismatch`：核对正在运行的服务和本地 CLI/wheel，不要绕过版本检查进行升级。
- `missing required tools`：运行实例与安装包契约不一致；升级或回退服务后重新验证。
- 配置成功但新会话没有工具：确认从正确 Git root 启动、项目已 trusted、配置路径是
  `<workspace>/.codex/config.toml`，并真正新开会话。
- stdio 失败：检查绝对 executable 仍存在、可执行，且其
  `MARKETCOW_MCP_BASE_URL` 指向可用 API。

安装器 `--help` 是命令参数的最终依据：

```bash
"$MARKETCOW_BIN/marketcow-mcp-project" --help
"$MARKETCOW_BIN/marketcow-mcp-project" install --help
```
