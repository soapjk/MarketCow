# 在 Agent 项目中安装 MarketCow MCP

本文说明如何让 Agent 在**指定项目范围内**接入 MarketCow MCP，而不修改用户级或
系统级 MCP 配置。

MarketCow API 已内置 Streamable HTTP MCP endpoint。对于支持 HTTP MCP 的客户端，
“安装”只需要在目标项目中添加一段客户端配置，不需要全局安装 MarketCow Python
包或额外启动 MCP 进程。

## 前置条件

先在 MarketCow 仓库中启动服务：

```bash
uv run marketcow --profile development start --host 127.0.0.1 --port 8792
```

确认 API 健康检查可访问：

```bash
curl http://127.0.0.1:8792/v1/health
```

MCP endpoint 为：

```text
http://127.0.0.1:8792/mcp
```

如果 MarketCow 使用了其他主机或端口，后续配置必须使用实际地址。

## 项目级客户端配置

不同 Agent 和 MCP 客户端使用不同的项目配置文件名与格式。请先查阅目标客户端的
项目级 MCP 配置约定，再把下面的 server 合并到该项目已有的配置中：

```json
{
  "mcpServers": {
    "marketcow": {
      "url": "http://127.0.0.1:8792/mcp",
      "transport": "http"
    }
  }
}
```

这段 JSON 表示通用配置结构，不代表所有客户端都使用相同的文件名或字段。安装时
应遵守以下边界：

- 只修改目标项目内的配置，不修改用户主目录或系统级配置；
- 保留配置中已有的其他 MCP Server，不要覆盖整个文件；
- 不要为 HTTP 模式全局安装 MarketCow、Python 包或其他依赖；
- 不要把 bearer token 或其他密钥写入会提交 Git 的配置文件；
- 需要认证时，使用环境变量或目标客户端提供的安全凭据机制；
- 如果客户端不支持项目级 HTTP MCP，停止并说明限制，不要自行退回全局安装。

是否把项目级 MCP 配置提交到 Git，应由项目维护者决定。提交前应确认配置中没有
密钥，并确保其中的服务地址对项目使用者适用。

## 交给目标 Agent 的安装指令

可以把下面的内容直接发送给需要接入 MarketCow 的 Agent：

```text
请为当前项目接入 MarketCow MCP，要求如下：

1. 只能使用项目级配置，不得修改用户级或系统级 MCP 配置。
2. 先识别当前 Agent/MCP 客户端支持的项目级配置文件及格式。
3. 检查并保留项目内已有的 MCP 配置，不得覆盖其他 MCP Server。
4. 添加名为 marketcow 的 Streamable HTTP MCP Server：
   URL: http://127.0.0.1:8792/mcp
5. 如果 MarketCow 实际运行在其他主机或端口，请使用真实 URL。
6. 不要全局安装 MarketCow、Python 包或其他依赖。
7. 不要把 bearer token 或其他密钥写入会提交 Git 的配置文件；需要认证时使用
   环境变量或客户端的安全凭据机制。
8. 配置完成后重新加载 MCP 配置，确认能够发现 MarketCow 工具，并调用
   service_health 验证连接。
9. 报告修改的文件、配置作用域和验证结果。
10. 如果当前客户端不支持项目级 HTTP MCP，请停止并说明限制，不要退回全局安装。
```

## 验证

重新加载目标客户端的 MCP 配置后：

1. 确认 server 列表中出现 `marketcow`；
2. 确认能够发现 `service_health`、`search_instruments` 等 MarketCow 工具；
3. 调用 `service_health` 并确认返回正常；
4. 检查修改的配置文件确实位于目标项目中；
5. 检查用户级和系统级 MCP 配置没有被修改。

如果客户端能看到 server 但无法连接，依次检查：

- `http://127.0.0.1:8792/v1/health` 是否可访问；
- MCP URL 是否为 `http://127.0.0.1:8792/mcp`，而不是健康检查地址；
- Agent 是否和 MarketCow 运行在同一网络环境中；
- 客户端是否支持 Streamable HTTP transport；
- 启用了认证时，凭据是否通过安全方式正确注入。

## 仅支持 stdio 的客户端

对于只支持子进程 transport 的客户端，MarketCow 仍提供 `marketcow-mcp` stdio
入口：

```bash
MARKETCOW_MCP_BASE_URL=http://127.0.0.1:8792 uv run marketcow-mcp
```

stdio 配置同样必须放在目标项目范围内。命令需要在能访问 MarketCow 源码及其本地
Python 环境的位置运行；不要因此执行全局包安装。具体服务协议、环境变量和工具清单
见 [MCP Server 文档](mcp-server.md)。
