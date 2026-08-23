# LLMAY 公网只读接入

MarketCow 为 LLMAY 提供独立的 `/public/v1/...` 入口。该入口是资源服务器边界，
只负责验证 Investrace 通过 OAuth 2.0 client credentials 流程签发的访问令牌；客户端
凭据和令牌签发端点不属于 MarketCow，也不得配置或记录在本服务中。

## 使用者、流程和权限边界

- 目标调用方：LLMAY 后端服务，不支持浏览器用户或匿名调用。
- 数据来源：响应仍由对应的 MarketCow `/v1/...` 内部只读接口生成；行情或财务数据的
  来源、采集时间和口径沿用各接口现有契约，网关不生成或修订数据。
- 调用流程：LLMAY 从 Investrace OAuth 服务取得 M2M access token，同时取得由
  MarketCow 信任域签发、绑定同一 `client_id` 的二次 JWT，然后请求
  `/public/v1/...`。
- 权限边界：只接受 `GET` 和 `HEAD`。路径必须逐项命中
  `MARKETCOW_PUBLIC_READ_ALLOWLIST_JSON`；`/v1/admin/*` 和 `/v1/auth/*` 永远不能
  加入白名单。公网前缀不会暴露未列出的现有路由。
- 合规边界：这是技术访问控制，不构成监管、法律或投资建议。公网部署必须由网关终止
  TLS，并由相关安全与合规负责人复核密钥托管、日志留存周期和数据许可。

## 认证契约

每次请求必须同时包含：

```http
Authorization: Bearer <Investrace access token>
X-MarketCow-JWT: <MarketCow secondary JWT>
X-Request-ID: <8-128 characters using A-Z a-z 0-9 dot underscore hyphen>
```

两枚令牌均只接受 `HS256`，必须包含有效 `kid`，并使用彼此独立、至少 32 字节的密钥。
它们必须通过签名、`iss`、`aud`、`exp`、`iat`、可选 `nbf`、最大生存期、`sub`、
`client_id`、`jti` 和 `scope` 校验。`sub` 必须等于 `client_id`，两枚令牌的
`client_id` 也必须一致。默认时钟偏差为 30 秒，默认最大生存期为 3600 秒。

Investrace access token 默认 scope 为 `marketcow:read`；二次 JWT 默认 scope 为
`llmay:read`。issuer、audience 和 scope 均可显式配置，但生产环境不应使用示例值。

## 必需配置

生产环境默认关闭公网入口。以下是启用一个只读健康接口和一个参数化工具接口的配置示例：

```dotenv
MARKETCOW_PUBLIC_READ_ENABLED=true
MARKETCOW_PUBLIC_READ_ALLOWLIST_JSON=["/v1/health","/v1/instruments/{instrument_id}"]
MARKETCOW_PUBLIC_RATE_LIMIT_REQUESTS=60
MARKETCOW_PUBLIC_RATE_LIMIT_WINDOW_SECONDS=60
MARKETCOW_PUBLIC_AUDIT_PATH=/Volumes/T9/data/marketcow/production/audit/public-access.jsonl

MARKETCOW_INVESTRACE_JWT_ISSUER=https://replace-with-investrace-issuer.example
MARKETCOW_INVESTRACE_JWT_AUDIENCE=marketcow-public
MARKETCOW_INVESTRACE_JWT_SCOPE=marketcow:read
MARKETCOW_INVESTRACE_JWT_KEYS_JSON={"2026-08":"replace-with-at-least-32-random-bytes"}

MARKETCOW_PUBLIC_JWT_ISSUER=marketcow
MARKETCOW_PUBLIC_JWT_AUDIENCE=llmay
MARKETCOW_PUBLIC_JWT_SCOPE=llmay:read
MARKETCOW_PUBLIC_JWT_KEYS_JSON={"2026-08":"replace-with-a-different-32-byte-secret"}
MARKETCOW_PUBLIC_JWT_LEEWAY_SECONDS=30
MARKETCOW_PUBLIC_JWT_MAX_LIFETIME_SECONDS=3600
```

密钥轮换时先在 JSON 对象中同时保留新旧 `kid`，待旧令牌的最大生存期结束后再移除旧
密钥。配置缺失、密钥过短、白名单为空、审计文件位于运行目录外或参数越界时，启用态
预检会失败。

## 响应、限流和审计

- 缺失、无效或过期令牌返回 `401`；令牌调用方不一致或路径未授权返回 `403`；写方法
  返回 `405`。
- 限流按已验证的 `client_id`、每个进程的固定时间窗计算。超过阈值返回 `429`、
  `detail.code=rate_limit_exceeded`，并包含 `Retry-After`、`X-RateLimit-Limit`、
  `X-RateLimit-Remaining` 和 `X-RateLimit-Reset`。多进程部署应在入口代理再配置一层共享
  限流，避免各进程额度相加。
- 每个允许或拒绝的请求同步追加到权限为 `0600` 的 JSONL 审计文件，记录调用方、规范化
  内部路径、UTC 时间、方法、HTTP 状态、结果和追踪 ID。查询参数、Authorization、
  Cookie、二次 JWT 和其他请求头均不会写入审计。

## 验证

专项测试：

```shell
uv run python -m unittest -v tests.test_public_access tests.test_config
```

全量测试：

```shell
uv run python -m unittest discover -v
```

静态检查：

```shell
uv run ruff check src tests
```
