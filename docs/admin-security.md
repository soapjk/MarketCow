# MarketCow administration security

Administration authentication is enabled by default for the production profile.
Browser users authenticate with a username and password. Store only a scrypt hash:

```dotenv
MARKETCOW_ADMIN_AUTH_REQUIRED=true
MARKETCOW_ADMIN_USERS_JSON={"admin":{"role":"admin","password_hash":"scrypt$..."}}
MARKETCOW_ADMIN_SESSION_SECONDS=2592000
```

Generate a password hash locally:

```bash
uv run python -c 'from marketcow.admin_auth import hash_admin_password; import getpass; print(hash_admin_password(getpass.getpass()))'
```

Long bootstrap tokens remain available for API automation:

```dotenv
MARKETCOW_ADMIN_TOKENS_JSON={"long-viewer-token":"viewer","long-operator-token":"operator","long-admin-token":"admin"}

## 服务间调用

批量历史数据下载使用独立的 Service Account，不使用管理员用户名、密码、
浏览器 Cookie，也不要求调用方位于 MarketCow 项目目录。

生成一次性密钥和服务端声明：

```bash
marketcow service-account generate \
  --id history-worker \
  --role operator \
  --scopes history:read,history:write
```

命令输出的 `api_key` 只交给调用服务保存。将
`service_accounts_json` 对象配置为服务端的
`MARKETCOW_SERVICE_ACCOUNTS_JSON`。服务端仅保存 API Key 的 SHA-256
摘要。调用服务只需：

```bash
export MARKETCOW_API_URL=http://127.0.0.1:8790
export MARKETCOW_API_KEY='mcsa.history-worker.<secret>'

curl -fsS "$MARKETCOW_API_URL/v1/admin/history-jobs?limit=20" \
  -H "Authorization: Bearer $MARKETCOW_API_KEY"
```

`history:read` 可查询任务，`history:write` 可创建、取消和重试任务。
该密钥访问其他后台管理接口会返回 `403 insufficient_scope`。通过
`enabled: false` 可以立即停用某个调用方；轮换时生成新密钥并更新摘要。
```

Keep these values in the uncommitted profile environment file. Passwords must contain
8 to 128 characters; tokens must contain at least 16 characters. Production startup
fails when authentication is required but neither a user nor token is configured.

## Session flow

1. `POST /v1/auth/session` accepts username/password or a bootstrap token.
2. The server creates a random, bounded session, configurable up to 30 days.
3. The session ID is stored in an HttpOnly, SameSite=Strict cookie.
4. A separate SameSite=Strict CSRF cookie must match `X-CSRF-Token` on mutations.
5. `DELETE /v1/auth/session` removes the server session and both cookies.

Bearer bootstrap tokens are supported for local automation and are not subject to
cookie CSRF validation. Browser code does not persist the bootstrap token.

## Roles

| Role | Administration reads and SSE | Commands | Configuration |
| --- | --- | --- | --- |
| Viewer | yes | no | no |
| Operator | yes | yes | no |
| Admin | yes | yes | reserved |

FastAPI middleware enforces the role for every `/v1/admin/*` route. The UI also
hides Operator actions from Viewers, but UI visibility is not a security control.
Audit actors are derived from the authenticated identity and cannot be selected
with a request header.

## Browser boundary

- Administrative responses use `nosniff`, same-origin referrer policy, restrictive
  permissions policy, `X-Frame-Options: DENY`, and CSP.
- The explicit CSP frame source is the local Grafana instance at
  `http://127.0.0.1:3001`.
- The frontend uses same-origin `/v1` requests. No wildcard CORS policy is enabled.
- Grafana has its own login and uses separate read-only database credentials.
- Set `MARKETCOW_ADMIN_COOKIE_SECURE=true` if the local reverse proxy terminates
  HTTPS.
