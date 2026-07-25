# MarketCow administration security

Administration authentication is enabled by default for the production profile.
Configure one or more long local bootstrap tokens:

```dotenv
MARKETCOW_ADMIN_AUTH_REQUIRED=true
MARKETCOW_ADMIN_TOKENS_JSON={"long-viewer-token":"viewer","long-operator-token":"operator","long-admin-token":"admin"}
```

Keep this value in the uncommitted profile environment file. Tokens must contain at
least 16 characters. Production startup fails when authentication is required but
no token is configured.

## Session flow

1. `POST /v1/auth/session` accepts a bootstrap token once.
2. The server creates a random, bounded, eight-hour session.
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
