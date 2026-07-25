# 历史行情后台任务

历史行情批次统一覆盖 Yahoo 日线/分钟线与 Tushare 分钟线。请求没有业务默认值；
调用方必须显式选择代码、provider、range、interval、adjustment、fallback、并发、
重试、canonical 等待时间和幂等键。

```bash
curl -X POST http://127.0.0.1:8790/v1/admin/history-jobs \
  -H 'content-type: application/json' -d '{
    "symbols":["AAPL.XNAS","MSFT.XNAS"],
    "provider":"yahoo",
    "range":"1mo",
    "interval":"1d",
    "adjustment":"raw",
    "allow_fallback":false,
    "max_concurrency":2,
    "max_attempts":3,
    "retry_backoff_seconds":0.5,
    "retry_max_backoff_seconds":30,
    "retry_jitter_seconds":0.5,
    "retry_budget_seconds":120,
    "canonical_wait_seconds":5,
    "idempotency_key":"portfolio-us-20260724-v1"
  }'
```

创建返回 HTTP 202、`job_id`、`created` 和当前 job。相同幂等键重复提交返回同一个
job，不会并发重复执行；该保证由 PostgreSQL 唯一键和事务内 get-or-create 提供，
不依赖最近任务列表。`symbols` 会去空白并转大写，规范化后重复的代码会返回 422，
不会静默合并。`provider=yahoo_chart` 会在入口明确规范化为实际路由名 `yahoo`；
其他受支持值为 `yahoo` 和 `tushare`。状态 API：

```text
GET  /v1/admin/history-jobs
GET  /v1/admin/history-jobs/{job_id}
POST /v1/admin/history-jobs/{job_id}/cancel
POST /v1/admin/history-jobs/{job_id}/retry-failed
```

管理页面：`http://127.0.0.1:8790/v1/admin/history-jobs-ui`，每两秒轮询 JSON API。
页面只显示批次、代码、provider/source、行数、canonical 状态和经过脱敏的错误。

## A 股批量拉取

A 股分钟历史行情使用 `provider=tushare`，服务通过生产配置中的
`TUSHARE_TOKEN` 访问上游。使用前可以在项目目录执行以下命令，只检查 Token 是否
已加载，不会输出 Token：

```bash
PYTHONPATH=src .venv/bin/python -c \
  'from marketcow.config import Settings; print(bool(Settings.from_env("production").tushare_token.strip()))'
```

输出 `True` 表示应用配置能够读取 Token。

A 股任务使用 provider-neutral `SYMBOL.MIC`：

- 上交所：`600519.XSHG`
- 深交所：`000001.XSHE`
- 北交所：`920001.XBSE`

`.SH`、`.SZ`、`.BJ` 是 Tushare/券商外部代码，只存在
`provider:tushare`、`provider:longport` 等显式 namespace 映射中，不能作为
MarketCow 内部 Instrument ID。

例如，一次拉取贵州茅台、平安银行和宁德时代最近三个月的 15 分钟原始行情：

```bash
curl -X POST http://127.0.0.1:8790/v1/admin/history-jobs \
  -H 'content-type: application/json' -d '{
    "symbols":["600519.XSHG","000001.XSHE","300750.XSHE"],
    "provider":"tushare",
    "range":"3mo",
    "interval":"15m",
    "adjustment":"raw",
    "allow_fallback":false,
    "max_concurrency":3,
    "max_attempts":5,
    "retry_backoff_seconds":1,
    "retry_max_backoff_seconds":60,
    "retry_jitter_seconds":1,
    "retry_budget_seconds":1800,
    "canonical_wait_seconds":10,
    "idempotency_key":"a-share-15m-3mo-20260725-v1"
  }'
```

每个请求最多包含 100 个不重复的代码。Tushare 历史适配器当前支持：

- `interval`：`1m`、`5m`、`15m`、`30m`、`60m`、`1h`
- `range`：`1d`、`5d`、`1mo`、`3mo`、`6mo`、`1y`、`2y`、`5y`、
  `10y`、`ytd`、`max`
- `adjustment`：仅 `raw`

当前 Tushare 任务链路不支持 A 股日线，也不支持前复权或后复权。不要为 A 股
Tushare 请求启用 fallback；Yahoo adapter 不接受 MarketCow 的规范 A 股代码。

创建接口返回 HTTP 202。保存响应中的 `job_id`，然后查看该任务：

```bash
curl http://127.0.0.1:8790/v1/admin/history-jobs/JOB_ID
```

查看最近任务：

```bash
curl 'http://127.0.0.1:8790/v1/admin/history-jobs?limit=50'
```

`idempotency_key` 表示一次逻辑任务。因网络超时而不确定创建结果时，应使用完全相同的
请求和幂等键再次提交；服务会返回原任务。若确实需要重新拉取一批数据，应换用新的、
可追踪的幂等键。

若任务出现 `partially_failed` 或 `failed`，在上游或配置恢复后只重试失败分片：

```bash
curl -X POST \
  http://127.0.0.1:8790/v1/admin/history-jobs/JOB_ID/retry-failed
```

取消任务：

```bash
curl -X POST \
  http://127.0.0.1:8790/v1/admin/history-jobs/JOB_ID/cancel
```

服务崩溃、重启或暂时断链时不需要创建新任务。任务、租约和分片 checkpoint 已持久化；
服务恢复后会自动接管未完成任务，已成功分片不会重新拉取。

如果正在运行的服务尚未加载该接口（请求返回 HTTP 404），重启本地 launchd 服务：

```bash
launchctl kickstart -k gui/$(id -u)/com.marketcow.production
```

重启后先读取任务列表确认接口可用，再提交生产数据任务。

批次状态为 `queued`、`running`、`succeeded`、`partially_failed`、`failed`、
`cancel_requested`、`canceled`。逐标的状态为 `queued`、`running`、
`succeeded`、`failed`、`canceled`；canonical 状态独立为 `pending`、
`completed`、`failed`。服务重启时会查询全部非终态任务，不受管理列表分页限制。
尚未开始的 queued item 会恢复调度；被中断的 running item 若仍有尝试额度，会记录
`service_interrupted` 后自动重新排队，额度耗尽时才转为 failed，不会伪装成功。

每个运行项由 PostgreSQL 原子认领并携带 `owner_id`、随机 `lease_token`、
`lease_expires_at` 和 `heartbeat_at`。worker 在 provider 调用期间持续续租；完成写入
必须匹配当前 owner 和 token，旧 worker 即使在接管后返回，也不能覆盖新 owner 的
状态。默认租约为 30 秒，可通过 `MARKETCOW_HISTORY_JOB_LEASE_SECONDS` 在 1–300 秒
范围内调整。watchdog 持续扫描 queued 和租约过期的 running 项；未过期的外部 owner
不会被接管，过期项通过同一个原子 claim 接管并增加 `takeover_count`。

新建任务使用 `history_job_schema_version=2`，在创建时冻结绝对范围并原子写入 shard
checkpoints。升级前已经持久化且没有 shard 记录的任务继续走旧版整段 range adapter；
系统不会在部署时猜测并补写分片边界。旧任务完成后，所有新任务都使用 v2 分片流程。

取消会立即取消 queued item。已经进入 provider/持久化原子步骤的 item 会完成该步骤，
随后转为 canceled，不再开始下一次重试。`retry-failed` 只重置 failed item。
指数退避为 `retry_backoff_seconds * 2^(attempt-1)`，并应用
`retry_max_backoff_seconds` 上限和 `[0,retry_jitter_seconds]` 抖动。HTTP
`Retry-After` 会覆盖较短的本地退避，但仍受最大退避约束；下一次 sleep 若会超过
`retry_budget_seconds`，该项立即以 `retry_budget_exhausted` 结束。

自动重试只用于可识别的瞬时故障：连接中断、超时、HTTP 429、HTTP 5xx 和明确的
上游限流。认证/授权、provider 不支持、无效 range/interval/adjustment、HTTP 4xx
请求拒绝和未知错误立即失败，并写入稳定 `error_code`；仍可由操作人员在修正配置后
显式执行 `retry-failed`。

服务关闭会先停止接受新调度，然后等待批次协调器和条目 worker 全部退出，再关闭
PostgreSQL/行情服务依赖；不会遗留 daemon thread 在关闭后访问连接池。

现有 `POST /v1/market-bars/query` 同步接口保持兼容；新管理流程应使用后台任务 API。
