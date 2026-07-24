# 历史行情后台任务

历史行情批次统一覆盖 Yahoo 日线/分钟线与 Tushare 分钟线。请求没有业务默认值；
调用方必须显式选择代码、provider、range、interval、adjustment、fallback、并发、
重试、canonical 等待时间和幂等键。

```bash
curl -X POST http://127.0.0.1:8794/v1/admin/history-jobs \
  -H 'content-type: application/json' -d '{
    "symbols":["AAPL","MSFT"],
    "provider":"yahoo",
    "range":"1mo",
    "interval":"1d",
    "adjustment":"raw",
    "allow_fallback":false,
    "max_concurrency":2,
    "max_attempts":3,
    "retry_backoff_seconds":0.5,
    "canonical_wait_seconds":5,
    "idempotency_key":"portfolio-us-20260724-v1"
  }'
```

创建返回 HTTP 202、`job_id`、`created` 和当前 job。相同幂等键重复提交返回同一个
job，不会并发重复执行。状态 API：

```text
GET  /v1/admin/history-jobs
GET  /v1/admin/history-jobs/{job_id}
POST /v1/admin/history-jobs/{job_id}/cancel
POST /v1/admin/history-jobs/{job_id}/retry-failed
```

管理页面：`http://127.0.0.1:8794/v1/admin/history-jobs-ui`，每两秒轮询 JSON API。
页面只显示批次、代码、provider/source、行数、canonical 状态和经过脱敏的错误。

批次状态为 `queued`、`running`、`succeeded`、`partially_failed`、`failed`、
`cancel_requested`、`canceled`。逐标的状态为 `queued`、`running`、
`succeeded`、`failed`、`canceled`；canonical 状态独立为 `pending`、
`completed`、`failed`。服务重启时，尚未开始的 queued item 会恢复调度；
被中断的 running item 会明确转为 `failed/service_interrupted`，不会伪装成功。

取消会立即取消 queued item。已经进入 provider/持久化原子步骤的 item 会完成该步骤，
随后转为 canceled，不再开始下一次重试。`retry-failed` 只重置 failed item。
指数退避为 `retry_backoff_seconds * 2^(attempt-1)`。

现有 `POST /v1/market-bars/query` 同步接口保持兼容；新管理流程应使用后台任务 API。
