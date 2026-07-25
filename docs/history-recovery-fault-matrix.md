# 历史行情恢复故障矩阵

| 故障点 | 自动行为 | 验收证据 |
|---|---|---|
| 上游断链、超时 | 按预算、抖动和退避重试 | `HistoryFaultMatrixTest` |
| HTTP 429、5xx | 遵守 Retry-After 后重试 | `HistoryFaultMatrixTest` |
| 认证、参数、其他 4xx | 不自动重试，稳定错误码失败 | `HistoryFaultMatrixTest` |
| ClickHouse 暂时不可用 | shard 保持可重试并重新执行 | `HistoryFaultMatrixTest` |
| worker/进程中断 | 租约到期后其他实例原子接管 | `HistoryJobManagerTest` |
| artifact 已写、bars 未写 | 审计为 `market_bars_missing` | `CrossStoreHistoryFaultInjectionTest` |
| bars 已写、checkpoint 未写 | 按 ingestion receipt 对账修复 | `CrossStoreHistoryFaultInjectionTest` |
| canonical 延迟 | 持久复核队列跨重启继续 | `HistoryCanonicalVerifierTest` |

故障测试使用 `tests/history_fault_harness.py` 的确定性 outcome 队列，不访问真实上游。
真实 PostgreSQL、ClickHouse 的进程级 kill、网络隔离和升级演练仍由 opt-in 集成测试执行。

## 健康与告警

`GET /v1/admin/history-health` 返回持久任务队列健康快照。默认阈值：

- queued item ≥ 100：`history_queue_backlog_high`
- 过期 running lease ≥ 1：`history_expired_leases_present`
- 过期 running lease ≥ 10：`history_expired_leases_critical`，服务不可用
- canonical pending ≥ 100：`history_canonical_backlog_high`
- 非终态任务累计 takeover ≥ 20：`history_takeover_rate_high`
- watchdog 线程意外退出：`history_watchdog_unavailable`，服务不可用

告警应以 `status` 和稳定 reason code 为条件；恢复到阈值以下后快照自动清除对应 reason。
