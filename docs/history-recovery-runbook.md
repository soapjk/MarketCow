# 历史行情恢复运维手册

本手册只使用管理 API，不要求也不允许直接修改 PostgreSQL 或 ClickHouse。

## 1. 判断影响范围

1. 请求 `GET /v1/admin/history-health`。
2. 请求 `GET /v1/admin/history-jobs?limit=200`。
3. 对目标批次请求 `GET /v1/admin/history-jobs/{job_id}`。
4. 记录 job、item、shard、`owner_id`、`lease_expires_at`、`heartbeat_at`、
   `takeover_count`、`error_code` 和 canonical 状态。

`lease_token` 是 fencing 凭证，不会由 API 返回，也不应出现在工单或日志中。

## 2. 常见处置

### queued 积压

- 确认 PostgreSQL 与 ClickHouse 健康。
- 检查 `history_watchdog_unavailable`。
- 不要重复创建相同任务；原任务会由 watchdog 调度。
- 若上游限流，降低新批次 `max_concurrency`，不要取消正在正常退避的批次。

### running 心跳停止

- 未到 `lease_expires_at`：等待当前 owner 续租，不要人工重试。
- 已过期：watchdog 会原子接管；观察 `takeover_count` 是否增加。
- 达到 critical 阈值：停止提交新批次，先恢复 PostgreSQL/worker 稳定性。

### failed 或 partially_failed

1. 查看稳定 `error_code`。
2. 认证、参数或 provider 不支持：先修正配置或请求。
3. 网络、429、5xx、storage unavailable：确认依赖恢复。
4. 调用 `POST /v1/admin/history-jobs/{job_id}/retry-failed`。

该接口只重置 failed item/shard，已经成功的 shard 不会重拉。

### 数据可能已写入但任务失败

1. 先执行：

   ```http
   POST /v1/admin/history-jobs/{job_id}/reconcile
   {"dry_run": true}
   ```

2. 审查 `mark_succeeded`、`no_change`、`unverifiable`、`lease_conflict`。
3. 无活跃 lease 且结果符合预期时执行：

   ```http
   POST /v1/admin/history-jobs/{job_id}/reconcile
   {"dry_run": false}
   ```

4. 再次读取 job 详情确认 shard、item、job 是否收敛。

### artifact、bars 或 ingestion 不一致

请求 `GET /v1/admin/history-consistency`：

- `market_bars_missing`：保留 artifact，按建议 replay。
- `raw_artifact_missing`：从原始备份恢复或重新拉取对应 shard。
- `ingestion_missing`：重新拉取 shard。
- `orphan_artifact`、`orphan_market_bars`：先隔离并调查来源，不直接删除。

当前 API 只提供审计和安全状态修复；文件隔离、重放和删除必须走独立受控流程。

### canonical pending/failed

- pending 会由持久复核队列跨重启继续。
- failed 表示复核预算耗尽；先检查 canonical scheduler/ClickHouse，再处理。
- 不应为了清除 pending 直接把 item 改成 completed。

## 3. 取消

调用 `POST /v1/admin/history-jobs/{job_id}/cancel`。queued shard 立即取消；已进入
provider/持久化原子步骤的 shard 完成当前步骤后停止，不会启动下一片。

## 4. 服务重启

优先正常停止，使 worker 完成当前原子步骤并关闭心跳。若进程被强制终止：

- 未过期 lease 不会被其他实例抢占；
- 到期后 watchdog 原子接管；
- 旧 worker 即使晚到，也会被 lease token fencing 拒绝更新终态。

## 5. 恢复完成标准

- history health 为 `healthy`，或仅有已解释的非阻断 degraded reason；
- 无过期 running lease；
- job 达到明确终态；
- `completed_shards == total_shards`；
- consistency audit 无未解释 finding；
- canonical 为 completed，或 pending 有正在工作的复核队列；
- 不存在需要直接改库才能解释的状态。

## 6. Schema 升级与回滚

1. 升级前停止历史任务领取，等待现有 lease 释放，并备份 PostgreSQL schema 与
   ClickHouse 数据库元数据和数据。
2. 先升级 PostgreSQL，再升级 ClickHouse，最后启动新版本 worker。新版本会拒绝
   未知版本、迁移缺口或描述漂移，避免旧二进制误写较新的 schema。
3. PostgreSQL 单次迁移在 advisory lock 和事务内执行；重复启动会跳过已记录版本。
   ClickHouse DDL 不具备跨语句事务性，若中断，应保留现场，用同一版本重跑幂等
   迁移并核对 `schema_migrations`，不可手工补写版本记录。
4. 代码回滚只允许回到仍认识当前 schema 的版本。版本 15–18 是增量字段和表，但旧
   worker 不理解 shard lease 与 canonical queue，因此回滚代码前必须停止历史任务。
   需要回退 schema 时，从升级前备份恢复到新数据库并切换连接，不执行原地破坏性
   `DROP`。
5. 恢复领取前检查：迁移版本完整、旧任务数量不变、running lease 可续约或接管、
   ClickHouse ingestion receipt 可查询，并完成一个单 symbol smoke job。
