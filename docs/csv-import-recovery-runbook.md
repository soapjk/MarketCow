# CSV 历史行情导入恢复 Runbook

## 任务状态

`queued → running → succeeded|failed|canceled`。取消请求先进入
`cancel_requested`；未认领分片立即取消，运行分片每 1000 行检查一次取消状态。

分片状态为 `queued/running/retry/succeeded/failed/canceled`。运行分片持有
`owner_id + lease_token + lease_expires_at`，只有 token 匹配的 worker 能提交结果。

## 服务崩溃

重启服务即可。启动时 coordinator 查询 `queued/running/cancel_requested` 任务；
租约到期的 running 分片可被新 worker 接管。相同 Manifest/shard/Instrument
生成相同 ingestion ID，因此“bars 已写但 checkpoint 未写”会安全重放。

检查：

1. `GET /v1/admin/csv-imports/{job_id}` 查看 shard lease、attempt 和错误。
2. 确认旧 lease 已过期；不要手工删除 raw bars。
3. 检查 ingestion receipt 与 `raw_artifact_id` 是否一致。
4. 等待重放和 canonical 质量门禁完成。

## ClickHouse 或 WAL 暂不可用

写入未同时满足 acknowledged/verified 时分片不会成功。系统在 `max_attempts`
范围内重试；耗尽后任务失败。恢复存储后调用
`POST /v1/admin/csv-imports/{job_id}/retry` 并提供新的请求幂等键。重试任务复用
原 Manifest、归档文件和稳定 ingestion ID；不要复制 CSV 或更换映射来伪造新任务。

## canonical 质量失败

任务错误码为 `csv_import_quality_failed`，`quality_report_json.failures` 给出：

- `raw_receipt_missing`
- `raw_row_count_mismatch`
- `raw_artifact_mismatch`
- `raw_coverage_mismatch`
- `canonical_coverage_incomplete`
- `manifest_row_count_mismatch`
- `first_bar_at_mismatch` / `last_bar_at_mismatch`

先修复 canonical scheduler/构建器，再使用原始 Manifest 和稳定 ingestion ID
重新执行受影响范围。不得直接把 job 状态改为 succeeded。

## 原始文件

归档路径为 `storage/csv-imports/<hash-prefix>/<manifest-id>.csv`。文件名由 Manifest
内容哈希决定，写入采用临时文件、fsync、哈希复核和原子 rename。不要修改归档
文件；若哈希冲突，隔离文件并重新从供应商原件导入。

默认保留策略禁止自动清理。需要清理时，先确认所有引用该 Manifest 的任务已进入
终态，导出 Manifest 与质量报告，记录审批凭据，再删除归档；数据库中的审计记录和
文件哈希必须继续保留。API 响应不会泄露上述服务器路径。
