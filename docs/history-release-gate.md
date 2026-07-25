# 历史数据可靠恢复发布门禁

本清单用于判断代码是否具备进入受控发布流程的条件，不代表已经推送、部署或操作生产
数据。任何远程发布仍需独立授权。

## 阻断项

- [x] 非终态 job/item/shard 可跨重启恢复，过期 lease 可接管，旧 token 被 fencing。
- [x] 自动重试区分永久/瞬时错误，并支持 Retry-After、抖动和预算。
- [x] 请求范围冻结为 UTC 绝对时间，provider 分片有界、连续且确定。
- [x] 成功 shard 不重复拉取；取消、失败重试和进度汇总在 shard 级收敛。
- [x] ingestion identity 稳定；ClickHouse 相同 identity 重复写入实测只保留一行。
- [x] 写后对账、canonical 持久队列和跨存储 consistency audit 有 dry-run 安全边界。
- [x] 管理 API、健康检查、指标、结构化日志、告警原因和运维手册齐备。
- [x] PostgreSQL v14→v18 实测保留 queued job/item，重复迁移安全。
- [x] ClickHouse v5→v6 实测成功，重复迁移安全；回滚限制已记录。
- [x] 全量本地测试通过：324 tests，19 skipped；跳过项均为需显式外部凭证/环境的
  既有集成测试。
- [x] Python bytecode 编译和 `git diff --check` 通过。

## 容量与故障证据

- 20 批、200 symbols、全局 4 worker soak 通过，无本 manager 线程泄漏。
- 10 年 Yahoo 1m 范围生成 500–600 个连续有界 shard。
- 自动故障矩阵覆盖 worker 崩溃、lease 过期、多实例接管、provider 限流、持久化失败、
  写成功但状态更新失败、canonical pending 和跨存储缺失/孤儿。

## 发布前人工确认

- 备份 PostgreSQL 与 ClickHouse，并验证恢复目标位置和访问权限。
- 暂停新任务领取，等待活跃 lease 释放；记录升级前 queued/running 数量。
- 在目标环境执行 schema 预检和单 symbol smoke job。
- 确认 `/v1/admin/history-health` ready，consistency audit 无未解释 finding。
- 确认告警接收人、回滚负责人和维护窗口。

## 回滚判定

出现迁移版本异常、旧任务数量变化、lease 无法续约/接管、重复写入或 consistency finding
持续增长时停止发布。禁止原地删除新 schema；按
`docs/history-recovery-runbook.md` 从升级前备份恢复到隔离数据库并切换。
