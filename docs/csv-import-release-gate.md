# CSV 历史行情导入发布门禁

- [x] CSV 契约与 Profile 版本固定，未知字段和缺列测试通过。
- [x] 所有供应商 symbol 均有显式 namespace 映射；US MIC 不推断。
- [x] dry-run 对坏时间、DST、NaN/Inf、OHLC、负 volume、重复、乱序、缺口和交易时段有稳定报告。
- [x] 大文件流式测试证明内存有界，错误样本数量有上限。
- [x] 原始 CSV 哈希归档和 Manifest 去重测试通过。
- [x] job/shard 幂等创建、租约 fencing、心跳、有限重试和取消测试通过。
- [x] 服务崩溃后的过期 running shard 能接管。
- [x] 相同 ingestion ID 重放不产生重复 raw bars。
- [x] raw receipt 的行数和 artifact 与 Manifest 一致。
- [x] 每个导入 raw key 均存在 canonical key，质量失败不得进入 succeeded。
- [x] CLI dry-run/正式导入与管理 API 使用同一服务层。
- [x] 管理页面可 dry-run、启动、查看实时状态、证据、取消和重试。
- [x] 路径穿越、allowed-root 外文件和超出大小限制的文件被拒绝。
- [x] 全量单元测试与 Ruff 通过。
- [x] 使用获授权的真实供应商样本完成生产前 smoke test。

真实样本门禁于 2026-07-25 完成。执行使用
`import-bars --evidence-output <new-file>`；脱敏摘要见
`artifacts/mcvi-real-sample-smoke-summary-20260725.md`。购买数据和完整 create-only
证据不提交到仓库。

百万行门禁证据（2026-07-25，本地合成 1 分钟数据）：

- 1,000,000 行全部有效，0 重复、0 乱序、0 缺口；
- 49,000,026 bytes；
- dry-run 6.523 秒；
- `time -l` maximum resident set size 27,688,960 bytes，peak memory footprint
  17,727,896 bytes。
