# CSV 历史行情导入发布门禁

- [ ] CSV 契约与 Profile 版本固定，未知字段和缺列测试通过。
- [ ] 所有供应商 symbol 均有显式 namespace 映射；US MIC 不推断。
- [ ] dry-run 对坏时间、NaN/Inf、OHLC、负 volume、重复和乱序有稳定报告。
- [ ] 大文件流式测试证明内存有界，错误样本数量有上限。
- [ ] 原始 CSV 哈希归档和 Manifest 去重测试通过。
- [ ] job/shard 幂等创建、租约 fencing、心跳、有限重试和取消测试通过。
- [ ] 服务崩溃后的过期 running shard 能接管。
- [ ] 相同 ingestion ID 重放不产生重复 raw bars。
- [ ] raw receipt 的行数和 artifact 与 Manifest 一致。
- [ ] 每个导入 raw key 均存在 canonical key，质量失败不得进入 succeeded。
- [ ] CLI dry-run/正式导入与管理 API 使用同一服务层。
- [ ] 管理页面可 dry-run、启动、查看实时状态和取消。
- [ ] 路径穿越及 allowed-root 外文件被拒绝。
- [ ] 全量单元测试与 Ruff 通过。
- [ ] 使用获授权的真实供应商样本完成生产前 smoke test。

最后一项需要操作者提供获授权的数据文件；测试仓库不得提交购买数据。
