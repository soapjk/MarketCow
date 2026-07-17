# 实施路线

## 2026-07-17 加固进展

已完成：

- DuckDB schema 迁移记录和向后兼容迁移；
- 基本面、通达信财务的追加式版本历史；
- 基本面、完整三表、通达信历史和漏斗的严格 `as_of` 查询；
- 不可变 raw artifact manifest，并完成已有本地原始文件回填；
- 全市场缓存数据的批量双源差异落库和 1% 阈值标记；
- provider 健康状态、主要采集和重建任务记录；
- Python 3.11 隔离环境、`uv.lock` 和 Ruff 正确性检查。

仍需按数据域继续推进：A 股复权行情、公司行动、公告索引、宏观、事件和一致预期。

## Phase 0：方案冻结

- 确定项目命名、Python 版本、包管理和 schema 版本策略。
- 建立数据源登记表：许可、频率、限流、字段、主备关系、健康状态。
- 冻结第一版 Instrument、PriceBar、FinancialFact、Event、Estimate 模型。

验收：能用样例数据回答“某字段从哪里来、何时可见、是否复权、如何交叉验证”。

## Phase 1：兼容既有消费者

- 实现证券目录导入器。
- 只读导入 `market_data.sqlite3` 与 `eps_revision.sqlite3`。
- 建立旧 symbol ID 到新 instrument ID 的映射。
- 实现与旧 `/api/v1/quotes/latest`、`history`、`calendar` 等价的只读接口。

验收：既有消费者无需改变业务逻辑即可切换到新服务的兼容 API。

## Phase 2：A 股全市场底座

- 接入通达信日线包与 Mootdx 增量行情。
- 接入批量财务 ZIP、BaoStock 估值/状态/指数成分。
- 实现除权除息、股本事件和复权序列。
- 完成 point-in-time 财务查询与基础质量审计。

验收：对全 A 股执行轻量漏斗，不逐股票调用完整财报接口；随机抽样与第二来源比对。

## Phase 3：深度财务层

- 接入 AKShare 完整报表、主营构成、分红和股本接口。
- 建立巨潮/交易所公告索引与原始文件核验流程。
- 计算 TTM、ROIC、自由现金流、净负债和估值分位等派生指标。

验收：可完整支持现有质量筛选和安琪酵母一类公司的深度分析，关键指标双源误差可见。

## Phase 4：下游切换

- 全市场筛选器改用新服务。
- 本地行情应用改成纯消费者，保留短期应急回退。
- 增加数据新鲜度、provider 健康和失败任务告警。

验收：所有下游使用统一 ID 和 API；关闭重复抓取后结果一致。

## 首批实现任务建议

1. 创建 Python 包和最小 FastAPI 服务。
2. 定义 canonical schema 与 JSON/Arrow API 契约。
3. 编写 legacy catalog importer 和 SQLite 只读 importer。
4. 迁移新浪/Yahoo/东方财富日线 provider 及其固定样本测试。
5. 建立 raw artifact manifest、provider health 和 job run 表。
6. 再开始通达信/Mootdx/BaoStock 的批量底座。
