# K 线复权数据契约

MarketCow 新写入的规范 K 线只允许三种 `adjustment`：

- `raw`：OHLC 是 Provider 返回的未复权价格。
- `qfq`：OHLC 是以前复权基准计算的价格。
- `hfq`：OHLC 是以后复权公式计算的价格。

CSV v1 历史声明已经一次性迁移到 v2。所有运行时入口只接受上述三个明确值，
不存在旧值兼容分支。

## 字段

每根规范 K 线的复权部分由以下字段组成：

| 字段 | 含义 |
| --- | --- |
| `adjustment` | `raw`、`qfq` 或 `hfq` |
| `factor_applicability` | `applicable` 或 `not_applicable` |
| `corporate_action_factor` | 该交易日由 Provider 给出的累计公司行动因子 |
| `applied_adjustment_multiplier` | 实际乘到原始 OHLC 上的乘数 |
| `adjustment_reference_date` | 前复权使用的基准交易日 |
| `reference_factor` | 前复权基准日的累计因子 |
| `factor_source` | 因子 Provider |
| `factor_artifact_id` | 不可变原始响应证据 |
| `factor_as_of` | 本次因子快照的 UTC 观测时间 |

股票等存在公司行动的资产使用 `factor_applicability=applicable`。即使保存的是
`raw` K 线，也必须保存真实的 `corporate_action_factor` 及其来源证据。

加密货币永续等不存在公司行动复权语义的资产使用
`factor_applicability=not_applicable`。它只能是 `raw`，应用乘数必须为 `1`，不得伪造
值为 `1` 的公司行动因子。

## 公式

设原始价格为 `P_raw`，交易日累计因子为 `F_t`，前复权基准日累计因子为 `F_ref`：

```text
raw: P = P_raw,                 applied_adjustment_multiplier = 1
qfq: P = P_raw × F_t / F_ref,   applied_adjustment_multiplier = F_t / F_ref
hfq: P = P_raw × F_t,           applied_adjustment_multiplier = F_t
```

`corporate_action_factor` 和 `applied_adjustment_multiplier` 是两个不同概念，不能再复用
同一个字段表达。所有因子和乘数通过十进制字符串进入契约，避免 JSON 浮点数先行损失
精度。

## 旧数据审计与回填

默认命令只生成计划，不写数据：

```bash
uv run python scripts/backfill_adjustment_contract.py \
  --profile production --limit 10000
```

生产写入需要同时提供两个显式参数：

```bash
uv run python scripts/backfill_adjustment_contract.py \
  --profile production --limit 10000 --apply \
  --confirm APPLY_ADJUSTMENT_BACKFILL
```

工具只自动处理两类可以证明语义的数据：

- Tushare `raw` K 线与同标的、同来源、同上海交易日的日因子精确匹配；
- 不存在公司行动语义的 CRYPTO `raw` K 线标记为 `not_applicable`。

找不到日因子的股票 K 线和来源语义无法证明的数据只进入 quarantine 报告，不会
猜测或覆盖。

## 2026-07-25 生产验收记录

- ClickHouse migration 8 已应用，raw/canonical 表均包含八个显式复权字段。
- Tushare 历史任务 `6587295140e240bb8f26b2ea61cd127c` 成功补取贵州茅台两个
  交易日的 60 分钟 K 线与日因子，写入 10 根 K 线。
- 安全回填批次 `adjustment-backfill-7f6b7a932854b798c8a83e93` 写入 731 条
  可证明记录；raw 层最终为 `applicable=20`、`not_applicable=721`。
- 64 条旧 Yahoo raw 记录因历史下载时没有保存可证明的因子来源，保持隔离，没有猜测。
- 已修复分组完成 canonical rebuild：贵州茅台两个标识分组各 5 条，
  BTC 永续 713 条；生产健康检查为 ready。
