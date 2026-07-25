# K 线复权数据契约

MarketCow 新写入的规范 K 线只允许三种 `adjustment`：

- `raw`：OHLC 是 Provider 返回的未复权价格。
- `qfq`：OHLC 是以前复权基准计算的价格。
- `hfq`：OHLC 是以后复权公式计算的价格。

旧值 `adjusted` 只允许出现在兼容读取或迁移入口。只有在调用方能够证明它具体表示
`qfq` 或 `hfq` 时才能迁移；服务不得猜测。

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

