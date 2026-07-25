# CSV 历史 K 线导入契约

状态：MCVI 基础契约，版本 `marketcow.csv-bars.v1`。

## 身份与来源边界

CSV 中的供应商代码不是 MarketCow Instrument ID。导入请求必须声明显式
namespace（如 `provider:vendor_name`）并提供完整映射：

```text
AAPL.US -> AAPL.XNAS
IBM.US  -> IBM.XNYS
```

美股 `.US` 后缀和裸 ticker 都不能决定交易场所。缺少映射、MIC 冲突或未知 MIC
时整行失败，不推断 XNAS/XNYS。

## Profile

每个供应商格式通过版本化 Schema Profile 描述：

- Profile 名称和版本；
- CSV 编码与单字符分隔符；
- canonical 字段到供应商列名的映射；
- timestamp 格式和 IANA 时区；
- symbol 列，或单标的文件使用固定供应商 symbol。

必需 canonical 字段为 `timestamp/open/high/low/close`，同时必须提供 symbol 列
或固定 symbol。可选字段为 `volume/amount`。

## dry-run

dry-run 以流式方式读取文件，不写业务数据库。它输出：

- 文件名、字节数和 SHA-256；
- 总行数、有效/无效行数、重复行和乱序行；
- 每个 canonical Instrument 的行数与首尾时间；
- 按稳定错误码聚合的错误统计；
- 有上限的错误样本（包含 CSV 行号）。

重复键为 `(instrument_id, bar_at)`，通过磁盘临时索引进行精确检查，因此内存
不会随 K 线行数线性增长。临时索引在 dry-run 结束后删除。

## 行级不变量

- timestamp 按 Profile 时区解析后转换为 UTC；
- OHLC 必须是有限正数并满足 `low <= open/close <= high`；
- volume 和 amount 可为空，但存在时必须是有限非负数；
- CSV 表头必须包含 Profile 声明的全部来源列；
- 未映射 symbol、非法时间和非法数值使用稳定错误码报告。

正式导入将在此契约之上增加 Manifest、分片 checkpoint、稳定
`ingestion_id`、ClickHouse receipt 对账、canonical 构建和质量门禁。
