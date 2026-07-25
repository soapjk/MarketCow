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

## CLI

声明文件使用与 API 相同的版本化结构：

```json
{
  "contract_version": "marketcow.csv-bars.v1",
  "source": "purchased_vendor",
  "interval": "1m",
  "adjustment": "raw",
  "profile": {
    "name": "purchased-vendor-us",
    "version": "1",
    "columns": {
      "symbol": "ticker",
      "timestamp": "datetime",
      "open": "open",
      "high": "high",
      "low": "low",
      "close": "close",
      "volume": "volume"
    },
    "timezone_name": "America/New_York",
    "timestamp_format": "%Y-%m-%d %H:%M:%S",
    "encoding": "utf-8-sig",
    "delimiter": ",",
    "fixed_external_symbol": null
  },
  "instruments": {
    "namespace": "provider:purchased_vendor",
    "symbols": {
      "AAPL.US": "AAPL.XNAS",
      "IBM.US": "IBM.XNYS"
    }
  }
}
```

先执行无写入检查：

```bash
marketcow --profile production import-bars \
  --file /allowed/imports/vendor-us-1m.csv \
  --config /allowed/imports/vendor-us-profile.json \
  --dry-run
```

正式任务必须提供幂等键；CLI 会等待任务进入终态并返回质量报告：

```bash
marketcow --profile production import-bars \
  --file /allowed/imports/vendor-us-1m.csv \
  --config /allowed/imports/vendor-us-profile.json \
  --idempotency-key vendor-us-1m-20260725 \
  --chunk-rows 100000 \
  --max-attempts 3
```

CSV 与声明文件都必须位于 `MARKETCOW_ALLOWED_ROOT` 下。原始 CSV 会以内容哈希
命名，原子复制到 MarketCow storage；同一 Manifest 重复提交不会产生第二份文件。

## 管理 API 与页面

- `POST /v1/admin/csv-imports/dry-run`
- `POST /v1/admin/csv-imports`
- `GET /v1/admin/csv-imports`
- `GET /v1/admin/csv-imports/{job_id}`
- `POST /v1/admin/csv-imports/{job_id}/cancel`
- `GET /v1/admin/csv-imports-ui`

管理页面每两秒刷新任务状态，显示 raw/canonical 质量门禁结果，并允许取消尚未
进入终态的任务。页面只提交服务器允许目录内的路径，不接受任意远程 URL。

## 恢复和质量门禁

导入任务及分片保存在 PostgreSQL。worker 使用租约、心跳和 fencing token；
服务重启会接管 queued/running 任务，失败分片在有界次数内重试。每个分片内的
Instrument 使用独立稳定 `ingestion_id`，因此重复执行由 ClickHouse raw
`ReplacingMergeTree` 和 ingestion receipt 幂等化。

raw 写入完成后，任务同步触发受影响范围的 canonical rebuild。质量门禁按
ingestion receipt 的精确 raw key 与 canonical 表连接，只有每一条导入 raw bar
都有 canonical 对应项且 artifact/行数一致时，任务才进入 `succeeded`；否则任务
进入 `failed` 并保存 `marketcow.csv-import-quality.v1` 报告。
