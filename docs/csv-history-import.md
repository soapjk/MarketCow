# CSV 历史 K 线导入契约

状态：MCVI 基础契约，版本 `marketcow.csv-bars.v1`。

机器可读契约见 `docs/csv-import-contract-v1.schema.json`；运行时由
`CsvImportRequest`、`CsvSchemaProfile` 和 `InstrumentMapping` 执行同一组约束。

## 身份与来源边界

CSV 中的供应商代码不是 MarketCow Instrument ID。导入请求必须声明显式
namespace（如 `provider:vendor_name`）并提供完整映射：

```text
AAPL.US -> AAPL.XNAS
IBM.US  -> IBM.XNYS
```

美股 `.US` 后缀和裸 ticker 都不能决定交易场所。缺少映射、MIC 冲突或未知 MIC
时整行失败，不推断 XNAS/XNYS。

`adjustment=raw` 表示供应商原始价格；`adjustment=adjusted` 表示供应商已经完成
拆股/分红复权的价格。两者使用不同的存储键和 Manifest 身份，不允许在同一声明中
混合。CSV 导入不会猜测或自行生成供应商未提供的复权因子。

## Profile

每个供应商格式通过版本化 Schema Profile 描述：

- Profile 名称和版本；
- CSV 编码与单字符分隔符；
- canonical 字段到供应商列名的映射；
- timestamp 格式和 IANA 时区；
- symbol 列，或单标的文件使用固定供应商 symbol。
- 价格、成交量、成交额的单位倍率和小数精度；
- volume/amount 默认值、额外来源列策略和本地交易时段。

必需 canonical 字段为 `timestamp/open/high/low/close`，同时必须提供 symbol 列
或固定 symbol。可选字段为 `volume/amount`。

## dry-run

dry-run 以流式方式读取文件，不写业务数据库。它输出：

- 文件名、字节数和 SHA-256；
- 总行数、有效/无效行数、重复行、乱序行和日内缺口；
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
- 无 UTC offset 的 DST 重复时间被拒绝；不存在的本地时间也被拒绝；
- 声明交易时段后，周末或时段外数据失败；未声明日历时报告明确 warning。

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
    "fixed_external_symbol": null,
    "allow_extra_columns": true,
    "defaults": {},
    "price_multiplier": "1",
    "volume_multiplier": "1",
    "amount_multiplier": "1",
    "price_precision": 4,
    "volume_precision": 0,
    "amount_precision": 2,
    "trading_sessions": [
      {"start": "09:30", "end": "16:00", "weekdays": [0, 1, 2, 3, 4]}
    ]
  },
  "instruments": {
    "namespace": "provider:purchased_vendor",
    "symbols": {
      "AAPL.US": "AAPL.XNAS",
      "IBM.US": "IBM.XNYS"
    }
  },
  "created_by": "data-operations",
  "source_proof": "purchase-order:PO-2026-001",
  "retention_policy": "retain-until-explicit-deletion"
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

生产前真实样本 smoke test 应额外输出脱敏证据文件：

```bash
marketcow --profile production import-bars \
  --file /allowed/imports/vendor-authorized-sample.csv \
  --config /allowed/imports/vendor-us-profile.json \
  --idempotency-key smoke-vendor-us-20260725 \
  --evidence-output /allowed/evidence/vendor-us-smoke-20260725.json
```

证据文件使用 `marketcow.csv-import-smoke-evidence.v1`，包含输入文件哈希和大小、
Manifest、终态、分片 receipt 与质量报告，但不包含 CSV 内容、原始配置、服务器
归档路径或购买数据路径。目标文件必须不存在，避免覆盖既有审计证据。

CSV 与声明文件都必须位于 `MARKETCOW_ALLOWED_ROOT` 下。原始 CSV 会以内容哈希
命名，原子复制到 MarketCow storage；同一 Manifest 重复提交不会产生第二份文件。
未配置 allowed root 时仅允许 storage root。文件大小默认上限为 100 GiB，可用
`MARKETCOW_CSV_IMPORT_MAX_FILE_BYTES` 调低。API 不返回服务器归档路径。

## 管理 API 与页面

- `POST /v1/admin/csv-imports/dry-run`
- `POST /v1/admin/csv-imports/upload?filename=<name.csv>`
- `POST /v1/admin/csv-imports/infer`
- `POST /v1/admin/csv-imports`
- `GET /v1/admin/csv-imports`
- `GET /v1/admin/csv-imports/{job_id}`
- `POST /v1/admin/csv-imports/{job_id}/cancel`
- `POST /v1/admin/csv-imports/{job_id}/retry`
- `GET /v1/admin/csv-imports/{job_id}/manifest`
- `GET /v1/admin/csv-imports/{job_id}/quality-report`
- `GET /v1/admin/csv-imports/{job_id}/errors`
- `GET /v1/admin/csv-imports-ui`

React 管理控制台的 `#/csv-imports` 页面支持直接从浏览器选择 CSV 文件。
上传请求使用流式 `application/octet-stream` 或 `text/csv` 请求体，受
`MARKETCOW_CSV_IMPORT_MAX_FILE_BYTES` 限制；服务端只返回不可猜测的
`upload_id`、文件大小和 SHA-256，不向浏览器暴露本地暂存路径。页面要求操作者
显式选择 MIC，并将供应商代码映射为唯一的 `SYMBOL.MIC`。美股不会根据 ticker
自动推断 XNAS、XNYS 或 ARCX。

上传响应同时返回检测到的表头和分隔符。管理页面会忽略大小写并按常见别名自动
匹配时间、OHLC 和成交量列，例如 `DateTime`、`Open`、`Volume`；操作者可以在
预检前通过下拉框修正每一项映射。映射或其他声明发生变化后，原预检结果立即失效，
必须重新预检才能开始正式导入。

`infer` 对源时区和复权语义给出建议值、分数、置信度及证据，不把推断冒充为
事实。时区推断会比较所选 MIC 的交易时段模型、UTC 和常见区域时区，并对带显式
offset 的时间戳优先采用其自身证据。实现使用确定性的蓄水池抽样，扫描文件但只
保留至多 20,000 个时间点。

复权推断遵循保守规则：选择 `Adjusted Close` 等显式列，或文件同时提供 Close
和独立调整列时，可以给出高置信度；只有普通 OHLCV、没有供应商元数据或公司行动
对照时，只给出低置信度 `raw` 建议，并明确要求人工确认。管理页面会自动预填建议，
但保留人工覆盖；列映射或 MIC 改变会重新推断，手工修改时区或复权状态后则使用
操作者选择并要求重新预检。任一语义结果为低置信度或无法判断时，管理页面要求
操作者显式确认后才开放正式导入按钮。

上传文件会先写入 `storage/csv-import-uploads` 的原子暂存文件。预检和正式导入
均以 `upload_id` 引用该文件；创建正式任务成功后删除暂存副本，长期留存由原有
内容寻址归档和 Manifest 负责。旧的服务器本地 `path` 调用仍可用于 CLI 和自动化，
但 API 请求必须在 `path` 与 `upload_id` 中且仅选择一个。

管理页面每两秒刷新分片进度，显示 Manifest、错误和 raw/canonical 质量报告，
并允许取消或用同一不可变 Manifest 重试。启动、取消、重试均要求浏览器确认。
页面只提交服务器允许目录内的路径，不接受任意远程 URL。

## 恢复和质量门禁

导入任务及分片保存在 PostgreSQL。worker 使用租约、心跳和 fencing token；
服务重启会接管 queued/running 任务，失败分片在有界次数内重试。每个分片内的
Instrument 使用独立稳定 `ingestion_id`，因此重复执行由 ClickHouse raw
`ReplacingMergeTree` 和 ingestion receipt 幂等化。

raw 写入完成后，任务同步触发受影响范围的 canonical rebuild。质量门禁按
ingestion receipt 的精确 raw key 与 canonical 表连接，只有每一条导入 raw bar
都有 canonical 对应项且 artifact/行数一致时，任务才进入 `succeeded`；否则任务
进入 `failed` 并保存 `marketcow.csv-import-quality.v1` 报告。报告还复核
Manifest 首尾时间、重复键、OHLC 预检结果、日内缺口和交易日历策略；缺口及未配置
日历是显式 warning，不会被静默忽略。

## Manifest 与保留

Manifest 记录文件 SHA-256/字节数、Profile 与契约版本、映射、interval、
adjustment、首尾时间、创建者、来源证明和保留策略。Job 保存 `manifest_id`，
Artifact 元数据也保存同一 ID，形成双向关联。默认保留策略
`retain-until-explicit-deletion` 表示系统不会自动删除购买的原始数据；清理必须由
获授权操作者先导出 Manifest/质量报告并验证不存在运行中或待重试任务。
Artifact 元数据中的 `manifest_id` 可用于
`GET /v1/admin/csv-imports?manifest_id=<id>` 反查全部关联任务。
