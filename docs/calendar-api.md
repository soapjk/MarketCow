# MarketCow 日历 API 契约

MarketCow 将宏观经济事件、经济指标和财报事件保存到本地 DuckDB，再通过只读 HTTP API 提供给本地消费者。读取接口不会隐式访问上游；采集只由 `/v1/admin/*/refresh` 显式触发。

## 公共约定

- 基础地址：`http://127.0.0.1:8790/v1`
- 日期格式：`YYYY-MM-DD`
- 时间格式：`HH:MM:SS`
- 默认过滤时区：`Asia/Shanghai`
- `event_date` 和 `report_date` 是上游公布的市场本地日历日期。
- 每条事件的 `timezone` 使用 IANA 时区名；有确定时刻时，`scheduled_at` 为带 UTC offset 的 ISO 8601 时间。
- 未提供 `from` 时，读取接口默认从上海时区的当天开始，因此过期事件不会返回。
- 如需读取历史事件，必须同时传入历史 `from` 和 `include_past=true`。
- `limit` 范围为 1–500；snapshot 的 `days` 范围为 1–120。

## 宏观经济日历

```http
GET /v1/economic-calendar?country=US&from=2026-07-18&to=2026-08-17&impact=Medium&limit=50
```

响应：

```json
{
  "count": 1,
  "from": "2026-07-18",
  "to": "2026-08-17",
  "filter_timezone": "Asia/Shanghai",
  "past_events_excluded": true,
  "events": [
    {
      "event_id": "4ef5fb92196e785da8d9cb9d",
      "country": "US",
      "event_date": "2026-07-21",
      "event_time": "08:30:00",
      "timezone": "America/New_York",
      "scheduled_at": "2026-07-21T08:30:00-04:00",
      "event_name": "Direct Investment by Country and Industry, 2025",
      "impact": "Medium",
      "actual": "",
      "estimate": "",
      "previous": "",
      "unit": "",
      "source": "bea_official",
      "source_url": "https://www.bea.gov/news/schedule",
      "observed_at": "2026-07-18T00:16:23+00:00",
      "ingested_at": "2026-07-18T00:16:23+00:00",
      "raw_response_locator": "calendar table row",
      "raw_path": "data/raw/calendars/economic_calendar/…json",
      "raw_artifact_id": "…"
    }
  ]
}
```

低频刷新：

```bash
curl -X POST 'http://127.0.0.1:8790/v1/admin/economic-calendar/refresh?days=30&country=US'
```

当前免费来源为 BEA Release Schedule 和 Census Economic Indicator Calendar。

## 经济指标

```http
GET /v1/economic-indicators?country=US&source=bls&limit=50
```

核心字段：

| 字段 | 含义 |
|---|---|
| `indicator_id` | 稳定指标标识，例如 `bls_cpi_all_items` |
| `source_series_id` | 上游序列 ID |
| `period` / `latest_date` | 数据所属期间及规范日期 |
| `value` / `previous_value` | 最新值与前值 |
| `change_value` / `change_pct` | 环比数值变化及百分比变化 |
| `unit` / `frequency` | 单位与频率 |

低频刷新：

```bash
curl -X POST 'http://127.0.0.1:8790/v1/admin/economic-indicators/refresh'
```

当前覆盖 CPI、失业率、非农就业、平均时薪和 PPI，来源为 BLS Public Data API。官方数据不包含市场预期值，接口不会伪造预期。

## 财报日历

```http
GET /v1/earnings-calendar?market=US&symbols=PDD&from=2026-07-18&to=2026-08-17&limit=50
```

核心字段：

| 字段 | 含义 |
|---|---|
| `market` | `US`、`CN` 或 `HK` |
| `symbol` / `name` | 市场代码与公司名称 |
| `report_date` / `report_time` | 预计披露日期与上游时间描述 |
| `timezone` / `scheduled_at` | 市场时区及可确定时的 ISO 8601 时刻 |
| `fiscal_period` | 财报所属期间 |
| `eps_forecast` / `previous_eps` | 上游提供时的 EPS 预期与前值 |

低频刷新：

```bash
curl -X POST 'http://127.0.0.1:8790/v1/admin/earnings-calendar/refresh?days=30&symbols=PDD,600519,00700'
```

数字代码会路由到上交所和港股日历；美股字母代码会路由到 Nasdaq。上交所适配器当前覆盖沪市证券，其他市场或缺少预约日期的证券会返回空结果。

## Snapshot 兼容接口

```http
GET /v1/snapshot?limit=50&days=30
```

响应稳定包含：

```json
{
  "generated_at": "2026-07-18T01:00:00+00:00",
  "filter_timezone": "Asia/Shanghai",
  "date_format": "YYYY-MM-DD",
  "from": "2026-07-18",
  "to": "2026-08-17",
  "past_events_excluded": true,
  "quotes": [],
  "economic_calendar": [],
  "economic_indicators": [],
  "earnings_calendar": []
}
```

三个日历数组及其旧字段名可替代旧服务 snapshot 中的同名字段。`quotes` 暂留为空数组，行情消费者继续使用 MarketCow 的 `/v1/quotes/*` 接口。
