# Provider 开发脚手架

MarketCow 的公开 API 只表达业务能力，不绑定上游。新增 provider 时，调用方仍通过统一接口请求，
可以显式指定 `provider`，也可以交给内部路由选择。

## 最小实现步骤

1. 在 `src/marketcow/providers/` 新建适配器模块，只处理认证、限流、超时、上游协议和字段规范化。
2. 在 `providers/contracts.py` 的 `DEFAULT_PROVIDER_MANIFESTS` 添加唯一 `provider_id`、真实
   `source_name`、支持的能力、市场和适配器方法。
3. 在 `FundamentalService` 构造函数创建适配器，并通过 `provider_registry.bind()` 注册。绑定点应
   明确列出它实际调用的能力；注册时会立即检查对应方法是否存在，未注册或能力不匹配会 fail-closed。
4. 为每项能力增加 provider 单元测试、统一输出契约测试和 provider routing 测试。
5. 记录凭证引用、批量上限、频率限制、超时、数据延迟、真实上游来源和已知许可限制。禁止在
   响应、异常或日志中输出凭证。

## 实时报价模板

```python
from typing import Any


class ExampleQuoteProvider:
    name = "example_api"  # 响应中可审计的真实来源，不一定等于 provider_id

    def __init__(self, token: str, timeout: float = 2.0):
        self._token = token
        self._timeout = timeout

    @property
    def configured(self) -> bool:
        return bool(self._token)

    def fetch_quote(self, symbol: str) -> dict[str, Any]:
        payload = self._bounded_request(symbol)  # 必须有总超时、限流和响应大小边界
        return {
            "instrument_id": "US.XNAS." + symbol,
            "symbol": symbol,
            "market": "US",
            "exchange": "XNAS",
            "currency": "USD",
            "price": float(payload["last"]),
            "previous_close": None,
            "change": None,
            "change_pct": None,
            "session": "unknown",
            "quote_at": payload.get("observed_at"),
            "price_adjustment": "raw",
            "quality_status": "single_source_unverified",
            "source": self.name,
            "source_url": "https://api.example.invalid/quotes",
            "raw_response_locator": "data[0]",
            "_raw_payload": payload,
        }
```

测试中调用 `validate_realtime_quote(result)`，然后验证代码映射、UTC 时间、空值、错误脱敏、超时、
限流、批量边界和上游失败。真实网络测试应 opt-in，默认测试必须使用固定 fixture。

## 能力与路由规则

- `realtime_quote`：标准化最新报价。
- `market_bar_history`：标准化历史行情和 provenance。
- provider 若有官方批量报价能力，应实现 `fetch_quotes()`；统一报价 API 会优先使用一次批量调用。
- 显式指定 provider 时，若市场或能力不支持，返回 `provider_not_supported`，默认不得静默换源。
- 未指定 provider 时，只在声明支持该能力与市场的 provider 中按配置优先级选择。
- provider 私有字段只能放在原始 Artifact；公开响应和 ClickHouse 行情字段保持统一。

新增能力时先定义统一业务契约，再扩展 `CapabilityDeclaration` 和公开 API；不要以 provider 名称创建
新的公开路由。
