# BTC 小时合约研究数据闭环：Nautilus 复用开发方案

日期：2026-09-10。状态：开发设计，未实施、未部署、未完成真实数据验收。

## 1. 目标与边界

MarketCow 交付可重放的 BTC 小时市场事实：Polymarket 目录、规则、两 token
盘口/成交/规格、独立结算证据，以及 Binance 现货 BTCUSDT 分钟/小时行情。
Tradude 负责概率估计、策略、Paper 账户和收益判断。数据齐备不等于策略盈利。

用户已提出复用 Nautilus。采用其 Binance DataClient，不重新实现完整交易所
HTTP/WS 客户端；不注册 ExecutionClient，不加载下单密钥，不启动真实订单。
现有股票服务、U1 Live/Discovery 和账户不因本文改变。
不做版本回滚工程；策略正常换池、实体恢复、事务一致性仍保留。

## 2. 已核实与未核实

| 项目 | 当前证据 | 开发结论 |
|---|---|---|
| 本机 Nautilus | `/Volumes/T9/projects/trade/nautilus_trader`，pyproject 标识 1.231.0 | 实施前固定 commit、实际导入路径、编译模块版本、依赖锁和许可证；不混用在线 latest 配置 |
| Binance 历史 bars | `adapters/binance/data.py::_request_bars` 调用历史 Kline 请求 | 复用有限回补；分页、缺口和结束边界另测 |
| 实时 Kline | `_handle_kline` 对 `k.x=false` 直接返回 | 现有回调只覆盖已完成 bars，不声称提供进行中官方 Kline |
| 公共行情配置 | `BinanceDataClientConfig` 支持 SPOT、代理和无 API key 行情 | 仅现货 BTCUSDT；禁止误用 PERP、Binance US 或测试网 |
| Binance 大历史 | 官方 daily/monthly 压缩归档、CHECKSUM、修订说明 | 批量历史优先归档，短缺口用客户端回补 |
| Polymarket 规则原件 | 现有 `source_market_evidence.rs` | 复用读取和 raw 投影，不从标题批准规则 |
| 结算 | 现有 `source_finality_reader.rs` 固定区块读取 CTF | 当前输出 resolved_unverified；token/collateral/adapter 与最终性证据仍需补齐 |
| 历史 L2 | 社区存在档案声明 | 未验证目标覆盖、完整性和数据授权，不算已取得 |

本次示例合约规则为 Binance BTC/USDT 指定 1H 最终 Close >= Open 则 Up。
这只是一个合约的规则，不能推广到全部 BTC 短周期产品。

## 3. 架构与进程

```text
Binance 官方归档 ─→ 有界下载/校验/解析 ───────────┐
Nautilus Binance DataClient ─→ 薄适配层 ─────────┤
现有 Rust Polymarket 事实/实时读面 ──────────────┤
独立结算读取器 ─────────────────────────────────┤
                                               ↓
                              身份、时间、连续性和版本校验
                                               ↓
                           内存更新 → 立即发布 → Tradude
                                               ↓
                                     有界异步持久化队列
                                               ↓
                              原件分段 + 精确索引 + manifest
```

Nautilus 作为独立数据 worker，避免与本机股票 API 生命周期绑死。优先使用
DataClient factory + 仅数据 Actor 的受支持入口；不依赖私有 HTTP 对象作为公共契约。
若通过 TradingNode 容器运行，执行客户端映射必须为空，测试确认没有账户/下单请求。
以组件 `binance-data` 接入现有可选启动器，不另复制整套 supervisor。
worker 与 MarketCow 用有界本地 IPC 传输；接口保留 connection/epoch/sequence，
不能把 IPC 重新连接误当交易所连续性恢复。具体 IPC 编码以现有可复用实现为先。

## 4. Binance 薄适配层

### 4.1 第一版输入

- 固定 Binance 全球现货 BTCUSDT，显式 instrument identity。
- 实时成交及已完成 1m、1h bars；记录上游 trade ID 或 bar 窗口。
- 原始进行中 Kline 更新：优先核实固定版本的扩展回调能力；如无公开 hook，
  在受控适配补丁中于标准解析前旁路捕获原文，单独输出 `kline_update`。
- 不复制一套 WS 重连/心跳实现。若必须维护 Nautilus 补丁，固定 patch hash 并测试升级兼容。

`ts_init` 只能按实际代码解释为适配器处理时间，不能直接称 socket 首包时间。
当前 Nautilus 适配器以应用最早拿到 raw callback 的入口记录 wall + monotonic；
这不是更早的 kernel/socket 时间，所以同时保留 adapter_processed_at 和
`socket_kernel_receive_time_unknown`，不可把二者混称。

### 4.2 防止未来信息泄漏

- `bar_final` 与 `kline_update` 是不同类型，不能用未完成小时的最终 high/low/volume。
- 固定预测时点只能使用该时点前已收到的更新/已完成分钟 bars。
- 今日下载的完整分钟 bar 可以用于标注为事后历史的研究，不能声称当时已收到。
- 一小时最终涨跌产生 `model_label`，不写入 Polymarket `settlement`。
- 官方小时 bar 与分钟聚合的 open/close 做比较；不一致保留双方原件并标冲突。

### 4.3 重连与补洞

成交按交易所 ID 去重，允许显式报告 ID 缺口；不能假设聚合成交 ID 等同逐笔成交 ID。
bars 按 instrument/interval/open_time 唯一化。同键不同内容保留修订版本。
重连产生新连接 epoch；检测缺口后按明确时间窗回补，直到验证完成才清除质量缺口。
实时事件不等待历史回补；受影响的聚合窗口标 incomplete，不影响其他独立窗口。

## 5. Polymarket 小时市场身份与轮换

每个市场必须保存 market/event/condition、Up/Down token、series、完整规则原件
hash、规则来源、抓取时间、观察窗口 UTC、原时区及转换依据。
ET 使用 `America/New_York`，不用固定 UTC-4/UTC-5。DST 歧义或不存在时刻须有
源端偏移/唯一 UTC 证据，否则拒绝自动绑定，不能猜 fold。

元数据预筛只找候选；准入须确认 BTCUSDT 现货、1H、窗口、平价规则以及异常条款。
不同规则版本不混入同一语义组。当前规则 capture 不冒充历史事前版本。

跟踪集合：当前小时、后续两个小时、过去已结束但尚未取得最终证据的市场。
前三者是订阅规划上限建议，不自动修改现有生产池。待结算队列独立存储并轮询，
不能因盘口退订而停止结算追踪；到期未决保留 censored。
超过运行预算则明确暂停准入/降低研究覆盖，不静默丢弃未结算身份。

完整盘口优先复用已有 Rust fullsync/ready/增量协议，两 token 绑定同一 scope/instance。
池外市场必须显式准备和激活；管理与只读接口隔离，不从目录发现直接扩池。

## 6. 统一事实合同

所有事实共用以下 envelope，字段不能被标准化过程隐式覆盖：

| 字段 | 语义 |
|---|---|
| schema_version / source / source_version | 固定解析与来源版本 |
| entity_id / market_id / condition_id / token_id | 按类型需要，缺失不能用空串代替 |
| event_at / source_published_at | 源有效时间/发布时间；未知 null |
| first_received_at / received_monotonic_ns | 本系统接收边界，历史补录未知则 null；monotonic 仅同进程可比 |
| ingested_at / capture_id | 本次入库及采集身份 |
| raw_sha256 / raw_locator | 原文身份、精确 segment/offset/length |
| connection_epoch / source_sequence | 实际连接与上游序号；无源序号则 null |
| local_sequence / durable_sequence | 本地已发布与已持久化水位，绝不混用 |
| quality / missing_reasons / supersedes | 缺口、修订与替代事实 |

事实类型：market_rule、trade、bar_final、kline_update、book_snapshot、book_delta、
instrument_spec、lifecycle、settlement_observation、model_label。
价格和数量使用 decimal 字符串，时间单位显式。Binance 2025 年起现货归档为微秒；
REST/WS 的单位按所用协议分别校验，不按数值长度盲猜。

手续费、tick、minsize 分别记录来源、生效时间、币种、公式/取整及证据版本。
未知手续费不能默认为零；Nautilus 默认费率表不能冒充 Polymarket 历史费用证据。

## 7. 原件、索引、查询与重放

复用现有存储后端，不增加数据库产品。原件按 source/date/hour 分段，append-only；
索引包含 raw offset/length/hash、实体、有效时间和接收时间。压缩时使用可定位分块，
保留分块 hash 和块内位置，不能把解压偏移当压缩文件字节偏移。

正常实时路径先更新内存和发布，独立 writer 批量写段、刷盘、提交索引及 durable 水位。
恢复时校验段尾，只接纳有完整证据的记录；发布过但未持久化的尾部明确为恢复缺口。
队列满或磁盘失败必须显式不可用/断流并报告，不默丢、不无限缓存。

拟定逻辑操作（尚未部署 HTTP 路由）：

- `list_hours(start,end,limit,cursor)`：固定市场身份、规则与覆盖。
- `query_facts(dataset_id,entity,type,start,end,asof,limit,cursor)`：有界读取。
- `read_raw(dataset_id,locator)`：原件重读及 hash。
- `subscribe(entity_ids,types,after_sequence)`：基线、ready、增量和恢复错误。
- `export_dataset(selection,interval)`：固定清单，不隐式扩大市场范围。

`asof` 支持 `observed` 与 `event_time_research` 两种明确模式。
前者只使用已有真实 first_received_at 的事实；后者允许事后历史，但返回该限制。
禁止用 event_at 伪造 first_received_at 来让 observed 查询通过。
分页固定 dataset/版本；超出增量保留范围要求新基线。

manifest 保存代码/配置/schema hash、每段 hash/bytes、身份和窗口、来源许可证据、
逐市场逐类型覆盖矩阵、缺口/修订、实际请求预算、capture 开始结束及可重放命令。

## 8. 历史可得性路线

1. 第一批固定 2026-09-07T00:00:00Z 至 2026-09-08T00:00:00Z 的窗口起点，
   最多 24 个候选小时；按 UTC 升序，不按价格/赢家选择。无对应合约则保留空缺。
2. Binance 下载该 UTC 日 1m 和 1h spot archives 及 checksum，验证后保留版本。
3. 每个实际存在合约读取规则、身份和独立结算；当前读取不提供历史首次可知时间。
4. 官方价格历史只算价格历史。社区 L2 先查精确 token/hour、文件目录、许可证和
   小样本，再决定能否采用。README 宣称覆盖不算已验证。
5. 没有合格 L2 时交付缺失矩阵、其余真实事实及前向归档，不输出虚假净收益回测。

社区审查必须覆盖 pmxt、Rocklabs、ibold、Pancake、LuciferForge 及相关 SDK issue、
Reddit 讨论。逐项记录维护时间、真实/合成语义、schema、许可、目标 token 覆盖、
样本完整性。代码开源许可证不自动等于托管数据可下载/转发授权。不得下载未知代码执行。

## 9. 结算独立验收

保留 Gamma reported、oracle proposed/disputed、CTF payout 和 finality policy 的区别。
CTF numerator/denominator 可以是分数，不强制一热赢家。验证 chain、部署代码、
condition/outcome/token/collateral/adapter 绑定，再按显式区块最终性策略读取。
缺 RPC 或部署依据就明确 unavailable，不猜 endpoint、不降为 latest。
最终区块观察不能自动提供实际 resolution_time；需要对应日志/交易证据，缺则 null。
first_seen 是本系统第一次观察时间，不是最终结算的历史首次可用时间。

## 10. 第一阶段预算（设计候选，未启动）

| 项目 | 上限 |
|---|---|
| 历史选择 | 一个 UTC 日、最多24小时合约 |
| 目录/规则/价格 HTTP | 共128请求，串行、请求间隔至少1秒，无重定向；重试计入总额 |
| 小样本下载 | 总256MiB、单文件64MiB；压缩展开总512MiB，超限停 |
| 历史批次 | 总15分钟，单请求20秒，每次请求前检查剩余时间 |
| 实时验证 | 先30分钟、最多3个小时合约/6tokens；真实跨小时轮换另选覆盖边界的60分钟窗口 |
| 实时归档 | 每次最多1GiB，队列最多10000记录且64MiB，单原件最多8MiB |
| 读取 | 单页最多1000记录且4MiB，单次导出最多256MiB |
| 保留 | 小样本固定保留；前向原件建议7天/10GiB双限，达到上限先停止新增归档并告警，不自动删除已有资料 |

以上是应用保留/处理预算，不冒称传输层硬字节或硬实时上限。
先测 CPU/RSS/吞吐/队列/磁盘成本再决定常驻参数，不从30分钟外推多年存储保证。
RPC 使用独立显式预算和配置，不挤入未验证调用数。收费数据另列，不采购。

## 11. 实施拆分与依赖

| 步骤 | 交付 | 完成条件 |
|---|---|---|
| D1 版本与数据源审计 | 固定 Nautilus 版本、许可证、实际能力矩阵、社区数据审查 | API/版本差异及数据许可缺口明确 |
| D2 统一事实与manifest | 模型、哈希、时间/身份/覆盖校验器 | 合法/非法共享向量，as-of 不泄漏 |
| D3 Binance 历史导入 | 有界归档下载/校验/解析/修订 | 真实1m+1h同日样本、原文可定位 |
| D4 Nautilus 只读 worker | DataClient+Actor、原件hook、适配与IPC | 无执行客户端；trade/bar时间和进行中更新真实验证 |
| D5 小时身份与轮换 | 规则读取、窗口绑定、订阅规划、待结算队列 | DST、延期、重复、池外拒绝、到期仍追结算 |
| D6 持久化与读取 | 异步writer、段索引、水位、查询/导出 | 阻塞磁盘仍正常发布直到有界容量；崩溃尾部不冒充durable |
| D7 结算与历史L2 | 权威绑定读取；合格档案适配或明确缺口 | 实际payout证据或确切不可得原因；不以推导标签替代 |
| D8 真实贯通 | 同一批hour manifest、运行命令、覆盖报告 | 目录→规则→Binance→报价→结算逐项有证据，缺项显式 |

D2 先行；D3/D4/D5 可独立推进；D6 复用现有基础；D7 不等待新市场结束才验证读取器。
历史 L2 缺失不阻断其他模块，但阻断“历史可执行收益已验证”的结论。
阶段报告不是全流程完成，最后交付必须列出仍不可得的数据字段。

## 12. 测试与交付边界

- 单测：UTC/ET/DST、未来时间、未完成bar、重复/修订、错token/condition、版本不符。
- 集成：只读客户端工厂、真实数据解码、无账户/下单端点、断线补洞、重启去重。
- 压力/故障：慢磁盘、队列字节/条数、写失败、慢消费者、连接断开、实体故障隔离。
- 业务事实：费用变化、规则变化、待定/争议/分数payout、结算迟到、缺历史first_seen。
- 重放：同manifest输出一致；observed查询不能看到后来补录的历史事实。
- 真证据：原件/配置/代码hash、命令、请求数、bytes、延迟分布、RSS峰值及失败报告。

本地开发和测试不自动授权生产扩池、远端上传、收费采购或部署。
常驻 Binance worker、U1 订阅及资源配置启用前集中说明实际影响；不为普通函数步骤重复询问。

## 13. 参考依据

- 示例规则：https://polymarket.com/event/bitcoin-up-or-down-september-10-2026-5pm-et
- Nautilus Binance：https://nautilustrader.io/docs/latest/integrations/binance/
- Binance 官方归档：https://github.com/binance/binance-public-data
- 历史价格粒度问题：https://github.com/Polymarket/py-clob-client/issues/216
- 社区档案线索：https://github.com/rocklabs-io/polymarket-dataset
- 其他待逐项核验：https://github.com/pmxt-dev/pmxt 、https://github.com/ibold-dev/polymarket-orderbook-history
- 非L2/合成边界待审：https://github.com/usepancake/polymarket-history 、https://github.com/LuciferForge/polymarket-historical-data

参考链接不构成数据背书；文档中“已核实”仅限上文明确的代码/说明范围。

## 14. 实施记录：离线归档导入 r1

已实现 `src/marketcow/btc_hourly_dataset.py`，只导入本地已取得的 Binance
2025年后现货日归档；不联网、不启动服务。输出精确 raw.csv 字节索引、facts.jsonl
及最后原子发布的 manifest.json。缺口不会补造，失败目录保留且不生成成功 manifest。
`status=complete` 仅表示本次导入完成，是否完整一天单独看 `coverage_complete`。

```bash
PYTHONPATH=src python3 -m marketcow.btc_hourly_dataset \
  --archive /absolute/BTCUSDT-1h-2026-09-07.zip \
  --checksum /absolute/BTCUSDT-1h-2026-09-07.zip.CHECKSUM \
  --output /absolute/new-dataset \
  --interval 1h --day 2026-09-07 \
  --captured-at 2026-09-10T00:00:00Z \
  --maximum-zip-bytes 67108864 \
  --maximum-uncompressed-bytes 536870912 --maximum-records 24
```

路径和 captured-at 必须替换为实际下载原件和时间，以上不是已执行真实采集。
当前测试为合成归档，不证明官方实际当日文件可得。下载、实时 worker、跨源统一模型、
查询服务、Polymarket配对及结算/L2仍未实现完毕，不以本导入模块替代D1–D8验收。
