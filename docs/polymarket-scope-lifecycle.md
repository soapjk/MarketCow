# Polymarket exact scope 生命周期与滚动切换

本设计面向使用 `:8790` / `:8791` 的模拟交易消费者。固定 scope 是内容寻址、不可变的
100 个 `market_id` 集合；市场生命周期变化只改变该成员的运行状态，不修改原 manifest，
也不以替补市场覆盖旧 scope。

## 数据与状态口径

- CLOB `POST /books` 是订单簿来源。请求 token 缺失经过有界重试后，collector 才进入
  生命周期核验；不能仅凭 CLOB 缺失判定到期。
- Gamma markets 是终态核验来源。只有 Gamma 明确给出 `closed`、`resolved` 或 `invalid`
  才发布 `market_terminal` 事件。事件保留最后市场状态、来源 URL、观测时间、原始证据
  SHA-256 和被退订 token；旧订单簿仍留作审计，但不再参与 freshness。
- Gamma 仍报告 active、Gamma 缺少该市场、响应错误或证据不完整时，CLOB 缺失继续按
  `ClobBooksCoverageError` 处理，不能降格为正常终态。
- 时间统一为带时区 UTC；金额/价格仍沿用十进制定点字符串契约。本改造不执行金额计算。

热健康响应新增 `active_market_count`、`terminal_market_count`、
`complete_market_count`、`missing_market_count`、`scope_status` 和 `scope_id`。
`99 active + 1 terminal` 返回 HTTP 200、`status=degraded`、
`scope_status=terminal_degraded`，不会返回 `index_ready`。活跃市场缺书则为
`scope_status=data_degraded`，继续 fail closed。

下游开仓前必须调用 `PolymarketOpenPositionGuard.require_eligible`。稳定拒绝码为：

- `polymarket_open_position_market_resolved`
- `polymarket_open_position_market_terminal`
- `polymarket_open_position_orders_not_accepted`
- `polymarket_open_position_fresh_book_required`

每次判断写入本地 append-only 审计 JSONL，包含 request、市场 revision、frame cursor、
原因及 `real_order_submission_enabled=false`。该 guard 只做判断，绝不提交订单。

## Candidate、验收与切换

每个 candidate 使用独立 root、collector、live stream 和 API 进程边界预热。启动器在
candidate root 写入不可变 `scope-runtime.json`，使 8790/8791 的 bootstrap、snapshot、
health 和 full-sync 都能返回 manifest 的内容寻址 `scope_id`。

```bash
PYTHONPATH=src python scripts/manage_polymarket_scopes.py \
  --registry-root /absolute/polymarket-scope-registry generate \
  --selection /absolute/tradude-scope-selection-v2.json \
  --output /absolute/candidate-manifest.json

PYTHONPATH=src python scripts/manage_polymarket_scopes.py \
  --registry-root /absolute/polymarket-scope-registry \
  prepare --manifest /absolute/candidate-manifest.json

PYTHONPATH=src python scripts/verify_polymarket_exact_scope.py \
  --scope-manifest /absolute/candidate-manifest.json \
  --expected-scope-id '<sha256>' --port 8790 --port 8791 \
  --rounds 2 --round-interval-seconds 5 \
  --output /absolute/candidate-acceptance.json

PYTHONPATH=src python scripts/manage_polymarket_scopes.py \
  --registry-root /absolute/polymarket-scope-registry \
  accept --scope-id '<sha256>' --evidence /absolute/candidate-acceptance.json

PYTHONPATH=src python scripts/manage_polymarket_scopes.py \
  --registry-root /absolute/polymarket-scope-registry \
  activate --scope-id '<sha256>' --grace-seconds 300
```

生成器只验证 Tradude 显式给出的 1–100 个市场和完整关系成员，并原样保留市场顺序。
MarketCow 不保留旧成员、不按流动性或结束时间排序，也不自动补足 Top 100。

验收器要求两端均为 HTTP 200 / `index_ready`，100 markets、200 books、100 complete、
tick 200/200、gap 0、disconnect 0，并至少两轮 cursor 推进。任何一项不满足都不会生成
可激活 acceptance。active pointer 通过临时文件、fsync 和 `os.replace` 原子切换；旧 scope
在宽限期内按其 `scope_id` 明确返回 `grace`，宽限后返回稳定
`polymarket_scope_retired`，而不是模糊的 market 404。回滚命令为：

```bash
PYTHONPATH=src python scripts/manage_polymarket_scopes.py \
  --registry-root /absolute/polymarket-scope-registry rollback --grace-seconds 300
```

candidate manifest、candidate descriptor 和 acceptance 都是 create-once 文件；内容不同的
重复写入会以 `polymarket_scope_immutable` 拒绝。active pointer 的每次切换另写带哈希的
`scope-transitions.jsonl` 审计记录。

## 权限与合规边界

这些工具只读官方公开市场数据并修改本地运行状态，不调用真实订单、转账、支付或账户
变更 API。prepare、accept、activate 和 rollback 证据均强制记录
`real_order_submission_enabled=false`。生产进程编排、监管适用性及跨系统交易权限仍需由
部署方按所在地和机构规则复核。
