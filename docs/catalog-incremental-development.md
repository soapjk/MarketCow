# 目录冷启动与增量维护

## 已接通的本地链路

Rust `catalog_capture` → 逐页原文/hash/请求/分页终点验证 → 原始库存合并 → dirty 关系组规范化 → 原有 phase1 mapper/index → SQLite 原子差分 → 不可变目录生成 → 现有鉴权 snapshot/page 与新增 changes/status → 参考消费者。

这是目录 worker，不是行情处理器或策略选择器；不操作 Live/Discovery 名单、账户和订单，不把目录更新放到实时发布前置，不添加服务版本回滚工程。

上游分页不是变更订阅：周期 open 遍历发现新增、known 定点复核最久未观察的身份、周期 closed 遍历对账，然后本地差分。没有官方 updated_since 或同一瞬间原子全目录承诺。404/缺席不自动视为关闭。

## 可执行入口

```sh
cargo build --offline --manifest-path tools/catalog-capture/Cargo.toml --release
PYTHONPATH=src python3 -m marketcow.catalog_refresh_worker --help
PYTHONPATH=src python3 -m marketcow.catalog_refresh_worker --config /absolute/operator.json
```

仅验证导入已有完整采集，不请求上游：

```sh
PYTHONPATH=src python3 -m marketcow.catalog_refresh_worker --config /absolute/operator.json --ingest-capture /absolute/new-capture
```

`catalog-refresh-worker.example.json` 是未启用候选，不是现网值。路径及 binary SHA 必须替换，全部预算必填。metric_unit=null 保持指标缺失，不猜币种。独占新采集/准备目录，不把旧 spool 复用成新证据。

worker 持有唯一 owner 锁，持久化各类到期时间与时钟高水位。回拨停止；首次失败记录错误并退出，无自动请求重试。重新运行属于明确调度行为；maximum_cycles 限制单次循环次数。原文采集固定 Gamma keyset/数字 ID 两种路径，无任意 URL、redirect、代理变更或服务操作。

## 事实、批次与发布

相同事实只更新核验时间，不生成事实事件。新增、关闭、重开、一般更新分别记录。记录、事件和 capture 水位同事务提交，失败不会提交半批。

原始库存逐市场保留采集时间；dirty 旧/新关系组才重新规范化，组数量/字节受限。原始库存和规范化输出仍流式完整扫描，发布仍需索引/备份；不宣称全过程 O(变化数)。未改变的市场不会因其他市场刷新而获得新时间。

生成不可变数据库/hash后，原子替换小型 current.json。旧 snapshot 持有旧 source，新请求读取新 source。数据库提交后指针发布失败，旧读面保持；下次成功发布可包含此前已提交变更。文件存在不单独代表发布成功。

## HTTP 及消费

现有 control server 显式增加 `catalog_publication_root` 才启用；保留现有专用 caller 和 catalog.read scope。既有 snapshot 路径继续 v1，增量消费者显式选择 v2 路径。

- GET `/v1/prediction-markets/polymarket/catalog/snapshot-v2?page_size=N`：动态 source 返回 `marketcow.polymarket.catalog-snapshot.v2`，原 v1 字段加 `change_sequence`。原 `/catalog/snapshot` 保持 v1，读取同一最新 source，不让已有严格 v1 消费者突然收到未知字段。
- GET `/.../catalog/page?snapshot_id=...&page_token=...&limit=N`：仍 v1 page，固定 source/page size/有效期。
- GET `/.../catalog/changes?after_sequence=S&limit=N`：`marketcow.polymarket.catalog-changes.v1`，字段 `events,capture_end_sequences,next_sequence,head_sequence,catalog_revision,has_more`。
- GET `/.../catalog/status`：`marketcow.polymarket.catalog-status.v1`，当前 generation/revision/sequence/count/capture 完成时间、coverage、上次失败与调度检查时间。不是 runner 活性或全市场新鲜证明。

event 字段为 `sequence,kind,market_id,record,capture_id,base_revision,catalog_revision,batch_start_sequence`。revision 是去掉自身字段后的 canonical event SHA256，连接前一条 revision；起点是 canonical 空数组 SHA256。整文件 hash 和 generation ID 独立，不拿事实 revision 冒充文件 hash。

先完整安装 snapshot，再从绑定 change_sequence 读取变更。一个 capture 可以跨页，暂存至 capture_end_sequences 指定终点才原子应用。`CatalogDeltaConsumer` 提供有界磁盘暂存/重启/连续性/hash 校验参考，不替代业务事实校验。

变更保留仅推进至完整 capture 边界。低于 floor 返回410 `catalog_resnapshot_required`，未启用返回503 `catalog_changes_unavailable`，全响应超限413 `response_size_exceeded`。不隐式切基线，不因 admitted 自动 activate。现有热切 runtime/root 不由目录 worker 改写；新 catalog 选池适配必须遵守其独立版本/身份要求。

## 覆盖与新鲜度

closed=false 不等于 accepting-orders，累计库存不等于本次全源遍历。coverage 明示 mixed/non-atomic、缺席需复核及规范化漏项。各记录 observed_at 仅代表该身份的实际接收；capture 完成时间不是所有记录时间。

纯 observation 更新不发事实事件，故增量 feed 不能证明所有市场刚重新核验。需要本次扫描新鲜证据时，取本次成功发布的完整 snapshot 并检查逐记录时间。不得用9/4旧 source 冒充今天新 capture。

## 资源边界

采集页/总字节、单次/总时限、执行次数、记录数、关系组、数据库主文件、保留事件数、artifact 总量准入与 free disk reserve 均显式。采集库单次 chunk 可以超过剩余保留预算，超限标截断。deadline 非硬实时保证。

运行前磁盘准入不是内核级总配额，WAL/临时文件和外部并发写入仍须观察。SQLite 主文件 cap 不是所有文件 cap。显式 retained_captures 限制采集批次数，只清理 worker 根内 UUID 命名的过期批次及准备件。目录版本按 snapshot_retention_seconds 清理，当前版本不删；宽限从退役时而非创建时计算，control 拒绝短于 reader TTL 的配置。清理后仍超 artifact 预算则停止。不得清除账户或其他业务历史；旧目录保留用于分页一致性，不属于服务回滚工程。

## 验证与部署边界

本地验证覆盖原文篡改/未完成拒绝、单写者/时钟回退/预算、定点更新保留其他身份时间、dirty group 重试/相同事实免规范化、两代 snapshot、HTTP鉴权、跨页批次/重启/篡改、事务失败与保留过期。

本地官方限量 GET（1页/2MiB/总20秒/请求15秒）在正文接收15秒超时。报告 `/private/tmp/marketcow-catalog-probe-IapzjP/capture/report.json`；未完成一页、未发布。该运行版本未保留部分正文，report retained_raw_bytes 名称过强；随后已修复截断原文保存，3个真实本机TCP测试通过，未再次请求官方。该失败不证明访问恢复、目录完整或生产吞吐。

尚未上传 U1、官方全遍历、启用 scheduler 或改现网 control 配置。正式交付需准确范围的安装/采集授权和真实运行证据，局部测试不替代这些条件。
