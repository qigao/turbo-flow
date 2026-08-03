# ADR：Flowie MQTT 集群采用接入节点与 Session Shard Owner 分离

## 状态

提议。本文定义集群能力的目标架构、状态所有权和故障语义；在本文列出的迁移门禁完成前，
现有单节点运行方式仍是唯一生产行为。

当前实现进度：`flowie_cluster` 内部协议内核已实现 version 1 stable shard hash、node/shard/connection
状态机、lease/capacity 溢出安全校验、owner token 与纯 fenced lease row 语义，并由
`test_flowie_cluster` 覆盖。endpoint 的 session 新建与持久化恢复已通过行为保持的单 shard 本地
runtime 获得 owner token，endpoint bindings 已接受 V1 前缀及未知尾字段。内部 PostgreSQL coordinator
已实现 server-time claim/renew/require/release，并由独占 worker 承担续租、重连和保守 deadline
self-fence。内部 cluster writer 已实现 ownership row 锁定、revision-checked MQTT fact、command receipt
与 outbox intent 的单事务提交，并以固定 command ID 和 BLAKE2b digest 确认丢失的 COMMIT 回包；有界
异步 fact worker 独占阻塞 libpq 连接，并在不确定提交后执行 reopen/confirm。其 live test 已编译，但
当前本机未配置 PostgreSQL，尚未运行。内部 peer wire v1 已实现固定头、严格上限、owned decode 和
command/reply/event 语义；内部 CoroNet link 已实现 fail-closed mTLS、证书身份授权、TLS channel binding
HELLO/ACK、有界 entries/bytes 队列及 drain-on-close，并由真实 mTLS 回环和 OS 线程 send/close 竞争测试
覆盖。内部 owner-lane adapter 已实现 copied command admission、执行时 ownership resolve、精确 epoch
fencing 和派生 reply；stale epoch 在调用 MQTT 状态执行器前返回 `TURBO_EBUSY`。adapter 还支持把已在
owner lane 暂存的 durable command 交给异步 PostgreSQL provider：completion 必须回投原 owner lane 后
才能执行 allocation-free finalize，durable success 才发布 staged MQTT 状态，durable failure 则丢弃 staged
状态并保持当前内存事实不变，随后生成 reply；close/drain 等待该 completion；单个 adapter 同时只允许一个 durable command in-flight，
其间后续 command 明确返回 `TURBO_EBUSY`，避免两个旧内存快照并行竞争 fact revision。post-CONNECT MQTT
command payload v1 已实现 version/header/total/packet/client ID 长度、MQTT version 与原始 packet 的有界编码，
并在 owner lane 执行前完成 operation/type 对应、完整 MQTT frame 和 typed packet 校验。`CONNECT_BIND`
payload v2 已实现 principal 的稳定字段编码和去凭据化 CONNECT：只保留 session expiry、规范 Client ID、
keepalive 与 Will 状态，明确剥离 username、password、Authentication Method/Data 及 edge-only properties。
v2 还携带有界源地址、实际 transport peer 地址和结构已校验但语义不解释的原始 PROXY v2 TLV。它们是
接入节点观察到的 advisory metadata，只在 owner command 边界用于审计、诊断或后续路由提示；owner 选择
仍只读取 PostgreSQL 派生 owner directory，TLV、edge proxy HashRing 和连接统计都不能授予 ownership 或
绕过 fencing。opaque TLV 全程按显式长度复制，不能使用 C 字符串语义。
公开 `flowie_endpoint_cluster_binding_t` 因 `connect` 增加 ingress 参数推进到 ABI v2，又因 PUBLISH
graph durable settlement 增加 `settle` callback 推进到 ABI v3；入口拒绝 ABI v1/v2，避免把旧函数指针
布局误判为兼容。该 cluster binding 尚未作为生产稳定接口发布，单节点 endpoint 及 bindings 的
size-gated 兼容行为不变。混合 ABI 节点不能滚动互通，升级必须先 drain 全部 cluster writer。
内部 staged executor 已把该 payload 连接到 session clone/create、规范 session record、fenced fact command、
`SESSION_BOUND` outbox intent 和 PostgreSQL fact worker；成功提交后由 owner-lane finalize 发布预构造状态，
失败或 completion 回投失败不会推进内存事实，后者触发 self-fence。返回 edge 的 `TFBR` version 1 payload
同时携带 accepted/close/session-present、owner route generation 与完整 CONNACK，避免把裸 MQTT packet 当作
跨节点绑定结果。`CONNECT_BIND` 持久化的 `TFSE` version 2 fact 已实现严格恢复：先在有界临时 registry
中逐条校验 TFCB、Client ID、security mode、canonical session record、revision 以及 session ID 唯一性，
任一记录失败都会污染并拒绝整个 recovery publish；全量成功后才以一次 registry 交换发布 inactive owner，
同时把下一个 session ID 推进到已恢复最大值，避免半恢复 shard 或 route ID 重用。内部 PostgreSQL fact
scan 已用单条 SQL 在同一 statement snapshot 中验证精确 `(node_id, boot_id, owner_epoch)`、node/shard lease
和读取指定 shard 的 TFSE；`max_records + 1` 在调用 visitor 前判定容量溢出，row view 只借用至 visitor 返回。
启动组合只允许处于 `RECOVERING` 的 lease，先把完整临时 registry 原子发布，再以同一 owner token 激活；
激活时 worker 会重新检查本地保守 deadline 与 token，失败时 shard 保持不可服务。
内部 NO_INSTALL shard runtime 已把 lease worker、只读 recovery scan connection、异步 fact worker、session
registry、peer-owner adapter 以及可选 TFTE PostgreSQL source/dispatcher 收归一个生命周期 owner；只有 claim、
全量 recovery、lease activation 和全部 adapter 创建成功后才返回可提交对象。统一 receive 入口把 COMMAND 交给
peer owner，把 REPLY 交给当前 shard 的 dispatcher。它借用 CoroNet execution，要求调用方在 execution 停止前
先关闭 runtime；关闭先设置本地 fence 并停止 dispatcher/peer admission，再释放 lease，依次 drain dispatcher
和 owner completion，最后停止 fact worker，避免异步回调进入已经销毁的 owner lane、source 或 session registry。
内部 NO_INSTALL connection-edge `CONNECT_BIND` port 已实现 PostgreSQL 派生 owner token 解析、去凭据化
command 构造、有界 pending entries/bytes、boot-scoped correlation ID、严格 owner/epoch/connection reply
匹配，以及 reply/close 回投 edge CoroNet owner lane；本地路径已用真实 peer-owner 与 session-bind 组合测试
验证，socket 侧代码不直接取得 session registry 指针。该 port 也已支持 post-CONNECT command：每次 admission
前重新读取 PostgreSQL 派生 owner token，并要求其与 CONNECT 安装的完整 token 精确匹配，ownership 变化时
返回 `TURBO_EBUSY` 而不隐式改投。成功 reply 使用独立 `TFRP` version 1 action，显式表达可选 MQTT packet、
close-after-send、owner 已完成的 settlement point 或 no-op；edge 只消费 socket action，不据此修改 session。
异常 socket 关闭已使用独立 `TFCL` contract 提交 `CONNECTION_LOST`，并以 fenced `TFSE + TFLE` transaction
持久化 inactive/Will-pending 状态。跨 edge CONNECT takeover 会在同一 fenced transaction 中把旧 binding 写成
`TFTE` intent；旧 edge 的内部 port 已能验证独立 `TFTC` contract、当前 PostgreSQL owner token 与精确旧
connection generation，再调用 endpoint-owned 幂等 close callback。内部 dispatch adapter 已能把单 mutation
的权威 TFTE outbox row 映射为当前 owner 签发的精确 TFTC，并只接受反向 route、correlation、generation
完全匹配的成功 TFRP no-op 作为 settlement。其单 worker dispatcher 通过当前 PostgreSQL owner 锁定并读取
最早的未发布 TFTE、增加 attempt count、单 in-flight 发送 TFTC，并仅在精确成功 TFRP 后按完整 event identity
设置 `published_at`；发送拒绝、transport failure、peer error 和 reply timeout 都保留 durable row 并有界重试，
不复制成无界内存队列。settle 的连接错误只 reopen PostgreSQL source 并重试 settle，不重复发送已确认的
socket close；进程崩溃或 owner 切换会由新 owner 至少一次重放，旧 edge 的精确 generation 幂等关闭吸收重复。
内部 peer link registry 已按精确 `(target_node_id, target_boot_id)` 把 dispatcher send 路由到借用的 link，并以
固定 `max_links/max_inflight_sends`、send lease、两阶段 unregister 和 close/drain 约束 link/回调生命周期；首个
返回 `TURBO_EBUSY` 的 unregister 已停止该 link 的新发送，允许 transport close，accepted send 完成后必须重试
unregister 成功才能销毁 link。真实 mTLS transport 测试已覆盖该组合。公开 cluster binding、endpoint 对该 port
的注入前，内部 peer listener 已在借用的 CoroNet execution 上完成 mTLS listen、认证后 ACTIVE 注册、精确
node/boot 注销和 server/link close-drain；真实回环测试覆盖 accepted socket 由 CoroNet 销毁后才释放 listener。
CoroNet 的公开 server admission limit 从 raw accept 起覆盖 TLS/WSS handshake、handler 与 close completion；
peer listener 把 `max_connections` 同时用于该握手前门禁和认证后的 slot 上限，满额连接在 TLS 前立即关闭。
内部 connector 已对精确 remote node/boot 维持单 link，网络错误按有界间隔重连，
认证/协议错误则 terminal fail-closed；固定由字典序较小 node 发起，listener 同时拒绝反向发起，避免双连接各自
成为本地 winner。真实双 execution 测试覆盖先失败后重连、双 router COMMAND 路由和 close/drain。内部 TFLE
consumer 已按当前 PostgreSQL owner 锁定并读取最早未结算事件，严格解码 connection/session generation、edge
identity 和预期 fact revision；单 worker、单 in-flight apply 保持事件存储到异步完成，只有 apply 明确证明动作已
持久化或事件已失效后才 settle。apply admission/执行失败保留 outbox 并有界重试；settle 连接不确定只 reopen source
并重试 settle，不重复已经完成的 lifecycle action。outbox 的 PostgreSQL `created_at` 已作为权威 epoch 秒进入
TFLE view，延迟重放不会从恢复时刻重新延长 Will/expiry。内建 owner-lane applicator 会先按该时间作纯判定：
Will 到期时以同一 fenced transaction 清除 session fact 中的 Will 并产生 TFPE，保留原 TFLE 继续驱动 expiry；
session 到期时先删除 fact，再从本地 registry 和派生 subscription index 删除。durable completion 必须回投
CoroNet owner lane；不确定的 `EALREADY` 会 self-fence 并由恢复重建内存，不猜测提交结果。shard runtime 以
非零 poll/retry 配置内建拥有 lifecycle source、owner applicator 和 dispatcher；关闭时先停止 dispatcher，
再 drain owner completion，最后释放 source、session bind 和 fact worker。PostgreSQL schema v3 已在 v2 的有界
`advertised_endpoint`、单调 membership revision 基础上加入 shared-selection 事实；repeatable-read snapshot
会按确定顺序复制 READY 节点，membership controller 再把 snapshot
与当前 outgoing connector identity 归并成有界、确定性的 REMOVE-before-ADD 操作。该路径拒绝 stale revision、
同 revision 分歧、本机 boot 不一致、反向连接和超限 endpoint，并且不会借用 SQL result 生命周期。

PUBLISH 第一段也已落地：publisher owner 对 QoS 0 使用 fenced event-only transaction，对 QoS 1/2 将 sender
inflight fact 与 `TFPE` broadcast intent 原子提交；requested settlement 为 `received` 时在同一 staged owner 中
完成 PUBACK/PUBREC。每个 session bind 维护只从 authoritative session fact 重建的 topic trie；SUBSCRIBE、
UNSUBSCRIBE 与 clean-start 会在 durable 提交前构建 staging index，提交成功后与 owner 一起无失败交换，失败则
两者均不推进。普通重叠订阅按 session 合并最高 QoS 与 subscription identifiers；共享订阅也进入派生索引。
每个 shard 对每个匹配的完整 `$share/...` filter 只提出 session ID 最小的本地 eligible candidate；PostgreSQL
`cluster_shared_selection` 再以源 event identity + shared filter 做全局唯一仲裁。首个 fenced transaction 原子写入
winner、目标 fact 与 outbox，精确赢家重放和其他 shard 的输家候选均返回 `TURBO_EALREADY`，不得推进本地轮询
游标。恢复发布会从 authoritative session fact 重建普通及共享订阅索引。PostgreSQL outbox row 可编码为独立 `TFBE` broadcast
envelope，携带稳定 `(command_id,event_index,source shard/epoch,fact revision)` 并嵌套严格验证的 `TFPE`，因此
Redis Stream ID 不进入 MQTT 去重身份。`TFPE` version 2 还携带源 owner 接收时的 epoch seconds，使目标
可按原始接收时刻递减 MQTT 5 Message Expiry；decoder 保留 v1 读取兼容，但不会为旧事件虚构时间。
PostgreSQL 已增加有界 `cluster_event_dedupe` 与 `cluster_shared_selection`：普通 target 的稳定源事件 identity、
目标 shard/session 和 TFBE digest 写入前者，共享 target 的全局 winner 写入后者；两者都与目标 fact/outbox 在
目标 fencing transaction 内原子提交。exact replay 返回幂等成功，identity/digest 分歧返回冲突。两表合计受同一
`max_dedupe_records` 硬上限约束，共享 winner 不重复占用普通 target dedupe。事务现支持
在线 `FACT_AND_EVENT`、离线 `FACT_ONLY`、QoS 0 source `EVENT_ONLY` 和终结丢弃 `dedupe-only`，容量只统计
实际 receipt/dedupe/outbox。`target_session_id>0` 表示单 session 终结，`target_session_id=0` 是该 target shard
完成全部目标后的权威 ACK；source owner 只有观察到固定 shard 集合的全部 ACK 才可 settle 原 publish outbox。
目标 socket intent 已定义为 `TFDA`，携带精确 edge boot、connection/session
generation 和嵌套连续 `TFEA`。edge action 的 `TFEA/TFEK` sequence contract 与 owner-lane apply 已实现，
会按 target/listener/fencing/generation/version 拒绝 stale 或 sequence gap，并幂等确认 replay。

source broadcast dispatcher 现已实现：每个配置了 publish port 的 shard runtime 拥有独立 PostgreSQL source，
把 publish outbox 编码为 TFBE 后追加到有界 broadcast bus；XADD 回复不确定时仍查询 PostgreSQL shard ACK，
未完成则按间隔重发同一 TFBE。只有 `target_session_id=0` 的 exact marker 数等于固定 `shard_count` 才在当前
owner fence 下 settle 原 outbox。runtime close 会先停止并 drain 此 dispatcher，再释放 lease。Redis adapter
提供线程安全、有 payload/MAXLEN 上限且不暴露 Stream ID 的 publisher port。应用层内部 Redis bus adapter
现已把它映射为 source publish port，并为每个本地 ACTIVE shard 创建独立、稳定命名的 consumer group/owner；
claim token 原样映射到 runtime ACK/requeue，bus 在 target 未销毁时拒绝销毁。group 从 ID `0` 创建，owner
先重放同一稳定 consumer 的 PEL，再读取新记录；Redis identity 与 payload view 均不进入 MQTT 事实。

target transaction planner 也已落地第一段：普通订阅 target 可在 owner lane 上生成 stable command ID 与
PostgreSQL dedupe；所有在线 delivery（包括 QoS 0）都原子写 TFSE+TFDA，离线持久 QoS 1/2 只写 TFSE，
离线非持久或 QoS 0、以及过期消息只写 dedupe。edge action sequence 归属于当前 session binding，只有
durable OK 才与 staged session 一起推进；dedupe replay 的 EALREADY 会丢弃新 staged copy，不能再次分配
packet ID 或推进 sequence。
每 shard 的零 mutation 完成 ACK command 也已有稳定构造函数。target consumer orchestration 现也已接入
shard runtime 的可选端口：transport claim 的 borrowed TFBE 在入口立即复制到 CoroNet owner lane，普通 target
按固定顺序逐个提交 fenced PostgreSQL transaction，全部完成后再写 shard marker；只有 marker durable OK 或
exact EALREADY 才 ACK 原 transport token。apply 可重试失败会 requeue，XACK 结果不确定只重试同一 token，
不会把 PEL、Stream ID 或本地计数提升为 MQTT 事实。runtime close 会停止 claim、drain 已接收 apply，再关闭
target owner 与 fact worker。

TFDA peer dispatcher 现已实现：每个配置了 delivery port 的 shard runtime 拥有独立 PostgreSQL source，
在当前 owner fence 下读取最早未结算 TFDA，复制出嵌套 TFEA 并构造精确 edge boot、connection generation、
owner epoch 与稳定 correlation 的 `EDGE_ACTION`。transport completion 只证明发送完成；dispatcher 必须继续
等待反向 route 全匹配且 sequence 相同的 `TFEK`，之后才按完整 outbox identity 结算 PostgreSQL。发送拒绝、
transport failure、peer operational error 与 reply timeout 都重放同一不可变动作；TFEK 已成功而 settle 连接
结果不确定时只 reopen source 并重试 settle，不重复写 socket。运行时按 reply operation 将 `MQTT_REPLY` 与
`EDGE_ACTION_ACK` 分别路由到 takeover 和 delivery dispatcher，关闭时在 peer/source 销毁前停止并 drain 两者。

应用层内部 cluster node composition root 已把单一 node router、Redis bus、本地 shard runtime/consumer target、
peer listener 和初始 connector 集合组成一个 generation。创建时按 shard ID 和 remote node ID 确定排序，拒绝
重复本地 shard、重复 connector 和 PostgreSQL/router/Redis/peer identity 分歧；所有 shard 在 peer admission
启动前完成 Redis/router 端口注入、ownership claim/recovery 和 router 注册。关闭时先 close/drain listener 与
connector，使精确 peer link 从 router 注销；再 close/drain router，注销并 self-fence shard；销毁时严格按
peer transport、shard runtime、Redis target、router、Redis bus 的依赖逆序释放。peer drain 失败不会提前关闭
router，调用方可在同一不可逆 generation 上重试 close。root 也能导出严格排序的当前 connector topology view，并由唯一 topology owner
应用 membership plan：REMOVE 必须完成 connector close/drain/destroy 后才执行 ADD；中途失败保留旧
revision，重试时 exact missing REMOVE 和 exact existing ADD 幂等，只有整份 plan 成功才推进 applied revision。
可选 membership runtime 在专用 TurboUtils worker 线程上独占同步 PostgreSQL coordinator，周期执行
expire、bounded snapshot、plan 和 apply，使阻塞 libpq 不进入 CoroNet owner lane。`EIO` 会销毁 coordinator，
下一轮异步 reopen；`ETIMEDOUT`/`EBUSY` 重试同一权威流程，配置、容量或协议错误则进入可查询的 FAULTED，
不会静默降级。composition root 关闭时先请求 worker 停止，并在调用方 deadline 内等待退出和 join；超时后
generation 保持存活且可重试关闭，只有 join 成功才停止 peer transport，因此 topology apply 与
connector/listener teardown 不并发。

server application 现进一步拥有完整 cluster generation root：root 创建并验证单一 CoroNet owner lane，把
同一 execution 注入 local shard、listener、初始及动态 connector 和 endpoint binding；PRIVATE/OWNED lane
只以 borrowed context 暴露给 MQTT endpoint，避免两个组件分别驱动同一 context。启动固定为 owner directory
首次完整 PostgreSQL snapshot、peer admission、Control、MQTT worker；关闭固定为 MQTT worker、endpoint edge、
node router/peer/shard、owner refresh，并在 worker 销毁后才停止 execution。endpoint 尚有连接时 close 返回
`EBUSY`，不会越过它拆除 node truth，调用方可在同一 generation 上重试。执行线程创建后先经过有界 lane
barrier，避免线程尚未进入 loop 就进入 stop/join 的启动竞态。该 application binding 当前仅能由内部 C config
以类型化结构启用，非集群 server 行为不变。

可信 PROXY v1/v2 现已绑定 socket transport 前置读取：只有显式 CIDR 信任的 transport peer 可以提供源地址；
v2 raw TLV 以有界二进制数据随 `CONNECT_BIND` v2 传到 session owner，v1 不产生 TLV。这些字段只用于
审计、统计和路由提示，不参与 owner 选择或 fencing。plaintext TCP endpoint 已允许在 MQTT parser 前强制
消费可信 PROXY header，供 HAProxy MQTTS termination 使用。尚未完成的生产门禁是 cluster YAML/CLI 公开
配置解析与 PostgreSQL/Redis/HAProxy live 故障演练。故当前状态仍为“提议”，不能启用多节点生产模式。

## 决策摘要

Flowie MQTT 集群固定为以下四层：

```text
MQTT client
    |
    v
HAProxy MQTTS edge
    |  只负责 TLS termination、Client ID 接入偏好、连接统计和 PROXY v1 源地址
    v
Flowie connection edge
    |  独占已接受 socket、协议解析器和该连接的发送队列
    |
    |  CoroNet mTLS：有界、版本化的 command/reply/event
    v
Flowie session shard owner
    |  在当前 fencing epoch 内独占该 shard 的 MQTT 可变状态
    |
    +--> PostgreSQL：membership、ownership、fencing、MQTT 事实、outbox
    |
    `--> Redis：可选的派生事件流、路由索引和缓存
```

核心决策如下：

- HAProxy 不是 MQTT 集群事实源。consistent hash、连接数和健康统计都不能授权
  MQTT 状态写入。
- 接受连接的 Flowie 节点是 `connection edge`，在连接整个生命周期内独占 socket。socket 不共享，
  也不在节点间迁移。
- 每个 MQTT 状态 shard 在任一时刻只有一个有效 owner。owner 由 PostgreSQL lease 选出，
  每次取得 ownership 都推进单调 `owner_epoch`。
- 所有 MQTT 事实修改必须携带并验证 `owner_epoch`。PostgreSQL 在同一事务中校验 fencing、
  修改事实并写入 outbox，避免 PostgreSQL 与 Redis 双写。
- Redis 只承载可重放的派生事件、缓存或路由加速。Redis 丢失、重复或延迟事件时，系统从
  PostgreSQL outbox 和 cursor 恢复。
- HAProxy 按非空 Client ID 把新连接路由到偏好节点，但 Flowie 必须根据 CONNECT 中的规范 Client ID
  自行计算 shard，并以 PostgreSQL ownership 为准。路由命中只减少一次跨节点转发。

该设计不把 HAProxy、Redis 或 PostgreSQL 的集群能力误当成 Flowie 自身的集群协议。外部集群只提供
可用性基础；Flowie 仍负责连接归属、MQTT 状态单写、fencing、消息路由和故障恢复。

## 背景与仓库事实

- `事实`：当前 Flowie endpoint 同时拥有 CoroNet server/socket、client、route、session、
  subscription index 和 retained cache。`flowie_endpoint_bindings_t` 目前只注入 security 与
  persistence，worker runtime 在 composition root 中把 persistence 绑定给 endpoint。
- `事实`：当前 route 的 `owner_instance_id` 是进程内 endpoint instance ID。入站与出站 route lookup
  都拒绝非本 instance 的 owner；持久化 session、subscription 与 route index 只在 endpoint 启动时
  scan 并重建。
- `事实`：现有 Record Store 已提供 revision-checked atomic batch，revision 不匹配返回
  `TURBO_EBUSY`。`test_flowie_session_store_faults.c` 已验证 stale writer CAS、commit reply 丢失恢复
  和 stale expiry cleanup 被拒绝。
- `事实`：现有 HTTPS Auth ADR 明确规定 `remote_address` 只来自 CoroNet 直接 socket peer，不解析
  PROXY protocol。
- `推论`：进程内 `owner_instance_id` 和启动时索引重建能表达单进程 owner，不能独立证明节点故障后的
  跨进程 owner epoch；现有 revision conflict 语义可以作为 fenced commit 的基础。
- `推论`：集群前置 HAProxy 后，如果不新增显式可信代理边界，Auth 只能看到 HAProxy 地址。

因此本 ADR 会影响 endpoint composition、route/session 所有权、持久化事务、节点间传输、
源地址契约、settlement 和部署配置，属于跨模块架构变更。

## 不变量与事实源

以下不变量必须同时成立：

1. 对同一 `(cluster_id, listener_id, shard_id)`，任一时刻最多只有一个未过期 owner epoch。
2. 只有 owner row 中匹配的 `(node_id, boot_id, owner_epoch)` 可以提交该 shard 的 MQTT 事实。
3. connection edge 可以不是 session shard owner；这不会转移 socket 所有权。
4. MQTT 事实修改与相应 outbox intent 在一个 PostgreSQL 事务中提交。
5. PostgreSQL membership、ownership、epoch、MQTT 事实和未完成 outbox 是主事实源。
6. HAProxy 状态、Redis Stream、路由索引、本地 cache 和内存 subscription index 都是可重建派生状态。
7. PostgreSQL 不可达时不能取得或延长 ownership。节点在保守本地 lease deadline 前自我隔离。
8. 同一 namespace 不允许 legacy 单节点 writer 与 cluster writer 混合运行。
9. `cluster.enabled: false` 时保持现有单节点用户可见行为，不隐式启用远程路由或外部协调。

“独占 MQTT 状态”包括：

- Client ID 对应的 session、subscription、QoS inflight、packet ID、Will、expiry 和 offline queue；
- connection generation 与当前 edge route；
- retained message、delivery event、dedupe/cursor 等全局 MQTT 事实。

session 相关事实按 Client ID 分 shard；retained message 按规范 topic 分 shard。两类 key 使用同一
ownership 表和 epoch 协议，因此不存在绕过 shard owner 的 retained 特例。

## 候选方案

| 方案 | 一致性 | 可用性与复杂度 | 结论 |
|---|---|---|---|
| 每个 Flowie 节点加载全部状态，依靠 Redis/PostgreSQL 集群 | 多个节点仍会同时处理 takeover、Will、expiry 和 offline delivery | 缓存失效和数据库 HA 不能提供业务单写 | 拒绝 |
| HAProxy consistent hash 作为 Client ID owner | ring 变化和节点健康视图延迟会改变路由；也不能约束旧 socket | 接入简单，但错误地把路由选择当写权限 | 拒绝 |
| 每个 Flowie 节点内嵌 Raft | 能协调 ownership，但仍需设计 socket、MQTT shard、数据面和持久化 | 增加 quorum、日志、snapshot、成员变更和运维系统 | 当前不采用 |
| PostgreSQL 协调的 shard owner，HAProxy/Redis 为派生层 | ownership 与事实写入可在单事务内 fencing；旧 owner 可确定拒绝 | 需要节点间传输、lease 和 outbox | 采用 |

内嵌 Raft 只有在 PostgreSQL lease 已被实测为性能或故障恢复瓶颈、且产品需要脱离外部协调数据库时，
才重新评估。届时 Raft 只替换 membership/ownership log，不改变 connection edge 与 session shard
owner 的边界。

## Shard 与节点身份

### 稳定身份

- `cluster_id`：隔离不同集群和持久化 namespace。
- `node_id`：部署分配的稳定逻辑节点 ID。
- `boot_id`：每次进程启动生成的随机 ID，防止同一 `node_id` 的旧进程恢复后继续写入。
- `listener_id`：隔离不同 MQTT listener 的 Client ID namespace。
- `connection_id`：仅在一个 `boot_id` 内唯一。
- `connection_generation`：同一 Client ID 每次成功绑定时单调推进。
- `owner_epoch`：同一 shard 每次 claim 时由 PostgreSQL 单调推进。

任何跨节点命令至少携带：

```text
protocol_version
cluster_id
source_node_id + source_boot_id
target_node_id + target_boot_id
listener_id + shard_id + owner_epoch
correlation_id
connection_id + connection_generation（连接相关命令）
payload_length + payload
```

接收方必须在读取 payload 前验证版本、长度、集群和目标身份；未知必需字段或超限 payload 直接失败，
不能猜测兼容。

内部 wire v1 使用 112-byte network-order 固定头，随后依次编码 cluster、listener、source node、target
node 与 payload。固定头包含总长及每个可变字段长度，因此接收端在分配 payload 前即可拒绝未知版本、
未知 flags、字段超限和不一致总长。解码结果拥有单份连续存储，跨 coroutine queue 不保留 CoroNet receive
buffer view。控制帧禁止夹带 shard/epoch/connection 状态；command/reply/event 强制携带 owner epoch 与
correlation ID，只有连接相关 operation 强制携带 connection ID/generation。

### Shard 计算

集群创建时固定 `shard_count`：

```text
session_shard = stable_hash(cluster_id, listener_id, canonical_client_id)
                % shard_count

retained_shard = stable_hash(cluster_id, listener_id, canonical_topic)
                 % shard_count
```

hash 算法、输入编码和 seed 是 schema version 的一部分，所有平台必须通过同一组 golden vectors。
`shard_count` 运行中不可修改；修改它是显式数据迁移，不是配置热重载。

READY 节点集合上的 rendezvous hash 只计算 preferred owner。preferred owner 必须再通过 PostgreSQL
claim transaction 才能成为真实 owner。membership revision 不一致时，hint 可以失效，但不能产生
写权限。

## 状态机

### 节点状态

```text
STARTING -> SYNCING -> READY -> DRAINING -> OFFLINE
                         |          |
                         `--------> EXPIRED
```

- `STARTING`：配置和本地依赖检查完成，但不接流量。
- `SYNCING`：注册 membership、建立 peer link、读取 ownership 与 event cursor。
- `READY`：HAProxy 可接入新连接，也可 claim shard。
- `DRAINING`：不接新连接、不 claim 新 shard，迁移现有 shard 并排空队列。
- `OFFLINE`：已释放 ownership 和资源。
- `EXPIRED`：lease 过期后由协调视图判定；该 boot ID 不能恢复成 READY，只能以新 boot ID 重启。

### Shard 状态

```text
UNASSIGNED -> CLAIMING -> RECOVERING -> ACTIVE -> DRAINING -> RELEASED
                    \         |           |          |
                     `--------+-----------+----------> FENCED
```

- `CLAIMING`：只执行 PostgreSQL claim，不处理 MQTT 命令。
- `RECOVERING`：提升本地 fence、加载事实、重放未完成 outbox/cursor 并重建派生索引。
- `ACTIVE`：接受与当前 epoch 匹配的命令和事实写入。
- `DRAINING`：停止接收新的 session，完成已接受事务和 delivery。
- `FENCED`：停止所有写入，拒绝后续命令并关闭或重定向相关 connection。

任一 owner 操作收到 `TURBO_EBUSY`/epoch mismatch 时必须立即进入 `FENCED`，不能把它当成普通数据库
冲突继续以当前 owner 身份重试。

### 连接状态

```text
ACCEPTED -> AUTHENTICATING -> BINDING -> ACTIVE -> CLOSING -> CLOSED
                               |           |
                               `----------> FAILED
```

connection edge 是该状态机和 socket 的唯一 owner。session shard owner 只能返回类型化 action，例如
发送 CONNACK、发送 MQTT packet 或关闭原因；不能持有 edge 的 socket 指针。

## PostgreSQL ownership 与 fencing

### 逻辑表

表名是逻辑契约，物理 schema 可由 PostgreSQL adapter 按项目命名规范实现：

```sql
flowie_cluster_config(
  cluster_id primary key,
  schema_version,
  hash_version,
  shard_count,
  membership_revision,
  created_at
)

flowie_cluster_node(
  cluster_id,
  node_id,
  boot_id,
  state,
  advertised_endpoint,
  lease_until,
  revision,
  primary key(cluster_id, node_id)
)

flowie_cluster_shard(
  cluster_id,
  listener_id,
  shard_id,
  owner_node_id,
  owner_boot_id,
  owner_epoch,
  lease_until,
  revision,
  primary key(cluster_id, listener_id, shard_id)
)

flowie_cluster_command_receipt(
  cluster_id,
  listener_id,
  command_id,
  command_digest,
  shard_id,
  owner_epoch,
  mutation_count,
  created_at,
  primary key(cluster_id, listener_id, command_id)
)

flowie_cluster_fact(
  cluster_id,
  listener_id,
  record_kind,
  record_key,
  shard_id,
  revision,
  owner_epoch,
  value,
  primary key(cluster_id, listener_id, record_kind, record_key)
)

flowie_cluster_outbox(
  cluster_id,
  listener_id,
  command_id,
  event_index,
  shard_id,
  owner_epoch,
  event_type,
  record_kind,
  record_key,
  fact_revision,
  payload,
  payload_size,
  created_at,
  published_at,
  attempt_count,
  primary key(cluster_id, listener_id, command_id, event_index)
)

flowie_cluster_event_dedupe(
  cluster_id,
  listener_id,
  target_shard_id,
  target_session_id,
  source_command_id,
  event_index,
  source_shard_id,
  source_owner_epoch,
  source_fact_revision,
  event_digest,
  created_at,
  primary key(cluster_id, listener_id, target_shard_id, target_session_id,
              source_command_id, event_index)
)

flowie_cluster_shared_selection(
  cluster_id,
  listener_id,
  source_command_id,
  event_index,
  shared_filter,
  source_shard_id,
  source_owner_epoch,
  source_fact_revision,
  event_digest,
  target_shard_id,
  target_session_id,
  created_at,
  primary key(cluster_id, listener_id, source_command_id, event_index, shared_filter)
)

flowie_cluster_cursor(
  cluster_id,
  listener_id,
  shard_id,
  consumer_kind,
  last_event_id,
  revision,
  primary key(cluster_id, listener_id, shard_id, consumer_kind)
)
```

command receipt 是 lost-COMMIT 与 command ID 冲突的权威确认记录；digest 覆盖 owner token、shard key、
fact mutation 和 outbox intent。MQTT fact record 包含 `shard_id`、fact revision 和最后修改它的
`owner_epoch`，外部格式版本保留在编码后的 fact value 中。
schema v3 只允许在没有任何 `cluster_node` membership row 时从 v1/v2 迁移；升级前必须先 drain 并停止全部
cluster writer、清空 membership，再由单个 migrator 建表。存在活动或残留 node row 时 fail fast，禁止混合
schema writer。
所有可增长字段、payload 和 batch 都必须有配置上限。

### Lease 时钟

数据库使用 PostgreSQL server time 计算 `lease_until`，不比较不同节点的 wall clock。节点收到续租结果后，
按本地 monotonic clock 计算更保守的失效点：

```text
local_deadline =
    renew_request_start_monotonic
  + returned_lease_validity
  - safety_margin
```

配置启动时必须满足：

```text
renew_interval
+ worst_case_db_latency
+ safety_margin
< lease_ttl
```

不满足时 fail fast。暂停、时钟跳变或调度延迟导致越过 `local_deadline` 时，节点先 self-fence，再尝试
恢复协调连接。

### Claim、renew 与 release

claim transaction 只允许：

- shard 当前无 owner；
- 当前 lease 已按 PostgreSQL server time 过期；
- 当前 owner 以完全匹配的 `(node_id, boot_id, owner_epoch)` 主动重领。

成功 claim 必须原子推进 `owner_epoch`，不能复用旧值。renew/release 必须同时匹配
`node_id`、`boot_id` 和 `owner_epoch`；受影响行数不是 1 时返回 fencing conflict。

不使用 session-scoped PostgreSQL advisory lock 表示 ownership。连接池换连接、网络断开和 session
结束会改变 advisory lock 生命周期，而显式 lease row、epoch 与事务检查具有可审计的业务语义。

### Fenced commit

每个 MQTT 写命令执行一个事务：

1. 锁定或原子检查 shard owner row；
2. 验证 node lease 未过期，且 `(node_id, boot_id, owner_epoch)` 完全匹配；
3. 校验 fact revision、connection generation 和 MQTT 不变量；
4. 修改 MQTT fact；
5. 插入唯一 `event_id` 的 outbox intent；
6. 一次提交。

事务任一步失败都不产生可见修改。revision/epoch 冲突映射为 `TURBO_EBUSY`，容量达到上限映射为
`TURBO_ENOSPC`，依赖不可用映射为明确 I/O/timeout 错误。

COMMIT reply 丢失时，调用方使用同一 command/event ID 和预期 revision 查询事实或 outbox 来确认，
不生成新 ID 猜测重试。这与当前 session store fault tests 的 stale writer 和 lost reply 语义一致。

### PostgreSQL HA 前提

ownership schema 要求一个可确定的 writable primary。部署必须选择能满足 ownership 表恢复点要求的
同步持久化策略；异步 standby 可能回退已确认 epoch，不能宣称提供严格 fencing。

Flowie 在 primary 切换期间：

- 暂停 join、claim、renew 和 fenced commit；
- 连接池重新解析并建立到当前 read-write primary 的连接；
- 重新读取 node lease、shard owner 和 epoch；
- 只有仍匹配的 shard 才能恢复 ACTIVE。

control PostgreSQL pool 已由独立 reconnect worker 对 DEAD slot 执行异步 reopen，acquire 在容量不足时唤醒
该 worker，并在同一 acquire deadline 内等待可用 slot；close/drain 也等待进行中的 reopen。cluster 发布门禁
仍需用真实 primary failover 验证重新解析 read-write primary、全部 slot 恢复和 owner lease fail-closed，不能只以
单元状态机代替 HA 验证。

## HAProxy 边界

### 连接与统计

HAProxy 的 frontend/backend/server 连接数、字节数、错误率、健康检查和状态可导出到监控或 Control UI。
Flowie 接收这些数据时必须标记 `source=haproxy`，仅用于：

- 运维观察；
- HAProxy 自身的新连接负载均衡；
- 节点 drain 前后的流量确认。

这些统计不参与 shard claim、fenced commit、session takeover、Will、expiry 或 settlement。

### 源地址

HAProxy 在 `8883` 终止公网 TLS，再向 Flowie plaintext MQTT listener 发送 PROXY protocol v1 和原始
MQTT 字节。Flowie listener 必须显式配置可信代理能力：

```yaml
adapters:
  mqtt.endpoint:
    kind: flowie_endpoint
    config:
      transport: tcp
      trusted_proxy_cidrs: '<trusted-haproxy-cidr>'
      proxy_header_timeout_ms: <bounded>
      proxy_header_max_bytes: <bounded>
```

配置值由部署提供并在启动时校验；上例不是默认值。只有直接 socket peer 命中 CIDR 才解析 PROXY
header。未信任 peer、缺失 header、版本错误、超时、长度超限或地址族不匹配都 fail closed。

HAProxy termination 模式下 PROXY header 位于 plaintext MQTT CONNECT 前，必须由 CoroNet socket admission
在 MQTT parser 之前消费。Auth request 中的
`remote_address` 随该能力版本化，保留直接 peer 地址作为诊断字段，避免代理链身份混淆。

内部 parser 对 v1 执行 107-byte 上限、严格 CRLF、TCP4/TCP6 地址和端口校验；对 v2 执行完整
signature/version/command/length、address block 与 TLV 边界校验。两者都支持碎片输入并精确返回 consumed
bytes；LOCAL/UNKNOWN 不信任被忽略的 address block，缺失 header、长度超限、非法地址族和 malformed TLV
均 fail closed。CoroNet admission 保留同次读取中的后续 MQTT bytes，因此 endpoint 可以在不丢失或重复读取
CONNECT 的前提下消费 header。bundled HAProxy backend 使用 `send-proxy`，固定发送 v1。

`X-Forwarded-For` 不适用于 raw MQTT。Flowie 不接受客户端自报源地址。

### 路由提示

HAProxy 3.2 的 `mqtt_is_valid` 与 `mqtt_field_value(connect,client_identifier)` 从解密后的首个 CONNECT 校验并
提取 Client ID。bundled 配置只接受有效且 Client ID 非空的 CONNECT，在有界 inspection delay/buffer 内完成
判定，然后以 `balance hash var(sess.mqtt_client_id)` 和 `hash-type consistent` 选择 connection edge。解析失败、
超时、buffer 耗尽或空 Client ID 都 fail closed，不回退到 source hash 或 round-robin。

标准 PROXY v1 只承载源/目标地址和端口；HAProxy 不把它解析出的 Client ID 再编码成自定义字段。
Flowie 必须从原始 CONNECT 独立解析规范 Client ID 并重新计算 shard，避免 proxy parser 成为认证或
ownership 输入。

HAProxy hash ring 变更只影响新连接。已有连接继续由原 connection edge 独占，直到 MQTT takeover、
客户端断开或节点 drain。

官方能力边界：

- [HAProxy 3.2 Configuration Manual：MQTT converter、hash 与 send-proxy](https://docs.haproxy.org/3.2/configuration.html)
- [HAProxy 3.2 Management Guide：master-worker reload](https://docs.haproxy.org/3.2/management.html)

## 节点间传输

节点间 transport 使用 CoroNet TLS endpoint 和 mTLS。证书身份必须绑定
`cluster_id/node_id/boot_id`，并由集群专用 CA 或现有 key provider 提供；密钥不进入 YAML 或日志。

第一版命令集保持有界：

- `CONNECT_BIND`：建立或 takeover session，返回 connection generation 与 CONNACK action；
- `PACKET_COMMAND`：PUBLISH、SUBSCRIBE、UNSUBSCRIBE、ACK、DISCONNECT 的所有权命令；
- `CONNECTION_LOST`：edge 失联通知；
- `TAKEOVER_CLOSE`：要求旧 edge 关闭指定 generation；
- `MQTT_REPLY`：发送 packet、close 或 settlement action；
- `EVENT_DELIVER` / `EVENT_ACK`：跨 shard delivery 与幂等确认。

ordering 只保证在
`(listener_id, shard_id, session_id, connection_generation)` 范围内，不提供全局 FIFO。
每个 command 使用唯一 correlation ID；超时后的重复 command 由 owner 按 ID 幂等确认。

transport queue 同时限制 entries 与 bytes。当前内部 link 在 admission 前编码一份 immutable frame；
入队成功后 link 拥有该编码副本直到 completion，调用方始终保留原始 frame/payload 所有权。队列满返回
`TURBO_ENOSPC` 且不产生部分入队；关闭先停止 admission，再唤醒 CoroNet owner 并排空已接受发送。
不能把 CoroNet receive buffer、socket、request context 或裸指针跨 coroutine suspension/thread 传递。

owner-lane adapter 再次复制并限制已接收 command 的 entries/bytes，在目标 CoroNet execution lane 内读取
当前 owner token，并用 command 携带的 `shard_id + owner_epoch` 与本地 `node_id + boot_id` 构造 presented
token。只有 `flowie_cluster_owner_token_require()` 完全匹配后才调用状态执行器；stale command 只生成明确
错误 reply，不修改 MQTT 状态。reply 是 command 结果的派生传输对象，最终事实写仍必须经过 PostgreSQL
transaction fencing。

阻塞 libpq 不进入 owner lane。状态执行器在 lane 内完成 decode、clone、全部可失败分配和事实命令暂存，
随后把 deep-copy command 提交给 PostgreSQL fact worker。异步 completion 回投同一 lane 后才能发布已
暂存的内存状态和生成 reply；owner-lane finalize 必须提前拥有 staged state 与预构造 reply，成功路径只做
无分配的 pointer/state transition，持久化失败路径销毁 staged state 且不得推进当前内存事实。completion
未到达前 adapter 拒绝新的 durable command。若 completion 无法
回投 execution lane，provider 必须触发 shard self-fence，不能从 worker thread 直接调用 reply 或继续以旧
内存状态服务。

post-CONNECT packet command payload 使用独立的 `TFMQ` version 1 contract，固定头携带总长、client ID 长度、
packet 长度和 MQTT version，body 依次为规范 client ID 与原始 MQTT packet。解码只返回指向已接收 payload
的 borrowed view，并要求 PUBLISH、SUBSCRIBE、UNSUBSCRIBE、ACK、DISCONNECT 与外层 operation 精确对应；
完整 MQTT frame 与对应 typed parser 均成功后才允许状态执行器消费。`CONNECT_BIND` 还需要认证 principal、
传输身份与 takeover 输入，不能复用上述 packet command。

`MQTT_REPLY` 成功 payload 使用独立的 `TFRP` version 1 contract。固定头携带总长、packet 长度、MQTT
version、flags 与可选 settlement point；packet body 必须是一个完整且与 MQTT version 一致的出站 packet，
只允许 PUBLISH、PUBACK/PUBREC/PUBREL/PUBCOMP、SUBACK、UNSUBACK、PINGRESP、DISCONNECT 或 AUTH。
空 packet 仍使用完整版本头表达 close、settlement 或显式 no-op，避免把空 payload 同时解释成成功与错误。
settlement point 只报告 session owner 已完成的边界，不能授权 edge 修改 session 状态；edge 的可执行副作用
仅限按顺序发送 packet 并在标志存在时关闭自己独占的 socket。

内部 session executor 已接入 SUBSCRIBE、UNSUBSCRIBE、delivery ACK/PUBREL 与 graceful DISCONNECT。
command 必须匹配 CONNECT 提交后保存的 edge node/boot、connection ID/generation、Client ID 和 MQTT version；
不匹配在 clone 或状态迁移前返回 `TURBO_EBUSY`。有状态更新继续使用 staged owner，并仅在 PostgreSQL fact
提交成功后原子替换 live owner；失败会销毁 staged owner，因此 subscription、delivery 和 active 状态均不
前进。更新 fact 保留最近一次 durable `TFCB` 作为 principal/Client ID 恢复材料，而不是错误地持久化当前
`TFMQ` command；对应 outbox 使用独立 `TFUE` updated event，避免把普通状态更新解释为新的 session bind。
graceful DISCONNECT 在同一 durable mutation 中应用 MQTT 5 expiry override 并关闭 owner，返回 close action。
edge 在提交 PUBLISH command 前解析并展开连接级 MQTT 5 Topic Alias，再以 CONNECT 时认证所得 principal
执行 topic ACL；未授权的 MQTT 5 QoS 1/2 发布由 edge 直接返回失败 ACK，不得触达 owner。
SUBSCRIBE 同样由 edge 逐项授权：只把获准子集提交 owner，再把 owner SUBACK 与本地拒绝 reason 按原始顺序
合并；该暂存映射只属于单次请求，不保存或推进订阅状态。
跨 shard subscription/event delivery 已由 PostgreSQL publish outbox、Redis broadcast、每 shard 派生
subscription index、target owner 的 fenced delivery transaction 和 peer delivery dispatcher 组合；Redis
只承载可重放事件，最终完成仍由 PostgreSQL shard ACK 判定。owner durable 接受 PUBLISH 后，edge 才把规范化
packet 投入 TurboFlow graph；`accepted`/`durable` 由现有 route settlement 回调触发，`processed` 由 graph
completion 触发，二者都经独立 `TFPS` version 1 command 回到 CONNECT 绑定的 owner。adapter 只在进程内把
endpoint-local generation 映射为 CONNECT reply 中保存的 authoritative session generation，wire 上不携带任何
process-local route capability；owner 再校验 node/boot、connection generation、Client ID、MQTT version 与
session generation，并仅在 PostgreSQL `TFSE`/`TFUE` 提交成功后推进 QoS inflight、返回 PUBACK/PUBREC。
重复 PUBLISH 未产生新的 durable TFPE 时不会再次进入 graph。SUBSCRIBE 的 retained replay 仍是生产启用门禁。

`CONNECT_BIND` 使用独立的 `TFCB` version 1 contract。edge 必须先完成基础或 enhanced authentication、
CONNECT 授权、Assigned Client Identifier 和连接级协商，再编码 authentication principal 与去凭据化
CONNECT。principal 使用定长 metadata 加有界长度字段，decode 后重新验证 scope、policy version、role、
group 和 root group 不变量；CONNECT body 重新经过完整 MQTT parser。仅 Session Expiry Interval 作为
session-owned CONNECT property 进入 payload，Will properties 原样保留给 owner，username、password、
Authentication Method/Data、Receive Maximum、Maximum Packet Size 和 Topic Alias Maximum 等 edge-owned
信息不会跨节点传递。decode 返回的 packet/connect view 只借用 payload；principal 按值复制。

owner 持久化使用 `TFSE` version 2 fact：固定头精确声明总长、TFCB 长度、canonical session owner record
长度、security mode，并保存 edge node/boot、connection ID/generation 与 `edge_action_sequence`；body 不再
维护另一份可独立推进的 session 状态。恢复必须在 shard 对外 ACTIVE 前完成；
扫描记录只写临时 registry，重复 Client ID、重复 session ID、revision/record 不匹配、未知 flags 或任一畸形
payload 都使本批次不可发布。成功发布只交换 registry，不在 live registry 上逐条插入，因此扫描中途失败不会
留下部分可见 owner。恢复出的 owner 一律 inactive，socket 仍只能由 edge CONNECT_BIND 重新绑定；若
node/boot/connection/generation 与恢复的最后 binding 完全相同，则下一动作延续 durable `N+1`，任一 identity
变化则把新 binding 的序列重置为 0。decoder 只为离线迁移接受 TFSE v1，并按“无 binding、sequence=0”解释；
encoder 只写 v2。旧 reader 不认识 v2，因此升级必须先 drain 全部 v1 writer/reader 后整体切换，禁止混合版本
节点同时进入 READY。

PostgreSQL recovery scan 不先读 ownership、再以另一个 statement 读事实。它在单条 statement 的 snapshot 中
同时要求精确 owner token、有效 shard lease 和有效 node lease，并以 `LEFT JOIN LATERAL` 的 owned-empty sentinel
区分“当前 owner 的空 shard”与“ownership 已失效”。查询最多返回 `max_records + 1` 行，超限时不调用 visitor；
key/value 的数据库实际长度也必须落在配置上限内并与 libpq 返回长度一致。registry 发布后才调用 lease worker
activate；activate 会再次校验初始 token 和本地 deadline，因此数据库快照读取后的 lease 丢失不能产生 ACTIVE
stale owner。若 publish 后 activate 失败，调用方必须销毁该尚不可服务的 bind 与 lease worker，不允许复用。

一个 session shard runtime 是这些资源的唯一生命周期 owner：

1. claim/renew lease worker；
2. recovery-only PostgreSQL scan connection；
3. bounded asynchronous fact worker；
4. recovered session registry；
5. CoroNet peer-owner adapter；
6. 可选的 TFTE PostgreSQL source 与单 in-flight dispatcher。

runtime 借用 CoroNet execution，不拥有或停止 event loop。创建失败按上述资源的逆序释放；成功返回前 lease 已
ACTIVE，peer adapter 与已配置 dispatcher 均完成创建。graceful close 先设置本地 fence并拒绝新的 dispatcher/
peer command，再停止 lease worker；已通过 resolve 但尚未 durable 的 command 仍必须经过 PostgreSQL transaction
fencing。随后在同一总 timeout budget 内先 drain dispatcher 的 send/reply/source 工作，再由 owner adapter 在
execution 仍运行时 drain 所有 completion/finalize/reply，最后才停止 fact worker 并销毁 session registry。
若 drain 超时，runtime 保持关闭 admission 和 fenced，但保留 execution 依赖资源，允许调用方再次 close；不得
强行销毁造成跨线程 use-after-free。

owner 的 `CONNECT_BIND` 结果使用 `TFBR` version 1 contract，而不是仅返回裸 CONNACK。固定头携带
accepted、close-after-reply、session-present、`owner_instance_id/session_id/session_generation`，body 是完整
CONNACK。edge 必须先验证 TFBR 长度、版本、flags 和完整 MQTT frame，再安装该 route；PG failure reply 不带
TFBR payload，也不得让 edge 把连接标记为 active。

`CONNECTION_LOST` 不是 MQTT packet，使用独立 `TFCL` version 1 contract；固定头携带总长、规范 Client ID
长度和 MQTT version，body 只包含 Client ID。edge 在提交前重新解析 PostgreSQL 派生 owner token，并要求它与
CONNECT 安装的 token 完全相同；owner 再要求 edge node/boot、connection ID/generation、Client ID 与 MQTT
version 全部匹配。异常断链只调用 `flowie_session_owner_close()`，不调用 graceful DISCONNECT 处理，因此 Will
会转成 pending；状态仅在 `TFSE` fact 和独立 `TFLE` outbox event 提交成功后发布。持久化失败保留 active owner，
已成功关闭的同一 binding 重复通知返回 `TFRP` no-op 且不重复写库，旧 generation 返回 `TURBO_EBUSY`。
`TAKEOVER_CLOSE` 使用独立 `TFTC` version 1 contract；固定头携带总长、旧连接 MQTT version 与规范 Client ID，
frame fencing 字段携带旧 connection ID/generation。cross-edge takeover 的 `TFTE` outbox payload 保存旧 edge
node/boot、旧 connection ID/generation、旧 MQTT version 与提交后的 session generation，record key 提供 Client
ID。旧 edge 重新解析 PostgreSQL 派生 owner token并要求 command source 的 node/boot/epoch 完全匹配，再要求
endpoint 提供的 socket identity 与 TFTC/frame 完全一致；stale generation 只返回错误 reply，不触发 close。
close callback 返回 `TURBO_EALREADY` 表示该精确 generation 已在关闭，并作为幂等成功返回 `TFRP` no-op。
TFTE dispatch adapter 只接受当前 session executor 产生的单 mutation `event_index=0`，稳定复用 command ID 作为
correlation；它允许新 epoch owner 重放旧 epoch 的 durable event，但 frame source 必须使用当前 owner token。
成功 settlement 要求 TFRP route、owner epoch、旧 binding 与 correlation 全匹配，payload 为同 MQTT version 的
no-op；edge error status 保持 outbox pending。

TFTE dispatcher 使用现有 `flowie_cluster_outbox` schema，不新增 claim column：在精确当前 owner row lock 下选择
最早的未发布 TFTE，并以 `FOR UPDATE SKIP LOCKED` 增加 `attempt_count`；一个 dispatcher 只持有一个 owned row 和
一个 peer send。成功 send completion 只证明 transport 接受，不授权回收；只有精确成功 TFRP 才进入 settle。
`published_at` 的条件更新覆盖 command ID、event index、shard、event owner epoch、event type、record identity、
fact revision 和 payload。COMMIT 回包丢失后，重连并用同一完整 identity 重试会得到 `TURBO_EALREADY`。因此协议为
至少一次：进程在 reply 后、settle 前崩溃会重发 TFTC，edge 以 socket generation 的 `TURBO_EALREADY` 归一成
成功 no-op。数据库是 durable queue 和 settlement 事实源，dispatcher 不把 peer queue、Redis 或本地计数提升为
事实源。peer link registry 只按 frame 中精确 node/boot identity 选择借用 link；它在锁内取得 bounded send
lease，锁外调用 transport 与用户 completion，unregister 必须等待该 link 的 accepted send 全部完成。registry
本身没有 ownership 选择权，其计数和 link 状态都不是 MQTT 事实源。shard runtime 可选创建并关闭/drain TFTE
dispatcher，统一 receive 入口负责将 TFRP 送回该 dispatcher。内部 node router 已把 peer receive 按精确
cluster/listener/local node/boot 和 shard ID 转到唯一借用 runtime，并把 dispatcher send 委托给 peer registry。
shard 表固定为配置的 `shard_count`，receive 使用短期 lease，首个 `TURBO_EBUSY` unregister 即停止新路由；
router close/drain 同时等待已接收入站调用与 peer send completion，但不关闭或销毁借用的 runtime/link。推荐组合
顺序是先创建 router，再以其 send callback 创建 shard runtime 并注册 runtime；peer link 以 router receive callback
创建并在认证 ACTIVE 后注册。内部 peer listener 已拥有 responder link 与 CoroNet server，关闭时先停止 admission
并关闭 link，等待 link 从 router 精确注销以及 CoroNet accepted task/socket 销毁后才释放配置。内部 connector
拥有单 target 的 client socket/link/retry coroutine，close 会中断活跃 receive，drain 等待 router 注销和 task 退出；
drain 在自身 execution lane 上拒绝同步自等。内部 node composition root 已按该契约先 close/drain
connector 与 listener，再 close router，注销/关闭借用 runtime 并 drain，最后依赖逆序 destroy transport、
runtime、Redis target、router 和 Redis bus。可选 membership runtime 使用独占 PostgreSQL coordinator 的
专用线程周期 expire/snapshot/plan/apply，root 必须先 stop/join 它再进入上述 peer 关闭序列。`TFLE` consumer 的 outbox/retry/settle
边界、Will/expiry owner-lane executor 以及 shard runtime 对 source/owner/dispatcher 的拥有和关闭顺序已经实现；
server application 已通过 generation root 持有 owner directory、node、endpoint binding 与共享 execution；公开
YAML/CLI 配置解析仍未完成。生命周期
协议不能依赖空包或特殊字符串约定。

mTLS 成功并不单独等价于节点授权。link 还必须取得 verified peer certificate SHA-256，经 membership/
证书映射回调授权 `node_id + boot_id`，并要求 HELLO/HELLO_ACK payload 精确匹配当前 TLS channel binding。
任一 certificate、cluster、source、target 或 boot identity 不匹配都关闭 link，不尝试路由 fallback。

建议的内部 port 均使用 opaque handle、小 operation table 和显式生命周期：

```text
cluster_membership: join / renew / transition / snapshot / leave
cluster_shard_store: claim / renew / release / recover / commit_fenced
cluster_transport: start / send_command / send_reply / publish_event / drain / stop
```

application composition root 拥有这些 provider，生命周期长于 endpoint。endpoint 只借用 validated
descriptor，不通过 global singleton 或 service locator 获取依赖。

## MQTT 关键流程

### CONNECT 与相同 Client ID takeover

1. HAProxy 终止 MQTTS，校验非空 Client ID，按 consistent hash 选择健康 connection edge，并通过可信
   PROXY v1 传递源地址。
2. edge 独占 plaintext socket，消费 PROXY header，完成 MQTT framing、CONNECT 独立校验和 Auth。
3. edge 由规范 Client ID 计算 session shard，读取 owner cache 与 membership revision。
4. owner 是本机时提交 same-lane command；否则经 CoroNet mTLS 发送 `CONNECT_BIND`。
5. shard owner 在 fenced transaction 中推进 connection generation、绑定新 edge route，并写
   takeover outbox。
6. 新 CONNACK action 返回
   `(edge_node_id, edge_boot_id, connection_id, connection_generation)`。
7. 旧 edge 收到 `TAKEOVER_CLOSE` 后只关闭匹配的旧 generation；迟到 reply 因 generation 不匹配被拒绝。

新连接只在 authoritative bind commit 后成功。live socket 不迁移；takeover 的含义是旧 socket 被旧
edge 关闭，新 socket 由新 edge 保持。

### Redis route projection

内部 `flowie_cluster_route_store` 已实现 Client ID keyed 的版本化 route record，并只依赖 FlowStore
StateStore，因此同一语义可由内存 backend 做 contract test、由 Redis standalone/Cluster/Sentinel backend
承载生产投影。active record 包含 edge node/boot、advertised endpoint、connection ID/generation、session
shard/generation、PG owner epoch、fact revision 和绝对 lease deadline；Client ID 必须非空且作为二进制 key，
不能退化成仅保存 gateway IP。

投影顺序固定比较 `(owner_epoch, fact_revision)`，StateStore revision 只用于 Redis CAS：旧 PG 版本是可结算的
stale no-op，相同版本且相同 payload 是幂等 replay，相同版本但 payload 分歧是 `TURBO_EPROTO`。断连写带版本的
tombstone 而不是删除 key，避免不同 outbox worker 或 Redis 重试乱序时由迟到 BOUND 复活旧 socket；读取 active
record 还必须检查 lease deadline。损坏或未知版本的 Redis value fail fast，不能覆盖后继续。

当前完成的是存储 contract，还未把 route projection 接到生产 outbox settlement。接入时 BOUND/UPDATED 可从
已提交 session fact 构造 active record，CONNECTION_LOST 构造 tombstone；TAKEN_OVER outbox 当前携带旧 edge
用于 close，必须读取同一事务已提交的新 session fact 后再投影，不能把旧 edge event 当作新 route。只有 Redis
project 成功、幂等或已被更高 PG 版本取代后才允许对应 outbox 阶段结算。该门禁完成前，Redis route 不能替代
现有 PG owner directory，也不能用于生产 delivery 正确性判定。

### PUBLISH、subscription 与 delivery

edge 把入站 packet 交给 publisher session shard owner。owner 负责 sender QoS/inflight 和 ingress
settlement，并在同一事务写事实与 publish event outbox。

第一阶段采用显式 `broadcast` routing mode：

1. dispatcher 从 PostgreSQL outbox 把 immutable event 发布到有界 Redis Stream transport；原 outbox 在固定 shard
   集合全部写入 PostgreSQL completion marker 前保持未结算；
2. 每个 shard 使用独立稳定 consumer group；只有该 shard 的本机 ACTIVE owner 创建稳定 consumer 并读取
   stream。不同 shard group 各自取得 TFBE，同一 shard owner 切换后继续重放该稳定 consumer 的 PEL；
   Redis PEL 只控制 transport replay，不充当 MQTT 完成事实；
3. target session 的 QoS/offline queue 修改仍走 target owner 的 fenced transaction；
4. 每个 target session 与最终 shard completion 都在 PostgreSQL 写 exact event dedupe，重复事件不重复产生
   MQTT 状态；
5. owner 切换后新 owner从 PostgreSQL fact/dedupe/outbox 恢复，Redis consumer cursor 只决定从哪里重新取得
   TFBE。

复杂度为：

```text
event fanout operations = published_events * shard_count
```

因此 `broadcast` 必须配置 `max_nodes`、event rate、queue bytes 和 outbox bytes 门禁。超过设计容量时
cluster 启动失败或运行时产生明确背压，不能无界增长。

后续可增加 topic-filter 到 shard bitmap 的派生路由索引，将复杂度降低为匹配 shard 数。索引必须带
revision 和 gap detection；revision 缺失时暂停 indexed routing 并从 PostgreSQL/outbox 重建，不能
静默改为可能漏消息的局部索引。routing mode 是显式配置，不做隐藏 fallback。

Redis append 成功后不能立即删除 PostgreSQL outbox。只有所有要求的 shard cursor/ACK 已跨过 event，
或该事件按明确 settlement/expiry 契约终结后，才可回收 outbox。Redis 异步复制导致已确认 event
回退时，dispatcher 会根据未完成 outbox 重发；`event_id` 负责去重。

### Retained、Will 与 expiry

- retained fact 由 retained shard owner fenced 写入，再产生 delivery event；
- Will 是 session fact，只能由该 session shard owner 安排和执行；
- connection edge crash 由 node/edge lease 和失联事件转化为一次 connection-lost fact；
- owner crash 后，新 epoch owner 恢复 timer 与 outbox，旧 owner 即使恢复也因 epoch 不匹配不能执行；
- expiry cleanup 同样携带预期 fact revision 与 owner epoch，避免 stale cleanup 删除新 session。

## Settlement 与错误语义

集群 transport 不能悄悄改变现有 settlement 定义。协议层需显式区分：

- `received`：connection edge 已完整解析并验证 packet；
- `accepted`：authoritative shard owner 已把 command 接入有界队列；
- `processed`：owner 已完成当前 MQTT 状态迁移和所需 target command 汇总；
- `durable`：要求的 PostgreSQL fact/outbox/target fact 已提交并可按 ID 恢复。

具体 MQTT QoS 和现有 public settlement 的映射由 contract tests 锁定。不能把“已进入 HAProxy”、
“已写 Redis Stream”或“已放入本地 queue”报告为 durable。

queue 满、peer timeout、owner fenced 和依赖故障都向当前能处理的协议边界返回明确错误。已承诺 durable
的 event 必须最终完成或进入可查询的失败/补偿状态；不能只写日志后返回成功。

## 背压与容量

集群配置至少必须限制：

- 最大节点数和固定 shard 数；
- 每个 peer 的连接数、command queue entries/bytes 和最大 payload；
- 每个 shard 的 inflight command、recovery batch 和 dedupe window；
- PostgreSQL outbox records/bytes 与最老事件年龄；
- Redis Stream bytes/entries 和 consumer lag；
- drain timeout、peer timeout、lease TTL、renew interval 和 safety margin。

queue capacity 的可复算下界为：

```text
required_entries =
    ceil(peak_commands_per_second * worst_peer_stall_seconds)
  + max_inflight_batch
```

还必须按最大 message size 复算 bytes 上限，并对乘加执行溢出检查。容量不足时：

- 新 CONNECT 可拒绝或关闭；
- SUBSCRIBE/UNSUBSCRIBE/PUBLISH 按 MQTT 版本返回可表达的失败；
- 无法表达可靠失败的连接直接 fail closed；
- PostgreSQL outbox 达到硬上限时，拒绝会产生新 event 的事实修改。

不允许为维持表面可用而只更新本地 cache、跳过 outbox 或降级为无界内存队列。

## 故障矩阵

| 故障 | 系统行为 | 禁止行为 |
|---|---|---|
| PostgreSQL 暂时不可用 | 停止 join/claim/renew/commit；现有 owner 仅运行至本地保守 deadline，随后 self-fence | 以 HAProxy/Redis 状态续写 |
| PostgreSQL primary 切换 | pool reopen，重读 lease/epoch，只恢复仍匹配 shard | 假设旧连接恢复后 ownership 仍有效 |
| Redis 不可用 | PostgreSQL outbox 累积；到达硬上限后对新 event 背压 | PG/Redis 双写后把 Redis 失败当成功 |
| Redis 回退或重复 | 未完成 outbox 重发，target 按 event ID 去重 | 让 Redis offset 成为唯一事实 |
| peer link 中断 | 有界排队至 deadline，随后明确失败；幂等 command 可由调用方用同 ID 重试 | 无限排队或无 ID 自动重试 |
| shard owner crash | lease 过期，新 owner 取得更高 epoch、recover 后 ACTIVE | 两个 owner 同时恢复 |
| connection edge crash | owner 根据 edge/node 失联记录一次 disconnect/Will 流程 | 让 HAProxy 连接计数直接触发 Will |
| HAProxy reconfigure | 新连接按新 ring；旧 socket 保持到断开/takeover/drain | 强制“迁移”已建立 TCP socket |
| stale node 恢复 | epoch mismatch，进入 FENCED，关闭相关连接 | 复用旧 boot ID/epoch |
| outbox 满 | 拒绝产生新 event 的写入并报警 | 删除未确认 event 腾空间 |

## Drain、关闭与滚动升级

节点 drain 顺序固定：

1. membership 进入 `DRAINING`；
2. 从 HAProxy backend 移除并等待新配置生效；
3. 停止 claim shard 和接收新的 session bind；
4. 把 session 重连提示交给客户端策略，完成或截止现有 command；
5. 排空 peer queue 和已提交 outbox；
6. 主动 release shard，确认新 epoch owner recover；
7. 关闭 client socket；
8. leave membership，drain CoroNet transport 和 PostgreSQL pool。

达到 drain deadline 后仍不能转移的 shard必须 self-fence 并关闭相关连接，不能无限等待。析构路径不得从
CoroNet owner callback 中直接同步等待自身 coroutine 完成。

滚动升级要求 wire protocol 和持久化 schema 具备显式版本兼容窗口。节点只在所有必需 capability
兼容时进入 READY；否则 fail fast，不靠字段猜测互操作。

## 配置与接口影响

以下仅表示目标配置结构，数值必须由容量规划给出：

```yaml
cluster:
  enabled: true
  id: <cluster-id>
  node_id: <stable-node-id>
  shard_count: <fixed-count>
  lease_ttl_ms: <validated>
  renew_interval_ms: <validated>
  safety_margin_ms: <validated>
  drain_timeout_ms: <bounded>
  routing_mode: broadcast
  limits:
    max_nodes: <bounded>
    peer_queue_entries: <bounded>
    peer_queue_bytes: <bounded>
    outbox_records: <bounded>
    outbox_bytes: <bounded>
  transport:
    advertise: <host:port>
    tls_ca_ref: <key-provider-reference>
    certificate_ref: <key-provider-reference>
    private_key_ref: <key-provider-reference>
  coordinator:
    backend: postgresql
    connection_ref: <secret-reference>
  event_bus:
    backend: redis
    connection_ref: <secret-reference>
```

Redis event bus 可在单节点开发模式中省略，但多节点 `broadcast` 模式不能静默改用进程内 bus。
cluster mode 要求 PostgreSQL coordinator 和支持 fenced transaction 的 PostgreSQL MQTT fact adapter；
现有 volatile/Redis authoritative Record Store 不能直接作为强一致 cluster writer。

兼容性与实现风险：

- `HIGH`：`flowie_endpoint_bindings_t` 当前按精确 size 校验。追加 cluster binding 前必须改为 size-gated
  compatibility 或提供新的 additive factory，旧尺寸调用方仍保持单节点行为。
- `HIGH`：persistence record 增加 shard/epoch/schema 字段，需要版本化 migration、旧 reader 拒绝规则
  和禁止混合 writer 的切换门禁，否则可能产生不可恢复的双写。
- `HIGH`：settlement 和 MQTT takeover 语义会影响客户端可见 ACK、重复投递与断开行为，必须用
  contract tests 固定，不能在 transport 层自行解释。
- `MED`：`remote_address` 增加可信 PROXY 来源后属于 Auth 契约变化，必须同步更新 HTTPS Auth 版本、
  trusted-peer 配置和安全测试。
- `MED`：shutdown/drain deadline、queue 满和 owner fenced 会增加新的明确错误路径，需要同步
  CLI/Control 状态与运维文档。

## 可观测性与审计

消息热路径使用 metrics，不记录逐条 PUBLISH 的 INFO 日志。至少暴露：

- node state、boot ID、membership revision、lease renew latency/failure；
- 各 shard 状态、owner epoch、recovery/drain duration、fencing rejects；
- 本地 connection 数、remote-owner connection 数和 takeover 次数；
- peer queue entries/bytes watermark、full/timeout、command latency；
- PostgreSQL pool healthy/dead/reopening slot、transaction latency/conflict；
- outbox pending records/bytes/oldest age、Redis publish/replay/duplicate、consumer lag；
- routing index revision/gap/rebuild；
- HAProxy frontend/backend/server connection/error/byte metrics，明确标记派生来源。

INFO 仅记录节点和 shard 的低频生命周期里程碑；lease 暂时重试使用有界 WARN，self-fence、数据契约错误
和无法恢复的依赖失败使用 ERROR。相同错误不在每层重复记录。

owner 变更、epoch 推进、self-fence、管理员 drain 和 cluster 配置变更属于可靠审计，写入 PostgreSQL
事务或可靠 outbox，而不是只写异步普通日志。审计包含 cluster/node/boot/shard、旧/新 epoch、reason、
actor 与 revision，不包含 credential、MQTT payload 或私钥材料。

## 迁移路径

### 阶段 1：进程内职责拆分

先把现有 endpoint 内的 socket owner 与 session/state owner 拆成两个内部接口，仍使用 same-lane 直接
调用。该阶段不引入网络和新用户行为，用现有 MQTT、session store 和 endurance tests 验证等价。

### 阶段 2：Cluster port 与 PostgreSQL schema

增加 membership、shard store、transport port 和 size-versioned binding。实现 PostgreSQL migration、
lease、epoch、fenced commit、outbox 与 contract tests；cluster 默认关闭。

### 阶段 3：Active/passive

两个 Flowie 节点中只有一个 READY owner，验证 PostgreSQL failover、pool reopen、self-fence、recover、
Will/expiry 一次性和 commit reply 丢失。

### 阶段 4：Active/active

启用多个 shard owner、CoroNet mTLS、remote `CONNECT_BIND` 和 `broadcast` delivery。通过两至三个节点的
QoS 0/1/2、offline session、retained、takeover 和故障注入门禁。

### 阶段 5：HAProxy 集成

增加 plaintext trusted PROXY v1 listener、源地址 Auth contract、HAProxy health/drain、MQTTS termination 和
Client ID consistent hash。即使把所有连接送入非 owner edge，所有集群正确性测试仍必须通过。

### 阶段 6：路由优化

在 broadcast 结果作为正确性基准后，再增加 topic-filter 到 shard bitmap 的派生索引。只有 benchmark
证明 broadcast 达到容量边界时才启用优化。

### 数据切换

cluster 使用独立 schema version 与 namespace。切换步骤：

1. drain 并停止所有 legacy writer；
2. 生成一致性 snapshot；
3. 按固定 hash version/shard count 导入事实；
4. 校验 record 数、revision、引用、outbox 和 checksum；
5. 原子标记 cluster namespace 可写；
6. 启动一个 active/passive owner 验证后再扩到 active/active。

不允许 legacy 与 cluster 双写。`shard_count` 变化采用同样的离线迁移流程；第一版不提供在线 reshard。

## 回滚

cluster 正式切换前，关闭 `cluster.enabled` 即回到未修改的 legacy namespace。

正式切换后的回滚不是自动 fallback，必须：

1. drain HAProxy 与全部 Flowie cluster writer；
2. 确认所有 shard 已 FENCED/OFFLINE；
3. 确认 outbox、target cursor 和 settlement 已收敛；
4. 导出一致性 snapshot 并校验；
5. 恢复或迁移到 legacy namespace；
6. 只启动一个 legacy writer。

若无法证明 outbox 收敛，不得回滚后继续接受写流量。这样避免两个事实源各自推进。

## 验证门禁

### 单元与契约

- stable hash 跨平台 golden vectors、边界与 shard_count 校验；
- node/shard/connection 状态机的合法与非法迁移；
- claim/renew/release、lease deadline、stale boot ID 与 stale epoch；
- fenced fact + outbox 原子提交、revision conflict 和 lost COMMIT reply；
- transport 编解码、长度/版本/身份限制、幂等 ID；
- CONNECT_BIND v2 principal round-trip、去凭据化、security on/off、源地址/transport peer/二进制 PROXY TLV、
  半缺失地址、嵌入 NUL、超长地址、畸形/超限 payload，以及 trusted PROXY pre-TLS admission 到 endpoint、
  应用 cluster adapter 的端到端传递；
- TFSE v2 round-trip、v1 只读迁移、durable action sequence/同 binding 重绑、不同 binding 重置、inactive owner
  恢复、未知版本/截断/畸形/重复 fact 整批拒绝、registry 原子发布与 session ID 水位推进；
- PostgreSQL TFSE scan 的 owned-empty、stale owner、`max_records + 1` 容量拒绝，以及恢复成功后才激活 lease；
- shard runtime 的单一 persistence/fence callback、容量匹配、失败清理、owner-lane CONNECT_BIND 与关闭 drain；
- peer registry 的精确 node/boot 路由、link 借用、in-flight 上限、两阶段 unregister 及 close/drain；
- node router 的精确 local identity/shard 路由、runtime 借用、回调外执行、两阶段 unregister 及 close/drain；
- peer listener 的 mTLS responder、ACTIVE 后注册、重复身份拒绝、handler/link 容量及 CoroNet socket drain；
- peer connector 的确定性单向发起、网络重连、认证 terminal error、双 router 路由及 close/drain；
- cluster node composition 的 identity/重复 shard 拒绝、Redis/router 端口接线、失败创建逆序回滚、
  peer-drain-before-router 关闭顺序、close 重试、完整 generation 销毁、topology snapshot、部分 replacement
  重试及 applied revision 提交；
- membership topology 的 snapshot revision、本机 boot fence、确定性方向、replace 顺序、容量和 owned view；
- membership runtime 的异步 coordinator reopen、瞬态 refresh/apply 重试、永久错误 fault、长间隔可中断关闭，
  以及 composition root 在 peer teardown 前 stop/join 的顺序；
- TLS 握手前 server task admission 上限、超限立即关闭和 stalled-handshake shutdown；
- TFTE dispatcher 的 send/reply timeout、owner 切换、source reopen、精确 settlement 与 runtime 生命周期组合；
- TFLE consumer 的格式/容量校验、权威 created-at deadline、apply admission 重试、Will-before-expiry 原子计划、
  session fact 删除、durable completion 回投、settle 不确定恢复不重复 action 和 late completion drain；
- post-CONNECT command payload golden vector、五类 operation/type 对应、畸形/超限/非法 UTF-8；
- PROXY v1/v2 trusted/untrusted、IPv4/IPv6、malformed、timeout 与 TLS 前置解析；
- queue entries/bytes、溢出和 shutdown/drain ownership。

### 集成与故障注入

- 两至三个 Flowie 节点、HAProxy、PostgreSQL HA 和 Redis；
- 同一 Client ID 并发 CONNECT，验证仅一个 generation/epoch，旧 socket 被正确 edge 关闭；
- 跨节点 QoS 0/1/2 pub/sub、offline persistent session、retained、Will delay 和 expiry；
- PostgreSQL 断连/primary failover、Redis 断连/回退/重复、peer partition 和节点暂停；
- HAProxy backend add/remove/hash 变化与 rolling drain；
- outbox/peer queue 满和恢复后的有界 replay；
- stale owner 与 stale expiry cleanup 始终返回 fencing conflict；
- 强制把连接送入非 owner edge 或清空全部 HAProxy 派生状态后，集群仍保持正确。

### 可判定 oracle

- 不存在同一 shard 的两个 ACTIVE owner；
- 不接受任何 stale epoch fact commit；
- durable settlement 不早于要求的 transaction 与 target ACK；
- 所有 queue、outbox、cursor lag 都不超过配置硬上限；
- 每个已接受 event ID 最终被幂等应用，或进入可查询的明确失败状态；
- 任何故障下，HAProxy 连接统计、健康状态和 consistent hash 都不会改变 MQTT 写权限。

只有这些门禁通过，才能把状态从“提议”改为“已采纳”。
