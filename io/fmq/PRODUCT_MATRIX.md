# FMQ product matrix

本矩阵定义当前可宣称的产品边界。FMQ 借鉴 ZeroMQ pattern，但不兼容 ZeroMQ wire/API；只支持 FMQ
wire v2，不支持 v1。

| 能力 | 状态 | 事实源/限制 |
| --- | --- | --- |
| PUB/SUB prefix fan-out | 支持 | subscription trie/index；无匹配返回 `TURBO_ENOTCONN`；订阅后才收消息 |
| PUSH/PULL | 支持 | Queue memory/SQLite；accept ACK 与 delivery ACK 分离 |
| REQ/REP | 支持 | 严格同步，一个 request 对应一个 terminal reply |
| ROUTER/DEALER | 支持 | live route 必须 detach 为 pointer-free owner value；支持 delayed async reply |
| XPUB/XSUB state/recovery | 支持 | 有界 topic/state/update owner；不是 ZeroMQ wire |
| Load balancer / reliable request / credit worker | 支持 | bounded broker/lease/dedup/settlement；按配置选择语义 |
| CoroNet transport | 支持 | TCP/TLS/UDP/KCP/Pipe/WS/WSS 由 adapter contract 限定；pattern 与 transport 正交 |
| Redis Stream durable replay | 支持 | Redis Stream/PEL 为事实源，真实 Redis contract suite 显式运行 |
| Redis Data SET/GET | 支持 | binary-safe data contract，不混用 Stream delivery ACK |
| SQLite Queue durable replay | 支持 | queue-private schema v2、legacy migration、transactional settlement |
| PgSQL durable outbox | 不在当前 FMQ 产品范围 | PostgreSQL adapter 可用于 query/sink；不宣称事务 outbox source/sink |
| TFMP management | 支持 | strict REQ/REP、typed command、operation/event、memory/SQLite/Redis store |
| Failure-domain deployment owner | 支持 | host-serialized membership/election/fencing API；authority epoch 由宿主强一致服务分配 |
| Rolling upgrade check | 支持 | explicit manifest；direct/gateway/stop/incompatible，不放宽 decoder |
| Durable side-effect auto recovery | 条件支持 | 仅对有 built-in 或 resource-owner typed inspector 的 command 宣告 durable |
| Human-authored config | `.yml` | resolver 产生 immutable resolved JSON；JSON 不是并行人工配置入口 |
| 认证/授权/租户 | 不支持 | 只能部署在宿主已建立的可信进程/网络边界；TLS 仅是 transport |
| ZeroMQ compatibility | 不支持 | 不提供 ZMTP、ZeroMQ socket option 或 v1 compatibility |

当前 FMQ library/data-plane 与上述 durable/management 边界可用于受信部署。Socket/FMQ/HTTP/RPC
均可从同一 immutable YAML resolved snapshot 投影，九种 resource kind 具备 schema-backed 治理文档；
host 仍须显式提供 CoroNet/TurboHTTP/Iris 等执行对象和 Observe attach。发布结论还受
[`fmq-release`](RELEASE_GATE.md) 的真实 Redis、soak/chaos 与全量回归 gate 约束。
