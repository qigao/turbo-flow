# Flowie 文档

Flowie 是 MQTT 协议与业务处理层。MQTT 协议事实（session、subscription、inflight、retained、
Will、presence 和 route）由独立 ProtocolStore 管理；Graph 业务事实由 FlowStore/业务 sink 管理。
Flowie 内部容器只能保存可重建的 owner cache。standalone 使用独立 SQLite `:memory:`
ProtocolStore，进程重启后由 Client 重连和重订阅重建；cluster 目标使用 Redis 协议存储，并由
PostgreSQL ownership/epoch/fencing 约束。长期业务数据进入独立 BusinessStore。两层即使使用相同
引擎，也必须分开 namespace、连接、权限、容量、migration 和故障语义。

认证与上述存储边界完全分离：bundled Flowie 只配置 HTTPS auth provider；`flowie-control` 可选择
Repository 本地 Auth 或第三方 HTTPS Auth。第三方 credential、OIDC、LDAP/AD、RADIUS 或身份数据库
都留在第三方认证服务内部。FlowStore、session store 和 Graph adapter 都不是认证来源。规范性决策见
[HTTPS 认证服务设计](ADR_HTTPS_AUTH_SERVICE.md)，部署配置见
[Flowie Control 部署与配置](CONTROL_GUIDE.md)。

---

## 文档导航与分类

### 1. 正式核心与指南文档 (Official Guides & Manuals)

- [架构说明](ARCHITECTURE.md)：Flowie 架构设计、分层模型与核心流程
- [配置式 Broker 概念与术语](CONFIGURED_BROKER_CONCEPTS.md)：Flowie 概念定义、模式与术语字典
- [服务端使用指南](SERVER_GUIDE.md)：`flowie_server` 与 `flowie_supervisor` 部署、运行与 CLI 参数
- [Flowie Control 部署与配置指南](CONTROL_GUIDE.md)：控制面 `flowie-control` 架构、部署与安全配置
- [第三方系统接入指南](THIRD_PARTY_INTEGRATION.md)：Domain、管理 API、service credential、MQTT 客户端与 ACL 接入流程
- [Flowie Control ACL 文法与使用](ACL_GRAMMAR.md)：用户 ACL、topic 树、wildcard 与发布流程
- [客户端开发指南](CLIENT_GUIDE.md)：Flowie MQTT 客户端 SDK 接口与集成
- [发布门禁](RELEASE_GATE.md)：生产发布与质量门禁验收标准

### 2. 架构决策记录 (Architectural Decision Records - ADRs)

- [HTTPS 认证服务设计](ADR_HTTPS_AUTH_SERVICE.md)：HTTPS Auth Provider 信任边界与 API 规范
- [动态 ACL Bundle 设计](ADR_DYNAMIC_ACL_BUNDLE.md)：版本化 ACL Bundle 格式、发布机制与同步
- [控制面启动参数决策](ADR_CONTROL_STARTUP_OPTIONS.md)：`flowie-control` CLI / Environment / DotEnv 优先顺序与配置隔离
- [控制面凭据管理与哈希](ADR_CONTROL_CREDENTIALS.md)：Argon2id 散列算法、参数与凭据存储规范
- [控制面存储 Provider 抽象](ADR_CONTROL_STORE_PROVIDER.md)：SQLite 与 PostgreSQL 存储层接口与迁移控制
- [协议数据与业务数据存储分层](ADR_PROTOCOL_BUSINESS_STORAGE.md)：ProtocolStore / FlowStore 边界与迁移

### 3. 测试与运维手册 (Test & Ops Runbooks)

- [MQTT 协议测试矩阵](MQTT_TEST_MATRIX.md)：MQTT 3.1.1 / 5.0 协议特性测试用例与验证覆盖矩阵
- [Linux 远程测试 Runbook](LINUX_REMOTE_TEST_RUNBOOK.md)：Linux 远程环境自动化测试、调试与验证手册

### 4. 关联组件与底层设计 (Related System ADRs)

- [FlowStore 统一状态、索引、日志与时间序列](../flowstore/ADR_FLOWSTORE.md)
- [StorageBackend ABI 决策](../io/common/storage/ADR_STORAGE_BACKEND_ABI.md)
- [StorageBackend 协议](../io/common/storage/STORAGE_BACKEND_PROTOCOL.md)
- [CoroNet Buffer 调优契约](../io/common/CORONET_BUFFER_TUNING.md)

### 5. 过程日志与临时清单 (Internal Logs & Work-in-Progress)

- [Flowie ACL/Auth 控制面交付清单](ACL_AUTH_CONTROL_TODO.md)：ACL/Auth 阶段性研发进度日志、历史验证 Gate 记录与待办事项
