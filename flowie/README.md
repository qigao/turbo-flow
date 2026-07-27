# Flowie 文档

Flowie 是 MQTT 协议与业务处理层。所有 MQTT 业务事实（session、subscription、inflight、
retained 和 Will）都通过 FlowStore 的 Record facade 管理；Flowie 内部容器只能保存可重建的
owner cache。默认配置使用 `tf_local_storage` 的 local volatile Record backend；需要重启恢复或
跨进程共享时，显式选择 `tf_redis` 或 `tf_pgsql`。三个 backend 是同级 shared library，并由
`io/common/storage` 的 StorageBackend registry 通过 `open()/close()` ABI 装配。

认证与上述存储边界完全分离：bundled Flowie 只配置 HTTPS auth provider；`flowie-control` 可选择
Repository 本地 Auth 或第三方 HTTPS Auth。第三方 credential、OIDC、LDAP/AD、RADIUS 或身份数据库
都留在第三方认证服务内部。FlowStore、session store 和 Graph adapter 都不是认证来源。规范性决策见
[HTTPS 认证服务设计](ADR_HTTPS_AUTH_SERVICE.md)，部署配置见
[Flowie Control 部署与配置](CONTROL_GUIDE.md)。

- [配置式 Broker 概念与术语](CONFIGURED_BROKER_CONCEPTS.md)
- [服务端使用指南](SERVER_GUIDE.md)
- [客户端开发指南](CLIENT_GUIDE.md)
- [CoroNet buffer 调优契约](../io/common/CORONET_BUFFER_TUNING.md)
- [架构说明](ARCHITECTURE.md)
- [FlowStore 统一状态、索引、日志与时间序列](../flowstore/ADR_FLOWSTORE.md)
- [StorageBackend ABI 决策](../io/common/storage/ADR_STORAGE_BACKEND_ABI.md)
- [StorageBackend 协议](../io/common/storage/STORAGE_BACKEND_PROTOCOL.md)
- [动态 ACL bundle 设计](ADR_DYNAMIC_ACL_BUNDLE.md)
- [发布门禁](RELEASE_GATE.md)
