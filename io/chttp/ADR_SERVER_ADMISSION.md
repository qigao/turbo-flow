# Deferred server admission control (#83)

## 背景与选择

#71 的 DLL generation 必须先 quiesce Source，再排空 Graph 和 deferred response。
原有 server stop 同时终止网络 owner，不适合在已接受请求仍执行时充当 admission gate。
选择在具体 server 的既有 mutex/state 上增加 QUIESCED 状态和 quiesce/resume 入口。
拒绝在插件中复制 admission 状态，也不借用全局 Flow pause 影响无关 Source。

## 所有权与并发协议

CHTTP 线程接收请求，Flow worker 执行已接受消息，控制线程调用 quiesce/resume。
槽位 reservation 与 admission 状态变更共用 server mutex，这是请求接受的线性化点。
quiesce 返回后不能再保留新槽位；之前保留的槽位可以继续发布并完成响应。
QUIESCED 只表示关闭 admission，不表示 active_requests 已归零。
已有槽位、payload、deferred handle 和 completion 的释放路径保持不变；
无新增队列、分配或等待，槽位数仍由 server->network.connection_capacity 限制，
snapshot.request_capacity 暴露同一上限。

新请求收到配置中的 unavailable_status，rejected_requests 增加，last_status 为
SALTS_ESHUTDOWN。恢复仅通过显式 resume；重复 quiesce/resume 成功且没有额外副作用。
只有 RUNNING/QUIESCED 可切换；其他生命周期返回 SALTS_ESHUTDOWN，空句柄返回
SALTS_EINVAL。start/stop/destroy 与控制调用仍由调用方串行；网络回调与查询可并发。

## 兼容性、迁移与验证

新枚举追加，不改变原枚举数值或现有 size/version snapshot 布局。
未调用新接口的客户端保持既有行为。网络 listener 保持运行以返回明确拒绝响应，
因此这不是关闭 socket 或拒绝 TLS handshake 的接口。
恢复旧 generation 时调用 resume；移除该能力可回退代码版本，不能增加静态 provider fallback。

验证 H1/H2 下阻塞已接受请求、关闭 admission、拒绝后续请求、完成原响应、恢复及再次接受；
核对精确计数、相同监听端口、重复调用、非法生命周期、停止和重启。
随后运行 CHTTP 相邻测试、C/C++ 安装消费者、Debug/ASan 和 Release。
WebSocket gate 和完整 DLL 装配继续由 #71 跟踪。
