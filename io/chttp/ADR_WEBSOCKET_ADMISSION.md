# WebSocket admission 与 session 排空（#85）

#71 的 plugin generation 必须先关闭 Source admission，再排空 Graph/网络 owner。
WebSocket 会话不能在 quiesce 时直接全部关闭，否则已接受帧的输出可能失去 session。
因此沿用具体 owner 的 mutex、session 槽位和 in_flight_frames，而不在插件层复制状态。

quiesce 在线性化点把 server 设为 QUIESCED，并将所有活跃 session 标为 close_on_drain。
新握手收到 HTTP 503；后续帧不再保留为 Graph 输入，增加拒绝计数。
已预留帧继续通过原 terminal 路径完成。当会话的 in_flight_frames 归零，提交 close 1013。
没有已接受帧的会话可立即提交 close。对端先关闭则沿既有终止路径退休，不重建 session。
此接口不等待对端 close handshake 完成；snapshot.active_sessions/in_flight_frames 保持可查询。

resume 允许新 session，不清除旧 session 的关闭标记；它们不能重新接受数据。
关闭命令在 mutex 内取得单次提交权，在锁外调用 CHTTP。原生 close admission 失败时，
恢复该 session generation 的提交权并记录 last_status；quiesce 返回同步错误，
异步排空错误通过 snapshot 观察。调用方可显式再次 quiesce 重试。
session 保留 close_retry_required 标记，阻止迟到完成回调重新提交失败命令；
只有显式 quiesce 可以越过该标记，session 重建/停止时清除。
没有后台重试、临时队列、无界缓存或协议 fallback。

控制调用与 Flow start/stop/destroy 由调用方串行；CHTTP owner 回调和 Flow worker 可并发。
quiesce 的时间复杂度为 O(session_capacity)，附加空间 O(1)，不分配新内存。
所有 session、frame 和网络命令仍受原配置的硬容量约束。
QUIESCED 枚举追加，保留既有枚举值和 snapshot ABI；未调用新接口的运行方式保持原状。

验证范围包含 H1 与 RFC8441/H2：阻塞已接受帧、quiesce、拒绝后续帧、恢复时拒绝旧
session、先收到原 echo 后收到 close 1013。另验证空闲会话关闭、握手拒绝及新会话恢复。
测试专用编译单元注入 ENOBUFS，并在失败后调用完成路径的关闭 helper，确定性验证
迟到完成尝试和迟到输入均不隐式重试；显式重试仅提交一次关闭，停止后可重新启动。
该白盒测试验证竞态的关键状态边界，不替代上述真实 H1/H2 Graph 完成测试。
迁移时 plugin owner 使用这些入口；回滚代码时先按原 stop/destroy 协议排空旧 owner，
不通过静态链接或第二套 owner 状态代替 DLL 管理。
