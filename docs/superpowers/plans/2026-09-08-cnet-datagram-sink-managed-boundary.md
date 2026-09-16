# CNet Datagram Sink Managed Boundary 计划

**Issue:** #59（父项 #28，依赖 #54）

## 目标

将现有固定 peer 的 CNet datagram terminal sink 注册为一个原子的 CFlow managed Sink，
使控制面能够在不推进 I/O 的前提下查询稳定描述符、生命周期、容量、背压及
accepted/completed/rejected 结算事实。不得保留普通 async-terminal 注册 fallback，也不得绕过
CNet 直接访问 socket。

## 边界与事实源

- Graph/adapter schema 仍定义 terminal stage 的静态契约；managed descriptor 只投影同一个 sink
  owner，不建立第二个 registry 或状态机。
- `cnet_datagram` 是 native UDP 请求与 completion 的唯一事实源；CFlow IO Actor 是请求容量、
  admitted/ready/in-flight 与 acknowledge 的唯一事实源。
- publish 在 `cflow_io_actor_try_submit()` 成功后把 claim 和消息 payload 所有权转移给 operation；
  admission 失败时 claim 保持归调用方。
- `send_capacity=N` 同时是 Actor request capacity 与 managed queue capacity。占用达到 N 时第 N+1
  个请求同步返回 `SALTS_ENOSPC`；不扩容、不阻塞、不丢弃、不重试。
- accepted 仅在 Actor admission 成功后饱和递增；completed 仅在已接受 claim 首次成功 terminal
  complete 后递增；rejected 对每个 pre-admission failure 独立饱和递增。
- CNet callback 借用的 peer/view 不被保存。operation 持有的 claim 直到 matching tagged completion、
  Actor close cancellation 或 backend failure 才恰好完成并释放。

## 生命周期与并发

- 一个 lifecycle mutex/condition 串行化 start、poll、stop、snapshot 和 destroy 对 Actor/executor/
  datagram 可变生命周期的访问；并发 poll 返回 `SALTS_EBUSY`。
- submitter 可并发进入短 admission 窗口；`submissions_active` 防止 stop 在 claim move/Actor admission
  期间销毁 owner。managed snapshot 在该窗口返回 `SALTS_EBUSY`，不拼接不一致事实。
- stop 先停止新 admission，再关闭 Actor，然后持续推进 CNet 到权威 completion/cancellation，确认
  Actor quiescent 后依次销毁 Actor、executor 和 datagram。stop 与 start/poll 冲突时等待 owner lane。
- start failure 保留首个错误及 FAILED 状态，同时完成局部初始化资源清理；没有降级启动路径。
- Flow reset/destroy 调用 shutdown，先完成上述停止协议，再把 owner 标为 DETACHED；外层 sink handle
  只在 detached 且所有内部 owner 均已销毁后可释放。

## Managed 契约

- domain: `TURBO_FLOW_DOMAIN_IO_TRANSPORT`
- kind: `TURBO_FLOW_RESOURCE_CONNECTION`
- role: `TURBO_FLOW_MANAGED_BOUNDARY_SINK`
- capability: `TURBO_FLOW_MANAGED_BOUNDARY_DURABLE_SETTLEMENT`
- commands: 无
- input: `application/octet-stream`, schema `CNetDatagram/NonEmptyBytes/v1`
- resource generation 固定为 1。普通 adapter name 在 owner 上限内原样保留；超长 name 用 XXH3-128
  生成稳定、有界 owner identity，UID 为 `cnet-datagram-sink:<owner>`。

## 实施与验证

1. 先扩展 `test_cnet_datagram_sink`，覆盖原子 descriptor/snapshot、N/N+1 背压、成功/取消 exact-once、
   start failure、边界/超长 identity 与 duplicate UID rollback，并确认旧实现按预期失败。
2. 最小实现 managed provider、identity、计数和生命周期串行化；修正 CNet completion 到 IO Actor
   completion 的合法映射（CANCELLED 的 error 必须为 `SALTS_OK`，非成功 bytes 必须为 0）。
3. 更新 managed-boundary 文档及公开 datagram sink 生命周期说明。
4. 使用 `cmake --preset win-dev-user`，依次构建/运行目标测试、CNet 相邻测试、全量 CTest 与
   `install-win-dev-user`；检查格式和安装消费端。

兼容性风险限于新增资源枚举可见性和更严格的生命周期并发返回值；既有 adapter name、Graph DSL、
datagram config、native send 成功语义与公开结构布局保持不变。验证以真实 UDP 发送、容量饱和、失败
启动、停止结算和安装消费端为准。
