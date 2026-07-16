# FMQ release gate

本 gate 是 FMQ v2 在可信网络/进程边界内发布的最低可复验条件，不扩大
`PRODUCT_MATRIX.md` 的能力范围。

## 运行

本机必须已有 `127.0.0.1:6379` Redis，且测试可创建/删除带随机后缀的 key、stream 和 group。

```powershell
cmake --preset win-release-user -DTURBO_FLOW_REDIS_LIVE_TESTS=ON
cmake --build --preset win-release-user --parallel
ctest --preset win-release-user -N -L fmq-release
ctest --preset win-release-user -L fmq-release --output-on-failure
ctest --preset win-release-user --output-on-failure
```

`test_turbo_flow_redis_live` 必须出现在 `-N -L fmq-release` 列表中且不得显示 Disabled。Gate 和
全量回归都必须零失败；不能用未启用 live suite 的结果替代。

## 固定门槛

- CoroNet：覆盖两个 FMQ adapter 共用一个 pool lane，以及各自 owned/host-driven context；所有
  stop 后 connection 与 in-flight message/byte 必须归零。
- Reconnect：真实 TCP broker 连续 restart 12 轮，每轮只交付新消息、不 replay 已交付消息；最终
  stop 小于 500 ms。retry delay interrupt 的更严路径小于 250 ms。
- Slow peer/backpressure：逐 peer HWM 的 fail、drop-oldest、disconnect 均有显式计数和终态；不允许
  silent drop。
- Bounded state：credit owner 固定压力为 4096 correlation/256 worker，覆盖乱序 completion、全部
  generation expiry 与容量拒绝；topic journal、management dedup 和 deployment membership 同样受
  配置上限约束。
- Persistence：真实 Redis 覆盖 binary-safe Data SET/GET、Stream consumer pending restart replay、
  XACK 终态和 BLOCK interrupt（小于 500 ms）；fake-server suite 覆盖协议/连接失败。SQLite Queue
  覆盖 committed 与 stale in-flight restart recovery、bounded multi-claim 和原子 settlement。
- Management：TFMP parser、operation store、typed crash-window reconcile、failure-domain fencing 和
  rolling manifest 均必须通过；未知副作用不能自动重放。
- Graph/config：YAML resolver、disabled-kind preflight、Socket/FMQ/HTTP/RPC projection、compile/
  Observe、resource governance、start/stop/replace/resize generation 均属于同一 label gate。

## 性能趋势

`test_fmq` 和 `test_fmq_broker` 输出两类 `FMQ_BENCH_RESULT`：真实 TCP ROUTER/DEALER serialized echo
的 throughput/P50/P95/P99，以及 4096 in-flight credit owner 的 dispatch/complete P50/P95/P99。
发布流水线应保留完整 CTest 日志。相同 runner、Release preset、无其他负载时，任一 throughput
低于最近通过基线 80%，或任一 P99 高于基线 125%，都视为 release regression；首次建基线时同时
保存 runner CPU、OS、编译器和 commit。跨机器数值只作诊断，不直接比较。

固定正确性/时限阈值在测试内 fail fast；性能趋势阈值由流水线对
`FMQ_BENCH_RESULT` 记录比较，不能以一次本地快跑替代长期趋势。
