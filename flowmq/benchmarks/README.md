# FlowMQ benchmarks

Build the standalone benchmark with the repository Release preset:

```powershell
cmake --preset win-release-user -DBUILD_BENCHMARKS=ON
cmake --build --preset win-release-user --target bench_fmq
```

Run all cases or select one latency/throughput profile:

```powershell
.\build\Msvc-Release\bin\bench_fmq.exe
.\build\Msvc-Release\bin\bench_fmq.exe --filter "64-byte serialized round trip"
.\build\Msvc-Release\bin\bench_fmq.exe --filter "64-KiB serialized round trip"
.\build\Msvc-Release\bin\bench_fmq.exe --filter "DEALER ROUTER"
.\build\Msvc-Release\bin\bench_fmq.exe --filter "PUB SUB"
.\build\Msvc-Release\bin\bench_fmq.exe --filter "PUSH PULL"
.\build\Msvc-Release\bin\bench_fmq.exe --filter "async micro-batch"
```

Enable the benchmark-only send owner-lane profiler for one run:

```powershell
$env:FLOWMQ_BENCH_PROFILE='1'
.\build\Msvc-Release\bin\bench_fmq.exe --filter "64-byte serialized round trip"
Remove-Item Env:FLOWMQ_BENCH_PROFILE
```

The REQ/REP cases measure one serialized TCP request/reply per sample through two graph-native
FlowMQ application facades. Their byte throughput counts application payload in both directions.

The DEALER/ROUTER, PUB/SUB, and PUSH/PULL cases measure batches of 64 one-way 64-byte messages.
Their operation throughput is messages/s and byte throughput counts one-way application payload.
Each batch waits until the receiving callback has observed every message, so the result covers both
send submission and delivery to the application facade. Setup, connection/subscription
establishment, and warmup are outside all timed blocks. Stable `FMQ_BENCH_RESULT` lines report
throughput plus nearest-rank P50/P95/P99 round-trip or batch latency for same-runner trend
comparisons.

The asynchronous micro-batch cases also wait for every accepted per-message completion. They
therefore measure the facade delivery boundary plus receiver observation, rather than queue
admission alone. Their configured batch size matches the 64-message benchmark burst.

Profiled runs add an `FMQ_PROFILE_RESULT` line. Its values are per-request averages in nanoseconds:
`enqueue` ends when the request becomes visible in the send queue; `owner_wait` ends when the owner
drain dequeues it; `post_call` is the overlapping subset spent inside `coro_post`; `owner_dispatch`
ends at the first socket send; `completion` ends immediately before completion notification; and
`waiter_wake` ends when the synchronous sender resumes. Serialized sends also report request-level
`socket_send`, `socket_thread_cpu`, and `socket_estimated_off_cpu`. A batch shares one socket write,
so these request-level socket counters exclude shared writes instead of attributing the same wall
time to every request.

Batch and asynchronous micro-batch runs add `FMQ_PROFILE_BATCH_RESULT`. It reports actual socket
call, frame, and iovec-segment totals plus per-call socket wall time, owner OS-thread CPU time, and
estimated off-CPU time. Its lifecycle averages are per batch: `build` covers the entire request
build through queue visibility; `payload_prepare` covers buffer allocation/retention, copying, and
message-view setup; `graph_publish` covers the synchronous batch call, including its producer;
`adapter_consume` is the subset spent encoding and constructing FlowMQ send requests;
`graph_runtime` is the remainder after producer and adapter work; `enqueue_prepare` covers iovec preparation
and queue commit through visibility; `owner_wait` ends when the owner dequeues the batch;
`owner_work` covers owner-side selection, authorization, socket calls, and completion; and
`waiter_wake` ends when the submitting thread resumes. Because the coroutine may yield while other
owner-lane work runs, the CPU value can include unrelated loop work and the off-CPU value is a
conservative estimate rather than exact per-coroutine time. A platform without a thread CPU clock
reports `socket_cpu_samples=0`.
Windows reports thread CPU in coarse accounting ticks, so prefer the longer one-way cases when
interpreting CPU/off-CPU ratios; the profiler derives off-CPU from aggregate totals rather than
subtracting individual messages.
Profiling is disabled by default because the additional clock reads and atomic aggregation perturb
the measured throughput.
