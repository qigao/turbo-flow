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

Profiled runs add one `FMQ_PROFILE_RESULT` line. All values are per-request averages in
nanoseconds: `enqueue` ends when the request becomes visible in the send queue; `owner_wait` ends
when the owner drain dequeues it; `post_call` is the overlapping subset spent inside `coro_post`;
`owner_dispatch` ends at the first socket send; `socket_send` spans the request's socket writes;
`socket_thread_cpu` is CPU consumed by the owner OS thread during that wall-clock interval;
`socket_estimated_off_cpu` is `socket_send - socket_thread_cpu`, clamped to zero; `completion` ends
immediately before completion notification; `waiter_wake` ends when the synchronous sender
resumes. Because the coroutine may yield while other owner-lane work runs, the CPU value can
include unrelated loop work and the off-CPU value is a conservative estimate rather than exact
per-coroutine time. A platform without a thread CPU clock reports `socket_cpu_samples=0`.
Windows reports thread CPU in coarse accounting ticks, so prefer the longer one-way cases when
interpreting CPU/off-CPU ratios; the profiler derives off-CPU from aggregate totals rather than
subtracting individual messages.
Profiling is disabled by default because the additional clock reads and atomic aggregation perturb
the measured throughput.
