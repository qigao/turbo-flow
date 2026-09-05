# TurboFlow Benchmarks

Build and run the Release benchmark target:

```powershell
cmake --build --preset win-release-user --target bench_turbo_flow
build\Msvc-Release\bin\bench_turbo_flow.exe
```

TinyTest filters can isolate a group:

```powershell
build\Msvc-Release\bin\bench_turbo_flow.exe --filter "executor metrics"
build\Msvc-Release\bin\bench_turbo_flow.exe --filter "publish"
build\Msvc-Release\bin\bench_turbo_flow.exe --filter "async ingress"
build\Msvc-Release\bin\bench_turbo_flow.exe --filter "teardown"
build\Msvc-Release\bin\bench_turbo_flow.exe --filter "expression"
build\Msvc-Release\bin\bench_turbo_flow.exe --filter "security"
```

Executor metrics warm up 1000 messages and measure 10000 messages with a
256-byte payload. Each `BENCH_RESULT` line identifies the `stage_plan`,
executor, worker/lane count, payload size, iterations, throughput, and
P50/P95/P99 single-publish latency. `salts_hrtime()` supplies monotonic nanoseconds. The
per-message latency includes two clock reads; throughput uses one outer elapsed
interval and is the better metric for comparing total work.

The publish group includes matching stateless/keyed baselines, count-based keyed
windows, and event-time tumbling windows. Event-time measurements separate the
per-event accumulator hot path from explicit watermark advancement over 1024
active windows; the latter includes the documented active-window scan and
deterministic candidate ordering.

Thread measurements cover 1, 2, 4, and 8 workers. Coroutine measurements cover
two lanes with and without a 128-shell pool. Worker cross-ring measures a
four-way worker data segment with real consumer threads and includes the
synchronous ring handoff plus completion wait. Teardown covers idle executor
shells and executors after a completed job.

Executor metrics also include four-producer runs for `worker 4 capacity 1024`
and `exec coro lanes 4 pool 128`. Their latency samples are merged across
producer-local message envelopes, while throughput uses the common start-to-
join interval. This measures MPMC ring admission and per-lane coroutine pool
behavior without sharing mutable benchmark messages.

The async-ingress benchmark submits 10000 cloned seven-byte messages through a
single Flow-owned ingress worker with capacity 16384, then waits for every
completion before teardown. Its reported operation latency is producer-side
bounded admission cost; the completion wait keeps end-to-end work outside the
timed submit loop while still verifying that no accepted message is lost.

The same worker-pool run first reports `BENCH_IDLE` over a 500 ms empty-ring
window. `cpu_wall_ratio` uses C process CPU time divided by monotonic wall time;
it verifies that waiter-aware Disruptor workers park rather than continuously
polling. The following throughput result checks the loaded path after those
workers are awakened.

TurboFlow currently requires thread and coroutine executor jobs to complete
before `turbo_flow_publish()` returns. A live-job teardown benchmark would
therefore require a future asynchronous completion contract; idle jobs are not
reported as live work.

Expression benchmarks separately measure parser/type-check cost, MIR
interpreter/JIT compilation, and evaluation of the same finalized typed
expression. The benchmark label identifies the expression shape and selected
backend; compile iterations are 200 and evaluation iterations are 100000.

The security benchmark places 64, 512, and 4096 rules in the same Domain/action/resource
bucket. `acl-subject-index` uses distinct subjects, while `acl-exact-index`, `acl-prefix-index`, and
`acl-adapter-compiled` place every pattern under one subject. This separates subject, exact,
prefix, and protocol matcher costs and catches a leaf regressing to a rule-count-proportional scan.

Exact patterns use a direct hash leaf. Prefix patterns use hash leaves probed by each prefix of the
requested resource, so lookup is bounded by resource length rather than the number of rules. A
reference Windows Release run sustained approximately 1.46-1.70M decisions/s for subject/exact,
0.69-0.71M decisions/s for prefix, and 1.48-1.58M decisions/s for a compiled adapter across 64, 512,
and 4096 rules. Adapter rules are compiled once per immutable subject leaf; authorization emits
leaf-local positions that Core bounds-checks before applying deny precedence and original rule
ordering. External protocol products may use their own trie while preserving distinct topic-match and
filter-containment semantics.
