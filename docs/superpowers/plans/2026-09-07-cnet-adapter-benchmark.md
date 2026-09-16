# CNet Adapter Benchmark Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a reproducible raw-UDP packet-terminal benchmark that closes TurboFlow issue #39 with throughput, tail latency, CPU, bounded-resource, saturation-recovery, shutdown, and allocation-accounting evidence.

**Architecture:** Keep all measurement code under `io/cnet/benchmarks`; link the installed-shape `TurboFlow::CNetAdapter` target so the measured path is the production DLL. A small private statistics unit computes nearest-rank percentiles plus median/MAD and is compiled into both the benchmark and a TinyTest executable. The benchmark uses one prebuilt `mem_buffer_t` payload, fixed capacities, a loopback CNet peer, seven independent replicates, and machine-readable `CNET_BENCH_*` records.

**Tech Stack:** C11, TurboFlow::Graph, TurboFlow::CNetAdapter, Salts::CNet, Salts::CFlow IO Actor, Salts::TinyTest, CMake presets.

**Spec:** GitHub issue #39 (`perf(cnet): establish packet adapter throughput and tail-latency baseline`).

## Global Constraints

- No TurboNet, TurboHttp, TurboParser, legacy transport alias, compatibility layer, or fallback path.
- Do not modify public APIs or the production CNet/Graph hot path for measurement.
- Raw UDP loopback, 256-byte payload, one producer, one async-ingress worker, explicit fixed capacities, warmup separated from samples, seven replicates.
- Release `win-release-user` produces baseline numbers; Debug/ASan validates correctness only.
- `allocations/message` counts TurboFlow-owned accepted-message allocation events at the async task, publication, and terminal-claim owner boundaries; RSS is not an allocation count.
- First baseline records median and MAD but sets no regression threshold without comparative history.

---

### Task 1: Private statistics contract

**Files:**
- Create: `io/cnet/benchmarks/cnet_adapter_benchmark_stats.h`
- Create: `io/cnet/benchmarks/cnet_adapter_benchmark_stats.c`
- Create: `io/cnet/tests/test_cnet_adapter_benchmark_stats.c`
- Modify: `io/cnet/tests/CMakeLists.txt`

**Interfaces:**
- Consumes: `SALTS_OK`, `SALTS_EINVAL`, `SALTS_ERANGE`, `SALTS_ENOMEM`.
- Produces: `tf_cnet_benchmark_percentile_u64(const uint64_t *, size_t, unsigned int, uint64_t *)` and `tf_cnet_benchmark_summarize(const double *, size_t, tf_cnet_benchmark_summary_t *)`.

- [ ] **Step 1: Write failing TinyTest cases**

```c
spec("CNet adapter benchmark statistics") {
  it("uses deterministic nearest-rank percentiles") {
    const uint64_t values[] = {50u, 10u, 40u, 30u, 20u};
    uint64_t result = 0u;
    check_equal(tf_cnet_benchmark_percentile_u64(values, 5u, 95u, &result), SALTS_OK);
    check_equal(result, UINT64_C(50));
  }
  it("summarizes independent replicates with median and MAD") {
    const double values[] = {30.0, 10.0, 200.0, 20.0, 5.0};
    tf_cnet_benchmark_summary_t result = {0};
    check_equal(tf_cnet_benchmark_summarize(values, 5u, &result), SALTS_OK);
    check_equal(result.median, 20.0);
    check_equal(result.mad, 10.0);
  }
}
```

- [ ] **Step 2: Register and run the focused test to verify it fails**

Run: `cmake --build --preset win-dev-user --target test_cnet_adapter_benchmark_stats`

Expected: compilation/link failure because the statistics functions do not exist.

- [ ] **Step 3: Implement checked scratch allocation, sorting, nearest-rank percentile, median, and MAD**

```c
typedef struct tf_cnet_benchmark_summary_s {
  double median;
  double mad;
} tf_cnet_benchmark_summary_t;

int tf_cnet_benchmark_percentile_u64(const uint64_t *values, size_t count,
                                     unsigned int percentile, uint64_t *out);
int tf_cnet_benchmark_summarize(const double *values, size_t count,
                                tf_cnet_benchmark_summary_t *out);
```

Reject null, empty, non-finite, non-positive, overflowed, or percentile-out-of-range input with a concrete Salts error.

- [ ] **Step 4: Run focused tests**

Run: `ctest --preset win-dev-user -R test_cnet_adapter_benchmark_stats --output-on-failure`

Expected: one test executable passes.

### Task 2: Production-shape raw UDP benchmark

**Files:**
- Create: `io/cnet/benchmarks/CMakeLists.txt`
- Create: `io/cnet/benchmarks/bench_cnet_adapter.c`
- Modify: `io/cnet/CMakeLists.txt`

**Interfaces:**
- Consumes: `TurboFlow::CNetAdapter`, `cnet_packet_endpoint_*`, `turbo_flow_publish_async`, `turbo_flow_cnet_packet_sink_poll`, the Task 1 statistics functions.
- Produces: executable target `bench_cnet_adapter` and records beginning `CNET_BENCH_ENV`, `CNET_BENCH_RUN`, and `CNET_BENCH_SUMMARY`.

- [ ] **Step 1: Add the benchmark target and verify the empty target fails**

```cmake
cmake_add_benchmark(
  bench_cnet_adapter
  SOURCES bench_cnet_adapter.c cnet_adapter_benchmark_stats.c
  FOLDER "io/cnet/benchmarks"
  LIBS TurboFlow::CNetAdapter Salts::TinyTest
  INCLUDES ${CMAKE_CURRENT_SOURCE_DIR})
```

Run: `cmake --build --preset win-dev-user --target bench_cnet_adapter`

Expected: failure until `bench_cnet_adapter.c` defines the TinyTest benchmark spec.

- [ ] **Step 2: Implement fail-fast fixture lifecycle**

Create one bounded receiving `cnet_packet_endpoint`, one graph with `source input -> stage output adapter cnet.packet.out`, one packet sink, and one reusable 256-byte `mem_buffer_t`. Every init, parse, compile, start, poll, stop, and destroy result must be checked; a deadline breach fails the benchmark rather than reducing load or changing backend.

- [ ] **Step 3: Implement one warmup and seven measured replicates**

For each message, write its publish timestamp into a preallocated completion slot, submit up to the explicit in-flight bound, poll the sink and peer on their owner lane, and write callback latency into the same slot. Compute throughput over the first-admission-to-last-terminal interval and process CPU over that identical interval. Use `benchmark_io` only for TinyTest's aggregate timing; custom output carries the per-message percentiles.

- [ ] **Step 4: Implement bounded saturation recovery and shutdown measurement**

Fill the one-entry sink path without polling until at least one publication completes with `SALTS_ENOSPC`, then resume progress and require a later message to complete with `SALTS_OK`. After `active_requests == 0`, measure `turbo_flow_stop`, `turbo_flow_destroy`, sink destroy, peer stop, and peer destroy without leaving an accepted claim unresolved.

- [ ] **Step 5: Emit machine-readable environment, run, and summary records**

```text
CNET_BENCH_ENV scenario=udp_packet_terminal preset=win-release-user payload_bytes=256 replicates=7 ...
CNET_BENCH_RUN replicate=1 throughput_msg_s=... p50_ns=... p95_ns=... p99_ns=... cpu_wall_ratio=... ...
CNET_BENCH_SUMMARY metric=throughput_msg_s median=... mad=...
```

Include `allocation_events_per_message=3`, `allocation_scope=turbo_flow_owner_objects`, the three source locations, `retained_payload_bytes_max = peak_active_requests * 256`, rejected count, recovered count, and shutdown microseconds.

- [ ] **Step 6: Build and run Debug/ASan correctness**

Run: `cmake --build --preset win-dev-user --target bench_cnet_adapter && build/Msvc/bin/bench_cnet_adapter.exe`

Expected: all benchmark assertions pass; numbers are explicitly labelled non-baseline because ASan is enabled.

### Task 3: Release baseline and documentation

**Files:**
- Create: `docs/performance/CNET_ADAPTER_BASELINE.md`

**Interfaces:**
- Consumes: `bench_cnet_adapter` machine-readable output.
- Produces: reproducible issue #39 baseline evidence and limitations for future #11 gates.

- [ ] **Step 1: Configure and build Release**

Run: `cmake --preset win-release-user && cmake --build --preset win-release-user --target bench_cnet_adapter`

Expected: the dedicated benchmark target builds against Release dependencies.

- [ ] **Step 2: Run the complete benchmark once, containing seven internal replicates**

Run: `build/Msvc-Release/bin/bench_cnet_adapter.exe`

Expected: seven `CNET_BENCH_RUN` lines and median/MAD summaries with no assertion failure.

- [ ] **Step 3: Record exact provenance, formulas, output, and limitations**

Document OS, CPU, logical processors, compiler, preset, commit, payload/capacities/warmup/samples/replicates, the measured output, percentile/throughput/CPU/MAD formulas, allocation-site evidence, and that this baseline covers raw UDP packet terminal only—not TCP/TLS/KCP/CHTTP/TurboDb or an enforceable regression percentage.

- [ ] **Step 4: Run focused, adjacent, and full verification**

Run:

```text
ctest --preset win-dev-user -R "test_cnet_adapter_benchmark_stats|test_cnet_packet_sink" --output-on-failure
ctest --preset win-dev-user -L cnet-adapter --output-on-failure
ctest --preset win-dev-user --output-on-failure
ctest --preset win-release-user --output-on-failure
```

Expected: all selected and full test sets pass.

- [ ] **Step 5: Review, commit, open PR, and link evidence**

Commit only the benchmark, tests, CMake, plan, and baseline document. Open a PR with `Closes #39`, link #5 and #11, wait for required CI, merge only after no HIGH/MED review findings remain, and update the parent checklists with the baseline evidence.
