# CNet Adapter Raw UDP Baseline

This document records the first reproducible TurboFlow CNet adapter baseline for issue #39. It is descriptive evidence for #5 and #11, not yet a regression threshold.

## Scope and ownership

The measured path is:

```text
one producer
  -> TurboFlow bounded async ingress (one worker)
  -> Graph async terminal claim
  -> CFlow IO Actor (64 commands / 64 active requests)
  -> CNet raw UDP tagged send
  -> authoritative Graph completion callback
  -> bounded peer progress and exact receive-count verification
```

The Graph owns async task, publication, and terminal-claim state. The packet sink owns its fixed operation slots and the single CNet progress lane. The peer endpoint has a separate caller-owned progress lane. The benchmark never polls either owner concurrently or reentrantly.

## Reproduction contract

Run from an x64 Visual Studio 2022 Developer Command Prompt:

```powershell
cmake --preset win-release-user
cmake --build --preset win-release-user --target bench_cnet_adapter
build\Msvc-Release\bin\bench_cnet_adapter.exe
```

The executable fails rather than changing backend, capacity, payload, sample count, or timeout when setup, admission, progress, receive verification, saturation recovery, or shutdown violates the contract.

Fixed inputs:

| Input | Value |
| --- | ---: |
| Protocol/backend | raw UDP / IOCP |
| Address | IPv4 loopback |
| Payload | 256 bytes, one prebuilt shared `mem_buffer_t` |
| Warmup | 256 messages per replicate |
| Measured sample | 131,072 messages per replicate |
| Replicates | 7 independent fixtures |
| Producer / ingress workers | 1 / 1 |
| Async-ingress queue | 128 messages / 32,768 logical retained bytes |
| Packet sink / Actor capacity | 64 / 64 |
| Deadline | 20,000 ms per bounded phase |

## Metric definitions

- `throughput_msg_s = completed_messages * 1,000,000,000 / measured_wall_ns`. The interval starts immediately before the first measured publish attempt and ends at the timestamp captured inside the final authoritative Graph completion callback. Fixture setup, warmup, peer catch-up, saturation, and shutdown are excluded, including peer polls that occur after that callback in the same progress call.
- Per-message latency starts immediately before `turbo_flow_publish_async` and ends in its unique Graph completion callback. P50/P95/P99 use nearest rank: sort `N` samples and select rank `ceil(percentile * N / 100)`.
- `cpu_wall_ratio = process_cpu_ns / measured_wall_ns`, where process CPU is Windows kernel plus user time around the same measured batch. The start CPU sample is taken immediately before the wall start; the final callback captures the wall end and then takes the CPU end sample before returning. Values above 1.0 are valid because the Flow ingress worker and progress owner can consume different cores. The 131,072-message interval lasts about 0.72--0.83 seconds here, long enough to span roughly 46--53 observed 15.625 ms Windows CPU-accounting ticks instead of the 1--3 ticks seen in the rejected short-window design.
- Replicate summaries report `median(x)` and `MAD = median(abs(x - median(x)))`. Samples from independent replicates are not pooled.
- `retained_payload_bytes_max = peak_active_requests * 256`. This is the logical payload-retention bound at the packet terminal, not RSS and not an assertion that the shared benchmark buffer is physically copied once per request.
- `allocation_events_per_message = 3` counts TurboFlow-owned steady-state owner allocations for an accepted `mem_buffer_t` message: async ingress task, async publication, and async terminal claim. Fixture allocation and the one prebuilt payload buffer are outside the measured message scope. RSS/working-set deltas are not used as allocation counts.
- Saturation deliberately submits 65 messages to the 64-entry sink without progressing it, requires at least one concrete `SALTS_ENOSPC` completion, drains every accepted claim, then requires a new message to complete successfully.
- `shutdown_us` covers Flow stop/destroy, packet-sink handle destroy, and peer endpoint stop/destroy. A snapshot must first report `pre_shutdown_active_requests=0`; otherwise the run fails before the timer starts.

The TinyTest `benchmark_io` table is labeled `harness total including peer drain and statistics`. It deliberately covers the whole harness call and is diagnostic only; it includes peer catch-up and percentile allocation/sorting. `CNET_BENCH_RUN throughput_msg_s` and the corresponding `CNET_BENCH_SUMMARY` are the authoritative baseline metrics defined above.

Before emitting any measurement, the executable queries Git again, requires live `HEAD` to equal its compiled commit, and requires `git status --porcelain=v1 --untracked-files=normal -- .` to be empty. A dirty tree fails with `SALTS_EBUSY`; an executable built for a different commit fails with `SALTS_EPROTO`. Thus ignored build products are allowed, but tracked changes and untracked source/document files cannot silently contaminate a baseline.

Allocation evidence at source commit `0f6767f3db51983bcd927bc77520ef54abba1b69`:

| Owner allocation | Source |
| --- | --- |
| Async ingress task | `turbo_flow/src/flow_async_ingress.c:227` |
| Async publication | `turbo_flow/src/flow_async_terminal.c:95` |
| Async terminal claim | `turbo_flow/src/flow_async_terminal.c:322` |

The packet sink allocates its operation and delivered-ID arrays during registration (`io/cnet/src/turbo_flow_cnet_packet_sink.c:776` and `:779`); its tagged-send path uses those fixed slots and adds no per-message heap allocation.

## Baseline environment

- Date: 2026-09-07
- Source commit: `0f6767f3db51983bcd927bc77520ef54abba1b69`
- Runtime-validated source state: clean (`source_dirty=0`)
- Preset/build type: `win-release-user` / `Release`
- AddressSanitizer/baseline eligibility: `asan=0` / `baseline_eligible=1`
- OS: Microsoft Windows 11 家庭版 中文版, version 10.0.26200, build 26200
- CPU: AMD Ryzen 9 7940HX with Radeon Graphics, 16 physical / 32 logical cores
- Visible memory: 15.2 GiB
- Compiler: MSVC 19.44.35217 (`_MSC_VER=1944`)
- CMake/Ninja: 4.1.1 / 1.13.1

## Baseline results

Every replicate observed `peak_active_requests=64`, `retained_payload_bytes_max=16384`, `saturation_rejected=1`, `saturation_recovered=1`, and `pre_shutdown_active_requests=0`.

| Replicate | Throughput msg/s | P50 ns | P95 ns | P99 ns | CPU/wall | Shutdown us |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 158,530.650 | 365,800 | 439,400 | 560,700 | 1.190595 | 403.000 |
| 2 | 179,825.300 | 324,600 | 365,500 | 474,400 | 1.243337 | 335.200 |
| 3 | 181,581.261 | 322,000 | 363,600 | 460,500 | 1.168893 | 544.600 |
| 4 | 181,581.689 | 319,800 | 354,800 | 485,600 | 1.190542 | 538.900 |
| 5 | 166,963.256 | 348,300 | 445,200 | 521,200 | 1.154407 | 402.600 |
| 6 | 162,701.133 | 352,200 | 484,800 | 531,400 | 1.202520 | 386.700 |
| 7 | 166,194.688 | 348,600 | 458,800 | 531,700 | 1.168905 | 427.400 |

| Metric | Median | MAD |
| --- | ---: | ---: |
| Throughput (msg/s) | 166,963.256 | 8,432.606 |
| P50 (ns) | 348,300 | 17,500 |
| P95 (ns) | 439,400 | 45,400 |
| P99 (ns) | 521,200 | 35,600 |
| CPU/wall ratio | 1.191 | 0.022 |
| Shutdown (us) | 403.000 | 24.400 |

The same harness also completed under `win-dev-user` with AddressSanitizer enabled. Debug/ASan numbers are intentionally excluded from the release baseline.

## Interpretation and limits

This establishes that the current raw-UDP packet terminal can sustain a bounded 64-request pipeline, preserve authoritative completion, recover after explicit saturation, and shut down with no unresolved claim on this machine. It does not establish a statistically justified allowed regression percentage; future comparable runs are needed before #11 can turn this baseline into a numeric gate.

This result does not cover TCP, TLS, Pipe, KCP, secure KCP/FEC, CHTTP, TurboDb, multiple producers, different payload sizes, power-management variance, or cross-platform comparisons. Those remain separate #11 matrix slices and must not reuse these numbers as their threshold.
