# Flowie benchmarks

Build and run the capacity benchmark with the repository presets:

```powershell
cmake --build --preset win-release-user --target bench_flowie
build\Msvc-Release\bin\bench_flowie.exe --filter "MQTT typed projection facts"
build\Msvc-Release\bin\bench_flowie.exe --filter "100k session and topic-index capacity"
build\Msvc-Release\bin\bench_flowie.exe --filter "compiled MQTT security matcher"
build\Msvc-Release\bin\bench_flowie.exe --filter "100k wildcard/shared rebuild"
build\Msvc-Release\bin\bench_flowie.exe --filter "real TCP MQTT pipeline burst"
build\Msvc-Release\bin\bench_flowie.exe --filter "100k live TCP MQTT selector and packet fan-out"
build\Msvc-Release\bin\bench_flowie.exe --filter "real TCP stalled-subscriber isolation"
```

The `MQTT typed projection facts` benchmark compares repeated full-schema rule reads from an
opaque MQTT 5 PUBLISH with reads from an ingress-style buffer-owned projection. Both rows evaluate
the same 15 fields in batches of 64; setup, payload copy, and projection binding are outside the
timed blocks. The opaque row reparses wire bytes on every facts-provider call, while the bound row
uses stable field IDs and branch-local inline bitmaps.

The benchmark holds 100,000 internal session owners concurrently, then builds a 100,000-filter
derived MQTT trie containing exact, `+`, `#`, and shared filters. It reports create/CONNECT rate,
index build/removal rate, match throughput, and match P50/P95/P99 latency. The removal pass deletes
all 100,000 bound entries and exercises empty-branch pruning without retaining historical nodes.

The `compiled MQTT security matcher` workload builds complete SecurityRealm snapshots with 64, 512,
and 4,096 adapter rules under one role leaf. It measures allocation-free authorization after setup:
PUBLISH uses concrete Topic Name matching, while SUBSCRIBE uses Topic Filter language containment.
Rule creation, snapshot compilation, and trie construction are outside the timed blocks. Separate
rows isolate UTF-8/topic validation, validation plus trie lookup, trie traversal after parser
validation, untrusted programmatic SecurityRealm calls, and Flowie's parser-validated endpoint path.
Each security row uses 1,000 samples with 64 real operations per sample to amortize timer and
scheduler noise; comparisons must retain both constants.

The separate wildcard/shared benchmark rebuilds an index containing 100,000 matching filters eight
times, then performs 256 matches that each return exactly 100,000 candidate entry indices. It
measures derived-index rebuild and candidate enumeration, not endpoint subscription merging,
croaring shared-member selection, packet encoding, or 100,000 simultaneous TCP connections.

The `real TCP MQTT fan-out` benchmark starts one real Flowie TCP endpoint, connects one publisher
and 16 MQTT 5 subscribers, subscribes every recipient to `bench/#`, and runs 1,000 QoS 0
publications. One sample contains one inbound packet plus 16 complete outbound deliveries. TinyTest
reports delivery operations per second and application-wire bytes per second; the additional
`FLOWIE_BENCH_RESULT` line reports latency from publisher send through the final subscriber receive.
The byte count includes the MQTT packet once on ingress and once per subscriber, not TCP/IP framing.

The `real TCP MQTT pipeline burst` benchmark sends 64 MQTT PUBLISH packets in one TCP write from one
publisher, then byte-compares all 64 deliveries at one subscriber. It isolates same-connection reply
queue batching: one sample includes 64 ingress packets and 64 outbound packets. It does not measure
cross-connection fan-out, where every subscriber still requires its own socket operation.

The `real TCP MQTT connection churn` benchmark performs 500 complete TCP connect, MQTT 5 CONNECT /
CONNACK, socket close, and owner-registry cleanup cycles against one running endpoint. Its sample
latency therefore includes network establishment, protocol admission, reply delivery, close
detection, and return of `connections_current` to zero; it is not a raw socket-connect benchmark.

The `100k live TCP MQTT selector and packet fan-out` workload is the deployment-capacity gate. It
creates one publisher plus 100,000 real MQTT 5 subscribers, gives every subscriber one `bench/#`
subscription, checks the Connection and ProtocolAggregate counts, sends one QoS 0 PUBLISH, and
byte-compares the complete wire packet at all 100,000 recipients. Explicit source-loopback address
and port windows rotate every 8,192 connections so one local ephemeral-port range is not mistaken
for the broker limit. `FLOWIE_BENCH_LIVE_TCP_CONNECTIONS` may lower the subscriber count for
diagnostics; only the default exact 100,000 run closes the gate.

Connection, session, subscription, and reply state are bounded O(N); one-packet fan-out is O(N).
Committed SUBSCRIBE/UNSUBSCRIBE mutations update stable selector entries on the owner lane; a full
rebuild is reserved for startup, clean-start replacement, or explicit invalid-state repair. The
published live-TCP setup timing predates that incremental path and remains functional-capacity
evidence rather than a current mutation-throughput measurement. The endpoint derives private CoroNet pool capacity from
`max_connections` and exposes private-context `coroutine_stack_size` plus `stream_recv_buffer_bytes`. The
workload uses a 64 KiB coroutine stack and 4 KiB for each of CoroNet's two receive chunks. MQTT
framing reassembles across chunks, so receive capacity is independent of `max_packet_size`.

The `real TCP stalled-subscriber isolation` benchmark keeps one publisher and one healthy
subscriber connected, then repeats eight slow-subscriber cycles. The slow client advertises a
1 KiB receive buffer and stops reading before four 512 KiB QoS 0 publications. Every cycle must
disconnect only that client at the configured send HWM, retain the two healthy connections, and
deliver all four packets byte-for-byte to the healthy subscriber. The TinyTest throughput includes
slow-client CONNECT/SUBSCRIBE and disconnect detection; the additional percentiles cover only the
four publisher-send through healthy-receive operations.

Local Windows/MSVC Release reference (same machine, not a portable SLA). Network figures are from
2026-07-16; selector figures were rerun on 2026-07-17 after incremental removal was added; security
matcher figures were measured on 2026-07-19:

- MQTT facts reads (2026-07-21, one 15-field Release run): opaque wire parse 1.53 million/s
  (0.654 us), bound typed projection 18.85 million/s (0.053 us), a 12.3x field-read improvement.
  This excludes ingress copy/bind, Queue handoff, graph scheduling, and network I/O.
- Historical single-operation 4,096-rule SecurityRealm before validated traversal: PUBLISH
  536,659/s (1.863 us), SUBSCRIBE 482,777/s (2.071 us). The current batched parser-proven endpoint
  path reaches 1,262,439/s (0.792 us) and 1,209,935/s (0.826 us), using three-run medians.
- Same-method batched Core A/B: removing the static-realm refresh probe and duplicate request
  validation raised PUBLISH authorize from 1,070,220/s to 1,262,439/s (+18.0%) and SUBSCRIBE from
  1,075,661/s to 1,209,935/s (+12.5%). Dynamic `policy_source` realms retain version/expiry refresh.
- Parser-proven trie traversal alone: PUBLISH 0.373 us, SUBSCRIBE containment 0.389 us. The endpoint
  fast path remains allocation-free and retains the same immutable snapshot and deny precedence.
- 16-recipient fan-out: 24,280 deliveries/s; P50/P95/P99 636.8/856.8/1173.5 us.
- 64-message same-connection pipeline (2026-07-18, seven-run median): 149,606 messages/s;
  P50/P95/P99 414.3/514.2/678.6 us per 64-message burst. The pre-batch baseline was 51,450
  messages/s and 1228.4/1419.7/1559.5 us.
- connect-close churn: 959 complete cycles/s; P50/P95/P99 1008.8/1342.8/1755.6 us.
- 100k unique-filter build/remove: 313,287 inserts/s and 382,310 removals/s; unique-topic match
  P50/P95/P99 1.3/2.2/2.9 us.
- 100k-filter wildcard/shared rebuild: 2.26 million inserts/s; P50/P95/P99
  43.72/47.31/47.31 ms.
- 100k-candidate match: 51.40 million candidates/s; P50/P95/P99 1.83/2.64/2.78 ms.
- stalled-subscriber isolation, three repeated runs: 89-102 healthy deliveries/s;
  four-delivery P50 35.7-42.5 ms and P95/P99 56.6-58.0 ms.
- 10k live TCP: 10,000 connected/delivered, 15.72 s setup, 227.97 ms fan-out.
- 50k live TCP: 50,000 connected/delivered, 701.02 s setup, 4.64 s fan-out, about 3.07 GiB peak
  private commit.
- 100k live TCP: 100,000 connected/delivered, 2,914.37 s setup, 19.56 s fan-out, about 5.93 GiB peak
  private commit; final status 0.

Keep the workload constants and build profile unchanged for comparisons. Use CI history rather than
one run to set regression gates.
