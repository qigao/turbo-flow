# TurboFlow Observe

`TurboFlow::Observe` is an opt-in metrics collector outside core. Core exposes
one read-only observer callback slot but does not depend on this module.

Attach before `turbo_flow_start()` and detach after stop, before destroying the
Observe object. Destroying the flow first automatically clears the attachment:

```c
turbo_flow_observe_t *observe = turbo_flow_observe_create(NULL);
turbo_flow_observe_attach(observe, flow);
turbo_flow_start(flow);
/* publish */
turbo_flow_stop(flow);
turbo_flow_observe_detach(observe);
turbo_flow_observe_destroy(observe);
```

One flow accepts one observer. Callbacks receive const message views and must
not re-enter the same flow. With no observer attached, timing is skipped. With
Observe attached, aggregate counters are atomic; per-stage series use one
CSTL vector protected by one Salts mutex and are bounded by `max_stages`
(default 256, hard limit 4096). Series beyond the bound increment
`dropped_stage_series` rather than allocating without limit.

The snapshot reports message/stage counts, errors, total/max latency, and
adapter start/stop/error counts. Stage latency covers the complete synchronous
executor/adapter dispatch, including thread handoff or coroutine ticks.

## Graph Snapshot And Control

`turbo_flow_observe_graph_snapshot()` combines runtime admission/topology,
traffic, pool saturation, typed adapter connection gauges, and Salts OS
CPU/memory/load into caller-owned storage. Serialize graph pulls with
parse/compile/start/stop/reset/destroy; message, pool, and connection counters
remain safe to update while a pull is in progress. Adapter connection details
are available with `turbo_flow_adapter_connection_snapshot_at()`. Unsupported
adapters return `SALTS_ENOTSUP`.

`turbo_flow_observe_control_facts()` captures the same traffic, OS, and graph
aggregates into caller-owned backing storage and returns a typed fact provider
for `turbo_flow_control_ex()`. Rules can use `traffic.*`, `system.*`, and
`graph.*`; evaluation is read-only and the caller serializes the subsequent
single typed command with lifecycle/configuration operations.

The core control primitives are explicit desired-state commands:

```c
turbo_flow_pause(flow);          /* idempotently close new admission */
turbo_flow_drain(flow, 5000);    /* keep paused and wait for accepted publishes */
turbo_flow_resume(flow);         /* idempotently reopen admission */
```

`turbo_flow_drain(flow, 0)` is a non-blocking check and `UINT64_MAX` waits
without a deadline. These calls do not mutate snapshots and do not own adapter
queues or external connections. A host reconcile loop should read observed
state, decide desired state, and issue commands; runtime and adapters remain the
only owners of live state.

`turbo_flow_observe_reconcile_pool()` provides one synchronous host-owned tick
for runtime pools. The caller owns the loop and persistent reconcile state. A
policy selects a stage and pool kind, min/max parallelism, scale steps,
high/low utilization watermarks, consecutive-observation hysteresis, cooldown,
drain deadline, and an optional per-core 1-minute OS load guard. The helper
reads observed snapshots and delegates the final generation comparison and
single checked command to `turbo_flow_resource_reconcile_tick()` only after the
policy permits it; it never creates a background controller thread.

Cross-owner pool changes use the core caller-owned resize workflow. It advances
one operation per tick through ingress quiesce, graph drain, pool resize, graph
resume, and ingress resume. A failed phase remains explicit until the caller
requests a retry; Observe does not own or schedule that workflow.

Pool resize closes core publish admission but does not pause source adapters.
An adapter that publishes during the resize window receives `SALTS_ESHUTDOWN`
and remains responsible for its documented retry or message-retention policy.

## Control Events

External adapters can report connection/control-plane events with
`turbo_flow_observe_record_control_event()`. Observe stores only generic
counters: peer connect/disconnect, reconnect scheduled/succeeded/failed,
heartbeat timeout, frame sent, high-water mark reached, frame dropped, non-OK
control event count, and the last status/value pair.

Observe also keeps a caller-configured bounded event history. When full, the oldest record is
discarded and `turbo_flow_observe_dropped_event_count()` advances. Command results are recorded
explicitly because Observe never dispatches owner commands.

`turbo_flow_observe_export_prometheus()` writes Prometheus text through a host callback.
`turbo_flow_observe_export_opentelemetry()` emits exporter-neutral metric samples for the host's
selected SDK. Both use resource snapshots only and never query or copy business payload or resource
document payload.

## Summary Sink

`turbo_flow_observe_register_log_sink()` registers a terminal adapter. Only
messages explicitly routed to that stage produce summaries. The host callback
owns the actual logger, level, batching, and persistence policy; Observe emits
no per-message INFO logs itself.

Payload is redacted by default. When `include_payload_preview` is explicitly
enabled, at most 256 configured bytes are represented as lowercase hexadecimal,
so binary input cannot inject log syntax. The summary always includes stage,
message id, status, and full payload length. The callback is synchronous and
must remain bounded.
