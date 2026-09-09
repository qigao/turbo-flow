# TurboFlow Architecture

This document describes the current repository behavior. Planned work belongs
in the repository `plan.md` or in an explicitly marked proposal document.
The target owner/resource/Disruptor architecture and its migration boundaries are
defined in `PRIMITIVE_GRAPH_ARCHITECTURE.md` and tracked in the repository `plan.md`.

The first two additive domain-contract slices are implemented: hosts can register
versioned primitive and operation descriptors, DSL nodes can bind them with
`operation`/`resource`, and compile validates roles, resource type/domain, five-
dimensional scope constraints, execution policy, typed operation edges, and
operation runtime requirements. Bounded `block`, `fail`, and `drop-newest`
handoff lowers to the existing worker Disruptor segment; `drop-oldest` remains
unsupported because an active sequence cannot be reclaimed safely. Ordering,
retry, and reject requirements bind to the existing reorder, adapter retry, and
reject-edge mechanisms. Operation execution deadlines are enforced at their
selected runtime boundary. Generic and protocol settlement actions execute only
through an explicitly registered settlement owner; a missing owner or unsupported
action fails compile with `SALTS_ENOTSUP`.
Every runtime node has a complete resolved operation contract. Callback stages
require an explicit DSL operation identity and separately registered descriptor
and provider. Node names never select a callback. Built-in `core.source`,
`core.port.input/output` and `core.stage.owner` contracts remain available for
sources, routing ports and adapter-owned consume; direct inline paths gain no extra ring.
The public fields, registration example, lifetime, and compile errors are
documented in `DOMAIN_CONTRACTS.md`.

All resource owners expose generation-aware metadata and snapshots. A stable
owner-scoped UID identifies connection, queue, pool, runtime, segment,
protocol, storage, and rule-set resources. Resize commands carry the observed
generation so stale reconcile decisions fail with `SALTS_EBUSY`; a UID is only
as durable as its owner contract and is not implicitly a cross-deployment ID.

Domain-specific management data can be projected through a schema-backed
resource document. Core owns only the fixed metadata envelope and immutable
payload bytes; a trusted descriptor identifies the DataBind schema and type.
Owner-native Status documents and independent common-governance schemas for
each resource kind are dynamically bound in tests with the real DataBind
library. The common Spec/Conditions/Event projection is derived from one
metadata/snapshot pair; its Event is a latest observation with an explicit
generation gap, not a replay log. DataBind remains consumer-side, so core does
not expose `DataBindValue` or acquire a JIT runtime dependency. HTTP, S3, and
database providers consequently publish different Status schemas without
adding their fields to `turbo_flow.h`.

## Authority

When documents disagree, use this order:

1. Public headers and compiled behavior.
2. Tests.
3. `parser/GRAMMARS.md` and `parser/OPTION_SCHEMAS.md`.
   Expression design is defined separately in `parser/EXPRESSIONS.md` until it
   is integrated into the production grammar.
4. This architecture summary.
5. TODO and feature backlog documents.

## Scope

TurboFlow is an embeddable C data/message graph runtime. The core owns:

- `.flow` parsing with re2c and Lemon.
- Graph validation and immutable topology plans; pool parallelism is changed
  only by the serialized runtime resize command.
- Broadcast and worker-pool data strategies.
- Inline, thread-pool, and coroutine-pool compute executors; network placement
  remains an external CNet/CHTTP ownership boundary, while host extensions use
  typed operations or adapters rather than a custom executor.
- Message ownership, stage dispatch, lifecycle, and structured errors.
- Backend-neutral expression parsing, schema-backed type checking, and private
  MIR interpreter/JIT evaluation through an opaque compiled-expression API;
  conditional routes evaluate the current successful upstream output, while
  RulesForge-backed operations own multi-rule schema data decisions.

Protocol codecs, storage, and codecs live in adapter modules. Network endpoints,
email transports, broker discovery, distributed routing, databases, UI, group
ownership, secrets management, and job-runner behavior are outside the core.

## Runtime Observation

Runtime observation is attached to a compiled Graph by the host; it is not a
source, sink, route, or state owner. `turbo_flow_register_observer()` registers a
named structured Observer before the flow starts. Registration is bounded to
`TURBO_FLOW_MAX_EVENT_OBSERVERS`, rejects duplicate names, survives
`turbo_flow_reset()`, and transfers `ctx` destruction to the flow only after
successful registration. Unregistration is allowed only while stopped.

An Observer selects events with `event_mask` and receives immutable borrowed
`turbo_flow_observe_event_t` views:

| Event | Semantic point |
| --- | --- |
| `SOURCE_RECEIVED` | One message has been admitted from the named source into Graph execution. |
| `STAGE_BEGIN` | An executable stage attempt is about to run. |
| `STAGE_END` | That stage attempt has ended; `status` and `duration_ns` describe the attempt. |
| `ROUTE_EVALUATED` | One ordinary, conditional, or reject edge was considered; `selected` is zero or one and `edge_kind` identifies the edge class. |
| `SINK_COMPLETE` | A selected terminal stage has completed. |
| `ADAPTER_START` | Adapter startup completed with the reported status. |
| `ADAPTER_STOP` | Adapter shutdown completed with the reported status. |
| `FLOW_COMPLETE` | The publication's selected Graph work has settled with the reported status and duration. |

Names, message pointers, and all other pointed-to values are valid only during
`on_event()`. The callback is synchronous and may execute concurrently for
concurrent publications; it must be thread-safe and must not mutate or re-enter
the flow. A non-OK callback result increments
`turbo_flow_observer_failure_count()` but never changes edge selection, message
status, adapter lifecycle, or Graph completion. This failure isolation prevents
telemetry from becoming a hidden data-plane state transition.

The structured Observer API is independent of
`turbo_flow_set_observer()`, which remains the compatibility callback surface.
It is also independent of log sinks: a host may translate events into metrics,
traces, or logs, but TurboFlow does not require a logging backend. The
`TurboFlow::Observe` summary adapter is a separate explicit terminal data-plane
sink. Consequently, `.flow` source/sink declarations need no observer grammar;
only a deliberately selected Observe summary adapter appears in Graph config.

## DSL

Top-level atomic `source` and `stage` declarations and reusable non-`main`
`stage` blocks precede the single root `stage main` block. A reusable stage has
explicit `in`/`out` ports and is instantiated with `use`; the removed `flow`,
`graph`, and `subgraph` block spellings are syntax errors. The root stage accepts
ordinary paths, explicit conditional routes, and named reject edges:

```flow
input -> validate
route validate -> accepted when msg.status == 0
route validate -> rejected when msg.status != 0
reject validation_failed validate -> rejected
```

Route predicates are compiled before start and evaluate the successful upstream
stage output. A false predicate filters only that edge; an evaluation error is
fail-fast. Conditional edge selection is per message and therefore bypasses the
otherwise static broadcast data plane.

A named reject edge activates only for a nonzero upstream execution result.
Each executable stage may define at most one, so matching is deterministic;
without one, publication returns the original error. A handled failure writes
message-owned stage, adapter, route, code, and attempt metadata before invoking
the reject handler. The first attempt is `1`. Clone/retain/move and cleanup
preserve or release these names with the message, allowing later queue and
dead-letter boundaries to retain the failure context safely.

Adapter stages may declare an explicit bounded policy:

```flow
stage fetch adapter service.fetch retry attempts 3 delay 100
reject fetch_dead fetch -> dead_letter
```

The compiled plan stores only total attempts and delay. Core never repeats an
ordinary `consume()` call. An adapter must opt into `consume_retry`, classify
retryable protocol errors, perform each operation, and interrupt delayed waits
when stopping. `turbo_flow_retry_execute()` provides the shared ownership
mechanism: every attempt receives a clone of the unchanged original message;
failed mutations are released and only a successful result moves back. On
exhaustion the original payload reaches the named reject/dead-letter sink with
the final attempt count in message-owned failure metadata.

The accepted executor spellings are exactly:

```text
inline | thread | coro
```

Former I/O placement and executor aliases are rejected. See `parser/GRAMMARS.md`
for the complete grammar.

Example:

```flow
source input
stage decode adapter "codec.length"
stage process worker 4 capacity 64 exec thread workers 4
stage output adapter "output.sink"

stage main {
  input -> decode -> process -> output
}
```

Data strategy and execution strategy are independent:

- `worker N capacity M` selects worker-pool delivery and its bounded disruptor
  ring. It is a data handoff/consumer lane, not a fourth compute executor.
  Capacity defaults to 1024 and must be a power of two.
- `exec thread workers N` selects the thread executor.
- `exec coro lanes N pool N` selects the Salts coroutine scheduler.
- Adapter-owned CNet/CHTTP placement is not an executor. An adapter-backed stage
  remains `inline`; the external adapter owner serializes its network context.
- TurboFlow Policy expression evaluation is an inline pure operation. A graph node
  may explicitly choose the common thread or coroutine pool for parallel evaluation.
- Executor-specific counts are fail-fast: other executor/count combinations are
  rejected instead of being ignored.

## Lifecycle

The public lifecycle states are:

```text
NEW -> PARSED -> COMPILED -> STARTED -> STOPPED
                  |                    |
                  +------ FAILED <-----+
```

The normal API sequence is:

```c
turbo_flow_t *flow = turbo_flow_create();
turbo_flow_parse_string(flow, text, text_len);
/* Register callbacks and adapters before compile. */
turbo_flow_compile(flow);
turbo_flow_start(flow);
turbo_flow_publish(flow, "source_name", &msg);
turbo_flow_stop(flow);
turbo_flow_destroy(flow);
```

`turbo_flow_reset(flow, keep_registry)` clears parsed and compiled graph state.
It is rejected while started. A stopped compiled flow may be started again.

## Message Ownership

`turbo_flow_msg_t` is the runtime envelope:

- `buffer` owns shared bytes through `mem_buffer_t` retain/release.
- `payload` is a borrowed view backed by `buffer` or `owned_payload`.
- `owned_payload` owns transformed text.
- `transport_context` has two explicit ownership forms. An address inside the
  message `buffer` is message-owned metadata and follows that retained buffer
  across graph/Disruptor boundaries. Any other address is borrowed adapter
  request state: core propagates it inline, never destroys it, and rejects it at
  worker, thread, coroutine, and broadcast boundaries.
- `_content_handle` is core-private; callers attach typed objects only through
  the schema projection APIs and never install arbitrary destroy callbacks.
- `content_descriptor` is a borrowed immutable content identity. Its adapter or
  host owner must outlive the message and every clone; message cleanup never
  destroys it.
- `status` carries protocol or stage status.

Cross-stage transfer uses the public retain, clone, move, and cleanup helpers.
An opaque message needs no schema: its payload bytes are directly processable.
A schema-bound message keeps the same bytes as its source of truth and attaches
an optional provider-owned `turbo_flow_data_schema_t` identity plus a derived
projection. Clearing that projection returns the message to opaque state without
changing the payload. Projection clone/destroy behavior belongs to its provider;
clone, retry, and fan-out fail with `SALTS_ENOTSUP` when no clone hook exists.
The Codec DataBind provider registers `data_bind_value_clone`, so its cloned
messages own independent value trees; this does not weaken the generic fail-fast
contract for other providers.
There is no arbitrary owned parsed-data path. Mutable fan-out is rejected unless the graph provides
a private ownership boundary and the projection provider supplies a clone hook.

Descriptor and projection domains may differ because they describe separate
boundaries, such as an HTTP ingress and its DataBind projection. They still refer
to one payload identity: once a descriptor declares a schema, projection encoding,
schema name, type, and version must match in either binding order. A mismatch fails
with `SALTS_EPROTO` and does not consume a caller-owned projection.

The host explicitly creates and destroys the content schema registry. Registration
deep-copies schema identity and projection-type strings, but rejects runtime schema
text; schema compilation/loading remains a provider responsibility. Resolution is
exact over domain, profile, normalized media type, and declared schema name/type/version.
Payload encoding is derived from normalized media type, while payload identity is
diagnostic and never changes the registry key. Domain adapters normalize protocol
metadata through this contract; unknown media remains opaque unless a binding was
explicitly declared, in which case a mismatch fails fast.

S3 PUT uses the configured Content-Type and validates an existing message descriptor
before issuing the external request. S3 GET first reads StatObject metadata and uses
the object's actual Content-Type for its task-local descriptor; PUT configuration is
not treated as ingress metadata. PostgreSQL parameter sinks use payload compatibility
validation, while the query adapter derives rowset or command descriptors from the
actual `PGresult` and optionally binds rows through an explicit mapper.

## Runtime Data Plane

The broadcast graph is lowered through `disruptor_topology_t`. Worker-pool
segments and completion handoffs are owned by TurboFlow because they cross
topology boundaries. Compile rejects unsafe ordered fan-in involving unordered
worker or thread-executor branches.

Each worker-pool segment owns fixed consumer threads. The publisher moves the
message envelope into one execution task, publishes its pointer through the
bounded ring, and synchronously waits for the selected consumer to return the
message and stage status. The disruptor owns admission and worker selection;
graph dependency release remains on the publishing path. An implicit DSL worker
uses blocking admission. An explicit operation contract may instead select
fail-fast admission (`SALTS_ENOSPC`) or drop-newest admission
(`SALTS_ECANCELED`); `drop-oldest` is rejected during compile. Every policy keeps
memory bounded and is validated against the worker capacity.
Worker claims briefly spin for handoff latency, then park inside Disruptor.
Publish wakes parked workers only when waiters exist. Stop changes the runtime
drain predicate and explicitly wakes all workers, so idle graphs do not poll and
shutdown does not depend on a timeout.
Pool snapshots are runtime-generation records. Start rebuilds exactly one
record for each thread, coroutine, or Disruptor pool; stop retains that
generation as `STOPPED`, and restart replaces it rather than appending stale
records. Disruptor submissions update the same submitted/queued/active/result
counters used by the other executor kinds.
`turbo_flow_pool_resource_status_at()` wraps one such record in a caller-owned
typed resource view. Its UID is `pool:<pool-kind-number>:<stage-name>` and its
conditions are derived from the same generation snapshot rather than maintained
as another state source.

Thread, coroutine, and disruptor execution share one internal task state
machine:

```text
NEW -> ACCEPTED -> RUNNING -> COMPLETED
                         \-> CANCELED
```

Runtime code owns task submission and waiting so stage code cannot bypass graph
completion. Stage callbacks may call `turbo_flow_execution_yield()`,
`turbo_flow_execution_abort()`, and
`turbo_flow_execution_cancel_requested()`. Abort is cooperative: accepted work
can be canceled before it runs; running work observes cancellation at yield or
query points. No thread or coroutine is forcibly terminated.

An operation deadline starts when its callback enters `RUNNING`; queue and ring
admission time remains a separate backpressure concern. Thread, coroutine, and
Disruptor tasks observe expiry cooperatively and return `SALTS_ETIMEDOUT` rather
than `SALTS_ECANCELED`. Inline and adapter callbacks are checked after
they return because arbitrary C callbacks cannot be safely preempted. A source
deadline still fails compile until its adapter defines a per-message owner contract.

A coroutine executor owns one scheduler and one mutex per lane. With
`exec coro lanes N pool M`, each lane also owns an independent
`turbo_coro_pool_t` of `M` reusable shells. Round-robin lane selection is atomic,
so up to `N` producer calls can run coroutine callbacks concurrently without
sharing scheduler or pool bookkeeping.

Executors report completion exactly once. Adapter consume operations are
synchronous with respect to the message envelope: they return only after they
no longer depend on the current dispatch-owned message.

`turbo_flow_publish()` remains synchronous and accepts concurrent producer
calls. It constructs a one-value managed CFlow Publisher, opens a Reactive run
on an inline Scheduler, requests one value, and waits for the terminal result;
there is no direct native fallback. The public `turbo_flow_run_*` boundary accepts
typed Publishers, preserves exact demand across `WAIT`, and distinguishes full
Scheduler admission from closed ingress and post-admission graph failure. A null
Scheduler selects the Flow-owned bounded worker Scheduler. Each run owns its
Subscription and first terminal result; the Flow owns the bounded registry of
non-terminal runs. `turbo_flow_publish_batch()` executes an ordered producer callback under
one publication admission and source lookup, cleans each transient message after
its attempt, stops at the first failure, and reports the successful prefix without
changing per-message graph execution or ownership.
Graph dependency arrays and errors are producer-local, sequence and
reorder reservations are allocated atomically, worker stages use the MPMC
disruptor ring, and coroutine stages synchronize per lane. The broadcast
topology fast path is limited to single-source static all-inline graphs and uses
a narrow cursor lock. Non-blocking producers use `turbo_flow_publish_async()`:
the Flow clones or retains the message, admits it to a lazily-created bounded
ingress thread pool, and reports graph completion on an ingress worker. This
handoff does not replace the stage executor selected by the DSL; it only keeps
source-owned timer or I/O threads out of graph execution. Queue, retained
per-message byte, and aggregate in-flight byte exhaustion fail immediately with
`SALTS_ENOSPC`. A retained buffer is charged by capacity, so a small view cannot
pin a large allocation outside the budget. Human YAML configures this one
handoff through process-level `runtime.ingress.workers`, `capacity`,
`max_message_bytes`, and `max_inflight_bytes`; the resolver expands omitted
values to the public defaults before a host configures its Flow. Every accepted
task owns one byte reservation and releases it after message cleanup, before
its completion callback.

Socket and protocol ingress expose this source boundary as two explicit
profiles. `inline` remains the compatibility default and executes Graph on the
producer/owner thread. `async_bounded` retains the message into the same Flow
async ingress; it never creates an adapter-local queue and never falls back to
inline execution. Socket queue/byte rejection fails the current receive.
Protocol Graph admission returns `PENDING`, invokes one worker completion, and
requires the host to marshal settlement back to the serialized protocol owner.

Stop first closes publish admission, asks adapters to interrupt pending work,
closes the asynchronous ingress, cancels independent Reactive runs, drains
already accepted synchronous facades, waits for the CFlow Scheduler to become
idle, and only then destroys data planes and executors. A new call after
admission closes returns `SALTS_ESHUTDOWN`.

Reorder boundaries issue their own contiguous tickets only for publications
that can reach that boundary. A publication that is filtered, fails upstream,
or is canceled releases its reservation explicitly, so independent sources and
conditional routes cannot create false missing-sequence timeouts.

## Adapter Boundary

Concrete adapter configuration is provided through C config structures. Adapter
schemas expose immutable option metadata for validation and tooling; serialized
configuration cannot provide host-object fields.
The host accepts human-authored configuration only as YAML and emits immutable
resolved JSON for tooling and runtime projection. External profiles may resolve
stage parameters to registered adapter names, but
cannot overlay concrete adapter fields. Adapter creation copies its resolved
configuration, and later endpoint changes use explicit owner commands rather
than mutating profile state.

## Provider, Message, and Graph Boundary

Every protocol or storage provider owns its external representation and converts it at the
adapter boundary. A decoder `frame`, HTTP request/response view, socket framing view, or Redis
Stream entry is temporary provider state; it is not a graph message and cannot be retained across
an asynchronous handoff. The adapter validates and materializes `turbo_flow_msg_t` with an owned
buffer (or an explicit retained slice), then the graph may run zero or more stages and finally
returns an owned message or settlement result to a provider sink.

```text
provider bytes/frame/view
  -> provider validation and metadata normalization
  -> turbo_flow_msg_t
  -> optional graph stages (Policy, process, store)
  -> provider sink or owner settlement
```

The graph does not become the owner of a protocol session, socket, protocol ACK, Redis consumer group,
or database transaction. Those owners expose typed operations and explicit commit results. A graph
stage may filter, transform, route, delay, or persist the message only within its registered
operation contract. Typed durable facts remain in product-owned TurboDB ORM repositories outside
the graph; asynchronous delivery remains a provider/executor responsibility rather than a hidden
second source of truth.

## Product Assembly

Every TurboFlow product uses the same application model:

```text
YAML resources/adapters -> provider conversion -> Graph source -> processors/subgraphs -> Graph sink
                                  |
                         bounded TurboFlow data plane
```

Trusted host code supplies a caller-owned `turbo_flow_product_provider_registry_t`.
Adapter providers own native source, sink, and protocol-bridge construction;
resource providers construct processors such as TurboFlow Policy and bind their typed
operations. `turbo_flow_product_preflight()` validates every resolved adapter
kind before a provider callback or native side effect. After parsing,
`turbo_flow_product_assemble_graph()` registers only resources and adapters
explicitly referenced by the Graph, with resources before adapters and one
callback per distinct name. Unused configured channels remain valid resources;
unused adapters still require an enabled provider but are not started.

The registry is dependency injection, not a global service locator or plugin
loader. It is borrowed for one synchronous build and never becomes a second
resource owner. A failed callback may have created native state, so the host
discards that Flow generation and destroys provider-owned resources in reverse
ownership order. Protocol FSM, connection/session state, ACK/QoS, database
transactions, and retry ownership remain inside the selected native provider;
the Graph sees complete owned messages and typed operations.

Implemented modules:

| Module | Current behavior |
| --- | --- |
| `TurboFlow::Codec` | Line/length framing and DataBind for TBE, JSON, CSV, XML |
| `TurboFlow::Observe` | Opt-in message/stage/adapter metrics and bounded summary sink |
| `TurboFlow::Schedule` | Interval, one-shot, bounded-repeat, and local-time cron sources |
| `TurboFlow::CNetAdapter` | Optional demand-driven CNet sources and async terminal sinks |
| `TurboFlow::CHTTPAdapter` | Optional caller-polled CHTTP async client flat-map stage |

Legacy socket and HTTP transport targets and their compatibility aggregates have
been removed. The optional CNet and CHTTP adapter components expose only explicit
owner boundaries over Salts; they do not restore a legacy implementation or
fallback. RPC, S3, email, product sessions, credentials, and endpoint policy
remain host/product integrations.

Core also provides an opt-in module catalog above primitive/operation
registrations. A module declares its version, capabilities, primitive type and
operation exports, plus already-registered dependency ranges. Typed operation
providers and native adapters can be bound to the module that owns the exported operation. The
catalog is validation and read-only discovery metadata: it is not a loader,
resource factory, or Graph DSL construct. Production registrations include
TurboFlow Policy operations. Durable repositories are composed by the product
through TurboDB ORM and do not become Graph adapter operations.

## Build Components

TurboFlow is configured and installed as a graph data-processing product.
Protocol codecs, security, codecs, observation, scheduling, and the MIR JIT backend form its
repository-owned build graph. Network adapter components remain separate from Core and Product;
external protocol products and TurboDB persistence repositories are not producer-side components.

Salts, SaltsUtils, RulesForge, and the other declared dependencies are required
by every product build.
`find_package(TurboFlow COMPONENTS ...)` remains a consumer-side target
availability check; it does not select or remove producer-side features.
TurboFlow 2.0 removes the aggregate `TurboFlow::Flow` target. `Graph` owns the
repository `vendor/mir` static target; no MIR type enters installed public headers.
Consumers link the component that owns each API rather than a compatibility aggregate.

Core exposes a single optional read-only observer callback slot. Timing and
callbacks are skipped when it is empty. `TurboFlow::Observe` implements that
ABI with atomic aggregate counters and bounded per-stage series; its explicit
summary sink defaults to payload redaction. Ownership and logging behavior are
documented in `observe/README.md`.

## Polling Decision

TurboFlow Core contains no protocol-client polling implementation. External
CNet/CHTTP adapter owners, including `TurboFlow::CNetAdapter` and
`TurboFlow::CHTTPAdapter`, own protocol-specific polling and connection state. `TurboFlow::Schedule`
is limited to generic ticks, one-shot delays, bounded repeats, and cron triggers.
It reuses the `Salts::Cron` five-field parser and local wall-clock calculation.
Catch-up is explicitly bounded, and stop interrupts waits and joins the adapter
worker before return.

## Error Semantics

Core and adapter boundaries fail fast. There is no implicit retry or silent
fallback. Stage failures stop normal downstream release unless the graph binds
an explicit retry policy or named reject/dead-letter edge. Retries are bounded
and require the adapter to classify the failure as retryable. Generic settlement
actions (`complete`, `requeue`, `dead-letter`, `canceled`, and protocol ACK) are
validated against the operation contract and dispatched only through the
registered state owner; a ring/HWM success is never promoted into a durable or
delivery ACK.

## Verification

Focused tests live beside each module. The core suite covers grammar rejection,
graph validation, ownership transfer, lifecycle, executors, fan-out/fan-in, and
completion behavior. The benchmark target covers compile, linear, diamond, and
worker-pool publish paths. It also emits machine-readable throughput and
P50/P95/P99 latency for asynchronous ingress, thread and coroutine executors,
cross-ring worker handoff, event-time processing, and teardown scenarios; see
`benchmarks/README.md` for the reproducible command and limits.

## Pool Resize Command

Pool snapshots and resource status are observed state only. A host control loop
issues `turbo_flow_resize_pool()` with a stage name, pool kind, desired
parallelism, drain deadline, and the status `observed_generation`. A zero
generation or truncated command fails with `SALTS_EINVAL`; a stale generation
fails with `SALTS_EBUSY`. The
runtime owner closes admission, drains accepted
publishes, updates the compiled stage/executor configuration, and rebuilds all
runtime-owned Disruptor, thread, and coroutine pool resources as one generation.

The command preserves the prior open/paused admission state after success. A
drain timeout leaves admission paused and does not change configuration. If the
new resources cannot be created, the runtime restores the previous
configuration and rebuilds it; failure of both rebuild and rollback moves the
flow to `FAILED`. Adapter connections are not drained or recreated by pool
resize. Hosts must serialize resize with lifecycle, snapshot-vector traversal,
and other configuration commands.

Successful start/restart and resize advance the runtime generation. A failed
resize whose rollback rebuild succeeds also advances it because the underlying
pool instances were replaced. Stop and no-op resize do not advance it. Observe's
pool-specific reconciler reads load, capacity, and generation from one status
snapshot and sends one checked command; it does not create a controller thread.

Pool resize does not implicitly quiesce source adapters. Hosts can use the
separate adapter owner command before resizing: quiesce ingress, drain core
publication, resize pools, then resume ingress. A source adapter publish racing
closed admission still receives `SALTS_ESHUTDOWN`; adapter-owned retention or
retry policy determines whether that external message is retried.

## Adapter Control Commands

Adapter commands use `turbo_flow_resource_command()` and a stable command-capable
connection resource provider. The former `turbo_flow_adapter_command()` API,
its payload types and the adapter ops command tail field have been removed.
Consumers must register resource metadata/command callbacks and recompile against
the new adapter ops layout. Package 2.0.0 is unchanged; plugin ABI is 2.0.
ABI 1.x plugins are rejected before load or registration: the unchanged root
vtable also gates adapter layouts passed later across DLL boundaries. Host config
and Product owner descriptors must be complete; old short layouts and padding
compatibility are removed. Existing `_v1` type names do not enable ABI 1.x support.
Snapshot data remains observed state.

Hosts serialize adapter commands with lifecycle/configuration calls. For sink
endpoint replacement, first pause and drain core publication so no consume call
races resource replacement. For source-side graph maintenance, quiesce source
adapters before core drain.

`turbo_flow_discovery_controller_t` reconciles one complete, versioned registry
snapshot onto a fixed set of adapter slots. The registry source fills
caller-owned, pointer-free peer values; the controller validates and copies the
whole snapshot before issuing commands. Stable peer IDs retain their slots,
new IDs take unused slots, and removed IDs quiesce their owners. A newer version
is committed only after every owner command succeeds. On failure, applied
commands are undone in reverse phase order and the old version remains the fact
source; a rollback failure poisons that controller so it cannot silently advance
from uncertain state. Replaying identical data at the current version is a
no-op, conflicting current-version data is rejected, and older versions are
rejected. Registry fetch errors issue no adapter command.

Creation, replacement and polling require STARTED; stopped polling never calls
the registry fetch callback. Creation preflights every slot's unique connection
provider and copies its stable UID before quiescing owners. Missing providers fail
with ENOENT and ambiguity with EPROTO. Every later command queries that UID's current
generation and owner/kind; a replacement UID is not silently rebound.

One flow owns a shared 256-record command history. Creation reserves `2*N` entries;
replacement reserves `3*A + 2*U + 2*R` for actual additions, updates and removals,
including compensation. Empty history therefore permits at most 128 creation slots.
Insufficient history returns ENOSPC and allocation failure returns ENOMEM before
owner effects. Version replays and unchanged peer sets consume no records. Capacity
is never increased, history is never evicted and requests are never implicitly split.
The internal scope reserves a raw CSTL vector; each dispatcher command claims its
record with raw resize before callbacks, then fills that record without allocating.
`vec_push` is unsuitable here because its temporary copy allocates even after reserve.

On create failure every applied slot is resumed as far as possible. A failed
compensation returns its first error, leaves the output controller null and records
both failure codes in the flow diagnostic. Replacement retains the old committed
peer/version facts on failure; owner actual state remains observable. Rollback of
a new slot makes it inactive but does not claim to restore an unmodeled endpoint.
Failed replacement compensation prevents further operations on that controller.

The controller borrows the flow and must be destroyed before flow reset/destroy.
Destroy only frees reconciliation metadata. Hosts serialize all discovery, control,
resource commands and lifecycle operations on a flow. Callbacks must not reenter
them, mutate registries or invalidate storage; the internal command guard rejects
reentrant commands with EBUSY and does not provide cross-thread synchronization.

## Conditional Control Rules

Data-plane and control-plane conditions have separate ownership:

- `route A -> B when <expr>` evaluates one immutable message and only selects
  an edge.
- `if|when <facts expr> then <command>` evaluates typed snapshots and issues
  at most one existing runtime, pool, or adapter owner command.

Core facts use `runtime.*`, `pool.<stage>.<kind>.*`, and
`adapter.<binding>.*`. Observe supplies `traffic.*`, `system.*`, and
`graph.*`; hosts can inject other resource or business-data facts through a
typed schema and read-only provider. Unknown paths, provider type mismatches,
counter overflow, and non-BOOL conditions fail before action dispatch.

Conditions do not mutate snapshots and cannot invoke arbitrary callbacks.
Commands retain their existing lifecycle, rollback, and error semantics. The
host owns scheduling, hysteresis, cooldown, rule storage, and serialization
with lifecycle/configuration operations.
