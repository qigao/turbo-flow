# CoroNet execution binding design

## Decision

Socket and FMQ adapters will use one shared execution-binding abstraction. A
binding selects exactly one CoroNet event-loop context and states who drives and
destroys it. Pool lane affinity means selecting one stable context from a
`coro_thread_pool_t`; it does not mean OS CPU affinity.

This design affects the public socket and FMQ registration APIs, the shared
CoroNet runtime adapter, adapter lifecycle, and synchronous send behavior. It
does not change `.flow` syntax or wire protocols. Execution bindings are host
objects and therefore remain outside serialized adapter configuration.

## Alternatives

1. Keep only raw `coro_context_t *`: smallest API, but the host cannot express
   which pool lane owns that context and validation cannot distinguish borrowed
   from transferred ownership.
2. Add integer `lane` independently to every adapter: easy to expose, but lane
   has no meaning without a specific pool and creates duplicated resolution and
   lifecycle rules.
3. Create a TurboFlow scheduler above CoroNet: centralizes placement but
   duplicates CoroNet's event loops, post queue, and thread pool.
4. Use the selected execution binding: one typed ownership boundary maps a pool
   lane to its existing CoroNet context and keeps scheduling in CoroNet.

Option 4 is selected because it expresses both resource identity and ownership
without introducing another scheduler or per-adapter interpretation.

## Evidence and current constraints

- CoroNet exposes one event loop per `coro_context_t`.
- `coro_post()` is the thread-safe cross-thread entry and wakes the owning loop.
- `coro_context_spawn()` directly modifies the context scheduler and must run on
  the context owner thread.
- `coro_thread_pool_get_context(pool, index)` already exposes a stable context
  for explicit affinity.
- Socket currently permits a borrowed context but directly spawns and pumps it
  from the caller thread. That can violate single-owner event-loop execution.
- FMQ currently rejects borrowed contexts and starts a private loop thread for
  owned contexts.

`HIGH`: adapters must never call `coro_context_run()`, `coro_context_stop()`, or
`coro_context_destroy()` for a borrowed context or pool lane. Cross-thread work
must enter through `coro_post()`.

## Public API

The shared public header will define:

```c
typedef enum turbo_flow_coronet_execution_kind_e {
  TURBO_FLOW_CORONET_EXECUTION_PRIVATE = 1,
  TURBO_FLOW_CORONET_EXECUTION_BORROWED_CONTEXT,
  TURBO_FLOW_CORONET_EXECUTION_OWNED_CONTEXT,
  TURBO_FLOW_CORONET_EXECUTION_POOL_LANE
} turbo_flow_coronet_execution_kind_t;

typedef struct turbo_flow_coronet_execution_binding_s {
  size_t size;
  turbo_flow_coronet_execution_kind_t kind;
  coro_context_t *context;
  coro_thread_pool_t *pool;
  uint32_t lane;
  uint32_t flags; /* must be zero */
} turbo_flow_coronet_execution_binding_t;

int turbo_flow_coronet_execution_binding_validate(
    const turbo_flow_coronet_execution_binding_t *binding);
```

Field rules are strict:

| Kind | `context` | `pool` | `lane` | Driver and owner |
| --- | --- | --- | --- | --- |
| `PRIVATE` | NULL | NULL | 0 | adapter creates, drives, stops, destroys |
| `BORROWED_CONTEXT` | non-NULL | NULL | 0 | host drives and owns |
| `OWNED_CONTEXT` | non-NULL | NULL | 0 | adapter drives and destroys transferred context |
| `POOL_LANE` | NULL | non-NULL | explicit index | pool drives and owns |

There is deliberately no automatic lane value. Stable connection ownership is
more important than round-robin placement; hosts may choose a lane using their
own deterministic policy before registration. Invalid field combinations return
`TURBO_EINVAL`; an out-of-range pool lane returns `TURBO_ERANGE`.

Existing config structs have no leading `size`, so extending them would not be
binary safe. New registration entry points accept the binding separately:

```c
int turbo_flow_coronet_register_socket_adapter_ex(
    turbo_flow_t *flow,
    const char *name,
    const turbo_flow_coronet_socket_config_t *config,
    const turbo_flow_coronet_execution_binding_t *execution);

int turbo_flow_fmq_register_adapter_ex(
    turbo_flow_t *flow,
    const char *name,
    const turbo_flow_fmq_config_t *config,
    const turbo_flow_coronet_execution_binding_t *execution);
```

The existing registration functions remain wrappers around the existing
`context` and `take_context_ownership` fields. The `_ex` functions reject a
config that also sets either legacy field, preventing two competing execution
facts. New code should use `_ex`; the legacy fields can be deprecated in a later
major release.

## Internal solution

`tf_executor_common` will own a resolved, adapter-local snapshot:

```c
typedef struct tf_coronet_execution_s {
  coro_context_t *context;
  turbo_flow_coronet_execution_kind_t kind;
  int drives_context;
  int owns_context;
  int loop_thread_started;
  turbo_thread_t loop_thread;
} tf_coronet_execution_t;
```

`tf_coronet_execution_init_with_pool()` is the private-context-only constructor path. It forwards
one caller-owned `coro_object_pool_config_t` snapshot to `coro_context_create_ex()` and rejects the
same argument with `TURBO_ENOTSUP` for borrowed, transferred-owned, or pool-lane bindings. The
legacy `tf_coronet_execution_init()` remains a wrapper with a NULL pool configuration, so existing
adapters retain CoroNet defaults and host-owned contexts remain entirely host-sized.

The shared implementation provides resolve, start, post, synchronous control
call, stop, and destroy operations. It is a thin ownership adapter over CoroNet,
not a second scheduler.

For owner commands, `tf_coronet_actor_t` adds a bounded mailbox on top of that
execution placement. Submit copies a small pointer-free command, assigns one
monotonic command ID, and posts it to the selected context. The caller receives
an opaque reply handle and may poll or wait; no callback crosses back into the
submitting thread. The mailbox owns command bytes until the handler returns and
the reply handle uses reference counting, so caller timeout or early reply
release cannot expose stack memory to a later lane callback.

### Dispatch

1. If `coro_context_current()` is the bound context, execute or spawn directly.
2. Otherwise post a small request with `coro_post()`.
3. The posted callback runs on the bound lane and calls
   `coro_context_spawn()` there.
4. Synchronous TurboFlow adapter calls wait on their existing request condition
   and operation deadline; they never pump a borrowed event loop.

FMQ endpoint lifecycle uses the mailbox for bind/connect/close/peer-clear
reactor commands. The host still owns multi-step quiesce/resume/replace
orchestration, while socket and peer mutations execute only on the CoroNet
owner lane. Command capacity is one because public lifecycle and resource
commands are host-serialized; data sends continue through their separate
bounded data-plane queue.

Adapter-specific send requests retain payload ownership until completion. The
shared execution layer owns only dispatch and lifecycle state. No lock is held
during socket I/O or user callbacks.

### Start

- `PRIVATE` and `OWNED_CONTEXT`: start one adapter loop thread, then dispatch
  socket creation, listen/connect, and coroutine creation onto that context.
- `BORROWED_CONTEXT` and `POOL_LANE`: the host/pool must already be driving the
  context. Start posts initialization and waits for its bounded completion.
- If a borrowed loop is not running, start returns `TURBO_ETIMEDOUT`; it does
  not silently create or drive another context.

FMQ connect waiting is changed to wait for its connection condition regardless
of who drives the context. Socket sink sending is changed from caller-side
`coro_context_run()` pumping to post plus condition wait.

Adapter-driven contexts set CoroNet persistent mode before starting the loop
thread so the loop cannot exit before the initialization post arrives. Stop
clears persistence only after accepted work and transport cleanup have drained.
An `OWNED_CONTEXT` binding is exclusive: the supplied context must not already
be running or shared with another owner.

### Stop and shutdown

1. Close admission and mark the adapter quiesced.
2. Post socket-wait interruption and resource close to the bound context.
3. Wait up to the configured drain/linger deadline for accepted work.
4. For adapter-driven contexts, stop and join the loop after resources drain.
5. Destroy only contexts owned by the adapter.

Borrowed contexts and pool lanes are never stopped or destroyed. Their owner
must keep them alive and running until every bound adapter has completed
shutdown. Destroying a pool/context earlier is a host lifecycle error.

Runtime lane rebinding is intentionally excluded from the first implementation.
Sockets, TLS state, timers, heartbeat waits, and reconnect state are context
affine; migration would require quiesce, drain, resource recreation, reconnect,
and rollback. A host that needs a different lane must stop and replace the
adapter explicitly.

## State and error semantics

The concrete execution binding is copied at registration and is the single
runtime fact. Profiles may select an adapter name but cannot alter its lane.

- Invalid binding or conflicting legacy fields: `TURBO_EINVAL`.
- Invalid pool lane: `TURBO_ERANGE`.
- Cross-thread post queue full: propagate CoroNet's explicit error.
- Actor mailbox full: `TURBO_ENOSPC`.
- Actor command after admission closes: `TURBO_ESHUTDOWN`.
- Copied actor command exceeds its configured bound: `TURBO_EMSGSIZE`.
- Borrowed loop not progressing before the operation deadline:
  `TURBO_ETIMEDOUT`.
- Dispatch after adapter shutdown begins: `TURBO_ESHUTDOWN`.
- Unsupported runtime rebind command: `TURBO_ENOTSUP`.

Posted control requests use heap ownership plus reference counting so a timeout
cannot leave a callback pointing at caller stack memory. A timed-out request is
marked canceled; a callback that has not started observes cancellation and
performs no external side effect.

Actor commands use an absolute monotonic deadline. A queued command that has
not started before that deadline completes with `TURBO_ETIMEDOUT` without
calling its handler. Once a synchronous command has started, its caller waits
for the terminal reply because an owner mutation cannot be safely canceled
mid-transition. Async callers may time out a wait and poll the same reply later.

## Performance model

- Same-lane dispatch: O(1), no post and no additional allocation.
- Cross-lane dispatch: O(1), one MPSC `coro_post`, one loop wake, and the
  adapter's existing completion synchronization.
- Lane selection occurs once during registration, not per message.
- Shared lanes reduce context threads and event-loop objects but can introduce
  queue contention. No claim of higher throughput is made without benchmarks.

Benchmark gates compare private context, shared borrowed context, and four-lane
pool placement under typical and peak send rates. A latency regression over 10%,
throughput regression over 10%, or memory growth over 20% requires explanation
or rollback.

## Validation matrix

- Every binding-kind/field combination and invalid lane.
- Legacy wrapper behavior remains unchanged.
- Callback observes the selected `coro_context_current()`.
- Two adapters on one lane execute context-affine operations serially.
- Adapters on different lanes execute on their selected pool threads.
- Cross-thread socket and FMQ sends use post, not concurrent context pumping.
- Borrowed context/pool shutdown does not stop or destroy the host resource.
- Unstarted borrowed loop fails with a bounded timeout.
- Stop during connect, send, receive, heartbeat, and reconnect drains or
  interrupts without use-after-free.
- Existing socket transport and full FMQ suites pass for private and pool-lane
  execution.

## Implementation status

Implemented in the current tree:

- Installed public binding type plus socket/FMQ `_ex` registration APIs.
- Shared private, owned, borrowed, and pool-lane execution helper.
- Owner-lane socket create/listen/spawn/close and FMQ bind/connect/close.
- Cross-thread synchronous calls and sends through `coro_post`, with bounded
  timeout and request lifetime protection.
- Bounded copy-by-value actor commands, monotonic command IDs, absolute
  deadlines, asynchronous reply handles, close/drain, and FMQ reactor-command
  integration.
- Legacy registration wrappers, explicit borrowed-context host-driver tests,
  explicit transferred-owned-context socket/FMQ data-path tests, pool-lane
  start/stop tests, and existing socket/FMQ transport regressions.

The multi-adapter same-lane/different-lane stress cases and the benchmark gates
above remain performance validation work; they do not change the API or
lifecycle contract.

## Migration and rollback

1. Add the shared public binding type and `_ex` registrations without changing
   existing APIs.
2. Add the internal execution helper and convert socket first, including removal
   of caller-side context pumping.
3. Convert FMQ and enable borrowed context/pool-lane validation.
4. Add lifecycle, affinity, transport, shutdown, and benchmark coverage.
5. Deprecate legacy ownership fields only after all CoroNet adapters share the
   binding.

The common binding header must become an installed public header shared by the
Socket and FMQ packages; the internal execution implementation remains in
`tf_executor_common`. This adds a public compile-time dependency on CoroNet's
context and thread-pool declarations but no new runtime library.

Rollback keeps the old registration functions and private-context behavior.
Because the design changes neither `.flow` data nor network protocols, rollback
requires no data or wire migration.
