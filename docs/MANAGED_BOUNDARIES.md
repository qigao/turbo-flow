# Managed Source/Sink Boundaries

TurboFlow projects operable Source and Sink owners through the existing stable-resource registry. The resource provider remains the only fact source for identity, generation, live state, documents, and commands. The managed-boundary contract does not create a second registry and does not infer capabilities from adapter roles, resource kinds, or legacy snapshots.

## Contract

An owner passes its existing resource operations plus descriptor and snapshot callbacks to `turbo_flow_register_managed_boundary_provider()` before graph compilation. The new versioned provider structure is additive and does not change the layout of `turbo_flow_resource_provider_ops_t` or `turbo_flow_resource_provider_registration_t`.

An owner whose Sink is also an asynchronous terminal adapter uses `turbo_flow_register_managed_async_terminal_adapter()` instead of making two independent registration calls. Its size/versioned aggregate names both explicit contracts and one shared owner context. The adapter schema must include the Sink role, and the copied managed descriptor must also declare the Sink role. The API does not infer either contract from the other.

A Source owner that feeds CFlow Reactive demand uses `turbo_flow_register_managed_source_adapter()` so its adapter and managed boundary commit as one transaction. During that exact Source adapter's synchronous `start` callback, it may call `turbo_flow_managed_source_run_open()` once. The callback-scoped API does not open global Flow admission, rejects copied stage views and calls from other threads, and leaves Publisher ownership with the caller on failure. The complete Source run contract is documented in [Managed Source Run Binding](MANAGED_SOURCE_RUNS.md).

The descriptor is copied into the host during registration. It declares:

- stable resource UID and owner identity;
- Source and/or Sink roles;
- demand, replay, durable-settlement, and manual-review capabilities;
- accepted resource-command kinds; an empty mask explicitly declares a read-only boundary and does not require a command callback;
- schema-declared input and output content contracts for the applicable roles.

The live snapshot remains owner-synchronized and reports lifecycle state, demand, queue depth/capacity, in-flight work, lag, accepted/completed/rejected counts, backpressure, last status, and generation. Reads never mutate owner state.

`turbo_flow_managed_boundary_count()` and the two indexed query functions enumerate only resources that explicitly registered through the managed-boundary entry point. An ordinary resource provider remains a normal resource and is not projected as a managed boundary; there is no snapshot or role inference path.

## Ownership and errors

- The owner synchronizes each metadata and snapshot callback. TurboFlow brackets a snapshot with metadata reads and returns `SALTS_EBUSY` when their generations change, so callers can retry rather than mistake a concurrent command for contract drift.
- TurboFlow owns the copied descriptor until registry reset or graph destruction. Query outputs are caller-owned values.
- Owner callback errors are returned unchanged. Malformed descriptors or snapshots, identity drift, future observations, impossible queue depth, and `completed > accepted` fail with `SALTS_EPROTO`. `rejected` counts pre-admission rejection and is independent of accepted/completed terminal accounting.
- The existing resource command dispatcher checks deadline, expected generation, and idempotency before dispatch. For a managed boundary it also rejects commands absent from the copied command mask with `SALTS_ENOTSUP`, before invoking the owner.
- All enumeration is O(R) in registered resources and performs no allocation. A descriptor query invokes one metadata callback; a live snapshot query invokes one snapshot callback bracketed by two metadata callbacks.

For the combined async-terminal registration, the caller owns the context until the whole call succeeds. Core first validates the aggregate and rejects a duplicate adapter before invoking boundary callbacks. It copies the adapter operations into a staging registration with `shutdown` suppressed, then registers the managed resource in the canonical registry. An adapter-stage failure occurs before the resource phase. A resource-stage callback, validation, duplicate, or allocation failure removes the staging adapter, returns the original error, and does not invoke shutdown. Only after both registries contain the validated owner does core restore the adapter shutdown callback as the ownership-transfer commit point. A successful Flow teardown follows that callback exactly once.

The managed Source aggregate uses the same staging transaction. On successful start, Core owns the returned run handle while the Source owner retains the Publisher backing state through cancellation. The run count shares `async_ingress_config.queue_capacity` with ordinary Reactive runs. Stop closes external admission, cancels ordinary runs, closes managed Source subscriptions in reverse adapter order, drains accepted graph work, and only then invokes Source `stop`; registry teardown invokes `shutdown` after no run can retain the owner context. No runtime registration, inferred capacity, alternate registry, or compatibility path is used.

This staging order was selected over three alternatives:

- Resource first: an adapter allocation failure would require resource rollback after owner callbacks had already run.
- Adapter first without staging: rolling back after a boundary failure would invoke the normal adapter shutdown callback and incorrectly consume caller ownership on a failed call.
- A separate staging registry: it would duplicate resource facts and add synchronization and migration cost without improving setup-path complexity.

The combined path remains setup-only. Duplicate checks retain the current O(A) adapter-name and O(R) resource-UID scans, while normal dispatch receives no new allocation, lookup, or branch.

## CNet stream Sink

`turbo_flow_cnet_stream_sink_register()` is the first concrete combined owner. One call atomically registers its async-terminal adapter and managed Sink boundary. For names that fit the managed contract, its owner is the adapter name and its stable UID is `cnet-stream-sink:<adapter-name>`. Longer existing adapter names remain valid: registration derives one bounded `xxh3-128:<digest>` owner identity and uses `cnet-stream-sink:<owner>` as the UID. The input descriptor declares IO-transport opaque `application/octet-stream` content with schema `CNetStream/NonEmptyBytes/v1`, matching the runtime rejection of null or empty payloads.

The boundary advertises durable settlement and no management commands. Its single-request CFlow IO Actor supplies capacity, queued, in-flight, and backpressure facts; it does not invent demand or lag. `accepted` advances only after Actor admission, `completed` only when the accepted terminal claim is consumed exactly once, and `rejected` records pre-admission failure independently. Registered, connecting, connected, stopping/draining, stopped/detached, and failed owner states map directly to the corresponding managed lifecycle. Snapshot access is synchronized with Actor initialization, admission, drain, and destruction; a query racing the short admission commit window returns `SALTS_EBUSY`.

The owner does not project `turbo_flow_cnet_stream_sink_snapshot_t` into this contract. Managed callbacks read the sink and IO Actor facts directly, and the zero command mask has no hidden command or compatibility path.

## CNet datagram Sink

`turbo_flow_cnet_datagram_sink_register()` uses the same atomic adapter/resource transaction for one fixed-peer raw UDP Sink. Its bounded owner identity follows the stream Sink rule, with stable UID `cnet-datagram-sink:<owner>`. The input descriptor is IO-transport opaque `application/octet-stream` with schema `CNetDatagram/NonEmptyBytes/v1`; durable settlement is explicit and the command mask is empty.

The configured CNet `send_capacity` is also the CFlow IO Actor request capacity and the managed queue capacity. Actor phase facts define queue depth, in-flight work, and backpressure; there is no inferred snapshot or secondary queue. `accepted`, `completed`, and `rejected` use the same exact admission/terminal/pre-admission boundaries as the stream Sink, but support N concurrent datagram operations. A managed query racing the short claim-transfer window returns `SALTS_EBUSY`.

Start, poll, stop, failure cleanup, and detach share one lifecycle owner lane. Managed snapshots may read the Actor's internally synchronized statistics while progress runs, but Actor creation/destruction and submission commit are protected by the sink lifecycle gate. Native sink snapshots fail fast with `SALTS_EBUSY` during owner-lane mutation. Stop closes Actor admission first, requests CNet cancellation, waits for authoritative tagged completions, drains delivery/acknowledgement, and only then destroys Actor, executor, and datagram. CNet cancellation is represented to the Actor as `CANCELLED` with zero bytes and `SALTS_OK`; the terminal claim exposes `SALTS_ECANCELED` to the publisher exactly once.

## CNet provider DLL

Gateway assembly loads `tf_cnet_plugin` through the canonical PluginHost entry and obtains all six CNet Source/Sink kinds from one transactional vtable catalog. The Gateway consumer links only `TurboFlow::PluginHost`; missing DLLs, symbols, kinds, dependencies, explicit fields, or capacity never select the embedded `TurboFlow::CNetAdapter` API. The module snapshot retains the DLL, the Graph generation retains the six bounded owner vtables, and network progress remains caller-driven through generation poll.

The three provider-created Sources use the combined managed Source registration. Their operational snapshot reads the concrete CNet Source snapshot, so demand, queue depth, in-flight receive, message count, and last status are observed owner facts rather than configuration-derived estimates. All three terminal Sinks use combined managed registration. See `io/cnet/ADR_CNET_PLUGIN.md` for ownership, rollback, deployment, and the no-fallback decision.

### CNet packet Sink

The fixed-peer UDP/KCP/secure-KCP/FEC Sink atomically registers its adapter, resource
metadata and managed boundary. Its UID is `cnet-packet-sink:<owner>`; oversized
adapter names use the same bounded XXH3-128 owner identity as the other CNet Sinks.
Input is opaque `application/octet-stream`, schema `CNetPacket/NonEmptyBytes` v1.
Durable settlement follows the existing UDP send completion or KCP ACK contract.
No managed write commands are advertised.

The Actor owns queue and in-flight state. Managed snapshots derive queue depth
from admitted plus ready requests and in-flight from the remaining active requests.
Accepted/completed/rejected are cumulative across restart; generation identifies
the registration, independently of packet session generations. Admission commits
return `SALTS_EBUSY` to a concurrent managed snapshot. Stop closes admission,
waits for admitted submitters, drains endpoint terminals and then destroys the
quiescent Actor. Successful stop has zero queue/in-flight and equal accepted and
completed counts. Native snapshot still reports `SALTS_EBUSY` while the owner lane
is active and waits for the short admission commit.

## CHTTP deferred server Source and Sink

`turbo_flow_chttp_server_register()` atomically registers the deferred HTTP server
adapter and one managed Source/Sink boundary. A bounded adapter name is preserved
as the owner and produces UID `chttp-server:<adapter-name>`; an oversized existing
name instead uses a stable `xxh3-128:<digest>` owner. The input is the opaque
`application/octet-stream` `CHTTPServerResponse/Body/v1` contract consumed by the
terminal adapter. The output is the corresponding
`CHTTPServerRequest/Body/v1` contract produced by request publication. Empty
bodies remain valid, and neither contract is inferred from an HTTP Content-Type.

The configured request-slot count is the managed capacity. Queue depth is zero
because the server has no second owner queue, while in-flight counts slots whose
request publication was accepted. `accepted` advances after publication accepts
the request; every accepted slot advances `completed` exactly once when reply,
cancel, or terminal failure releases it. `rejected` counts failures before that
managed admission point, independently of the native server counters. In
particular, a failure to defer after successful publication is one managed
acceptance and completion but remains a native rejected request. A terminal
settlement means that the server-owned deferred request has been resolved; it
does not assert that a remote peer consumed the bytes or that any payload was
persisted.

The command mask contains `QUIESCE` and `RESUME`. They run on the same host thread
that owns server progress, serialize with snapshot reads under the server mutex,
honor the dispatcher deadline/idempotency checks, and recheck expected generation
at the mutation point. A real transition increments generation exactly once; a
same-state command is a successful no-op, and generation overflow fails without
changing admission state. Registered, starting, and running map directly;
quiesced with accepted slots still in flight maps to draining and otherwise to
quiescent; stopping maps to stopping; stopped and detached map to stopped; failed
maps to failed. The short publication-to-slot-accounting window is reported as
`SALTS_EBUSY`, rather than exposing a mixed snapshot.

## Migration and rollback

Existing owners and embedded resource-registration structures retain their layout and continue to register as ordinary resources. Migration is explicit: use the additive managed-provider entry point, the combined async-terminal entry point for a terminal Sink, or the combined managed Source entry point for a Reactive Source. Advertise only capabilities the owner actually implements, and keep the owner-native snapshot as the sole mutable state. There is no compatibility fallback from a managed contract to `turbo_flow_resource_snapshot_t`.

Removing the paired callbacks rolls a standalone owner back to an ordinary resource without changing its data path or existing resource commands. This is a source-level migration reversal, not a runtime fallback. The CNet stream and datagram Sinks, CNet packet Sink, and CHTTP deferred server now require their combined managed registration; other CHTTP and TurboDb owners remain independently tracked under issue #28.

The deterministic, buildable examples are `turbo_flow/tests/test_flow_managed_boundary.c`, `turbo_flow/tests/test_flow_managed_async_terminal.c`, and `turbo_flow/tests/test_flow_managed_source.c`. The installed-package consumer in `tests/install_consumer/main.c` validates the same headers, initializers, and exported symbols in both C and C++ modes.
