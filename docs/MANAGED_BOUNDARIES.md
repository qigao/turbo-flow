# Managed Source/Sink Boundaries

TurboFlow projects operable Source and Sink owners through the existing stable-resource registry. The resource provider remains the only fact source for identity, generation, live state, documents, and commands. The managed-boundary contract does not create a second registry and does not infer capabilities from adapter roles, resource kinds, or legacy snapshots.

## Contract

An owner passes its existing resource operations plus descriptor and snapshot callbacks to `turbo_flow_register_managed_boundary_provider()` before graph compilation. The new versioned provider structure is additive and does not change the layout of `turbo_flow_resource_provider_ops_t` or `turbo_flow_resource_provider_registration_t`.

An owner whose Sink is also an asynchronous terminal adapter uses `turbo_flow_register_managed_async_terminal_adapter()` instead of making two independent registration calls. Its size/versioned aggregate names both explicit contracts and one shared owner context. The adapter schema must include the Sink role, and the copied managed descriptor must also declare the Sink role. The API does not infer either contract from the other.

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

This staging order was selected over three alternatives:

- Resource first: an adapter allocation failure would require resource rollback after owner callbacks had already run.
- Adapter first without staging: rolling back after a boundary failure would invoke the normal adapter shutdown callback and incorrectly consume caller ownership on a failed call.
- A separate staging registry: it would duplicate resource facts and add synchronization and migration cost without improving setup-path complexity.

The combined path remains setup-only. Duplicate checks retain the current O(A) adapter-name and O(R) resource-UID scans, while normal dispatch receives no new allocation, lookup, or branch.

## Migration and rollback

Existing owners and embedded resource-registration structures retain their layout and continue to register as ordinary resources. Migration is explicit: use the additive managed-provider entry point, or the combined async-terminal entry point for a terminal Sink, advertise only capabilities the owner actually implements, and keep the owner-native snapshot as the sole mutable state. There is no compatibility fallback from a managed contract to `turbo_flow_resource_snapshot_t`.

Removing the paired callbacks rolls a standalone owner back to an ordinary resource without changing its data path or existing resource commands. A combined owner can roll back by returning to its previous explicit async-adapter registration; no data format or persisted state changes. The first migration target is the CNet stream Sink tracked by issue #55, while other CNet, CHTTP, and TurboDb owners remain independently tracked under issue #28.

The deterministic, buildable examples are `turbo_flow/tests/test_flow_managed_boundary.c` and `turbo_flow/tests/test_flow_managed_async_terminal.c`. The installed-package consumer in `tests/install_consumer/main.c` validates the same header, initializer, and exported symbol in both C and C++ modes.
