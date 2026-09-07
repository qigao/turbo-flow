# Managed Source/Sink Boundaries

TurboFlow projects operable Source and Sink owners through the existing stable-resource registry. The resource provider remains the only fact source for identity, generation, live state, documents, and commands. The managed-boundary contract does not create a second registry and does not infer capabilities from adapter roles, resource kinds, or legacy snapshots.

## Contract

An owner passes its existing resource operations plus descriptor and snapshot callbacks to `turbo_flow_register_managed_boundary_provider()` before graph compilation. The new versioned provider structure is additive and does not change the layout of `turbo_flow_resource_provider_ops_t` or `turbo_flow_resource_provider_registration_t`.

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

## Migration and rollback

Existing owners and embedded resource-registration structures retain their layout and continue to register as ordinary resources. Migration is explicit: use the additive managed-provider entry point, advertise only capabilities the owner actually implements, and keep the owner-native snapshot as the sole mutable state. There is no compatibility fallback from a managed contract to `turbo_flow_resource_snapshot_t`.

Removing the paired callbacks rolls an owner back to an ordinary resource without changing its data path or existing resource commands. Concrete CNet, CHTTP, and TurboDb owner migrations are tracked separately under GitHub issue #28.

The deterministic, buildable C example is `turbo_flow/tests/test_flow_managed_boundary.c`. The installed-package consumer in `tests/install_consumer/main.c` validates the same header and query surface in both C and C++ modes.
