# ADR: Unified DLL PluginHost Boundary

## Status

Accepted for #65 and extended by #67 and #70 as implementation slices of #63.

## Context

Product assembly previously received caller-built arrays of adapter and resource providers, while
Protocol and Business Protocol owned separate dynamic-library registries. That split produced three
module/registry ownership models. Moving CNet, CHTTP, TurboDB, FlowMQ, protocol, and business
capabilities behind DLLs requires one host-owned boundary.

Salts supplies the graph, metadata, containers, schedulers, and platform primitives used by
TurboFlow, but it does not currently expose a general-purpose plugin loader. The platform dynamic
library calls therefore remain isolated in this PluginHost adapter and do not enter Graph or CFlow.

## Decision

Add `TurboFlow::PluginHost`, with one canonical export named `turbo_flow_plugin_get_api`. The root
API and every host/registration/config/error structure are pure C and size/versioned. ABI minor 0
supports Product adapter/resource providers. ABI minor 1 adds typed Protocol/Business registration
and snapshot catalogs. ABI minor 2 adds separate transactional Product factories and Graph
generation ownership without changing the root symbol or ABI major. Later ABI-minor extensions may
add typed operation and schema registration.

Plugin loading is a control-plane transaction:

```text
open explicit DLL -> resolve root symbol -> validate root API -> plugin load
  -> stage bounded providers -> validate declared capabilities -> commit all entries
```

Any failure discards staged entries, shuts down and destroys a produced plugin instance, then unloads
the DLL. Duplicate plugin IDs or provider kinds and every capacity violation fail before commit.

Gateway Graph assembly is a second transaction:

```text
snapshot catalog -> validate every referenced resource/adapter -> reserve owner slots
  -> run every resource/adapter preflight -> move parsed Graph
  -> materialize resources -> materialize adapters -> compile -> publish generation
```

Preflight cannot mutate the Graph. Before materialization, failure leaves the parsed Graph with the
caller. Once materialization begins, failure destroys the new Graph and all transferred owners in
reverse order; the legacy Product provider catalog is never a fallback. Exactly one owner vtable is
required per successful factory call, and that DLL destroys the opaque object it created.

A catalog snapshot copies every committed typed provider descriptor and holds one lease on every
module represented by those descriptors. Protocol/Business registries retain that snapshot rather
than owning DLL handles. A Graph generation owner retains the snapshot until its adapters,
resources, protocol owners, CFlow runs, pending claims, and callbacks have drained. Host shutdown returns
`SALTS_EBUSY` while a snapshot exists. Once leases reach zero, modules quiesce, shut down, destroy,
and unload in reverse load order. A failed quiesce/shutdown leaves the host allocated in an explicit
retryable state; it never unloads code that may still be callable.

Generation retirement is caller-serialized and retryable. An explicit lease covers every active
CFlow run or asynchronous callback. With no leases, owners quiesce in reverse order, Graph stops,
owners drain and shut down in reverse order, Graph is destroyed, owners are destroyed in reverse
order, and only then is the catalog snapshot released. A successful lifecycle transition is not
repeated after a later callback fails.

## Consequences

- Architecture: Config and Graph remain unaware of DLLs. Product and Protocol consume read-only
  provider views; PluginHost owns module handles, lifecycle, committed catalogs, and generation
  leases. The specialized Protocol/Business loaders and direct registry mutation APIs are removed.
- Interface: a new additive installed C ABI and CMake component are introduced. Existing Product
  calls remain source-compatible but are embedded-only and are not accepted by Gateway generation.
- State: PluginHost is the sole fact source for module state and lease counts. Snapshot arrays are
  derived immutable copies.
- Errors: file, symbol, ABI, identity, lifecycle, callback, duplicate, and capacity failures report a
  structured stage and preserve the first status. No alternate DLL or static provider is attempted.
- Concurrency: load/snapshot/shutdown are confined to one control thread. Provider callbacks retain
  their own declared synchronization obligations.
- Security: ABI v1 loads trusted in-process DLLs and provides lifecycle isolation, not memory or
  crash isolation. Manifest allowlists, signatures, permissions, and out-of-process isolation remain
  later #63 deployment work.
- Performance: discovery and string lookup occur only during load or Product preflight. Compiled
  Graph callbacks retain direct vtable/function pointers under a module lease.
- Migration: #67 migrates both protocol registries and all current codec/business DLLs. Subsequent
  #63 slices add remaining capability categories, package concrete I/O provider DLLs, then make
  Gateway manifests the only assembly source.
- Rollback: before Gateway migration, removing the new target/API restores the prior deployment.
  After a provider migrates, rollback is deployment-level selection of an older complete package,
  never a runtime fallback to a statically linked implementation.

## Verification

DLL fixtures cover missing symbols, incompatible ABI, invalid root tables, partial registration,
duplicates, capacity boundaries, snapshot leases, and lifecycle order. Product preflight consumes a
snapshot view. Installed C and C++ consumers compile the public ABI. Debug/ASan, Release, and
install-tree test suites remain mandatory.
