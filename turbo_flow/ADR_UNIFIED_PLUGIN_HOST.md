# ADR: Unified DLL PluginHost Boundary

## Status

Accepted for #65 as the first implementation slice of #63.

## Context

Protocol and Business Protocol currently own separate dynamic-library registries. Product assembly
instead receives caller-built arrays of adapter and resource providers. The former retains DLLs but
cannot register general flow capabilities; the latter can assemble configured Graphs but has no
module ownership, transaction, or lease. Moving CNet, CHTTP, TurboDB, FlowMQ, protocol, and business
capabilities behind DLLs requires one host-owned boundary before individual providers can migrate.

Salts supplies the graph, metadata, containers, schedulers, and platform primitives used by
TurboFlow, but it does not currently expose a general-purpose plugin loader. The platform dynamic
library calls therefore remain isolated in this PluginHost adapter and do not enter Graph or CFlow.

## Decision

Add `TurboFlow::PluginHost`, with one canonical export named `turbo_flow_plugin_get_api`. The root
API and every host/registration/config/error structure are pure C and size/versioned. This first ABI
minor supports Product adapter and resource provider capabilities. Later ABI-minor extensions add
typed operation, schema, protocol, and business capability registration without changing the root
symbol or ABI major.

Plugin loading is a control-plane transaction:

```text
open explicit DLL -> resolve root symbol -> validate root API -> plugin load
  -> stage bounded providers -> validate declared capabilities -> commit all entries
```

Any failure discards staged entries, shuts down and destroys a produced plugin instance, then unloads
the DLL. Duplicate plugin IDs or provider kinds and every capacity violation fail before commit.

A catalog snapshot copies the committed Product provider descriptors and holds one lease on every
module represented by those descriptors. A Graph generation owner retains the snapshot until its
adapters, resources, CFlow runs, pending claims, and callbacks have drained. Host shutdown returns
`SALTS_EBUSY` while a snapshot exists. Once leases reach zero, modules quiesce, shut down, destroy,
and unload in reverse load order. A failed quiesce/shutdown leaves the host allocated in an explicit
retryable state; it never unloads code that may still be callable.

## Consequences

- Architecture: Config and Graph remain unaware of DLLs. Product consumes a read-only provider view;
  PluginHost owns module handles, lifecycle, committed catalogs, and generation leases.
- Interface: a new additive installed C ABI and CMake component are introduced. Existing Product
  calls remain source-compatible.
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
- Migration: subsequent #63 slices add capability categories, migrate the two protocol registries,
  package concrete I/O provider DLLs, then make Gateway manifests the only assembly source.
- Rollback: before Gateway migration, removing the new target/API restores the prior deployment.
  After a provider migrates, rollback is deployment-level selection of an older complete package,
  never a runtime fallback to a statically linked implementation.

## Verification

DLL fixtures cover missing symbols, incompatible ABI, invalid root tables, partial registration,
duplicates, capacity boundaries, snapshot leases, and lifecycle order. Product preflight consumes a
snapshot view. Installed C and C++ consumers compile the public ABI. Debug/ASan, Release, and
install-tree test suites remain mandatory.
