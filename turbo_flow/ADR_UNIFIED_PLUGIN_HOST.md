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

ABI minor 3 adds the root `TURBO_FLOW_PLUGIN_CAP_EXTERNAL_POLL` modifier and bounded
`turbo_flow_plugin_product_owner_publish()` output negotiation. A plugin that may return an
external-poll owner declares the modifier, so a pre-v1.3 host rejects the unknown capability before
calling load or materialize. Materializers preserve the caller-seeded output size and never assign
an extended owner directly across the DLL boundary.

Transactional Product owners may append an `EXTERNAL_POLL` capability to the v1 descriptor. The
flag requires the full descriptor tail and a non-NULL callback; lifecycle-only v1.0 prefixes remain
valid. `turbo_flow_plugin_generation_poll()` is serialized with generation lifecycle on the Gateway
control thread, visits every flagged owner once, and assigns the call's only nonzero wait budget to
one rotating owner. Polling is rejected after retirement begins. This keeps CNet/CHTTP caller-owned
progress explicit without a hidden worker or a concrete adapter cast in Gateway.

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


## Generation Config removal — ABI 3.5 (2026-10-11)

The replacement stage/resource paths now cover every input consumed by
`turbo_flow_plugin_generation_create()`: provider instances use the explicit
Component scope and typed config, while operation policy/materializer records
come from `.flow` and deployment owners arrive through exact resource bindings.
The remaining resolved-config argument only rejected legacy binding tables and
forced callers to allocate an otherwise unused YAML document.

The chosen change removes that argument, rather than keeping a nullable argument,
a dummy document, or a compatibility wrapper. PluginHost exact ABI is 3.5;
hosts and modules must be rebuilt together. This is a source/binary breaking
change to the generation creation function. A rollback requires the complete
previous host/plugin package, not mixing binaries across versions.

Graph transfer, preflight-before-side-effects, cleanup retry handles, independent
result domains and Component/resource release ordering keep their existing
owners. The C/C++ signature contracts and installed operation consumer are
updated. Operation and materializer runtime fixtures no longer parse YAML;
legacy YAML-only rejection cases are removed from these generation suites.
Typed-policy mismatch, capacity, preflight failures, result retention and
cleanup-retry cases continue to exercise the canonical inputs. Generation config
admission also rejects the previous and future minor ABI before Graph transfer.

The retained-projection DLL integration now materializes two canonical provider
owners with explicit resource leases, destroys that generation and unloads its
provider/resource modules, then clones and reads the independently retained
projection. Its own DLL remains pinned until the last projection is released.
No legacy transactional provider is substituted for the canonical path.

This closes only the generation-create consumer of #241. Product assembly,
configured host loading, Security/Policy authoring and the Config target itself
remain migration work; the subsystem is not yet removed.

### Verification of this slice

Windows `win-release-user` full graph build and `install-win-release-user` pass.
The targeted generation/operation/materializer/durable/TCP/UDP/TurboDB/RulesForge
selection passes 10/10 CTests; the subsequent `ctest --preset win-release-user
-L plugin --output-on-failure` regression passes 25/25, including the migrated
projection lifetime test and previous/current/future ABI admission checks.

`test_turbo_flow_install_consumer` builds the installed full C++ consumer with
the new generation signature. Schema-only and installed CNet plugin consumers
run successfully. The parent installed-consumer test still fails at the existing
CNet missing-transitive negative fixture (`tests/install_consumer/run.cmake`):
copying only the plugin does not isolate dependency search from the inherited
runtime paths. No gate is skipped and full installed runtime qualification is
not claimed. Debug/sanitizers and other platforms were not rerun for this slice.


## Explicit deployment module loading (2026-10-11)

The remaining configured host-loading consumer used resolved YAML only to
project ordered module ID/version/path selections. It is replaced by
`turbo_flow_plugin_host_create_modules()` and a borrowed native module-spec array.
All entries, duplicate IDs and capacity are admitted before any module opens;
identity/version checks still precede each plugin load callback. Successful
creation retains no pointers into the caller's array. Rollback preserves the
existing phase order and returns a cleanup-only host if quiesce/shutdown fails.

Keeping a nullable resolved input would retain the Config dependency; switching
to unchecked host_load calls would lose deployment identity admission. The chosen
entry point reuses the existing loader and destruction owner. It introduces no
new module registry, parser or canonical provider publication path. Canonical
Salts Plugin/Component modules remain outside this PluginHost module list.

The old create_configured symbol is removed without an alias. Consumers must
supply explicit deployment entries and handle a non-NULL cleanup owner on error.
The root vtable/config layout and ABI 3.5 remain unchanged; this is a host API
symbol replacement, not a provider ABI extension. Rollback uses the previous
complete host package and its matching caller. Security/Policy, Product assembly
and physical Config removal remain open under #241.


Validation for this slice: `cmake --build --preset win-release-user`,
`cmake --build --preset install-win-release-user`,
`ctest --preset win-release-user -R "^test_flow_plugin_host$" --output-on-failure`
and `ctest --preset win-release-user -L plugin --output-on-failure` pass
(1/1 focused host test and 25/25 plugin regressions). The host suite covers
ordered loading, ID/version mismatch before load, invalid later entries before
any load, empty/null/duplicate/capacity admission, reverse rollback and retryable
cleanup. The C++ exact-signature contract compiles in the host suite.

The formal `test_turbo_flow_install_consumer` still fails. Its full installed
C++ consumer compiles/links the new module-list API, but is not run before the
CNet consumer build fails: installed Salts `platform.h` includes missing
`cmeta_api.h`. The shared Salts root changed during validation; its preceding
2.3.0-rc.11 provenance record is now in a backup directory. This run neither
qualifies the current shared SDK nor reaches the previously failing missing-DLL
negative case. Debug/sanitizers and other platforms were not run in this slice.
