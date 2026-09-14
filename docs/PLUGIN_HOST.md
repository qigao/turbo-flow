# Unified PluginHost Core

`TurboFlow::PluginHost` is the cold-path owner for explicit TurboFlow capability DLLs. It loads one
canonical root symbol, commits Product, Protocol, and Business providers transactionally, and
exposes immutable catalog snapshots. CFlow, compiled Graphs, and protocol Sources retain direct
provider callbacks; none performs DLL discovery, symbol lookup, string lookup, allocation, or
registry locking per message.

This document describes ABI major 1, minor 1. Minor 1 adds typed Protocol and Business providers to
the Product adapter/resource capabilities introduced in minor 0. Typed operation/schema
registration and concrete CNet/CHTTP/TurboDB provider DLLs remain tracked by #63. Their absence
never activates a static or legacy fallback.

## DLL contract

Each plugin defines exactly one discovery symbol:

```c
TURBO_FLOW_PLUGIN_ENTRY const turbo_flow_plugin_api_v1_t *
turbo_flow_plugin_get_api(void);
```

Define `TURBO_FLOW_PLUGIN_BUILD` only while compiling the provider DLL so that the canonical entry is
exported. The returned vtable, its strings, callback code, provider descriptors, and provider
contexts stay owned by the DLL. `plugin_id` accepts 1–127 ASCII letters, digits, `.`, `_`, and `-`;
`plugin_version` accepts those characters plus `+`. ABI major must equal 1. A newer ABI minor is
accepted only because the declared structure is at least the complete v1 size.

`load` receives a host allocator vtable whose lifetime ends after the plugin's `destroy` callback.
Memory obtained through it must be released with the matching host `deallocate`. Alternatively, a
plugin may allocate with its own CRT, but then its `destroy` callback must free that memory inside the
same DLL. C++ objects, exceptions, STL containers, `FILE *`, and platform socket handles do not cross
the ABI.

ABI v1 treats provider DLLs as trusted in-process code. It prevents callback use after unload through
leases, but it does not isolate memory corruption or process crashes. Manifest allowlists, signature
verification, permissions, and optional process isolation remain later #63 work.

`register_capabilities` runs synchronously on the Gateway control thread. The registration vtable is
call-scoped and must not be retained or called from another thread. Every descriptor crosses the
boundary in a size/versioned wrapper: Product wrappers are in `turbo_flow_plugin.h`; Protocol and
Business wrappers are in `turbo_flow_plugin_protocol.h`. The outer wrapper and embedded provider
descriptor are both validated. Every declared capability must register at least one matching
provider, and a plugin may not register a capability it did not declare. Provider identities are
unique within their typed catalog.

## Host API

All mutable PluginHost calls are control-thread confined.

- `turbo_flow_plugin_host_create(config, host_out, error)` validates the size/versioned capacity
  configuration and allocates bounded CSTL registries. Module capacity must be 1–1024; adapter,
  resource, protocol, and business capacities may be zero and may not exceed 65536. A minor-0-sized
  config has no Protocol/Business fields, so those capacities are exactly zero; trailing caller
  memory is never inspected. It returns `SALTS_OK`, `SALTS_EINVAL`, or `SALTS_ENOMEM`; `host_out` is
  null on failure.
- `turbo_flow_plugin_host_load(host, path, error)` accepts one non-empty UTF-8 path of at most 511
  bytes. On Windows it uses `LoadLibraryExW`; on POSIX it uses `dlopen(RTLD_NOW | RTLD_LOCAL)`. It
  returns the exact callback/registry status, including `SALTS_ENOENT`, `SALTS_EPROTO`,
  `SALTS_EALREADY`, or `SALTS_ENOSPC`. `error.stage`, `plugin_id`, `path`, and `message` identify the
  failing boundary. No other path or implementation is attempted.
- `turbo_flow_plugin_catalog_snapshot_create(host, snapshot_out, error)` copies all typed provider
  arrays and acquires one module lease per committed DLL. It returns `SALTS_OK`, `SALTS_EBUSY`,
  `SALTS_ENOSPC`, or `SALTS_ENOMEM`.
- `turbo_flow_plugin_catalog_snapshot_product_registry(snapshot, registry_out)` fills a caller-owned
  `turbo_flow_product_provider_registry_t`. The arrays are immutable and valid until snapshot
  destruction. It returns `SALTS_OK` or `SALTS_EINVAL`.
- `turbo_flow_plugin_catalog_snapshot_protocol_catalog(snapshot, catalog_out)` fills borrowed,
  immutable Protocol and Business arrays. `turbo_flow_protocol_registry_create` and
  `turbo_flow_protocol_business_registry_create` retain the snapshot and copy only bounded registry
  entries; they never open a DLL.
- `turbo_flow_plugin_catalog_snapshot_retain(snapshot)` adds a caller-serialized reference.
  `turbo_flow_plugin_catalog_snapshot_destroy(snapshot)` releases one reference; the final release
  drops every module lease and invalidates all borrowed catalog views. Destroy accepts null.
- `turbo_flow_plugin_host_destroy(host, quiesce_timeout_ms, error)` returns `SALTS_EBUSY` without
  lifecycle side effects while a snapshot exists. Otherwise it quiesces all modules, shuts them
  down, then destroys and unloads them, with each phase in reverse load order. A quiesce/shutdown
  failure leaves a retryable explicit host state; load and new snapshots remain rejected. On
  `SALTS_OK`, the host is freed.

`turbo_flow_plugin_host_config_t` and `turbo_flow_plugin_error_t` carry the same explicit ABI
major/minor as the root vtable. The error must be initialized with `TURBO_FLOW_PLUGIN_ERROR_INIT`
before every call that accepts it. The lifecycle observer is synchronous, runs on the calling control thread, and
receives a transient `plugin_id` view. It is intended for bounded diagnostics/audit collection and
must not call back into the same host.

## Registration transaction

```text
open explicit DLL
  -> resolve turbo_flow_plugin_get_api
  -> validate root ABI and identity
  -> load plugin instance
  -> stage typed Product/Protocol/Business providers
  -> validate capabilities and duplicates
  -> commit module plus all providers
```

Until the final commit, count/query APIs expose no part of the staged plugin to other host code. Any
failure truncates every staged provider vector to its original size, calls `shutdown` and
`destroy` when `load` produced an instance, and unloads the DLL. The first failure status remains the
reported result even when cleanup reports a later error through the lifecycle observer.

## Generation ownership

The PluginHost is the single fact source for module handles, module lifecycle state, catalog entries,
and lease counts. A catalog snapshot is a derived immutable view. The owner of a Graph generation
must retain its snapshot until Source admission is stopped and all adapters, resources, CFlow runs,
pending claims, and callbacks are drained. Releasing it earlier permits a later host shutdown to
unload callable code and is therefore a lifecycle contract violation.

The implementation permits load followed by whole-host shutdown; it does not expose runtime
per-module unload or hot reload. Later generation cutover must load and validate the new module and
Graph generation, switch admission atomically, drain the old generation, then release its snapshot.

## Complete host example

This program loads one explicit plugin and inspects its Product catalog. It intentionally has no
fallback path.

```c
#include <salts_error.h>
#include <turbo_flow_plugin.h>

#include <stdio.h>

int main(int argc, char **argv) {
  turbo_flow_plugin_host_config_t config = TURBO_FLOW_PLUGIN_HOST_CONFIG_INIT;
  turbo_flow_plugin_error_t error = TURBO_FLOW_PLUGIN_ERROR_INIT;
  turbo_flow_product_provider_registry_t providers =
      TURBO_FLOW_PRODUCT_PROVIDER_REGISTRY_INIT;
  turbo_flow_plugin_catalog_snapshot_t *snapshot = NULL;
  turbo_flow_plugin_host_t *host = NULL;
  int rc;

  if (argc != 2) return 2;
  rc = turbo_flow_plugin_host_create(&config, &host, &error);
  if (rc == SALTS_OK) rc = turbo_flow_plugin_host_load(host, argv[1], &error);
  if (rc == SALTS_OK)
    rc = turbo_flow_plugin_catalog_snapshot_create(host, &snapshot, &error);
  if (rc == SALTS_OK)
    rc = turbo_flow_plugin_catalog_snapshot_product_registry(snapshot, &providers);
  if (rc == SALTS_OK)
    printf("adapters=%zu resources=%zu\n", providers.adapter_provider_count,
           providers.resource_provider_count);
  else
    fprintf(stderr, "plugin stage=%d status=%d: %s\n", (int)error.stage, rc,
            error.message);

  turbo_flow_plugin_catalog_snapshot_destroy(snapshot);
  if (host) {
    turbo_flow_plugin_error_t shutdown_error = TURBO_FLOW_PLUGIN_ERROR_INIT;
    const int shutdown_rc = turbo_flow_plugin_host_destroy(host, 5000u, &shutdown_error);
    if (rc == SALTS_OK && shutdown_rc != SALTS_OK) rc = shutdown_rc;
  }
  return rc == SALTS_OK ? 0 : 1;
}
```

Build consumers with `find_package(TurboFlow CONFIG REQUIRED COMPONENTS PluginHost)` and link
`TurboFlow::PluginHost`.
