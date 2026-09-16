# Protocol and Business Unified PluginHost Migration Plan

GitHub: #67 (parent #63)

## Goal

Make the unified `turbo_flow_plugin_get_api` entry and PluginHost catalog the only dynamic loading
path for protocol codecs and business protocol providers, while preserving provider-neutral wire and
business behavior.

## Tasks

- [x] Add a failing focused test for typed Protocol/Business capability registration and snapshots.
- [x] Extend PluginHost ABI minor with bounded typed provider wrappers, atomic registration, snapshot
      views, and snapshot reference ownership.
- [x] Move Protocol/Business registry ownership behind PluginHost snapshots; remove direct register,
      specialized load, platform module, and legacy export APIs.
- [x] Convert all codec DLLs and the OCPP business DLL to the one unified root export.
- [x] Migrate protocol, business, install-consumer, and export-table tests without changing wire
      behavior or service ownership.
- [x] Update ABI, lifecycle, package, migration, and no-fallback documentation.
- [x] Run focused repeats, protocol/business regressions, Debug/ASan and Release suites, install-tree
      C/C++ verification, CodeGraph affected analysis, symbol/fallback scans, and diff checks.
- [ ] Commit, open a PR closing #67, wait for required checks, merge, and update #63/#28/#2.

## Invariants

- PluginHost is the only owner of module handles, root instances, provider catalogs, and leases.
- A registry retains its source snapshot; an owner retains its registry entry and prevents registry
  destruction until provider close completes.
- No DLL can commit only one part of a multi-category registration.
- No protocol frame path performs DLL discovery, symbol lookup, catalog lookup, allocation, or lock.
- Existing protocol/business service objects are created and destroyed by the provider DLL side.
- Missing DLL/symbol/ABI/provider, duplicate identity, and capacity exhaustion fail before open/start;
  there is no direct-C, CMake, alternate-symbol, or specialized-loader fallback.
