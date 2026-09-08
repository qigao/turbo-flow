# Unified PluginHost Core Implementation Plan

GitHub: #65 (parent #63)

## Goal

Introduce the first usable unified DLL boundary without changing the Graph data path: one canonical
root vtable, bounded transactional Product provider registration, immutable catalog snapshots, and
module leases that prevent callback code from being unloaded while a generation can still use it.

## Tasks

- [x] Add failing tests and DLL fixtures for valid load, bad symbol/ABI, duplicate registration,
      callback failure, bounded capacities, rollback, lifecycle order, and live-snapshot shutdown.
- [x] Add the public size/versioned PluginHost ABI and structured error/lifecycle contracts.
- [x] Implement the bounded host registry, explicit platform loader, staged commit/rollback,
      catalog snapshot, lease accounting, and retryable quiesce/shutdown state transitions.
- [x] Export/install `TurboFlow::PluginHost` and compile the installed header as C and C++.
- [x] Document ownership, state, failure semantics, concurrency limits, and the later migration path.
- [x] Run focused repeat tests, adjacent Product/Protocol tests, Debug/ASan and Release suites,
      install-tree verification, CodeGraph affected analysis, and whitespace/fallback scans.
- [ ] Commit, open a PR closing #65, wait for required checks, merge, and update #63/#28/#2.

## Invariants

- A failed DLL load or registration changes none of the committed module/provider counts.
- Provider descriptors and callback contexts stay owned by their DLL; the host only copies the
  size/versioned descriptor values and never frees plugin memory.
- A catalog snapshot copies provider arrays and holds one lease on each represented module.
- No lifecycle callback, dynamic-library call, allocation, or catalog lookup occurs per message.
- The host is control-thread confined; snapshot consumers may only read their copied arrays.
- Shutdown never unloads a module until all leases are zero and every module has quiesced and shut
  down successfully. Errors remain explicit; there is no static-provider or legacy-loader fallback.
