# Managed Source/Sink Boundary Contracts Implementation Plan

**Goal:** Add the provider-neutral, read-only managed Source/Sink contract tracked by GitHub issue #52 without creating a second resource registry.

**Architecture:** The existing resource provider remains the identity, generation, snapshot, document, and command fact source. An explicitly registered boundary descriptor advertises immutable role/capability/schema information; an owner callback supplies the synchronized runtime snapshot. Core enumeration validates both projections against stable resource metadata and never infers capabilities from legacy callbacks or resource kinds.

**Tech Stack:** C11, TurboFlow resource registry, CMeta-compatible size/version contracts, TinyTest, CMake Presets.

## Task 1: Public contract and failing tests

- [x] Add pointer-free descriptor/snapshot types, role/capability/command flags, lifecycle states, and initialization macros.
- [x] Add an additive managed-provider ops contract and public count/descriptor/snapshot queries without changing the existing provider ABI.
- [x] Write fake-owner tests for explicit enumeration, all required metrics, and immutable descriptor behavior.
- [x] Prove RED before implementing core queries.

## Task 2: Registry validation and queries

- [x] Validate the descriptor at registration against resource metadata and reject malformed flags, schema version, identity drift, or missing paired callbacks.
- [x] Enumerate only providers that explicitly declare the managed-boundary contract.
- [x] Validate every live snapshot against descriptor and current metadata, including generation ordering and bounded queue depth.
- [x] Return owner errors unchanged and protocol violations as `SALTS_EPROTO`; do not infer or fall back to legacy snapshots.

## Task 3: Package consumers and verification

- [x] Exercise the installed C and C++ headers, registration symbol, and query API.
- [x] Run focused resource tests, adjacent domain/control tests, then full Debug/ASan and Release suites with `win-dev-user` and `win-release-user`.
- [x] Request independent review, resolve all findings, and verify the tree before updating #52/#28 and merging.
