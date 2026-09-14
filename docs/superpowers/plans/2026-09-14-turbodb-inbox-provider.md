# TurboDB durable inbox provider implementation plan

> **Issue:** #118
> **Contract:** incompatible Inbox API v2 using the unified provider vtable
> **Compatibility:** v2 only; no migration, conversion, shim, or provider fallback

## Goal

Add a durable TurboDB implementation of the unified intake inbox. A successful
admission means that the complete immutable record and capacity counters were
committed before Graph execution can begin. Every database failure is returned
and never selects another provider.

## Architecture and state ownership

- TurboDB is the sole fact source for record phase/bytes, owner generation,
  IDs, bounds, current counters and lifecycle counters.
- The provider owns a finite ORM connection pool. Slot bookkeeping uses a short
  mutex; database I/O never runs while that mutex is held.
- Claimed bytes are copied into a bounded lease cache only to preserve the
  borrowed claim view. The cache cannot advance persisted state.
- Stable `source_id + admission_id` identifies admission. Replaying identical
  content returns the existing record ID, including after close and terminal
  settlement; conflicting content is EPROTO.
- Complete/discard produces a bounded terminal tombstone. Live plus tombstone
  rows consume record and byte quotas until explicit forget; durable destroy
  never erases this history.
- Every mutation requests SERIALIZABLE isolation, checks generation with
  affected rows exactly one, and commits record/meta changes together. On the
  supported file-backed SQLite path, any COMMIT error remains a failed
  operation and the still-active transaction is rolled back during destruction;
  the provider does not infer success or perform automatic reconciliation.
- Normal open requires CLOSED. Explicit coordinator-authorized takeover checks
  `expected_generation`, advances it, and marks stale CLAIMED rows
  FAILED/OWNER_LOST_UNKNOWN. Scan plus explicit retry/discard is required;
  takeover never automatically requeues business work. A stale provider's next
  complete/fail returns ECANCELED and invalidates that process-local claim view.

## Schema contract

The caller provisions one exact namespaced v2 metadata row and v2 record table.
Create validates but never runs DDL, migrates, repairs or deletes data. Names
use a validated identifier prefix (`[A-Za-z_][A-Za-z0-9_]*`) and fixed suffixes.

Metadata includes singleton/schema magic/version, generation/owner state,
persisted limits, IDs, live/history partition and byte counts, and cumulative
counters. Records include the complete v2 envelope, stable admission identity,
flattened content descriptor, blobs, phase, claim generation/token,
failure status/kind, and terminal kind. `scan_failed` and `scan_history` expose
ordered read-only recovery pages without advancing state.
External uint64 values use canonical big-endian blobs. Preflight checks the
single metadata row, exact version/limits, partitions, aggregates and current
envelope; inconsistency is EPROTO, never repair.

The first implementation supports only proven file-backed SQLite. Other ORM
drivers and SQLite `:memory:` return ENOTSUP until separate backend isolation,
commit-outcome and concurrency tests exist.

## Tasks

1. Replace the short-lived Inbox v1 layout with v2 admission identity,
   generation snapshot and failed-record scan; update memory behavior/tests,
   with no v1 compatibility shim.
2. Add the exact-version TurboDB config/default/create API and document
   ownership, errors, schema, supported backend and explicit takeover.
3. Add RED tests for schema/backend rejection, idempotent durable round trip,
   capacity 0/1/N/N+1, terminal replay/forget, fail/scan/retry/discard,
   close/drain, crash takeover,
   stale generation and database failure without memory fallback.
4. Implement pool, preflight, transactional state transitions, explicit SQLite
   commit-error rollback semantics, and a bounded lease cache.
5. Update architecture and installed C/C++ consumer coverage.
6. Verify focused Release, Debug/ASan, adjacent regressions, full Release,
   installed consumer, exports/dependencies and CodeGraph sync.

## Verification

```powershell
cmake --preset win-release-user
cmake --build --preset win-release-user --target test_turbodb_inbox test_flow_inbox test_turbodb_outbox_source
ctest --preset win-release-user -R "^(test_turbodb_inbox|test_flow_inbox|test_turbodb_outbox_source)$" --output-on-failure
cmake --preset win-dev-user
cmake --build --preset win-dev-user --target test_turbodb_inbox test_flow_inbox
ctest --preset win-dev-user -R "^(test_turbodb_inbox|test_flow_inbox)$" --output-on-failure
```

The full Release and installed-consumer gates run only after focused tests are
green. Completion claims require fresh command output.
