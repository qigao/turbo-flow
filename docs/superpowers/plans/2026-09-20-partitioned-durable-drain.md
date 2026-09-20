# Partitioned Durable-Buffer Drain Plan

Tracking: #128. Parent: #127. Related: #130, #131.

Base: master `3a1b9e689432c701ebcf482bbafe5424dd0d5521`.

## Goal

Remove the single-global-claim bottleneck without weakening the existing Inbox
ownership, explicit settlement, or durable recovery semantics.

The implementation reuses one existing `flow_inbox_driver_t` per live worker.
A worker still owns exactly one provider claim and one downstream Graph run at a
time. Parallelism is coordinated above those drivers; workers never mutate
provider state directly.

## Critical invariant

Multiple drivers cannot safely call the current global FIFO `claim()` blindly.

If the head record belongs to a partition that is already running, claiming it
would either run two records from one partition concurrently or consume bounded
`max_in_flight` capacity while waiting. The latter allows one slow partition to
block unrelated partitions.

Therefore the first implementation slice is a provider-neutral partition-aware
claim operation:

- global ordering: claim the oldest pending record;
- partition ordering: claim the oldest pending record whose partition key is not
  in the caller's bounded active-partition exclusion set;
- the provider remains the only component that changes PENDING -> CLAIMED and
  creates the unique claim token;
- an exclusion match is not a claim, retry, failure, or settlement and changes no
  provider counters.

The first supported persisted partition source is `source_id`, which already
exists in the exact durable envelope for both memory and TurboDB. Device/session
and stable custom keys will be added by persisting one canonical partition key at
admission; they must use the same claim primitive rather than a second scheduler.

## Slice A — partition-safe provider claim

Add an exact-version claim request to the Inbox ABI with:

- ordering: GLOBAL or PARTITION_SOURCE_ID;
- caller-owned bounded array of excluded partition keys;
- exact validation for null/empty/duplicate-invalid views;
- no provider callback into host code.

Memory provider scans pending records by record ID and selects the oldest eligible
record. TurboDB performs the same selection transactionally before assigning the
claim token. Both providers preserve current capacity and takeover behavior.

Conformance tests prove:

1. global mode is unchanged;
2. with source A active, A2 is skipped and B1 may be claimed;
3. after A is removed from the exclusion set, A2 becomes claimable before newer A3;
4. no eligible record returns ENOENT without mutating pending/in-flight counters;
5. malformed/unbounded request input fails closed.

## Slice B — bounded N-driver coordinator

Extend durable-buffer binding configuration with:

- `ordering = global | partition`;
- `workers = N`;
- `max_in_flight = M`;
- `batch_claim = K`;
- `partition_by = source_id` initially.

Constraints:

- all values are finite and non-zero;
- `workers <= max_in_flight`;
- `batch_claim <= max_in_flight`;
- global ordering forces one live claim regardless of larger configured worker
  counts;
- no internal queue can exceed `max_in_flight`.

Coordinator progress:

1. poll/settle every live driver;
2. release its active partition only after claim ownership is terminal;
3. build the bounded active-partition exclusion set;
4. request at most `min(batch_claim, free_workers, free_in_flight)` new claims;
5. each successful claim is immediately owned by one driver and starts a distinct
   downstream execution;
6. ENOENT means no currently eligible partition, not global backlog emptiness.

No hidden retry or reconcile is introduced.

## Slice C — observability

Add one bounded worker/partition snapshot containing at least:

- configured workers / max_in_flight / batch_claim / ordering;
- active workers;
- active partitions;
- in-flight owned claims;
- claims started/completed/failed;
- partition-blocked claim attempts;
- worker saturation count.

This snapshot plugs the #128-dependent portion of #131. Sink-completion metrics
remain a separate #131 item.

## Slice D — generalized stable partition key

Add one canonical pointer-free `partition_key` to the durable envelope and exact
TurboDB schema, then map:

- `source_id`;
- `device_id`;
- `session_id`;
- stable custom key

into that field at admission. Old/unknown persisted schema is rejected; there is
no migration or runtime fallback.

## Gates

- memory and TurboDB claim conformance;
- same-partition FIFO under concurrent workers;
- cross-partition overlap demonstrated with controlled async sinks;
- one slow partition does not block another partition;
- global ordering deterministic;
- cancellation/stale owner cannot produce two live claim tokens;
- bounded burst stress with invariant `active <= max_in_flight`;
- Release and Debug/ASan focused/full gates;
- installed consumer / ABI / export checks when the public Inbox ABI changes.
