# ADR: TurboDB as the durable intake inbox owner

## Status

Accepted for the incompatible `turbo-flow.inbox.record` v2 implementation tracked by #118.

## Context

Transport Source DLLs must persist an accepted record before a shared business Graph processes it.
HTTP, WebSocket, socket, MQTT, and future transports must not own separate business queues or
database formats. The unified `turbo_flow_inbox_ops_v2_t` defines admission plus explicit
claim/complete/fail/retry/discard/forget transitions and read-only failed/history scans.

The durable implementation must use TurboDB ORM transactions, remain bounded under concurrency,
recover interrupted claims, and reject old formats. A database error must never run the Graph
directly or switch to the memory provider.

## Decision

### Provider and connection ownership

`turbo_flow_turbodb_inbox_create()` opens and owns a finite connection pool from the supplied ORM
configuration. The configuration and its string views are borrowed only during create. Connection
slots are checked out under a short mutex; every ORM operation runs after that mutex is released.
Pool exhaustion returns `SALTS_EBUSY`.

The provider owns the persisted records and a finite in-process claim lease cache. A lease contains
the owned copy that backs one immutable `turbo_flow_inbox_claim_t` view. It cannot independently
advance durable state. Callers must exclude provider destruction from all concurrent callbacks.

### Persisted schema

For namespace `orders`, the caller must provision these exact tables:

- `orders_inbox_meta_v2`
- `orders_inbox_records_v2`

The namespace must match `[A-Za-z_][A-Za-z0-9_]*` and is capped by
`TURBO_FLOW_TURBODB_INBOX_NAMESPACE_MAX`. It can therefore be used as an ORM identifier prefix;
record values remain bound parameters.

The metadata table has exactly one initialized row with these columns:

```sql
singleton_id integer primary key not null,
schema_magic text not null,
schema_version integer not null,
generation bigint not null,
owner_state integer not null,
next_record_id bigint not null,
next_claim_token bigint not null,
max_records bigint not null,
max_total_bytes bigint not null,
max_record_bytes bigint not null,
max_claims bigint not null,
records bigint not null,
history_records bigint not null,
pending_records bigint not null,
failed_records bigint not null,
in_flight_claims bigint not null,
retained_bytes bigint not null,
admitted bigint not null,
completed bigint not null,
failed bigint not null,
retried bigint not null,
discarded bigint not null
```

`singleton_id` is one, `schema_magic` is `turbo-flow.turbodb.inbox`, and `schema_version` is two.
The initial owner state is CLOSED, generation is zero, IDs start at one, current and cumulative
counters start at zero, and persisted limits exactly match runtime configuration. Counters and IDs
are non-negative signed 64-bit database values; exhaustion at `INT64_MAX` fails fast.

The record table has these columns:

```sql
record_id bigint primary key not null,
phase integer not null,
claim_generation bigint not null,
claim_token bigint not null,
failure_status integer not null,
failure_kind integer not null,
terminal_kind integer not null,
envelope_schema text not null,
envelope_schema_version integer not null,
source_id bytea not null,
admission_id bytea not null,
source_sequence_be bytea not null,
timestamp_ns_be bytea not null,
message_type bigint not null,
message_flags bigint not null,
content_domain integer not null,
content_profile integer not null,
content_encoding integer not null,
content_flags bigint not null,
content_schema_version bigint not null,
content_media_type text not null,
content_schema_name text not null,
content_type_name text not null,
content_identity text not null,
correlation bytea not null,
payload bytea not null,
retained_bytes bigint not null
```

Phase values are `0=PENDING`, `1=CLAIMED`, `2=FAILED`, and `3=TOMBSTONE`. `terminal_kind` is zero
for live rows, one for completed tombstones, and two for discarded tombstones. The table requires a
unique index on `(source_id, admission_id)` and an index on `(phase, record_id)`. External
`uint64_t` values use canonical eight-byte big-endian blobs so the relational signed integer range
cannot truncate them.

Before publishing a provider, create validates the metadata version, single-row cardinality,
persisted limits, clean CLOSED state, partition/cumulative counter invariants, retained-byte
aggregate, canonical SQLite storage classes, record domains, current envelope version, column
order, and indexes. It never issues DDL or repairs inconsistent data.

### Transaction and state boundaries

- `admit` first looks up stable `(source_id, admission_id)`. Identical content returns the existing
  receipt, including after close or terminal settlement; differing content returns `SALTS_EPROTO`.
  A new identity checks lifecycle and hard bounds, inserts the complete record, and updates metadata
  in the same transaction. The receipt is published only after commit succeeds.
- `claim` reserves one bounded local lease, selects the oldest PENDING record, validates and copies
  it, changes it to CLAIMED with the current generation and a new token, and updates metadata in one
  transaction. The immutable claim view is published only after commit succeeds.
- `complete` validates generation and token, changes CLAIMED to a completed TOMBSTONE, and updates
  the live/history/in-flight/completed counters in one transaction.
- `fail` validates generation and token, changes CLAIMED to FAILED, and updates the partition and
  failure counters in one transaction.
- `retry` changes only FAILED to PENDING. Retry is never automatic.
- `discard` changes only FAILED to a discarded TOMBSTONE.
- `forget` deletes only a TOMBSTONE. This explicit acknowledgement releases record and byte quota
  and allows the same identity to become a new admission while the namespace is accepting.
- `scan_failed` returns a read-only, record-ID-ordered page of FAILED entries and their failure kind.
- `scan_history` returns a read-only, record-ID-ordered page of TOMBSTONE entries and distinguishes
  completed from discarded outcomes. Neither scan advances state.
- `close` atomically stops new identities. Already accepted PENDING records remain claimable for
  drain, and an exact retained-identity replay still resolves to its original receipt.

Every mutation requests `ORM_ISOLATION_SERIALIZABLE`, checks the captured generation, and requires
the expected affected-row count. A failure before commit is rolled back. For the only supported
backend, file-backed SQLite, a COMMIT error remains an operation failure: no receipt or transition
success is published and no reconciliation converts it to success. SQLite documents that
`SQLITE_BUSY` leaves the transaction active, after which TurboDB transaction destruction rolls it
back; an SQLite error that already caused automatic rollback likewise cannot be treated as a
successful commit. See the
[SQLite transaction rules](https://www.sqlite.org/lang_transaction.html) and
[`sqlite3_get_autocommit()` contract](https://www.sqlite.org/c3ref/get_autocommit.html).
The provider rejects journal modes OFF/MEMORY and synchronization weaker than FULL; SQLite
documents MEMORY-journal crash recovery as potentially corrupting the database in its
[journal-mode contract](https://www.sqlite.org/pragma.html#pragma_journal_mode). It uses one main
database and TurboDB's SQLite backend does not install a custom WAL hook; a future change to those
constraints requires new commit-outcome evidence. The provider performs no automatic database
retry, business retry, or provider fallback.

`records` counts PENDING, CLAIMED, and FAILED rows; `history_records` counts TOMBSTONE rows. Their
sum is bounded by `max_records`. `retained_bytes` includes both classes, so terminal idempotency
history is finite and cannot evade quota.

### Crash recovery and fencing

Normal create succeeds only when owner state is CLOSED; ACTIVE returns `SALTS_EBUSY`. Crash
recovery is an explicit `OPEN_TAKEOVER` operation carrying the exact generation authorized by an
upper coordinator. It advances generation and changes old CLAIMED rows to FAILED with
`TURBO_FLOW_INBOX_FAILURE_OWNER_LOST_UNKNOWN` in one transaction. It never automatically requeues
them. `turbo_flow_inbox_scan_failed()` exposes those IDs for an explicit retry/discard decision.

After takeover, an old provider cannot settle its durable row. Its next complete/fail observes the
generation fence, returns `SALTS_ECANCELED`, invalidates the caller's claim view, and releases the
old provider's local lease. The stale provider cannot close or overwrite the new owner's durable
state; once all of its local leases are invalidated, it can destroy only its own process-local
resources. The new owner remains the sole durable fact owner.

### Alternatives rejected

- One shared ORM connection protected across I/O violates the repository lock boundary and
  serializes unrelated Source admissions.
- Opening one connection per operation makes connection setup part of the hot path and removes a
  finite, observable concurrency budget.
- Auto-creating or migrating tables silently changes deployment data and conflicts with the
  explicit no-compatibility/no-fallback requirement.
- Persisting DLL pointers, sessions, or claim views stores process-local values that cannot survive
  restart or safe plugin unload.
- End-to-end exactly-once is not claimed because database admission, Graph effects, Sink delivery,
  and transport ACK do not share one transaction.

## Consequences and verification

The first implementation supports only TurboDB file-backed SQLite with a durable rollback/WAL
journal and FULL-or-stronger synchronization. Other drivers, SQLite `:memory:`, journal OFF/MEMORY,
and weaker synchronization fail with `SALTS_ENOTSUP`; adding a backend requires its own isolation,
commit-outcome, and concurrency evidence.

The finite connection pool and metadata row add bounded memory and one transactional metadata
update per state transition. Claim and scan selection use the required `(phase, record_id)` index;
lease-cache lookup is `O(max_claims)` and bounded by configuration. Multi-process opening uses an
upper-layer-authorized generation fence rather than implicit leader election; Raft coordination can
select the active host above this provider when required.

Tests cover exact schema rejection, capacity edges, transaction failure, stable admission replay,
terminal history scans, explicit forget, crash takeover, stale-lease cancellation, claim lifetime,
close/drain, installed ABI, and Debug/ASan plus Release configurations.
