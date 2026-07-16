# TurboFlow Queue

`TurboFlow::Queue` provides bounded in-memory and durable SQLite queue
boundaries. A host-owned `turbo_flow_queue_t` is the single queue state and may
be shared by source and sink adapters registered in different flows.

The preferred product configuration is YAML. A named channel owns backend,
pattern, capacity, and persistence state; adapters only reference that channel
and select a role. See [`examples/queue.yml`](examples/queue.yml).

```yaml
version: 1
channels:
  orders:
    kind: queue
    config:
      backend: memory
      pattern: push_pull
      resource_uid: queue:orders
      owner_name: orders-queue
      capacity: 1024
      max_payload_size: 1048576
      full_policy: fail
adapters:
  queue.orders.sink:
    kind: queue
    config: {channel: orders, role: sink}
  queue.orders.source:
    kind: queue
    config: {channel: orders, role: source}
```

After `turbo_flow_config_resolve_yaml()`, create the shared object with
`turbo_flow_queue_create_resolved()` and register each binding with
`turbo_flow_queue_register_resolved_adapter()`. Queue currently realizes only
`push_pull`; other patterns fail with `TURBO_ENOTSUP` instead of being inferred
from adapter placement.

For a live protocol `accepted` policy, the in-memory sink enqueue is also an
exact admission settlement boundary. The sink first commits its owned clone,
then routes a one-shot settlement command to the registered protocol owner.
The queued clone keeps the generation-fenced process-local route for downstream
delivery but does not carry the consumed settlement envelope. If command routing
fails after enqueue, Queue does not roll back the accepted record; the publisher
sees the failure and the protocol peer receives no promised ACK, so redelivery is
explicitly at-least-once. A SQLite sink accepts a live route only with a
matching `durable` envelope: it strips both sidecars from the persistent clone,
commits the payload transaction, and only then routes the one-shot command.
Unqualified live routes remain rejected. Queue recreation tests prove payload
replay without serializing a stale session capability.

```c
turbo_flow_queue_config_t config = {
    .resource_uid = "queue:orders",
    .owner_name = "orders-queue",
    .capacity = 1024,
    .max_payload_size = 1024 * 1024,
    .full_policy = TURBO_FLOW_QUEUE_FULL_FAIL,
};
turbo_flow_queue_t *queue = turbo_flow_queue_create(&config);

turbo_flow_queue_register_sink_adapter(producer, "queue.orders.out", queue);
turbo_flow_queue_register_source_adapter(consumer, "queue.orders.in", queue);
```

For a durable queue, use the same bounded behavior inside the SQLite config:

```c
turbo_flow_sqlite_queue_config_t config = {
    .queue = {
        .resource_uid = "queue:orders",
        .owner_name = "orders-queue",
        .capacity = 1024,
        .max_payload_size = 1024 * 1024,
        .full_policy = TURBO_FLOW_QUEUE_FULL_FAIL,
    },
    .database_path = "orders.sqlite3",
    .queue_name = "orders",
    .busy_timeout_ms = 1000,
    .max_state_size = 16 * 1024 * 1024,
};
turbo_flow_queue_t *queue = turbo_flow_sqlite_queue_create(&config);
```

SQLite also provides the atomic blob boundary used by durable owners. In YAML,
define a separate channel rather than pretending a queue delivery ACK is an
operation commit:

```yaml
channels:
  management-operations:
    kind: blob_store
    config:
      backend: sqlite
      database_path: flow-management.sqlite3
      key: tfmp:local-management
      busy_timeout_ms: 1000
      max_value_size: 8388608
```

After resolving YAML, call `turbo_flow_sqlite_blob_store_create_resolved()`.
It creates the provider and copies the configured key for the owner binding.
The SQLite transaction atomically replaces one bounded snapshot; malformed
configuration or database failure is returned without a memory fallback.

Large state owners must use the multi-record contract rather than rewriting one
whole snapshot per mutation:

```yaml
channels:
  mqtt.sessions:
    kind: record_store
    config:
      backend: sqlite
      database_path: flowie.sqlite3
      namespace_name: mqtt.sessions
      busy_timeout_ms: 1000
      max_key_size: 65535
      max_value_size: 1048576
      max_batch_size: 4096
      max_records: 100000
```

`turbo_flow_sqlite_record_store_create_resolved()` creates a namespaced store.
Every PUT/DELETE carries an expected revision; a conflict returns
`TURBO_EBUSY`, and capacity or any mutation failure leaves the complete batch
unchanged. Successful COMMIT is the durable ACK.

```flow
source input
stage enqueue adapter queue.orders.out
flow producer {
  input -> enqueue
}
```

```flow
source dequeue adapter queue.orders.in
stage process
flow consumer {
  dequeue -> process
}
```

Destroy both flows before calling `turbo_flow_queue_destroy()`. Destroy returns
`TURBO_EBUSY` while any registered adapter retains the queue. Register/destroy
operations are host lifecycle operations and must not run concurrently.

## Capacity Policy

Capacity and payload size are required, finite, and hard bounded. The in-memory
deque reserves its complete configured capacity during creation. The SQLite
backend enforces the same logical capacity inside its serialized transaction.

- `FAIL` returns `TURBO_ENOSPC` without changing the queue.
- `BLOCK` waits for at most `enqueue_timeout_ms`, then returns
  `TURBO_ETIMEDOUT`. Adapter stop wakes the wait and returns `TURBO_ESHUTDOWN`.
- `DROP_OLDEST` removes the oldest queued item and records `dropped_oldest`
  before accepting the new item. An in-flight item is never dropped; if it is
  the only occupied slot, enqueue returns `TURBO_ENOSPC`.

Drop behavior is therefore explicit host configuration and visible in the
snapshot. There is no silent default drop.

## Ownership And Acknowledgement

Queue exposes two different ACK counters through
`turbo_flow_queue_ack_snapshot()`:

- `accept_acks`: the memory enqueue completed, or the SQLite insert transaction
  committed. This says the backend accepted ownership; it does not say a
  consumer processed the message.
- `delivery_acks`: synchronous downstream graph publication succeeded and the
  backend finalized the claim. For SQLite, the delete transaction must also
  commit.

Failed delivery restoration is observable as `delivery_requeues` or
`delivery_requeue_failures`. These counters are per queue-object lifetime;
SQLite rows, not counters, are the durable fact source.

The sink clones accepted message state before returning. Buffer-backed and
owned payloads become queue-owned. Borrowed `transport_context`, borrowed
content descriptors, and schema-bound projections are rejected with
`TURBO_ENOTSUP`, because their lifetime cannot safely cross this asynchronous
boundary. A memory Queue preserves message-owned content descriptors and
process-local protocol routes such as an FMQ delayed ROUTER reply. SQLite
rejects both: its row format is durable payload state and must never serialize
or later replay a live session capability. Payloads exceeding the bound return
`TURBO_EMSGSIZE`.

One source may be active per queue. It reserves the front item as in-flight,
publishes synchronously, and acknowledges/removes it only when downstream
returns `TURBO_OK`. A downstream failure restores the item to the queue front,
increments `publish_failures`, and stops that source worker. Restarting a
source is an explicit retry decision; Queue does not spin or retry implicitly.
In-flight occupancy continues to consume capacity until acknowledgement.

Delayed-reply consumers use the same fact source through the non-blocking
`turbo_flow_queue_claim()` API. It returns a queue-local token and a borrowed,
immutable message. The item continues to consume capacity and a graph source
cannot start while any direct claim is active. The default bound remains one.
Memory and SQLite Queue both accept a larger finite bound before claiming:

```c
turbo_flow_queue_claim_owner_config_t owner_config =
    TURBO_FLOW_QUEUE_CLAIM_OWNER_CONFIG_INIT;
owner_config.max_active_claims = 64;
int rc = turbo_flow_queue_configure_claims(queue, &owner_config);
```

YAML exposes the same bound as `max_active_claims`; it must be positive and no
greater than queue capacity. SQLite additionally exposes `max_state_size`, the
hard bound for a durable coordinator snapshot stored beside its claims. Each
active token has an independent stable view.
Call
`turbo_flow_queue_claim_ack()` only after the remote worker reply is accepted;
call `turbo_flow_queue_claim_requeue()` when dispatch fails or the worker lease
expires. `turbo_flow_queue_claim_drop()` terminally removes failed at-most-once
work without incrementing `delivery_acks`. A coordinator can obtain the same
three operations as a borrowed `turbo_flow_claim_settler_t` through
`turbo_flow_queue_claim_settler()`. Stale/double settlement returns
`TURBO_EALREADY`.

```c
turbo_flow_queue_claim_t claim = TURBO_FLOW_QUEUE_CLAIM_INIT;
int rc = turbo_flow_queue_claim(queue, &claim);
if (rc == TURBO_OK) {
    rc = worker_completed
             ? turbo_flow_queue_claim_ack(queue, claim.token)
             : turbo_flow_queue_claim_requeue(queue, claim.token);
}
```

For memory, requeued claims are ordered by their original enqueue sequence, not
by the order in which concurrent workers fail. Settlement of one token does not
move or invalidate another token's borrowed view. SQLite applies the same token
isolation to independent durable rows. Reopening the database resets every
process-crashed in-flight row to pending. No process-local protocol route is
written to SQLite.

SQLite queue schema version 2 is recorded in `turbo_flow_queue_schema`, not the
database-global `PRAGMA user_version`. Opening a legacy queue without metadata
creates the bounded claim-state table and records version 2 without rewriting
message rows; a future/unsupported queue schema fails with `TURBO_EPROTO`.
The SQLite settler's `commit_state` performs the TFCS snapshot upsert and the
selected ACK/requeue/drop in one transaction. Exact retries compare both the
stored snapshot and row disposition, so a committed transaction whose response
was lost is reconciled without applying the claim twice.

The SQLite row is the durable fact source. Sink success means its transaction
committed. Source delivery first changes the oldest pending row to in-flight and
deletes it only after graph publication returns `TURBO_OK`; publication failure
restores it to pending. Opening the queue after a process restart also restores
stale in-flight rows. This is at-least-once delivery: a crash after a downstream
external side effect but before the delete commit can redeliver the message.
Consumers must therefore use an idempotency key or record their side effect and
message completion atomically when duplicate effects are unacceptable.

Stopping an empty source wakes and joins its worker. Stopping during downstream
publication waits for the synchronous flow dispatch to finish, then applies the
same acknowledge-or-requeue rule. No publication starts after source stop has
cleared its started state.
