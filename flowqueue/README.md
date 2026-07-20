# FlowQueue

`TurboFlow::Queue` is the bounded, process-local queue boundary. Its pending
storage is a fixed-entry TurboUtils Disruptor in worker-pool mode, so multiple
sink producers and competing source/direct-claim consumers use the same MPMC
claim/publish/release protocol.

The pending-slot primitive is lock-free. Queue lifecycle, retry-heap mutation,
claim-token indexing, counters and blocking/timeout policy remain protected by
the Queue control mutex; FlowQueue does not claim that every public operation
is lock-free.

Redis Stream is the durable and cross-process queue boundary. It remains a
`TurboFlow::Redis` provider instead of being hidden behind the local Queue
object, because Redis owns the consumer group, PEL, entry IDs and restart
recovery state.

```text
process-local: producer -> FlowQueue Disruptor -> consumer
distributed:   producer -> Redis XADD -> XREADGROUP -> graph -> XACK
```

SQLite is not a FlowQueue YAML backend. The old direct SQLite queue constructor
is deprecated for source migration only. Existing SQLite blob and record stores
are separate persistence interfaces and are unaffected by this queue contract.

## Local Disruptor Queue

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
      max_active_claims: 64
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

`capacity` is the logical hard bound. The physical Disruptor capacity is the
smallest power of two greater than or equal to that value; the extra physical
slots are never exposed as additional logical capacity.

The pending slot contains an owning `turbo_flow_msg_t`. Enqueue clones or
retains the payload before publishing the slot. A successful enqueue transfers
that clone to FlowQueue. A failed enqueue leaves no queue-owned message.

The worker-pool protocol is:

```text
FREE -> PRODUCER_CLAIMED -> PUBLISHED -> WORKER_CLAIMED -> FREE
                                      -> direct claim -> ACK/DROP -> FREE
                                                      -> REQUEUE -> retry heap
```

Requeued items use a bounded minimum heap ordered by their original enqueue
sequence. They remain part of the same logical capacity budget. A successful
claim must reach exactly one of ACK, DROP or REQUEUE before its borrowed message
view becomes invalid.

```c
turbo_flow_queue_config_t config = {
    .resource_uid = "queue:orders",
    .owner_name = "orders-queue",
    .capacity = 1024,
    .max_payload_size = 1024 * 1024,
    .full_policy = TURBO_FLOW_QUEUE_FULL_FAIL,
};
turbo_flow_queue_t *queue = turbo_flow_queue_create(&config);
```

## Redis Stream Queue

Use Redis Stream when messages must survive process restart or cross process
boundaries. The sink performs `XADD`; the source uses `XREADGROUP` and executes
`XACK` only after graph publication succeeds.

```yaml
version: 1
fragments:
  connection:
    local-redis:
      host: 127.0.0.1
      port: 6379
      database: 0
      timeout_ms: 5000
adapters:
  redis.events.sink:
    kind: redis
    fragments: {connection: local-redis}
    config:
      pattern: stream
      role: sink
      stream: flow-events
      field: payload
      maxlen: 100000
  redis.events.source:
    kind: redis
    fragments: {connection: local-redis}
    config:
      pattern: stream
      role: source
      stream: flow-events
      field: payload
      group: workers
      consumer: worker-1
      group_start_id: "0"
      read_count: 64
      block_ms: 1000
      poll_interval_ms: 10
      create_group: true
```

`stream`, `group` and `consumer` identify the durable owner. The source first
replays its PEL and then reads `>` entries. `maxlen` bounds retained Stream
entries; it is not a producer-side admission guarantee, so Redis memory and
eviction policy must also be configured operationally.

For delayed replies, use `turbo_flow_redis_stream_owner_create_ex()` and settle
every token with `turbo_flow_redis_stream_owner_ack()`, `_requeue()` or
`_drop()`. An uncertain `XACK` result remains active and is reconciled against
the PEL before retrying.

## Capacity And Backpressure

- `FAIL` returns `TURBO_ENOSPC` without changing FlowQueue.
- `BLOCK` waits no longer than `enqueue_timeout_ms`; shutdown wakes the wait and
  returns `TURBO_ESHUTDOWN`.
- `DROP_OLDEST` removes only a pending item. In-flight items are never evicted.
- Payloads exceeding `max_payload_size` return `TURBO_EMSGSIZE`.
- Destroy returns `TURBO_EBUSY` while adapters or direct claims retain the
  queue.

FlowQueue is process-local and is drained or explicitly released before
destroy. Redis Stream owns restart recovery through its consumer group and PEL;
do not mirror the same durable queue state into another database.
