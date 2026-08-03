# TurboFlow Redis

`TurboFlow::Redis` exposes Redis Streams and ordinary Redis data as separate
adapter contracts. Both use TurboNet's Redis client, which owns the copied
connection configuration and performs AUTH/SELECT during connection setup.

Human-authored configuration is YAML. The resolved adapter selects
`pattern: stream|data`; Stream also requires `role: source|sink`, while Data
requires `operation: set|get`. See [`examples/redis.yml`](examples/redis.yml)
and register a resolved binding with
`turbo_flow_redis_register_resolved_adapter()`.

## Redis Streams

`turbo_flow_redis_register_stream_adapter()` selects its role from
`poll_interval_ms`:

- zero registers an XADD sink;
- non-zero registers an XREADGROUP source and requires `group` plus `consumer`.

TurboNet returns owned Stream results together with transport outcome and Redis
server-error classification; the adapter transfers that ownership into its
task result without a second deep copy. Redis errors are propagated and are not
reported as an empty queue. The source publishes each entry to the graph and
sends XACK only after that publication succeeds. A downstream failure stops the worker without
acknowledging the entry, leaving Redis consumer-group state as the fact source.
On every source start, the configured consumer first drains its own pending
entries with `XREADGROUP ... 0`; after that set is empty it switches to `>` for
new messages. Keep the consumer name stable when process-restart replay of that
consumer's pending entries is required. Reassigning abandoned entries owned by
a different consumer remains an explicit Redis operator/controller action.
Adapter stop interrupts a blocked XREADGROUP before joining the worker.

The two ACK boundaries are intentionally different: successful XADD is an
accept ACK from Redis, while XACK after successful graph publication is a
delivery ACK. XADD success must never be reported as downstream completion.
When an input message carries a matching protocol `durable` settlement
envelope, the Stream sink routes that settlement only after Redis returns a
successful XADD reply. Redis stores the payload field, never the process-local
route or settlement sidecar. If the command outcome is uncertain, no protocol
ACK is promised and peer redelivery remains at-least-once. The Redis live suite
also composes this boundary with a Flowie TCP endpoint and replays the stored
MQTT packet through a consumer group.

An embedded dispatcher that already owns its retry and durable-completion
policy can use `turbo_flow_redis_stream_publisher_create()` and pass
`turbo_flow_redis_stream_publisher_publish` as a type-erased append callback.
The publisher enforces `max_payload_size` and exact `MAXLEN`, serializes access,
and deliberately does not expose the Redis Stream ID. A successful call means
only that XADD replied successfully; the embedding dispatcher must retain its
own durable work until its authoritative completion condition is satisfied.

For asynchronous FMQ worker replies,
`turbo_flow_redis_stream_owner_create()` exposes the same Redis fact source as
an explicit `claim -> ack/requeue` contract with the compatibility bound of one.
Use `turbo_flow_redis_stream_owner_create_ex()` to configure a larger bounded
`max_active_claims`. Each claim reads one entry with XREADGROUP and keeps it in
the same consumer PEL. `ack` requires an XACK count of exactly one. `requeue`
deliberately does not XACK; the owner replays locally requeued entries by their
original read order. Across owner restart, a monotonic pending cursor recovers
multiple entries from that consumer's PEL before returning to `>`.

```c
turbo_flow_redis_stream_owner_t *owner = NULL;
turbo_flow_redis_stream_claim_owner_config_t owner_config =
    TURBO_FLOW_REDIS_STREAM_CLAIM_OWNER_CONFIG_INIT;
turbo_flow_redis_stream_claim_t claim = TURBO_FLOW_REDIS_STREAM_CLAIM_INIT;
owner_config.max_active_claims = 64;
int rc = turbo_flow_redis_stream_owner_create_ex(&stream, &owner_config, &owner);
if (rc == TURBO_OK) rc = turbo_flow_redis_stream_owner_claim(owner, &claim);
if (rc == TURBO_OK) {
    rc = worker_completed
             ? turbo_flow_redis_stream_owner_ack(owner, claim.token)
             : turbo_flow_redis_stream_owner_requeue(owner, claim.token);
}
turbo_flow_redis_stream_owner_destroy(owner);
```

Reaching the active bound returns `TURBO_EBUSY` without reading another entry.
Settling one token does not invalidate the borrowed entry ID or payload view of
another active token. Keep the consumer name stable across restart to recover
its PEL. Moving an
abandoned claim to a different consumer requires an explicit Redis ownership
transfer policy; this API does not silently steal another consumer's work.

`turbo_flow_redis_stream_owner_settler()` exports ACK, requeue, and drop as a
borrowed `turbo_flow_claim_settler_t` for the FMQ credit coordinator. ACK and
drop both use exact XACK storage settlement, but only ACK may become a worker
completion ACK at the coordinator boundary. An uncertain XACK result keeps the
claim active. The next ACK retry queries the exact ID in the group-wide PEL:
absence confirms the prior XACK, ownership by the same consumer permits an
XACK retry, and ownership by another consumer returns `TURBO_EBUSY` without
acknowledging transferred work. This closes ambiguity while the owner remains
alive; a durable settlement marker/outbox is still required to recover a
process crash between Redis commit and coordinator persistence, so
cross-process reliable credit YAML remains disabled.

```c
turbo_flow_redis_stream_config_t stream = {
    .host = "127.0.0.1",
    .port = 6379,
    .stream = "orders",
    .field = "payload",
    .group = "workers",
    .consumer = "worker-1",
    .poll_interval_ms = 10,
    .block_ms = 1000,
};
int rc = turbo_flow_redis_register_stream_adapter(flow, "redis.orders.in", &stream);
```

## Redis Data

`turbo_flow_redis_register_data_adapter()` provides a fixed-key, binary-safe
SET sink or GET transform. SET stores the input payload. GET replaces the input
payload only after a successful bulk-string reply; a missing key returns
`TURBO_ENOENT` and preserves the input. `max_value_size` bounds SET input and GET
output, with `TURBO_FLOW_REDIS_DEFAULT_MAX_VALUE_SIZE` used when it is zero.

```c
turbo_flow_redis_data_config_t data = {
    .host = "127.0.0.1",
    .port = 6379,
    .key = "orders:last",
    .operation = TURBO_FLOW_REDIS_DATA_SET,
    .max_value_size = 1024 * 1024,
};
int rc = turbo_flow_redis_register_data_adapter(flow, "redis.orders.last", &data);
```

Redis Data does not inherit Streams acknowledgement semantics: SET success is
an accept ACK represented by the Redis command result, while GET is a
synchronous read transform and has no queue delivery ACK. Applications
that need durable replay and consumer-group acknowledgement should use the
Streams adapter.

For a durable owner snapshot, use the separate atomic blob-store channel:

```yaml
channels:
  management-operations:
    kind: blob_store
    config:
      backend: redis
      host: 127.0.0.1
      port: 6379
      database: 0
      timeout_ms: 2000
      key: tfmp:local-management
      max_value_size: 8388608
```

`turbo_flow_redis_blob_store_create_resolved()` validates the immutable YAML
projection, creates the binary-safe SET/GET provider, and copies the configured
key for the owner binding. A connection, Redis command, size, or parse failure
is returned directly; the provider never substitutes an in-process store.

For per-owner records and atomic fan-out state, use a bounded record-store
channel:

```yaml
channels:
  mqtt.sessions:
    kind: record_store
    config:
      backend: redis
      host: 127.0.0.1
      port: 6379
      database: 0
      timeout_ms: 2000
      key: flowie:mqtt:sessions
      max_key_size: 65538
      max_value_size: 1048576
      max_batch_size: 4096
      max_records: 100000
```

The Redis storage backend's `open()` binds one Redis Hash for a record channel.
Register `turbo_flow_redis_storage_backend_api()` in the common storage registry,
then create an owner for `TURBO_FLOW_STORAGE_MODEL_RECORD`; the owner exposes the
provider-neutral record facade and is the only destroy path. A Lua transaction
validates all expected revisions and capacity before applying any HSET/HDEL, so
success is one durable batch ACK and conflict returns `TURBO_EBUSY`; there is no
memory fallback. The maximum key size is 65538 bytes, which accommodates Flowie's
binary retained-record prefix plus a maximum MQTT Topic Name.

The same Hash/Lua fact source can be used through the typed FlowStore API:

```c
turbo_flow_redis_record_store_config_t redis = {
    .host = "127.0.0.1",
    .port = 6379,
    .key = "flowie:mqtt:state",
    .max_records = 100000,
};
turbo_flow_store_limits_t limits = TURBO_FLOW_STORE_LIMITS_INIT;
turbo_flow_state_store_t *state = NULL;

limits.max_records = 100000;
limits.max_bytes = 256u * 1024u * 1024u;
limits.max_item_bytes = 1024u * 1024u;
/* Register the builtin backend, create an owner for STATE, and obtain `state`
 * with turbo_flow_storage_backend_owner_service(). */
int rc = turbo_flow_storage_backend_owner_create_registered(
    registry, "redis", &request, &owner, &error);
```

The STATE service is binary safe and uses `HGET` for point reads. One Lua CAS
updates the record Hash and capacity metadata Hash atomically, so initialized
put/remove operations are O(1) and multiple writers share one records/bytes
admission fact. The first mutation of a legacy namespace performs one bounded
`HGETALL` inside the Lua critical section to rebuild metadata. Redis errors are
returned directly and never fall back to memory.

FlowStore State, Index, Log, and Record configs accept an optional versioned
`connection`. A zeroed connection keeps the legacy standalone `host/port`
behavior. Cluster mode uses TurboNet Redis slot discovery and MOVED/ASK handling;
Sentinel mode discovers the named master and preserves uncertain-write results.
Cluster uses database 0 and every multi-key namespace must include a non-empty
hash tag:

```c
const char *seeds[] = {"redis-a", "redis-b", "redis-c"};
uint16_t ports[] = {6379, 6379, 6379};
turbo_flow_redis_record_store_config_t redis = {0};

redis.connection = (turbo_flow_redis_connection_config_t)
    TURBO_FLOW_REDIS_CONNECTION_CONFIG_INIT;
redis.connection.deployment = TURBO_FLOW_REDIS_DEPLOYMENT_CLUSTER;
redis.connection.seed_hosts = seeds;
redis.connection.seed_ports = ports;
redis.connection.seed_count = 3;
redis.key = "flowie:{cluster-a:session:42}:state";
redis.max_records = 100000;
```

For Sentinel, select `TURBO_FLOW_REDIS_DEPLOYMENT_SENTINEL`, provide the Sentinel
endpoints in `seed_hosts/seed_ports`, and set `service_name`. Sentinel is HA for
one Redis master dataset; it does not shard writes.

For membership and mapping queries, open the INDEX model through the same Redis
backend owner. It maps each binary index name to a native Redis Set. The namespace and index name
are hex encoded into Redis Cluster-compatible keys sharing one hash tag.
`SISMEMBER`, `SCARD`, `SMEMBERS`, and `SINTER` implement the typed query
contract. Add/remove use Lua to update the Set and namespace-wide
`max_records/max_bytes` counters atomically, so capacity remains one fact even
with multiple writers. Bitmap and Redis string/stream types are not used for
membership storage.

For ordered event payloads, open the LOG model through the same Redis backend
owner. It uses one Redis Stream as the payload fact source, one ZSet for bounded order metadata, and one
Hash for cursor/capacity metadata. Their keys share one Redis Cluster hash tag.
Append and trim validate the stored schema and immutable limits before one Lua
mutation; writers with different limits receive `TURBO_EBUSY`. A rejected
append does not consume a cursor. Retention and `TRIM_OLDEST` delete only an
oldest prefix, while reads copy payloads into caller-owned `mem_buffer_t`
records and return `TURBO_ERANGE` for a stale cursor.

```c
turbo_flow_redis_log_store_config_t redis = {
    .host = "127.0.0.1",
    .port = 6379,
    .key = "flowie:mqtt:events",
    .max_operation_records = 4096,
};
turbo_flow_store_limits_t limits = TURBO_FLOW_STORE_LIMITS_INIT;
turbo_flow_log_store_t *log = NULL;

limits.max_records = 4096;
limits.max_bytes = 64u * 1024u * 1024u;
limits.max_item_bytes = 1024u * 1024u;
limits.retention_ms = 24u * 60u * 60u * 1000u;
limits.full_policy = TURBO_FLOW_STORE_FULL_TRIM_OLDEST;
/* Open LOG through the registered Redis backend and obtain `log` from the owner. */
int rc = turbo_flow_storage_backend_owner_create_registered(
    registry, "redis", &request, &owner, &error);
```

`max_operation_records` bounds a read and every Lua prefix scan. It must be at
least `limits.max_records` and is capped at
`TURBO_FLOW_REDIS_LOG_MAX_OPERATION_RECORDS`. The Redis provider accepts
timestamps and cursors through `2^53 - 1`, because Lua performs exact integer
capacity and ordering checks. There is no memory fallback.

## Live verification

The real-server integration test is opt-in and targets `127.0.0.1:6379`:

```powershell
cmake --preset win-dev-user -DTURBO_FLOW_REDIS_LIVE_TESTS=ON
cmake --build --preset win-dev-user --target test_turbo_flow_redis_live
ctest --preset win-dev-user -R "test_turbo_flow_redis_live$" --output-on-failure
```

It covers the binary-safe revisioned StateStore, Redis Set IndexStore, bounded
Redis Stream LogStore cursor/trim/retention/reopen behavior, binary-safe Data
SET/GET, bounded multi-claim, stable borrowed views,
same-consumer multi-entry PEL restart replay, independent requeue/exact-XACK
settlement, and interruption of a blocked XREADGROUP.
