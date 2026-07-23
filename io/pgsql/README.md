# TurboFlow PostgreSQL adapter

The PostgreSQL module provides the existing parameterized sink/query adapters, a bounded
durable-outbox source/sink pair, and a revision-checked record store. The outbox is a persistence
primitive: it does not add a second queue runtime, processor model, or protocol owner.

## Record-store contract

- `turbo_flow_record_store_t` remains the provider-neutral API; libpq types never cross it.
- One fixed table stores binary keys/values, revision, and namespace. A transaction advisory lock
  serializes each namespace across processes.
- Revision validation, capacity checks, and all PUT/DELETE mutations commit atomically. Conflicts
  return `TURBO_EBUSY`; capacity exhaustion returns `TURBO_ENOSPC`.
- A stable repeatable-read scan is bounded by `max_records`. No PostgreSQL failure falls back to
  process memory.

```yaml
channels:
  mqtt.sessions:
    kind: record_store
    config:
      backend: postgresql
      conninfo: "host=127.0.0.1 port=5432 dbname=turboflow user=turboflow password=change-me"
      namespace_name: flowie_smb_sessions
      max_key_size: 65538
      max_value_size: 16777216
      max_batch_size: 4096
      max_records: 200000
      create_table: true
```

The PostgreSQL storage backend opens and validates the database immediately when a registry owner
is created for `TURBO_FLOW_STORAGE_MODEL_RECORD`. This fail-fast behavior means product `--check`
requires PostgreSQL to be reachable when a PostgreSQL session store is selected. Obtain the
provider-neutral record facade with `turbo_flow_storage_backend_owner_service()` and release it
with `turbo_flow_storage_backend_owner_destroy()`.

## Outbox contract

- The fixed `turbo_flow_outbox` table is the durable fact source. `outbox_name` partitions rows.
- A sink serializes capacity checks per outbox with a transaction advisory lock and reports
  `DURABLE` only after `COMMIT` succeeds. Full capacity returns `TURBO_ENOSPC`.
- Payload, message type/flags, and an optional serializable protocol origin commit atomically.
  Process-local protocol routes are never stored.
- A source obtains a session advisory lock for one row, reconstructs the owned message through
  the configured TurboFlow source, and deletes the row only after graph success.
- Graph failure releases the lock without deleting the row and stops that source fail fast. A new
  source instance can recover the pending row.
- A process or connection failure releases the PostgreSQL session lock. A crash after graph side
  effects but before `DELETE` can redeliver, so delivery is explicitly at-least-once and downstream
  effects need an idempotency key or transactional deduplication.
- The adapter status document contains no `conninfo`, password, SQL text, outbox name, or payload.

The row lock is held across synchronous graph publication, but no database transaction is held
across the graph. This avoids a long-running transaction while retaining multi-source exclusion.
The locking behavior follows PostgreSQL's transaction/session advisory-lock contracts:
<https://www.postgresql.org/docs/current/explicit-locking.html#ADVISORY-LOCKS>.

## YAML

Human configuration is YAML. A named channel owns the shared PostgreSQL contract; source and sink
adapters contain only the channel reference and role. See [`examples/pgsql.yml`](examples/pgsql.yml).
Unknown fields, wrong types, unsupported backends, zero bounds, and cross-kind references fail
before any database connection is opened.

```yaml
channels:
  orders.outbox:
    kind: outbox
    config:
      backend: postgresql
      conninfo: "host=127.0.0.1 port=5432 dbname=flow user=flow password=change-me"
      outbox_name: orders
      capacity: 1024
      max_payload_size: 1048576
      poll_interval_ms: 50
      claim_scan_limit: 64
      create_table: true

adapters:
  pg.orders.sink:
    kind: pgsql_outbox
    config: { channel: orders.outbox, role: sink }
  pg.orders.source:
    kind: pgsql_outbox
    config: { channel: orders.outbox, role: source }
```

`create_table: false` validates the fixed table projection and fails startup if it is unavailable.
There is no memory fallback.

`create_table: true` also upgrades an older outbox table in place by adding bounded message metadata
and protocol-origin columns with `ALTER TABLE ... ADD COLUMN IF NOT EXISTS`. Existing payload rows
remain intact; rows written before origin support have no live route and are accepted only by
downstream graphs that do not require protocol-origin semantics.

## Verification

The regular suite validates ABI bounds, graph roles, credential-free status documents, strict YAML,
and the installed example without opening PostgreSQL. The opt-in live suite additionally verifies
COMMIT-backed `DURABLE`, capacity, failed-delivery retention, restart recovery, binary payloads, and
two concurrent sources:

```powershell
$env:TURBO_FLOW_PGSQL_TEST_CONNINFO = 'host=127.0.0.1 port=5432 dbname=postgres user=postgres password=postgres'
cmake --preset win-dev-user --fresh -DTURBO_FLOW_PGSQL_LIVE_TESTS=ON
cmake --build --preset win-dev-user --target test_turbo_flow_pgsql_live
ctest --preset win-dev-user -R test_turbo_flow_pgsql_live --output-on-failure
```

The supplied role must be able to create or alter the fixed table/index when `create_table` is
enabled and to `SELECT`, `INSERT`, and `DELETE` its rows.
