# TurboFlow PostgreSQL adapter

The PostgreSQL module provides the existing parameterized sink/query adapters and a bounded
durable-outbox source/sink pair. The outbox is a persistence primitive: it does not add a second
queue runtime, processor model, or protocol owner.

## Outbox contract

- The fixed `turbo_flow_outbox` table is the durable fact source. `outbox_name` partitions rows.
- A sink serializes capacity checks per outbox with a transaction advisory lock and reports
  `DURABLE` only after `COMMIT` succeeds. Full capacity returns `TURBO_ENOSPC`.
- A source obtains a session advisory lock for one row, publishes an owned binary payload through
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

The supplied role must be able to create the fixed table/index when `create_table` is enabled and
to `SELECT`, `INSERT`, and `DELETE` its rows.
