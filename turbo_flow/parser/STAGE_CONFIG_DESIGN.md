# Turbo Flow Stage And Config Design

This document combines the implemented stage/config contract with explicitly
marked proposals. The current implementation includes root `stage main`,
non-parameterized reusable stages and `use` expansion, the YAML v1 resolver,
immutable resolved JSON, fragments, profiles, channels, adapter views, and
process-level asynchronous ingress configuration. Parameterized stage
definitions, `with`, and `$parameter` substitution remain proposed and are not
accepted by the parser.

## Goals

- Keep `.flow` focused on stage topology and message semantics.
- Move deployment configuration, secrets, endpoints, SQL, TLS material, and
  adapter options into external config.
- Support reusable multi-level stage compositions that expand into the existing
  immutable stage and edge plan.
- Support large static pipelines, including hundreds or thousands of stages,
  without making the DSL a macro or scripting language.
- Provide a human-authored config format and a stable resolved machine format.

## Non-Goals

- Do not add inline adapter option blocks to `.flow`.
- Do not put passwords, bearer tokens, TLS keys, SQL, hostnames, or paths in
  `.flow`.
- Do not add unbounded loops, recursion, dynamic stage generation, or runtime
  topology mutation.
- Do not make FMQ, broker, codec, DataBind, database, or HTTP behavior grammar
  keywords.

## File Roles

```text
*.flow        stage topology, ports, instance composition, route/reject/retry/reorder
*.yml         human-authored profiles, reusable fragments, adapter configs, and bindings
*.resolved.json canonical resolved config after profile expansion and secret resolution
```

The public file-input path accepts YAML only. Resolved JSON is an immutable
tool/diagnostic view returned by the resolver and is not deserialized as a
second runtime config path. Typed C config structs remain host APIs, not file
readers. TOML and human-authored JSON are not accepted alternate inputs.

## DSL Shape

The parser shape is `stage main` as the root orchestration stage and non-`main`
`stage` blocks as reusable stages with ports. The older `graph`, `subgraph`,
and `flow` block spellings are not part of the grammar. The parser currently
supports non-parameterized `use <alias> = <stage>` expansion, including nested
`use` inside reusable stages. The remaining parameterized form, `with`, and
`$parameter` substitution are proposed:

```flow
stage order_pipeline(bind_adapter, store_adapter) {
  in input
  out stored
  out rejected

  step bind adapter $bind_adapter
  step store adapter $store_adapter

  input -> bind -> store -> stored
  reject invalid bind -> rejected
}

stage main {
  source orders adapter fmq.orders.in
  step dead adapter sqlite.dead

  use us = order_pipeline with orders_us
  use eu = order_pipeline with orders_eu

  orders -> us.input
  orders -> eu.input
  us.rejected -> dead
  eu.rejected -> dead
}
```

Concepts:

- `stage` is a reusable finite processing unit.
- `step` is an atomic processing node inside a composite stage.
- `in` and `out` are explicit stage ports.
- `use <alias> = <stage>` instantiates a stage inside another stage. This
  non-parameterized form is implemented by parse-time expansion.
- `with <profile>` binds stage parameters from external config. This is not
  implemented yet.
- `$name` is a stage parameter reference. It may substitute adapter binding
  names and other explicitly supported identifiers.
- Adapter binding names still resolve through the host adapter registry.

The top-level root uses `stage main`.

## Root Sources

By default, reusable stages should not declare `source`. Network and polling
sources are lifecycle owners and should live in the root stage:

```flow
stage main {
  source inbound adapter fmq.orders.in
  use pipe = order_pipeline with orders_us

  inbound -> pipe.input
}
```

If endpoint stages are required later, they should use an explicit opt-in
syntax and lifecycle validation. They should not be introduced as implicit
sources inside ordinary reusable stages.

## YAML Shape

Human-authored config separates stage profiles, reusable runtime fragments,
shared channels, and concrete adapter configs. A channel owns backend state;
source/sink adapters reference it rather than duplicating its configuration:

```yaml
version: 1
runtime:
  ingress:
    workers: 1
    capacity: 1024
profiles:
  orders_us:
    bind_adapter: databind.order
    store_adapter: sqlite.orders.us
  orders_eu:
    bind_adapter: databind.order
    store_adapter: pgsql.orders.eu

fragments:
  connection:
    local_tls:
      transport: tls
      host: 127.0.0.1
      port: 5555

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
  fmq.orders.in:
    kind: fmq
    fragments:
      connection: local_tls
    config:
      pattern: sub
      mode: connect
      topic: orders.
  databind.order:
    kind: databind
    config:
      schema_path: schemas/order.schema
      type_name: Order
      input_format: json
  sqlite.orders.us:
    kind: sqlite
    config:
      path: orders-us.db
      statement: insert into orders(payload) values (?)
  pgsql.orders.eu:
    kind: postgresql
    config:
      conninfo: ${PG_ORDERS_EU}
      statement: insert into orders(payload) values ($1)
```

Rules:

- `runtime.ingress` is process-level configuration for the Flow-owned bounded
  asynchronous source handoff. It is not profile-scoped and does not create a
  second graph ingress; omitted fields resolve to the public Flow defaults.
- `profiles` bind stage parameters only. They should not contain protocol
  options or secrets directly.
- A profile may select a named adapter through a declared stage parameter, but
  it may not overlay, merge, or replace fields inside that adapter's config.
- `adapters` are the single source of truth for concrete external behavior.
- `fragments` are named input templates in fixed categories such as
  `connection`, `timer`, `thread`, and `coro`. Resolution copies their fields
  into one concrete adapter entry; fragments are never runtime state.
- Secret values should use explicit secret references, usually environment
  variables or host-provided secret handles.
- Config parsing must be strict: unknown fields, wrong types, invalid enum
  values, missing required fields, and unresolved references fail before stage
  compilation.

### Profile-to-adapter decision

Profiles are references, not adapter configuration layers. Host resolution
substitutes stage parameters first; an adapter-valued parameter must resolve to
exactly one name in `adapters`. The selected concrete adapter config is then
validated once by its schema and copied into the owning adapter before runtime
start. There is no field merge order, field-presence override, or implicit
fallback between `profiles` and `adapters`.

Hosts that currently maintain deployment overlays must materialize each result
as a named concrete adapter entry before TurboFlow compilation. Diagnostics may
show the profile reference and selected adapter name, but must not synthesize a
second runtime config. Runtime endpoint changes remain explicit adapter-owner
commands with adapter-specific validation and rollback; they do not mutate the
profile or the host's resolved artifact.

## Resolved JSON Shape

Resolved JSON is the canonical tool-facing artifact after YAML parsing, profile
expansion, and secret-reference normalization:

```json
{
  "version": 1,
  "runtime": {
    "ingress": {
      "workers": 1,
      "capacity": 1024
    }
  },
  "profiles": {
    "orders_us": {
      "bind_adapter": "databind.order",
      "store_adapter": "sqlite.orders.us"
    }
  },
  "adapters": {
    "sqlite.orders.us": {
      "kind": "sqlite",
      "config": {
        "role": "sink",
        "path": "orders-us.db",
        "statement": "insert into orders(payload) values (?)"
      },
      "sources": {
        "role": "adapter.config",
        "path": "adapter.config",
        "statement": "adapter.config"
      }
    }
  }
}
```

Resolved JSON should not include raw secret values in diagnostics or logs.
Each resolved adapter has exactly `{kind, config, sources}`. `config` is the
final merged value and `sources` records the fragment or `adapter.config` that
provided each field. Fragments are absent from the resolved artifact. Before
module registration, the host calls
`turbo_flow_resolved_config_preflight_adapter_kinds()` with the adapter kinds
compiled into that binary; a disabled kind fails at its `kind` path without
projecting its fields. Runtime projection then uses typed read-only adapter
views. `profiles` remain provenance for tooling and are not independently
mutable runtime state.

## Expansion Model

Stage expansion produces unique internal names while preserving adapter binding
names:

```text
main.us.bind   uses adapter databind.order
main.us.store  uses adapter sqlite.orders.us
main.eu.bind   uses adapter databind.order
main.eu.store  uses adapter pgsql.orders.eu
```

The expanded stage topology is finite and feeds the existing compile path:

```text
parse .flow
parse config
resolve profiles
expand stage instances
substitute stage parameters
validate adapter schemas and config
compile immutable stage/edge plan
start adapters and runtime
```

## Validation Rules

Fail before runtime start when any of these occur:

- Unknown stage, profile, parameter, port, or adapter.
- Stage recursion, directly or indirectly.
- Profile recursion or unresolved profile reference.
- Duplicate stage instance names in one scope.
- Duplicate expanded stage or source names.
- Connection to a stage instance without a port.
- Wrong port direction, such as `out -> out`.
- Required stage input or output left unbound when the stage marks it required.
- Adapter role mismatch after expansion.
- Source declared in a reusable stage.
- Expanded stage topology exceeds configured structural limits.

Recommended default limits:

```text
max_stage_depth       32
max_stage_instances   4096
max_expanded_stages   65536
max_expanded_edges    262144
```

These limits are compile-time limits. Runtime sources may still process an
unbounded stream of messages while the topology remains finite.

## Broker And Custom Pattern Semantics

Broker-specific behavior should stay in adapter config and metadata. FMQ
primitive endpoints can be composed into broker shapes by preserving selected
metadata across stages:

```yaml
version: 1
adapters:
  fmq.orders.in:
    kind: fmq
    config:
      pattern: sub
      mode: connect
      transport: tcp
      host: 127.0.0.1
      port: 7001
      topic: orders.
  fmq.orders.out:
    kind: fmq
    config:
      pattern: pub
      mode: bind
      transport: tcp
      host: 0.0.0.0
      port: 7002
      topic_policy: inherit
```

The stage composes broker steps:

```flow
stage broker_ingress {
  in frames
  out routed
  out rejected

  step decode adapter fmq.frame.decode
  step route adapter databind.order.route
  step encode adapter fmq.frame.encode

  frames -> decode -> route -> encode -> routed
  reject unroutable route -> rejected
}

stage main {
  source inbound adapter fmq.orders.in
  stage outbound adapter fmq.orders.out

  use broker = broker_ingress

  inbound -> broker.frames
  broker.routed -> outbound
}
```

This avoids adding broker pattern keywords to the grammar while still allowing
custom patterns through typed adapter config and host-owned callbacks. Metadata
inheritance is only valid during synchronous dispatch of an FMQ-originated
message; if no FMQ metadata is available, the adapter fails rather than
silently publishing with an empty or unrelated route key.

Reusable proxy devices follow the same rule. `pubsub_proxy`, `queue_proxy`,
`router_dealer_proxy`, and future `xpub_xsub_proxy` are ordinary `stage`
templates expanded with `use`, not DSL keywords. Their concrete behavior comes
from adapter schemas and runtime adapter configs: FMQ owns message patterns and
routing metadata, queue owns buffering/balancing policy, codec/DataBind own
payload parsing and validation, and Observe receives only generic control
events.

## Large Pipelines

A 1000-step processor should be represented as nested static stages, not DSL
loops:

```text
main
  ingest
  normalize.001-099
  validate.100-199
  enrich.200-499
  route.500-599
  persist.600-799
  notify.800-899
  observe.900-999
```

Tooling may generate `.flow` or resolved JSON, but the compiler must see a
finite stage topology and validate it before start.

## Migration Plan

Implemented baseline:

1. Keep source/stage/step and root `stage main` topology syntax stable.
2. Resolve YAML v1 into one immutable canonical JSON snapshot around existing
   adapter schemas.
3. Expand non-parameterized reusable stages through `use` with structural limit
   and recursion diagnostics.

Remaining proposals:

1. Add parameterized stage definitions and `with` bindings.
2. Add `$parameter` substitution for the explicitly supported binding fields.
3. Extend generated examples and tooling for broker, codec/DataBind, and storage
   pipelines without adding a second runtime configuration fact source.

Each remaining proposal must preserve existing top-level source/stage/step
behavior.
