# TurboFlow `.flow` Node Configuration Design

TurboFlow uses **`.flow` as its sole product/dataflow language**.

## Language ownership

```text
DataBind IDL/schema  = contract / validation / Service / Channel truth
TurboFlow *.flow     = product topology + node policy + provider/resource references
Praktor *.yml        = operational workflow/orchestration
TurboSCXML *.scxml   = explicit long-lived statechart semantics
```

TurboFlow must not introduce `product.yml`, YAML profiles/fragments, resolved-JSON
product snapshots, or a second product authoring grammar.

## Goals

- Keep topology and every ExecutionPlan-affecting node policy together in one
  reviewable `.flow` file.
- Keep DataBind/CMeta/Plugin semantic facts authoritative rather than copying
  them into the DSL.
- Keep deployment instance data and secrets outside `.flow` behind explicit
  resource references.
- Compile accepted configuration once into the immutable ExecutionPlan.
- Preserve finite reusable stage composition through `use`.

## Non-goals

- No YAML product/configuration layer.
- No per-node sidecar files by default.
- No plaintext credentials, TLS private keys, bearer tokens, deployment DSNs,
  or machine-specific paths in `.flow`.
- No duplicate type/function/schema/validation declarations already available
  from DataBind/CMeta/Plugin reflection.
- No runtime config dictionary/string lookup on the message hot path.
- No hidden provider/transport fallback.

## Node configuration ownership

Configuration belongs inline with a node when it changes the compiled
ExecutionPlan semantics.

TurboFlow-owned policy includes:
- provider/operation/resource binding;
- capacity and backpressure;
- retry;
- ordering and partitioning;
- durability and settlement;
- execution/data-plane placement;
- routing/topology.

Provider-specific, non-secret configuration may also appear in a node block,
but its meaning and validation contract are owned by the selected provider and
admitted through canonical DataBind/CMeta/Plugin metadata. TurboFlow Core must
not hard-code provider-specific field semantics.

## Target syntax

Existing one-line declarations remain valid:

```flow
stage normalize operation Vehicle.normalize
```

A block keeps policy beside the node and reuses the same option validation.
For an atomic `stage`, at least one option must precede `{` so the existing
bare `stage name { ... }` syntax remains unambiguously reserved for reusable
composite stages. `source` and `step` blocks do not have that ambiguity.



```flow
source telemetry adapter cnet.stream {
  resource vehicle_listener
  channel Vehicle.Telemetry
  capacity 4096
  backpressure block
}

stage normalize operation Vehicle.normalize

stage classify operation Vehicle.classify {
  exec thread workers 4
  retry attempts 3 delay 100
}

buffer inbox provider turbodb.inbox {
  resource telemetry_db
  capacity 100000
  partition device_id
}

sink output adapter flowmq.publisher {
  resource telemetry_bus
}

stage main {
  telemetry -> normalize -> classify -> inbox -> output
}
```

Exact accepted keywords are defined by the production grammar and land
incrementally. Unknown fields fail fast; they are not retained as arbitrary
maps.

## Do not duplicate semantic truth

Good:

```flow
stage normalize operation Vehicle.normalize
```

Bad:

```text
input_type = VehicleTelemetry
output_type = NormalizedTelemetry
effects = pure
nullable = false
required = true
pattern = ...
```

Those facts belong to DataBind/CMeta.

## Provider-specific typed configuration

```text
.flow node
  selected adapter/provider capability
  provider-specific source fields
              |
              v
Plugin capability resolution
              |
              v
canonical provider Config DataDesc / DataBind plan
              |
              v
compile/preflight validation + materialization
              |
              v
immutable provider binding in ExecutionPlan
```

Requirements:
- unknown/missing/wrong-type/out-of-range fields fail before side effects;
- the selected provider owns field semantics;
- reflected/generated metadata may drive diagnostics/editor completion but
  never becomes a second TurboFlow schema registry;
- runtime uses only the sealed provider binding and never reparses config.

### Bounded structured literals (#270 / #271)

Provider values additionally admit arrays and records on one physical line:

```flow
stage request adapter chttp.client {
  headers [{ name "x-trace", value "a" }, { name "x-trace", value "b" }]
}
```

Arrays preserve order and repeated values. Records use existing identifiers as
keys and commas between fields; arrays use commas between values. Empty arrays,
records and string values are admitted for schema validation. Existing scalar syntax and lexer
identifier rules remain unchanged. Unknown fields, duplicate nested keys,
presence, types and validation constraints remain DataBind MessagePlan policy.
Duplicate top-level provider fields still fail in the parser.
The current resource production already uses `adapter_name`, admitting adjacent
dotted and quoted binding names; this extension preserves that documented
behavior. An unquoted hyphenated identifier remains invalid. The older issue
wording about rejecting dotted resources does not describe the current grammar.

The parser stores a flat sequence of scalar/container events, with optional
field names. This is source state, not a runtime dictionary or a second schema.
A node permits at most 256 top-level fields, 1024 CSerde tokens including keys
and the implicit outer map, 32 container levels including that map, and 65536
owned text bytes including string terminators. Checked admission returns
`SALTS_ENOSPC` before provider lookup when any limit is exceeded; malformed
structure returns the existing parse error with source location. There is no
truncation, fallback, or deferred provider-side capacity check.

Parsing is exclusive and single-threaded. Parser events borrow source text only
until node materialization copies it into stage-owned `tstr` storage. Failed
copies release every partially owned name/value. Reusable `use` deep-copies the
complete event sequence. The reader emits the implicit outer map plus those
events in order, borrowing immutable stage text until reset/destroy; no reader
may outlive that stage. Reset and destruction release the flat sequence without
recursive cleanup. There are no asynchronous callbacks or queues on this path.

This additive syntax reuses the existing parser, CSTL storage and CSerde reader.
A provider-specific header parser or JSON/YAML bridge would duplicate semantic
authority and is excluded. Existing scalar documents need no migration; a
rollback requires removing structured fields before using an older binary.
Formal config tests cover token order, exact limits, malformed syntax, deep
copy/reset, and generated `list<Record>` MessagePlan admission and diagnostics.

## Deployment resources and secrets

`.flow` expresses stable product intent:

```flow
source telemetry adapter cnet.stream {
  resource vehicle_listener
}

buffer inbox provider turbodb.inbox {
  resource telemetry_db
}
```

Deployment resolves those references before publication:

```text
vehicle_listener -> endpoint / TLS material / secret reference / provider instance
telemetry_db      -> DSN / credentials / runtime instance
```

Rules:
- missing/unauthorized/incompatible resources fail preflight;
- no hidden PATH/environment/default-endpoint fallback substitutes for an
  explicit resource reference;
- secrets are not copied into graph diagnostics or durable metadata;
- changing a live provider resource creates a new generation rather than
  mutating a sealed plan behind its back.

## Reusable stages

The root remains `stage main`. Non-`main` stage blocks remain finite reusable
compositions instantiated with `use`.

Inheritance/override, when implemented, applies only to explicit product-policy
or provider-config fields with deterministic rules. It must not override
DataBind/CMeta semantic facts.

## Compile model

```text
parse .flow
  -> resolve DataBind contracts + CMeta functions + Plugin capabilities
  -> validate TurboFlow node policy
  -> validate provider-specific typed config
  -> resolve deployment resource references
  -> transactionally materialize providers
  -> compile CFlow regions + provider/durable boundaries
  -> seal immutable ExecutionPlan
```

Any failure before publication aborts the generation. There is no YAML/JSON,
provider, transport, or compatibility fallback.

## Tracking

- #236 — canonical authoring epic
- #237 — first-class `.flow` node configuration blocks
- #238 — typed provider-specific config admission
- #239 — deployment resource/secrets boundary
- #240 — remove stale YAML authoring documentation
- #241 — remove YAML/resolved-config runtime and `TurboFlow::Config`
