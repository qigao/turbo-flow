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
