# Immutable CMeta/CFlow-backed graph plan

Status: Accepted for issue #3

## Context

TurboFlow currently stores compiled nodes, CSR edges, data segments, and executor records as
independent mutable vectors on `turbo_flow_t`. Runtime dispatch locates an executor or worker-pool
segment by scanning those vectors, and pool resize mutates both the parsed stage and compiled
records. The resulting ownership boundary is unclear: the compiler output is neither one object nor
immutable, while a single message can repeat an O(V) executor lookup for each reachable stage.

TurboFlow operation callbacks also use the domain-specific `turbo_flow_msg_t` ABI. The installed
Salts CFlow callable universe does not admit that callback signature. Treating a message pointer as
an integer callable would violate CMeta type identity and would make CFlow validation meaningless.

## Decision

The graph compiler produces one `flow_compiled_plan_t` owned by the flow. It contains the stable
stage-indexed nodes, CSR edges, executor records, segment records, O(1) lookup tables, semantic stage
metadata, and lowering regions. Construction is transactional: all storage is built in a temporary
plan, verified, sealed, and then moved into the flow. Runtime code receives only const views of this
plan. Reset or destroy is the only operation that releases it.

CMeta identities use stable strings and semantic comparison. They never use descriptor addresses as
cross-translation-unit type IDs. Each executable stage records its input/output descriptor, mapped
`cmeta_effects`, and explicit barriers. Barriers include untyped callbacks, state, asynchronous or
bounded handoff, retry, settlement, windowing, dynamic routing, external I/O, ordering, and graph
relations. Stateless bounded emission maps to CFlow `FLAT_MAP`; keyed and window emission retain
their state/window barriers. Connected typed stages whose only remaining obstacle is the legacy
callable ABI form candidate CFlow lowering regions. Candidate regions are planning metadata in this
issue; they do not pretend that the existing message callback ABI is a CFlow typed callable. The
internal required-CFlow compile gate therefore fails transactionally with `SALTS_ENOTSUP` whenever
a region cannot be represented by admitted CFlow callables. It publishes no partial plan and never
continues through native execution. Issue #4 owns the public size/versioned Reactive run surface and
typed execution bridge.

Pool parallelism is mutable runtime state. A stage-indexed runtime override table is initialized from
the sealed plan and becomes the sole source for resize/rebuild. Pool resize no longer changes parsed
stages, executors, segment widths, semantic descriptors, or lowering decisions.

## Alternatives considered

1. Keep the four vectors and add two hash maps. This fixes average lookup cost but leaves split
   ownership, mutation after compile, hashing overhead, and no transactional plan boundary.
2. Embed an executor and segment directly in every node. This is O(1), but duplicates optional
   records and complicates public segment enumeration and future compiled instruction layouts.
3. Convert callbacks to fake scalar CFlow callables. This was rejected because it breaks type
   identity, callable ABI, ownership, and error semantics.
4. Replace runtime execution with CFlow immediately. This was rejected for #3 because WAIT/waker,
   cancellation, settlement, and independent run ownership are the scope of #4.

## Consequences

- Runtime executor and worker-segment lookup are deterministic O(1) array accesses.
- Single-message topology work remains O(V+E); it performs no nested linear plan scans.
- The compiled plan is immutable during concurrent publish. Per-message scratch and mutable pool
  capacity remain outside the plan.
- Existing enum values, DSL grammar, source locations, callback behavior, and public plan views stay
  unchanged.
- Plan memory adds bounded O(V) index and semantic arrays. No O(V^2) reachability cache is created.
- The existing public compile path remains native-compatible. The internal CFlow-required path
  reports `SALTS_ENOTSUP` until every requested stage has an admitted typed callable and no semantic
  barrier; it has no native fallback.
- Each ordinary message slice traverses the CSR plan in O(V+E). A bounded emitter output is a new
  message slice and therefore retains its existing independent completion/error semantics.

## Migration and rollback

The migration is internal: callers continue to use `turbo_flow_compile()`, `turbo_flow_start()`, and
the existing plan query APIs. Runtime files move from independent vectors to const plan accessors.
Tests compare the old stage/edge representation with the compiled CSR, routes, effects, and error
locations. A rollback reverts the internal plan carrier and lookup-table commits; no persisted data,
DSL, public enum, or deployment format requires migration.

## Verification

- Parser/compile positive and negative suites retain their error code, line, and column assertions.
- Plan tests verify CSR ordering, semantic identity equality across translation units, barriers,
  lookup tables, sealing, and resize immutability.
- Differential tests reconstruct the legacy topology view from parsed stages/edges and compare it
  with the sealed plan.
- Concurrent publish and pool resize suites verify that runtime state is isolated from plan state.
- Benchmarks cover last-stage executor lookup in a 512-stage sealed plan and end-to-end
  linear/diamond/emitter graphs; full CTest remains the final regression gate.
