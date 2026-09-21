# #159 Durable business payload → typed CMeta materialization design

Status: proposed design for #159. This document does not change the public ABI by itself.

Related: #73, #76, #116, #117, #118, #127.

## 1. Problem statement

TurboFlow now has three independently verified pieces:

1. real protocol/network intake that decodes wire data, assigns explicit schema identity and admits canonical payload bytes into the durable-buffer boundary;
2. transactional PluginHost Graph generations with bounded owners and module leases;
3. ABI3 typed operations, including the real RulesForge provider, which intentionally require an exact native CMeta typed projection.

The missing boundary is between (1) and (3).

A durable record may safely persist schema identity/version/encoding and canonical bytes, but it must not persist process pointers, DLL-owned descriptors, native pointer-bearing layouts or callback addresses. After a claim is restored into a Graph message, the runtime therefore needs a bounded, versioned way to reconstruct the exact native typed projection required by an operation.

Raw-byte shortcuts are rejected. RulesForge must not parse transport/protocol payloads itself, and Graph Core must not become a JSON/TBE/protocol decoder.

## 2. Architectural invariant

The full business path is:

```text
wire
  -> protocol Source / decoder
  -> explicit business schema identity + canonical encoded payload
  -> durable admission
  -> claim
  -> schema materialization
  -> exact message-owned CMeta projection
  -> typed operation
  -> explicit Sink
```

Two distinct transformations must remain separate:

- **semantic normalization**: protocol/business ownership decides which business schema a wire message represents;
- **native materialization**: a verified schema provider decodes that already-declared canonical payload into the exact native CMeta representation.

The materializer may reject a declared schema. It must never choose a different schema, infer one from transport metadata, or fall back to another codec.

## 3. State ownership

| State / fact | Owner |
| --- | --- |
| wire framing, protocol session and ACK | protocol/native Source owner |
| declared business schema identity/version/encoding | normalized message / durable record |
| durable admission, claim and settlement | durable-buffer provider |
| schema descriptor + decoder implementation | materializer provider DLL |
| native projection lifetime | Graph message / projection owner |
| business decision | typed operation result owner |
| destination/send terminal | Sink owner |

No second registry or hidden state machine is introduced.

## 4. Candidate designs

### A. RulesForge-specific payload decoder

Rejected.

It couples a policy engine to protocol/storage encoding, creates an engine-specific fallback path and makes the first successful integration impossible to generalize to TurboScript or another typed operation.

### B. Graph Core reflection decoder

Rejected.

Graph Core would need to understand every payload encoding and schema source. That makes Core a parser/runtime registry and crosses the current PluginHost boundary.

### C. Versioned schema materializer capability

Selected.

A plugin registers an immutable descriptor binding:

- exact schema identity;
- schema version;
- canonical encoding;
- exact CMeta descriptor;
- maximum encoded bytes;
- exact/bounded native object bytes;
- threading/ownership profile;
- decode/materialize callback;
- projection clone/destroy contract where required.

Generation assembly resolves this descriptor once and compiles its vtable into a materialization stage. The data path performs no symbol, plugin, schema-name or registry lookup.

## 5. Proposed capability shape

The implementation phase may adjust names after header review, but the semantic contract is fixed.

```c
typedef struct turbo_flow_plugin_materializer_v1_s {
  size_t size;
  uint32_t abi_major;
  uint32_t abi_minor;

  const char *schema_id;
  uint32_t schema_version;
  uint32_t encoding;

  const cmeta_data_desc *data;

  size_t max_encoded_bytes;
  size_t native_bytes;

  uint32_t threading;
  uint32_t ownership;

  void *ctx;
  turbo_flow_plugin_materialize_fn materialize;
  turbo_flow_plugin_materialize_destroy_fn destroy;
} turbo_flow_plugin_materializer_v1_t;
```

Important constraints:

- descriptor and referenced CMeta metadata remain immutable for the module generation;
- the host copies only the versioned wrapper, not the DLL-owned descriptor tree;
- materialized output is either copied into message-owned bounded storage or returned under an explicit same-module destroy contract;
- allocation and release remain on the same side of the DLL boundary;
- no C++ STL, exception, FILE, socket handle or unversioned object crosses the boundary.

## 6. Configuration

Do not add implicit materialization based on an operation name.

A future explicit binding should identify the expected business schema/materializer in resolved configuration. Exact syntax is an implementation decision and is a public configuration change, so it requires a separately reviewed implementation slice.

Generation preflight must be able to prove:

```text
durable record declared schema
       ==
configured materializer schema
       ==
operation input schema
```

before Flow start where the configured topology fixes all three identities.

For runtime records, each claimed record must still match the compiled schema/version/encoding before decode.

## 7. Generation lifecycle

### Preflight

Before external business side effects:

1. resolve configured materializer provider from immutable catalog snapshot;
2. reject duplicates and ambiguous schema registrations;
3. verify exact ABI size/major/minor;
4. verify schema/version/encoding;
5. validate CMeta descriptor and native size bounds;
6. validate configured encoded/native capacities;
7. validate threading/ownership profile;
8. validate operation input schema equality;
9. reserve bounded generation metadata.

Failure leaves no materializer owner or Graph provider visible.

### Materialize

For one claimed message:

1. read immutable schema/version/encoding + canonical byte view;
2. compare against the already-compiled descriptor;
3. reject oversize input before callback;
4. create one bounded output claim;
5. invoke the compiled materializer callback;
6. validate callback status/output contract;
7. bind the exact CMeta projection to the message;
8. continue to the typed operation.

No registry lookup occurs in steps 1–8.

### Failure

- malformed/unsupported input does not call the typed operation;
- partial native output is destroyed exactly once;
- the durable claim remains owned by the durable-buffer state machine;
- retry/reconcile/discard policy remains explicit and external to the materializer;
- no memory fallback, alternate schema or alternate engine is selected.

## 8. Projection lifetime

The native projection may outlive the materialization callback but may not outlive its declared owner contract.

The implementation must choose one of the existing supported ownership forms:

1. host/message-owned fixed-size copy for pointer-free native structs; or
2. module-owned projection with explicit clone/destroy owner and retained module lease.

The first RulesForge `Applicant` proof should use the pointer-free fixed-size path. This validates the architecture without prematurely adding an arena or arbitrary graph allocator ABI.

## 9. First real acceptance slice

Do not use CNet/CHTTP raw bytes directly as RulesForge input.

Use the already existing real protocol acceptance infrastructure:

```text
JT/T808/TCP
  -> protocol decode
  -> durable buffer
  -> common business schema
  -> materializer
  -> rulesforge.apply
  -> configured CNet datagram Sink

CoAP/UDP
  -> protocol decode
  -> durable buffer
  -> same common business schema
  -> same materializer
  -> same rulesforge.apply
  -> same Sink type / explicit destination
```

The two wire protocols must produce semantically equivalent business objects for the test cases. Only the Source/protocol side differs.

This is the direct proof required by the transport-independent business graph design: source identity is not business identity.

## 10. RulesForge integration constraints

RulesForge remains unchanged as an engine boundary:

- it receives `rulesforge.Applicant.data` only as an exact CMeta typed projection;
- it does not decode protocol payloads;
- it does not inspect Source kind or transport;
- the operation binding remains resolved during generation assembly;
- its output continues through the existing ABI3 result-domain ownership model.

If the normalized business schema is not exactly the RulesForge input schema, use a separately declared typed business transform. Do not alias schema identities merely to make the test pass.

## 11. Negative tests

At minimum:

- missing materializer provider;
- duplicate schema registrations;
- ABI major/minor/size mismatch;
- unknown schema;
- wrong schema version;
- wrong encoding;
- operation input schema mismatch;
- encoded input at 0/1/N/N+1 capacity boundaries;
- callback returns failure before output;
- callback returns malformed/oversize output;
- stale generation;
- unload attempt while a projection/message/run lease is live;
- decode failure proves operation invocation count remains zero;
- durable-buffer failure proves materializer invocation count remains zero.

## 12. CI gates

Implementation CI should include:

- C and C++ public header compile;
- focused unit tests with bad materializer fixtures;
- Debug + ASan/UBSan;
- Release;
- installed-tree provider loading;
- exact export/dependency checks;
- real JT/T808 and CoAP network acceptance;
- real installed RulesForge provider;
- clean-tree/exact-head identity;
- full CTest before merge.

A fixture may test generic ABI failure cases, but the final architecture acceptance must use the installed real RulesForge DLL and real network protocol paths.

## 13. Performance boundary

No performance claim is made by this design.

Required measurements after correctness:

- materialization ns/message;
- allocations/message;
- bytes copied/message;
- operation end-to-end latency with and without materialization;
- P50/P95/P99 under bounded backlog.

The hot path must not perform catalog/symbol/string-name lookup. A fixed pointer-free projection may require one bounded decode/copy; zero-copy is not claimed.

## 14. Compatibility and migration

This design does not change Inbox v2 storage data.

A later implementation may add a public PluginHost ABI category and explicit resolved-config syntax. Those are public interface/configuration changes and must be reviewed as their own implementation slice.

No old stored record is reinterpreted under a new schema. Existing records with unsupported schema/version/encoding fail explicitly.

Rollback is deployment of the previous complete verified package/config. Runtime fallback to raw bytes or a previous parser is forbidden.

## 15. Implementation order

1. freeze public materializer descriptor semantics and configuration shape;
2. add ABI/header + bad-fixture tests;
3. add PluginHost transactional registration/catalog snapshot support;
4. add generation preflight and compiled binding;
5. add pointer-free message-owned materialization stage;
6. add durable-claim integration and failure ownership tests;
7. add RulesForge Applicant real-provider integration;
8. extend JT/T808 and CoAP installed-tree acceptance to the same typed business Graph;
9. run Debug/ASan, Release, full CTest and hot-path lookup assertions.

Only after step 8 is green may #159 be used as evidence toward the remaining #73/#117 acceptance.
