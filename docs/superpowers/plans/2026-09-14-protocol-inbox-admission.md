# Protocol Inbox Admission Refactor Plan

> **Issue:** #124, parent #117/#118
>
> **Compatibility:** intentionally incompatible; no old C ABI, target, component,
> symbol, layout, runtime branch or CMake fallback is retained

## Goal

Make protocol ingress a Source-only pipeline:

```text
CNet/CHTTP/Gateway receive
  -> protocol frame/decode runtime
  -> exact protocol Inbox adapter
  -> configured memory/TurboDB Inbox
  -> turbo_flow_inbox_source claim/request/poll
  -> shared CFlow Graph / RulesForge / TurboScript
  -> independently configured Sink
```

The Source boundary is complete when the configured Inbox owns an immutable
record. It never waits for Graph execution, business state, reply generation or
socket delivery. Every send, including a protocol reply, belongs to a Sink.

## Evidence and rejected alternatives

`flow_protocol_graph.c` currently calls `turbo_flow_publish()` for inline mode
and `turbo_flow_publish_async()` for bounded mode. Its public sink accepts the
short `TURBO_FLOW_PROTOCOL_GRAPH_SINK_V1_SIZE` prefix, and
`test_protocol_graph.c` asserts that legacy layout. The protocol runtime also
owns `PENDING -> runtime_settle() -> reply`, so deleting only the graph adapter
would leave the Source coupled to Graph completion and sending.

Keeping `PENDING` until Inbox processing finishes was rejected because it makes
the receive owner wait for business execution and allows the Source runtime to
send a reply. Returning the existing `SETTLED` while retaining runtime reply
was rejected because Inbox admission would trigger an implicit send. A native
`turbo_flow_protocol_metadata_t` memory dump was rejected because it contains
`size_t`, enum representation and padding that are not a durable wire contract.

## Architecture and state ownership

- CNet/CHTTP/Gateway owns receive buffers, connection/session identity, receive
  timestamp and transport backpressure.
- Protocol runtime owns bounded frame reassembly and decode only.
- The new protocol Inbox adapter owns input validation, canonical envelope
  serialization and one synchronous `turbo_flow_inbox_admit()` call.
- The configured Inbox provider is the sole fact source for accepted input,
  idempotency and claim lifecycle.
- `turbo_flow_inbox_source_t` exclusively owns claim -> Graph -> settlement.
- A configured Sink owns response encoding, endpoint generation validation,
  send admission and send completion.

No layer mirrors another layer's mutable state. Borrowed receive/decode bytes
cannot survive the admission callback; a successful Inbox provider has copied
all record bytes. Session/socket pointers and DLL addresses never enter a
durable record.

## Protocol Source ABI 1

The decoder/session runtime becomes a new Source API rather than reusing an old
symbol with changed semantics. Rename the opaque type and every public function,
typedef, macro and installed header from `protocol_runtime` to
`protocol_source`. Remove, rather than deprecate:

- `turbo_flow_protocol_publish_disposition_t` and both SETTLED/PENDING values;
- `turbo_flow_protocol_runtime_publish_fn` and publish-named request;
- `turbo_flow_protocol_runtime_settle()`;
- runtime ops `settled` and `reply`;
- `pending_delivery_id`, `pending_settlements`, `WAIT_SETTLEMENT` and the
  Graph-settlement meaning of `DRAINING`;
- all `turbo_flow_protocol_runtime_*` symbols and the old installed runtime
  header;
- all short ops/config/request/result handling.

Replace the publish callback with an admit callback whose `SALTS_OK` means only
that a Source boundary accepted the decoded message. It returns the exact error
from the selected Inbox/provider and has no disposition output. The runtime
consumes a frame only after admit succeeds. Capacity errors leave the frame
buffered and report backpressure; other errors fail that session explicitly.

The exact admit request carries only borrowed, call-scoped decoded data and
logical session/delivery values. The Inbox adapter requires an exact identity
provider vtable owned by the Gateway/transport Source. It is called once for
each decoded frame and returns borrowed `source_id`, opaque `admission_id`,
`source_sequence` and receive timestamp into a caller-owned exact result. The
Inbox copies them during the same admission call. A resettable process counter
alone is not a valid persistent identity. Calling the identity provider per
frame supports multiple frames per feed without deriving record identity from
the feed chunk or runtime delivery counter. Empty, truncated, duplicate-with-
different-content or reused-generation identities fail rather than being
invented or repaired.

`turbo_flow_protocol_reply()` and protocol encode remain pure codec operations
available to a later Sink. The Source runtime never calls them. Shutdown closes
new Source admission and its sessions; it does not wait for Graph work that the
Inbox already owns.

## Durable protocol envelope

Use an authored TBE schema as the single durable payload contract and generate
C sources at build time with the installed SaltsUtils `tbe_compiler`. Generated
runtime code links the installed `Salts::DataBind` target actually exported
by the package; no TurboParser package or compiler-at-runtime path may return.

The schema uses fixed-width scalar representations and bounded variable-length
bytes/strings for:

- envelope schema version;
- protocol kind and direction;
- protocol version;
- device identity and logical operation;
- protocol sequence and correlation identity;
- original decoded payload bytes;
- logical response route data only when a Sink can validate its generation.

The first schema is little-endian `turbo-flow.protocol.inbox` version 1 with a
`ProtocolInboxEnvelope` message containing `uint32` envelope/protocol/direction/
message-type values, `uint64` sequence, protocol-version/device/operation/
correlation strings and payload bytes. String bounds come from the existing
protocol metadata maxima; payload and final binary size are bounded by Source
and Inbox configuration. “Fixed-width” does not mean a fixed total blob size.

The serialized bytes are the Inbox record payload. The content descriptor is
`DATA / PROTOCOL_DATA / TBE` and declares the exact envelope schema, type and
version. Inbox typed fields retain source identity, admission identity,
receive timestamp, message type, flags, sequence and correlation for indexing.
The descriptor is `DATA / PROTOCOL_DATA / TBE`, media type
`application/vnd.tbe`, schema `turbo-flow.protocol.inbox`, type
`ProtocolInboxEnvelope`, version 1 and stable identity `protocol.ingress`.
Generated parse/serialize APIs or validated typed descriptors are the only
binary encoding route; raw `memcpy` of the native metadata struct is forbidden.
The package installs the generated C codec target/header plus the authored
schema, generated RulesForge DSL and TypeScript declaration from this one
source contract.

RulesForge/TurboScript consume the declared schema through host-side DataBind
ABI 8 object/projection registration. Generated TypeScript is a type declaration,
not a browser-side TBE codec, so no direct TS binary-decode claim is made.
Native consumers use generated accessors or a checked projection; the removed
`turbo_flow_protocol_graph_metadata()` pointer view has no alias.

## Error and state transitions

```text
receive/decode error       -> session failure, no Inbox record
identity/envelope error    -> explicit error, no Inbox record
Inbox capacity error       -> Source backpressure, frame retained
other Inbox/provider error -> exact error, no frame consumption, no Graph
Inbox admit SALTS_OK       -> frame consumed; Source admission complete

Inbox PENDING --claim--> Graph active --terminal--> complete/fail
Graph/Sink outcome never changes the already completed Source receive action
```

No automatic provider retry, database-to-memory fallback, direct Graph publish,
implicit response send or alternate envelope encoding is permitted.

## Implementation tasks

1. Add RED Source tests for exact ABI 1, admit-only frame consumption,
   capacity backpressure, ordinary provider failure and shutdown without a
   pending Graph settlement state.
2. Replace and rename the runtime publish/disposition/settle/reply surface and
   update its repository callers. Delete every old runtime symbol, header and
   layout assertion.
3. Add the canonical TBE schema, generated build target and round-trip/short-
   buffer/malformed/version tests using installed SaltsUtils DataBind ABI 8.
4. Add `TurboFlow::ProtocolIngressInbox` with an exact versioned binding,
   stable identity provider, bounded serialization and synchronous Inbox admit.
5. Replace `test_protocol_graph` with protocol Inbox tests: prove successful
   admit executes no Graph, then explicitly run `turbo_flow_inbox_source_t` and
   verify the same metadata/payload reaches business and Sink stages once.
6. Run the contract against memory and real file-backed TurboDB providers,
   including duplicate identity, full/closed/provider failure and restart read.
7. Delete `tf_protocol_graph`, its header/source/test/component/export/docs and
   update installed C/C++ consumers to link and call only the new ABI.
8. Update #117/#118/#124 and document response/downlink as an explicit Sink
   requirement; do not claim a real network protocol until its Source/Sink DLL
   issue passes network tests.

## Compatibility, migration and rollback

This changes public runtime and package APIs, accepted configuration and durable
payload schema. Old binaries/configuration cannot be mixed with the new
generation. Deployment must stop new receive admission, drain/close the old
protocol generation, select a new Inbox namespace/schema and atomically replace
the complete verified binary/config set. Existing stored data is rejected but
not migrated, rewritten or deleted.

Rollback is an explicit deployment of the previous complete binary/config/data
namespace. There is no runtime downgrade, layout conversion, old symbol probe,
static implementation, C branch or CMake discovery fallback.

## Verification

Run the smallest changed tests first, then adjacent protocol/Inbox/Graph tests,
installed consumers and both full profiles from the x64 Visual Studio developer
environment:

```powershell
cmake --build --preset win-dev-user --target test_protocol_source test_protocol_inbox test_flow_inbox_source
ctest --preset win-dev-user -R "^(test_protocol_source|test_protocol_inbox|test_flow_inbox_source)$" --output-on-failure
cmake --build --preset win-release-user --target test_protocol_source test_protocol_inbox test_flow_inbox_source
ctest --preset win-release-user -R "^(test_protocol_source|test_protocol_inbox|test_flow_inbox_source)$" --output-on-failure
cmake --build --preset install-win-dev-user
ctest --preset win-dev-user -R "^test_turbo_flow_install_consumer$" --output-on-failure
ctest --preset win-dev-user --output-on-failure
ctest --preset win-release-user --output-on-failure
```

Completion also requires CodeGraph affected analysis, generated-schema semantic
round trips, C/C++ header probes, `clang-format --dry-run --Werror`,
`git diff --check`, DLL export/dependency inspection, and searches proving the
old target/header/symbol/layout and all fallback paths are absent.
