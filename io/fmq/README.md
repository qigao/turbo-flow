# TurboFlow FMQ

Current support and deployment boundaries are summarized in
[`PRODUCT_MATRIX.md`](PRODUCT_MATRIX.md). Failure-domain ownership, route
fencing, rolling upgrades, and durable side-effect recovery are specified in
[`DEPLOYMENT_CONTROL.md`](DEPLOYMENT_CONTROL.md).

## Decision

FMQ provides ZeroMQ-like messaging patterns over CoroNet transports without
claiming ZeroMQ wire compatibility. The repository does not depend on libzmq,
and adding it would expand the package, ABI, license-review, and deployment
surface. A small versioned protocol is sufficient for TurboFlow-to-TurboFlow
boundaries and keeps socket ownership in the existing CoroNet event loop.

Alternatives considered:

- libzmq would provide mature interoperable sockets, but adds a new runtime and
  a second I/O/threading model beside CoroNet.
- Reusing the generic socket adapter would not preserve TCP message boundaries
  and cannot express subscriptions, identities, fan-out, or round robin.
- An in-process broker would not satisfy the network boundary use case.

The module can be removed independently by disabling/removing its CMake
subdirectory. Core owns only protocol-neutral role compatibility, candidate
iteration, route matching, and synchronous exchange state; FMQ wire frames,
subscriptions, peers, queues, transport and delivery ACK state remain here.

## Frozen Product Contract

- The local C API is version 1. Public config values must start from
  `TURBO_FLOW_FMQ_CONFIG_INIT`; callback events expose `size` and `version`.
- The wire protocol is version 2 only. API and wire versions advance
  independently, and protocol v1 is rejected.
- FMQ borrows ZeroMQ-style messaging patterns but does not provide ZeroMQ wire
  or API compatibility.
- REQ/REP is deliberately synchronous. REP must publish its reply before the
  current TurboFlow dispatch returns; asynchronous reply handles are not part
  of the FMQ product line.
- Persistence and replay are graph compositions using Redis Streams, SQLite,
  or another storage owner. FMQ does not maintain a hidden durable queue.
- Authentication and authorization are deployment concerns outside the FMQ v2
  product contract. Applications choose an appropriate CoroNet transport and
  network boundary.
- PUB/SUB fan-out, PUSH/PULL round robin, REQ/REP lockstep and ROUTER/DEALER
  route association reuse the protocol-neutral pattern core. This is an
  implementation boundary only and does not change the frozen FMQ v2 wire.

Human-authored configuration is YAML version 1. The resolver produces one
immutable JSON snapshot, profile lookup selects the concrete adapter, and FMQ
projects that entry into the same typed config validation used by the C API.
See `examples/fmq.yml`.

```c
turbo_flow_fmq_config_t config = TURBO_FLOW_FMQ_CONFIG_INIT;
config.pattern = TURBO_FLOW_FMQ_PUB;
config.mode = TURBO_FLOW_FMQ_BIND;
config.transport = TURBO_FLOW_FMQ_TCP;
config.host = "127.0.0.1";
config.port = 7701;
```

## Primitive Bindings

The `.flow` grammar uses normal dotted adapter names:

```flow
source events adapter fmq.sub
stage publish adapter fmq.pub
source jobs adapter fmq.pull
stage dispatch adapter fmq.push
source requests adapter fmq.router
stage reply adapter fmq.router
source responses adapter fmq.dealer
stage request adapter fmq.dealer
source peer_in adapter fmq.pair
stage peer_out adapter fmq.pair
source replies adapter fmq.req
stage request adapter fmq.req
source requests adapter fmq.rep
stage reply adapter fmq.rep
source subscriptions adapter fmq.xpub
stage publish adapter fmq.xpub
source publications adapter fmq.xsub
stage subscribe adapter fmq.xsub
```

The host registers each name with `turbo_flow_fmq_register_adapter()`. Pairings
and endpoint modes are intentionally strict:

| Local primitive | Mode | Peer | Flow role |
| --- | --- | --- | --- |
| PUB | bind | SUB/connect | sink |
| SUB | connect | PUB/bind | source |
| PUSH | bind | PULL/connect | sink |
| PULL | connect | PUSH/bind | source |
| ROUTER | bind | DEALER/connect | source and reply sink |
| DEALER | connect | ROUTER/bind | request sink and response source |
| PAIR | bind or connect | PAIR opposite mode | source and sink |
| REQ | connect | REP/bind | request sink and reply source |
| REP | bind | REQ/connect | request source and reply sink |
| XPUB | bind | SUB or XSUB/connect | subscription source and publication sink |
| XSUB | connect | PUB or XPUB/bind | publication source and subscription sink |

PUB broadcasts to every matching subscription. PUSH chooses one PULL peer in
round-robin order. ROUTER can reply through the current borrowed message context
or through a detached, message-owned route. PAIR bind accepts at most one peer.
DEALER identities must be non-empty and unique at a ROUTER.

SUB and XSUB send a SUBSCRIBE control after HELLO when `topic` is configured;
an empty string subscribes to every topic. A null XSUB topic sends no initial
control so an XPUB source can drive subscriptions through a proxy graph. XPUB
owns a reference-counted prefix set for each peer session and publishes each
SUBSCRIBE/UNSUBSCRIBE as a synchronous TurboFlow message. Disconnect emits the
remaining UNSUBSCRIBE events before the peer state is destroyed. Read controls
with `turbo_flow_fmq_message_subscription()`; ordinary DATA returns
`TURBO_ENOENT`. XSUB also retains its desired subscription counts across a
transport reconnect and replays them after the next compatible HELLO; a full
flow stop clears that desired state.

REQ/REP is stricter than DEALER/ROUTER. REQ allows one outstanding request:
`READY -> WAIT_REPLY -> READY`; a second send while waiting returns
`TURBO_EBUSY`. REP state belongs to each peer session:
`READY -> PROCESSING_REQUEST -> READY`. A reply without the current request
context returns `TURBO_EBUSY`, and a reply must reuse that request's wire
message ID as its correlation ID. A transport or dispatch failure resets the
affected session. REQ becomes ready again only after a new connection completes
HELLO, so a reply from an old session cannot complete a later request.

Unlike ZeroMQ PUB, FMQ returns `TURBO_ENOTCONN` when no subscription matches.
It also returns the first send error after any earlier peer sends have already
completed. This preserves TurboFlow's fail-fast/no-silent-drop policy; callers
must treat a failed broadcast as a potentially partial external side effect.

## Wire Protocol

Each transport connection begins with a bidirectional HELLO frame. DATA messages
follow only after compatible patterns are validated. Protocol v2 uses one packet
for payloads up to 64 KiB and automatically emits multiple ordered packets for
larger payloads. Integers use network byte order. Supported transports are `tcp`,
`tls`, `udp`, `kcp`, `pipe`, `ws`, and
`wss`; `pipe` uses `path` as the endpoint, while `ws`/`wss` use `path` as the
request path and default it to `/`.

```text
offset  size  field
0       4     magic "TFMQ"
4       1     version (2)
5       1     frame kind (HELLO=1, DATA=2, PING=3, PONG=4, SUBSCRIBE=5, UNSUBSCRIBE=6)
6       1     sender pattern
7       1     packet flags (FIRST=0x01, LAST=0x02)
8       2     identity length
10      2     topic length
12      4     packet payload length
16      8     message ID (DATA only; connection-local and nonzero)
24      4     complete message payload length
28      4     packet payload offset
32      ...   identity, topic, packet payload
```

Identity and topic occur only in the FIRST packet. Subsequent packets must carry
the same kind, pattern, message ID, complete payload length, and contiguous
offset; otherwise the connection fails with `TURBO_EPROTO`. HELLO, PING, and
PONG, SUBSCRIBE, and UNSUBSCRIBE are always single FIRST|LAST packets.
Subscription controls use a zero message ID and carry only a topic. Protocol v1
is deliberately rejected; there is no rolling-upgrade compatibility path.

The v2 wire-size formula is:

```text
H = 32                         fixed packet header bytes
C = 65536                      maximum packet payload chunk bytes
B = identity_len + topic_len + payload_len
N = DATA && payload_len > 0 ? ceil(payload_len / C) : 1
encoded_size = B + N * H

packet[k] = header(H)
          + (k == 0 ? identity_len + topic_len : 0)
          + min(C, payload_len - payload_offset[k])
```

The decoder first derives each `record_len` as
`H + identity_len + topic_len + chunk_len`, then validates the complete packet
sequence. Every packet must retain the first packet's kind, pattern, message ID,
and complete payload length; `payload_offset[k]` must equal the number of payload
bytes already accepted. A frame is complete only when LAST is present and the
accepted byte count equals `payload_len`. A single packet is exposed as borrowed
views; a fragmented payload is copied into frame-owned contiguous storage.

`max_frame_size` bounds the combined identity, topic, and complete payload before
reassembly allocation. Identity is limited to 255 bytes, topic to 1024 bytes,
each packet payload to 64 KiB, complete payload to `UINT32_MAX`, and configured
bind peers to 65535. Partial and coalesced transport reads are accumulated with
TurboUtils `tstr_t`; no stream `recv` call is treated as a message boundary.

FMQ supports a lightweight reconnect strategy for connect-mode endpoints:
`reconnect_initial_ms` and `reconnect_max_ms` control the exponential backoff.
Legacy zero-initialized configs use the documented defaults. Set
`reconnect_initial_ms` to `TURBO_FLOW_FMQ_RECONNECT_DISABLED` to disable
reconnect explicitly. Set `reconnect_max_ms` to
`TURBO_FLOW_FMQ_RECONNECT_MAX_UNBOUNDED` for exponential backoff without a cap.
Each retry applies equal jitter to the exponential base and waits in
`[ceil(base / 2), base)` milliseconds (`base <= 1` is unchanged). The reported
`RECONNECT_SCHEDULED` delay is the actual wait, while the next exponential step
continues from the unjittered base. This prevents synchronized reconnect bursts
without exceeding `reconnect_max_ms` or introducing a zero-delay retry loop.
No broker discovery, persistence, or protocol-level ack remains implicit.
A transport failure closes that peer and later sends fail unless the connection
is re-established.

## Flow Control V1 over REQ/REP

The broader versioned management contract is specified in
[`MANAGEMENT_PROTOCOL.md`](MANAGEMENT_PROTOCOL.md). It keeps this Control V1
wire and YAML entry unchanged while adding capability discovery, typed
operations, failure-domain identity, and a separate event PUB/SUB channel.
The TFMP envelope, canonical LTV body codec, frozen v1 typed field registry,
and phase-1 single-thread owner are implemented. The owner serves
capability/health plus single-target target/resource list/get and status
document queries through a borrowed `turbo_flow_t`. It also executes
`FLOW_PAUSE`, `FLOW_RESUME`, and `FLOW_DRAIN` as generation-checked,
`WAIT_TERMINAL + VOLATILE` commands with bounded TTL idempotency records. The
same owner adapts resource quiesce/resume, endpoint replacement, and pool resize
to the existing typed TurboFlow resource owner without copying resource state.
It accepts bounded volatile operations before mutation and, when the host binds
an atomic blob store, durable operations whose acceptance and cancellation are
committed before the REP reports success. SQLite and Redis providers resolve
`kind: blob_store` YAML channels; malformed/unavailable stores fail owner
creation without falling back to memory. Restart restores accepted and terminal
records, while an operation left RUNNING by a crash becomes `FAILED/INTERNAL`
instead of being blindly repeated. Query/cancel use monotonic revisions and the
owner-lane `run_one` API executes one claim at a time. The bounded live-event
journal emits content-derived `tfmp/1/event/<category>` topics through a
separate FMQ PUB adapter. Set `event_replay_store` to the same external
`blob_store` named by `operation_store` to persist the journal incarnation,
sequence barrier, and retained events in the same atomic snapshot as durable
operation transitions. Split stores fail startup because they cannot provide
one outbox commit. The strict `fmq_management` YAML parser and thin TFMP stage
are available: the stage runs inside an ordinary FMQ REP graph, so
TCP/TLS/KCP/Pipe/WS/WSS remain CoroNet transport choices rather than
protocol-specific socket code.

Remote management is an application protocol inside an ordinary FMQ DATA
payload; it is not a new FMQ wire version or socket pattern. A dedicated
management flow owns a REP graph and targets a different data flow:

```flow
source request adapter fmq.control.rep
stage control
stage reply adapter fmq.control.rep
stage main {
  request -> control -> reply
}
```

The host registers `control` with `turbo_flow_fmq_control_stage()` and a service
created from the `kind: fmq_control` YAML channel in `examples/fmq.yml`. The
service borrows the managed flow and must be destroyed first. It serializes its
own requests; lifecycle calls made outside the service still require host
serialization. Putting the REP stage in the managed flow is invalid because a
`flow drain` command could wait for its own dispatch.

Control V1 has two explicit acknowledgements. A stage return of `TURBO_OK`
means the request was decoded or converted into a terminal protocol-error
reply. The decoded reply's `status` is the management action result. Thus an
invalid command, unknown target, condition failure, or owner error still
completes the strict REP exchange instead of leaving the session in
`PROCESSING_REQUEST`.

`STATUS` is read-only and carries no idempotency key. `EXECUTE` carries a
pointer-free typed `turbo_flow_control_command_t`, a nonzero request ID, and a
nonempty idempotency key. The service keeps a bounded, non-evicting result
history: an exact retry replays the terminal reply; reusing a key for different
bytes returns `TURBO_EPROTO`; exhaustion returns `TURBO_ENOSPC`. This history is
memory state, not durable workflow storage. Durable control operations should
return an operation ID and store their state in Redis/SQLite/PG, then be polled
by another synchronous request.

Endpoint transport remains an independent FMQ adapter choice. The same Control
V1 stage works with `tcp`, `tls`, `kcp`, `pipe`, `ws`, or `wss` by changing the
YAML connection fragment; control semantics do not depend on CoroNet transport.
TCP is covered end-to-end by the control test, while the existing FMQ REQ/REP
suite separately covers Pipe and the transport handshake/state machine.

Control integers use network byte order and reserved fields must be zero. The
64-byte request header is followed, in order, by target, idempotency key,
command target, condition, endpoint host, and endpoint path bytes; lengths do
not include a terminator.

```text
offset  size  request field
0       4     magic "TFCQ"
4       2     protocol version (1)
6       2     header size (64)
8       2     operation (STATUS=1, EXECUTE=2)
10      2     control kind
12      2     pool kind
14      2     adapter command kind
16      4     parallelism
20      4     endpoint port
24      8     request ID
32      8     command timeout milliseconds
40      12    six uint16 string lengths
52      12    reserved zero
```

The 88-byte reply header is followed by at most 159 non-NUL diagnostic bytes.
It carries the complete fixed runtime snapshot; `request ID == 0` is valid only
for an `EPROTO`/`EMSGSIZE` reply whose request header could not be trusted.

```text
offset  size  reply field
0       4     magic "TFCP"
4       2     protocol version (1)
6       2     header size (88)
8       8     request ID
16      4     signed application status
20      4     flags (bit 0 = replayed)
24      4     runtime state
28      4     accepting publishes (0 or 1)
32      4     active publishes
36      4     reserved zero
40      32    stage/edge/adapter/pool counts (four uint64)
72      8     error line and column (two uint32)
80      2     diagnostic length
82      6     reserved zero
```

KCP endpoints may enable CoroNet packet-erasure FEC with explicit
`kcp_fec_*` options. Enabling FEC on non-KCP transports is rejected, and an
unavailable backend returns `TURBO_ENOTSUP` during configuration.
TCP-backed transports (`tcp`, `tls`, `ws`, `wss`) can pass OS TCP keepalive and
SO_LINGER options through CoroNet. `send_hwm_bytes` is a socket-level CoroNet
send cap for stream transports (`tcp`, `tls`, `ws`, `wss`, `pipe`); it is
separate from FMQ frame/message queue HWM.

UDP endpoints expose CoroNet multicast loopback, multicast TTL/hop-limit and
IPv4 broadcast through `udp_option_flags` plus their typed values. The flags
distinguish an explicit zero/false value from an unset legacy config, which
preserves the OS default. `udp_multicast_group` is valid only for BIND endpoints;
the adapter joins after a successful bind and leaves before listener teardown.
`udp_multicast_interface` requires a group and is an IPv4 local address or an
IPv6 decimal interface index. UDP-only fields on another transport, a group on
CONNECT, an interface without a group, unknown flags, and TTL values above 255
fail configuration.

`frame_hwm_messages` and `frame_hwm_bytes` cap FMQ encoded frames that are
currently in flight through the adapter; 0 disables that cap.
`frame_admission_policy=fail` is the default and returns `TURBO_ENOSPC` when a
cap is reached. `frame_admission_policy=block` waits for capacity for up to
`frame_admission_timeout_ms`; stop closes admission and wakes blocked senders
with `TURBO_ESHUTDOWN`. A zero timeout checks immediately and `UINT64_MAX`
waits without a deadline. Reaching a FAIL limit or a BLOCK deadline emits
`TURBO_FLOW_FMQ_EVENT_HWM_REACHED` with the returned status.
`frame_admission_policy=drop_oldest` removes only the oldest request that is
still queued, completes that request with `TURBO_ECANCELED`, and emits both
`TURBO_FLOW_FMQ_EVENT_HWM_REACHED` and `TURBO_FLOW_FMQ_EVENT_FRAME_DROPPED`.
The active send is never evicted. If no queued request can free enough
capacity, the new request returns `TURBO_ENOSPC`.

PUB/XPUB can additionally enable bounded per-peer fan-out with
`turbo_flow_fmq_register_fanout_adapter()` or the resolved YAML fields
`peer_hwm_messages`, `peer_hwm_bytes`, and `slow_peer_policy`. At least one
per-peer HWM and the policy must be present together; the mode is valid only
for PUB/XPUB bind over a connected CoroNet transport. Every remote SUB/XSUB
must then send a non-empty identity that is unique among live peers. The old
registration APIs retain their synchronous direct fan-out behavior.

The adapter aggregate HWM and the per-peer HWM have distinct ownership. The
aggregate budget owns one shared encoded frame from adapter admission until
all selected peers reach a terminal outcome. Each peer budget owns only that
peer's active/queued pointer to the shared frame. No payload is copied per
subscriber, and neither HWM is a credit grant.

A successful explicit fan-out publish is a volatile queue-admission ACK, not a
delivery ACK. Actual writes emit `FRAME_SENT` with `peer_identity`; transport
failures emit `FRAME_DROPPED`. Slow-peer actions additionally emit
`SLOW_PEER_HWM`, `SLOW_PEER_DROPPED`, or `SLOW_PEER_DISCONNECTED` with the
affected identity and frame size:

- `fail` preflights every matched peer and returns `TURBO_ENOSPC` without
  enqueueing the current publication anywhere when one peer is full.
- `drop_oldest` evicts only the oldest queued item of the saturated peer; an
  active socket send is never evicted.
- `disconnect` rejects the current frame for the saturated peer, fails its
  backlog, interrupts only that session, and continues admission for remaining
  matched peers. If none remains, the publication returns `TURBO_ENOTCONN`.

Per-peer deque capacity is reserved before a publication mutates any queue, so
the `fail` policy cannot become a partial enqueue because of a later allocation
failure. Queue drain and socket I/O remain on the owning CoroNet lane; stop or
disconnect interrupts active sends, completes every queued peer outcome, and
releases the shared frame only after the final peer terminal event.

`frame_linger_ms` controls FMQ stop-time drain for in-flight frames. The default
0 stops immediately; a positive value waits up to that many milliseconds for
current sends to finish before interrupting sockets and stopping the CoroNet
context. Stop first closes frame admission, so blocked and future sends return
`TURBO_ESHUTDOWN`. Requests already accepted into the adapter-owned FIFO retain
their encoded frame and may finish until the linger deadline. At the deadline,
FMQ interrupts transport waits and completes every request with its final
transport status or `TURBO_ESHUTDOWN`; a send whose transport result was already
finalized is not retroactively changed. Every completion path releases message
and byte budget, so stopped snapshots have zero in-flight counters.

Disconnect does not create a replay queue. An active or queued request completes
once with the transport error observed for that attempt, releases its frame and
budget, and is never attached to a later peer. Connect-mode recovery recreates
only transport/session state; it does not retain completed send requests.
Consequently, reconnect affects future sends only. Applications that need
redelivery must compose FMQ with the queue/storage layer and an explicit ACK
contract.

Sink stages support TurboFlow's explicit retry clause, for example
`stage outgoing adapter fmq.output retry attempts 3 delay 10`. Each attempt
uses an isolated message clone. FMQ retries only temporary transport failures:
connection timeout/refusal/reset/abort, disconnected or unreachable endpoints,
broken pipes, network-down conditions, and transport I/O failures. Invalid
configuration or pattern use, oversized/invalid frames, missing inherited
metadata, allocation failures, and FMQ HWM `TURBO_ENOSPC` failures are not
retried. Retry delay uses the shared interruptible timer and returns
`TURBO_ESHUTDOWN` when flow stop interrupts it.

Connect-mode reconnect backoff uses CoroNet's coroutine-aware `coro_wait`.
The wait yields the event loop rather than blocking its thread, and FMQ stop
interrupts an active backoff with `TURBO_ESHUTDOWN` without fixed polling
slices.

`heartbeat_interval_ms` and `heartbeat_timeout_ms` enable protocol heartbeat.
Both must be set, and timeout must be at least the interval. PING/PONG control
frames are consumed inside FMQ and are never published as DATA. Receive waits
use the earliest absolute deadline among the next PING, peer heartbeat expiry,
and configured receive-operation expiry. Partial frames retain that same
deadline across every underlying CoroNet recv. Flow stop interrupts the active
socket wait with `TURBO_ESHUTDOWN`; it does not wait for a polling slice.

Timeout fields retain legacy fallback behavior when set to 0. Send, receive,
and handshake timeouts may be set to `TURBO_FLOW_FMQ_TIMEOUT_DISABLED` to
disable that CoroNet operation timeout explicitly. The default and connect
timeouts cannot be disabled because synchronous FMQ startup requires a finite
connection deadline.

FMQ can report local control-plane events through `event_callback` and
`event_ctx`. The callback receives borrowed metadata for peer connect/disconnect,
reconnect scheduling/success/failure, heartbeat timeout, frame sent, HWM
reached, frame dropped, and identity-bearing slow-peer events. It is a
host-owned monitoring hook and must not re-enter the same flow or adapter.
Hosts that need aggregate metrics can map these events to
`turbo_flow_observe_control_event_t` and call
`turbo_flow_observe_record_control_event()`.

## Endpoint Control

The host can route `TURBO_FLOW_ADAPTER_QUIESCE`,
`TURBO_FLOW_ADAPTER_RESUME`, and `TURBO_FLOW_ADAPTER_REPLACE_ENDPOINT` through
`turbo_flow_adapter_command()` using the FMQ adapter binding name. Quiesce and
resume are idempotent. Quiesce closes live CoroNet resources while preserving
the flow lifecycle reference, so a later resume recreates the same endpoint.

Replacement accepts structured `host`/`port`/`path` data. FMQ validates and
copies the candidate before changing its owner state. For a live adapter it
stops the old resources, starts the candidate, and restores the previous
endpoint and ready resources if candidate startup fails. Use core pause/drain
before replacing a sink endpoint. Quiesce source endpoints before draining or
resizing a graph, then resume them after the graph command succeeds.

## Ownership And Threading

All sockets and peer vectors belong to one CoroNet context. Flow worker threads
submit sends through CoroNet's thread-safe post queue and wait for synchronous
completion, so adapter consume never retains the flow message. A supplied
context must transfer ownership to FMQ; non-owned contexts are rejected until
their shutdown/drain contract can guarantee no posted callback outlives the
adapter.

Endpoint lifecycle is a separate control plane. Bind, connect scheduling,
resource close, and peer clear are copied as bounded pointer-free commands into
the adapter's CoroNet actor mailbox and execute on that owner lane. Each command
has a monotonic local ID, an absolute deadline, and a reply handle; the host
orchestrates quiesce/resume/replace synchronously and never receives a
cross-thread callback. The mailbox has one slot because flow lifecycle and
resource commands are host-serialized. This local actor contract does not add a
new FMQ v2 frame or make control commands remotely addressable.

Incoming payloads are copied into message-owned `tstr_t`. Topic and identity
metadata are borrowed and available through `turbo_flow_fmq_message_topic()`,
`turbo_flow_fmq_message_identity()`, and
`turbo_flow_fmq_message_correlation_id()` only during synchronous dispatch.
DATA and subscription-control messages also borrow a request-local immutable
content descriptor whose identity is the actual topic. It follows the same
synchronous-dispatch lifetime unless an API explicitly copies it.

ROUTER supports a real delayed reply boundary. A synchronous ROUTER input stage
calls `turbo_flow_fmq_message_detach_router_route(msg)`, which copies the
descriptor and a pointer-free `{owner instance, session, generation}` route into
message-owned storage and clears `transport_context`. The message can then be
cloned, moved, sent through a worker/fan-out or a memory Queue, and later enter
the same ROUTER sink. Peer lookup and socket I/O still execute on the owning
CoroNet context. If the DEALER disconnects, reconnects with the same identity,
or the ROUTER restarts, the old route returns `TURBO_ENOTCONN` and is never
retargeted to the new session.

The detached route is process-local session state, not a durable correlation
identity. SQLite Queue and other serialization boundaries must reject it; a
durable workflow must persist an application correlation key and establish a
new live route after recovery. REP and bound PAIR remain synchronous and reject
the ROUTER detach API. FMQ intentionally provides no asynchronous REQ/REP reply
path.

## Composed Patterns

Higher-level broker shapes are built by composing ordinary stages. Sink
endpoints normally use their configured `topic` and `identity`. Set
`topic_policy` or `identity_policy` to `inherit` when an outbound endpoint must
preserve metadata from an upstream FMQ source:

```flow
source inbound adapter fmq.orders.sub
stage decode adapter codec.json.in
stage validate adapter databind.order.in
stage outbound adapter fmq.orders.pub

stage main {
  inbound -> decode -> validate -> outbound
}
```

The `fmq.orders.pub` adapter can publish with `topic_policy = inherit`, so a
message received from `fmq.orders.sub` on topic `orders.created` leaves the
bridge with the same topic. Inherit policies fail when the current message does
not carry FMQ metadata; this keeps accidental silent rerouting out of the data
plane.

### Advanced request-reply load balancer

`turbo_flow_fmq_broker_t` is the Chapter 3 control-state primitive above two
ROUTER/DEALER legs. It does not create sockets or hide graph edges. A worker
first sends an application-level READY message. The backend ROUTER stage
detaches that live route and registers it with
`turbo_flow_fmq_broker_worker_ready()`. A frontend request then supplies its
detached client route and an application request ID to
`turbo_flow_fmq_broker_dispatch()`; the result selects the least-recently-used
idle worker route. On reply, `turbo_flow_fmq_broker_complete()` returns the
retained client route.

The state owner is bounded and configured from the normal YAML resolver:

```yaml
version: 1
channels:
  request-broker:
    kind: fmq_pattern
    config:
      pattern: load_balancer
      scheduler: lru
      max_workers: 256
      max_inflight: 4096
adapters: {}
```

Broker calls on one object are host-serialized. The broker copies only logical
names and process-local routes; it never owns payloads. A failed worker send is
rolled back explicitly with `turbo_flow_fmq_broker_cancel()`. Completion means
routing correlation finished—it is neither an accept ACK nor a delivery ACK.
Worker expiry, request requeue, replay and persistence are reliable
request-reply policies layered on this primitive rather than implicit Chapter 3
behavior.

Inter-broker forwarding uses
`turbo_flow_fmq_broker_logical_address_t`, not a copied
`turbo_flow_protocol_route_t`. This follows the separation between a logical
application address and a connection-local ROUTER identity described by
[ZGuide Chapter 3](https://zguide.zeromq.org/docs/chapter3/). The `TFBR`
envelope is explicitly versioned and byte encoded:

| Offset | Width | Field |
| ---: | ---: | --- |
| 0 | 4 | magic `TFBR` |
| 4 | 2 | protocol version, big-endian |
| 6 | 2 | header size (`24`), big-endian |
| 8 | 2 | origin broker ID length |
| 10 | 2 | client ID length |
| 12 | 4 | reserved, must be zero |
| 16 | 8 | non-zero request ID, big-endian |
| 24 | variable | origin broker ID followed by client ID |

The origin broker owns the mapping from `(client_id, request_id)` to the
current live local route. A receiving broker may persist or forward the logical
address, but must never persist a route's owner/session/generation values. When
the reply returns, the origin broker resolves the logical client again; a
missing or stale live route is a visible delivery failure, not a reason to
invent a replacement route. This envelope does not alter the FMQ v2 transport
frame and is not ZeroMQ wire compatible.

For the first Chapter 4 reliability boundary, select `reliable_request` and an
explicit failure policy:

```yaml
channels:
  reliable-request-broker:
    kind: fmq_pattern
    config:
      pattern: reliable_request
      scheduler: lru
      max_workers: 256
      max_inflight: 4096
      reliability: at_least_once
      worker_lease_ms: 3000
      dedup_capacity: 4096
      dedup_ttl_ms: 300000
      max_attempts: 3
```

Reliable brokers use only caller-supplied monotonic timestamps. Register with
`turbo_flow_fmq_broker_worker_ready_at()`, refresh liveness with
`turbo_flow_fmq_broker_worker_heartbeat()`, and dispatch with
`turbo_flow_fmq_broker_dispatch_at()`. A stale worker is never selected.
`turbo_flow_fmq_broker_expire()` removes one expired worker per call and returns
one of three decisions: idle removal, drop for `at_most_once`, or requeue for
`at_least_once`. The result carries the request ID and client route, but the
broker deliberately stores no payload and performs no retransmission. The graph
queue/storage owner must execute a returned requeue decision. Memory and SQLite
Queue use `turbo_flow_queue_claim()` plus `claim_ack/requeue`; Redis Streams use
`turbo_flow_redis_stream_owner_claim()` plus `owner_ack/requeue`. This keeps the
payload claimed across a real delayed ROUTER/DEALER reply instead of deleting it
after dispatch.

The lease heartbeat proves only that the worker session is live; it is not an
ACK. After a memory Queue enqueue, SQLite commit, Redis XADD, or equivalent
owner commit succeeds, the host records that boundary with
`turbo_flow_fmq_broker_record_accept_commit()`. The returned `ACK_ACCEPT` route
is the first ACK. The committed request remains pending until
`turbo_flow_fmq_broker_dispatch_accepted()` selects a live worker. A successful
worker reply is completed separately and converted to
`ACK_WORKER_COMPLETION` with `turbo_flow_fmq_broker_completion_ack()`.

`turbo_flow_fmq_retry_ledger_t` supplies the bounded in-memory Chapter 4 retry
and duplicate state machine. Its key is
`(origin_broker_id, client_id, request_id)`. The YAML fields have these exact
meanings:

- `dedup_capacity` bounds all active and retained terminal records. Capacity
  pressure returns `TURBO_ENOSPC`; it never silently evicts an active request.
- `max_attempts` counts the first worker attempt. A failed final attempt moves
  the record to `POISONED`; it is not requeued.
- `dedup_ttl_ms` starts when a request becomes `COMPLETED` or `POISONED`.
  Pending and in-flight records do not expire through this TTL.

Duplicate accept is idempotent and reports the existing pending, in-flight,
completed, or poisoned state. `at_most_once` requires `max_attempts: 1`;
`at_least_once` defaults to three attempts. The ledger restores pointer-free
records but does not normalize restored `INFLIGHT` state: the host must first
reconcile the old attempt and then explicitly finish it as failed to requeue.
This follows the unique client ID plus request sequence and idempotent-service
model described in
[ZGuide Chapter 4](https://zguide.zeromq.org/docs/chapter4/).

The current ledger is a memory fact owner. A copy returned after a transition
is not a SQLite/Redis commit and must not be presented as `ACK_ACCEPT`.
Durable integration must make the storage command the primary fact transition,
then update/rebuild the ledger from that committed record; this boundary is
kept explicit to avoid dual facts advancing independently.

The broker stores only service/request correlation and the current process-local
client route. Queue/Redis/SQLite remains the payload fact source. Therefore the
accept API must be called only after its commit succeeds, and a broker restart
does not make a live client route durable. The logical envelope makes a return
address network-safe, but restart recovery still requires the origin broker to
restore the logical request record and wait for the client to establish a new
live route; no FMQ route is serialized as a substitute.

### Credit worker bulk pipeline

`credit_worker` is a configuration-driven application pattern over ordinary
FMQ ROUTER/DEALER DATA frames. TFCW/1 is a versioned 40-byte envelope with a
canonical TurboUtils LTV body; it does not add an FMQ frame kind or claim
ZeroMQ wire compatibility. The opaque, host-serialized owner tracks only live
worker routes, lease, message/byte credits, and request correlations. It never
stores payloads or creates a hidden retry queue.

```yaml
channels:
  bulk-workers:
    kind: fmq_pattern
    config:
      pattern: credit_worker
      scheduler: lru
      reliability: at_most_once
      max_workers: 256
      max_inflight: 4096
      worker_lease_ms: 15000
      max_credit_messages_per_worker: 64
      max_credit_bytes_per_worker: 67108864
      max_job_bytes: 8388608
```

Credits are incremental and generation-scoped. A dispatch consumes one message
credit plus its encoded byte size; completion, cancellation, or transport send
success never manufactures replacement credit. When live workers exist but no
worker has both credits, dispatch returns `TURBO_FLOW_FMQ_EAGAIN`. READY starts
a route generation at sequence 1, duplicate grants are accepted only when
identical, and gaps or conflicting duplicates fail without partial mutation.

The two ACKs remain separate from credit: storage acceptance owns
`ACK_ACCEPT`, while current-generation worker completion followed by successful
storage settlement owns `ACK_WORKER_COMPLETION`. The host-serialized
`turbo_flow_fmq_credit_settlement_t` binds request IDs to borrowed Queue/Redis
claim tokens without copying payload. Completion, cancellation, and lease
expiry synchronously apply ACK, requeue, or drop through a
`turbo_flow_claim_settler_t`; a backend error retains a bounded pending record
for `turbo_flow_fmq_credit_settlement_retry_one()`. Completion ACK is not
returned until claim ACK succeeds, and drop cannot manufacture a delivery ACK.

For cross-process recovery, `turbo_flow_fmq_credit_durable_t` uses the storage
owner's `load_state/commit_state` capability. It commits the versioned TFCS/1.0
LTV snapshot and Redis Stream or SQLite Queue ACK/requeue/drop in the same
backend transaction.
Only the stable `(origin_broker_id, client_id, request_id)` is durable; claim
tokens, worker request IDs, and CoroNet routes are rebuilt after restart. An
`INFLIGHT` snapshot restarts as `PENDING`, while a committed completion remains
in a logical-address outbox until the host confirms publication. Exact retry
also reconciles a transaction whose Redis reply was lost.

Strict YAML enables this path only through
`turbo_flow_fmq_credit_durable_create_resolved()` with a matching explicit
storage binding:

```yml
channels:
  bulk-workers:
    kind: fmq_pattern
    config:
      pattern: credit_worker
      scheduler: lru
      reliability: at_least_once
      max_workers: 256
      max_inflight: 4096
      worker_lease_ms: 15000
      max_credit_messages_per_worker: 64
      max_credit_bytes_per_worker: 67108864
      max_job_bytes: 8388608
      storage_channel: redis.claims
      state_key: fmq:bulk-workers:state
      max_attempts: 3
      dedup_ttl_ms: 300000
      shutdown_policy: requeue
      shutdown_max_steps: 4096
```

The ordinary `turbo_flow_fmq_credit_worker_create_resolved()` remains the
volatile `at_most_once` entry point and rejects this configuration. The binding
may be a Redis Stream or SQLite Queue settler, but it must match
`storage_channel`; an unavailable or incompatible backend fails creation and is
never replaced by a volatile fallback.

Before destroying a durable coordinator, the host calls
`turbo_flow_fmq_credit_durable_shutdown()`. This permanently closes dispatch
admission for that instance. `shutdown_policy: requeue` cancels live credit
correlations and atomically requeues (or drops at `max_attempts`) at most
`shutdown_max_steps` records per call; a backend error leaves the instance
quiesced and retryable. `shutdown_policy: preserve` keeps the already durable
state for backend restart replay. Destroy is memory cleanup only and cannot
report settlement failure. See
[BULK_CREDIT_PROTOCOL.md](BULK_CREDIT_PROTOCOL.md) for the state and ownership
contract.

## Codec And DataBind

FMQ transports bytes plus routing metadata. Framing does not parse application
payloads. Put codec and DataBind stages explicitly before or after FMQ:

```flow
source incoming adapter fmq.sub
stage decode adapter codec.length.in
stage bind adapter databind.order.in
stage encode adapter codec.length.out
stage outgoing adapter fmq.push

stage main {
  incoming -> decode -> bind -> encode -> outgoing
}
```

This keeps protocol state, parsed-data ownership, and errors attached to their
own modules and gives input/output the same codec/DataBind behavior as the other
network adapters.

## Proxy Device Templates

Proxy devices are reusable stage compositions. They are not special grammar
forms; hosts bind concrete FMQ/codec/DataBind/queue adapters through normal
adapter configuration.

PUB/SUB filtering keeps topic metadata on the FMQ side and lets codec/DataBind
stages own payload validation:

```flow
stage pubsub_proxy {
  in frames
  out routed
  step decode adapter codec.json.in
  step filter adapter databind.filter
  step encode adapter codec.json.out

  frames -> decode -> filter -> encode -> routed
}

source orders_in adapter fmq.orders.sub
stage orders_out adapter fmq.orders.pub

stage main {
  use proxy = pubsub_proxy
  orders_in -> proxy -> orders_out
}
```

PUSH/PULL queueing can insert a queue/balancer stage without changing FMQ
framing:

```flow
stage queue_proxy {
  in jobs
  out balanced
  step balance adapter queue.balance

  jobs -> balance -> balanced
}
```

ROUTER/DEALER request and reply legs stay explicit. A service that crosses an
asynchronous boundary first detaches the live ROUTER route; the later reply leg
must target the same configured ROUTER adapter:

```flow
stage router_dealer_proxy {
  in front_requests
  out back_requests
  in back_replies
  out front_replies
  step tag adapter databind.identity

  front_requests -> tag -> back_requests
  back_replies -> front_replies
}
```

The detach stage callback performs the ownership transition before returning:

```c
static int detach_router_request(turbo_flow_msg_t *msg, void *ctx) {
  (void)ctx;
  return turbo_flow_fmq_message_detach_router_route(msg);
}
```

XPUB/XSUB uses two bidirectional FMQ adapters. The front XPUB publishes
subscription events upstream and sends publications downstream; the back XSUB
forwards controls upstream and receives publications. The front XPUB must
inherit the upstream publication topic:

```flow
source subscriptions adapter fmq.front_xpub
source publications adapter fmq.back_xsub
stage upstream adapter fmq.back_xsub
stage downstream adapter fmq.front_xpub

stage main {
  subscriptions -> upstream
  publications -> downstream
}
```

Configure `fmq.back_xsub.topic = NULL` and
`fmq.front_xpub.topic_policy = TURBO_FLOW_FMQ_METADATA_INHERIT`. Subscription
events are valid only during synchronous dispatch; storing their transport
context or forwarding them through an asynchronous queue is rejected by the
XSUB sink.

### Chapter 5 last-value state and ordered recovery

`turbo_flow_fmq_pubsub_state_t` is the bounded state owner used to compose a
last-value cache or Clone-style recovery graph. It is deliberately separate
from sockets: XPUB subscription controls select a byte-prefix subtree, while
the owner keeps exact-topic values and a strictly ordered put/delete journal.
The host serializes calls and decides whether the owner is fed from live FMQ,
Redis, SQLite, or another durable graph boundary.

Configure it as a YAML channel:

```yml
channels:
  pubsub-state:
    kind: fmq_pattern
    config:
      pattern: pubsub_state
      max_topics: 4096
      max_state_bytes: 16777216
      update_capacity: 8192
      max_update_bytes: 33554432
```

Remote recovery uses TFPS/1, an application protocol carried inside ordinary
FMQ DATA payloads. It does not add an FMQ pattern or change the frozen FMQ v2
frame. TFPS/1 has a fixed 56-byte network-order header and pointer-free records
with a fixed 20-byte header followed by topic and payload bytes. Its message
kinds are `CAPABILITIES`, `SNAPSHOT`, `UPDATES`, `LIVE_UPDATE`, and
`PROTOCOL_ERROR`. Stable TFPS status drives client behavior; `native_status` is
diagnostic only.

The live and recovery paths borrow the same state owner and must run on the
same serialized graph lane. A typical proxy composition is:

```flow
source subscriptions adapter fmq.front_xpub
source publications adapter fmq.back_xsub
source recovery_request adapter fmq.recovery_rep
stage state_put
stage upstream adapter fmq.back_xsub
stage downstream adapter fmq.front_xpub
stage recover
stage recovery_reply adapter fmq.recovery_rep

stage main {
  subscriptions -> upstream
  publications -> state_put -> downstream
  recovery_request -> recover -> recovery_reply
}
```

Register `state_put` with `turbo_flow_fmq_pubsub_put_stage()` and `recover`
with `turbo_flow_fmq_pubsub_recovery_stage()`. The PUT stage obtains the topic
from FMQ input metadata, or from a validated message-owned content descriptor
for local publication, commits the exact value and journal entry, then replaces
the publication payload with a sequenced `LIVE_UPDATE`. A DELETE
graph uses the distinct `turbo_flow_fmq_pubsub_delete_stage()`; an empty PUT is
not interpreted as deletion. The recovery REP remains strict synchronous
REQ/REP and always emits a terminal TFPS response when its configured reply
bound permits one.

A recovering subscriber first subscribes to the live XPUB prefix so updates
can queue, then requests `CAPABILITIES` and a prefix `SNAPSHOT` over the
separate REQ/REP connection. It applies the snapshot, discards queued live
records at or below the snapshot barrier, and applies contiguous records above
the barrier. On a sequence gap it requests paged `UPDATES` with the last applied
sequence. The first page freezes an `upper_bound`; every following page sends
that same bound until `COMPLETE`. A `STALE_CURSOR` response means the bounded
journal no longer covers the interval, so the client must restart from a new
snapshot instead of accepting incomplete state.

Snapshot replies are atomic: if the complete prefix snapshot exceeds
`max_reply_bytes` or the record-count limit, the service returns terminal
`RESOURCE_EXHAUSTED` and never emits a partial snapshot. Update pages are
bounded by both the request page limit and `max_update_records`. Tombstones are
journaled even for a missing local key so remote state can delete stale values
deterministically.

The state limits and transport limits serve different owners. `max_topics` and
`max_state_bytes` bound current state; `update_capacity` and
`max_update_bytes` bound the recovery window. `frame_hwm_*`,
`frame_admission_policy`, and `send_hwm_bytes` continue to control FMQ transport
queues and slow-subscriber pressure.

The current prefix matcher remains a direct byte-prefix scan. CRoaring is not
used merely because subscriber IDs can be represented as a bitmap: a bitmap
still needs a topic-to-subscriber index, and there is not yet profiling evidence
that the existing scan is the bottleneck. A future measured hot path can map
trie/hash prefix nodes to roaring subscriber sets without changing the public
state or cursor contract.
