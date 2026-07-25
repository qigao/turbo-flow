# Flowie MQTT application architecture

## Decision

The configured Flowie broker is a TurboFlow application assembled from a product provider
registry, resolved YAML, a `.flow` Graph, and the Flowie protocol/session owner. The protocol
library alone is not an application, and an embedded Flowie endpoint is only one reusable
component until its host supplies the complete topology. The normative terminology and the three
composition forms are defined in
[`CONFIGURED_BROKER_CONCEPTS.md`](CONFIGURED_BROKER_CONCEPTS.md).

Flowie does not embed or link the TurboMQTT client, broker, socket, queue, worker, processor,
sink, or plugin runtime. The server application and
the independent `flowie_client` SDK share only the Flowie protocol module; the client SDK builds
its own single-owner CoroNet transport state. The protocol knowledge migrated from TurboMQTT
remains deterministic and free of I/O:

- MQTT packet framing and fundamental constants;
- UTF-8, Topic Name, Topic Filter, and shared-subscription parsing;
- re2c/Lemon parser structure;
- legacy MQTT ACL text parsing, but not the ACL plugin or its lifecycle.

The initial source reference is TurboMQTT commit
`bb589e1812c66707fec32e2dccce0543121c54fc`. Every migrated public symbol is renamed to the
`flowie_mqtt_` namespace and is owned by this repository. Flowie behavior is verified against
the MQTT specification; the old implementation is a source reference, not a compatibility
authority.

## Alternatives

1. Link the existing TurboMQTT runtime. Rejected because its I/O, queues, processors, sinks,
   plugin lifecycle, and state ownership would bypass TurboFlow primitives.
2. Copy the complete TurboMQTT tree. Rejected because it would create two owners for sessions,
   subscriptions, inflight QoS, transport, and persistence.
3. Selectively migrate protocol-only code and rebuild the runtime from primitives. Chosen
   because parser state remains local while each mutable business state has one explicit owner.

## Layers and ownership

Flowie endpoint is itself a TurboFlow adapter primitive, in the same architectural position as
the FlowMQ endpoint runtime. It owns its CoroNet listener and accepted connection lanes; it does not depend on or
compose a generic `io/socket` adapter. Reusable code below this boundary is limited to the
protocol-neutral CoroNet execution/runtime and connection snapshot helpers in `io/common`.

MQTT business facts have one source of truth: the FlowStore MQTT fact facade, assembled around the
Record service by the StorageBackend registry. Session, subscription, inflight, retained, and Will mutations commit to
that service before Flowie swaps its owner/cache state. The in-process vectors, maps, and topic
trie are rebuildable indexes and scheduling caches only; they must never advance independently or
serve as a fallback fact source. Flowie does not call backend callbacks after facade construction.
With no explicit `session_store` channel, the composition root binds the volatile Record service
from the `tf_local_storage` shared library. Redis/PostgreSQL are selected only by an explicit
storage channel and are never silently substituted.

Endpoint registration installs the `protocol.mqtt.server` module catalog. The graph-visible
operations are `mqtt.publish.ingress` for an admitted application PUBLISH and
`mqtt.packet.egress` for an encoded routed packet. They bind to the existing endpoint owner;
CONNECT, SUBSCRIBE, UNSUBSCRIBE, PING, AUTH, QoS transitions, retained state, and session
persistence remain internal owner behavior rather than invented graph operations.

| Layer | Owner | Mutable state | Forbidden dependencies |
|---|---|---|---|
| `flowie_protocol` | caller-owned parse invocation | none; results borrow input bytes | CoroNet, sockets, queues, graph runtime, storage |
| `flowie_client` | caller coroutine or DLL-owned CoroNet worker | one outbound transport, framing, packet IDs, inbound QoS 2 state, bounded async command queue | Flowie endpoint, graph runtime, server session or persistence state |
| endpoint resource | Flowie endpoint owner | listener lifecycle, limits, connection registry | graph mutation of connection/session state |
| connection resource | CoroNet-bound Flowie owner | transport handle, receive buffer, negotiated version | MQTT parser performing reads or writes |
| session owner (internal) | Flowie session owner | bounded cache reconstructed from session facts | independent public resource identity or queue/sink state advancement |
| subscription index | Flowie subscription owner | rebuildable filter/member query index | graph-owned subscriber membership |
| application graph | TurboFlow | private message attempt and settlement result | direct MQTT ACK, reconnect, or socket access |
| persistence resource | selected FlowStore MQTT facade assembled by StorageBackend registry | session/subscription/inflight/retained/Will facts | protocol-specific fallback or a second in-memory fact source |

Parser output is a zero-copy borrowed view. The connection owner must either finish all use
before the receive buffer changes or copy selected fields into its own bounded session/message
storage.

The client SDK has two explicit execution modes. A non-NULL client `context` preserves the
caller-owned coroutine API. A V2 config with `context == NULL` creates a private persistent
CoroNet context and one DLL worker thread. Managed operations deep-copy their packet descriptions
into a queue bounded by both command count and owned bytes. Only the worker mutates the MQTT
state machine or socket; cross-thread submissions use `coro_post()` to wake that owner. Completion,
inbound PUBLISH, and unsolicited-disconnect callbacks run on the worker and may enqueue more async
commands, but may not perform synchronous client I/O or destroy the client. Destruction marks the
queue closed, interrupts pending I/O, completes accepted queued commands with `TURBO_ESHUTDOWN`,
joins the worker, and only then frees the context and client state.

Stream transport and graph execution are separate bounded paths:

- connection lane: socket recv -> framing/parser -> complete MQTT packet;
- graph admission: owned packet -> configured TurboFlow source -> inline/worker stage.
- reply admission: encoded control packet + message-owned route -> bounded endpoint Queue ->
  owner-lane route lookup -> CoroNet send.

When `manage_sessions` is enabled, CONNECT is the protocol-owner exception to application
publication. The endpoint's private session primitive consumes the complete CONNECT on the same
CoroNet lane, authenticates and authorizes it when an explicit security binding is installed,
opens or resumes the bounded session, atomically rebinds
the provisional connection route to the session generation, and enqueues CONNACK. Rejected and
accepted CONNECT packets do not enter the application graph. SUBSCRIBE, UNSUBSCRIBE, PUBREL,
PINGREQ, DISCONNECT, and enhanced AUTH are also consumed by that protocol owner; only admitted
PUBLISH packets enter the application graph.
When `manage_sessions` is disabled, the endpoint preserves external-session mode and publishes
CONNECT for a separately assembled session processor.

Each MQTT connection has exactly one CoroNet-lane owner for socket receive, framing, parser,
negotiated protocol state, close, and backpressure. Raw stream bytes never cross a lane boundary.
The first client packet must be a valid CONNECT. Its protocol level fixes MQTT 3.1 level 3,
MQTT 3.1.1 level 4, or MQTT 5 level 5 for all later packets on that connection; a non-CONNECT
first packet or a second CONNECT is a terminal protocol error. The CONNECT protocol-name/level
pair is exact: `MQIsdp`/3 or `MQTT`/4|5.
Connections using all three supported levels may coexist in one endpoint. Fan-out is encoded for
the subscriber session's negotiated level: MQTT 5 properties are forwarded only between MQTT 5
sessions, are removed for MQTT 3.x subscribers, and an MQTT 3.x publication becomes a valid MQTT 5
PUBLISH with an empty property block for an MQTT 5 subscriber.

Level 3 is a compatibility dialect, not a relaxation of the parser's security boundary. It keeps
strict UTF-8 validation and the legacy 1--23 byte Client Identifier limit. Because level 3 has
neither CONNACK Session Present nor a negative SUBACK code, Flowie keeps resumed-session state
internally but emits a zero CONNACK flag, and closes on subscription ACL/capacity rejection instead
of reporting false success. MQTT-over-WS was standardized after MQTT 3.1, so accepting level 3 over
WS/WSS is an explicit Flowie transport extension. Protocol differences are tracked against the
[MQTT.org 3.1/3.1.1 comparison](https://github.com/mqtt/mqtt.org/wiki/Differences-between-3.1.0-and-3.1.1)
and the [OASIS MQTT 3.1.1 specification](https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/mqtt-v3.1.1.html).
The tested Flowie WS/WSS path uses the WebSocket subprotocol token `mqtt` for MQTT levels 3, 4,
and 5, with the CONNECT protocol-name/level pair remaining the version fact source. Some legacy
MQTT 3.1 clients instead require `Sec-WebSocket-Protocol: mqttv3.1`; Flowie does not currently
claim or test that token. This token difference does not affect MQTT 3.1 clients using TCP, TLS,
or Pipe.
The public callback client accepts an optional verified TLS identity for TLS and WSS plus
MQTT 5 Enhanced AUTH challenge and re-authentication callbacks. CA, certificate, key, and password
strings are copied at creation; certificate and key are an atomic pair, peer verification cannot
be disabled, and the copied key password is wiped at destruction. Only the current complete client
configuration layout is accepted.
The framing buffer owns incomplete bytes only; it is neither a durable Queue nor an ACK fact
source. Once parsing identifies a complete PUBLISH packet, Flowie materializes the complete wire
packet in a reference-counted `mem_buffer_t` carried by `turbo_flow_msg_t`, consumes the framing
bytes, and performs one graph publication to the configured source. Managed CONNECT is instead
consumed by the same-lane session primitive described above. Only a complete owned message may
cross an execution boundary. The graph plan, not Flowie ingress, selects direct execution or a
bounded worker Disruptor. A graph admission failure is returned to the connection/session owner
and is never retried implicitly by the framer. Protocol, HWM, and graph errors put that
connection ingress into a terminal state; its owner must close or rebuild the connection before
accepting more bytes.

## MQTT broker processing stages

Flowie separates the MQTT protocol owner from the application message graph. The owner is the
only authority for connection, authentication, session and QoS state; the graph is a programmable
pipeline for messages that have already crossed that security boundary.

```text
socket/frame
  -> parse, size and protocol validation
  -> CONNECT authentication and operation ACL
  -> session/inflight admission (owner lane)
  -> turbo_flow_msg_t (owned buffer + MQTT metadata)
  -> optional TurboFlow Policy filter/route/transform
  -> optional store/durable boundary
  -> optional after-process stages
  -> MQTT fan-out / HTTP / socket / Redis sinks
  -> settlement result returned to the session owner
  -> protocol ACK when the configured prerequisite is complete
```

The first three steps are mandatory owner work; authentication and ACL are mandatory whenever a
security binding is configured, and the graph can never bypass that binding. A message that fails
authentication, ACL, protocol validation, inflight limits, or packet-size limits never reaches a
user graph. A graph stage may reject or transform an admitted PUBLISH, but it may not emit CONNACK,
PUBACK, PUBREC, SUBACK, or otherwise mutate a session generation. TurboFlow Policy facts are evaluated over the
versioned MQTT metadata and payload view already attached to `turbo_flow_msg_t`; it does not read
the connection receive buffer or a live socket.

TurboFlow can process any provider message represented by `turbo_flow_msg_t`. The current Flowie
endpoint contract exposes only `mqtt.publish.ingress` and `mqtt.packet.egress` to the graph;
CONNECT, AUTH, SUBSCRIBE and other control packets remain owner-internal. Exposing a future
normalized control-event source would require a separate protocol contract for authorization,
session mutation and ACK ownership; it is not implied by adding a generic Policy stage.

The remaining stages are composition points, not mandatory broker behavior:

- **accepted** is completed explicitly by the selected graph admission stage; it is not tied to a
  hidden queue;
- **store** commits a selected durable record (for example Redis Stream or a record-store
  adapter) and is the `durable` settlement point;
- **after-process** is an optional post-routing side effect or audit stage. Its failure is
  part of the synchronous graph result when it is connected to the selected path.

The graph may place `store` before Policy when the durable fact must be the original admitted
packet, or after Policy when the durable fact must be the transformed/filtered message. These
are different topologies and must be selected explicitly in `.flow`; Flowie does not infer or
silently reorder them. A branch that is not part of the configured settlement prerequisite is
not created implicitly. To keep after-process work out of ACK latency, the graph must first commit
an explicit accepted/durable handoff and run that work from its separately consumed path;
the original synchronous graph cannot silently ignore a selected branch failure.

## Pattern composition

Flowie and FMQ share protocol-neutral pattern vocabulary and algorithms, not the FMQ v3 wire
codec. Flowie currently uses the core candidate iterator for ordinary/shared subscription
selection and its generation-fenced route matcher for live connection lookup:

- ordinary MQTT subscription: PUB/SUB fan-out over the subscription index;
- shared subscription: PUSH/PULL selection within one share group;
- MQTT 5 Response Topic and Correlation Data: an MQTT-owned correlation use case, not currently
  bound to the core synchronous exchange state;
- connection and session addressing: ROUTER/DEALER-style logical routes;
- delayed graph completion: settlement owned by Flowie, after which Flowie emits the MQTT ACK.

The reusable core owns selection, route matching and synchronous correlation state, but it does
not own an MQTT socket or emit an MQTT packet. FMQ and Flowie adapters translate their own
wire-level identity into the applicable protocol-neutral contracts; unused core mechanisms do
not imply a Flowie feature.

## Two ACK classes

Transport/protocol ACK and application settlement are separate facts:

1. **Protocol ACK** is emitted only by the Flowie session owner. It advances MQTT QoS state and
   is governed by the configured received/accepted/processed/durable settlement point.
2. **Storage/application ACK** is returned by a selected local, Redis, PostgreSQL, or sink
   primitive after its own write contract succeeds. It never writes directly to the client.

For a durable policy, successful graph processing without successful durable commit is not an
ACK. For an accepted policy, ownership transfer into the configured bounded primitive is the
commit point. No implicit fallback between policies is allowed.

## Configuration contract

Human configuration is YAML and uses the `.yml` suffix. JSON may exist only as an internal
resolved projection. The target composition is illustrated by
[`examples/flowie.yml`](examples/flowie.yml). The `adapters.mqtt.endpoint` entry has a strict
typed projection for its transport, bounded reply Queue, and optional managed-session fields.
The process-level `runtime.ingress` entry configures the single Flow-owned bounded asynchronous
handoff used by timer and I/O producers; it is not an adapter, profile, or second graph ingress.
Unknown fields and wrong types fail before registration. The profile selects the MQTT endpoint
and Queue sink/source; it selects a `rule_set` channel resource only when the Graph uses
`rules.apply`. Business data sources and sinks are explicit
Graph adapter references; they are not inferred from a single profile `output`. The RuleSet
owns its stable identity, evaluation mode, quotas, and ordered `{when, action}` entries; the graph
binds it through `operation rules.apply resource <channel-name>`. Security remains explicit target
composition until its host binding is complete. Session and retained persistence share the explicit
record-store binding but remain different versioned record kinds.
The settlement-, retained-, slow-subscriber-, coroutine-capacity-, and receive-buffer-aware endpoint
configuration is the only public endpoint layout. Local endpoint/client ABI version numbers are not
part of the API; MQTT 3.1/3.1.1/5 remain the protocol version facts.
TurboFlow graph topology remains DSL; `flowie.yml` does not invent an unsupported `flows` root.

### MQTT 5 managed-session contract

The managed endpoint negotiates Receive Maximum and Maximum Packet Size in both directions, applies the client
Receive Maximum as the broker-owned QoS 1/2 send window, and derives idle receive timeout from
1.5 times the CONNECT Keep Alive without exceeding an explicitly tighter endpoint timeout. An empty
clean-start Client Identifier is replaced by a UUIDv7-based identifier returned in CONNACK; an empty
non-clean identifier is rejected. `topic_alias_maximum` bounds a connection-owner alias table. Alias
resolution occurs before ACL evaluation, session admission, and graph publication, so downstream
code never authorizes an empty or stale topic.

Subscription Identifier is owned by each subscription and deduplicated across overlapping matches
for one delivery. It is appended to the outbound MQTT 5 PUBLISH while inbound Subscription
Identifier properties are not forwarded. Canonical session records now write FSES 1.3. FSES 1.2
added the bounded identifier to the subscription options record; FSES 1.3 extends each outbound
delivery metadata record with its absolute Message Expiry epoch deadline. Restore accepts FSES
1.0-1.3: FSES 1.0/1.1 restore Subscription Identifier as zero, and FSES 1.0-1.2 restore without a
broker delivery-expiry deadline because those layouts did not preserve that fact.

The session owner prunes expired PUBLISH deliveries before reconnect replay and the connection
owner checks the same deadline again before a queued socket send. A non-expired MQTT 5 replay
derives its remaining Message Expiry Interval from the absolute deadline and rewrites only the
fixed-width four-byte property value. The absolute deadline remains the fact source; the wire value
is a derived view. MQTT 3 outbound deliveries retain the same broker deadline but have no expiry
property to rewrite. Once QoS 2 advances to `WAIT_PUBCOMP`, the stored packet is PUBREL rather than
PUBLISH and its delivery expiry is cleared. New writers never downgrade records. An older FSES 1.2
reader cannot consume FSES 1.3 records, so rollback requires draining/removing 1.3 session records or
using a rollback binary that also understands FSES 1.3.

### Settlement compatibility, migration, and rollback

`FLOWIE_ENDPOINT_CONFIG_INIT` and an omitted YAML settlement field retain the legacy `received`
behavior. The receive boundary commits the session transition and admits its ACK to the
connection-owned bounded reply Queue before graph publication. Socket scheduling may overlap graph
execution, but graph success is not an ACK prerequisite: if publication terminates with an error,
the endpoint drains that already committed ACK and then closes the connection. This exception is
limited to protocol replies whose owner transition already committed; it does not turn graph
failure into success or keep the connection accepting new work.

| YAML value | ACK prerequisite | Required graph boundary |
| --- | --- | --- |
| omitted or `received` | Session owner receive transition | None; legacy-compatible default |
| `accepted` | Explicit graph ownership-transfer commit | Selected admission stage completes the settlement envelope |
| `processed` | Successful synchronous graph publication | All configured inline/worker stages complete |
| `durable` | Successful persistent commit | PostgreSQL COMMIT or Redis Stream XADD acknowledgement |

QoS 0 remains receive-settled and has no MQTT ACK. There is no implicit promotion, downgrade, or
fallback between the four policies. Moving away from `received` changes ACK latency and the
failure/redelivery window, so deployment must quiesce ingress, drain or explicitly disposition
inflight work, stop the endpoint, change the YAML policy and graph boundary together, validate the
resolved profile, and then restart it. These fields are not hot-reloaded. Rollback uses the same
sequence and explicitly restores `received` (or removes both settlement fields); rolling back only
the graph while leaving `accepted` or `durable` configured fails instead of silently acknowledging
at another boundary. Endpoint config ABI v8 is the public layout for this contract.

## Worker process topology

The product host has two process-level roles. `flowie_server` is the Worker/data-plane owner: one
invocation resolves exactly one YAML configuration, one profile, and one graph, then owns that
configuration generation until shutdown. `flowie_supervisor` is an optional control-plane owner
that starts exactly one Worker through `turbo_process_t`. It passes arguments directly without a
shell, sets no execution deadline for the long-running Worker, owns the child process tree, and
terminates/reaps it on Supervisor shutdown. The Supervisor does not access Flow, Queue, CoroNet,
session, or storage state and does not automatically restart a failed Worker.

Worker stdout/stderr inherit the Supervisor streams by default, so normal long-running logs do not
accumulate in Supervisor memory. `--capture-output BYTES` is an explicit bounded diagnostic mode;
the byte limit is cumulative for the Worker lifetime and exceeding it terminates the Worker. This
mode is therefore intended for configuration checks and controlled runs, not as a log transport.
An explicit restart/backoff policy may be added later only as configuration-owned deployment
behavior; it must not be inferred from a child failure.

Configuration creates resources and binds them; it does not instantiate hidden MQTT queues or
processors. A Flowie endpoint owns its shared CoroNet socket binding, framing, and parser on one
lane, and declares the TurboFlow source that receives complete owned MQTT packets. A graph
declares protocol/session processors, worker capacity, and sinks. Persistence is an injected
resource selected explicitly per state class.

Security uses the same composition rule. YAML selects `security_realm`, its exact `policy_source`,
and `auth_method`; it does not contain ACL rules or a policy version. The host creates the selected
HTTPS or SQLite ACL provider, binds its borrowed interface to the realm, registers the realm as a
resource, and injects the typed realm plus HTTPS authentication provider through
`flowie_endpoint_security_binding_t`. The endpoint copies only the validated principal into session
state and never stores password/authentication bytes. Version mismatch or expiry triggers a bounded
provider refresh; normal authorization executes only against a refcounted immutable local snapshot.
The binding's channels and method must equal YAML exactly, so stale or miswired composition fails
before registration. There is no realm pointer lookup, service locator, plaintext user database,
YAML policy body, provider fallback, or implicit anonymous fallback. The complete control/data-plane
decision is documented in `ADR_DYNAMIC_ACL_BUNDLE.md`.

## Failure and rollback

- malformed protocol input fails at the connection boundary and does not enter the graph;
- authorization denial does not allocate session/application work;
- a graph or persistence failure leaves the session inflight record at its previous committed
  generation and follows the configured retry/disconnect policy;
- migration can be rolled back by disabling `TURBO_FLOW_BUILD_FLOWIE`; FMQ and other adapters do
  not depend on Flowie;
- no TurboMQTT runtime file is modified, so source migration can be audited or repeated without
  changing the original application.

## Verification gates

1. protocol framing, fragmentation, malformed input, limits, UTF-8, topic, shared-filter, and ACL
   parser tests;
2. endpoint/session tests with synthetic primitive completions and no network;
3. real CoroNet TCP/Pipe/WebSocket protocol integration tests;
4. Redis/PostgreSQL durable reconnect and duplicate-delivery tests;
5. FMQ release regression and full TurboFlow CTest.

## Current implementation boundary

The packet envelope, CONNECT/PUBLISH/SUBSCRIBE/UNSUBSCRIBE typed decoders, bounded
CONNACK/PUBACK/PUBREC/PUBREL/PUBCOMP/SUBACK/UNSUBACK/PINGRESP/DISCONNECT/AUTH codec,
MQTT 5 property iterator,
UTF-8/topic/shared-filter/ACL parsers, PUBLISH protocol/data bridge, and SecurityRealm MQTT
matcher are implemented with focused tests. The bidirectional `flowie_endpoint` adapter now owns
TCP/TLS/WS/WSS/Pipe listener startup, accepted connections, same-lane receive/framing/parser
state, connection limits, stop/drain, and Connection, QueueBuffer, plus ProtocolAggregate
resource snapshots. Incomplete receive bytes remain private connection state and are not a Queue.
The QueueBuffer resource accounts for the real bounded outbound path. Cross-thread commands first
enter one endpoint owner-lane queue. After route and protocol validation they move, without losing
their aggregate reservation, into the target connection's TurboUtils deque. `send_hwm_bytes`
bounds each connection's pending wire bytes; the endpoint aggregate byte capacity is the checked
product of that limit and `max_connections`.
Shared bounded stream primitives provide a
demand-grown, single-owner framing accumulator. A private Flowie connection ingress handles
fragmented/sticky MQTT packets on the connection lane, transfers each complete wire packet to
owned message storage, attaches a process-local `{owner_instance_id, session_id, generation}`
route, and publishes each application packet once to the configured TurboFlow source. In
external-session mode CONNECT follows that path; in managed mode it is consumed before graph
admission. The route survives owned
message clone/worker boundaries but is never durable. Reply stages emit encoded control packets
to the same endpoint adapter; an owner-lane TurboUtils hash map resolves the route in O(1), and
the connection handler does not leave CoroNet ownership while an active send can still use its
socket. Focused tests cover direct and worker-Disruptor graph plans. A private,
single-CoroNet-lane session owner now
copies client identity, fences reconnects by generation, applies complete SUBSCRIBE and
UNSUBSCRIBE packets atomically, bounds inbound inflight records, and can advance QoS 1/2 only
after a selected received/accepted/processed/durable settlement. It produces
PUBACK/PUBREC/PUBCOMP intents but performs no
I/O; the intent now maps directly to the pure control packet encoder. Persistent QoS 2 release
state survives reconnect while unsettled graph attempts are discarded for explicit redelivery.
The session owner also produces one deterministic CONNECT decision containing acceptance,
session-present, generation-fenced route, CONNACK command, and close-after-rejection policy.
With explicit `manage_sessions: true`, the endpoint owns a bounded client-id hash index of these
session owners. Focused real-TCP tests cover persistent reconnect, session-present,
active-client duplicate rejection, route rebinding, atomic SUBACK/UNSUBACK, PINGRESP,
QoS1 PUBACK, QoS2 PUBREC/PUBCOMP, duplicate suppression, unknown PUBREL, enhanced AUTH/re-auth,
password-provider CONNECT authentication, CONNECT/PUBLISH/SUBSCRIBE authorization denial,
close-after-reply, wildcard/no-local fan-out, shared-subscription round robin, unsubscribe
withdrawal, broker-owned outbound packet IDs, and QoS 2 subscriber reconnect/replay. Only admitted
owned PUBLISH packets cross into the configured worker
Disruptor stage. ProtocolAggregate is the public management resource for MQTT session state. Its
owner-native Status schema v2 reports transport, session load/capacity, retained count/capacity, persistence/security
binding state, lifecycle, and admission state without exposing client IDs, topics, credentials, or
payload. QUIESCE and RESUME execute on the CoroNet owner lane. QUIESCE keeps the OS listener bound
and preserves established MQTT connections/sessions, but closes MQTT admission for newly accepted
connections; RESUME reopens that admission. A command changes generation exactly once, repeated
idempotency keys replay the prior result, and stale generation/deadline/concurrent commands fail
before state mutation. Individual session owners are intentionally not registered as `Connection`
resources because they are protocol state, not transport connections.

The same bidirectional endpoint adapter can be named as an explicit graph sink. For a complete
PUBLISH it submits one bounded fan-out command; the graph lane never reads mutable session state.
After a session-owner commit, the CoroNet owner lane incrementally updates a derived subscription
selector, matches each unique filter, merges overlapping ordinary subscriptions to one delivery per
session, and selects one active member per exact shared filter with an independent round-robin
cursor. Startup, clean-start replacement, and an explicitly invalid selector use an atomic full
repair from session owners. Session owners remain the only subscription fact source; any incremental
update failure invalidates the derived state, and the next fan-out must complete that repair before
using membership. A failed repair fails fan-out instead of reading a partial selector. No match is a
successful MQTT publish, not `TURBO_ENOTCONN`.

Candidate iteration and live-route comparison use the protocol-neutral pattern core in
`turbo_flow_protocol`; Flowie does not include an FMQ frame, peer type, or wire enum. The MQTT topic
trie, wildcard rules, shared groups, CRoaring membership, `no_local`, QoS and session generation
remain Flowie-owned inputs to that core. The core therefore cannot mutate subscription truth or
turn MQTT no-match semantics into FMQ's `TURBO_ENOTCONN` behavior.

MQTT Will state is owned by the same session owner. CONNECT deep-copies the Will topic, payload,
properties, QoS, retain bit, and delay into the bounded session record. A normal DISCONNECT reason
suppresses it; abnormal transport close and DISCONNECT `0x04` make it pending. The owner schedules
publication at the Will Delay boundary or session end, whichever is earlier, and a reconnect with
the same client id cancels a still-pending Will before fan-out. The generated owned MQTT PUBLISH
enters the configured TurboFlow graph with a pointer-free internal flag and route token; an endpoint
sink transfers the graph-transformed packet into the existing bounded owner command queue before
the durable Will record is cleared. Redis and PostgreSQL use the same canonical session record, so
restart restores the remaining absolute delay/expiry boundary. A failed graph or store operation
keeps the Will pending and schedules a bounded retry. This provides durable at-least-once recovery,
not exactly-once publication: a crash after owner-command admission but before the record clear can
repeat the Will after restart.

Each selected delivery is re-encoded for the subscriber MQTT version. Its QoS is
`min(inbound QoS, granted QoS)`; `no_local` and retain-as-published are applied before admission.
MQTT 5 Topic Alias and inbound Subscription Identifier properties are not forwarded because both
belong to a connection/subscription context. The endpoint keeps a bounded per-connection Topic
Alias map: a PUBLISH carrying both a topic and alias updates that map, while a later alias-only
PUBLISH is normalized to a concrete-topic packet before graph publication. Unknown, zero, or
out-of-range aliases close MQTT 5 with Topic Alias invalid (`0x94`). QoS 1/2 deliveries use
broker-owned packet IDs and owner-held outbound inflight records. PUBACK removes QoS 1 state;
PUBREC emits PUBREL; PUBCOMP completes QoS 2. A persistent reconnect replays pending PUBLISH with
DUP set or the pending PUBREL after CONNACK.

Fan-out prebuilds all target packets and atomically reserves their endpoint aggregate budget before
peer admission. Each accepted packet then enters the matching connection's own bounded queue and
same-connection FIFO drain. A subscriber that exhausts its pending-send HWM or MQTT outbound
inflight quota is disconnected and its unsent queue is released; other matching subscribers keep
their deliveries and the publisher is not failed for that peer-local condition. Aggregate/resource
admission failure still fails the fan-out command so QoS 1/2 can redeliver; durable subscriber
inflight state remains eligible for reconnect replay. Endpoint config ABI v8 and strict YAML expose
this fixed behavior as `slow_subscriber_policy: disconnect`; zero in the C API selects the same
default and any other value fails registration. Queue Status schema v2 keeps the aggregate
`load/capacity` view and additionally reports `connection_hwm_bytes`, the stable policy enum, and a
saturating `slow_subscriber_disconnects` counter. A peer-local overflow increments that counter but
does not mark the endpoint aggregate saturated or stop admission for healthy connections.
The selector uses a topic-level trie for exact, `+`, and `#` candidate pruning. Typed member
metadata stays in a hash index, while a derived CRoaring64 set accelerates integer membership and
rank queries; neither structure owns MQTT payload or session facts. A filter hash resolves
the stable entry slot in expected O(1); entry and member mutations update only that filter. Trie
terminal bindings support O(1) bucket removal, own exact-level tokens, prune empty branches, and
reuse inactive entry slots, so subscribe/unsubscribe churn does not retain removed filter storage.
CRoaring deduplicates overlapping ordinary matches and selects shared members by rank without
copying a candidate array. Unrelated mutations preserve each shared filter's cursor. The trie also
enforces the MQTT `$SYS` root-wildcard boundary before the defensive protocol matcher.
The internal capacity benchmark holds 100k session owners concurrently and builds/matches a
100k-filter derived trie containing exact, `+`, `#`, and shared filters. A second workload performs
eight complete 100k wildcard/shared rebuilds and 256 matches returning exactly 100k candidates each.
The live-TCP workload adds one publisher plus 100,000 real MQTT 5 subscribers, verifies the
Connection and ProtocolAggregate counts, and byte-compares one complete QoS 0 delivery at every
subscriber. The exact Windows/MSVC Release run on 2026-07-16 completed with
`connected=100000`, `delivered=100000`, and `status=0`; this closes the functional deployment-capacity
gate, not a portable connection-rate or latency SLA.

The endpoint derives the private CoroNet object-pool capacity from `max_connections` plus fixed
headroom and exposes `coroutine_stack_size` and `stream_recv_buffer_bytes` for that private context.
Explicit values on borrowed, transferred-owned, or pool-lane contexts fail with `TURBO_ENOTSUP`
because their host owns capacity. Flowie defaults each of CoroNet's two ping-pong receive chunks to
4 KiB; MQTT framing reassembles packets across chunks up to `max_packet_size`. Before this change,
two fixed 128 KiB receive chunks plus a 32 KiB coroutine stack accounted for about 288 KiB of fixed
per-connection capacity. The private-context minimum stack is 64 KiB for managed
security and MQTT 5 AUTH state, so 4 KiB receive chunks now account for about 72 KiB
(`2 * 4 KiB + 64 KiB`). Explicit 32 KiB configurations must migrate to at least 64 KiB. The 100k
run peaked near 5.93 GiB private commit on the reference host; the
50k run peaked near 3.07 GiB. Those live-TCP setup timings predate incremental selector mutation and
combine serial TCP/MQTT handshakes, owner creation, subscription commits, and test-driver overhead;
they are functional capacity evidence, not a current connection- or subscription-rate benchmark.

One owner-lane dispatcher preserves command order up to route admission; it never waits for socket
I/O. Each connection coroutine owns both receive/framing and its bounded FIFO send drain. Enqueue
interrupts a pending receive wait, after which that same owner drains queued packets before arming
receive again. A TCP drain combines 2-64 already-admitted replies into one scatter/gather send;
single replies and non-TCP transports retain the existing one-packet send path. A terminal reply is
always the final vector item, and queue budgets are released only after the send completes. Fan-out
therefore does not transiently allocate a second coroutine per recipient.
Different socket owners can still make progress independently, while one connection keeps strict
FIFO and one mutable transport owner. Shutdown closes admission, interrupts socket waits, releases
both global and connection budgets, and waits for connection owners before destroying state. Real
TCP tests cover peer-local HWM
closure, outbound-inflight exhaustion, continued delivery to a healthy subscriber, QoS replay, and
stop/drain cleanup. A Release benchmark additionally forces a 1 KiB receive-buffer subscriber to
stop reading 512 KiB packets over repeated connection cycles; all healthy deliveries complete and
only the stalled connection is removed. Its latency remains a platform-specific capacity reference,
not a portable SLA.

The endpoint owner lane also owns a bounded exact-topic retained table. A retained PUBLISH replaces
the previous packet only after the complete graph command reaches the configured Flowie fan-out
sink; a zero-length payload removes it, and an expired MQTT 5 Message Expiry Interval removes it
before subscription replay. SUBSCRIBE captures whether each filter existed before the atomic
session mutation, sends SUBACK first, then applies Retain Handling 0/1/2. Shared subscriptions never
receive retained replay. Replayed QoS 1/2 packets use the same broker packet-id allocator, session
delivery state, durable session CAS, and bounded reply Queue as live fan-out. Retained capacity is
independently bounded by `max_retained_messages` (zero in the C API selects `max_sessions`). When a
`session_store` is bound, retained PUT/replace/delete crosses the durable record-store CAS boundary
before the owner changes its in-memory fact. A reserved binary key prefix separates canonical
versioned `FRET` records from the existing client-id keyed session records. Startup restores valid
non-expired retained records and CAS-deletes expired records before opening the listener.

Finite MQTT 5 session expiry is scheduled on the same CoroNet owner lane with a reusable wait.
Disconnect records a monotonic deadline fenced by session generation; reconnect clears the old
deadline, and expiry removes the inactive session and its selector memberships on the owner lane.
An already-invalid selector is left invalid for the next atomic repair. A zero interval removes
state immediately, while MQTT 3.1 and MQTT 3.1.1 persistent sessions retain
their unbounded in-process lifetime. MQTT 5 DISCONNECT Session Expiry Interval overrides are
validated by the session owner before close, including the rule that a zero CONNECT interval cannot
be changed to a non-zero value. The live timer remains process-local, while a persistent record
carries its absolute wall-clock expiry so restart cannot turn a finite session into an unbounded
one. A shared `turbo_flow_record_store_t` provides bounded namespace scan,
per-record revision CAS, and atomic batch commit through PostgreSQL transactions or a Redis
Hash/Lua transaction. The internal session codec emits canonical versioned LTV and deliberately excludes
live routes, credentials, reserved outbound identifiers, and unsettled graph attempts. Decode
always creates an inactive owner under the new endpoint instance. The additive
`flowie_endpoint_bindings_t` injects a borrowed store without extending endpoint config ABI;
resolved YAML must name the same channel with `session_store`. Registration scans and validates the
namespace, deletes expired records with revision CAS, rebuilds the trie/bitmap selector, and
advances the local session-id allocator before the listener starts. CONNECT, SUBSCRIBE,
UNSUBSCRIBE, inbound QoS transitions, outbound delivery transitions, disconnect and close use
clone -> durable CAS commit -> owner swap. CONNACK, SUBACK, UNSUBACK and QoS ACK/socket sends occur
only after the relevant commit. Principal identity, roles and groups are encoded field-by-field;
credentials, live routes and unsettled graph attempts are never stored. Provider-neutral fault
tests cover commit/recovery semantics; live backends verify their own restart behavior.

Secure PUBLISH checks one concrete Topic Name before session inflight admission. Secure SUBSCRIBE
checks every requested Topic Filter before the atomic session mutation; adapter-match ACL rules use
filter-language containment, not string matching, so a policy for `root-a/#` does not authorize a
broader `#`. MQTT 5 authentication failure maps to CONNACK `0x86`, authorization failure to `0x87`,
and unauthorized QoS 1/2 publication or subscription returns per-packet `0x87` without graph work.
The MQTT packet parser validates these Topic Name and Topic Filter spans before the endpoint copies
them into its session-owned authorization scratch string. That exact copy carries an internal,
single-call provenance token and length into the immutable matcher, avoiding duplicate UTF-8 and
wildcard scans. The token type is not installed or exposed as public API. Programmatic matcher calls
without this provenance still perform full validation; truncated contexts, unknown resource kinds,
or forged provenance fail with a protocol error instead of falling back to the fast path. Both paths
share the same trie traversal, `$SYS` boundary, filter-containment semantics, rule ordering, and deny
precedence.
MQTT 3.1.1 uses its available CONNACK/SUBACK failures and closes when the protocol has no negative
publish ACK. MQTT 3.1 uses legacy CONNACK authentication failures, but closes on an authorization
or capacity failure that cannot be represented by its successful-only SUBACK/PUBACK vocabulary.
MQTT 5 Authentication Method selects the configured enhanced provider without basic
authentication fallback. Initial challenge exchange uses AUTH `0x18`, returns the final method/data
in CONNACK, and connected re-authentication starts with AUTH `0x19`. Re-authentication may refresh
roles, expiry, and policy version only for the same principal/type/root-group session owner; an owner
change or CONNECT re-authorization failure closes the connection.

These ingress/session owners remain internal and their headers are not installed. The endpoint
now has a tested graph-to-socket control reply path, but it is not yet a complete MQTT broker.
The transport contract runs real MQTT 3.1, MQTT 3.1.1, and MQTT 5 CONNECT, PING, and shutdown
traffic over TCP, TLS, WS, WSS, and Pipe. TLS/WSS use a verified test CA and certificate; a
successful config projection is not accepted as transport evidence.
`flowie_server` is the first product host migrated to the shared product-provider registry. It
resolves one Flowie profile as the allowed product boundary, preflights all adapter kinds before
native resource creation, parses the separate TurboFlow DSL graph, then creates only optional
RuleSet resources, the endpoint, and injected data source/sink adapters referenced by that Graph.
The bundled composition root injects socket and, when built, HTTP, Redis, PostgreSQL outbox, and
PostgreSQL record-store providers. Storage providers are assembled separately: `flowie_server`
creates one `TurboFlow::StorageBackend` registry, registers the three sibling shared-library APIs
(`tf_local_storage`, `tf_redis`, and `tf_pgsql`) when enabled, and loads external modules through the
same versioned `open()/close()` ABI.
Record owner creation, service lookup, and teardown stay in the registry owner lifecycle; Flowie
does not call a backend's concrete record/state/index/log/hash functions. Resource providers run
before adapter providers; repeated references to the same endpoint binding do not create
a second owner. The profile constrains the endpoint plus a RuleSet name
when Policy is configured; the
Graph is the authority for business source/sink names. It then compiles the
Graph and owns start/signal/stop order. `--check` performs the same resolution, resource creation,
provider assembly, and graph compilation without binding the listener.
The current host supports unsecured endpoints and an optional explicit `session_store` record-store
channel backed by Redis or PostgreSQL. When the field is absent, it injects the volatile local
Record backend through the same registry owner path. It creates the selected provider from the same
resolved snapshot, injects the borrowed store through
`flowie_endpoint_bindings_t`, and destroys the endpoint before the store. Provider selection is
strictly driven by `backend`; malformed provider fields, unavailable Redis support, connection
failure, or incompatible stored records fail before the listener starts. `--check` creates and
scans the selected store, then tears it down without starting the listener. The product check
proves host composition; the live Redis/PostgreSQL record-store suites prove
the persistence contracts, including binary retained keys and pending Will recovery. Configured
security bindings and disabled/unknown adapter kinds still fail rather than silently degrading.
Authentication providers are selected from `profiles.<name>.auth_provider` and the referenced
`kind: auth_provider` channel. The worker reads `config.backend`, selects exactly one factory
registered by the product composition root, and fails before listener startup when the backend is
missing, duplicated, unavailable, or malformed. Flowie owns only the generic provider lifecycle
envelope and authentication ABI. The bundled product registers only the `https` authentication
backend. Redis, SQLite, PostgreSQL, and any other credential database remain private implementation
details of that service and cannot be configured through an `auth_provider` channel. The CONNECT
coroutine invokes TurboHTTP directly; DNS, TCP, TLS, send, and receive waits yield on the CoroNet
owner loop instead of blocking its thread. Redirects and retries are disabled, request/response
sizes and timeouts are bounded, the service token is acquired by reference for every request, and
all transport, certificate, status, content-type, version, or principal-validation failures deny
authentication without a database or anonymous fallback. The returned principal is then evaluated
by the local SecurityRealm; publish/subscribe ACL checks never perform an HTTP or database call.
Managed sessions, retained publications, and pending Wills use the implicit local Record fact store
when no explicit session store is selected; that store is process-local and is not restart durable.
See [ADR_HTTPS_AUTH_SERVICE.md](ADR_HTTPS_AUTH_SERVICE.md) for the trust boundary and deployment
requirements.
External-session mode remains available for later composition. A requested
`session_store` never falls back to volatile state, and malformed or incompatible records fail
endpoint registration before the listener starts.

### Internal ACL management plane

`flowie/control` now contains an internal, non-installed management plane. Its SQLite control store
is the single fact source for root-scoped users, credentials, hierarchical groups, roles, ACL draft
rules and audit records. Draft rules are canonical v3 rule lines. A publish command runs under one
`BEGIN IMMEDIATE` transaction, revalidates the complete draft and every referenced principal,
group, role and MQTT adapter filter, then atomically replaces the provider-compatible
`turbo_flow_acl_bundle_v3`/`turbo_flow_acl_rule_v3` generation. Store revision and policy version
advance independently. Failed or stale publishes leave the previous provider snapshot unchanged.

The management service is the only interface used above the store. It enforces the root/actor
identity supplied by a trusted transport adapter and the `viewer`, `user_admin`, `policy_admin` and
`security_admin` permission matrix. Read APIs use bounded keyset pages; no HTTP or Dashboard handler
receives a SQLite handle.

The optional Iris JSON-RPC adapter binds a caller-owned `rpc_context_t` to an explicit app/path and
registers 24 bounded management methods. Introspection, batch and notifications are disabled; body
fields cannot supply root group, actor or audit time. The SSR Dashboard uses the same service,
normal POST/Redirect/GET forms, constant-time CSRF validation, strict security headers and escaped
dynamic output. It has no CDN or client framework dependency.

These targets remain internal and create no executable or listener. A production management
session adapter, login rate limit, HTTPS-only independent `flowie-control` composition root,
bootstrap flow and operational gates are still required before the management paths may be exposed.
The live app must stop before Dashboard/RPC objects are destroyed; the caller-owned RPC context is
destroyed last.

The managed runtime supports explicit `received`, `accepted`, `processed`, and `durable` QoS
settlement policies. `received` advances the session owner before graph admission. `processed` advances it
only after the synchronous TurboFlow publication returns successfully, including configured
worker-stage completion. `accepted` requires the selected graph admission stage to consume the
one-shot settlement envelope explicitly and route the settlement command back to the Flowie owner.
That callback only enters the endpoint's bounded reply command queue; the CoroNet owner lane still
owns session mutation and socket send. An arbitrary successful stage is not an ACCEPTED boundary.
The current endpoint config exposes this typed policy and intentionally rejects every non-current
layout.

`durable` is emitted only by an explicit storage primitive after its commit boundary, including a
PostgreSQL outbox COMMIT or Redis Stream XADD acknowledgement. The sink may use the current live,
generation-fenced route to enqueue the owner settlement command after commit, but that route is
never serialized. PostgreSQL stores the complete packet, message type/flags, and a serializable
protocol origin containing the MQTT version and stable publisher session identity. Its source
reconstructs a route-less message; the current endpoint owner performs subscription selection and
creates current delivery routes. Whole-graph success is not evidence of durable commit, especially
with branching.

The current Flowie composition evaluates Rules directly after endpoint admission. A different
`.flow` may connect a durable store before Rules when the original admitted packet is the fact
source, or after Rules when the transformed message is the fact source; the order is explicit and
not inferred. Ingress stores the negotiated MQTT version and the packet's fixed-header flags in
private message metadata; the complete wire packet remains the
message-owned bytes in the retained buffer. An internal Flowie facts provider uses that metadata and the
existing typed parser to materialize a 15-field bounded schema. The base fields are `mqtt.topic`,
`mqtt.payload`, `mqtt.payload_size`, `mqtt.qos`, `mqtt.retain`, `mqtt.duplicate`, `mqtt.packet_id`,
and `mqtt.version`. MQTT 5 adds nullable `mqtt.payload_format_indicator`,
`mqtt.message_expiry_interval`, `mqtt.content_type`, `mqtt.response_topic`, and binary-safe
`mqtt.correlation_data`; `mqtt.user_property_count` reports the bounded repeatable-property count,
and `mqtt.broker_will` identifies a broker-generated Will. String facts are borrowed
length-delimited views valid only for the current `rules.apply` call. Optional properties use NULL
when absent rather than a fabricated zero or empty value. No parsed projection, bitmap, parser
owner, live connection route, or duplicated payload is persisted. Store replay reparses the
authoritative wire payload and rebuilds the projection. The version bits are Flowie protocol
metadata and must not be overwritten before a later MQTT facts evaluation; current transforms
mutate status instead.

The composition tests exercise multiple concrete graphs. Endpoint admission feeds `rules.apply`,
which mutates private status and routes each MQTT PUBLISH either to the existing Flowie endpoint
fan-out sink or to the existing CoroNet TCP socket sink. Provider-neutral record-store tests exercise
commit faults and recovery independently. The product host resolves a RuleSet
program only when configured, while the Graph explicitly names every CoroNet, HTTP, Redis, or PostgreSQL
source/sink adapter. The host does not infer routes, facts providers, storage destinations, or
unregistered adapter kinds.

The SMB product graph is deliberately two paths: admitted PUBLISH goes directly to the PostgreSQL
outbox sink so QoS 1/2 ACK cannot precede COMMIT; an independent PostgreSQL source replays the
committed packet through Policy and the endpoint fan-out sink, deleting the row only after graph
success. Downstream failure retains the row and stops that source fail fast. This is at-least-once
delivery; consumers still require idempotency where duplicate side effects are not acceptable.

This durable point proves message persistence, not full broker recovery or exactly-once delivery.
Flowie's managed session, subscription, QoS, retained, and pending Will state are restored only when
the host injects the separate session record-store binding. They do not inherit the Queue's message
durability. A crash after message storage commit but before PUBACK can therefore cause the client
to redeliver an already stored PUBLISH. Durable message ACK, durable session state, and retained
or Will storage are distinct facts and must not be inferred from one another.
