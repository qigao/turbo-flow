# ADR: CHTTP WebSocket Flow Boundary

## Context

WebSocket frames are long-lived bidirectional session traffic, not deferred HTTP
responses. CHTTP callback views are borrowed, while Flow work may cross worker,
Reactive, Actor, and terminal boundaries. CHTTP already owns H1 Upgrade, WSS,
RFC 8441, subprotocol policy, the CNet WebSocket engine, and a bounded
generation-checked server command queue.

## Decision

Add a separate CHTTP `SOURCE|SINK` adapter. On open it captures a
`chttp_server_websocket_session` into a fixed local session slot. Each
text/binary/ping/pong/close callback reserves a fixed frame slot, copies the
payload and versioned event context into one message buffer, and calls
`turbo_flow_publish_async()`. The terminal reads command intent from
`turbo_flow_msg_t.type`, validates the original session slot/generation, and
submits only through `chttp_server_websocket_send_*()`.

CHTTP/CNet automatically perform mandatory pong and peer-close protocol work.
Flow receives those control events for observation and routing; it does not
become the WebSocket parser or protocol state owner.

## Ownership and concurrency

- CHTTP owns listener, TLS, handshake, H2 streams, WebSocket parsing and wire IO.
- The adapter owns fixed session/frame tables, copied event messages, counters,
  and the Flow registration.
- Flow owns accepted message clones until publication completion.
- One mutex protects only adapter state and slot transitions. No CHTTP call,
  Flow callback, allocation, or blocking wait occurs while it is held.
- Local close submission and peer-close observation are separate states. A slot
  is reusable only after peer close is observed and every admitted frame has
  completed, preventing stale generation aliasing.

## Capacity and failure semantics

- `session_capacity`, `frame_capacity`, frame/message/input byte limits,
  CHTTP network command capacity, and Flow async ingress are hard bounds.
- A full session table rejects only that handshake with HTTP 503.
- A full frame table or Flow ingress closes only that session with RFC 6455
  status 1013. Other failures use 1011.
- A full CHTTP command queue returns `SALTS_ENOBUFS`; stale and duplicate
  terminal commands return `SALTS_ENOENT` and `SALTS_EALREADY`.
- No command is retried, rerouted, or converted to a generic socket operation.
- Requested WSS, ALPN, subprotocol, or HTTP/2 failures propagate from CHTTP.

## Shutdown

Flow first closes async publication admission and drains accepted graph work.
The source adapter then stops and destroys CHTTP, which closes sessions and the
listener. Successful registry shutdown detaches the adapter; public destroy
requires no native owner, active session, or in-flight frame.

## Verification

Loopback tests cover H1 text/binary/control frames, duplicate close, stale local
generation, byte-bound validation, fixed session/frame and Flow-ingress
saturation, peer disconnect, stop/drain, WSS with CA/SNI/ALPN and subprotocol
selection, plus two isolated RFC 8441 streams on one H2 connection. The C++
header probe and installed consumer validate the additive ABI.

## Rollback

Remove the additive WebSocket source, declarations, tests, and this ADR. Graphs
using the adapter then fail preflight; no legacy or generic transport path is
selected.
