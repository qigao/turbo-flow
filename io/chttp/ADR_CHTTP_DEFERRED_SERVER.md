# ADR: CHTTP Deferred Server Graph Boundary

## Context

Issue #7 replaces the former synchronous HTTP server boundary with Salts CHTTP.
CHTTP route request views and response builders are callback-borrowed, while a
TurboFlow publication may cross worker, coroutine, Reactive, Actor, and async
terminal boundaries. Passing either callback object through
`turbo_flow_msg_t.transport_context` would therefore create a use-after-return
risk.

CHTTP provides generation-checked deferred responses for HTTP/1.1 and HTTP/2.
The HTTP/2 ownership and cancellation contract was completed by
[`qigao/salts#214`](https://github.com/qigao/salts/issues/214) and is consumed
directly here; TurboFlow does not mirror CHTTP connection or stream state.

## Decision

Add an optional CHTTP server adapter whose single registration can be used by
one `source ... adapter http.server` declaration and one terminal response stage
using the same adapter name. The adapter copies the CHTTP configuration and route
policy at registration. Flow start initializes CHTTP and starts its background
owner after both configured bindings have been validated.

The CHTTP handler performs this ordered handoff:

1. Reserve one fixed request slot and copy method, target, path, headers, route
   parameters, body, peer endpoint, and peer-certificate digest into one owned
   message buffer.
2. Try `turbo_flow_publish_async()`. Rejection releases the slot and sends the
   configured synchronous 429 or 503 response.
3. Only after Flow returns `SALTS_OK`, call
   `chttp_server_response_defer()` inside the handler callback.
4. Attach the deferred handle to the same generation-checked slot. A graph that
   completed before this point leaves its terminal result in the slot; attaching
   the handle performs the pending reply.

The response stage is an async-terminal adapter. It retains one self-contained
response message in the request slot, consumes its Flow claim, and does not reply
yet. The publication completion callback is the sole reply decision point, so a
fan-out error cannot race an early successful branch response. It chooses the
staged graph response only when the complete publication succeeded; otherwise it
uses the configured graph-error response. CHTTP copies the selected body before
reply submission returns.

The same handoff serves HTTP/1.1, cleartext HTTP/2 (h2c), and HTTP/2 negotiated
by TLS ALPN `h2`. CHTTP remains the sole owner of each H2 connection, stream,
and deferred response. TurboFlow stores only the opaque deferred token beside
its matching request-slot generation. A stream-local graph failure therefore
selects that request's configured graph-error response without closing or
rewriting a sibling stream. Requested HTTP/2, TLS, or ALPN setup errors are
returned unchanged; the adapter never starts an H1 listener or synchronous Flow
path as a fallback.

WebSocket Upgrade and RFC 8441 handshakes remain CHTTP responsibilities. Their
frame and captured-session integration is a distinct CHTTP WebSocket Flow
adapter because it has different message, close, and backpressure semantics;
HTTP request slots are never reused for WebSocket frames and the CNet engine is
not exposed across the CHTTP boundary.

## Data and ownership protocol

- **Data unit:** one HTTP request maps to one Flow publication and at most one
  staged response message.
- **Primary fact sources:** CHTTP owns protocol, connection, stream, and deferred
  handle validity. The adapter request slot owns only its correlation generation,
  publication state, opaque handle token, staged response, and terminal status.
- **Lifetime:** CHTTP views end with the handler. The message buffer owns every
  copied request byte. Flow owns an accepted message. The slot owns a retained
  response until CHTTP has copied it.
- **Topology and order:** one configured source must reach one configured
  terminal response stage. A second response for the same slot is rejected.
- **Capacity:** request slots are fixed to CHTTP connection capacity; CHTTP
  bounds headers and bodies; `max_buffered_response_body_bytes` must be explicit
  and at least 17 bytes so every synchronous adapter error fits. It is the sole
  terminal-response limit. Flow async ingress separately bounds queued message
  count and bytes. No queue grows on pressure.
- **Buffered response boundary:** the terminal response must fit one owned Flow
  message and the configured CHTTP buffered-response bound. Streaming response
  sources and WebSocket frames are separate adapters, not an unbounded escape
  path from this contract.
- **Backpressure:** slot exhaustion or Flow `SALTS_ENOSPC` returns the configured
  overload status synchronously and creates no deferred handle.
- **Threading:** CHTTP serializes handlers on its owner thread. Flow completion
  and response-stage calls may be concurrent. One adapter mutex protects only
  slot transitions, counters, lifecycle state, native-owner presence, and bound
  port. Native CHTTP start/stop operations run outside that mutex and commit
  their result under it; no Flow or CHTTP callback runs while held.
- **Close:** Flow closes async ingress and reactive subscriptions, stops
  non-source adapters so terminal sinks settle their claims, and waits for every
  publication to finish. It then asks source adapters such as CHTTP to stop
  listener admission and drain accepted requests. Successful destroy requires
  registry detachment.

## Failure semantics

- Invalid configuration, binding, request view, or slot generation:
  `SALTS_EINVAL`/`SALTS_EPROTO`/`SALTS_ENOENT` without fallback.
- Flow ingress full: configured overload response, default 429.
- Flow unavailable or stopping: configured unavailable response, default 503.
- Graph error, timeout, or cancellation: configured graph-error response,
  default 500.
- Missing or duplicate terminal response: publication fails or receives the
  graph-error response; CHTTP is invoked at most once for a slot.
- Oversized graph response: the async terminal completes with
  `SALTS_EMSGSIZE`; the still-live deferred handle receives the bounded
  graph-error response.
- A nonempty terminal body paired with HTTP 204, 205, or 304 is rejected before
  it reaches CHTTP; the publication receives the configured graph-error response.
- CHTTP deferred generation mismatch: preserve `SALTS_ENOENT`, record the
  invariant failure, and do not attempt another protocol path.
- Any other CHTTP deferred-reply failure is recorded, then the same
  generation-checked handle is canceled through `chttp_server_deferred_cancel()`
  so the owner can release the admitted request. Cancel writes no replacement
  response. The adapter does not retry, synthesize a response, or select another
  transport path.
- CHTTP stop/destroy failure is reported synchronously through the Flow adapter
  stop scope. Flow enters `FAILED`, retains only bindings that did not stop, and
  rejects reset/parse/start until an explicit `turbo_flow_stop()` retry succeeds.
  Concurrent stop calls are rejected while one stop owns the lifecycle.

## Compatibility and migration

The grammar and adapter spelling `http.server` are unchanged. Existing code that
only parses this DSL is unaffected. A runnable server graph adds a terminal stage
bound to the same adapter and registers a size/versioned server configuration.
Status codes remain explicit configuration values. This is an additive C API and
component target; no TurboHttp, TurboNet, or generic socket fallback is present.

The server start remains coupled to `turbo_flow_start()`. A very early connection
accepted before Flow commits its STARTED state receives the configured 503 and is
not deferred. Stop ordering is deterministic and restart recreates the CHTTP
owner from copied configuration.

## Verification

Integration tests use a real loopback CHTTP client and cover request metadata
copying, H1 success, h2c and TLS-ALPN H2 success, H2 sibling-stream isolation,
graph error, timeout/cancellation status propagation,
duplicate response rejection, bounded ingress overload, response-size failure,
deferred allocation failure followed by no-response cancellation, CHTTP-owned
stop/drain ordering, restart, and concurrent lifecycle snapshots.
Header probes and the installed consumer verify the additive public API and
`Salts::CHTTP` dependency.

## Rollback

The implementation is isolated in the optional CHTTP adapter component.
Rollback removes the server source file and additive declarations/tests. Graphs
configured with `http.server` then fail adapter preflight; they never fall back to
the removed legacy stack.
