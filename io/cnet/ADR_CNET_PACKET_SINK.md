# ADR: CNet tagged packet terminal sink

## Context

UDP admission, native UDP completion, and KCP acknowledgement are different
boundaries. TurboFlow must retain an async-terminal claim until Salts CNet
reports the authoritative terminal for that exact logical send. Treating
`cnet_packet_send()` admission as completion would release message storage too
early and would make KCP delivery claims incorrect.

## Decision

The packet sink owns one CNet packet endpoint and one fixed peer session. Its
configuration is size/versioned and copied at registration. Endpoint observer
fields must be empty; the sink installs the only observers and terminal policy.

Each accepted graph claim moves into a preallocated operation slot. The CNet tag
encodes the slot index and a nonzero generation. A terminal is accepted only
when tag, generation, session, request id, and successful byte count all match.
The matching terminal completes the CFlow IO Actor request exactly once; Actor
delivery then settles the TurboFlow claim and acknowledgement releases the slot.

The Actor bound and terminal-operation bound are the same `send_capacity`.
Actor exhaustion is reported as `SALTS_ENOSPC`. Errors returned by CNet admission
or terminal callbacks are preserved. There is no raw-datagram, synchronous-wait,
or admission-as-terminal fallback.

A lifecycle mutex linearizes submit admission with stop. Poll and stop share a
single-owner lane: stop first closes sink admission, waits for the current poll
owner, and only then closes or destroys the Actor. The request id is established
by the Actor backend or its authoritative completion callback; submit never
writes an id into a slot after publishing that slot to another thread.

Shutdown closes Actor admission first, stops the CNet endpoint so all admitted
native/protocol operations emit terminals, drains Actor delivery and
acknowledgements, then destroys Actor, Executor, and endpoint in that order. A
native timeout is a retry slice rather than permission to abandon a claim.
After CNet reports a concrete stop error, its terminal callbacks are still
drained through the Actor before that error is reported to TurboFlow. Destroy
errors preserve initialized resources so the same stopped binding can be
retried.

This ordering depends on `CNET_STOP_DRAIN_CONTRACT_VERSION >= 1`. TurboFlow
checks that capability while configuring its source tree and while consumers
load the installed package; older Salts packages fail configuration instead of
silently using weaker stop semantics.

## Consequences

- The send and terminal hot paths allocate no memory.
- UDP settles at NativeIO completion; KCP and secure KCP/FEC settle only after
  cumulative peer acknowledgement covers the logical message.
- One sink represents one outbound peer session. Multi-peer routing remains a
  separate adapter/composition concern.
- Generation exhaustion retires a slot and fails fast with `SALTS_ERANGE` rather
  than allowing a stale terminal to alias a newer operation.
- The copied secure-KCP policy is retained for Flow restart and explicitly wiped
  on every registration-failure or final-destroy path.
