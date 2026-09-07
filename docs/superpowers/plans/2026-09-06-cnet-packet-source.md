# CNet Packet Source Owner Implementation Plan

> **Issue:** #23
> **Base:** `feat/issue-22-cnet-listener-source` / PR #24

## Goal

Add a size/versioned opaque `TurboFlow::CNetAdapter` owner around
`cnet_packet_endpoint` for UDP, plain KCP, authenticated KCP, and Reed-Solomon
FEC. CNet remains the only session table; TurboFlow owns only a bounded queue of
complete copied messages and drains it under Graph demand.

## Design invariants

- One serialized caller owns endpoint polling, the manual CFlow scheduler, the
  Graph run, and every public session/send operation.
- Endpoint callbacks never retain borrowed CNet views. Each accepted message is
  copied once into a `mem_buffer_t` that also owns immutable session metadata.
- A pre-reserved CSTL queue has an explicit element limit. Queue full,
  oversize, allocation failure, invalid callback data, counter overflow, and
  message-id exhaustion are terminal; no message is truncated or partially
  published.
- Graph demand controls only queue draining. `cnet_packet_poll()` continues
  independently so ACK, handshake, retransmit, and FEC progress are never
  coupled to downstream demand.
- Secure KCP admission stays inside CNet: an invalid client hello is rejected
  before `on_admit`, session creation, or message publication.
- Message transport context stores a copied generation handle and
  `cnet_packet_session_info`, so identity survives queueing and later slot reuse.

## Task 1: Lock the public contract with failing tests

Files:

- Add `io/cnet/tests/test_cnet_packet_source.c`
- Modify `io/cnet/tests/cnet_stream_source_header_cpp.cpp`
- Modify `io/cnet/tests/CMakeLists.txt`
- Modify `tests/install_consumer/main.c`

Cover C/C++ size/version initialization, invalid owner/config/capacity bounds,
session wrappers, demand gating, metadata ownership, and lifecycle. Configure
and build `test_cnet_packet_source`; confirm the missing API fails compilation
before adding production declarations.

## Task 2: Add the ABI and architecture record

Files:

- Modify `io/cnet/include/turbo_flow_cnet.h`
- Add `io/cnet/ADR_CNET_PACKET_SOURCE.md`

Declare the opaque owner, config, snapshot, owned message context accessor,
open/request/poll/session/send/stop/destroy functions, and exact ownership and
error contracts. This is additive; existing stream/listener ABI remains
unchanged and no legacy alias is introduced.

## Task 3: Implement the bounded owner

Files:

- Add `io/cnet/src/turbo_flow_cnet_packet_source.c`
- Modify `io/cnet/CMakeLists.txt`

Pre-reserve a raw CSTL queue of `turbo_flow_msg_t`, compose the CNet observer,
copy callback views plus session info into message-owned buffers, implement the
CFlow Publisher/waker boundary, and serialize Scheduler/endpoint/Scheduler
progress. Forward session APIs directly to CNet and preserve exact CNet status.
Stop closes the Graph run, drains/destroys the endpoint, clears queued message
ownership, then shuts down the scheduler; timeout leaves a retryable STOPPING
owner.

## Task 4: Exercise real protocol and failure boundaries

Files:

- Expand `io/cnet/tests/test_cnet_packet_source.c`

Use loopback endpoints to cover UDP and plain KCP round trips, stale handle
rejection after a one-slot reuse, queue overflow, oversize receive, malformed
plain-KCP status propagation, matching and mismatched PSKs, and valid/invalid
FEC bounds. Assert that failures publish neither partial payloads nor sessions.

## Task 5: Verify, document, and publish

Run from a Visual Studio x64 developer shell:

```powershell
cmake --fresh --preset win-dev-user
cmake --build --preset win-dev-user --parallel
ctest --preset win-dev-user -R test_cnet_packet_source --output-on-failure
ctest --preset win-dev-user --output-on-failure
```

Also run the configured Release/ASan presets already used by the branch when
available, inspect exports/install-consumer coverage, run `codegraph affected`
for touched files, check the diff for legacy TurboNet/TurboHttp/TurboParser
symbols, commit, push, and open a stacked PR against PR #24's head branch.
