# CHTTP WebSocket Flow Adapter Implementation Plan

> Issue: [#31](https://github.com/qigao/turbo-flow/issues/31)
>
> Stacked prerequisites: TurboFlow PR #32, Salts PR #229, and Salts PR #233.
>
> Implementation: TurboFlow PR #33.

## Goal

Add one bounded bidirectional Flow adapter for WebSocket server traffic. CHTTP owns HTTP/1.1
Upgrade, WSS/TLS, RFC 8441 Extended CONNECT, route policy, subprotocol selection, and the
generation-checked command queue. TurboFlow owns copied event messages, graph execution,
terminal command intent, lifecycle accounting, and fail-fast backpressure. No CNet engine,
callback-borrowed object, generic socket path, C fallback, or CMake fallback crosses the boundary.

## Contract

- Register one adapter name with `SOURCE|SINK` and one source name.
- Copy each CHTTP text, binary, ping, pong, or close event into a message-owned buffer.
- Store a versioned event context, the captured `chttp_server_websocket_session`, and a
  TurboFlow-local session slot/generation in that buffer.
- Use `turbo_flow_msg_t.type` for the typed terminal command and `status` for a close code.
- Enforce explicit session, in-flight frame, frame-byte, message-byte, and buffered-input limits.
- Treat `SALTS_ENOSPC` from Flow ingress and `SALTS_ENOBUFS` from CHTTP command admission as
  backpressure. Close only the affected WebSocket; never reroute or retry through another stack.
- Let CHTTP/CNet auto-handle peer ping replies and the peer close handshake. A graph chooses
  whether an observed control event reaches this sink or another terminal.
- Keep a local session slot alive until every already-admitted event completes. Retire it only
  after close observation plus zero in-flight events, so peer-disconnect and graph completion can
  race without use-after-free or session aliasing.
- Flow stop closes publication admission first, drains accepted graph work, then the adapter stops
  CHTTP and its listener. Destruction requires registry detachment and no live native owner.

## Tasks

1. [x] Add a failing H1 Upgrade text round-trip test that uses the new public adapter API.
2. [x] Add the public versioned config, event context, lifecycle snapshot, and helper declarations.
3. [x] Implement bounded session/frame slots, copied event publication, terminal command
       admission, subprotocol selection, start/stop/restart, and snapshot logic.
4. [x] Add text, binary, ping, pong, close, stale generation, duplicate close, frame and ingress
       saturation, session capacity, stop/drain, and peer-disconnect tests.
5. [x] Add WSS CA/SNI handshake coverage with the existing TLS fixture.
6. [x] Add RFC 8441 two-sibling isolation coverage over one H2 connection.
7. [x] Update CMake exports, C/C++ header probes, README/ADR, and issue acceptance evidence.
8. [x] Run focused tests, full Debug and Release presets, install/consumer verification, then push
       a stacked PR without merging it.

## Verification

```powershell
cmake --fresh --preset win-dev-user
cmake --build --preset win-dev-user
ctest --preset win-dev-user --output-on-failure
ctest --preset win-dev-user -R ^test_chttp_websocket_adapter$ --repeat until-fail:100 --output-on-failure
cmake --fresh --preset win-release-user
cmake --build --preset win-release-user
ctest --preset win-release-user --output-on-failure
ctest --preset win-release-user -R ^test_chttp_websocket_adapter$ --repeat until-fail:100 --output-on-failure
cmake --build --preset install-win-dev-user
```

Verified on 2026-09-07 with Visual Studio 2022 Developer Command Prompt 17.14.15:

- Debug: focused WebSocket test passed 100 consecutive runs; full suite passed 37/37.
- Release: focused WebSocket test passed 100 consecutive runs; full suite passed 37/37.
- Debug install completed at C:/projects/cpp/external/pkgs/turboflow/debug; the full suites also
  passed the install-consumer test in both configurations.
