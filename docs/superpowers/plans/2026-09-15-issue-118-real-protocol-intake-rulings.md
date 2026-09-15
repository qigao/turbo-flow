# Issue #118 Real Protocol Intake Execution Rulings

Plan: `docs/superpowers/plans/2026-09-15-issue-118-real-protocol-intake.md`
Spec: `docs/superpowers/specs/2026-09-15-issue-118-real-protocol-intake-design.md`

## Ruling 1: IPv6 endpoint formatting

Task 2's generic CNet Source connection snapshot must format IPv6 authorities with brackets.

- IPv4 listener: `tcp://127.0.0.1:<port>`
- IPv6 listener: `tcp://[::1]:<port>`
- IPv4 UDP packet source: `udp://127.0.0.1:<port>`
- IPv6 UDP packet source: `udp://[::1]:<port>`
- KCP formatting follows the same host-authority rule but ProtocolNetworkIntake v1 still rejects KCP before native network side effects.

The implementation must not directly concatenate a raw IPv6 host as `<scheme>://<host>:<port>`. This is a documentation/formatting correction only and does not broaden the approved #118 acceptance scope, which remains JT/T 808 over TCP and CoAP over UDP.

## Execution authority

This ruling file is part of the reviewed implementation baseline. Where the implementation plan's Task 2 formatter example conflicts with this file, this file wins. The design spec remains authoritative for architecture and lifecycle semantics.
