# TurboFlow

**Provider-neutral graph, workflow, and durable execution infrastructure for the Salts ecosystem.**

TurboFlow turns Salts/CFlow execution primitives into configurable application graphs with explicit provider boundaries. It owns the Graph DSL, compilation and execution, configuration resolution, typed projection, scheduling, observability, protocol ingress, plugin hosting, and optional infrastructure adapters.

TurboFlow does **not** own broker products, database engines, transport implementations, or application-specific business state. Those capabilities are integrated through public provider/adapter boundaries.

**Tags:** C11 · workflow · dataflow · graph · durable-execution · plugins · protocol-ingress · CFlow · RulesForge · TurboDB

## Built on Salts

TurboFlow builds directly on the installed [Salts](https://github.com/qigao/salts) and [SaltsUtils](https://github.com/qigao/salts-utils) SDKs.

The shared foundation provides:

- **CFlow** for typed graph, reactive execution, demand/backpressure, scheduling, and lifecycle semantics.
- **CMeta** for stable typed metadata and operation contracts.
- **CNet** for network-facing adapters with explicit stop/drain behavior.
- **DataBind / schema tooling** for canonical protocol envelopes and generated bindings.
- **RulesForge** for optional rule-driven business execution.
- **TurboDB** for optional durable storage and persistent Inbox providers.
- **CHTTP** for HTTP/WebSocket adapter boundaries.

TurboFlow keeps those capabilities provider-neutral: Graph/Core does not statically absorb product-specific implementations.

## Ecosystem role

```text
Salts
  ├── salts-utils
  ├── salts-net
  └── DataBind
        ↓
 CHTTP / TurboDB / RulesForge / Flowie
        ↓
      TurboFlow
        ↓
 products / gateways / business applications
```

TurboFlow is a **framework/application execution layer**. It composes lower-level infrastructure but keeps ownership of each external capability at its provider boundary.

## Canonical data path

The target data plane is:

```text
Source DLL
  -> canonical protocol envelope
  -> memory or durable Inbox
  -> RulesForge / application graph
  -> Sink DLL
```

The same business payload can arrive through HTTP, WebSocket, socket, MQTT, or another protocol and then reuse the same business graph.

Input transport does not implicitly choose output transport. Routing remains explicit in configuration or business rules.

## Provider-neutral durable Inbox

TurboFlow exposes a common Inbox boundary for memory and durable providers.

Key rules:

- Source admission uses stable source/admission identity.
- Accepted records become immutable Inbox records.
- Graph execution claims records independently from ingress.
- Complete/discard/fail are explicit terminal transitions.
- Durable-provider failure does **not** fall back to memory.
- Retry is explicit.
- Provider runtime state is not exposed as Graph identity.
- Ownership and tombstone/history state remain bounded.
- A provider must satisfy the same public boundary regardless of whether it is memory-backed or TurboDB-backed.

The in-memory provider is intentionally not crash-durable. A TurboDB provider supplies durability through the same upper-layer contract.

See [transport-independent business graph design](docs/architecture/transport-independent-business-graph.md).

## Protocol ingress

`ingress/protocol` contains product-independent protocol codecs and Sources, including protocol families such as:

- OCPP
- JT/T 808
- GB/T 32960
- CoAP
- LwM2M
- MQTT-SN

Protocol decoders validate framing and then normalize accepted data into the canonical Inbox envelope.

Connection listeners, HTTP endpoints, and network lifecycle remain owned by CNet/CHTTP-facing adapters. MQTT receive is a Source and MQTT send is a Sink; MQTT is not TurboFlow's internal message format.

## Public package targets

| CMake target | Responsibility |
| --- | --- |
| `TurboFlow::Config` | Parse/validate product configuration and produce resolved configuration |
| `TurboFlow::Graph` | Graph DSL, compile/execute, generic operation and adapter contracts |
| `TurboFlow::Product` | Assemble Graph plus repository-owned adapters |
| `TurboFlow::PluginHost` | Load/register versioned plugin capabilities and protect module lifetime |
| `TurboFlow::ProtocolIngress` | Product-independent protocol codec/Source layer |
| `TurboFlow::ProtocolIngressInbox` | Admit canonical protocol envelopes into Inbox |
| `TurboFlow::ProtocolIngressInboxSchema` | Generated static DataBind codec/schema boundary |
| `TurboFlow::CNetAdapter` | Optional CNet Source/Sink owner |
| `TurboFlow::CHTTPAdapter` | Optional HTTP/WebSocket Source/Sink adapter |
| `TurboFlow::TurboDbAdapter` | Optional TurboDB provider/ORM adapter |

TurboFlow 2.0 removed the old aggregate `TurboFlow::Flow` target and umbrella runtime assumptions. Consumers should link only the components they actually use.

## Plugin host

External capabilities are loaded through a versioned C ABI rather than hard-coded implementation selection.

The plugin host:

1. validates plugin ID/version/capability descriptors;
2. freezes a catalog snapshot;
3. performs bounded preflight;
4. materializes adapters/resources;
5. compiles a graph generation;
6. protects live generations with explicit leases.

A failed new generation is rolled back without falling back to a legacy/static implementation.

DLL/module lifetime remains valid until the final generation/snapshot/lease is released.

## Graph execution

TurboFlow Graph uses CFlow's typed execution model.

Callback stages bind explicit operation names rather than node display names. A provider must register the operation descriptor and implementation before compile succeeds.

Reactive execution is demand-driven and bounded. Waiting resumes only through a valid waker, cancellation is explicit, and queue-full/closed states remain visible errors rather than implicit retries.

## Dependency roots

The build resolves dependencies from explicit installed SDK roots and fails if required roots are absent.

Current required roots include:

```text
SALTS_ROOT
SALTS_UTILS_ROOT
RULES_FORGE_ROOT
```

When the TurboDB adapter is enabled:

```text
TURBODB_ROOT
```

The CHTTP adapter is validated through the repository's package contract helper.

TurboFlow does not search unrelated profiles as a hidden fallback.

## Build

Typical Windows Release flow:

```powershell
cmake --preset win-release-user
cmake --build --preset win-release-user
ctest --preset win-release-user --output-on-failure
cmake --build --preset install-win-release-user
```

Linux uses the corresponding `linux-*` presets.

## Design principles

- **Provider-neutral boundaries.** Memory, TurboDB, CHTTP, CNet, broker, and rule providers satisfy explicit contracts.
- **No fallback.** A failed durable/provider path does not silently switch implementation.
- **Bounded ownership.** Admission, claims, queues, histories, plugin state, and execution state have explicit limits/lifetimes.
- **Canonical typed envelopes.** Protocol-specific data is normalized before business execution.
- **Transport-independent business graphs.** Business behavior is not coupled to its ingress transport.
- **Stable identity.** Durable/application identity is not raw runtime pointer/driver state.
- **One execution foundation.** TurboFlow reuses CFlow semantics instead of embedding a second graph scheduler/runtime.
- **Explicit lifecycle.** Poll, drain, shutdown, retirement, and settlement are visible transitions.

## Related projects

- [Salts](https://github.com/qigao/salts) — typed systems foundation.
- [SaltsUtils](https://github.com/qigao/salts-utils) — parser/utility extension layer.
- [RulesForge](https://github.com/qigao/RulesForge) — rule execution.
- [TurboDB](https://github.com/qigao/turbodb) — durable storage/data infrastructure.
- [CHTTP](https://github.com/qigao/chttp) — HTTP/WebSocket infrastructure.
- [Flowie](https://github.com/qigao/flowie) — MQTT server/client infrastructure.

---

**Salts provides the typed execution foundation. TurboFlow composes it into provider-neutral business graphs and durable workflows.**
