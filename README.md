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
  ├── salts-utils (including DataBind)
  └── salts-net
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
  -> generic durable-buffer (memory or TurboDB provider)
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

Protocol decoders validate framing and normalize accepted data into the canonical protocol envelope. Real network intake then publishes that message into the generic durable-buffer boundary; protocol ingress does not own a separate storage runtime.

NetworkIntake v4 selects its Source and decoder by exact `.flow` stage names and receives explicit Component/provider/resource resolvers. Both configurations use generated typed Messages; decoder schema 1 has an optional complete mapper record and no resolved-config input. CNet deployment resource Interfaces retain endpoint/TLS ownership. The compiled Source instance and generation scope survive stop until final destruction; failed cleanup returns an owner for explicit retry. See the [CNet migration and lifetime contract](io/cnet/ADR_CNET_PLUGIN.md#networkintake-v4-消费端迁移) and the real TCP/UDP integration tests.

Deployment-owned PluginHost module lists use `turbo_flow_plugin_host_create_modules()`
with explicit ID/version/absolute-path entries. The resolved-config
`turbo_flow_plugin_host_create_configured()` entry point is removed. The list is
borrowed only during creation; failed rollback returns an owner for destroy retry.
Canonical provider modules continue to use Salts Plugin / Component scopes.

PluginHost ABI 3.5 removes the resolved-config argument from
`turbo_flow_plugin_generation_create()`. Pass the parsed `.flow` Graph, explicit
Component/provider/resource resolvers, catalog snapshot and result domain directly.
Operation policy and materializer selection come from typed stage records; no
empty YAML document is required. Rebuild hosts and plugins together. Config's
remaining Product, host-loading and Security/Policy consumers are still being
migrated under [#241](https://github.com/qigao/turbo-flow/issues/241).

Connection listeners, HTTP endpoints, and network lifecycle remain owned by CNet/CHTTP-facing adapters. MQTT receive is a Source and MQTT send is a Sink; MQTT is not TurboFlow's internal message format.

## Authoring boundary

TurboFlow product/dataflow authoring uses `.flow` exclusively. DataBind owns
contract/schema semantics; Praktor owns YAML workflow/orchestration; TurboSCXML
owns explicit SCXML statechart applications. Deployment-specific endpoints,
secrets and provider instances are referenced from `.flow` through named
resources and resolved before ExecutionPlan publication.

## Public package targets

| CMake target | Responsibility |
| --- | --- |
| `TurboFlow::Config` | Legacy YAML/resolved-config component; removal tracked by #241. Do not use for new product authoring. |
| `TurboFlow::Graph` | Graph DSL, compile/execute, generic operation and adapter contracts |
| `TurboFlow::Product` | Assemble Graph plus repository-owned adapters |
| `TurboFlow::PluginHost` | Load/register versioned plugin capabilities and protect module lifetime |
| `TurboFlow::ProtocolIngress` | Product-independent protocol codec/Source layer |
| `TurboFlow::ProtocolNetworkIntake` | Real CNet Source + protocol decode owner; decoded messages publish only through the generic durable-buffer boundary |
| `TurboFlow::ProtocolIngressInboxSchema` | Generated static DataBind codec/schema boundary for the canonical protocol envelope; no separate storage runtime |
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

The build resolves exact published producer SDKs from explicit installed roots and fails if required roots are absent or resolve outside those roots.

The Windows Release SDK roots used for focused provider/runtime qualification
on 2026-10-11 are:

- **Salts.Native 2.3.0-rc.10**, source `6ba9868a965f3273cf87ab243dab1884072eb7a3`
- **SaltsUtils.Native 4.3.0-rc.8** (including DataBind), source `3d39b8c23403bad5fc3e04241bb74a7889bd8a03`
- **CHttp.Native 2.1.0-rc.3**, source `b8cedaeca1d858ca7866530934c6b9025cb70f53`
- **TurboDB.Native 2.3.4** when the TurboDB adapter is enabled, source `a8f849e7cd7193b89a8ade596bf9ef072a0533fc`

This records the installed SDK manifests, not completion of the full platform
or adapter qualification gates. The optional RulesForge plugin requires its
matching published SDK. Generated Message and transport targets explicitly
select `BINARY_CODEC` to preserve their existing codec ABI with current IDLC.

Required roots for the base build are:

```text
SALTS_ROOT
SALTS_UTILS_ROOT
```

There is no independent `DATABIND_ROOT`; DataBind is resolved only through the selected SaltsUtils package.

When the RulesForge plugin/provider is enabled:

```text
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

Run from the Visual Studio developer environment. Windows user presets consume
the shared [vcpkg-cache](https://github.com/qigao/vcpkg-cache) toolchain at
`%LOCALAPPDATA%/qigao/vcpkg-cache`, using `qigao-x64-windows-v145` for both target
and host packages. Supply `GITHUB_TOKEN` with package-read access in the parent
environment (for example, load it locally from `.env` before invoking CMake).
Presets do not read `.env`. The versioned NuGet config references the token;
the shared feed is read-only and the local binary cache is writable. CHTTP's
SaltsUtils Crypto dependency is supplied by the manifest's GmSSL package.

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

## Component runtime candidate qualification

The integration-only `Component runtime conformance` workflow checks out the
consumer event SHA and restores the exact Salts 3.0 prerelease
`3.0.0-cmeta.06fa2b10b418f997c38f123ddefa00e448897617` for `linux-x64`.
It verifies package SHA256
`682122da918658bf958fc409dd148157962e128884b21a91df18c5b7e94589ca`
and the SDK commit/RID/profile manifest before configuration. SaltsUtils source
`1c00cab3c5fe4722d4c8488a47f0ada6ec2f3e6b` is rebuilt against that SDK;
CHTTP source `0c8919bb9d4732133ecdec8befd3eb5d5d543235` is rebuilt as well.
the workflow does not use stable first-party binaries for this candidate.

The Graph test creates two live generations backed by separate provider DSOs and observes their message-consumption counters and markers. Graph owners retain their generation scope through provider shutdown. Adjacent provider tests cover capacity rejection and cleanup rollback. Thread and Plugin calls use the canonical CMeta SDK names; obsolete Salts compatibility names are not restored.

With the workflow's installed dependency roots and shared vcpkg/re2c environment,
run the complete configured build and CTest suites, then verify the installed
consumer independently:

```sh
cmake --preset ci-component-release-user
cmake --build --preset ci-component-release-user -j2
ctest --preset ci-component-release-user --no-tests=error --output-on-failure
cmake --build --preset install-ci-component-release-user -j2
cd tests/install_consumer/component
cmake --preset ci-component-installed-user
cmake --build --preset ci-component-installed-user -j2
ctest --preset ci-component-installed-user --no-tests=error --output-on-failure
```

CI selects Component and adjacent business suites from the full configured graph
and runs the rebuilt SaltsUtils MessagePlan regression suites. It uploads
consumer/dependency identities, JUnit results, and CTest logs as
`component-acceptance-linux-x64`. The root CTest command above runs the broader
suite. The CNet, CHTTP, and durable-memory factory tests also adapt their existing
plugin exports into
local Component deployments; the outer plugin lease outlives the graph, owner,
bindings, and Component scope. The IO tests exercise provider materialization;
the durable-memory suite also verifies payload delivery, replay deduplication,
capacity limits, and backlog retirement before Graph stop. A green Linux Release run establishes only this
profile's acceptance;
sanitizer qualification and Windows/macOS downstream runs remain separate release
gates. This workflow neither merges nor publishes a stable release.
