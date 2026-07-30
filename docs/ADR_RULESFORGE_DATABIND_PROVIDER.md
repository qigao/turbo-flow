# RulesForge DataBind Provider

## Status

Accepted.

## Background

TurboFlow already exposes `rulesforge.apply` as a graph operation and can carry
owned schema projections through graph and Disruptor execution. The original
bridge delegates all rule behavior to an application callback, so applications
must repeat the same DataBind object ownership, session creation, rule firing,
and status conversion logic.

RulesForge stateful sessions are mutable. Sharing one session across worker-pool
consumers would make message isolation and concurrency ownership ambiguous.

## Options

1. Share one long-lived stateful session between graph workers.
2. Serialize every message through one provider-owned session.
3. Create one short-lived session for each message.

Option 1 was rejected because it requires synchronization and gives rules access
to facts from unrelated messages. Option 2 was rejected as the default because
it prevents worker-pool parallelism and still retains cross-message state.

## Decision

The built-in DataBind provider uses option 3:

- The application initializes RulesForge and owns the knowledge base.
- Each message owns either a `ruleforge_data_bind_object_t` or Codec-produced
  `DataBindValue` projection.
- The projection owner supplies its clone and destroy callbacks.
- Each provider invocation creates a session, copies the value into it, fires a
  bounded number of rules, records the match in `msg.flags`, and destroys the
  session. Codec-produced values are not serialized or parsed again.
- Provider configuration is borrowed and must outlive the flow registry entry.

The existing callback registration remains the extension point for intentional
cross-message RETE state and continuous-session processing.

## Consequences

- Worker-pool execution does not share mutable RulesForge session state.
- A message cannot observe facts from another message through the built-in
  provider.
- Session creation adds per-message allocation and RETE initialization cost.
- Match routing is explicit through a caller-selected non-zero message flag.
- RulesForge errors are converted to TurboFlow status codes and propagated.

## Compatibility And Migration

The existing `turbo_flow_rulesforge_register_data_operation()` API and callback
semantics are unchanged. Applications may migrate schema-bound message filters
by binding a RulesForge DataBind object or by placing the standard Codec before
the isolated provider. Applications relying on persistent facts should not
migrate to this provider.

The RulesForge dependency was already public on `TurboFlow::Flow`; exposing its
opaque C handle in the bridge header does not add a new link dependency.

## Rollback

The new bind and provider APIs can be removed without changing the existing
generic callback path. Messages using custom projections and existing graph
definitions remain compatible.

## Verification

The RulesForge bridge tests create real schemas, knowledge bases, rules, and
DataBind projections. They verify matching, filtering, projection ownership
across Worker-Pool Disruptor execution, and cleanup. The HTTP Todo example
demonstrates the Codec-produced `DataBindValue` path.
