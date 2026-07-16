# Turbo Flow Grammars

This directory owns the `.flow` DSL lexer and parser inputs:

- `flow_lexer.re`: re2c lexer specification.
- `flow_grammar.y`: Lemon grammar specification.

The current parser supports non-parameterized reusable stage composition. The
external YAML v1 resolver and its immutable resolved-JSON diagnostic view are
also implemented public APIs, but they remain a separate configuration layer
and are not parsed by this grammar. Parameterized stages, `with`, and `$name`
substitution remain proposals in `STAGE_CONFIG_DESIGN.md`.

The generated C files are build artifacts created by `cmake_add_grammar()` and
must stay out of source control.

## Control Grammar

The control parser accepts exactly one action:

```text
flow pause
flow resume
flow drain timeout 5000
pool transform thread resize 4 timeout 5000
adapter fmq.input quiesce
adapter fmq.input replace host "127.0.0.1" port 9000 path "/"
when pool.transform.thread.saturated then pool transform thread resize 8 timeout 5000
if traffic.message_errors > 0 then flow pause
when system.load.one_minute > 4.0 then flow pause
```

`if` and `when` are aliases. Their condition uses the shared typed expression
engine and must return BOOL. Parsing is side-effect-free; execution captures
core facts, merges an optional host provider, evaluates once, and dispatches
one typed command only when true. Unknown fields and type mismatches are errors,
not false or zero. Multi-action rules and arbitrary function calls are not part
of this grammar.

Pool actions and adapter actions with a stable connection resource are lowered
through `turbo_flow_resource_command()`, so target identity, generation,
deadline, idempotency, and provider-owned command semantics stay on the shared
resource-command boundary. Adapters registered without a command-capable stable
resource retain the direct adapter-command path as a compatibility boundary.
That direct path and `turbo_flow_adapter_command()` are deprecated. New
controllable adapters must register a command-capable stable resource with
`turbo_flow_register_adapter_with_resources()`; adapters that expose no control
surface may continue to use `turbo_flow_register_adapter_ex()`.

## Generation Flow

```text
flow_lexer.re
    -> re2c
    -> turbo_flow_lexer_gen.c

flow_grammar.y
    -> lemon
    -> turbo_flow_grammar_gen.c
    -> turbo_flow_grammar_gen.h
```

`turbo_flow_parse_string()` initializes the lexer, sends each token to the
generated Lemon parser, and appends a synthetic trailing newline when the input
does not end with one. Grammar actions build the in-memory stage and edge plans;
they do not execute callbacks or adapters.

## Lexical Rules

Whitespace:

- Spaces and tabs are skipped.
- Newlines are significant and terminate declarations or edge statements.
- `#` comments are skipped until end of line.
- `%%` comments are skipped until end of line.

Identifiers:

```text
[A-Za-z_][A-Za-z0-9_]*
```

Numbers:

```text
[0-9]+
```

Strings:

```text
"any bytes except quote or newline"
```

String tokens expose the unquoted content to parser actions. Escapes are not
currently part of the lexer contract.

Arrows:

```text
-> | -->
```

Both forms map to the same `ARROW` token.

Reserved words:

```text
adapter
operation
resource
coro
exec
in
inline
lanes
out
pool
reject
retry
reorder
route
source
stage
step
thread
when
worker
workers
attempts
delay
capacity
timeout
use
```

`inline`, `thread`, and `coro` are the only executor keywords. The removed
spellings `socket`, `io`, `custom`, `threadpool`, `thread_pool`, `coro_pool`,
`coronet`, and `socks` are not reserved and are rejected after `exec` as unknown
executor names. Network placement belongs to an adapter owner and is not an
executor class.

## Top-Level Grammar

The DSL is stage-oriented. `stage main` is the single root orchestration stage.
Non-`main` stage blocks are reusable composite stages with explicit ports. `use
<alias> = <stage>` instantiates a reusable stage into the current block. The
older `graph`, `subgraph`, and `flow` block spellings are removed and parse as
syntax errors.

Declarations and reusable stage definitions after the root block are rejected.

```text
program         := top_items

top_item        := source_decl NEWLINE
                 | stage_decl NEWLINE
                 | stage_block
                 | NEWLINE

source_decl     := "source" IDENT source_options
stage_decl      := "stage" IDENT stage_options
step_decl       := "step" IDENT stage_options
use_stmt        := "use" IDENT "=" IDENT
```

`source` declarations are accepted at top level for compatibility and inside
the root block. Reusable composite stages cannot declare sources.

Supported source options:

```text
source_options  := empty
                 | source_options "adapter" adapter_name
                 | source_options "operation" adapter_name
                 | source_options "resource" adapter_name

adapter_name    := dotted_name | STRING
dotted_name     := binding_segment
                 | dotted_name "." binding_segment
```

Binding segments may use identifier or reserved-word text, so protocol names
such as `socket.server` and direction names such as `codec.json.in` do not need
quotes. The dot must be adjacent to both segments; `http . client` is rejected.
Quoted names remain supported for compatibility and names outside this shape.

FMQ primitives use this same binding grammar; `pub`, `sub`, `push`, `pull`,
`router`, `dealer`, and `pair` are registry name segments, not lexer keywords:

```flow
source events adapter fmq.sub
stage publish adapter fmq.pub
source jobs adapter fmq.pull
stage dispatch adapter fmq.push
source requests adapter fmq.router
stage reply adapter fmq.router
source responses adapter fmq.dealer
stage request adapter fmq.dealer
source peer_in adapter fmq.pair
stage peer_out adapter fmq.pair
```

The host registration determines each binding's primitive and endpoint mode.
Compile rejects a source/sink declaration that is incompatible with the
registered FMQ adapter schema.

Supported stage options:

```text
stage_options   := empty
                 | stage_options "worker" NUMBER
                 | stage_options "capacity" NUMBER
                 | stage_options "adapter" adapter_name
                 | stage_options "operation" adapter_name
                 | stage_options "resource" adapter_name
                 | stage_options "exec" exec_spec
                 | stage_options "retry" "attempts" NUMBER
                 | stage_options "retry" "attempts" NUMBER "delay" NUMBER
                 | stage_options "reorder" "capacity" NUMBER "timeout" NUMBER
```

`operation` binds a host-registered, versioned domain operation descriptor to
the node. `resource` binds a host-registered resource primitive required by
that operation. The DSL cannot declare or override scope/authority; those are
trusted registry metadata. A `resource` option without `operation` is rejected
during compile. A node without a domain-specific binding resolves to a complete
`core.source` or `core.stage.inline/thread/coro/worker` contract; it does not
bypass operation validation.

The compiler validates source/stage role, resource domain/type, selected
executor and worker capability, owner/pool concurrency scope, and explicit
operation edge input/output domain and type. A cross-domain operation must be
registered with the bridge role. Management/owner-command operations cannot be
bound to payload graph nodes.

The registered operation runtime contract is also checked against DSL execution
options. A bounded operation requires a worker stage with the same capacity.
Explicit operation contracts support blocking, fail-fast, and drop-newest
backpressure; drop-oldest returns `TURBO_ENOTSUP` because an active sequence
cannot be reclaimed safely. Preserve-input worker operations require `reorder`;
reject/retry error modes require a reject edge or retry policy. Operation
execution deadlines are enforced by the selected stage runtime; source deadlines
remain unsupported until an adapter owns a per-message contract. Settlement
requirements must be supported by the operation and an explicitly registered
settlement owner, otherwise compile returns `TURBO_ENOTSUP`. Truncated operation
descriptors are rejected, so a worker node with an explicit operation must
register the complete current bounded contract. A worker without an explicit
binding receives a concrete bounded/block core operation with the node's exact
capacity.

`worker N` selects the data-plane worker-pool strategy for the stage. Optional
`capacity M` sets its disruptor ring capacity; `M` must be a power of two from
1 through 1048576 and defaults to 1024. `capacity` requires `worker`, but the
two options may appear in either order. This capacity is separate from both
executor worker counts and the coroutine shell `pool` option. Worker admission
is bounded and blocking: synchronous publishers wait when the ring is full;
there is no inline DSL spelling that changes this default. Fail-fast and
drop-newest are available only when a bound operation descriptor explicitly
declares that runtime contract. A direct DSL option would be a future grammar
addition.

`retry attempts N` is explicit and counts the first adapter call. `N` must be
between 2 and 64. Optional `delay MS` is bounded to one hour. The compiled stage
is accepted only when its adapter registered `consume_retry`; an ordinary stage
callback may not override that adapter path. No retry option means exactly one
attempt.

`reorder capacity N timeout MS` creates an explicit ordered boundary. `N` must
be between 1 and 1048576 and `MS` between 1 and 3600000. Executions arriving
after a missing sequence wait at the boundary; capacity overflow fails with
`TURBO_ENOSPC`, a missing sequence fails with `TURBO_ETIMEDOUT`, and stop wakes
waiters with `TURBO_ESHUTDOWN`. An unordered worker/thread branch may enter an
ordered fan-in only after crossing such a boundary.

## Executor Grammar

```text
exec_spec       := "inline" exec_options
                 | "thread" exec_options
                 | "coro" exec_options

exec_options    := empty
                 | exec_options "workers" NUMBER
                 | exec_options "lanes" NUMBER
                 | exec_options "pool" NUMBER

```

Explicit executor counts must be greater than zero. `thread` accepts only
`workers`; `coro` accepts only `lanes` and `pool`; `inline` does not accept count
options. `socket`, `io`, and `custom` are rejected executor names. A coroutine executor owns one scheduler per
lane. When `pool M` is present, each lane owns an independent `M`-shell
`turbo_coro_pool_t`; the maximum shell count is therefore `lanes * M`. Pools are
not shared across lanes because their acquire/release bookkeeping is lane-owned.

Examples:

```flow
step parse exec inline
step enrich worker 4 capacity 64 exec thread workers 8
step fetch exec coro lanes 2 pool 128
step socket_sink adapter socket.tcp
step rules operation rules.evaluate exec inline
step fetch adapter http.client retry attempts 3 delay 100
step ordered reorder capacity 128 timeout 1000
source ingress operation mqtt.publish_in resource mqtt.session
step validate operation data.validate
```

Top-level `stage parse ...` remains accepted during migration. Inside a
composite `stage { ... }` block, prefer `step parse ...` for atomic processing
steps.

## Stage Block Grammar

Only one root stage block is supported and it must be named `main`. A stage
block whose name is not `main` is treated as a reusable composite stage with a
scoped namespace and explicit ports.

```text
stage_block     := "stage" IDENT "{" NEWLINE stage_lines "}" NEWLINE

stage_line      := source_decl NEWLINE
                 | step_decl NEWLINE
                 | use_stmt NEWLINE
                 | port_decl NEWLINE
                 | edge_stmt NEWLINE
                 | route_stmt NEWLINE
                 | reject_stmt NEWLINE
                 | NEWLINE

port_decl       := "in" IDENT
                 | "out" IDENT
```

In `stage main`, `source`, `step`, `use`, edge, route, and reject lines are
accepted. In non-root reusable stages, `in`, `out`, `step`, `use`, and ordinary
edge lines are accepted. `source`, `route`, and `reject` inside reusable stages
are rejected by semantic validation.

`use <alias> = <stage>` is currently non-parameterized. It expands the target
stage's ports, steps, nested `use` instances, and internal edges into the alias
namespace during parsing. A bare alias in an edge uses the target's single input
or output port according to edge direction:

```flow
stage cleanse {
  in raw
  out clean
  step trim

  raw -> trim -> clean
}

stage pipeline {
  in raw
  out done
  use c = cleanse
  step encode

  raw -> c -> encode -> done
}

stage main {
  source inbound
  step load adapter sqlite.orders
  use p = pipeline

  inbound -> p -> load
}
```

The expanded plan contains finite names such as `p.raw`, `p.c.trim`, and
`p.done`. Parameterized `use ... with profile`, YAML profiles, and resolved JSON
are design-level items and are not part of the current parser grammar.

Example:

```flow
stage cleanse {
  in raw
  out clean
  step trim

  raw -> trim -> clean
}

stage main {
  source inbound
  step normalize
  step load adapter sqlite.orders

  inbound -> cleanse -> normalize -> load
}
```

This shape is intended for clear data pipelines such as ingest -> cleanse ->
transform -> load.

## Edge And Route Grammar

```text
edge_stmt       := path
route_stmt      := "route" node ARROW node "when" EXPR
reject_stmt     := "reject" IDENT node ARROW node
path            := term
                 | path ARROW term
term            := node
                 | "[" node_list "]"
node_list       := node
                 | node_list "," node
node            := IDENT
                 | IDENT "." IDENT
```

Edges are expanded pairwise between the left and right terms. Grouped terms
express fan-out or fan-in without changing callback behavior.

A route connects exactly one top-level node to one top-level node. `EXPR` is the
trimmed remainder of the line after `when`; it is parsed and type-checked by the
separate expression grammar during `turbo_flow_compile()`. The expression must
return BOOL. It evaluates the successful upstream stage output, and false
filters the edge without failing publication. Conditional routes are not
accepted inside reusable stage declarations.

A reject statement names one failure edge from an executable stage:

```flow
reject validation_failed validate -> rejected
```

It activates only when `validate` returns a nonzero execution status. A stage
may own at most one reject edge and reject names are unique within the stage
plan. Reject edges cannot originate at sources or ports. When no reject edge
exists, the original fail-fast behavior is unchanged.

Examples:

```flow
stage clean {
  in input
  out output
  step trim

  input -> trim -> output
}

stage main {
  source input
  step parse
  step validate
  step persist
  step metrics
  step enrich
  step sink
  use c = clean

  input -> parse -> validate -> persist
  parse -> [metrics, enrich] -> sink
  c -> sink
  route validate -> persist when msg.status == 0
  reject validation_failed validate -> rejected
}
```

## Current Rejection Rules

The parser or compile step rejects:

- Unknown executor keywords after `exec`.
- Numeric options outside `uint32_t`.
- Explicit worker, executor worker, lane, or pool counts of zero.
- Worker capacity that is zero, above 1048576, not a power of two, or declared
  without `worker`.
- Repeated `adapter`, `worker`, `capacity`, `exec`, `workers`, `lanes`, or
  `pool`, `operation`, or `resource` options in one declaration.
- Unknown operation/resource bindings, wrong primitive domain/type, operation
  role or execution-scope mismatch, and incompatible typed operation edges.
- Resource binding without an operation, or management owner-command operation
  bound to a payload graph node.
- Executor count options not consumed by the selected executor kind.
- Whitespace around dots in unquoted adapter binding names.
- Duplicate stage or source names.
- `source` declarations inside reusable stages.
- Nested stage declarations.
- Unknown `use` target stages and `use` aliases that collide with expanded
  names.
- Multiple root `stage main` blocks.
- Top-level declarations or reusable definitions after the root block.
- Ambiguous stage shorthand when a reusable stage does not have exactly one
  input and one output port.
- Unexpected characters and malformed grouped node lists.
- Empty, non-BOOL, or invalid conditional route expressions.
- Conditional routes inside reusable stages or with grouped endpoints.
- Duplicate reject names, multiple reject edges from one stage, and reject
  edges originating at sources or ports.
- Retry attempts outside 2..64, delay above one hour, duplicate retry options,
  and retry on adapters without an explicit retry callback.
- Zero or excessive reorder capacity/timeout values and duplicate reorder
  options.

## Maintenance Rules

When changing `.flow` syntax:

- Update `flow_lexer.re` for new tokens or aliases.
- Update `flow_grammar.y` for grammar shape and parser actions.
- Update this file with the accepted surface syntax and rejection behavior.
- Add or update parser tests in `turbo_flow/tests/test_turbo_flow.c`.
- Keep syntax decisions in the DSL layer; runtime behavior belongs in the
  compiler, executor, adapter, or data-plane modules.
