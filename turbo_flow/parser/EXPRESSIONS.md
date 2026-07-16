# TurboFlow Expression Grammar

This document defines the expression language contract used by the `.flow`
grammar. The expression parser and typed IR are backend-neutral.
Both execution modes lower the same typed IR to one MIR module:

- MIR interpreter through `MIR_interp_arr()`.
- MIR JIT through `MIR_gen()`.

The DSL never exposes MIR instructions or selects a backend. Backend selection
is host configuration and does not change expression results.

## Integration Boundary

Expressions are used by explicit conditional route statements:

```flow
route validate -> accepted when msg.status == 0
route validate -> rejected when msg.status != 0
```

A conditional route is one directed edge. Its predicate runs only after the
upstream stage succeeds and observes that stage's output message. `true`
activates the edge; `false` filters that edge without failing the message.
Predicates are read-only: validation and transformation remain stage work.
Chained conditional paths are not accepted because predicate ownership would
be ambiguous. Normal topology keeps the existing syntax:

```flow
input -> validate -> persist
```

The topology lexer captures the text after `when` through the end of that line
and passes it to the separate expression parser. Expression tokens and MIR
details are therefore not duplicated in the topology grammar.

## Lexical Elements

```text
identifier  := [A-Za-z_][A-Za-z0-9_]*
integer     := decimal integer with optional leading '-'
float       := decimal floating point with optional exponent
string      := double-quoted UTF-8 bytes with defined escapes
boolean     := true | false
null        := null
```

Reserved expression words are `true`, `false`, `null`, `and`, `or`, and `not`.
Symbol aliases `&&`, `||`, and `!` are accepted for logical operators. The IR
normalizes word and symbol spellings to the same operators.

## Grammar

```text
expression      := or_expression

or_expression   := and_expression
                 | or_expression OR and_expression

and_expression  := equality_expression
                 | and_expression AND equality_expression

equality_expression
                := relation_expression
                 | equality_expression EQ relation_expression
                 | equality_expression NE relation_expression

relation_expression
                := additive_expression
                 | relation_expression LT additive_expression
                 | relation_expression LE additive_expression
                 | relation_expression GT additive_expression
                 | relation_expression GE additive_expression

additive_expression
                := multiply_expression
                 | additive_expression PLUS multiply_expression
                 | additive_expression MINUS multiply_expression

multiply_expression
                := unary_expression
                 | multiply_expression MUL unary_expression
                 | multiply_expression DIV unary_expression
                 | multiply_expression MOD unary_expression

unary_expression
                := primary_expression
                 | NOT unary_expression
                 | PLUS unary_expression
                 | MINUS unary_expression

primary_expression
                := literal
                 | field_reference
                 | LPAREN expression RPAREN

field_reference := identifier
                 | field_reference DOT identifier

literal         := integer | float | string | boolean | null
```

Function calls, indexing, assignments, mutation, loops, regex literals, and
implicit host callbacks are outside the first grammar.

## Type System

Initial value types:

```text
NULL | BOOL | I64 | F64 | STRING
```

Rules:

- Logical operators require BOOL and short-circuit left to right.
- Unary `not` requires BOOL.
- Arithmetic requires numeric operands.
- Mixed I64/F64 arithmetic promotes I64 to F64.
- Modulo requires two I64 operands.
- Ordering compares numeric pairs or string pairs; it does not coerce strings
  to numbers.
- Equality accepts identical types, mixed numeric types, or NULL comparison.
- NULL supports only equality/inequality in the first version.
- Division or modulo by zero returns an evaluation error; it does not fallback
  to zero or false.
- Integer overflow returns an evaluation error unless an operator is explicitly
  added later with saturating semantics.

## Fields

Built-in message fields have fixed types:

| Field | Type |
| --- | --- |
| `msg.id` | I64, interpreted as non-negative |
| `msg.ts_ns` | I64, interpreted as non-negative |
| `msg.type` | I64 |
| `msg.flags` | I64 |
| `msg.status` | I64 |
| `msg.payload` | STRING/byte view |

Structured fields use `parsed.<path>`. They require a compile-time resolver
provided by the configured DataBind schema. Unknown or dynamically typed paths
fail compile. The expression engine does not introduce a generic object model.

The public compile boundary is declared in `turbo_flow_expr.h`. Hosts provide a
borrowed array of fully qualified field paths, value types, and stable field
IDs. `turbo_flow_expr_compile()` parses, resolves, and type-checks before
returning an opaque expression. The compiled object owns copied paths and does
not retain the schema array.

## Typed IR

The parser produces an AST with source locations. A separate type-check pass
resolves fields and produces immutable typed IR. Every node records:

- operation kind;
- result type;
- source line and column;
- child indices;
- literal value or resolved field id.

The typed IR owns copied string literals and field paths. It never stores views
into the source text after compile.

Built-in and schema fields have separate scopes, so a schema field ID cannot be
mistaken for `msg.status` or another built-in ID. Type-checking uses a temporary
IR and commits resolved types/IDs only after every node succeeds; a failed
recheck leaves an already typed AST unchanged.

Suggested operation groups:

```text
CONST, LOAD_FIELD
NOT, NEG
ADD, SUB, MUL, DIV, MOD
EQ, NE, LT, LE, GT, GE
AND, OR
```

`AND` and `OR` lower to MIR control flow, not eager bitwise operations.

## Evaluation Context ABI

`turbo_flow_expr_eval_context_t` is the stable read-only boundary shared by the
interpreter and JIT backends. It contains:

- a size field for ABI versioning;
- one borrowed immutable `turbo_flow_msg_t`;
- an optional schema-field reader callback and borrowed callback context.

`turbo_flow_expr_read_field()` reads built-in `msg.*` fields directly and
delegates schema field IDs to the callback. Returned strings are borrowed views.
Message IDs and timestamps outside signed I64 range fail with `TURBO_ERANGE`;
schema callbacks returning invalid types or views fail with `TURBO_EPROTO`.
Neither the context nor returned values may be retained by an evaluator.

## MIR Execution

One lowering pass creates a MIR function with a stable C ABI over a read-only
evaluation context. The same MIR item is used by both backends:

```text
TURBO_FLOW_EXPR_MIR_INTERP
TURBO_FLOW_EXPR_MIR_JIT
TURBO_FLOW_EXPR_AUTO
```

`AUTO` is selected at expression compile time. It may use JIT only when the
platform supports code generation and configured policy permits executable
memory. It never changes backend during message evaluation.

MIR context/module/function ownership belongs to the compiled expression. JIT
code is finalized and released before destroying its MIR context. Lowering,
linking, JIT generation, and interpreter instruction generation all finish in
`turbo_flow_expr_compile_ex()`. Evaluation uses a fixed 256-slot stack frame;
expressions exceeding that evaluation depth fail during compile. Interpreter
calls for one compiled expression are serialized by a TurboUtils mutex because
the MIR interpreter context is mutable. Generated JIT code is called directly.

## Error Semantics

Parse and type errors report code, line, column, and a bounded message.
Evaluation errors identify the operation and return a status code. Predicates do
not convert errors to false; routing follows the flow failure policy.

For conditional fan-in, a stage waits until every potentially reachable input
edge has resolved. It runs once when at least one input edge is active. If none
are active, the stage and its downstream path are skipped. Graphs containing
conditional edges use the direct scheduler because the broadcast disruptor
topology is static while route selection is per message.

## Required Tests

- Lexer coverage for every token and invalid character.
- Precedence and associativity for arithmetic, comparison, and logic.
- Parentheses and unary operators.
- String escapes and malformed literals.
- Field resolution and unknown-field rejection.
- Type mismatch, NULL rules, divide/modulo by zero, and integer overflow.
- Logical short-circuit with an erroring right operand.
- MIR interpreter/JIT parity for every semantic test vector.
- JIT-disabled platform/config behavior.
- Parse, type-check, compile, evaluate, reset, and destroy ownership paths.
- Benchmarks for parse/type-check, MIR compile, interpreter evaluation, and JIT
  evaluation.

## Build Boundary

TurboFlow uses the repository-owned `vendor/mir` sources directly through the
private `mir_static` target. MIR headers, objects, lifecycle types, and error
details do not cross the installed TurboFlow ABI. This keeps the expression
contract backend-neutral while avoiding a dependency on an unavailable
`TurboUtils::MIR` export.

`TURBO_FLOW_EXPR_ENABLE_JIT=OFF` removes JIT selection while retaining the MIR
interpreter. In that build, `AUTO` selects the interpreter and an explicit JIT
request fails with `TURBO_ENOTSUP`; it never silently changes an explicit host
policy. Re-enabling JIT is a configure-time rollback and does not change the
expression syntax or serialized data.
