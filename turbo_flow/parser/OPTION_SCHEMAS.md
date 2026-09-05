# Turbo Flow Option Schemas

This file defines the design boundary for typed options attached to `.flow`
adapter bindings. It is a schema design document, not a generated source input.

## Scope

`.flow` owns topology:

```flow
source input adapter "host.input"
stage decode adapter "codec.length"
stage bind adapter "databind.order"
stage output adapter "host.output"

stage main {
  input -> decode -> bind -> output
}
```

The host owns typed configuration for each adapter name:

```text
"host.input"     -> host source adapter options
"codec.length"   -> length-prefix codec options
"databind.order" -> DataBind schema options
"host.output"    -> host sink adapter options
```

This keeps secrets, deployment-specific endpoints, TLS material, and large
schemas out of `.flow` text while still letting the compiler validate that every
referenced adapter has a registered schema and runtime binding.

Binding names may be quoted or written as adjacent dotted segments:

```flow
source events adapter host.events
stage decode adapter codec.json.in
stage store adapter "storage.archive.prod"
```

These are registry binding names, not inline protocol configuration.

## Terms

Executor:

- Selects one of the three core compute executors: inline, thread, or coro.
- Worker handoff and external CNet/CHTTP adapter placement are separate runtime
  contracts, not additional executor kinds.
- Does not define HTTP, socket, email, codec, or DataBind behavior.

Adapter:

- Binds a source or stage to an external boundary or transformation.
- Owns protocol-specific lifecycle, consume, publish, and shutdown behavior.

Codec:

- Converts message payload bytes without owning product behavior.
- Examples: line framing, length-prefix framing, text/binary encoding.

DataBind:

- Validates or binds a payload to a typed schema.
- May attach a typed projection through `turbo_flow_msg_bind_projection()` with provider-owned
  clone/destroy hooks; there is no public raw parsed slot.
- Current supported input formats are `bin`, `json`, `csv`, and `xml`; msgpack
  is intentionally out of scope.

## Schema Shape

Each registered adapter option schema should describe:

```text
name              stable adapter binding name used in `.flow`
kind              one `turbo_flow_adapter_kind_t` value; in-tree modules use file, sqlite, codec,
                  databind, observe, schedule, and custom
role              source | sink | transform
direction         input | output | bidirectional
required_options  fields required before compile/start
optional_options  fields with defaults
secret_options    fields that must not be emitted in diagnostics
runtime_owner     core | adapter | host
codec_chain       optional inbound/outbound codec references
databind_schema   optional inbound/outbound schema reference
```

The implementation represents this as copied C metadata registered through
`turbo_flow_register_adapter_ex()`. Existing callers can continue to use
`turbo_flow_register_adapter()` without a schema. The public YAML v1 resolver
maps external configuration onto immutable typed views and emits canonical
resolved JSON for diagnostics and tooling. Module registration projects those
views into the same typed adapter/owner configs before allocation. TOML,
human-authored JSON, and resolved JSON are not alternate runtime config inputs.

## Common Option Field Types

Use a small, typed field system:

```text
bool
u32
u64
size
string
string-list
string-map
enum
enum-set
duration_ms
path
secret
schema_text
schema_path
```

Rules:

- Numeric fields must declare min and max when zero is not meaningful.
- Strings that name adapters, codecs, schemas, or types must declare whether
  they are identifiers, dotted names, filesystem paths, or opaque text.
- Secret fields must never appear in parse errors, compile errors, logs, or
  generated debug dumps.
- File paths must be host-resolved. The core DSL parser should not normalize or
  expand them.

## Direction Model

Source input:

- External bytes enter a flow source.
- Optional input codec transforms raw bytes into one message payload.
- Optional input DataBind validates or attaches a typed schema projection.

Sink output:

- A flow message reaches a sink adapter.
- Optional output DataBind validates required fields or typed payload shape.
- Optional output codec serializes or frames the message payload.
- The sink writes to the external boundary.

Transform stage:

- Receives one message and emits one downstream message in the MVP.
- Codec and DataBind adapters are transform stages unless a later source/sink
  adapter explicitly embeds them through a host-owned schema.

Recommended explicit stage plan:

```flow
source input adapter "host.input"
stage frame_in adapter "codec.length.in"
stage bind_order adapter "databind.order.in"
stage encode_out adapter "codec.length.out"
stage output adapter "host.output"

stage main {
  input -> frame_in -> bind_order -> encode_out -> output
}
```

This stage plan keeps codec/DataBind errors attached to clear stages instead of
hiding processing inside a host adapter.

## Network Adapter Boundary

Socket, HTTP, RPC, S3, and email transport schemas are not part of this package.
A host that integrates network I/O must register typed adapters backed by CNet/CHTTP
and must own connection, protocol, credential, backpressure, and shutdown state.
TurboFlow provides no in-tree transport fallback.

The planned CNet/CHTTP integration is tracked by GitHub issues #5, #6, and #7.

## Codec Adapter Schemas

Current codec adapters are line framing, length-prefix framing, and DataBind.

Line codec:

```text
kind: codec
role: transform
max_frame_size        size, optional
delimiter             enum(lf, crlf), required/default lf
strip_delimiter       bool, optional
```

Length-prefix codec:

```text
kind: codec
role: transform
max_frame_size        size, optional
prefix                enum(le32, le64, be32, be64), required/default le32
```

MVP constraint:

- One input message produces one downstream message.
- Multiple complete frames in one payload return an error instead of silently
  dropping or buffering trailing records.
- Streaming multi-frame expansion requires a separate fan-out/message expansion
  design.

## Storage Adapter Schemas

File source:

```text
kind: file
role: source
resource_uid          string, required
owner_name            string, required
path                  path, required
max_payload_size      size, optional
encoding              enum(bin, utf8), optional, default bin
```

Directory source:

```text
kind: file
role: source
resource_uid          string, required
owner_name            string, required
path                  path, required
max_payload_size      size, optional
max_files             size, optional
encoding              enum(bin, utf8), optional, default bin
```

The directory source scans one directory once, publishes regular files only,
does not recurse, and does not persist cursors or delete files.

File sink:

```text
kind: file
role: sink
resource_uid          string, required
owner_name            string, required
path                  path, required
max_payload_size      size, optional
encoding              enum(bin, utf8), optional, default bin
write_mode            enum(truncate, append, create_new), optional, default truncate
fsync                 bool, optional
```

Append-log sink:

```text
kind: file
role: sink
resource_uid          string, required
owner_name            string, required
path                  path, required
max_payload_size      size, optional
encoding              enum(bin, utf8), optional, default bin
record_mode           enum(line, length_prefixed_le64), optional, default line
fsync                 bool, optional
```

SQLite sink:

```text
kind: sqlite
role: sink
resource_uid          string, required
owner_name            string, required
path                  path, required
statement             string, required, exactly one bind parameter
binary_payload        bool, optional
busy_timeout_ms       duration_ms, optional
```

The SQLite sink executes one fixed parameterized statement per message and
binds the message payload to parameter 1 as TEXT or BLOB. It does not build SQL
from payload data or infer columns from parsed/DataBind fields.

## Observe Summary Adapter Schema

`TurboFlow::Observe` registers an explicit terminal summary sink:

```text
kind                    observe
role                    sink
max_preview_bytes       size, optional, min 0, max 256
include_payload_preview bool, optional, default false
write                   host callback, required, not serializable
write_ctx               host object, optional, not serializable
```

Payload content is redacted unless preview is explicitly enabled. Enabled
preview is bounded and hex-encoded. The host callback owns log level and sink
policy; the adapter does not emit per-message INFO logs.

## Schedule Source Adapter Schema

`TurboFlow::Schedule` registers source-only time events:

```text
kind              schedule
role              source
mode              enum(interval, one_shot, cron), required
delay_ms          duration_ms, interval/one_shot only, min 1
repeat_limit      u64, interval only, optional; 0 means unbounded
cron_expression   string, cron only, required
catch_up_limit    u32, cron only, optional, max 1024
payload           string, optional
payload_len       size, optional, max 65536
manual_clock      bool, cron only, optional, default false
```

Mode-specific combinations are validated by the concrete config registration
API before adapter allocation. Cron text is parsed by
`turbo_cron_parse_ex()` during registration. At most one due tick plus
`catch_up_limit` additional due ticks are emitted in chronological order. One
additional match detects truncation, after which the remaining range is skipped
without an unbounded scan. Configuration remains host-owned and no protocol
polling is hidden behind a schedule source. Local-time and DST behavior is
documented in `schedule/README.md`.


## DataBind Adapter Schema

DataBind options:

```text
kind: databind
role: transform
schema_path           schema_path, exactly one of schema_path/schema_text
schema_text           schema_text, exactly one of schema_path/schema_text
schema_text_len       size, optional
type_name             string, required
input_format          enum(bin, json, csv, xml), required
validate_only         bool, optional
bind_all              bool, optional
csv_row               size, optional
xml_xpath             string, optional
max_payload_size      size, optional
```

Ownership:

- When `validate_only` is false, the adapter attaches a DataBind value through
  `turbo_flow_msg_bind_projection()`.
- The projection provider supplies clone/destroy hooks; no public raw parsed slot exists.
- Downstream stages access values with `turbo_flow_msg_projection()` and follow message lifetime unless
  they explicitly clone through the documented API.

## Registry API

The public C API is declared in `turbo_flow/include/turbo_flow.h`. It includes:

- typed option fields, flags, enum values, and numeric bounds;
- adapter kind, role, and direction metadata;
- `turbo_flow_register_adapter_ex()` for schema-aware registration;
- registry query APIs for adapter count and schema lookup.

Core types include:

```c
typedef enum turbo_flow_option_type_e {
  TURBO_FLOW_OPTION_BOOL = 0,
  TURBO_FLOW_OPTION_U32,
  TURBO_FLOW_OPTION_U64,
  TURBO_FLOW_OPTION_SIZE,
  TURBO_FLOW_OPTION_STRING,
  TURBO_FLOW_OPTION_ENUM,
  TURBO_FLOW_OPTION_DURATION_MS,
  TURBO_FLOW_OPTION_PATH,
  TURBO_FLOW_OPTION_SECRET,
  TURBO_FLOW_OPTION_SCHEMA_TEXT,
  TURBO_FLOW_OPTION_SCHEMA_PATH
} turbo_flow_option_type_t;

typedef enum turbo_flow_adapter_role_e {
  TURBO_FLOW_ADAPTER_SOURCE = 1,
  TURBO_FLOW_ADAPTER_SINK = 2,
  TURBO_FLOW_ADAPTER_TRANSFORM = 4
} turbo_flow_adapter_role_t;
```

File, directory, SQLite, line codec, length codec, DataBind, observe, and schedule
registrations publish schema metadata while continuing to use concrete config
structs for runtime setup. External adapters may publish their own typed schema;
the registry deep-copies field names and enum values.

Compile validation should check:

- Every `.flow` adapter name resolves to a registered adapter.
- The registered adapter role matches the source/stage usage.
- Required options are present in host configuration.
- Enum values and numeric ranges are valid.
- Secret options are not included in diagnostics.
- Codec/DataBind chains referenced by schema metadata resolve to registered
  transform adapters or are expanded by the host before compile.

## DSL Evolution

Do not add inline options to `.flow` as the first step. Keep this as the stable
form:

```flow
stage out adapter "host.output"
```

Config profiles are resolved by the external YAML v1 configuration layer and
are not currently part of the `.flow` grammar. The parser therefore never
accepts a profile reference that the runtime would ignore. Binding a resolved
profile directly from `.flow` requires the proposed parameterized-stage and
`with` expansion. A possible future syntax is:

```flow
stage out adapter "host.output" with "prod.output"
```

Here `prod.output` would name a host config profile, not embed secrets in the DSL.
The resolver may use that profile to select a named adapter config; it must not
merge profile fields into the adapter config. Hosts needing deployment-specific
values must resolve them into a concrete adapter entry before registration.

## Validation Order

1. Parse `.flow` topology and adapter names.
2. Resolve adapter names against the host registry.
3. Validate adapter role against `source` or `stage`.
4. Validate option schemas and host-provided option values.
5. Expand host-declared codec/DataBind chains if the host supports expansion.
6. Compile the stage topology into immutable stage and edge plans.
7. Start adapters and executors only after all validation succeeds.

Fail fast at the first invalid schema or binding. Do not create partially
started adapters when option validation fails.

## Security Rules

- `.flow` must not contain passwords, bearer tokens, TLS private keys, SMTP
  credentials, API keys, or cookie material.
- Secret fields must be redacted before logging or diagnostics.
- Schema text and external schema paths are trusted host configuration, not
  untrusted network input.
- External protocol header mapping and file path mapping require
  explicit allowlists.
- Missing TLS, authentication, size-limit, schema-validation, or payload-bound
  configuration fails validation; the adapter boundary does not supply fallback defaults.

## Documentation Update Rules

When adding an adapter option schema:

- Document its role: source, sink, transform, or a restricted combination.
- List required and optional fields with types and defaults.
- State which fields are secrets.
- State input/output codec and DataBind behavior.
- Add compile/start rejection rules.
- Add tests for missing required options, invalid enum values, invalid sizes,
  and role mismatch.
