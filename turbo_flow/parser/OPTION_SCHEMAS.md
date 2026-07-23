# Turbo Flow Option Schemas

This file defines the design boundary for typed options attached to `.flow`
adapter bindings. It is a schema design document, not a generated source input.

## Scope

`.flow` owns topology:

```flow
source http_in adapter "http.server"
stage decode adapter "codec.length"
stage bind adapter "databind.order"
stage email_out adapter "smtp"

stage main {
  http_in -> decode -> bind -> email_out
}
```

The host owns typed configuration for each adapter name:

```text
"http.server"    -> HTTP source adapter options
"codec.length"   -> length-prefix codec options
"databind.order" -> DataBind schema options
"smtp"           -> SMTP sink adapter options
```

This keeps secrets, deployment-specific endpoints, TLS material, and large
schemas out of `.flow` text while still letting the compiler validate that every
referenced adapter has a registered schema and runtime binding.

Binding names may be quoted or written as adjacent dotted segments:

```flow
source events adapter http.client.poll
stage decode adapter codec.json.in
stage store adapter "s3.archive.prod"
```

These are registry binding names, not inline protocol configuration.

## Terms

Executor:

- Selects one of the three core compute executors: inline, thread, or coro.
- Worker handoff and adapter-owned socket/CoroNet placement are separate runtime
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
kind              socket | http | rpc | s3 | email | file | sqlite | codec | databind | custom
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
source socket_in adapter "socket.tcp.in"
stage frame_in adapter "codec.length.in"
stage bind_order adapter "databind.order.in"
stage encode_out adapter "codec.length.out"
stage socket_out adapter "socket.tcp.out"

stage main {
  socket_in -> frame_in -> bind_order -> encode_out -> socket_out
}
```

This stage plan is more debuggable than hiding all processing inside the socket
adapter, and it keeps codec/DataBind errors attached to clear stages.

## Socket Adapter Schema

Current implemented socket adapter fields come from
`turbo_flow_coronet_socket_config_t`.

Kind:

```text
kind: socket
role: source | sink
```

Common options:

```text
role                  enum(source, sink), required
transport             enum(tcp, udp, kcp, tls, ws, wss, pipe), required
kcp_fec               bool, optional, KCP only
kcp_fec_backend       enum(none, wirehair), optional, required when kcp_fec is true
kcp_fec_data_shards   u32, KCP FEC only, min 1, max 256
kcp_fec_parity_shards u32, KCP FEC only, min 1, max 256
kcp_fec_max_payload_size u32, KCP FEC only, min 1, max 65535
host                  string, required for tcp/udp/kcp/tls/ws/wss
port                  u32, required for tcp/udp/kcp/tls/ws/wss, min 1, max 65535
path                  string, optional, WS/WSS sink path or Pipe endpoint, default "/"
timeout_ms            duration_ms, optional
connect_timeout_ms    duration_ms, optional, falls back to timeout_ms when unset
send_timeout_ms       duration_ms, optional, falls back to timeout_ms when unset
recv_timeout_ms       duration_ms, optional, falls back to timeout_ms when unset
handshake_timeout_ms  duration_ms, optional, falls back to timeout_ms when unset (WS/TLS)
reuse_port            bool, source/listener only
tcp_keepalive         bool, optional, TCP/TLS/WS/WSS only
tcp_keepalive_idle_ms duration_ms, optional, requires tcp_keepalive
tcp_keepalive_interval_ms duration_ms, optional, requires tcp_keepalive
tcp_keepalive_count   u32, optional, requires tcp_keepalive
linger                bool, optional, TCP/TLS/WS/WSS only
linger_ms             duration_ms, optional, requires linger; 0 means abortive close
send_hwm_bytes        size, optional, TCP/TLS/WS/WSS/Pipe only
udp_multicast_group   string, optional, UDP source only; joined after bind
udp_multicast_interface string, optional, requires udp_multicast_group
udp_option_flags      u32, optional, explicit loop/TTL/broadcast presence bits
udp_multicast_loop    bool, optional, UDP only, requires loop presence bit
udp_multicast_ttl     u32, optional, UDP only, min 0, max 255, requires TTL presence bit
udp_broadcast         bool, optional, IPv4 UDP only, requires broadcast presence bit
max_pump_iterations   u32, optional
context               host object, optional, not serializable
take_context_ownership bool, optional
```

Source behavior:

- Listens on `host:port`, or on the configured Pipe endpoint for `pipe`.
- Publishes received bytes as flow messages.
- May own and drive a CoroNet context when the host does not supply one.

Sink behavior:

- Connects to `host:port`, or to the configured Pipe endpoint for `pipe`.
- Sends the current message payload.
- Requires `consume` behavior; missing consume support is a compile/start error.

Codec/DataBind integration:

- Prefer explicit codec/DataBind stages after socket sources and before socket
  sinks.
- Embedded input/output codec chains are allowed only as host-side expansion
  into equivalent stages or as adapter-owned behavior with identical error
  semantics.

## HTTP Adapter Schema

`TurboFlow::HttpServer` implements HTTP servers with Iris and
`TurboFlow::HttpClient` implements clients with TurboHTTP. `TurboFlow::Http`
remains a compatibility aggregate linking both components. The server is one
source/reply-sink boundary. The client is a transform or a periodic source.

HTTP server options:

```text
port                  u32, required, min 1, max 65535
route                 string, required
method                enum(GET, POST, PUT, PATCH, DELETE), required
max_body_size         size, optional
response_status       u32, optional, default 200
response_content_type string, optional
context               host object, optional, not serializable
app                   host object, optional, not serializable
```

HTTP client options:

```text
client                host object (http_client_t), optional, not serializable
url                   string, required
method                enum(GET, POST, PUT, PATCH, DELETE), required
headers               string-list, optional
bearer_token          secret, optional
timeout_ms            duration_ms, optional
max_response_size     size, optional
success_status_min    u32, optional, default 200
success_status_max    u32, optional, default 299
max_pump_iterations   u32, optional
poll_interval_ms      duration_ms, optional
```

With `poll_interval_ms == 0`, input payload becomes the request body and a
successful response body replaces it. With a non-zero interval, only GET is
accepted and each response is published as a source message. Transport and
status failures publish an empty message with a non-success `msg.status`.
When `client` is supplied, the adapter uses that TurboHTTP client directly;
ownership transfer is controlled by the C config and polling requires exclusive
context driving while the adapter is running. No standalone coroutine context
is part of the HTTP client adapter contract.
Authentication values are secret. Codec/DataBind processing remains explicit
before or after the adapter.

## FMQ Adapter Schema

`TurboFlow::FMQ` implements ZeroMQ-like primitives over its own CoroNet
transport framing protocol. It is not ZeroMQ wire compatible.

```text
pattern               enum(pub, sub, push, pull, router, dealer, pair), required
mode                  enum(bind, connect), required
transport             enum(tcp, tls, udp, kcp, pipe, ws, wss), required
kcp_fec               bool, optional, KCP only
kcp_fec_backend       enum(none, wirehair), optional, required when kcp_fec is true
kcp_fec_data_shards   u32, KCP FEC only, min 1, max 256
kcp_fec_parity_shards u32, KCP FEC only, min 1, max 256
kcp_fec_max_payload_size u32, KCP FEC only, min 1, max 65535
host                  string, required except pipe
port                  u32, required except pipe, min 1, max 65535
path                  string, pipe endpoint or WS/WSS path, optional, default "/"
topic                 string, optional, PUB topic or SUB prefix, max 1024 bytes
identity              string, required for DEALER, max 255 bytes
topic_policy          enum(static, inherit), optional, default static
identity_policy       enum(static, inherit), optional, default static
max_frame_size        size, optional, default 8 MiB, max UINT32_MAX
max_connections       u32, bind only, optional, default 1024, max 65535
timeout_ms            duration_ms, optional, default 1000
connect_timeout_ms    duration_ms, optional, falls back to timeout_ms when unset
send_timeout_ms       duration_ms, optional, falls back to timeout_ms when unset
recv_timeout_ms       duration_ms, optional, falls back to timeout_ms when unset
handshake_timeout_ms  duration_ms, optional, falls back to timeout_ms when unset (WS/WSS)
reconnect_initial_ms  duration_ms, optional, default 1000 when unset or zero
reconnect_max_ms      duration_ms, optional, default 30000 when unset or zero
heartbeat_interval_ms duration_ms, optional, requires heartbeat_timeout_ms
heartbeat_timeout_ms  duration_ms, optional, requires heartbeat_interval_ms, must be >= interval
event_callback        host object, optional, not serializable
event_ctx             host object, optional, not serializable
reuse_port            bool, bind/listener only
tcp_keepalive         bool, optional, TCP/TLS/WS/WSS only
tcp_keepalive_idle_ms duration_ms, optional, requires tcp_keepalive
tcp_keepalive_interval_ms duration_ms, optional, requires tcp_keepalive
tcp_keepalive_count   u32, optional, requires tcp_keepalive
linger                bool, optional, TCP/TLS/WS/WSS only
linger_ms             duration_ms, optional, requires linger; 0 means abortive close
send_hwm_bytes        size, optional, TCP/TLS/WS/WSS/Pipe only
udp_multicast_group   string, optional, UDP bind only; joined after bind
udp_multicast_interface string, optional, requires udp_multicast_group
udp_option_flags      u32, optional, explicit loop/TTL/broadcast presence bits
udp_multicast_loop    bool, optional, UDP only, requires loop presence bit
udp_multicast_ttl     u32, optional, UDP only, min 0, max 255, requires TTL presence bit
udp_broadcast         bool, optional, IPv4 UDP only, requires broadcast presence bit
frame_hwm_messages    size, optional, 0 disables FMQ in-flight frame count cap
frame_hwm_bytes       size, optional, 0 disables FMQ in-flight encoded-byte cap, min 16 when set
frame_admission_policy enum, optional, fail|block, default fail
frame_admission_timeout_ms duration_ms, optional, BLOCK only; 0 immediate, UINT64_MAX unbounded
frame_linger_ms       duration_ms, optional, default 0, stop-time drain for in-flight frames
context               host object, optional, not serializable
take_context_ownership bool, required when context is supplied
```

Pairings are strict: PUB(bind)/SUB(connect), PUSH(bind)/PULL(connect),
ROUTER(bind)/DEALER(connect), and PAIR with opposite modes. PUB/PUSH are sinks,
SUB/PULL are sources, and ROUTER/DEALER/PAIR may bind one source plus one sink
to the same adapter name. A supplied CoroNet context must transfer ownership;
host-owned contexts are rejected because shutdown cannot otherwise prove that
posted sends have drained before adapter destruction.

`topic_policy = inherit` and `identity_policy = inherit` copy metadata from the
current FMQ input message into an outbound FMQ frame. This enables stage-level
broker compositions such as `SUB -> codec/DataBind/filter -> PUB` while keeping
pattern selection out of the DSL. Inherit policies require synchronous FMQ
input metadata; using them on a message that did not originate from FMQ fails.

FMQ moves opaque payload bytes only. Place codec and DataBind transform stages
explicitly around it. Incoming topic/identity metadata is borrowed for the
current synchronous dispatch and is not a DataBind object.

`event_callback` receives FMQ-local control events for peer connection state,
reconnect state, heartbeat timeout, frame sent, HWM reached, and frame dropped.
Hosts can bridge those events into `TurboFlow::Observe` with
`turbo_flow_observe_record_control_event()`.

## RPC Adapter Schema

`TurboFlow::RPC` uses TurboHTTP JSON-RPC and Iris. The client fields are:

```text
url                   string, required
method                string, required
bearer_token          secret, optional
timeout_ms            duration_ms, optional
poll_params           string (JSON), optional, default null
poll_interval_ms      duration_ms, optional
```

Without polling, input is params JSON and output is result JSON. With polling,
the configured params are sent repeatedly and every result is published by a
source adapter.

## S3 Adapter Schema

`TurboFlow::S3` is a fixed-object TurboHTTP S3 client:

```text
host                  string, required
port                  u32, required, min 1, max 65535
use_https             bool, optional
region                string, required
virtual_style         bool, optional
bucket                string, required
object                string, required
content_type          string, optional, default application/octet-stream
credentials           enum(static, aws_env, minio_env), required
access_key            secret, required for static credentials
secret_key            secret, required for static credentials
session_token         secret, optional
max_pump_iterations   u32, optional
poll_interval_ms      duration_ms, optional
```

Without polling, it is a PutObject output sink. With polling, it repeatedly
downloads the configured object and publishes an input message. TLS, region,
and URL style are explicit and never silently changed. Credential providers are
owned and destroyed by the adapter.

## Email Adapter Schema

Current implemented email adapter is an SMTP sink.

Kind:

```text
kind: email
role: sink
```

SMTP sink options:

```text
host                  string, required
port                  u32, required, min 1, max 65535
use_tls               bool, optional
use_starttls          bool, optional
auth_method           enum(none, plain, login, cram_md5), optional
username              string, optional
password              secret, optional
timeout_ms            duration_ms, optional
from_name             string, optional
from_email            string, required
to_name               string, optional
to_email              string, required
subject               string, optional
html_body             bool, optional
max_pump_iterations   u32, optional
context               host object, optional, not serializable
take_context_ownership bool, optional
```

MIME callback transform options:

```text
callbacks             host object, required, not serializable
pool_size             size, optional
```

MIME owned extract transform options:

```text
max_payload_size      size, optional, default 16 MiB
max_headers           size, optional, default 1024, total across root and parts
max_parts             size, optional, default 256
max_decoded_bytes     size, optional, default 32 MiB, combined root and parts
pool_size             size, optional, default 16 KiB
```

The extract transform preserves the raw payload and installs an opaque,
message-owned parsed result. Header names/values, metadata, and transfer-decoded
bodies are copied out of the parser pool. Downstream stages read them through
the `turbo_flow_email_mime_*` accessors; those views remain valid only while the
flow message is alive. Parse, decode, and quota failures do not replace an
existing parsed result.

RFC 2822/MIME encode transform options:

```text
from_name             string, optional
from_email            string, required
to_name               string, optional
to_email              string, required
subject               string, optional
html_body             bool, optional
alternative_text      string, optional, valid only with html_body
priority              u32, optional, 0 normal, 1 high, 2 low
attachments           host object, optional, not serializable
attachment_count      size, optional, max 1024
max_payload_size      size, optional, default 16 MiB
max_output_size       size, optional, default 64 MiB
pool_size             size, optional, default 16 KiB
```

RFC 2557 MHTML encode transform options:

```text
charset               string, optional, default utf-8
resources             host object, optional, not serializable
resource_count        size, optional, max 1024
max_payload_size      size, optional, default 16 MiB
max_output_size       size, optional, default 64 MiB
pool_size             size, optional, default 16 KiB
```

The MIME encoder maps payload to an explicitly selected plain-text or HTML
body. The MHTML encoder maps payload to the root HTML entity. Attachment and
resource descriptors are deep-copied during adapter registration; their data,
media type, filename/location, and content ID are never inferred from arbitrary
parsed/DataBind fields. Both transforms replace payload only after successful,
bounded serialization.

Email input:

- POP3/IMAP are polling client sources, not email servers.
- They use separate configured client adapters under the `email` kind.
- Mailbox polling, durable cursors, deletion policy, and retry policy belong to
  the adapter/product layer.
- Raw messages should enter an explicit MIME codec stage backed by
  `TurboNet::MimeParser`; TurboFlow must not implement a second MIME parser.
- The owned MIME extract adapter copies selected parser data into a typed schema projection before
  returning; core releases it through the projection ownership contract. The lower-level callback adapter
  still exposes views only during `consume`.

POP3 source options:

```text
host                  string, required
port                  u32, required, min 1, max 65535
use_tls               bool, optional
use_stls              bool, optional
username              string, required
password              secret, required
timeout_ms            duration_ms, optional
poll_interval_ms      duration_ms, optional
max_messages          size, optional
delete_after_fetch    bool, optional
max_pump_iterations   u32, optional
context               host object, optional, not serializable
take_context_ownership bool, optional
```

IMAP source options:

```text
host                  string, required
port                  u32, required, min 1, max 65535
use_tls               bool, optional
use_starttls          bool, optional
username              string, required
password              secret, required
mailbox               string, optional, default INBOX
search_query          string, optional, default ALL
use_uid               bool, optional
timeout_ms            duration_ms, optional
poll_interval_ms      duration_ms, optional
max_messages          size, optional
max_pump_iterations   u32, optional
context               host object, optional, not serializable
take_context_ownership bool, optional
```

Codec/DataBind integration:

- SMTP sink default output is message payload as body.
- Structured MIME/MHTML output reuses the existing TurboNet Email and
  `TurboNet::MimeParser` builder APIs.
- Structured email construction from DataBind fields should be explicit adapter
  mapping, for example body field, subject field, and recipient field.
- Do not infer email headers from arbitrary parsed data without a configured
  allowlist.

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

Socket, HTTP, RPC, S3, SMTP/POP3/IMAP email, file, directory, SQLite, line
codec, length codec, and DataBind registrations publish schema metadata while
continuing to use concrete config structs for runtime setup. The registry
deep-copies field names and enum values.

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
stage out adapter "smtp"
```

Config profiles are resolved by the external YAML v1 configuration layer and
are not currently part of the `.flow` grammar. The parser therefore never
accepts a profile reference that the runtime would ignore. Binding a resolved
profile directly from `.flow` requires the proposed parameterized-stage and
`with` expansion. A possible future syntax is:

```flow
stage out adapter "smtp" with "prod.smtp"
```

Here `prod.smtp` would name a host config profile, not embed secrets in the DSL.
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
- HTTP header mapping, email header mapping, and file path mapping require
  explicit allowlists.
- Fallback defaults must not silently disable TLS, authentication, size limits,
  schema validation, or payload bounds.

## Documentation Update Rules

When adding an adapter option schema:

- Document its role: source, sink, transform, or a restricted combination.
- List required and optional fields with types and defaults.
- State which fields are secrets.
- State input/output codec and DataBind behavior.
- Add compile/start rejection rules.
- Add tests for missing required options, invalid enum values, invalid sizes,
  and role mismatch.
