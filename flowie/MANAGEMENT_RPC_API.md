# Flowie Management JSON-RPC API

This document is the integration contract for external systems that administer Flowie Control.
It describes the HTTP transport, session authentication, authorization, idempotency model, and
all currently registered JSON-RPC methods.

The endpoint version is carried by the default path, `/v2/control/rpc`. There is no runtime
schema discovery or version negotiation. Deployments may change the path with
`management.rpc_path`, so clients must receive the complete endpoint URL from configuration.

## Integration prerequisites

- Use HTTPS and validate the server certificate. A reverse proxy must preserve `Host`.
- Set `dashboard.enabled: true` if the external system must create its own management session.
  The current session-issuing login route is registered with the Dashboard routes.
- Dashboard-backed login requires `listener.tls.client_auth: none`; configuration rejects
  `client_auth: required` together with `dashboard.enabled: true`.
- Create a local enabled principal in the intended Domain and assign the required reserved
  management role before integrating it.
- Treat management session tokens, human passwords, and generated machine tokens as secrets.
  Do not put them in logs, URLs, request IDs, or error telemetry.

The generated token returned by `control.credential.generate` or
`control.credential.rotate` is an opaque principal credential. It can be provisioned as an MQTT
password or as a Broker-facing service credential according to the principal's purpose and Roles.
It is not a Management RPC bearer token and cannot authenticate this endpoint.

## Endpoint configuration

```yaml
management:
  rpc_path: /v2/control/rpc
  rpc_max_request_size: 65536
  session:
    capacity: 1024
    max_sessions_per_principal: 5
    ttl_seconds: 3600

dashboard:
  enabled: true
```

`rpc_max_request_size` accepts `1024..65536` bytes. The listener request-body limit must be at
least as large. Session TTL is also capped by the authenticated principal expiry. Issuing more
than `max_sessions_per_principal` sessions evicts that principal's oldest session.

## Authentication

### Create a management session

There is currently no JSON login method. A client creates a session through the exact form
contract used by the Dashboard:

```http
POST /v2/control/login HTTP/1.1
Host: control.example.com
Origin: https://control.example.com
Content-Type: application/x-www-form-urlencoded

domain=root-a&principal=integration-admin&password=<url-encoded-password>
```

The `Content-Type` value must be exactly `application/x-www-form-urlencoded`. The form must
contain exactly `domain`, `principal`, and `password`. `Origin` must equal
`https://<Host>`. As an alternative for a same-origin user agent, an absent or `null` Origin is
accepted only with `Sec-Fetch-Site: same-origin`.

| HTTP status | Meaning |
| --- | --- |
| `303` | Success. Capture the `flowie_session` cookie from `Set-Cookie`. |
| `400` | Invalid request shape, content type, size, or origin. |
| `401` | Credentials were rejected or the principal has no management permission. |
| `429` | The bounded local login executor is full. |
| `503` | Login authentication timed out or is temporarily unavailable. |

The success cookie contains a 64-character opaque token and is scoped as
`Path=/v2/control; Secure; HttpOnly; SameSite=Strict`. Clients must use the cookie value
verbatim and must not decode it.

### Authenticate RPC calls

Send the session either as the cookie:

```http
Cookie: flowie_session=<opaque-session-token>
```

or as a bearer token:

```http
Authorization: Bearer <opaque-session-token>
```

If both are present, they must be identical. A missing, expired, evicted, malformed, or unknown
token produces JSON-RPC error `-32001`. The server re-reads the principal's enabled state and
effective reserved roles on every request. Disabling the principal or removing all management
permissions therefore invalidates subsequent use of an existing session.

To revoke a cookie-backed session, send a same-origin `POST /v2/control/logout` with the
`flowie_session` cookie. Bearer-token clients can send the same token as that cookie for logout.

### Third-party backend integration

Third-party platforms call Management RPC from their backend, not directly from a browser hosted
on another origin. Provision one principal in the platform's Domain, label its `principal_type`
as `service` or `human` according to ownership, set a strong textual password, and assign only the
reserved management roles that the integration needs. Do not share `system/admin` with a business
platform.

The backend creates a bounded management session through `/v2/control/login`, captures the
`flowie_session` value, and sends that value as `Authorization: Bearer` on RPC calls. It must renew
the session after expiry and after a Flowie restart. Disabling the principal or removing its
effective management roles invalidates subsequent requests without waiting for session expiry.

For example, a Domain-scoped read-only integration normally receives `viewer`; Group maintenance
receives `viewer` plus `user_admin`; policy maintenance receives `viewer` plus `policy_admin`.
Grant `security_admin` only when the platform owns all administration inside that Domain.

Database-backed service principals with `flowie_auth_client` or `flowie_acl_client` protect Broker
calls to `/v4/authenticate` and `/v4/acl/check`. Their generated credentials are not Management RPC
bearer tokens. The current Management RPC has no durable client-credentials grant; an integration
that requires one must not emulate it by reusing a service or MQTT credential. See
[THIRD_PARTY_INTEGRATION.md](THIRD_PARTY_INTEGRATION.md) for the complete credential boundaries and
onboarding flow.

Because the login route requires an exact same-origin request and the session cookie is
`SameSite=Strict`, a third-party browser application must use its own backend as a BFF. Flowie does
not enable cross-origin browser login or RPC access.

## JSON-RPC transport

- Method: HTTP `POST` only.
- Request media type: `application/json`.
- Protocol: JSON-RPC 2.0 over JSON.
- `params`: an object. Positional arrays are not supported.
- Request `id`: a non-null string or number and unique among client in-flight calls.
- Batch requests, notifications, streaming, and introspection are disabled.
- Unknown parameter names are rejected; clients must not send speculative fields.
- JSON-RPC success and error envelopes normally use HTTP `200`. Inspect `result` versus `error`;
  do not use HTTP status alone to decide whether an RPC operation succeeded.
- Responses include `Cache-Control: no-store`, `Pragma: no-cache`, and
  `X-Content-Type-Options: nosniff`.

Example request:

```json
{
  "jsonrpc": "2.0",
  "method": "control.system.status",
  "params": {"domain_id": "root-a"},
  "id": "status-1"
}
```

Success response:

```json
{
  "jsonrpc": "2.0",
  "result": {
    "domain": "root-a",
    "policy_version": 3,
    "draft_rules": 5,
    "published_rules": 5
  },
  "id": "status-1"
}
```

Error response:

```json
{
  "jsonrpc": "2.0",
  "error": {"code": -32009, "message": "Concurrent update conflict"},
  "id": "user-create-1"
}
```

## Common data rules

### Strings and integers

| Field | Contract |
| --- | --- |
| Domain, principal, and Group IDs | Non-empty string, at most 255 bytes, with no ASCII control bytes. |
| Role ID and `principal_type` | Non-empty string, at most 63 bytes, with no ASCII control bytes. |
| `request_id` | Non-empty string, at most 255 bytes; `control.password.change` allows at most 244 bytes because it derives two internal IDs. |
| `new_password` | `16..4096` bytes after JSON string decoding. |
| `limit` | Integer `1..100`; omitted value defaults to `25`. |
| Cursors, ordinals, versions, and times | Unsigned JSON integers. Fractions and signed values are rejected. |
| Time fields | Unix epoch seconds. `expires_at: 0` means no policy expiry. |

`principal_type` is an application-defined bounded label; existing deployments commonly use
`human`, `device`, or `service`. The RPC layer does not restrict it to that set.

### Domain scope

Most Domain-scoped methods accept optional `domain_id`:

- When omitted, the authenticated session's Domain is used.
- A `system_admin` authenticated in the `system` Domain may name another existing Domain.
- Other callers receive `-32003` when selecting a different Domain.
- A missing selected Domain produces `-32004`; the server never falls back to the login Domain.
- The audit actor always comes from the authenticated session. It cannot be supplied in params or
  headers.

### Write idempotency

Every write requires `request_id`, a business idempotency key independent of the JSON-RPC `id`.
The public API does not expose or accept store revisions. In particular, `expected_revision` is
an unknown parameter and is rejected with `-32602`.

For normal writes, retry the same logical command with the same `request_id` after an uncertain
transport outcome. A committed retry returns `replayed: true`. Never reuse a request ID for
another operation, target, actor, or payload.

Credential generation is deliberately different because plaintext secrets are not persisted. If
a generate/rotate call committed but its response was lost, retrying the request ID returns
`-32010`, not the secret. Reconcile by rotating with a new request ID and store the newly returned
secret atomically in the external secret manager.

An account restricted by `password_change_required` can only change its own password. It does not
need to read system status before submitting `control.password.change`.

### Pagination

List methods use exclusive keyset cursors and return:

```json
{"items": [], "has_more": false}
```

For the first page, omit the cursor. When `has_more` is true, obtain the next cursor from the last
item in the current page:

| Method family | Cursor parameter | Last-item field |
| --- | --- | --- |
| Domain list | `after` | `domain_id` |
| User, Group, Role lists | `after` | `id` |
| Policy rule list | `after_ordinal` | `ordinal` |
| Audit list | `after` | `cursor` |

Do not use page numbers or reuse a cursor across Domains.

## Authorization roles

Reserved roles are exact, case-sensitive strings:

| Role | Permission |
| --- | --- |
| `viewer` | Status and read/list/effective/validation methods. |
| `user_admin` | User, Group, and Group membership writes. It does not imply `viewer`. |
| `policy_admin` | Policy draft writes and publish. It does not imply `viewer`. |
| `security_admin` | All Domain-scoped management operations, including credential, password, role, audit, user, Group, and policy operations. |
| `system_admin` | All operations when effective in the `system` Domain, plus Domain creation/listing and explicit cross-Domain administration. It is ignored outside `system`. |
| `password_change_required` | Masks all other permissions until the caller changes its own password. |

In the method tables below, the listed role is the minimum specialized permission.
`security_admin` and effective `system_admin` are also accepted for Domain-scoped methods.

## Result types

Most writes return a command result:

```json
{"replayed": false}
```

`replayed` reports whether an earlier command with the same `request_id` supplied the result.

A User is:

```json
{
  "id": "device-1",
  "type": "device",
  "enabled": true,
  "created_at": 1710000000,
  "updated_at": 1710000000
}
```

A Group is:

```json
{
  "id": "operators",
  "parent_id": null,
  "depth": 0,
  "enabled": true
}
```

Domain and Group are separate resource types. A top-level Group has `parent_id: null` and
`depth: 0`; it can be used as a direct membership target.

A Role is:

```json
{"id": "viewer", "enabled": true}
```

## Methods

In parameter signatures, `?` means optional. `domain_id?` follows the scope rules above.
`write` abbreviates required `request_id: string`.

### System and Domains

| Method | Permission | Params | Result / behavior |
| --- | --- | --- | --- |
| `control.system.status` | `viewer` | `domain_id?` | `{domain, policy_version, draft_rules, published_rules}`. |
| `control.auth.external_https.stats` | `security_admin` | none or `{}` | `{enabled:false}` when external HTTPS Auth is disabled; otherwise returns the counter object below. |
| `control.domain.create` | `system_admin` in `system` | `domain_id`, `write` | Command result. `system` is reserved and cannot be created. |
| `control.domain.list` | `system_admin` in `system` | `after?`, `limit?` | Page of `{domain_id}` items. Does not accept a target Domain. |

When enabled, `control.auth.external_https.stats` returns:

```json
{
  "enabled": true,
  "started_requests": 100,
  "in_flight": 2,
  "succeeded": 80,
  "denied": 10,
  "local_overload": 1,
  "remote_overload": 2,
  "remote_server_failures": 1,
  "transport_failures": 2,
  "protocol_failures": 1,
  "local_failures": 1
}
```

Counters are process-lifetime aggregate snapshots, not windowed rates or SLO calculations.
The current composition root rejects `auth.external_https` with `TURBO_ENOTSUP`, so deployed
instances report `{"enabled":false}`. The expanded counter shape is reserved for a future runtime
that explicitly enables the provider; clients must not infer availability from schema presence.

### Users, passwords, and credentials

| Method | Permission | Params | Result / behavior |
| --- | --- | --- | --- |
| `control.user.get` | `viewer` | `domain_id?`, `principal_id` | User object; missing user returns `-32004`. |
| `control.user.list` | `viewer` | `domain_id?`, `after?`, `limit?` | Page of User objects ordered by `id`. |
| `control.user.create` | `user_admin` | `domain_id?`, `principal_id`, `principal_type`, `write` | Command result. It does not create a password or machine credential. |
| `control.user.disable` | `user_admin` | `domain_id?`, `principal_id`, `write` | Command result. The row and audit history remain; current draft or published policy references block disabling. |
| `control.password.set` | `security_admin` | `domain_id?`, `principal_id`, `new_password`, `mode`, `write` | Command result. `mode` is exactly `create` or `replace`. |
| `control.password.change` | `password_change_required`, `security_admin`, or `system_admin` | `new_password`, `request_id` | Changes the authenticated caller's own password and returns a command result. No target or Domain can be supplied. |
| `control.credential.generate` | `security_admin` | `domain_id?`, `principal_id`, `write` | `{token}`; creates the first random machine credential. |
| `control.credential.rotate` | `security_admin` | `domain_id?`, `principal_id`, `write` | `{token}`; replaces and re-enables the machine credential. |
| `control.credential.revoke` | `security_admin` | `domain_id?`, `principal_id`, `write` | Command result; removes credential validity without deleting history. |

`token` is printable ASCII in the form `flw_mqtt_v1_<Base64URL-no-padding>` and is returned only
on the successful, non-replayed generate/rotate response. The prefix identifies the credential
format; it does not by itself select MQTT or service use. Never Base64-decode the token.

For an MQTT principal, send the token bytes unchanged as the MQTT Password. The MQTT User Name maps
to `principal_id`; the MQTT Client Identifier is a separate session/takeover/routing key and has no
credential meaning. Local Auth resolves the User Name across all Domains and accepts only one unique
enabled match. Duplicate User Names in different Domains fail closed, so integrations should allocate
globally unique names.

For a service principal, store the token in the secret provider and send it with
`X-Flowie-Service-Domain` and `X-Flowie-Service-Id`. Exact Role `flowie_auth_client` permits
`POST /v4/authenticate`; exact Role `flowie_acl_client` permits `POST /v4/acl/check`. These are
Broker-facing endpoint Roles and grant no Management RPC permission. `service_domain` locates the
service credential only; it does not constrain the business Domain returned for an MQTT principal.
Provisioning examples are in [THIRD_PARTY_INTEGRATION.md](THIRD_PARTY_INTEGRATION.md).

`control.password.set` does not return the password and does not silently switch between create and
replace modes.

### Groups and membership

| Method | Permission | Params | Result / behavior |
| --- | --- | --- | --- |
| `control.group.list` | `viewer` | `domain_id?`, `after?`, `limit?` | Page of Group objects ordered by `id`. |
| `control.group.create` | `user_admin` | `domain_id?`, `group_id`, `parent_group_id?`, `write` | Command result. Omit the parent for a top-level Group; otherwise the parent must exist, be enabled, and remain within maximum depth 15. |
| `control.group.delete` | `user_admin` | `domain_id?`, `group_id`, `write` | Permanently deletes an enabled or disabled Group. Any child, direct membership, or policy reference blocks deletion. |
| `control.group.member.add` | `user_admin` | `domain_id?`, `principal_id`, `group_id`, `write` | Command result. Adds direct membership; effective ancestors are derived. |
| `control.group.member.remove` | `user_admin` | `domain_id?`, `principal_id`, `group_id`, `write` | Command result. Removes one direct assignment. |
| `control.group.effective` | `viewer` | `domain_id?`, `principal_id` | Array of direct and inherited Group ID strings. The Domain is not a Group and is not included. |

The effective Group array is bounded to 16 entries.

### Roles and assignments

| Method | Permission | Params | Result / behavior |
| --- | --- | --- | --- |
| `control.role.list` | `viewer` | `domain_id?`, `after?`, `limit?` | Page of Role objects ordered by `id`. |
| `control.role.create` | `security_admin` | `domain_id?`, `role_id`, `write` | Command result. |
| `control.role.disable` | `security_admin` | `domain_id?`, `role_id`, `write` | Command result. Existing assignments remain but become ineffective; policy references block disabling. |
| `control.role.assign` | `security_admin` | `domain_id?`, `principal_id`, `role_id`, `write` | Command result. Adds a direct role assignment. |
| `control.role.remove` | `security_admin` | `domain_id?`, `principal_id`, `role_id`, `write` | Command result. Removes a direct assignment. |
| `control.role.effective` | `viewer` | `domain_id?`, `principal_id` | Array of enabled direct Role ID strings. |

The effective Role array is bounded to 8 entries. Only the reserved role names listed under
Authorization Roles grant Management RPC permissions; other roles remain application roles.

### Policy draft and publishing

| Method | Permission | Params | Result / behavior |
| --- | --- | --- | --- |
| `control.policy.status` | `viewer` | `domain_id?` | `{policy_version, expires_at, draft_rules, published_rules}`. |
| `control.policy.rule.list` | `viewer` | `domain_id?`, `after_ordinal?`, `limit?` | Page of `{ordinal, rule_line, updated_at}` ordered by ordinal. |
| `control.policy.rule.put` | `policy_admin` | `domain_id?`, `ordinal`, `rule_line`, `request_id` | Command result. Inserts or replaces the stable ordinal. |
| `control.policy.rule.delete` | `policy_admin` | `domain_id?`, `ordinal`, `request_id` | Command result. Other ordinals are not renumbered. |
| `control.policy.validate` | `viewer` | `domain_id?` | `{rule_count, deny_rule_count}` without modifying state. |
| `control.policy.publish` | `policy_admin` | `domain_id?`, `request_id`, `expires_at?` | `{policy_version, replayed}`. Publishes an immutable generation atomically. |

`ordinal` is `0..4095`. Despite its historical name, `rule_line` now contains one complete canonical
user ACL document, not one pipe-delimited internal rule. The Management JSON-RPC boundary accepts at
most 2047 bytes. For example:

```text
user device-7 allow {
  write topic root-a/groups/operators/devices/%u/{event,heartbeat}
  read topic root-a/groups/operators/devices/%c/command
  deny readwrite topic root-a/groups/operators/devices/%u/private
}
```

Each enabled user may have only one draft ACL document in a Domain. The document's embedded Domain
must match the selected RPC Domain, and all named groups must form an existing enabled parent chain.
`read` authorizes SUBSCRIBE, `write` authorizes PUBLISH, `%u` matches the MQTT username, and `%c`
matches the MQTT client ID. See [ACL_GRAMMAR.md](ACL_GRAMMAR.md) for the complete grammar, canonical
format, topic tree, limits, deny precedence, and UI/RPC publishing workflow.

### Audit

| Method | Permission | Params | Result / behavior |
| --- | --- | --- | --- |
| `control.audit.list` | `security_admin` | `domain_id?`, `after?`, `limit?` | Page of Audit objects ordered by an opaque numeric cursor. |

Each item is:

```json
{
  "request_id": "user-create-202",
  "actor": "integration-admin",
  "operation": "user.create",
  "target": "device-202",
  "cursor": 43,
  "occurred_at": 1710000100
}
```

Audit reads are Domain-scoped. Treat `cursor` only as the exclusive keyset cursor returned by the
service; gaps are expected and its numeric value has no business meaning.

## Error codes

| Code | Message | Meaning and client action |
| --- | --- | --- |
| `-32700` | `Invalid request` | Malformed JSON. Fix the request; do not retry unchanged. |
| `-32600` | `Invalid request`, `Invalid request size`, `Batch requests are disabled`, or `Notifications are disabled` | Invalid JSON-RPC envelope, missing ID, batch, notification, or size violation. |
| `-32601` | `Method not found` | Unknown or unavailable method. Do not retry unchanged. |
| `-32602` | `Invalid params` | Missing, unknown, wrongly typed, out-of-range, or domain-invalid params. |
| `-32603` | `Internal error` | Internal or currently unclassified domain failure. Reconcile by reading state before deciding whether a retry is safe. |
| `-32001` | `Authentication required` | Session token is absent or invalid. Log in again. |
| `-32003` | `Forbidden` | The current principal, role set, login Domain, or target Domain is not authorized. |
| `-32004` | `Not found` | The selected Domain or requested object does not exist. |
| `-32009` | `Concurrent update conflict` | A concurrent command or state constraint prevented the update. Re-read the affected resource before deciding whether to retry. |
| `-32010` | `Credential token is unavailable; use a new request_id` | A machine credential token cannot be returned, commonly because generation already committed. Rotate with a new request ID when reconciliation requires a known token. |

Errors intentionally expose little domain detail. Use `control.system.status`, the relevant read
method, and `control.audit.list` to reconcile uncertain state. A transport timeout is not evidence
that a write failed; apply the idempotency rules before retrying.

## End-to-end write example

Create a user with a stable business request ID:

```json
{
  "jsonrpc": "2.0",
  "method": "control.user.create",
  "params": {
    "domain_id": "root-a",
    "principal_id": "device-202",
    "principal_type": "device",
    "request_id": "provisioning-8f6f-user"
  },
  "id": "user-create-1"
}
```

A first successful response is:

```json
{"jsonrpc":"2.0","result":{"replayed":false},"id":"user-create-1"}
```

If the response was lost, resend the same logical params and `request_id` with a new JSON-RPC
`id`. A committed retry returns `replayed: true`.
