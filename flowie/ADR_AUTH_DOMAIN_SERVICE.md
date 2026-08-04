# ADR: Control ownership, bundled server deployment, and Domain-scoped administration

## Status

Accepted as a breaking contract change. This decision clarifies that Auth/ACL belong to Control, that
the standalone `flowie-server` Docker image bundles Flowie server and Control, and that Domain
replaces the legacy Root Group concept in the public administration model.

## Context

The current implementation combines four different concerns under `flowie-control`:

- credential verification for MQTT clients;
- generic user, Role, Group, policy, and audit administration;
- a Flowie-specific Root Group name in the public RPC and Dashboard;
- a Control runtime embedded in the standalone `flowie-server` deployment.

That combination makes a general identity service look like a private Flowie database. It also
makes store reset and declarative import appear to be exceptional recovery procedures even though
they are normal lifecycle operations for development, testing, tenant bootstrap, and controlled
environment replacement.

## Decision

### Service boundary

Control is an independently bounded platform component. Auth and ACL are Control capabilities;
Flowie is one consumer, and another platform may use the same authentication and management
contracts without linking Flowie or receiving Flowie database coordinates.

```text
platform client ---- HTTPS authenticate ----> Control Auth ----> Control Repository
operator UI/RPC ---- HTTPS management ------> Control ---------> Control Repository
Flowie broker ------ HTTPS authenticate/ACL -> Control ---------> Control Repository
```

The standalone `flowie-server` Docker image is a deliberate deployment bundle containing Flowie
server and Control. The current composition root may create and supervise both runtimes in one
process, but module ownership remains separate: Control exclusively owns its Repository, Auth/ACL,
Management RPC, and UI; Flowie server owns MQTT and ProtocolStore state. Bundling lifecycle does not
allow either module to bypass the other's public service/repository boundary.

Cluster deployments may choose a different process/container topology later, but they must preserve
the same ownership and protocol contracts. Container topology is not a state-ownership decision.

### Public Domain model

The public tenant and authorization root is `Domain`, identified by `domain_id`. A Domain contains:

- users and machine principals;
- a bounded hierarchy of Groups;
- Roles and assignments;
- policy drafts, published policy bundles, and audit history.

The Domain itself is not presented as a Group. A Group has either another Group as its parent or
the Domain as its parent. Existing depth and cycle constraints continue to apply. UI trees render
the Domain as the tree root and Group objects as selectable descendants; membership never targets
the Domain root.

Canonical Management RPC methods use the `control.*` namespace. In particular,
`control.domain.create` and `control.domain.list` replace the public Root Group operations, and scoped
methods accept `domain_id`.

### Contract replacement

The new release removes the legacy `flowie.*` Management RPC namespace, `root_group` login field,
`root_group_id` RPC/config fields, and Root Group database contract. Only `control.*`, `domain`,
and `domain_id` are accepted. Unknown legacy methods and fields fail fast.

There is no online compatibility adapter and no dual-read or dual-write period. Existing stores are
exported before upgrade and rebuilt through the reset/import workflow. This keeps one fact model and
avoids carrying two tenant vocabularies through every domain command.

### Store lifecycle

Reset and import are first-class offline operator operations:

1. stop all writers;
2. verify the exact SQLite file or PostgreSQL schema target;
3. create a backup unless an explicit disposable-store flag is supplied;
4. reset the selected Auth Repository as one unit;
5. start the service in schema migration mode when required;
6. run the fixed first-login password change;
7. apply a versioned declarative Domain manifest through Management RPC;
8. verify users, Group hierarchy, Roles, and policy status.

Reset never deletes an entire shared Docker volume and never performs unordered per-table deletes.
Import uses domain commands and audit boundaries rather than direct data-table writes. Credentials
are injected from a secret provider and are not stored in the manifest.

## Consequences

- The standalone Docker remains operationally simple while Control keeps an explicit module and
  data boundary inside the bundle.
- Control Repository data is owned by Control, not FlowStore, ProtocolStore, or a Flowie worker.
- A third-party platform can use the same Domain administration model without Flowie terminology.
- The release is intentionally incompatible with old RPC clients, configuration, and Control
  Repository schemas; deployment tooling must gate the coordinated upgrade.
- The bundled Docker must expose Control HTTPS through the configured reverse proxy and provision
  least-privilege service bindings for each consuming platform.

## Migration and rollback

Migration order is fixed:

1. stop all writers and export each existing Domain as a versioned Domain manifest;
2. reset the Control Repository using the exact-target operator tool;
3. deploy the bundled server/Control version with the Domain schema;
4. change Flowie and third-party consumers to `domain_id` and `control.*`;
5. import manifests and verify Domain, Group, Role, policy, and audit state before reopening traffic.

Rollback stops all writers and restores the previous bundled server image together with the complete
pre-upgrade Control Repository backup. A Domain-schema Repository cannot be opened by the previous
binary and is never converted in place.

## Verification

- server application tests prove the bundled Control runtime starts before MQTT and shuts down in
  the documented order;
- Control runtime tests continue to cover SQLite and PostgreSQL providers independently;
- Management RPC contract tests cover canonical Domain methods, rejection of every removed legacy
  method/field, permissions, idempotent replay, and concurrent-update conflicts;
- Dashboard tests require Domain terminology and hierarchical Group selection;
- reset/import scripts have dry-run, exact-target, backup, replay, and invalid-manifest tests;
- deployment checks run the bundled Docker and verify Control HTTPS access from both Flowie and a
  non-Flowie client.
