#include "tinytest.h"
#include "turbo_flow.h"

#include <string.h>

typedef struct managed_boundary_fixture_s {
  turbo_flow_resource_metadata_t metadata;
  turbo_flow_managed_boundary_descriptor_t descriptor;
  turbo_flow_managed_boundary_snapshot_t snapshot;
  int metadata_status;
  int descriptor_status;
  int snapshot_status;
  int advance_generation_after_snapshot;
  int command_calls;
} managed_boundary_fixture_t;

static void managed_boundary_fixture_init(managed_boundary_fixture_t *fixture, const char *uid,
                                          const char *owner_name) {
  memset(fixture, 0, sizeof(*fixture));
  fixture->metadata = (turbo_flow_resource_metadata_t)TURBO_FLOW_RESOURCE_METADATA_INIT;
  fixture->metadata.domain = TURBO_FLOW_DOMAIN_IO_TRANSPORT;
  fixture->metadata.kind = TURBO_FLOW_RESOURCE_CONNECTION;
  memcpy(fixture->metadata.uid, uid, strlen(uid) + 1u);
  memcpy(fixture->metadata.owner_name, owner_name, strlen(owner_name) + 1u);
  fixture->metadata.generation = 7u;
  fixture->metadata.observed_generation = 7u;

  fixture->descriptor =
      (turbo_flow_managed_boundary_descriptor_t)TURBO_FLOW_MANAGED_BOUNDARY_DESCRIPTOR_INIT;
  fixture->descriptor.domain = fixture->metadata.domain;
  fixture->descriptor.kind = fixture->metadata.kind;
  fixture->descriptor.role_flags =
      TURBO_FLOW_MANAGED_BOUNDARY_SOURCE | TURBO_FLOW_MANAGED_BOUNDARY_SINK;
  fixture->descriptor.capability_flags =
      TURBO_FLOW_MANAGED_BOUNDARY_DEMAND_AWARE | TURBO_FLOW_MANAGED_BOUNDARY_DURABLE_SETTLEMENT;
  fixture->descriptor.command_flags =
      TURBO_FLOW_MANAGED_BOUNDARY_COMMAND_QUIESCE | TURBO_FLOW_MANAGED_BOUNDARY_COMMAND_RESUME;
  memcpy(fixture->descriptor.uid, uid, strlen(uid) + 1u);
  memcpy(fixture->descriptor.owner_name, owner_name, strlen(owner_name) + 1u);
  check_equal(turbo_flow_content_descriptor_init(
                  &fixture->descriptor.input, TURBO_FLOW_DOMAIN_PROTOCOL_PATTERN,
                  TURBO_FLOW_CONTENT_PROFILE_PROTOCOL_DATA, TURBO_FLOW_DATA_ENCODING_JSON,
                  "application/json", "managed-input"),
              SALTS_OK);
  check_equal(turbo_flow_content_descriptor_declare_schema(&fixture->descriptor.input,
                                                           "ManagedBoundary", "Input", 1u),
              SALTS_OK);
  check_equal(turbo_flow_content_descriptor_init(
                  &fixture->descriptor.output, TURBO_FLOW_DOMAIN_PROTOCOL_PATTERN,
                  TURBO_FLOW_CONTENT_PROFILE_PROTOCOL_DATA, TURBO_FLOW_DATA_ENCODING_JSON,
                  "application/json", "managed-output"),
              SALTS_OK);
  check_equal(turbo_flow_content_descriptor_declare_schema(&fixture->descriptor.output,
                                                           "ManagedBoundary", "Output", 2u),
              SALTS_OK);

  fixture->snapshot =
      (turbo_flow_managed_boundary_snapshot_t)TURBO_FLOW_MANAGED_BOUNDARY_SNAPSHOT_INIT;
  memcpy(fixture->snapshot.uid, uid, strlen(uid) + 1u);
  fixture->snapshot.generation = 7u;
  fixture->snapshot.observed_generation = 7u;
  fixture->snapshot.state = TURBO_FLOW_MANAGED_BOUNDARY_RUNNING;
  fixture->snapshot.demand = 11u;
  fixture->snapshot.queue_depth = 3u;
  fixture->snapshot.queue_capacity = 16u;
  fixture->snapshot.in_flight = 2u;
  fixture->snapshot.lag = 5u;
  fixture->snapshot.accepted = 23u;
  fixture->snapshot.completed = 18u;
  fixture->snapshot.rejected = 3u;
  fixture->snapshot.backpressured = 0;
  fixture->snapshot.last_status = SALTS_OK;
}

static int managed_boundary_metadata(void *ctx, turbo_flow_resource_metadata_t *out) {
  managed_boundary_fixture_t *fixture = (managed_boundary_fixture_t *)ctx;
  if (!fixture || !out || out->size < sizeof(*out)) return SALTS_EINVAL;
  if (fixture->metadata_status != SALTS_OK) return fixture->metadata_status;
  *out = fixture->metadata;
  return SALTS_OK;
}

static int managed_boundary_descriptor(void *ctx, turbo_flow_managed_boundary_descriptor_t *out) {
  managed_boundary_fixture_t *fixture = (managed_boundary_fixture_t *)ctx;
  if (!fixture || !out || out->size < sizeof(*out)) return SALTS_EINVAL;
  if (fixture->descriptor_status != SALTS_OK) return fixture->descriptor_status;
  *out = fixture->descriptor;
  return SALTS_OK;
}

static int managed_boundary_snapshot(void *ctx, turbo_flow_managed_boundary_snapshot_t *out) {
  managed_boundary_fixture_t *fixture = (managed_boundary_fixture_t *)ctx;
  if (!fixture || !out || out->size < sizeof(*out)) return SALTS_EINVAL;
  if (fixture->snapshot_status != SALTS_OK) return fixture->snapshot_status;
  *out = fixture->snapshot;
  if (fixture->advance_generation_after_snapshot) {
    fixture->metadata.generation += 1u;
    fixture->metadata.observed_generation = fixture->metadata.generation;
  }
  return SALTS_OK;
}

static int managed_boundary_command(void *ctx, turbo_flow_t *flow,
                                    const turbo_flow_resource_command_t *command) {
  managed_boundary_fixture_t *fixture = (managed_boundary_fixture_t *)ctx;
  (void)flow;
  (void)command;
  fixture->command_calls += 1;
  fixture->metadata.generation += 1u;
  fixture->metadata.observed_generation = fixture->metadata.generation;
  fixture->snapshot.generation = fixture->metadata.generation;
  fixture->snapshot.observed_generation = fixture->metadata.observed_generation;
  return SALTS_OK;
}

static int managed_boundary_register(turbo_flow_t *flow, managed_boundary_fixture_t *fixture,
                                     int with_descriptor, int with_snapshot) {
  turbo_flow_managed_boundary_provider_ops_t ops = TURBO_FLOW_MANAGED_BOUNDARY_PROVIDER_OPS_INIT;
  ops.resource.metadata = managed_boundary_metadata;
  ops.resource.command = managed_boundary_command;
  ops.descriptor = with_descriptor ? managed_boundary_descriptor : NULL;
  ops.snapshot = with_snapshot ? managed_boundary_snapshot : NULL;
  if (!with_descriptor && !with_snapshot) {
    return turbo_flow_register_resource_provider(flow, fixture->metadata.owner_name, &ops.resource,
                                                 fixture);
  }
  return turbo_flow_register_managed_boundary_provider(flow, fixture->metadata.owner_name, &ops,
                                                       fixture);
}

spec("Turbo Flow managed boundary") {
  it("enumerates only explicitly declared boundaries and copies immutable descriptors") {
    turbo_flow_t *flow = turbo_flow_create();
    managed_boundary_fixture_t plain;
    managed_boundary_fixture_t managed;
    turbo_flow_managed_boundary_descriptor_t descriptor =
        TURBO_FLOW_MANAGED_BOUNDARY_DESCRIPTOR_INIT;
    turbo_flow_managed_boundary_snapshot_t snapshot = TURBO_FLOW_MANAGED_BOUNDARY_SNAPSHOT_INIT;

    check_not_null(flow);
    managed_boundary_fixture_init(&plain, "connection:plain", "plain");
    managed_boundary_fixture_init(&managed, "connection:managed", "managed");
    check_equal(managed_boundary_register(flow, &plain, 0, 0), SALTS_OK);
    check_equal(managed_boundary_register(flow, &managed, 1, 1), SALTS_OK);
    managed.descriptor.role_flags = TURBO_FLOW_MANAGED_BOUNDARY_SOURCE;

    check_equal(turbo_flow_managed_boundary_count(flow), 1u);
    check_equal(turbo_flow_managed_boundary_descriptor_at(flow, 0u, &descriptor), SALTS_OK);
    check_equal(descriptor.version, TURBO_FLOW_MANAGED_BOUNDARY_API_VERSION);
    check_equal(descriptor.uid, "connection:managed");
    check_equal(descriptor.owner_name, "managed");
    check_equal(descriptor.role_flags,
                TURBO_FLOW_MANAGED_BOUNDARY_SOURCE | TURBO_FLOW_MANAGED_BOUNDARY_SINK);
    check_equal(descriptor.input.schema_version, 1u);
    check_equal(descriptor.output.schema_version, 2u);

    check_equal(turbo_flow_managed_boundary_snapshot_at(flow, 0u, &snapshot), SALTS_OK);
    check_equal(snapshot.uid, "connection:managed");
    check_equal(snapshot.state, TURBO_FLOW_MANAGED_BOUNDARY_RUNNING);
    check_equal(snapshot.demand, 11u);
    check_equal(snapshot.queue_depth, 3u);
    check_equal(snapshot.queue_capacity, 16u);
    check_equal(snapshot.in_flight, 2u);
    check_equal(snapshot.lag, 5u);
    check_equal(snapshot.accepted, 23u);
    check_equal(snapshot.completed, 18u);
    check_equal(snapshot.rejected, 3u);
    check_equal(turbo_flow_managed_boundary_descriptor_at(flow, 1u, &descriptor), SALTS_ENOENT);
    check_equal(turbo_flow_managed_boundary_snapshot_at(flow, 1u, &snapshot), SALTS_ENOENT);
    turbo_flow_destroy(flow);
  }

  it("rejects incomplete or malformed boundary providers at registration") {
    turbo_flow_t *flow = turbo_flow_create();
    managed_boundary_fixture_t fixture;
    turbo_flow_managed_boundary_provider_ops_t ops = TURBO_FLOW_MANAGED_BOUNDARY_PROVIDER_OPS_INIT;

    check_not_null(flow);
    managed_boundary_fixture_init(&fixture, "connection:invalid", "invalid");
    ops.resource.metadata = managed_boundary_metadata;
    ops.resource.command = managed_boundary_command;
    ops.descriptor = managed_boundary_descriptor;
    ops.snapshot = managed_boundary_snapshot;
    fixture.metadata_status = SALTS_EIO;
    check_equal(turbo_flow_register_managed_boundary_provider(flow, fixture.metadata.owner_name,
                                                              &ops, &fixture),
                SALTS_EIO);
    fixture.metadata_status = SALTS_OK;
    ops.version = 0u;
    check_equal(turbo_flow_register_managed_boundary_provider(flow, fixture.metadata.owner_name,
                                                              &ops, &fixture),
                SALTS_EINVAL);
    ops.version = TURBO_FLOW_MANAGED_BOUNDARY_API_VERSION;
    ops.size = sizeof(ops) - 1u;
    check_equal(turbo_flow_register_managed_boundary_provider(flow, fixture.metadata.owner_name,
                                                              &ops, &fixture),
                SALTS_EINVAL);
    check_equal(managed_boundary_register(flow, &fixture, 1, 0), SALTS_EINVAL);
    check_equal(managed_boundary_register(flow, &fixture, 0, 1), SALTS_EINVAL);
    fixture.descriptor_status = SALTS_EIO;
    check_equal(managed_boundary_register(flow, &fixture, 1, 1), SALTS_EIO);
    fixture.descriptor_status = SALTS_OK;
    fixture.descriptor.version = 0u;
    check_equal(managed_boundary_register(flow, &fixture, 1, 1), SALTS_EPROTO);
    fixture.descriptor.version = TURBO_FLOW_MANAGED_BOUNDARY_API_VERSION;
    fixture.descriptor.uid[0] = 'x';
    check_equal(managed_boundary_register(flow, &fixture, 1, 1), SALTS_EPROTO);
    check_equal(turbo_flow_managed_boundary_count(flow), 0u);
    turbo_flow_destroy(flow);
  }

  it("keeps ordinary resource providers outside managed-boundary enumeration") {
    turbo_flow_t *flow = turbo_flow_create();
    managed_boundary_fixture_t fixture;
    turbo_flow_resource_provider_ops_t ops = TURBO_FLOW_RESOURCE_PROVIDER_OPS_INIT;

    check_not_null(flow);
    managed_boundary_fixture_init(&fixture, "connection:ordinary", "ordinary");
    ops.metadata = managed_boundary_metadata;
    ops.command = managed_boundary_command;
    check_equal(
        turbo_flow_register_resource_provider(flow, fixture.metadata.owner_name, &ops, &fixture),
        SALTS_OK);
    check_equal(turbo_flow_managed_boundary_count(flow), 0u);
    turbo_flow_destroy(flow);
  }

  it("propagates an existing owner error while registering a managed boundary") {
    turbo_flow_t *flow = turbo_flow_create();
    managed_boundary_fixture_t existing;
    managed_boundary_fixture_t managed;

    check_not_null(flow);
    managed_boundary_fixture_init(&existing, "connection:existing", "existing");
    managed_boundary_fixture_init(&managed, "connection:new", "new");
    check_equal(managed_boundary_register(flow, &existing, 0, 0), SALTS_OK);
    existing.metadata_status = SALTS_EIO;
    check_equal(managed_boundary_register(flow, &managed, 1, 1), SALTS_EIO);
    check_equal(turbo_flow_managed_boundary_count(flow), 0u);
    turbo_flow_destroy(flow);
  }

  it("registers an explicitly read-only boundary with an empty command mask") {
    turbo_flow_t *flow = turbo_flow_create();
    managed_boundary_fixture_t fixture;
    turbo_flow_managed_boundary_provider_ops_t ops = TURBO_FLOW_MANAGED_BOUNDARY_PROVIDER_OPS_INIT;
    turbo_flow_resource_command_t command = TURBO_FLOW_RESOURCE_COMMAND_INIT;
    turbo_flow_resource_command_result_t result = TURBO_FLOW_RESOURCE_COMMAND_RESULT_INIT;

    check_not_null(flow);
    managed_boundary_fixture_init(&fixture, "connection:readonly", "readonly");
    fixture.descriptor.command_flags = 0u;
    ops.resource.metadata = managed_boundary_metadata;
    ops.descriptor = managed_boundary_descriptor;
    ops.snapshot = managed_boundary_snapshot;
    check_equal(turbo_flow_register_managed_boundary_provider(flow, fixture.metadata.owner_name,
                                                              &ops, &fixture),
                SALTS_OK);
    check_equal(turbo_flow_managed_boundary_count(flow), 1u);

    for (int kind = TURBO_FLOW_RESOURCE_COMMAND_QUIESCE;
         kind <= TURBO_FLOW_RESOURCE_COMMAND_RESIZE_POOL; ++kind) {
      command = (turbo_flow_resource_command_t)TURBO_FLOW_RESOURCE_COMMAND_INIT;
      result = (turbo_flow_resource_command_result_t)TURBO_FLOW_RESOURCE_COMMAND_RESULT_INIT;
      command.kind = (turbo_flow_resource_command_kind_t)kind;
      memcpy(command.target_uid, fixture.metadata.uid, strlen(fixture.metadata.uid) + 1u);
      command.idempotency_key[0] = (char)('0' + kind);
      command.idempotency_key[1] = '\0';
      command.expected_generation = fixture.metadata.generation;
      if (command.kind == TURBO_FLOW_RESOURCE_COMMAND_REPLACE_ENDPOINT) {
        memcpy(command.endpoint_host, "example.test", sizeof("example.test"));
        command.endpoint_port = 443;
      } else if (command.kind == TURBO_FLOW_RESOURCE_COMMAND_RESIZE_POOL) {
        command.parallelism = 2u;
      }
      check_equal(turbo_flow_resource_command(flow, &command, &result), SALTS_ENOTSUP);
    }
    check_equal(fixture.command_calls, 0);
    turbo_flow_destroy(flow);
  }

  it("revalidates descriptor identity and propagates metadata errors") {
    turbo_flow_t *flow = turbo_flow_create();
    managed_boundary_fixture_t fixture;
    turbo_flow_managed_boundary_descriptor_t descriptor =
        TURBO_FLOW_MANAGED_BOUNDARY_DESCRIPTOR_INIT;

    check_not_null(flow);
    managed_boundary_fixture_init(&fixture, "connection:descriptor", "descriptor");
    check_equal(managed_boundary_register(flow, &fixture, 1, 1), SALTS_OK);
    fixture.metadata_status = SALTS_EIO;
    check_equal(turbo_flow_managed_boundary_descriptor_at(flow, 0u, &descriptor), SALTS_EIO);
    fixture.metadata_status = SALTS_OK;
    fixture.metadata.uid[0] = 'x';
    check_equal(turbo_flow_managed_boundary_descriptor_at(flow, 0u, &descriptor), SALTS_EPROTO);
    turbo_flow_destroy(flow);
  }

  it("fails fast on owner errors and inconsistent live snapshots") {
    turbo_flow_t *flow = turbo_flow_create();
    managed_boundary_fixture_t fixture;
    turbo_flow_managed_boundary_snapshot_t snapshot = TURBO_FLOW_MANAGED_BOUNDARY_SNAPSHOT_INIT;

    check_not_null(flow);
    managed_boundary_fixture_init(&fixture, "connection:live", "live");
    check_equal(managed_boundary_register(flow, &fixture, 1, 1), SALTS_OK);
    fixture.snapshot_status = SALTS_EIO;
    check_equal(turbo_flow_managed_boundary_snapshot_at(flow, 0u, &snapshot), SALTS_EIO);
    fixture.snapshot_status = SALTS_OK;
    fixture.snapshot.observed_generation = fixture.snapshot.generation + 1u;
    check_equal(turbo_flow_managed_boundary_snapshot_at(flow, 0u, &snapshot), SALTS_EPROTO);
    fixture.snapshot.observed_generation = fixture.snapshot.generation;
    fixture.snapshot.queue_depth = fixture.snapshot.queue_capacity + 1u;
    check_equal(turbo_flow_managed_boundary_snapshot_at(flow, 0u, &snapshot), SALTS_EPROTO);
    fixture.snapshot.queue_depth = 0u;
    fixture.snapshot.accepted = 0u;
    fixture.snapshot.completed = 0u;
    fixture.snapshot.rejected = 9u;
    check_equal(turbo_flow_managed_boundary_snapshot_at(flow, 0u, &snapshot), SALTS_OK);
    fixture.snapshot.accepted = 23u;
    fixture.snapshot.completed = 18u;
    fixture.snapshot.rejected = 3u;
    fixture.snapshot.size = sizeof(fixture.snapshot) - 1u;
    check_equal(turbo_flow_managed_boundary_snapshot_at(flow, 0u, &snapshot), SALTS_EPROTO);
    snapshot = (turbo_flow_managed_boundary_snapshot_t)TURBO_FLOW_MANAGED_BOUNDARY_SNAPSHOT_INIT;
    fixture.snapshot.size = sizeof(fixture.snapshot);
    fixture.snapshot.version = 0u;
    check_equal(turbo_flow_managed_boundary_snapshot_at(flow, 0u, &snapshot), SALTS_EPROTO);
    fixture.snapshot.version = TURBO_FLOW_MANAGED_BOUNDARY_API_VERSION;
    fixture.snapshot.generation += 1u;
    fixture.snapshot.observed_generation += 1u;
    check_equal(turbo_flow_managed_boundary_snapshot_at(flow, 0u, &snapshot), SALTS_EPROTO);
    fixture.snapshot.generation -= 1u;
    fixture.snapshot.observed_generation -= 1u;
    fixture.advance_generation_after_snapshot = 1;
    check_equal(turbo_flow_managed_boundary_snapshot_at(flow, 0u, &snapshot), SALTS_EBUSY);
    fixture.advance_generation_after_snapshot = 0;
    fixture.metadata.generation -= 1u;
    fixture.metadata.observed_generation = fixture.metadata.generation;
    fixture.snapshot.uid[0] = 'x';
    check_equal(turbo_flow_managed_boundary_snapshot_at(flow, 0u, &snapshot), SALTS_EPROTO);
    memcpy(fixture.snapshot.uid, fixture.metadata.uid, strlen(fixture.metadata.uid) + 1u);
    fixture.metadata.owner_name[0] = 'x';
    check_equal(turbo_flow_managed_boundary_snapshot_at(flow, 0u, &snapshot), SALTS_EPROTO);
    turbo_flow_destroy(flow);
  }

  it("enforces the declared command mask before invoking the owner") {
    turbo_flow_t *flow = turbo_flow_create();
    managed_boundary_fixture_t fixture;
    turbo_flow_resource_command_t command = TURBO_FLOW_RESOURCE_COMMAND_INIT;
    turbo_flow_resource_command_result_t result = TURBO_FLOW_RESOURCE_COMMAND_RESULT_INIT;

    check_not_null(flow);
    managed_boundary_fixture_init(&fixture, "connection:commands", "commands");
    fixture.descriptor.command_flags = TURBO_FLOW_MANAGED_BOUNDARY_COMMAND_QUIESCE;
    check_equal(managed_boundary_register(flow, &fixture, 1, 1), SALTS_OK);

    command.kind = TURBO_FLOW_RESOURCE_COMMAND_REPLACE_ENDPOINT;
    memcpy(command.target_uid, fixture.metadata.uid, strlen(fixture.metadata.uid) + 1u);
    memcpy(command.idempotency_key, "replace-masked", sizeof("replace-masked"));
    command.expected_generation = fixture.metadata.generation;
    memcpy(command.endpoint_host, "example.test", sizeof("example.test"));
    command.endpoint_port = 443;
    check_equal(turbo_flow_resource_command(flow, &command, &result), SALTS_ENOTSUP);
    check_equal(fixture.command_calls, 0);
    result = (turbo_flow_resource_command_result_t)TURBO_FLOW_RESOURCE_COMMAND_RESULT_INIT;
    check_equal(turbo_flow_resource_command(flow, &command, &result), SALTS_ENOTSUP);
    check_equal(result.replayed, 1);
    check_equal(fixture.command_calls, 0);

    command = (turbo_flow_resource_command_t)TURBO_FLOW_RESOURCE_COMMAND_INIT;
    result = (turbo_flow_resource_command_result_t)TURBO_FLOW_RESOURCE_COMMAND_RESULT_INIT;
    command.kind = TURBO_FLOW_RESOURCE_COMMAND_QUIESCE;
    memcpy(command.target_uid, fixture.metadata.uid, strlen(fixture.metadata.uid) + 1u);
    memcpy(command.idempotency_key, "quiesce-allowed", sizeof("quiesce-allowed"));
    command.expected_generation = fixture.metadata.generation;
    check_equal(turbo_flow_resource_command(flow, &command, &result), SALTS_OK);
    check_equal(fixture.command_calls, 1);
    check_equal(result.generation_after, 8u);
    turbo_flow_destroy(flow);
  }

  it("validates caller-owned output structures") {
    turbo_flow_t *flow = turbo_flow_create();
    turbo_flow_managed_boundary_descriptor_t descriptor =
        TURBO_FLOW_MANAGED_BOUNDARY_DESCRIPTOR_INIT;
    turbo_flow_managed_boundary_snapshot_t snapshot = TURBO_FLOW_MANAGED_BOUNDARY_SNAPSHOT_INIT;

    check_not_null(flow);
    descriptor.size = sizeof(descriptor) - 1u;
    snapshot.size = sizeof(snapshot) - 1u;
    check_equal(turbo_flow_managed_boundary_descriptor_at(flow, 0u, &descriptor), SALTS_EINVAL);
    check_equal(turbo_flow_managed_boundary_snapshot_at(flow, 0u, &snapshot), SALTS_EINVAL);
    check_equal(turbo_flow_managed_boundary_descriptor_at(NULL, 0u, &descriptor), SALTS_EINVAL);
    check_equal(turbo_flow_managed_boundary_snapshot_at(NULL, 0u, &snapshot), SALTS_EINVAL);
    turbo_flow_destroy(flow);
  }
}
