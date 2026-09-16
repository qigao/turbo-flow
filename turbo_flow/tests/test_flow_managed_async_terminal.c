#include "flow_internal.h"
#include "tinytest.h"
#include "turbo_flow.h"

#include <salts/clock.h>
#include <stdatomic.h>
#include <string.h>

typedef struct managed_async_terminal_fixture_s {
  turbo_flow_resource_metadata_t metadata;
  turbo_flow_managed_boundary_descriptor_t descriptor;
  turbo_flow_managed_boundary_snapshot_t snapshot;
  int metadata_status;
  int descriptor_status;
  int metadata_calls;
  int shutdown_calls;
  atomic_size_t submit_calls;
} managed_async_terminal_fixture_t;

typedef struct managed_async_terminal_completion_s {
  atomic_size_t calls;
  atomic_int status;
} managed_async_terminal_completion_t;

typedef struct managed_async_terminal_registration_fault_s {
  flow_registration_checkpoint_t fail_at;
  int status;
  size_t calls[FLOW_REGISTRATION_CHECKPOINT_COUNT];
} managed_async_terminal_registration_fault_t;

static void managed_async_terminal_fixture_init(managed_async_terminal_fixture_t *fixture,
                                                const char *uid, const char *owner_name) {
  memset(fixture, 0, sizeof(*fixture));
  atomic_init(&fixture->submit_calls, 0u);
  fixture->metadata = (turbo_flow_resource_metadata_t)TURBO_FLOW_RESOURCE_METADATA_INIT;
  fixture->metadata.domain = TURBO_FLOW_DOMAIN_IO_TRANSPORT;
  fixture->metadata.kind = TURBO_FLOW_RESOURCE_CONNECTION;
  memcpy(fixture->metadata.uid, uid, strlen(uid) + 1u);
  memcpy(fixture->metadata.owner_name, owner_name, strlen(owner_name) + 1u);
  fixture->metadata.generation = 1u;
  fixture->metadata.observed_generation = 1u;

  fixture->descriptor =
      (turbo_flow_managed_boundary_descriptor_t)TURBO_FLOW_MANAGED_BOUNDARY_DESCRIPTOR_INIT;
  fixture->descriptor.domain = fixture->metadata.domain;
  fixture->descriptor.kind = fixture->metadata.kind;
  fixture->descriptor.role_flags = TURBO_FLOW_MANAGED_BOUNDARY_SINK;
  fixture->descriptor.capability_flags = TURBO_FLOW_MANAGED_BOUNDARY_DURABLE_SETTLEMENT;
  memcpy(fixture->descriptor.uid, uid, strlen(uid) + 1u);
  memcpy(fixture->descriptor.owner_name, owner_name, strlen(owner_name) + 1u);
  check_equal(turbo_flow_content_descriptor_init(
                  &fixture->descriptor.input, TURBO_FLOW_DOMAIN_PROTOCOL_PATTERN,
                  TURBO_FLOW_CONTENT_PROFILE_PROTOCOL_DATA, TURBO_FLOW_DATA_ENCODING_OPAQUE,
                  "application/octet-stream", "managed-terminal-input"),
              SALTS_OK);
  check_equal(turbo_flow_content_descriptor_declare_schema(&fixture->descriptor.input,
                                                           "ManagedTerminal", "Bytes", 1u),
              SALTS_OK);

  fixture->snapshot =
      (turbo_flow_managed_boundary_snapshot_t)TURBO_FLOW_MANAGED_BOUNDARY_SNAPSHOT_INIT;
  memcpy(fixture->snapshot.uid, uid, strlen(uid) + 1u);
  fixture->snapshot.generation = 1u;
  fixture->snapshot.observed_generation = 1u;
  fixture->snapshot.state = TURBO_FLOW_MANAGED_BOUNDARY_REGISTERED;
  fixture->snapshot.queue_capacity = 8u;
}

static int managed_async_terminal_metadata(void *ctx, turbo_flow_resource_metadata_t *out) {
  managed_async_terminal_fixture_t *fixture = (managed_async_terminal_fixture_t *)ctx;
  if (!fixture || !out || out->size < sizeof(*out)) return SALTS_EINVAL;
  fixture->metadata_calls += 1;
  if (fixture->metadata_status != SALTS_OK) return fixture->metadata_status;
  *out = fixture->metadata;
  return SALTS_OK;
}

static int managed_async_terminal_descriptor(void *ctx,
                                             turbo_flow_managed_boundary_descriptor_t *out) {
  managed_async_terminal_fixture_t *fixture = (managed_async_terminal_fixture_t *)ctx;
  if (!fixture || !out || out->size < sizeof(*out)) return SALTS_EINVAL;
  if (fixture->descriptor_status != SALTS_OK) return fixture->descriptor_status;
  *out = fixture->descriptor;
  return SALTS_OK;
}

static int managed_async_terminal_snapshot(void *ctx, turbo_flow_managed_boundary_snapshot_t *out) {
  managed_async_terminal_fixture_t *fixture = (managed_async_terminal_fixture_t *)ctx;
  if (!fixture || !out || out->size < sizeof(*out)) return SALTS_EINVAL;
  *out = fixture->snapshot;
  return SALTS_OK;
}

static int managed_async_terminal_submit(void *ctx, turbo_flow_t *flow,
                                         const turbo_flow_stage_plan_t *stage,
                                         const turbo_flow_msg_t *message,
                                         turbo_flow_async_terminal_claim_t *claim) {
  managed_async_terminal_fixture_t *fixture = (managed_async_terminal_fixture_t *)ctx;
  (void)flow;
  (void)stage;
  (void)message;
  (void)claim;
  (void)atomic_fetch_add_explicit(&fixture->submit_calls, 1u, memory_order_release);
  return SALTS_ENOSPC;
}

static void managed_async_terminal_publish_complete(void *ctx,
                                                    const turbo_flow_publish_result_t *result) {
  managed_async_terminal_completion_t *completion = (managed_async_terminal_completion_t *)ctx;
  atomic_store_explicit(&completion->status, result->status, memory_order_relaxed);
  (void)atomic_fetch_add_explicit(&completion->calls, 1u, memory_order_release);
}

static void managed_async_terminal_wait_for(atomic_size_t *value, size_t expected) {
  size_t attempt;
  for (attempt = 0u;
       attempt < 5000u && atomic_load_explicit(value, memory_order_acquire) < expected; ++attempt) {
    salts_sleep_ms(1u);
  }
}

static int managed_async_terminal_fail_registration(void *ctx,
                                                    flow_registration_checkpoint_t checkpoint) {
  managed_async_terminal_registration_fault_t *fault =
      (managed_async_terminal_registration_fault_t *)ctx;
  check_true(checkpoint >= FLOW_REGISTRATION_ALLOC_ADAPTER_NAME);
  check_true(checkpoint < FLOW_REGISTRATION_CHECKPOINT_COUNT);
  fault->calls[checkpoint] += 1u;
  return checkpoint == fault->fail_at ? fault->status : SALTS_OK;
}

static int managed_async_terminal_consume(void *ctx, turbo_flow_t *flow,
                                          const turbo_flow_stage_plan_t *stage,
                                          turbo_flow_msg_t *message) {
  (void)ctx;
  (void)flow;
  (void)stage;
  (void)message;
  return SALTS_OK;
}

static void managed_async_terminal_shutdown(void *ctx) {
  managed_async_terminal_fixture_t *fixture = (managed_async_terminal_fixture_t *)ctx;
  fixture->shutdown_calls += 1;
}

static turbo_flow_managed_async_terminal_registration_t managed_async_terminal_registration(
    managed_async_terminal_fixture_t *fixture, turbo_flow_adapter_ops_t *adapter_ops,
    turbo_flow_async_terminal_adapter_ops_t *async_ops, turbo_flow_adapter_schema_t *schema,
    turbo_flow_managed_boundary_provider_ops_t *boundary_ops) {
  turbo_flow_managed_async_terminal_registration_t registration =
      TURBO_FLOW_MANAGED_ASYNC_TERMINAL_REGISTRATION_INIT;
  memset(adapter_ops, 0, sizeof(*adapter_ops));
  *async_ops = (turbo_flow_async_terminal_adapter_ops_t)TURBO_FLOW_ASYNC_TERMINAL_ADAPTER_OPS_INIT;
  memset(schema, 0, sizeof(*schema));
  *boundary_ops =
      (turbo_flow_managed_boundary_provider_ops_t)TURBO_FLOW_MANAGED_BOUNDARY_PROVIDER_OPS_INIT;
  adapter_ops->shutdown = managed_async_terminal_shutdown;
  async_ops->submit = managed_async_terminal_submit;
  schema->kind = TURBO_FLOW_ADAPTER_KIND_CUSTOM;
  schema->roles = TURBO_FLOW_ADAPTER_SINK;
  schema->direction = TURBO_FLOW_ADAPTER_OUTPUT;
  boundary_ops->resource.metadata = managed_async_terminal_metadata;
  boundary_ops->descriptor = managed_async_terminal_descriptor;
  boundary_ops->snapshot = managed_async_terminal_snapshot;
  registration.adapter_name = fixture->metadata.owner_name;
  registration.adapter_ops = adapter_ops;
  registration.async_ops = async_ops;
  registration.schema = schema;
  registration.owner_name = fixture->metadata.owner_name;
  registration.boundary_ops = boundary_ops;
  registration.ctx = fixture;
  return registration;
}

spec("Flow managed async terminal registration") {
  it("registers the adapter and managed Sink as one explicit owner") {
    static const char graph[] = "source input\n"
                                "stage output adapter managed.terminal\n"
                                "stage main {\n"
                                "  input -> output\n"
                                "}\n";
    managed_async_terminal_fixture_t fixture;
    turbo_flow_adapter_ops_t adapter_ops;
    turbo_flow_async_terminal_adapter_ops_t async_ops;
    turbo_flow_adapter_schema_t schema;
    turbo_flow_managed_boundary_provider_ops_t boundary_ops;
    turbo_flow_managed_async_terminal_registration_t registration;
    turbo_flow_managed_boundary_descriptor_t descriptor =
        TURBO_FLOW_MANAGED_BOUNDARY_DESCRIPTOR_INIT;
    managed_async_terminal_completion_t completion;
    turbo_flow_msg_t message;
    turbo_flow_t *flow = turbo_flow_create();

    check_not_null(flow);
    atomic_init(&completion.calls, 0u);
    atomic_init(&completion.status, SALTS_EALREADY);
    managed_async_terminal_fixture_init(&fixture, "connection:managed-terminal",
                                        "managed.terminal");
    registration = managed_async_terminal_registration(&fixture, &adapter_ops, &async_ops, &schema,
                                                       &boundary_ops);

    check_equal(turbo_flow_register_managed_async_terminal_adapter(flow, &registration), SALTS_OK);
    check_not_null(turbo_flow_find_adapter_schema(flow, registration.adapter_name));
    check_equal(turbo_flow_managed_boundary_count(flow), (size_t)1u);
    check_equal(turbo_flow_resource_metadata_count(flow), (size_t)1u);
    check_equal(turbo_flow_managed_boundary_descriptor_at(flow, 0u, &descriptor), SALTS_OK);
    check_equal(descriptor.role_flags, (uint32_t)TURBO_FLOW_MANAGED_BOUNDARY_SINK);
    check_equal(descriptor.uid, "connection:managed-terminal");
    check_equal(fixture.shutdown_calls, 0);
    check_equal(
        turbo_flow_register_adapter_async_terminal(flow, registration.adapter_name, &async_ops),
        SALTS_EALREADY);
    check_equal(turbo_flow_parse_string(flow, graph, sizeof(graph) - 1u), SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_OK);
    check_equal(turbo_flow_start(flow), SALTS_OK);
    turbo_flow_msg_init(&message);
    check_equal(turbo_flow_publish_async(flow, "input", &message,
                                         managed_async_terminal_publish_complete, &completion),
                SALTS_OK);
    managed_async_terminal_wait_for(&completion.calls, 1u);
    check_equal(atomic_load_explicit(&fixture.submit_calls, memory_order_acquire), (size_t)1u);
    check_equal(atomic_load_explicit(&completion.calls, memory_order_acquire), (size_t)1u);
    check_equal(atomic_load_explicit(&completion.status, memory_order_acquire), SALTS_ENOSPC);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_msg_cleanup(&message);

    turbo_flow_destroy(flow);
    check_equal(fixture.shutdown_calls, 1);
  }

  it("rejects invalid aggregate and nested contracts without registry mutation") {
    managed_async_terminal_fixture_t fixture;
    turbo_flow_adapter_ops_t adapter_ops;
    turbo_flow_async_terminal_adapter_ops_t async_ops;
    turbo_flow_adapter_schema_t schema;
    turbo_flow_managed_boundary_provider_ops_t boundary_ops;
    turbo_flow_managed_async_terminal_registration_t registration;
    turbo_flow_t *flow = turbo_flow_create();

    check_not_null(flow);
    managed_async_terminal_fixture_init(&fixture, "connection:invalid", "invalid");
    registration = managed_async_terminal_registration(&fixture, &adapter_ops, &async_ops, &schema,
                                                       &boundary_ops);
    registration.version += 1u;

    check_equal(turbo_flow_register_managed_async_terminal_adapter(flow, &registration),
                SALTS_EINVAL);
    registration.version = TURBO_FLOW_MANAGED_ASYNC_TERMINAL_REGISTRATION_API_VERSION;
    registration.size = sizeof(registration) - 1u;
    check_equal(turbo_flow_register_managed_async_terminal_adapter(flow, &registration),
                SALTS_EINVAL);
    registration.size = sizeof(registration);
    async_ops.version += 1u;
    check_equal(turbo_flow_register_managed_async_terminal_adapter(flow, &registration),
                SALTS_EINVAL);
    async_ops.version = TURBO_FLOW_ASYNC_TERMINAL_API_VERSION;
    boundary_ops.version += 1u;
    check_equal(turbo_flow_register_managed_async_terminal_adapter(flow, &registration),
                SALTS_EINVAL);
    boundary_ops.version = TURBO_FLOW_MANAGED_BOUNDARY_API_VERSION;
    adapter_ops.consume = managed_async_terminal_consume;
    check_equal(turbo_flow_register_managed_async_terminal_adapter(flow, &registration),
                SALTS_EINVAL);
    check_null(turbo_flow_find_adapter_schema(flow, "invalid"));
    check_equal(turbo_flow_managed_boundary_count(flow), (size_t)0u);
    check_equal(turbo_flow_resource_metadata_count(flow), (size_t)0u);
    check_equal(fixture.metadata_calls, 0);
    check_equal(fixture.shutdown_calls, 0);
    turbo_flow_destroy(flow);
  }

  it("rejects adapter schema errors before invoking boundary callbacks") {
    managed_async_terminal_fixture_t fixture;
    turbo_flow_adapter_ops_t adapter_ops;
    turbo_flow_async_terminal_adapter_ops_t async_ops;
    turbo_flow_adapter_schema_t schema;
    turbo_flow_managed_boundary_provider_ops_t boundary_ops;
    turbo_flow_managed_async_terminal_registration_t registration;
    turbo_flow_t *flow = turbo_flow_create();

    check_not_null(flow);
    managed_async_terminal_fixture_init(&fixture, "connection:bad-schema", "bad.schema");
    registration = managed_async_terminal_registration(&fixture, &adapter_ops, &async_ops, &schema,
                                                       &boundary_ops);
    schema.kind = (turbo_flow_adapter_kind_t)-1;

    check_equal(turbo_flow_register_managed_async_terminal_adapter(flow, &registration),
                SALTS_EINVAL);
    check_equal(fixture.metadata_calls, 0);
    check_null(turbo_flow_find_adapter_schema(flow, registration.adapter_name));
    check_equal(turbo_flow_managed_boundary_count(flow), (size_t)0u);
    check_equal(turbo_flow_resource_metadata_count(flow), (size_t)0u);
    check_equal(fixture.shutdown_calls, 0);
    turbo_flow_destroy(flow);
  }

  it("rejects a source-only boundary without registering its adapter") {
    managed_async_terminal_fixture_t fixture;
    turbo_flow_adapter_ops_t adapter_ops;
    turbo_flow_async_terminal_adapter_ops_t async_ops;
    turbo_flow_adapter_schema_t schema;
    turbo_flow_managed_boundary_provider_ops_t boundary_ops;
    turbo_flow_managed_async_terminal_registration_t registration;
    turbo_flow_t *flow = turbo_flow_create();

    check_not_null(flow);
    managed_async_terminal_fixture_init(&fixture, "connection:source-only", "source.only");
    fixture.descriptor.role_flags = TURBO_FLOW_MANAGED_BOUNDARY_SOURCE;
    fixture.descriptor.capability_flags = TURBO_FLOW_MANAGED_BOUNDARY_DEMAND_AWARE;
    fixture.descriptor.output = fixture.descriptor.input;
    registration = managed_async_terminal_registration(&fixture, &adapter_ops, &async_ops, &schema,
                                                       &boundary_ops);

    check_equal(turbo_flow_register_managed_async_terminal_adapter(flow, &registration),
                SALTS_EPROTO);
    check_null(turbo_flow_find_adapter_schema(flow, registration.adapter_name));
    check_equal(turbo_flow_managed_boundary_count(flow), (size_t)0u);
    check_equal(turbo_flow_resource_metadata_count(flow), (size_t)0u);
    check_equal(fixture.shutdown_calls, 0);
    turbo_flow_destroy(flow);
  }

  it("leaves existing registrations intact on duplicate adapter or resource UID") {
    managed_async_terminal_fixture_t existing;
    managed_async_terminal_fixture_t duplicate_adapter;
    managed_async_terminal_fixture_t duplicate_uid;
    turbo_flow_adapter_ops_t existing_adapter_ops;
    turbo_flow_adapter_ops_t duplicate_adapter_ops;
    turbo_flow_adapter_ops_t duplicate_uid_adapter_ops;
    turbo_flow_async_terminal_adapter_ops_t existing_async_ops;
    turbo_flow_async_terminal_adapter_ops_t duplicate_adapter_async_ops;
    turbo_flow_async_terminal_adapter_ops_t duplicate_uid_async_ops;
    turbo_flow_adapter_schema_t existing_schema;
    turbo_flow_adapter_schema_t duplicate_adapter_schema;
    turbo_flow_adapter_schema_t duplicate_uid_schema;
    turbo_flow_managed_boundary_provider_ops_t existing_boundary_ops;
    turbo_flow_managed_boundary_provider_ops_t duplicate_adapter_boundary_ops;
    turbo_flow_managed_boundary_provider_ops_t duplicate_uid_boundary_ops;
    turbo_flow_managed_async_terminal_registration_t existing_registration;
    turbo_flow_managed_async_terminal_registration_t duplicate_adapter_registration;
    turbo_flow_managed_async_terminal_registration_t duplicate_uid_registration;
    turbo_flow_t *flow = turbo_flow_create();

    check_not_null(flow);
    managed_async_terminal_fixture_init(&existing, "connection:existing", "existing");
    managed_async_terminal_fixture_init(&duplicate_adapter, "connection:other", "other");
    managed_async_terminal_fixture_init(&duplicate_uid, "connection:existing", "duplicate.uid");
    existing_registration =
        managed_async_terminal_registration(&existing, &existing_adapter_ops, &existing_async_ops,
                                            &existing_schema, &existing_boundary_ops);
    duplicate_adapter_registration = managed_async_terminal_registration(
        &duplicate_adapter, &duplicate_adapter_ops, &duplicate_adapter_async_ops,
        &duplicate_adapter_schema, &duplicate_adapter_boundary_ops);
    duplicate_adapter_registration.adapter_name = existing_registration.adapter_name;
    duplicate_uid_registration = managed_async_terminal_registration(
        &duplicate_uid, &duplicate_uid_adapter_ops, &duplicate_uid_async_ops, &duplicate_uid_schema,
        &duplicate_uid_boundary_ops);

    check_equal(turbo_flow_register_managed_async_terminal_adapter(flow, &existing_registration),
                SALTS_OK);
    check_equal(
        turbo_flow_register_managed_async_terminal_adapter(flow, &duplicate_adapter_registration),
        SALTS_EALREADY);
    check_equal(duplicate_adapter.metadata_calls, 0);
    check_equal(
        turbo_flow_register_managed_async_terminal_adapter(flow, &duplicate_uid_registration),
        SALTS_EALREADY);
    check_null(turbo_flow_find_adapter_schema(flow, duplicate_uid_registration.adapter_name));
    check_equal(turbo_flow_managed_boundary_count(flow), (size_t)1u);
    check_equal(turbo_flow_resource_metadata_count(flow), (size_t)1u);
    check_equal(duplicate_adapter.shutdown_calls, 0);
    check_equal(duplicate_uid.shutdown_calls, 0);

    turbo_flow_destroy(flow);
    check_equal(existing.shutdown_calls, 1);
  }

  it("propagates owner errors before adapter ownership transfers") {
    managed_async_terminal_fixture_t fixture;
    turbo_flow_adapter_ops_t adapter_ops;
    turbo_flow_async_terminal_adapter_ops_t async_ops;
    turbo_flow_adapter_schema_t schema;
    turbo_flow_managed_boundary_provider_ops_t boundary_ops;
    turbo_flow_managed_async_terminal_registration_t registration;
    turbo_flow_t *flow = turbo_flow_create();

    check_not_null(flow);
    managed_async_terminal_fixture_init(&fixture, "connection:owner-error", "owner.error");
    fixture.descriptor_status = SALTS_ENOMEM;
    registration = managed_async_terminal_registration(&fixture, &adapter_ops, &async_ops, &schema,
                                                       &boundary_ops);

    check_equal(turbo_flow_register_managed_async_terminal_adapter(flow, &registration),
                SALTS_ENOMEM);
    fixture.descriptor_status = SALTS_OK;
    fixture.metadata_status = SALTS_EIO;
    check_equal(turbo_flow_register_managed_async_terminal_adapter(flow, &registration), SALTS_EIO);
    check_null(turbo_flow_find_adapter_schema(flow, registration.adapter_name));
    check_equal(turbo_flow_managed_boundary_count(flow), (size_t)0u);
    check_equal(turbo_flow_resource_metadata_count(flow), (size_t)0u);
    check_equal(fixture.shutdown_calls, 0);
    turbo_flow_destroy(flow);
  }

  it("rolls back every allocation commit point without transferring ownership") {
    static const flow_registration_checkpoint_t checkpoints[] = {
        FLOW_REGISTRATION_ALLOC_ADAPTER_NAME, FLOW_REGISTRATION_ALLOC_ADAPTER_SCHEMA,
        FLOW_REGISTRATION_ALLOC_ADAPTER_VECTOR, FLOW_REGISTRATION_ALLOC_RESOURCE_OWNER_NAME,
        FLOW_REGISTRATION_ALLOC_RESOURCE_VECTOR};
    size_t index;

    for (index = 0u; index < sizeof(checkpoints) / sizeof(checkpoints[0]); ++index) {
      flow_registration_checkpoint_t checkpoint = checkpoints[index];
      managed_async_terminal_fixture_t fixture;
      managed_async_terminal_registration_fault_t fault;
      turbo_flow_adapter_ops_t adapter_ops;
      turbo_flow_async_terminal_adapter_ops_t async_ops;
      turbo_flow_adapter_schema_t schema;
      turbo_flow_option_field_t schema_field;
      turbo_flow_managed_boundary_provider_ops_t boundary_ops;
      turbo_flow_managed_async_terminal_registration_t registration;
      turbo_flow_t *flow = turbo_flow_create();

      check_not_null(flow);
      memset(&fault, 0, sizeof(fault));
      memset(&schema_field, 0, sizeof(schema_field));
      fault.fail_at = checkpoint;
      fault.status = SALTS_ENOMEM;
      managed_async_terminal_fixture_init(&fixture, "connection:allocation-fault",
                                          "allocation.fault");
      registration = managed_async_terminal_registration(&fixture, &adapter_ops, &async_ops,
                                                         &schema, &boundary_ops);
      schema_field.name = "capacity";
      schema_field.type = TURBO_FLOW_OPTION_SIZE;
      schema.fields = &schema_field;
      schema.field_count = 1u;
      flow->registration_fault.before_commit = managed_async_terminal_fail_registration;
      flow->registration_fault.ctx = &fault;

      check_equal(turbo_flow_register_managed_async_terminal_adapter(flow, &registration),
                  SALTS_ENOMEM);
      check_equal(fault.calls[checkpoint], (size_t)1u);
      check_null(turbo_flow_find_adapter_schema(flow, registration.adapter_name));
      check_equal(turbo_flow_managed_boundary_count(flow), (size_t)0u);
      check_equal(turbo_flow_resource_metadata_count(flow), (size_t)0u);
      check_equal(fixture.shutdown_calls, 0);
      if (checkpoint <= FLOW_REGISTRATION_ALLOC_ADAPTER_VECTOR) {
        check_equal(fixture.metadata_calls, 0);
      } else {
        check_true(fixture.metadata_calls > 0);
      }
      turbo_flow_destroy(flow);
    }
  }

  it("rolls back the staged adapter when async-terminal binding fails") {
    managed_async_terminal_fixture_t fixture;
    managed_async_terminal_registration_fault_t fault;
    turbo_flow_adapter_ops_t adapter_ops;
    turbo_flow_async_terminal_adapter_ops_t async_ops;
    turbo_flow_adapter_schema_t schema;
    turbo_flow_managed_boundary_provider_ops_t boundary_ops;
    turbo_flow_managed_async_terminal_registration_t registration;
    turbo_flow_t *flow = turbo_flow_create();

    check_not_null(flow);
    memset(&fault, 0, sizeof(fault));
    fault.fail_at = FLOW_REGISTRATION_ASYNC_TERMINAL_BIND;
    fault.status = SALTS_EIO;
    managed_async_terminal_fixture_init(&fixture, "connection:bind-fault", "bind.fault");
    registration = managed_async_terminal_registration(&fixture, &adapter_ops, &async_ops, &schema,
                                                       &boundary_ops);
    flow->registration_fault.before_commit = managed_async_terminal_fail_registration;
    flow->registration_fault.ctx = &fault;

    check_equal(turbo_flow_register_managed_async_terminal_adapter(flow, &registration), SALTS_EIO);
    check_equal(fault.calls[FLOW_REGISTRATION_ASYNC_TERMINAL_BIND], (size_t)1u);
    check_null(turbo_flow_find_adapter_schema(flow, registration.adapter_name));
    check_equal(turbo_flow_managed_boundary_count(flow), (size_t)0u);
    check_equal(turbo_flow_resource_metadata_count(flow), (size_t)0u);
    check_equal(fixture.metadata_calls, 0);
    check_equal(fixture.shutdown_calls, 0);
    turbo_flow_destroy(flow);
  }
}
