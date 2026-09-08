#include "flow_internal.h"
#include "tinytest.h"
#include "turbo_flow.h"

#include <cflow/publishers.h>

#include <limits.h>
#include <stdatomic.h>
#include <string.h>

enum {
  MANAGED_SOURCE_EVENT_PUBLISHER_DESTROY = 1,
  MANAGED_SOURCE_EVENT_STOP,
  MANAGED_SOURCE_EVENT_SHUTDOWN,
  MANAGED_SOURCE_EVENT_SINK
};

typedef struct managed_source_fixture_s {
  turbo_flow_resource_metadata_t metadata;
  turbo_flow_managed_boundary_descriptor_t descriptor;
  turbo_flow_managed_boundary_snapshot_t snapshot;
  turbo_flow_msg_t message;
  turbo_flow_run_t *opened_run;
  int open_on_start;
  int open_status;
  int publisher_valid_after_open;
  int use_counted_publisher;
  int try_copied_stage;
  int copied_stage_status;
  int try_duplicate_open;
  int duplicate_open_status;
  int try_invalid_config;
  int invalid_config_status;
  int try_invalid_publisher;
  int invalid_publisher_status;
  int try_wrong_publisher_type;
  int wrong_publisher_type_status;
  int try_other_thread;
  int other_thread_status;
  int other_thread_publisher_valid;
  int ignore_open_error;
  int start_return_status;
  int stop_calls;
  int publisher_cancels;
  int publisher_error_on_resume;
  atomic_int publisher_destroys;
  atomic_int publisher_emits;
  atomic_int sink_calls;
  atomic_int sink_entered;
  atomic_int sink_release;
  int block_sink;
  int events[16];
  size_t event_count;
  int metadata_calls;
  int descriptor_status;
  int start_calls;
  int shutdown_calls;
} managed_source_fixture_t;

static void managed_source_record_event(managed_source_fixture_t *fixture, int event) {
  if (fixture->event_count < sizeof(fixture->events) / sizeof(fixture->events[0])) {
    fixture->events[fixture->event_count++] = event;
  }
}

static const char *managed_source_publisher_name(void *state) {
  (void)state;
  return "managed-source-test";
}

static const cmeta_type_desc *managed_source_publisher_type(void *state) {
  (void)state;
  return turbo_flow_message_type();
}

static cflow_step managed_source_publisher_resume(void *state, cflow_publish_context *context,
                                                  void *out_value) {
  managed_source_fixture_t *fixture = (managed_source_fixture_t *)state;
  (void)context;
  if (atomic_fetch_add_explicit(&fixture->publisher_emits, 1, memory_order_relaxed) != 0) {
    return (cflow_step){CFLOW_STEP_DONE, {0}, NULL};
  }
  if (fixture->publisher_error_on_resume) {
    return (cflow_step){CFLOW_STEP_ERROR, {0}, "managed Source test error"};
  }
  if (turbo_flow_msg_clone((turbo_flow_msg_t *)out_value, &fixture->message) != SALTS_OK) {
    return (cflow_step){CFLOW_STEP_ERROR, {0}, "message clone failed"};
  }
  return (cflow_step){CFLOW_STEP_VALUE, {0}, NULL};
}

static void managed_source_publisher_cancel(void *state) {
  managed_source_fixture_t *fixture = (managed_source_fixture_t *)state;
  fixture->publisher_cancels += 1;
}

static void managed_source_publisher_destroy(void *state) {
  managed_source_fixture_t *fixture = (managed_source_fixture_t *)state;
  managed_source_record_event(fixture, MANAGED_SOURCE_EVENT_PUBLISHER_DESTROY);
  (void)atomic_fetch_add_explicit(&fixture->publisher_destroys, 1, memory_order_release);
}

static void managed_source_publisher_bind(void *state, cflow_waker waker) {
  (void)state;
  (void)waker;
}

static cflow_publisher_terminal managed_source_publisher_poll(void *state, const char **error) {
  (void)state;
  if (error) *error = NULL;
  return CFLOW_PUBLISHER_OPEN;
}

CMETA_IMPLEMENTS(cflow_publisher, managed_source_publisher, CFLOW_PUBLISHER_CAP_CONSTRUCTS_VALUES,
                 .name = managed_source_publisher_name,
                 .output_type = managed_source_publisher_type,
                 .resume = managed_source_publisher_resume,
                 .cancel = managed_source_publisher_cancel,
                 .destroy = managed_source_publisher_destroy,
                 .bind_terminal_waker = managed_source_publisher_bind,
                 .poll_terminal = managed_source_publisher_poll);

typedef struct managed_source_thread_open_s {
  managed_source_fixture_t *fixture;
  turbo_flow_t *flow;
  const turbo_flow_stage_plan_t *stage;
  int status;
  int publisher_valid;
} managed_source_thread_open_t;

static void managed_source_open_from_thread(void *ctx) {
  managed_source_thread_open_t *call = (managed_source_thread_open_t *)ctx;
  cflow_publisher publisher = {0};
  turbo_flow_run_t *run = NULL;
  if (!cflow_publisher_from_array(&publisher, turbo_flow_message_type(), &call->fixture->message,
                                  1u)) {
    call->status = SALTS_ENOMEM;
    return;
  }
  call->status =
      turbo_flow_managed_source_run_open(call->flow, call->stage, &publisher, NULL, &run);
  call->publisher_valid = cflow_publisher_valid(&publisher) ? 1 : 0;
  if (cflow_publisher_valid(&publisher)) cflow_publisher_destroy(&publisher);
}

static int managed_source_metadata(void *ctx, turbo_flow_resource_metadata_t *out) {
  managed_source_fixture_t *fixture = (managed_source_fixture_t *)ctx;
  if (!fixture || !out || out->size < sizeof(*out)) return SALTS_EINVAL;
  fixture->metadata_calls += 1;
  *out = fixture->metadata;
  return SALTS_OK;
}

static int managed_source_descriptor(void *ctx, turbo_flow_managed_boundary_descriptor_t *out) {
  managed_source_fixture_t *fixture = (managed_source_fixture_t *)ctx;
  if (!fixture || !out || out->size < sizeof(*out)) return SALTS_EINVAL;
  if (fixture->descriptor_status != SALTS_OK) return fixture->descriptor_status;
  *out = fixture->descriptor;
  return SALTS_OK;
}

static int managed_source_snapshot(void *ctx, turbo_flow_managed_boundary_snapshot_t *out) {
  managed_source_fixture_t *fixture = (managed_source_fixture_t *)ctx;
  if (!fixture || !out || out->size < sizeof(*out)) return SALTS_EINVAL;
  *out = fixture->snapshot;
  return SALTS_OK;
}

static int managed_source_start(void *ctx, turbo_flow_t *flow,
                                const turbo_flow_stage_plan_t *stage) {
  managed_source_fixture_t *fixture = (managed_source_fixture_t *)ctx;
  cflow_publisher publisher = {0};
  fixture->start_calls += 1;
  if (!fixture->open_on_start) return SALTS_OK;
  if (fixture->try_copied_stage) {
    turbo_flow_stage_plan_t copied_stage = *stage;
    cflow_publisher copied_publisher = {0};
    turbo_flow_run_t *copied_run = NULL;
    check_true(cflow_publisher_from_array(&copied_publisher, turbo_flow_message_type(),
                                          &fixture->message, 1u));
    fixture->copied_stage_status = turbo_flow_managed_source_run_open(
        flow, &copied_stage, &copied_publisher, NULL, &copied_run);
    check_null(copied_run);
    check_true(cflow_publisher_valid(&copied_publisher));
    cflow_publisher_destroy(&copied_publisher);
  }
  if (fixture->try_invalid_config) {
    turbo_flow_run_config_t invalid_config = TURBO_FLOW_RUN_CONFIG_INIT;
    cflow_publisher invalid_publisher = {0};
    turbo_flow_run_t *invalid_run = NULL;
    invalid_config.version += 1u;
    check_true(cflow_publisher_from_array(&invalid_publisher, turbo_flow_message_type(),
                                          &fixture->message, 1u));
    fixture->invalid_config_status = turbo_flow_managed_source_run_open(
        flow, stage, &invalid_publisher, &invalid_config, &invalid_run);
    check_null(invalid_run);
    check_true(cflow_publisher_valid(&invalid_publisher));
    cflow_publisher_destroy(&invalid_publisher);
  }
  if (fixture->try_invalid_publisher) {
    cflow_publisher invalid_publisher = {0};
    turbo_flow_run_t *invalid_run = NULL;
    fixture->invalid_publisher_status =
        turbo_flow_managed_source_run_open(flow, stage, &invalid_publisher, NULL, &invalid_run);
    check_null(invalid_run);
    check_false(cflow_publisher_valid(&invalid_publisher));
  }
  if (fixture->try_wrong_publisher_type) {
    int value = 1;
    cflow_publisher wrong_publisher = {0};
    turbo_flow_run_t *wrong_run = NULL;
    check_true(cflow_publisher_from_array(&wrong_publisher, &cmeta_type_int, &value, 1u));
    fixture->wrong_publisher_type_status =
        turbo_flow_managed_source_run_open(flow, stage, &wrong_publisher, NULL, &wrong_run);
    check_null(wrong_run);
    check_true(cflow_publisher_valid(&wrong_publisher));
    cflow_publisher_destroy(&wrong_publisher);
  }
  if (fixture->try_other_thread) {
    managed_source_thread_open_t call = {fixture, flow, stage, SALTS_OK, 0};
    salts_thread_t thread = NULL;
    check_equal(salts_thread_create(&thread, managed_source_open_from_thread, &call), SALTS_OK);
    check_equal(salts_thread_join(&thread), SALTS_OK);
    fixture->other_thread_status = call.status;
    fixture->other_thread_publisher_valid = call.publisher_valid;
  }
  if (fixture->use_counted_publisher) {
    publisher = managed_source_publisher_as_cflow_publisher(fixture);
  } else if (!cflow_publisher_from_array(&publisher, turbo_flow_message_type(), &fixture->message,
                                         1u)) {
    fixture->open_status = SALTS_ENOMEM;
    return fixture->open_status;
  }
  fixture->open_status =
      turbo_flow_managed_source_run_open(flow, stage, &publisher, NULL, &fixture->opened_run);
  fixture->publisher_valid_after_open = cflow_publisher_valid(&publisher) ? 1 : 0;
  if (cflow_publisher_valid(&publisher)) cflow_publisher_destroy(&publisher);
  if (fixture->try_duplicate_open && fixture->open_status == SALTS_OK) {
    cflow_publisher duplicate_publisher = {0};
    turbo_flow_run_t *duplicate_run = NULL;
    check_true(cflow_publisher_from_array(&duplicate_publisher, turbo_flow_message_type(),
                                          &fixture->message, 1u));
    fixture->duplicate_open_status =
        turbo_flow_managed_source_run_open(flow, stage, &duplicate_publisher, NULL, &duplicate_run);
    check_null(duplicate_run);
    check_true(cflow_publisher_valid(&duplicate_publisher));
    cflow_publisher_destroy(&duplicate_publisher);
  }
  if (fixture->start_return_status != SALTS_OK) return fixture->start_return_status;
  return fixture->ignore_open_error ? SALTS_OK : fixture->open_status;
}

static int managed_source_sink(turbo_flow_msg_t *message, void *ctx) {
  managed_source_fixture_t *fixture = (managed_source_fixture_t *)ctx;
  (void)message;
  if (fixture) {
    atomic_store_explicit(&fixture->sink_entered, 1, memory_order_release);
    while (fixture->block_sink &&
           !atomic_load_explicit(&fixture->sink_release, memory_order_acquire)) {
      salts_thread_yield();
    }
    managed_source_record_event(fixture, MANAGED_SOURCE_EVENT_SINK);
    (void)atomic_fetch_add_explicit(&fixture->sink_calls, 1, memory_order_release);
  }
  return SALTS_OK;
}

static int managed_source_consume(void *ctx, turbo_flow_t *flow,
                                  const turbo_flow_stage_plan_t *stage, turbo_flow_msg_t *message) {
  (void)ctx;
  (void)flow;
  (void)stage;
  (void)message;
  return SALTS_OK;
}

static int managed_source_later_fail_start(void *ctx, turbo_flow_t *flow,
                                           const turbo_flow_stage_plan_t *stage) {
  (void)ctx;
  (void)flow;
  (void)stage;
  return SALTS_EIO;
}

static void managed_source_stop(void *ctx, turbo_flow_t *flow,
                                const turbo_flow_stage_plan_t *stage) {
  managed_source_fixture_t *fixture = (managed_source_fixture_t *)ctx;
  (void)flow;
  (void)stage;
  fixture->stop_calls += 1;
  managed_source_record_event(fixture, MANAGED_SOURCE_EVENT_STOP);
}

static void managed_source_shutdown(void *ctx) {
  managed_source_fixture_t *fixture = (managed_source_fixture_t *)ctx;
  fixture->shutdown_calls += 1;
  managed_source_record_event(fixture, MANAGED_SOURCE_EVENT_SHUTDOWN);
}

static void managed_source_fixture_init_named(managed_source_fixture_t *fixture, const char *uid,
                                              const char *owner_name) {
  memset(fixture, 0, sizeof(*fixture));
  atomic_init(&fixture->publisher_destroys, 0);
  atomic_init(&fixture->publisher_emits, 0);
  atomic_init(&fixture->sink_calls, 0);
  atomic_init(&fixture->sink_entered, 0);
  atomic_init(&fixture->sink_release, 0);
  turbo_flow_msg_init(&fixture->message);
  fixture->message.id = 41u;
  fixture->open_status = SALTS_EINVAL;
  fixture->metadata = (turbo_flow_resource_metadata_t)TURBO_FLOW_RESOURCE_METADATA_INIT;
  fixture->metadata.domain = TURBO_FLOW_DOMAIN_IO_TRANSPORT;
  fixture->metadata.kind = TURBO_FLOW_RESOURCE_CONNECTION;
  check(strlen(uid) < sizeof(fixture->metadata.uid));
  check(strlen(owner_name) < sizeof(fixture->metadata.owner_name));
  memcpy(fixture->metadata.uid, uid, strlen(uid) + 1u);
  memcpy(fixture->metadata.owner_name, owner_name, strlen(owner_name) + 1u);
  fixture->metadata.generation = 1u;
  fixture->metadata.observed_generation = 1u;
  fixture->descriptor =
      (turbo_flow_managed_boundary_descriptor_t)TURBO_FLOW_MANAGED_BOUNDARY_DESCRIPTOR_INIT;
  fixture->descriptor.domain = fixture->metadata.domain;
  fixture->descriptor.kind = fixture->metadata.kind;
  memcpy(fixture->descriptor.uid, fixture->metadata.uid, strlen(fixture->metadata.uid) + 1u);
  memcpy(fixture->descriptor.owner_name, fixture->metadata.owner_name,
         strlen(fixture->metadata.owner_name) + 1u);
  fixture->descriptor.role_flags = TURBO_FLOW_MANAGED_BOUNDARY_SOURCE;
  fixture->descriptor.capability_flags = TURBO_FLOW_MANAGED_BOUNDARY_DEMAND_AWARE;
  check_equal(turbo_flow_content_descriptor_init(
                  &fixture->descriptor.output, TURBO_FLOW_DOMAIN_IO_TRANSPORT,
                  TURBO_FLOW_CONTENT_PROFILE_GENERIC, TURBO_FLOW_DATA_ENCODING_OPAQUE,
                  "application/octet-stream", "managed.source"),
              SALTS_OK);
  check_equal(turbo_flow_content_descriptor_declare_schema(&fixture->descriptor.output,
                                                           "ManagedSource", "Bytes", 1u),
              SALTS_OK);
  fixture->snapshot =
      (turbo_flow_managed_boundary_snapshot_t)TURBO_FLOW_MANAGED_BOUNDARY_SNAPSHOT_INIT;
  memcpy(fixture->snapshot.uid, fixture->metadata.uid, sizeof("source:managed"));
  fixture->snapshot.generation = 1u;
  fixture->snapshot.observed_generation = 1u;
  fixture->snapshot.state = TURBO_FLOW_MANAGED_BOUNDARY_REGISTERED;
  fixture->snapshot.queue_capacity = 1u;
}

static void managed_source_fixture_init(managed_source_fixture_t *fixture) {
  managed_source_fixture_init_named(fixture, "source:managed", "managed.source");
}

static turbo_flow_managed_source_registration_t managed_source_registration(
    managed_source_fixture_t *fixture, turbo_flow_adapter_ops_t *adapter_ops,
    turbo_flow_adapter_schema_t *schema, turbo_flow_managed_boundary_provider_ops_t *boundary_ops) {
  turbo_flow_managed_source_registration_t registration =
      TURBO_FLOW_MANAGED_SOURCE_REGISTRATION_INIT;
  memset(adapter_ops, 0, sizeof(*adapter_ops));
  memset(schema, 0, sizeof(*schema));
  *boundary_ops =
      (turbo_flow_managed_boundary_provider_ops_t)TURBO_FLOW_MANAGED_BOUNDARY_PROVIDER_OPS_INIT;
  adapter_ops->start = managed_source_start;
  adapter_ops->stop = managed_source_stop;
  adapter_ops->shutdown = managed_source_shutdown;
  schema->kind = TURBO_FLOW_ADAPTER_KIND_CUSTOM;
  schema->roles = TURBO_FLOW_ADAPTER_SOURCE;
  schema->direction = TURBO_FLOW_ADAPTER_INPUT;
  boundary_ops->resource.metadata = managed_source_metadata;
  boundary_ops->descriptor = managed_source_descriptor;
  boundary_ops->snapshot = managed_source_snapshot;
  registration.adapter_name = fixture->metadata.owner_name;
  registration.adapter_ops = adapter_ops;
  registration.schema = schema;
  registration.owner_name = fixture->metadata.owner_name;
  registration.boundary_ops = boundary_ops;
  registration.ctx = fixture;
  return registration;
}

typedef struct managed_source_registration_fault_s {
  flow_registration_checkpoint_t fail_at;
  size_t calls[FLOW_REGISTRATION_CHECKPOINT_COUNT];
} managed_source_registration_fault_t;

typedef struct managed_source_stop_call_s {
  turbo_flow_t *flow;
  atomic_int status;
} managed_source_stop_call_t;

static void managed_source_stop_flow(void *ctx) {
  managed_source_stop_call_t *call = (managed_source_stop_call_t *)ctx;
  atomic_store_explicit(&call->status, turbo_flow_stop(call->flow), memory_order_release);
}

static int managed_source_fail_registration(void *ctx, flow_registration_checkpoint_t checkpoint) {
  managed_source_registration_fault_t *fault = (managed_source_registration_fault_t *)ctx;
  fault->calls[checkpoint] += 1u;
  return checkpoint == fault->fail_at ? SALTS_ENOMEM : SALTS_OK;
}

static void managed_source_check_unregistered(const turbo_flow_t *flow,
                                              const managed_source_fixture_t *fixture,
                                              const char *adapter_name) {
  check_null(turbo_flow_find_adapter_schema(flow, adapter_name));
  check_equal(turbo_flow_resource_metadata_count(flow), (size_t)0u);
  check_equal(turbo_flow_managed_boundary_count(flow), (size_t)0u);
  check_equal(fixture->shutdown_calls, 0);
}

spec("Flow managed Source registration") {
  it("registers one adapter and explicit managed Source atomically") {
    managed_source_fixture_t fixture;
    turbo_flow_adapter_ops_t adapter_ops;
    turbo_flow_adapter_schema_t schema;
    turbo_flow_managed_boundary_provider_ops_t boundary_ops;
    turbo_flow_managed_source_registration_t registration;
    turbo_flow_managed_boundary_descriptor_t descriptor =
        TURBO_FLOW_MANAGED_BOUNDARY_DESCRIPTOR_INIT;
    turbo_flow_t *flow = turbo_flow_create();

    check_not_null(flow);
    managed_source_fixture_init(&fixture);
    registration = managed_source_registration(&fixture, &adapter_ops, &schema, &boundary_ops);
    check_equal(turbo_flow_register_managed_source_adapter(flow, &registration), SALTS_OK);
    check_not_null(turbo_flow_find_adapter_schema(flow, "managed.source"));
    check_equal(turbo_flow_resource_metadata_count(flow), (size_t)1u);
    check_equal(turbo_flow_managed_boundary_count(flow), (size_t)1u);
    check_equal(turbo_flow_managed_boundary_descriptor_at(flow, 0u, &descriptor), SALTS_OK);
    check_equal(descriptor.role_flags, (uint32_t)TURBO_FLOW_MANAGED_BOUNDARY_SOURCE);
    check_equal(fixture.shutdown_calls, 0);
    turbo_flow_destroy(flow);
    check_equal(fixture.shutdown_calls, 1);
  }

  it("opens one Flow-owned Reactive run from the exact managed Source start callback") {
    static const char graph[] = "source input adapter managed.source\n"
                                "stage sink\n"
                                "stage main {\n"
                                "  input -> sink\n"
                                "}\n";
    managed_source_fixture_t fixture;
    turbo_flow_adapter_ops_t adapter_ops;
    turbo_flow_adapter_schema_t schema;
    turbo_flow_managed_boundary_provider_ops_t boundary_ops;
    turbo_flow_managed_source_registration_t registration;
    turbo_flow_run_t *ordinary_run = NULL;
    cflow_publisher ordinary_publisher = {0};
    turbo_flow_t *flow = turbo_flow_create();

    check_not_null(flow);
    managed_source_fixture_init(&fixture);
    fixture.open_on_start = 1;
    registration = managed_source_registration(&fixture, &adapter_ops, &schema, &boundary_ops);
    check_equal(turbo_flow_register_managed_source_adapter(flow, &registration), SALTS_OK);
    check_equal(turbo_flow_register_stage_ex(flow, "sink", managed_source_sink, NULL, NULL),
                SALTS_OK);
    check_equal(turbo_flow_parse_string(flow, graph, sizeof(graph) - 1u), SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_OK);

    check_true(cflow_publisher_from_array(&ordinary_publisher, turbo_flow_message_type(),
                                          &fixture.message, 1u));
    check_equal(turbo_flow_run_open(flow, "input", &ordinary_publisher, NULL, &ordinary_run),
                SALTS_EINVAL);
    check_null(ordinary_run);
    check_true(cflow_publisher_valid(&ordinary_publisher));
    cflow_publisher_destroy(&ordinary_publisher);

    check_equal(turbo_flow_start(flow), SALTS_OK);
    check_equal(fixture.start_calls, 1);
    check_equal(fixture.open_status, SALTS_OK);
    check_not_null(fixture.opened_run);
    check_equal(fixture.publisher_valid_after_open, 0);
    check_equal(vec_size(&flow->active_runs), (size_t)1u);
    check_equal(flow->active_publishes, (uint32_t)1u);

    check_equal(turbo_flow_stop(flow), SALTS_OK);
    check_equal(vec_size(&flow->active_runs), (size_t)0u);
    check_equal(flow->active_publishes, (uint32_t)0u);
    turbo_flow_destroy(flow);
    check_equal(fixture.shutdown_calls, 1);
  }

  it("rejects copied-stage, invalid-config, cross-thread, duplicate, and out-of-scope opens") {
    static const char graph[] = "source input adapter managed.source\n"
                                "stage sink\n"
                                "stage main {\n"
                                "  input -> sink\n"
                                "}\n";
    managed_source_fixture_t fixture;
    turbo_flow_adapter_ops_t adapter_ops;
    turbo_flow_adapter_schema_t schema;
    turbo_flow_managed_boundary_provider_ops_t boundary_ops;
    turbo_flow_managed_source_registration_t registration;
    turbo_flow_stage_plan_t fake_stage;
    cflow_publisher outside_publisher = {0};
    turbo_flow_run_t *outside_run = NULL;
    turbo_flow_t *flow = turbo_flow_create();

    check_not_null(flow);
    managed_source_fixture_init(&fixture);
    fixture.open_on_start = 1;
    fixture.try_copied_stage = 1;
    fixture.try_invalid_config = 1;
    fixture.try_invalid_publisher = 1;
    fixture.try_wrong_publisher_type = 1;
    fixture.try_other_thread = 1;
    fixture.try_duplicate_open = 1;
    registration = managed_source_registration(&fixture, &adapter_ops, &schema, &boundary_ops);
    check_equal(turbo_flow_register_managed_source_adapter(flow, &registration), SALTS_OK);
    check_equal(turbo_flow_register_stage_ex(flow, "sink", managed_source_sink, &fixture, NULL),
                SALTS_OK);
    check_equal(turbo_flow_parse_string(flow, graph, sizeof(graph) - 1u), SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_OK);

    memset(&fake_stage, 0, sizeof(fake_stage));
    fake_stage.name = "input";
    fake_stage.is_source = 1;
    fake_stage.adapter_name = "managed.source";
    check_true(cflow_publisher_from_array(&outside_publisher, turbo_flow_message_type(),
                                          &fixture.message, 1u));
    check_equal(turbo_flow_managed_source_run_open(flow, &fake_stage, &outside_publisher, NULL,
                                                   &outside_run),
                SALTS_EINVAL);
    check_null(outside_run);
    check_true(cflow_publisher_valid(&outside_publisher));
    check_equal(vec_size(&flow->active_runs), (size_t)0u);
    check_equal(flow->active_publishes, (uint32_t)0u);
    cflow_publisher_destroy(&outside_publisher);

    check_equal(turbo_flow_start(flow), SALTS_OK);
    check_equal(fixture.copied_stage_status, SALTS_EINVAL);
    check_equal(fixture.invalid_config_status, SALTS_EINVAL);
    check_equal(fixture.invalid_publisher_status, SALTS_EINVAL);
    check_equal(fixture.wrong_publisher_type_status, SALTS_EPROTO);
    check_equal(fixture.other_thread_status, SALTS_EINVAL);
    check_equal(fixture.other_thread_publisher_valid, 1);
    check_equal(fixture.open_status, SALTS_OK);
    check_equal(fixture.duplicate_open_status, SALTS_EINVAL);
    check_equal(vec_size(&flow->active_runs), (size_t)1u);
    check_equal(flow->active_publishes, (uint32_t)1u);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
  }

  it("enforces the shared Reactive run capacity without taking the rejected Publisher") {
    static const char graph[] = "source first adapter source.first\n"
                                "source second adapter source.second\n"
                                "stage sink\n"
                                "stage main {\n"
                                "  first -> sink\n"
                                "  second -> sink\n"
                                "}\n";
    managed_source_fixture_t first;
    managed_source_fixture_t second;
    turbo_flow_adapter_ops_t first_ops;
    turbo_flow_adapter_ops_t second_ops;
    turbo_flow_adapter_schema_t first_schema;
    turbo_flow_adapter_schema_t second_schema;
    turbo_flow_managed_boundary_provider_ops_t first_boundary;
    turbo_flow_managed_boundary_provider_ops_t second_boundary;
    turbo_flow_managed_source_registration_t first_registration;
    turbo_flow_managed_source_registration_t second_registration;
    turbo_flow_async_ingress_config_t ingress = TURBO_FLOW_ASYNC_INGRESS_CONFIG_INIT;
    turbo_flow_t *flow = turbo_flow_create();

    check_not_null(flow);
    managed_source_fixture_init_named(&first, "source:first", "source.first");
    managed_source_fixture_init_named(&second, "source:second", "source.second");
    first.open_on_start = 1;
    second.open_on_start = 1;
    second.ignore_open_error = 1;
    first_registration =
        managed_source_registration(&first, &first_ops, &first_schema, &first_boundary);
    second_registration =
        managed_source_registration(&second, &second_ops, &second_schema, &second_boundary);
    ingress.workers = 1u;
    ingress.queue_capacity = 1u;
    check_equal(turbo_flow_configure_async_ingress(flow, &ingress), SALTS_OK);
    check_equal(turbo_flow_register_managed_source_adapter(flow, &first_registration), SALTS_OK);
    check_equal(turbo_flow_register_managed_source_adapter(flow, &second_registration), SALTS_OK);
    check_equal(turbo_flow_register_stage_ex(flow, "sink", managed_source_sink, NULL, NULL),
                SALTS_OK);
    check_equal(turbo_flow_parse_string(flow, graph, sizeof(graph) - 1u), SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_OK);
    check_equal(turbo_flow_start(flow), SALTS_OK);
    check_equal(first.open_status, SALTS_OK);
    check_equal(first.publisher_valid_after_open, 0);
    check_equal(second.open_status, SALTS_ENOSPC);
    check_equal(second.publisher_valid_after_open, 1);
    check_equal(vec_size(&flow->active_runs), (size_t)1u);
    check_equal(flow->active_publishes, (uint32_t)1u);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    check_equal(vec_size(&flow->active_runs), (size_t)0u);
    check_equal(flow->active_publishes, (uint32_t)0u);
    turbo_flow_destroy(flow);
  }

  it("rejects managed Source registry mutation after the runtime has compiled") {
    static const char graph[] = "source input adapter managed.source\n"
                                "stage sink\n"
                                "stage main {\n"
                                "  input -> sink\n"
                                "}\n";
    managed_source_fixture_t fixture;
    managed_source_fixture_t late;
    turbo_flow_adapter_ops_t adapter_ops;
    turbo_flow_adapter_ops_t late_ops;
    turbo_flow_adapter_schema_t schema;
    turbo_flow_adapter_schema_t late_schema;
    turbo_flow_managed_boundary_provider_ops_t boundary_ops;
    turbo_flow_managed_boundary_provider_ops_t late_boundary;
    turbo_flow_managed_source_registration_t registration;
    turbo_flow_managed_source_registration_t late_registration;
    turbo_flow_t *flow = turbo_flow_create();

    check_not_null(flow);
    managed_source_fixture_init(&fixture);
    managed_source_fixture_init_named(&late, "source:late", "source.late");
    registration = managed_source_registration(&fixture, &adapter_ops, &schema, &boundary_ops);
    late_registration = managed_source_registration(&late, &late_ops, &late_schema, &late_boundary);
    check_equal(turbo_flow_register_managed_source_adapter(flow, &registration), SALTS_OK);
    check_equal(turbo_flow_register_stage_ex(flow, "sink", managed_source_sink, NULL, NULL),
                SALTS_OK);
    check_equal(turbo_flow_parse_string(flow, graph, sizeof(graph) - 1u), SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_OK);
    check_equal(turbo_flow_register_managed_source_adapter(flow, &late_registration), SALTS_EBUSY);
    check_null(turbo_flow_find_adapter_schema(flow, "source.late"));
    check_equal(turbo_flow_start(flow), SALTS_OK);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    check_equal(turbo_flow_register_managed_source_adapter(flow, &late_registration), SALTS_EBUSY);
    check_null(turbo_flow_find_adapter_schema(flow, "source.late"));
    check_equal(late.shutdown_calls, 0);
    turbo_flow_destroy(flow);
    check_equal(fixture.shutdown_calls, 1);
    check_equal(late.shutdown_calls, 0);
  }

  it("closes a partial managed Source run before rollback stop and owner shutdown") {
    static const char graph[] = "source input adapter managed.source\n"
                                "stage sink\n"
                                "stage main {\n"
                                "  input -> sink\n"
                                "}\n";
    managed_source_fixture_t fixture;
    turbo_flow_adapter_ops_t adapter_ops;
    turbo_flow_adapter_schema_t schema;
    turbo_flow_managed_boundary_provider_ops_t boundary_ops;
    turbo_flow_managed_source_registration_t registration;
    turbo_flow_t *flow = turbo_flow_create();

    check_not_null(flow);
    managed_source_fixture_init(&fixture);
    fixture.open_on_start = 1;
    fixture.use_counted_publisher = 1;
    fixture.start_return_status = SALTS_EIO;
    registration = managed_source_registration(&fixture, &adapter_ops, &schema, &boundary_ops);
    check_equal(turbo_flow_register_managed_source_adapter(flow, &registration), SALTS_OK);
    check_equal(turbo_flow_register_stage_ex(flow, "sink", managed_source_sink, &fixture, NULL),
                SALTS_OK);
    check_equal(turbo_flow_parse_string(flow, graph, sizeof(graph) - 1u), SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_OK);
    check_equal(turbo_flow_start(flow), SALTS_EIO);
    check_equal(fixture.publisher_cancels, 1);
    check_equal(atomic_load_explicit(&fixture.publisher_destroys, memory_order_acquire), 1);
    check_equal(fixture.stop_calls, 1);
    check_equal(fixture.shutdown_calls, 0);
    check_equal(fixture.event_count, (size_t)2u);
    check_equal(fixture.events[0], MANAGED_SOURCE_EVENT_PUBLISHER_DESTROY);
    check_equal(fixture.events[1], MANAGED_SOURCE_EVENT_STOP);
    check_equal(vec_size(&flow->active_runs), (size_t)0u);
    check_equal(flow->active_publishes, (uint32_t)0u);
    turbo_flow_destroy(flow);
    check_equal(fixture.shutdown_calls, 1);
    check_equal(fixture.events[2], MANAGED_SOURCE_EVENT_SHUTDOWN);
  }

  it("propagates a Publisher error and releases its ownership before Source stop") {
    static const char graph[] = "source input adapter managed.source\n"
                                "stage sink\n"
                                "stage main {\n"
                                "  input -> sink\n"
                                "}\n";
    managed_source_fixture_t fixture;
    turbo_flow_adapter_ops_t adapter_ops;
    turbo_flow_adapter_schema_t schema;
    turbo_flow_managed_boundary_provider_ops_t boundary_ops;
    turbo_flow_managed_source_registration_t registration;
    turbo_flow_run_result_t result = TURBO_FLOW_RUN_RESULT_INIT;
    turbo_flow_t *flow = turbo_flow_create();

    check_not_null(flow);
    managed_source_fixture_init(&fixture);
    fixture.open_on_start = 1;
    fixture.use_counted_publisher = 1;
    fixture.publisher_error_on_resume = 1;
    registration = managed_source_registration(&fixture, &adapter_ops, &schema, &boundary_ops);
    check_equal(turbo_flow_register_managed_source_adapter(flow, &registration), SALTS_OK);
    check_equal(turbo_flow_register_stage_ex(flow, "sink", managed_source_sink, &fixture, NULL),
                SALTS_OK);
    check_equal(turbo_flow_parse_string(flow, graph, sizeof(graph) - 1u), SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_OK);
    check_equal(turbo_flow_start(flow), SALTS_OK);
    check_equal(turbo_flow_run_request(fixture.opened_run, 1u), SALTS_OK);
    check_equal(turbo_flow_run_wait(fixture.opened_run, 2000u, &result), SALTS_EIO);
    check_equal(result.status, SALTS_EIO);
    check_equal(atomic_load_explicit(&fixture.sink_calls, memory_order_acquire), 0);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    check_equal(atomic_load_explicit(&fixture.publisher_destroys, memory_order_acquire), 1);
    check_equal(fixture.stop_calls, 1);
    check_equal(fixture.events[0], MANAGED_SOURCE_EVENT_PUBLISHER_DESTROY);
    check_equal(fixture.events[1], MANAGED_SOURCE_EVENT_STOP);
    turbo_flow_destroy(flow);
    check_equal(fixture.shutdown_calls, 1);
  }

  it("closes the managed Source run when a later adapter start fails") {
    static const char graph[] = "source input adapter managed.source\n"
                                "stage output adapter later.fail\n"
                                "stage main {\n"
                                "  input -> output\n"
                                "}\n";
    managed_source_fixture_t fixture;
    turbo_flow_adapter_ops_t source_ops;
    turbo_flow_adapter_ops_t later_ops;
    turbo_flow_adapter_schema_t source_schema;
    turbo_flow_adapter_schema_t later_schema;
    turbo_flow_managed_boundary_provider_ops_t boundary_ops;
    turbo_flow_managed_source_registration_t registration;
    turbo_flow_t *flow = turbo_flow_create();

    check_not_null(flow);
    managed_source_fixture_init(&fixture);
    fixture.open_on_start = 1;
    fixture.use_counted_publisher = 1;
    registration =
        managed_source_registration(&fixture, &source_ops, &source_schema, &boundary_ops);
    memset(&later_ops, 0, sizeof(later_ops));
    memset(&later_schema, 0, sizeof(later_schema));
    later_ops.start = managed_source_later_fail_start;
    later_ops.consume = managed_source_consume;
    later_schema.kind = TURBO_FLOW_ADAPTER_KIND_CUSTOM;
    later_schema.roles = TURBO_FLOW_ADAPTER_SINK;
    later_schema.direction = TURBO_FLOW_ADAPTER_OUTPUT;
    check_equal(turbo_flow_register_managed_source_adapter(flow, &registration), SALTS_OK);
    check_equal(turbo_flow_register_adapter_ex(flow, "later.fail", &later_ops, NULL, &later_schema),
                SALTS_OK);
    check_equal(turbo_flow_parse_string(flow, graph, sizeof(graph) - 1u), SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_OK);
    check_equal(turbo_flow_start(flow), SALTS_EIO);
    check_equal(fixture.publisher_cancels, 1);
    check_equal(atomic_load_explicit(&fixture.publisher_destroys, memory_order_acquire), 1);
    check_equal(fixture.stop_calls, 1);
    check_equal(fixture.events[0], MANAGED_SOURCE_EVENT_PUBLISHER_DESTROY);
    check_equal(fixture.events[1], MANAGED_SOURCE_EVENT_STOP);
    check_equal(vec_size(&flow->active_runs), (size_t)0u);
    check_equal(flow->active_publishes, (uint32_t)0u);
    turbo_flow_destroy(flow);
    check_equal(fixture.shutdown_calls, 1);
  }

  it("preserves an accepted message and closes the managed run before normal Source stop") {
    static const char graph[] = "source input adapter managed.source\n"
                                "stage sink exec thread workers 1\n"
                                "stage main {\n"
                                "  input -> sink\n"
                                "}\n";
    managed_source_fixture_t fixture;
    turbo_flow_adapter_ops_t adapter_ops;
    turbo_flow_adapter_schema_t schema;
    turbo_flow_managed_boundary_provider_ops_t boundary_ops;
    turbo_flow_managed_source_registration_t registration;
    managed_source_stop_call_t stop_call;
    salts_thread_t stop_thread = NULL;
    flow_admission_state_t admission = FLOW_ADMISSION_OPEN;
    turbo_flow_t *flow = turbo_flow_create();

    check_not_null(flow);
    managed_source_fixture_init(&fixture);
    fixture.open_on_start = 1;
    fixture.use_counted_publisher = 1;
    fixture.block_sink = 1;
    registration = managed_source_registration(&fixture, &adapter_ops, &schema, &boundary_ops);
    check_equal(turbo_flow_register_managed_source_adapter(flow, &registration), SALTS_OK);
    check_equal(turbo_flow_register_stage_ex(flow, "sink", managed_source_sink, &fixture, NULL),
                SALTS_OK);
    check_equal(turbo_flow_parse_string(flow, graph, sizeof(graph) - 1u), SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_OK);
    check_equal(turbo_flow_start(flow), SALTS_OK);
    check_equal(turbo_flow_run_request(fixture.opened_run, 1u), SALTS_OK);
    for (size_t attempt = 0u;
         attempt < 2000u && atomic_load_explicit(&fixture.sink_entered, memory_order_acquire) == 0;
         ++attempt) {
      salts_sleep_ms(1u);
    }
    check_equal(atomic_load_explicit(&fixture.publisher_emits, memory_order_acquire), 1);
    check_equal(atomic_load_explicit(&fixture.sink_entered, memory_order_acquire), 1);

    stop_call.flow = flow;
    atomic_init(&stop_call.status, INT_MIN);
    check_equal(salts_thread_create(&stop_thread, managed_source_stop_flow, &stop_call), SALTS_OK);
    for (size_t attempt = 0u; attempt < 2000u; ++attempt) {
      salts_mutex_lock(&flow->runtime_mutex);
      admission = flow->admission_state;
      salts_mutex_unlock(&flow->runtime_mutex);
      if (admission == FLOW_ADMISSION_STOPPING) break;
      salts_sleep_ms(1u);
    }
    check_equal(admission, FLOW_ADMISSION_STOPPING);
    check_equal(atomic_load_explicit(&stop_call.status, memory_order_acquire), INT_MIN);
    atomic_store_explicit(&fixture.sink_release, 1, memory_order_release);
    check_equal(salts_thread_join(&stop_thread), SALTS_OK);
    check_equal(atomic_load_explicit(&stop_call.status, memory_order_acquire), SALTS_OK);
    check_equal(atomic_load_explicit(&fixture.sink_calls, memory_order_acquire), 1);
    check_equal(atomic_load_explicit(&fixture.publisher_destroys, memory_order_acquire), 1);
    check_equal(fixture.stop_calls, 1);
    check_equal(fixture.events[0], MANAGED_SOURCE_EVENT_SINK);
    check_equal(fixture.events[1], MANAGED_SOURCE_EVENT_PUBLISHER_DESTROY);
    check_equal(fixture.events[2], MANAGED_SOURCE_EVENT_STOP);
    turbo_flow_destroy(flow);
  }

  it("rejects invalid and non-Source contracts without transferring ownership") {
    managed_source_fixture_t fixture;
    turbo_flow_adapter_ops_t adapter_ops;
    turbo_flow_adapter_schema_t schema;
    turbo_flow_managed_boundary_provider_ops_t boundary_ops;
    turbo_flow_managed_source_registration_t registration;
    turbo_flow_t *flow = turbo_flow_create();

    check_not_null(flow);
    managed_source_fixture_init(&fixture);
    registration = managed_source_registration(&fixture, &adapter_ops, &schema, &boundary_ops);
    check_equal(turbo_flow_register_managed_source_adapter(flow, NULL), SALTS_EINVAL);
    check_equal(turbo_flow_register_managed_source_adapter(NULL, &registration), SALTS_EINVAL);
    registration.size -= 1u;
    check_equal(turbo_flow_register_managed_source_adapter(flow, &registration), SALTS_EINVAL);
    managed_source_check_unregistered(flow, &fixture, "managed.source");
    registration.size = sizeof(registration);
    registration.version += 1u;
    check_equal(turbo_flow_register_managed_source_adapter(flow, &registration), SALTS_EINVAL);
    managed_source_check_unregistered(flow, &fixture, "managed.source");
    registration.version = TURBO_FLOW_MANAGED_SOURCE_REGISTRATION_API_VERSION;
    registration.adapter_name = NULL;
    check_equal(turbo_flow_register_managed_source_adapter(flow, &registration), SALTS_EINVAL);
    registration.adapter_name = "managed.source";
    registration.adapter_ops = NULL;
    check_equal(turbo_flow_register_managed_source_adapter(flow, &registration), SALTS_EINVAL);
    registration.adapter_ops = &adapter_ops;
    adapter_ops.start = NULL;
    check_equal(turbo_flow_register_managed_source_adapter(flow, &registration), SALTS_EINVAL);
    adapter_ops.start = managed_source_start;
    adapter_ops.consume = managed_source_consume;
    check_equal(turbo_flow_register_managed_source_adapter(flow, &registration), SALTS_EINVAL);
    adapter_ops.consume = NULL;
    registration.schema = NULL;
    check_equal(turbo_flow_register_managed_source_adapter(flow, &registration), SALTS_EINVAL);
    registration.schema = &schema;
    schema.roles = TURBO_FLOW_ADAPTER_SINK;
    check_equal(turbo_flow_register_managed_source_adapter(flow, &registration), SALTS_EINVAL);
    schema.roles = TURBO_FLOW_ADAPTER_SOURCE;
    schema.direction = TURBO_FLOW_ADAPTER_OUTPUT;
    check_equal(turbo_flow_register_managed_source_adapter(flow, &registration), SALTS_EINVAL);
    schema.direction = TURBO_FLOW_ADAPTER_INPUT;
    registration.owner_name = NULL;
    check_equal(turbo_flow_register_managed_source_adapter(flow, &registration), SALTS_EINVAL);
    registration.owner_name = "managed.source";
    registration.boundary_ops = NULL;
    check_equal(turbo_flow_register_managed_source_adapter(flow, &registration), SALTS_EINVAL);
    registration.boundary_ops = &boundary_ops;
    boundary_ops.version += 1u;
    check_equal(turbo_flow_register_managed_source_adapter(flow, &registration), SALTS_EINVAL);
    boundary_ops.version = TURBO_FLOW_MANAGED_BOUNDARY_API_VERSION;
    boundary_ops.resource.metadata = NULL;
    check_equal(turbo_flow_register_managed_source_adapter(flow, &registration), SALTS_EINVAL);
    boundary_ops.resource.metadata = managed_source_metadata;
    boundary_ops.descriptor = NULL;
    check_equal(turbo_flow_register_managed_source_adapter(flow, &registration), SALTS_EINVAL);
    boundary_ops.descriptor = managed_source_descriptor;
    boundary_ops.snapshot = NULL;
    check_equal(turbo_flow_register_managed_source_adapter(flow, &registration), SALTS_EINVAL);
    boundary_ops.snapshot = managed_source_snapshot;
    managed_source_check_unregistered(flow, &fixture, "managed.source");
    fixture.descriptor.role_flags = TURBO_FLOW_MANAGED_BOUNDARY_SINK;
    check_equal(turbo_flow_register_managed_source_adapter(flow, &registration), SALTS_EPROTO);
    managed_source_check_unregistered(flow, &fixture, "managed.source");
    turbo_flow_destroy(flow);
    check_equal(fixture.shutdown_calls, 0);
  }

  it("rolls back every deterministic allocation failure without transferring ownership") {
    static const flow_registration_checkpoint_t checkpoints[] = {
        FLOW_REGISTRATION_ALLOC_ADAPTER_NAME, FLOW_REGISTRATION_ALLOC_ADAPTER_SCHEMA,
        FLOW_REGISTRATION_ALLOC_ADAPTER_VECTOR, FLOW_REGISTRATION_ALLOC_RESOURCE_OWNER_NAME,
        FLOW_REGISTRATION_ALLOC_RESOURCE_VECTOR};

    for (size_t index = 0u; index < sizeof(checkpoints) / sizeof(checkpoints[0]); ++index) {
      managed_source_fixture_t fixture;
      managed_source_registration_fault_t fault;
      turbo_flow_adapter_ops_t adapter_ops;
      turbo_flow_adapter_schema_t schema;
      turbo_flow_option_field_t schema_field;
      turbo_flow_managed_boundary_provider_ops_t boundary_ops;
      turbo_flow_managed_source_registration_t registration;
      turbo_flow_t *flow = turbo_flow_create();

      check_not_null(flow);
      memset(&fault, 0, sizeof(fault));
      memset(&schema_field, 0, sizeof(schema_field));
      fault.fail_at = checkpoints[index];
      managed_source_fixture_init(&fixture);
      registration = managed_source_registration(&fixture, &adapter_ops, &schema, &boundary_ops);
      schema_field.name = "capacity";
      schema_field.type = TURBO_FLOW_OPTION_SIZE;
      schema.fields = &schema_field;
      schema.field_count = 1u;
      flow->registration_fault.before_commit = managed_source_fail_registration;
      flow->registration_fault.ctx = &fault;

      check_equal(turbo_flow_register_managed_source_adapter(flow, &registration), SALTS_ENOMEM);
      check_equal(fault.calls[checkpoints[index]], (size_t)1u);
      check_null(turbo_flow_find_adapter_schema(flow, registration.adapter_name));
      check_equal(turbo_flow_resource_metadata_count(flow), (size_t)0u);
      check_equal(turbo_flow_managed_boundary_count(flow), (size_t)0u);
      check_equal(fixture.shutdown_calls, 0);
      turbo_flow_destroy(flow);
      check_equal(fixture.shutdown_calls, 0);
    }
  }

  it("rolls an adapter back when managed identity registration fails") {
    managed_source_fixture_t first;
    managed_source_fixture_t duplicate;
    turbo_flow_adapter_ops_t first_ops;
    turbo_flow_adapter_ops_t duplicate_ops;
    turbo_flow_adapter_schema_t first_schema;
    turbo_flow_adapter_schema_t duplicate_schema;
    turbo_flow_managed_boundary_provider_ops_t first_boundary;
    turbo_flow_managed_boundary_provider_ops_t duplicate_boundary;
    turbo_flow_managed_source_registration_t first_registration;
    turbo_flow_managed_source_registration_t duplicate_registration;
    turbo_flow_t *flow = turbo_flow_create();

    check_not_null(flow);
    managed_source_fixture_init(&first);
    managed_source_fixture_init(&duplicate);
    first_registration =
        managed_source_registration(&first, &first_ops, &first_schema, &first_boundary);
    duplicate_registration = managed_source_registration(&duplicate, &duplicate_ops,
                                                         &duplicate_schema, &duplicate_boundary);
    duplicate_registration.adapter_name = "managed.source.duplicate";
    check_equal(turbo_flow_register_managed_source_adapter(flow, &first_registration), SALTS_OK);
    check_equal(turbo_flow_register_managed_source_adapter(flow, &duplicate_registration),
                SALTS_EALREADY);
    check_null(turbo_flow_find_adapter_schema(flow, "managed.source.duplicate"));
    check_equal(turbo_flow_resource_metadata_count(flow), (size_t)1u);
    check_equal(first.shutdown_calls, 0);
    check_equal(duplicate.shutdown_calls, 0);
    turbo_flow_destroy(flow);
    check_equal(first.shutdown_calls, 1);
    check_equal(duplicate.shutdown_calls, 0);
  }
}
