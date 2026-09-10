#include "../../tests/flow_operation_fixture.h"
#include "tinytest.h"
#include "turbo_flow.h"

#include <stdatomic.h>
#include <string.h>

#define EXEC_INLINE_ONLY TURBO_FLOW_OPERATION_EXEC_INLINE
#define DOMAIN_ADMISSION_CAPACITY 4u
#define DOMAIN_ADMISSION_CAPACITY_TEXT "4"
#define DOMAIN_ADMISSION_PUBLISHER_COUNT DOMAIN_ADMISSION_CAPACITY
#define DOMAIN_ADMISSION_EXPECTED_QUEUED (DOMAIN_ADMISSION_CAPACITY - 1u)
#define DOMAIN_ADMISSION_TOTAL_SUBMISSIONS (DOMAIN_ADMISSION_PUBLISHER_COUNT + 1u)

typedef struct deadline_probe_s {
  int called;
  int terminal_status;
} deadline_probe_t;

typedef struct admission_probe_s {
  atomic_int entered;
  atomic_int release;
} admission_probe_t;

typedef struct domain_publish_s {
  turbo_flow_t *flow;
  atomic_int result;
} domain_publish_t;

typedef struct settlement_probe_s {
  turbo_flow_settlement_action_t report_action;
  int report_status;
  int report_twice;
  int owner_status;
  int stage_called;
  int owner_called;
  turbo_flow_settlement_result_t observed;
} settlement_probe_t;

typedef struct emission_probe_s {
  uint32_t output_count;
  int callback_status;
  int attach_transport_context;
  int emitter_called;
  uint32_t sink_count;
  uint64_t sink_ids[8];
} emission_probe_t;

typedef enum domain_batch_probe_mode_e {
  DOMAIN_BATCH_PROBE_NORMAL = 0,
  DOMAIN_BATCH_PROBE_INCOMPLETE_SUCCESS,
  DOMAIN_BATCH_PROBE_OUT_OF_ORDER
} domain_batch_probe_mode_t;

typedef struct domain_batch_probe_s {
  domain_batch_probe_mode_t mode;
  size_t scalar_calls;
  size_t batch_calls;
  size_t next_calls;
  size_t observed_count;
  uint64_t observed_ids[8];
  uint64_t fail_id;
  int fail_status;
} domain_batch_probe_t;

typedef struct domain_batch_prepare_probe_s {
  size_t calls;
  size_t fail_index;
  int fail_status;
} domain_batch_prepare_probe_t;

static int domain_noop_stage(turbo_flow_msg_t *message, void *ctx) {
  (void)message;
  (void)ctx;
  return SALTS_OK;
}

static int domain_count_stage(turbo_flow_msg_t *message, void *ctx) {
  int *called = (int *)ctx;
  (void)message;
  *called += 1;
  return SALTS_OK;
}

static int domain_emitting_stage(const turbo_flow_msg_t *input, turbo_flow_emitter_t *emitter,
                                 void *ctx) {
  emission_probe_t *probe = (emission_probe_t *)ctx;
  static int transport_marker;

  probe->emitter_called += 1;
  for (uint32_t i = 0u; i < probe->output_count; ++i) {
    turbo_flow_msg_t output;
    int rc;
    if (probe->attach_transport_context == 2) {
      rc = turbo_flow_msg_clone(&output, input);
      if (rc != SALTS_OK) return rc;
    } else {
      turbo_flow_msg_init(&output);
    }
    output.id = input->id * 10u + i;
    output.type = i;
    if (probe->attach_transport_context == 1) output.transport_context = &transport_marker;
    rc = turbo_flow_emitter_emit_move(emitter, &output);
    turbo_flow_msg_cleanup(&output);
    if (rc != SALTS_OK && rc != SALTS_ENOSPC) return rc;
  }
  return probe->callback_status;
}

static int domain_emission_sink(turbo_flow_msg_t *message, void *ctx) {
  emission_probe_t *probe = (emission_probe_t *)ctx;
  if (probe->sink_count >= sizeof(probe->sink_ids) / sizeof(probe->sink_ids[0])) {
    return SALTS_ENOSPC;
  }
  probe->sink_ids[probe->sink_count++] = message->id;
  return SALTS_OK;
}

static int domain_slow_inline_stage(turbo_flow_msg_t *message, void *ctx) {
  deadline_probe_t *probe = (deadline_probe_t *)ctx;
  (void)message;
  probe->called += 1;
  salts_sleep_ms(15u);
  probe->terminal_status = SALTS_OK;
  return SALTS_OK;
}

static int domain_deadline_yield_stage(turbo_flow_msg_t *message, void *ctx) {
  deadline_probe_t *probe = (deadline_probe_t *)ctx;
  (void)message;
  probe->called += 1;
  for (;;) {
    int status = turbo_flow_execution_yield();
    if (status != SALTS_OK) {
      probe->terminal_status = status;
      return status;
    }
  }
}

static int domain_blocking_stage(turbo_flow_msg_t *message, void *ctx) {
  admission_probe_t *probe = (admission_probe_t *)ctx;
  (void)message;
  atomic_fetch_add_explicit(&probe->entered, 1, memory_order_acq_rel);
  while (!atomic_load_explicit(&probe->release, memory_order_acquire)) salts_thread_yield();
  return SALTS_OK;
}

static void domain_publish_thread(void *ctx) {
  domain_publish_t *publish = (domain_publish_t *)ctx;
  turbo_flow_msg_t message;
  int status;
  turbo_flow_msg_init(&message);
  status = turbo_flow_publish(publish->flow, "input", &message);
  turbo_flow_msg_cleanup(&message);
  atomic_store_explicit(&publish->result, status, memory_order_release);
}

static int domain_settlement_stage(turbo_flow_msg_t *message, void *ctx) {
  settlement_probe_t *probe = (settlement_probe_t *)ctx;
  turbo_flow_settlement_result_t result = TURBO_FLOW_SETTLEMENT_RESULT_INIT;
  (void)message;
  probe->stage_called += 1;
  result.action = probe->report_action;
  result.status = probe->report_status;
  if (turbo_flow_settlement_report(&result) != SALTS_OK) return SALTS_EPROTO;
  if (probe->report_twice && turbo_flow_settlement_report(&result) != SALTS_EALREADY) {
    return SALTS_EPROTO;
  }
  return SALTS_OK;
}

static int domain_settlement_consume(void *ctx, turbo_flow_t *flow,
                                     const turbo_flow_stage_plan_t *stage,
                                     turbo_flow_msg_t *message) {
  (void)flow;
  (void)stage;
  return domain_settlement_stage(message, ctx);
}

static int domain_settlement_apply(void *ctx, turbo_flow_t *flow,
                                   const turbo_flow_stage_plan_t *stage,
                                   const turbo_flow_msg_t *message,
                                   const turbo_flow_settlement_result_t *result) {
  settlement_probe_t *probe = (settlement_probe_t *)ctx;
  (void)flow;
  (void)message;
  if (!stage || !result) return SALTS_EINVAL;
  probe->owner_called += 1;
  probe->observed = *result;
  return probe->owner_status;
}

static int domain_noop_retry(void *ctx, turbo_flow_t *flow, const turbo_flow_stage_plan_t *stage,
                             turbo_flow_msg_t *message, const turbo_flow_retry_policy_t *policy) {
  (void)ctx;
  (void)flow;
  (void)stage;
  (void)message;
  (void)policy;
  return SALTS_OK;
}

static int domain_noop_consume(void *ctx, turbo_flow_t *flow, const turbo_flow_stage_plan_t *stage,
                               turbo_flow_msg_t *message) {
  (void)ctx;
  (void)flow;
  (void)stage;
  (void)message;
  return SALTS_OK;
}

static int domain_batch_consume(void *ctx, turbo_flow_t *flow,
                                const turbo_flow_stage_plan_t *stage,
                                turbo_flow_msg_t *message) {
  domain_batch_probe_t *probe = (domain_batch_probe_t *)ctx;
  (void)flow;
  (void)stage;
  if (!probe || !message) return SALTS_EINVAL;
  probe->scalar_calls += 1u;
  if (probe->observed_count < sizeof(probe->observed_ids) / sizeof(probe->observed_ids[0])) {
    probe->observed_ids[probe->observed_count++] = message->id;
  }
  return message->id == probe->fail_id ? probe->fail_status : SALTS_OK;
}

static int domain_batch_consume_native(void *ctx, turbo_flow_t *flow,
                                       const turbo_flow_stage_plan_t *stage,
                                       const turbo_flow_adapter_batch_t *batch,
                                       size_t *consumed) {
  domain_batch_probe_t *probe = (domain_batch_probe_t *)ctx;
  size_t limit;
  int rc = SALTS_OK;
  (void)flow;
  (void)stage;
  if (!probe || !batch || !batch->next || !consumed) return SALTS_EINVAL;
  *consumed = 0u;
  probe->batch_calls += 1u;
  if (probe->mode == DOMAIN_BATCH_PROBE_OUT_OF_ORDER) {
    turbo_flow_msg_t message;
    turbo_flow_msg_init(&message);
    probe->next_calls += 1u;
    rc = batch->next(batch->ctx, 1u, &message);
    turbo_flow_msg_cleanup(&message);
    return rc;
  }
  limit = probe->mode == DOMAIN_BATCH_PROBE_INCOMPLETE_SUCCESS && batch->message_count > 2u
              ? 2u
              : batch->message_count;
  for (size_t index = 0u; index < limit; ++index) {
    turbo_flow_msg_t message;
    turbo_flow_msg_init(&message);
    probe->next_calls += 1u;
    rc = batch->next(batch->ctx, index, &message);
    if (rc == SALTS_OK) {
      if (probe->observed_count <
          sizeof(probe->observed_ids) / sizeof(probe->observed_ids[0])) {
        probe->observed_ids[probe->observed_count++] = message.id;
      }
      if (message.id == probe->fail_id) {
        rc = probe->fail_status;
      } else {
        *consumed = index + 1u;
      }
    }
    turbo_flow_msg_cleanup(&message);
    if (rc != SALTS_OK) return rc;
  }
  return SALTS_OK;
}

static int domain_batch_prepare(void *ctx, size_t index, turbo_flow_msg_t *message) {
  domain_batch_prepare_probe_t *probe = (domain_batch_prepare_probe_t *)ctx;
  if (!probe || !message) return SALTS_EINVAL;
  probe->calls += 1u;
  if (index == probe->fail_index) return probe->fail_status;
  message->id = index + 1u;
  return SALTS_OK;
}

static void domain_batch_message_observed(void *ctx, const char *source_name,
                                          const turbo_flow_msg_t *message,
                                          uint64_t duration_ns, int status) {
  size_t *observed = (size_t *)ctx;
  (void)source_name;
  (void)message;
  (void)duration_ns;
  (void)status;
  if (observed) *observed += 1u;
}

static int domain_noop_adapter_start(void *ctx, turbo_flow_t *flow,
                                     const turbo_flow_stage_plan_t *stage) {
  (void)ctx;
  (void)flow;
  (void)stage;
  return SALTS_OK;
}

static void domain_count_shutdown(void *ctx) {
  int *shutdown_count = (int *)ctx;
  *shutdown_count += 1;
}

static turbo_flow_primitive_descriptor_t
resource_descriptor(const char *name, const char *type_name, turbo_flow_domain_t domain) {
  turbo_flow_primitive_descriptor_t descriptor;
  memset(&descriptor, 0, sizeof(descriptor));
  descriptor.size = sizeof(descriptor);
  descriptor.name = name;
  descriptor.type_name = type_name;
  descriptor.version = 1;
  descriptor.domain = domain;
  descriptor.kind = TURBO_FLOW_PRIMITIVE_RESOURCE;
  return descriptor;
}

static turbo_flow_operation_descriptor_t
operation_descriptor(const char *name, turbo_flow_domain_t domain, turbo_flow_domain_t input_domain,
                     const char *input_type, turbo_flow_domain_t output_domain,
                     const char *output_type, uint32_t flags) {
  turbo_flow_operation_descriptor_t descriptor;
  memset(&descriptor, 0, sizeof(descriptor));
  descriptor.size = sizeof(descriptor);
  descriptor.name = name;
  descriptor.version = 1;
  descriptor.domain = domain;
  descriptor.input_domain = input_domain;
  descriptor.input_type = input_type;
  descriptor.output_domain = output_domain;
  descriptor.output_type = output_type;
  descriptor.scope.data = TURBO_FLOW_DATA_SCOPE_MESSAGE;
  descriptor.scope.state = TURBO_FLOW_STATE_SCOPE_NONE;
  descriptor.scope.lifetime = TURBO_FLOW_LIFETIME_DISPATCH;
  descriptor.scope.concurrency = TURBO_FLOW_CONCURRENCY_INLINE_LANE;
  descriptor.scope.authority = TURBO_FLOW_AUTHORITY_PURE;
  descriptor.flags = flags;
  descriptor.execution_mask = EXEC_INLINE_ONLY;
  return descriptor;
}

suite("Explicit operation binding") {
  it("reuses one operation across differently named stages") {
    const char *dsl = "source input\n"
                      "stage first operation test.transform\n"
                      "stage second operation test.transform\n"
                      "stage main {\n input -> first -> second\n}\n";
    turbo_flow_t *flow = turbo_flow_create();
    int calls = 0;
    flow_test_operation_t operation =
        flow_test_operation_init("test.transform", domain_count_stage, &calls);
    turbo_flow_msg_t message;
    operation.descriptor.scope.state = TURBO_FLOW_STATE_SCOPE_GRAPH;
    operation.descriptor.scope.lifetime = TURBO_FLOW_LIFETIME_RUNTIME_GENERATION;
    check_equal(flow_test_operation_register(flow, &operation), SALTS_OK);
    check_equal(turbo_flow_parse_string(flow, dsl, strlen(dsl)), SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_OK);
    check_equal(turbo_flow_start(flow), SALTS_OK);
    turbo_flow_msg_init(&message);
    check_equal(turbo_flow_publish(flow, "input", &message), SALTS_OK);
    check_equal(calls, 2);
    turbo_flow_msg_cleanup(&message);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
  }

  it("does not bind a same-name operation without explicit DSL identity") {
    const char *dsl = "source input\nstage transform\nstage main {\n input -> transform\n}\n";
    turbo_flow_t *flow = turbo_flow_create();
    int calls = 0;
    flow_test_operation_t operation =
        flow_test_operation_init("transform", domain_count_stage, &calls);
    check_equal(flow_test_operation_register(flow, &operation), SALTS_OK);
    check_equal(turbo_flow_parse_string(flow, dsl, strlen(dsl)), SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_EINVAL);
    check_equal(calls, 0);
    turbo_flow_destroy(flow);
  }

  it("rejects a missing descriptor or provider before any callback") {
    const char *dsl = "source input\nstage transform operation test.transform\n"
                      "stage main {\n input -> transform\n}\n";
    for (int provider_only = 0; provider_only < 2; ++provider_only) {
      turbo_flow_t *flow = turbo_flow_create();
      int calls = 0;
      flow_test_operation_t operation =
          flow_test_operation_init("test.transform", domain_count_stage, &calls);
      check_equal(provider_only ? turbo_flow_register_operation_provider(flow, &operation.provider)
                                : turbo_flow_register_operation(flow, &operation.descriptor),
                  SALTS_OK);
      check_equal(turbo_flow_parse_string(flow, dsl, strlen(dsl)), SALTS_OK);
      check_equal(turbo_flow_compile(flow), SALTS_EINVAL);
      check_equal(calls, 0);
      turbo_flow_destroy(flow);
    }
  }

  it("locks compiled registration and resets both contracts together") {
    const char *dsl = "source input\nstage transform operation test.transform\n"
                      "stage main {\n input -> transform\n}\n";
    turbo_flow_t *flow = turbo_flow_create();
    flow_test_operation_t operation =
        flow_test_operation_init("test.transform", domain_noop_stage, NULL);
    check_equal(flow_test_operation_register(flow, &operation), SALTS_OK);
    check_equal(turbo_flow_register_operation(flow, &operation.descriptor), SALTS_EALREADY);
    check_equal(turbo_flow_register_operation_provider(flow, &operation.provider), SALTS_EALREADY);
    check_equal(turbo_flow_parse_string(flow, dsl, strlen(dsl)), SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_OK);
    check_equal(turbo_flow_register_operation(flow, &operation.descriptor), SALTS_EBUSY);
    check_equal(turbo_flow_register_operation_provider(flow, &operation.provider), SALTS_EBUSY);
    check_equal(turbo_flow_reset(flow, 1), SALTS_OK);
    check_equal(turbo_flow_parse_string(flow, dsl, strlen(dsl)), SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_OK);
    check_equal(turbo_flow_reset(flow, 0), SALTS_OK);
    check_equal(flow_test_operation_register(flow, &operation), SALTS_OK);
    check_equal(turbo_flow_parse_string(flow, dsl, strlen(dsl)), SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_OK);
    turbo_flow_destroy(flow);
  }
}

static int register_domain_batch_graph(turbo_flow_t *flow, domain_batch_probe_t *probe,
                                       size_t registration_size, int native_batch) {
  static const char *dsl =
      "source input\n"
      "stage sink adapter native.batch.instance operation native.batch.consume\n"
      "stage main {\n"
      "  input -> sink\n"
      "}\n";
  static const char *const operation_names[] = {"native.batch.consume"};
  turbo_flow_operation_descriptor_t operation = operation_descriptor(
      "native.batch.consume", TURBO_FLOW_DOMAIN_PROTOCOL_PATTERN, TURBO_FLOW_DOMAIN_DATA,
      "Message", TURBO_FLOW_DOMAIN_NONE, NULL,
      TURBO_FLOW_OPERATION_STAGE | TURBO_FLOW_OPERATION_BRIDGE);
  turbo_flow_module_descriptor_t module = {0};
  turbo_flow_adapter_ops_t ops = {0};
  turbo_flow_adapter_schema_t schema = {0};
  turbo_flow_module_adapter_registration_t adapter =
      TURBO_FLOW_MODULE_ADAPTER_REGISTRATION_INIT;
  int rc;

  operation.scope.state = TURBO_FLOW_STATE_SCOPE_ADAPTER_OWNER;
  operation.scope.concurrency = TURBO_FLOW_CONCURRENCY_OWNER_CONTEXT;
  operation.scope.authority = TURBO_FLOW_AUTHORITY_OWNER_LOCAL;
  module.size = sizeof(module);
  module.name = "native.batch";
  module.version = 1u;
  module.capability_flags =
      TURBO_FLOW_MODULE_GRAPH_OPERATIONS | TURBO_FLOW_MODULE_NATIVE_API;
  module.operation_names = operation_names;
  module.operation_count = 1u;
  ops.consume = domain_batch_consume;
  schema.kind = TURBO_FLOW_ADAPTER_KIND_CUSTOM;
  schema.roles = TURBO_FLOW_ADAPTER_SINK;
  schema.direction = TURBO_FLOW_ADAPTER_OUTPUT;
  adapter.size = registration_size;
  adapter.module_name = module.name;
  adapter.adapter_name = "native.batch.instance";
  adapter.ops = &ops;
  adapter.ctx = probe;
  adapter.schema = &schema;
  adapter.operation_names = operation_names;
  adapter.operation_count = 1u;
  adapter.consume_batch = native_batch ? domain_batch_consume_native : NULL;

  rc = turbo_flow_register_module_contract(flow, &module, &operation, 1u);
  if (rc != SALTS_OK) return rc;
  rc = turbo_flow_register_module_adapter(flow, &adapter);
  if (rc != SALTS_OK) return rc;
  rc = turbo_flow_parse_string(flow, dsl, strlen(dsl));
  if (rc != SALTS_OK) return rc;
  return turbo_flow_compile(flow);
}

static void require_resource(turbo_flow_operation_descriptor_t *operation,
                             turbo_flow_domain_t domain, const char *type_name) {
  operation->resource_domain = domain;
  operation->resource_type = type_name;
  operation->scope.state = TURBO_FLOW_STATE_SCOPE_RESOURCE_OWNER;
  operation->scope.authority = TURBO_FLOW_AUTHORITY_OWNER_LOCAL;
}

static void require_worker_handoff(turbo_flow_operation_descriptor_t *operation,
                                   uint32_t capacity) {
  operation->scope.concurrency = TURBO_FLOW_CONCURRENCY_POOL;
  operation->runtime.handoff = TURBO_FLOW_HANDOFF_BOUNDED;
  operation->runtime.backpressure = TURBO_FLOW_BACKPRESSURE_BLOCK;
  operation->runtime.capacity = capacity;
}

static int register_emitting_graph(turbo_flow_t *flow, emission_probe_t *probe,
                                   uint32_t max_outputs, const char *dsl) {
  turbo_flow_operation_descriptor_t input =
      operation_descriptor("data.input", TURBO_FLOW_DOMAIN_DATA, TURBO_FLOW_DOMAIN_NONE, NULL,
                           TURBO_FLOW_DOMAIN_DATA, "Message", TURBO_FLOW_OPERATION_SOURCE);
  turbo_flow_operation_descriptor_t expand =
      operation_descriptor("data.expand", TURBO_FLOW_DOMAIN_DATA, TURBO_FLOW_DOMAIN_DATA,
                           "Message", TURBO_FLOW_DOMAIN_DATA, "Message",
                           TURBO_FLOW_OPERATION_STAGE);
  turbo_flow_emitting_operation_provider_registration_t provider =
      TURBO_FLOW_EMITTING_OPERATION_PROVIDER_REGISTRATION_INIT;
  int rc;

  provider.operation_name = "data.expand";
  provider.fn = domain_emitting_stage;
  provider.ctx = probe;
  provider.max_outputs = max_outputs;
  rc = turbo_flow_register_operation(flow, &input);
  if (rc != SALTS_OK) return rc;
  rc = turbo_flow_register_operation(flow, &expand);
  if (rc != SALTS_OK) return rc;
  rc = turbo_flow_register_emitting_operation_provider(flow, &provider);
  if (rc != SALTS_OK) return rc;
  flow_test_operation_t operation_provider_0 =
      flow_test_operation_init("test.sink", domain_emission_sink, probe);
  operation_provider_0.descriptor.scope.state = TURBO_FLOW_STATE_SCOPE_GRAPH;
  operation_provider_0.descriptor.scope.lifetime = TURBO_FLOW_LIFETIME_RUNTIME_GENERATION;
  rc = flow_test_operation_register(flow, &operation_provider_0);
  if (rc != SALTS_OK) return rc;
  return turbo_flow_parse_string(flow, dsl, strlen(dsl));
}

suite("Turbo Flow Domain Contracts") {
  group("Registry") {
    it("copies primitive and operation descriptors into the flow registry") {
      turbo_flow_t *flow = turbo_flow_create();
      char primitive_name[] = "queue.jobs";
      char operation_name[] = "queue.enqueue";
      turbo_flow_primitive_descriptor_t primitive =
          resource_descriptor(primitive_name, "Queue", TURBO_FLOW_DOMAIN_BUFFER_PERSISTENCE);
      turbo_flow_operation_descriptor_t operation =
          operation_descriptor(operation_name, TURBO_FLOW_DOMAIN_BUFFER_PERSISTENCE,
                               TURBO_FLOW_DOMAIN_DATA, "Message", TURBO_FLOW_DOMAIN_DATA, "Message",
                               TURBO_FLOW_OPERATION_STAGE | TURBO_FLOW_OPERATION_BRIDGE);
      const turbo_flow_primitive_descriptor_t *stored_primitive;
      const turbo_flow_operation_descriptor_t *stored_operation;

      require_resource(&operation, TURBO_FLOW_DOMAIN_BUFFER_PERSISTENCE, "Queue");
      check_not_null(flow);
      check_equal(turbo_flow_register_primitive(flow, &primitive), SALTS_OK);
      check_equal(turbo_flow_register_operation(flow, &operation), SALTS_OK);
      primitive_name[0] = 'x';
      operation_name[0] = 'x';

      stored_primitive = turbo_flow_find_primitive(flow, "queue.jobs");
      stored_operation = turbo_flow_find_operation(flow, "queue.enqueue");
      check_not_null(stored_primitive);
      check_not_null(stored_operation);
      check_equal(stored_primitive->type_name, "Queue");
      check_equal(stored_operation->resource_type, "Queue");
      check_equal(turbo_flow_primitive_count(flow), 1);
      check_equal(turbo_flow_operation_count(flow), 1);
      turbo_flow_destroy(flow);
    }

    it("rejects truncated operation descriptors") {
      turbo_flow_t *flow = turbo_flow_create();
      turbo_flow_operation_descriptor_t operation = operation_descriptor(
          "data.validate", TURBO_FLOW_DOMAIN_DATA, TURBO_FLOW_DOMAIN_DATA, "Message",
          TURBO_FLOW_DOMAIN_DATA, "Message", TURBO_FLOW_OPERATION_STAGE);
      operation.size = offsetof(turbo_flow_operation_descriptor_t, runtime);
      check_not_null(flow);
      check_equal(turbo_flow_register_operation(flow, &operation), SALTS_EINVAL);
      turbo_flow_destroy(flow);
    }

    it("rejects invalid bounded runtime contracts at registration") {
      turbo_flow_t *flow = turbo_flow_create();
      turbo_flow_operation_descriptor_t operation = operation_descriptor(
          "data.parallel", TURBO_FLOW_DOMAIN_DATA, TURBO_FLOW_DOMAIN_DATA, "Message",
          TURBO_FLOW_DOMAIN_DATA, "Message", TURBO_FLOW_OPERATION_STAGE);

      check_not_null(flow);
      operation.runtime.handoff = TURBO_FLOW_HANDOFF_BOUNDED;
      operation.runtime.capacity = 3u;
      operation.runtime.backpressure = TURBO_FLOW_BACKPRESSURE_BLOCK;
      check_equal(turbo_flow_register_operation(flow, &operation), SALTS_EINVAL);

      operation.runtime.capacity = 64u;
      operation.runtime.backpressure = TURBO_FLOW_BACKPRESSURE_NONE;
      check_equal(turbo_flow_register_operation(flow, &operation), SALTS_EINVAL);
      turbo_flow_destroy(flow);
    }

    it("rejects cross-domain operation contracts without the bridge role") {
      turbo_flow_t *flow = turbo_flow_create();
      turbo_flow_operation_descriptor_t operation = operation_descriptor(
          "mqtt.decode", TURBO_FLOW_DOMAIN_PROTOCOL_PATTERN, TURBO_FLOW_DOMAIN_PROTOCOL_PATTERN,
          "PublishFrame", TURBO_FLOW_DOMAIN_DATA, "Message", TURBO_FLOW_OPERATION_STAGE);

      check_not_null(flow);
      check_equal(turbo_flow_register_operation(flow, &operation), SALTS_EINVAL);
      turbo_flow_destroy(flow);
    }

    it("rejects owner state scope without a resource contract") {
      turbo_flow_t *flow = turbo_flow_create();
      turbo_flow_operation_descriptor_t operation = operation_descriptor(
          "queue.enqueue", TURBO_FLOW_DOMAIN_BUFFER_PERSISTENCE,
          TURBO_FLOW_DOMAIN_BUFFER_PERSISTENCE, "Record", TURBO_FLOW_DOMAIN_BUFFER_PERSISTENCE,
          "Record", TURBO_FLOW_OPERATION_STAGE);

      operation.scope.state = TURBO_FLOW_STATE_SCOPE_RESOURCE_OWNER;
      operation.scope.authority = TURBO_FLOW_AUTHORITY_OWNER_LOCAL;
      check_not_null(flow);
      check_equal(turbo_flow_register_operation(flow, &operation), SALTS_EINVAL);
      turbo_flow_destroy(flow);
    }

    it("rejects non-exact operation descriptor layouts without registry side effects") {
      turbo_flow_t *flow = turbo_flow_create();
      turbo_flow_operation_descriptor_t operation = operation_descriptor(
          "queue.exact", TURBO_FLOW_DOMAIN_BUFFER_PERSISTENCE, TURBO_FLOW_DOMAIN_DATA,
          "Message", TURBO_FLOW_DOMAIN_DATA, "Message",
          TURBO_FLOW_OPERATION_STAGE | TURBO_FLOW_OPERATION_BRIDGE);
      static const size_t invalid_sizes[] = {
          0u,
          sizeof(size_t),
          offsetof(turbo_flow_operation_descriptor_t, resource_min_version),
          sizeof(turbo_flow_operation_descriptor_t) - 1u,
          sizeof(turbo_flow_operation_descriptor_t) + 1u,
          SIZE_MAX};
      unsigned char original[sizeof(operation)];
      size_t *physical_short;
      unsigned char *legacy_prefix;

      require_resource(&operation, TURBO_FLOW_DOMAIN_BUFFER_PERSISTENCE, "Queue");
      operation.resource_min_version = 9u;
      operation.resource_max_version = 9u;
      check_not_null(flow);
      physical_short = (size_t *)malloc(sizeof(*physical_short));
      check_not_null(physical_short);
      *physical_short = sizeof(*physical_short);
      check_equal(turbo_flow_register_operation(
                      flow, (const turbo_flow_operation_descriptor_t *)physical_short),
                  SALTS_EINVAL);
      check_equal(*physical_short, sizeof(*physical_short));
      check_equal(turbo_flow_operation_count(flow), 0u);
      check_null(turbo_flow_find_operation(flow, operation.name));
      free(physical_short);

      legacy_prefix = (unsigned char *)malloc(
          offsetof(turbo_flow_operation_descriptor_t, resource_min_version));
      check_not_null(legacy_prefix);
      memcpy(legacy_prefix, &operation,
             offsetof(turbo_flow_operation_descriptor_t, resource_min_version));
      *(size_t *)legacy_prefix =
          offsetof(turbo_flow_operation_descriptor_t, resource_min_version);
      memcpy(original, legacy_prefix,
             offsetof(turbo_flow_operation_descriptor_t, resource_min_version));
      check_equal(turbo_flow_register_operation(
                      flow, (const turbo_flow_operation_descriptor_t *)legacy_prefix),
                  SALTS_EINVAL);
      check_equal(legacy_prefix, original,
                  offsetof(turbo_flow_operation_descriptor_t, resource_min_version));
      check_equal(turbo_flow_operation_count(flow), 0u);
      check_null(turbo_flow_find_operation(flow, operation.name));
      free(legacy_prefix);

      for (size_t i = 0u; i < sizeof(invalid_sizes) / sizeof(invalid_sizes[0]); ++i) {
        operation.size = invalid_sizes[i];
        memcpy(original, &operation, sizeof(operation));
        check_equal(turbo_flow_register_operation(flow, &operation), SALTS_EINVAL);
        check_equal(&operation, original, sizeof(operation));
        check_equal(turbo_flow_operation_count(flow), 0u);
        check_null(turbo_flow_find_operation(flow, operation.name));
      }
      operation.size = sizeof(operation);
      check_equal(turbo_flow_register_operation(flow, &operation), SALTS_OK);
      check_equal(turbo_flow_operation_count(flow), 1u);
      check_not_null(turbo_flow_find_operation(flow, operation.name));
      turbo_flow_destroy(flow);
    }

    it("preserves exact resource versions and rejects inverted ranges") {
      turbo_flow_t *flow = turbo_flow_create();
      turbo_flow_operation_descriptor_t bounded = operation_descriptor(
          "queue.bounded", TURBO_FLOW_DOMAIN_BUFFER_PERSISTENCE, TURBO_FLOW_DOMAIN_DATA,
          "Message", TURBO_FLOW_DOMAIN_DATA, "Message",
          TURBO_FLOW_OPERATION_STAGE | TURBO_FLOW_OPERATION_BRIDGE);
      turbo_flow_operation_descriptor_t invalid = operation_descriptor(
          "queue.invalid", TURBO_FLOW_DOMAIN_BUFFER_PERSISTENCE, TURBO_FLOW_DOMAIN_DATA,
          "Message", TURBO_FLOW_DOMAIN_DATA, "Message",
          TURBO_FLOW_OPERATION_STAGE | TURBO_FLOW_OPERATION_BRIDGE);
      const turbo_flow_operation_descriptor_t *stored;

      require_resource(&bounded, TURBO_FLOW_DOMAIN_BUFFER_PERSISTENCE, "Queue");
      bounded.resource_min_version = 9u;
      bounded.resource_max_version = 9u;
      require_resource(&invalid, TURBO_FLOW_DOMAIN_BUFFER_PERSISTENCE, "Queue");
      invalid.resource_min_version = 3u;
      invalid.resource_max_version = 2u;
      check_not_null(flow);
      check_equal(turbo_flow_register_operation(flow, &bounded), SALTS_OK);
      stored = turbo_flow_find_operation(flow, "queue.bounded");
      check_not_null(stored);
      check_equal(stored->resource_min_version, 9u);
      check_equal(stored->resource_max_version, 9u);
      check_equal(turbo_flow_register_operation(flow, &invalid), SALTS_EINVAL);
      turbo_flow_destroy(flow);
    }

    it("preserves contracts only when reset keeps the registry") {
      turbo_flow_t *flow = turbo_flow_create();
      turbo_flow_primitive_descriptor_t primitive =
          resource_descriptor("queue.jobs", "Queue", TURBO_FLOW_DOMAIN_BUFFER_PERSISTENCE);
      turbo_flow_operation_descriptor_t operation = operation_descriptor(
          "data.validate", TURBO_FLOW_DOMAIN_DATA, TURBO_FLOW_DOMAIN_DATA, "Message",
          TURBO_FLOW_DOMAIN_DATA, "Message", TURBO_FLOW_OPERATION_STAGE);

      check_not_null(flow);
      check_equal(turbo_flow_register_primitive(flow, &primitive), SALTS_OK);
      check_equal(turbo_flow_register_operation(flow, &operation), SALTS_OK);
      check_equal(turbo_flow_reset(flow, 1), SALTS_OK);
      check_equal(turbo_flow_primitive_count(flow), 1);
      check_equal(turbo_flow_operation_count(flow), 1);
      check_equal(turbo_flow_reset(flow, 0), SALTS_OK);
      check_equal(turbo_flow_primitive_count(flow), 0);
      check_equal(turbo_flow_operation_count(flow), 0);
      check_equal(turbo_flow_module_count(flow), 0);
      turbo_flow_destroy(flow);
    }

    it("copies module exports and validates dependency contracts") {
      turbo_flow_t *flow = turbo_flow_create();
      char native_name[] = "io.native";
      char operation_name[] = "data.validate";
      const char *operation_names[] = {operation_name};
      turbo_flow_operation_descriptor_t operation = operation_descriptor(
          operation_name, TURBO_FLOW_DOMAIN_DATA, TURBO_FLOW_DOMAIN_DATA, "Message",
          TURBO_FLOW_DOMAIN_DATA, "Message", TURBO_FLOW_OPERATION_STAGE);
      turbo_flow_module_descriptor_t native_module = {0};
      turbo_flow_module_requirement_t requirement = {0};
      turbo_flow_module_descriptor_t graph_module = {0};
      const turbo_flow_module_descriptor_t *stored;

      check_not_null(flow);
      native_module.size = sizeof(native_module);
      native_module.name = native_name;
      native_module.version = 2u;
      native_module.capability_flags = TURBO_FLOW_MODULE_NATIVE_API;
      check_equal(turbo_flow_register_module(flow, &native_module), SALTS_OK);

      check_equal(turbo_flow_register_operation(flow, &operation), SALTS_OK);
      requirement.size = sizeof(requirement);
      requirement.module_name = native_name;
      requirement.min_version = 3u;
      requirement.capability_flags = TURBO_FLOW_MODULE_NATIVE_API;
      graph_module.size = sizeof(graph_module);
      graph_module.name = "data.validation";
      graph_module.version = 1u;
      graph_module.capability_flags = TURBO_FLOW_MODULE_GRAPH_OPERATIONS;
      graph_module.operation_names = operation_names;
      graph_module.operation_count = 1u;
      graph_module.requirements = &requirement;
      graph_module.requirement_count = 1u;
      check_equal(turbo_flow_register_module(flow, &graph_module), SALTS_EPROTO);

      requirement.min_version = 2u;
      check_equal(turbo_flow_register_module(flow, &graph_module), SALTS_OK);
      native_name[0] = 'x';
      operation_name[0] = 'x';
      stored = turbo_flow_find_module(flow, "data.validation");
      check_not_null(stored);
      check_equal(stored->operation_names[0], "data.validate");
      check_equal(stored->requirements[0].module_name, "io.native");
      check_equal(turbo_flow_module_count(flow), 2u);
      check_equal(turbo_flow_reset(flow, 1), SALTS_OK);
      check_equal(turbo_flow_module_count(flow), 2u);
      check_equal(turbo_flow_reset(flow, 0), SALTS_OK);
      check_equal(turbo_flow_module_count(flow), 0u);
      turbo_flow_destroy(flow);
    }

    it("registers complete module contracts idempotently and rejects drift") {
      static const char *const operation_names[] = {"data.validate"};
      turbo_flow_t *flow = turbo_flow_create();
      turbo_flow_operation_descriptor_t operation = operation_descriptor(
          "data.validate", TURBO_FLOW_DOMAIN_DATA, TURBO_FLOW_DOMAIN_DATA, "Message",
          TURBO_FLOW_DOMAIN_DATA, "Message", TURBO_FLOW_OPERATION_STAGE);
      turbo_flow_module_descriptor_t module = {0};

      module.size = sizeof(module);
      module.name = "data.validation";
      module.version = 1u;
      module.capability_flags = TURBO_FLOW_MODULE_GRAPH_OPERATIONS;
      module.operation_names = operation_names;
      module.operation_count = 1u;
      check_not_null(flow);
      check_equal(turbo_flow_register_module_contract(flow, &module, &operation, 1u), SALTS_OK);
      check_equal(turbo_flow_register_module_contract(flow, &module, &operation, 1u), SALTS_OK);
      check_equal(turbo_flow_operation_count(flow), 1u);
      check_equal(turbo_flow_module_count(flow), 1u);
      operation.version = 2u;
      check_equal(turbo_flow_register_module_contract(flow, &module, &operation, 1u),
                   SALTS_EPROTO);
      turbo_flow_destroy(flow);
    }

    it("rejects non-exact operation layouts from module contracts before registry access") {
      static const char *const operation_names[] = {"data.validate"};

      for (size_t existing = 0u; existing < 2u; ++existing) {
        turbo_flow_t *flow = turbo_flow_create();
        turbo_flow_operation_descriptor_t operation = operation_descriptor(
            "data.validate", TURBO_FLOW_DOMAIN_DATA, TURBO_FLOW_DOMAIN_DATA, "Message",
            TURBO_FLOW_DOMAIN_DATA, "Message", TURBO_FLOW_OPERATION_STAGE);
        turbo_flow_module_descriptor_t module = {0};
        size_t *physical_short;
        unsigned char *legacy_prefix;

        module.size = sizeof(module);
        module.name = "data.validation";
        module.version = 1u;
        module.capability_flags = TURBO_FLOW_MODULE_GRAPH_OPERATIONS;
        module.operation_names = operation_names;
        module.operation_count = 1u;
        check_not_null(flow);
        if (existing != 0u) {
          check_equal(turbo_flow_register_module_contract(flow, &module, &operation, 1u),
                      SALTS_OK);
        }

        physical_short = (size_t *)malloc(sizeof(*physical_short));
        check_not_null(physical_short);
        *physical_short = sizeof(*physical_short);
        check_equal(turbo_flow_register_module_contract(
                        flow, &module,
                        (const turbo_flow_operation_descriptor_t *)physical_short, 1u),
                    SALTS_EINVAL);
        check_equal(turbo_flow_operation_count(flow), existing);
        check_equal(turbo_flow_module_count(flow), existing);
        free(physical_short);

        legacy_prefix = (unsigned char *)malloc(
            offsetof(turbo_flow_operation_descriptor_t, resource_min_version));
        check_not_null(legacy_prefix);
        memcpy(legacy_prefix, &operation,
               offsetof(turbo_flow_operation_descriptor_t, resource_min_version));
        *(size_t *)legacy_prefix =
            offsetof(turbo_flow_operation_descriptor_t, resource_min_version);
        check_equal(turbo_flow_register_module_contract(
                        flow, &module,
                        (const turbo_flow_operation_descriptor_t *)legacy_prefix, 1u),
                    SALTS_EINVAL);
        check_equal(turbo_flow_operation_count(flow), existing);
        check_equal(turbo_flow_module_count(flow), existing);
        free(legacy_prefix);

        operation.size = sizeof(operation) + 1u;
        check_equal(turbo_flow_register_module_contract(flow, &module, &operation, 1u),
                    SALTS_EINVAL);
        check_equal(turbo_flow_operation_count(flow), existing);
        check_equal(turbo_flow_module_count(flow), existing);
        turbo_flow_destroy(flow);
      }
    }

    it("binds typed providers to the module that exports their resource type") {
      static const char *const primitive_types[] = {"Queue"};
      static const char *const operation_names[] = {"queue.enqueue"};
      turbo_flow_t *flow = turbo_flow_create();
      turbo_flow_primitive_descriptor_t primitive =
          resource_descriptor("queue.jobs", "Queue", TURBO_FLOW_DOMAIN_BUFFER_PERSISTENCE);
      turbo_flow_operation_descriptor_t operation = operation_descriptor(
          "queue.enqueue", TURBO_FLOW_DOMAIN_BUFFER_PERSISTENCE, TURBO_FLOW_DOMAIN_DATA,
          "Message", TURBO_FLOW_DOMAIN_DATA, "Message",
          TURBO_FLOW_OPERATION_STAGE | TURBO_FLOW_OPERATION_BRIDGE);
      turbo_flow_module_descriptor_t module = {0};
      turbo_flow_operation_provider_registration_t provider =
          TURBO_FLOW_OPERATION_PROVIDER_REGISTRATION_INIT;

      require_resource(&operation, TURBO_FLOW_DOMAIN_BUFFER_PERSISTENCE, "Queue");
      module.size = sizeof(module);
      module.name = "queue.core";
      module.version = 1u;
      module.capability_flags = TURBO_FLOW_MODULE_GRAPH_OPERATIONS |
                                TURBO_FLOW_MODULE_MANAGED_RESOURCES |
                                TURBO_FLOW_MODULE_NATIVE_API;
      module.primitive_types = primitive_types;
      module.primitive_type_count = 1u;
      module.operation_names = operation_names;
      module.operation_count = 1u;
      provider.operation_name = "queue.enqueue";
      provider.resource_name = "queue.jobs";
      provider.fn = domain_noop_stage;

      check_not_null(flow);
      check_equal(turbo_flow_register_primitive(flow, &primitive), SALTS_OK);
      check_equal(turbo_flow_register_operation(flow, &operation), SALTS_OK);
      check_equal(turbo_flow_register_module(flow, &module), SALTS_OK);
      check_equal(turbo_flow_register_operation_provider(flow, &provider), SALTS_OK);
      check_null(turbo_flow_operation_provider_module(flow, "queue.enqueue", "queue.jobs"));
      check_equal(turbo_flow_bind_operation_provider_module(
                       flow, "queue.core", "queue.enqueue", "queue.jobs"),
                   SALTS_OK);
      check_equal(turbo_flow_operation_provider_module(flow, "queue.enqueue", "queue.jobs"),
                   "queue.core");
      turbo_flow_destroy(flow);
    }

    it("atomically binds native adapter operations to their module owner") {
      static const char *const operation_names[] = {"native.transform"};
      turbo_flow_t *flow = turbo_flow_create();
      turbo_flow_operation_descriptor_t operation = operation_descriptor(
          "native.transform", TURBO_FLOW_DOMAIN_PROTOCOL_PATTERN, TURBO_FLOW_DOMAIN_DATA,
          "Message", TURBO_FLOW_DOMAIN_DATA, "Message",
          TURBO_FLOW_OPERATION_STAGE | TURBO_FLOW_OPERATION_BRIDGE);
      turbo_flow_module_descriptor_t module = {0};
      turbo_flow_adapter_ops_t ops = {0};
      turbo_flow_adapter_schema_t schema = {0};
      turbo_flow_module_adapter_registration_t adapter =
          TURBO_FLOW_MODULE_ADAPTER_REGISTRATION_INIT;
      int shutdown_count = 0;

      operation.scope.state = TURBO_FLOW_STATE_SCOPE_ADAPTER_OWNER;
      operation.scope.concurrency = TURBO_FLOW_CONCURRENCY_OWNER_CONTEXT;
      operation.scope.authority = TURBO_FLOW_AUTHORITY_OWNER_LOCAL;
      module.size = sizeof(module);
      module.name = "native.protocol";
      module.version = 1u;
      module.capability_flags = TURBO_FLOW_MODULE_GRAPH_OPERATIONS |
                                TURBO_FLOW_MODULE_NATIVE_API;
      module.operation_names = operation_names;
      module.operation_count = 1u;
      ops.consume = domain_noop_consume;
      ops.shutdown = domain_count_shutdown;
      schema.kind = TURBO_FLOW_ADAPTER_KIND_CUSTOM;
      schema.roles = TURBO_FLOW_ADAPTER_TRANSFORM;
      schema.direction = TURBO_FLOW_ADAPTER_BIDIRECTIONAL;
      adapter.module_name = "native.protocol";
      adapter.adapter_name = "native.instance";
      adapter.ops = &ops;
      adapter.ctx = &shutdown_count;
      adapter.schema = &schema;
      adapter.operation_names = operation_names;
      adapter.operation_count = 1u;

      check_not_null(flow);
      check_equal(turbo_flow_register_operation(flow, &operation), SALTS_OK);
      check_equal(turbo_flow_register_module(flow, &module), SALTS_OK);
      schema.roles = TURBO_FLOW_ADAPTER_SOURCE;
      check_equal(turbo_flow_register_module_adapter(flow, &adapter), SALTS_EPROTO);
      check_equal(turbo_flow_adapter_count(flow), 0u);
      check_equal(shutdown_count, 0);
      schema.roles = TURBO_FLOW_ADAPTER_TRANSFORM;
      check_equal(turbo_flow_register_module_adapter(flow, &adapter), SALTS_OK);
      check_equal(turbo_flow_adapter_operation_module(
                       flow, "native.instance", "native.transform"),
                   "native.protocol");
      check_equal(turbo_flow_reset(flow, 1), SALTS_OK);
      check_equal(turbo_flow_adapter_operation_module(
                       flow, "native.instance", "native.transform"),
                   "native.protocol");
      check_equal(shutdown_count, 0);
      check_equal(turbo_flow_reset(flow, 0), SALTS_OK);
      check_null(turbo_flow_adapter_operation_module(
          flow, "native.instance", "native.transform"));
      check_equal(shutdown_count, 1);
      turbo_flow_destroy(flow);
    }

    it("rejects every non-exact module adapter layout without registry or ownership changes") {
      static const char *const operation_names[] = {"layout.consume"};
      const size_t invalid_sizes[] = {
          0u,
          sizeof(size_t),
          offsetof(turbo_flow_module_adapter_registration_t, operation_resource_names),
          offsetof(turbo_flow_module_adapter_registration_t, consume_batch),
          sizeof(turbo_flow_module_adapter_registration_t) - 1u,
          sizeof(turbo_flow_module_adapter_registration_t) + 1u,
          SIZE_MAX};

      for (size_t retained = 0u; retained < 2u; ++retained) {
        turbo_flow_t *flow = turbo_flow_create();
        turbo_flow_operation_descriptor_t operation = operation_descriptor(
            "layout.consume", TURBO_FLOW_DOMAIN_PROTOCOL_PATTERN, TURBO_FLOW_DOMAIN_DATA,
            "Message", TURBO_FLOW_DOMAIN_NONE, NULL,
            TURBO_FLOW_OPERATION_STAGE | TURBO_FLOW_OPERATION_BRIDGE);
        turbo_flow_module_descriptor_t module = {0};
        turbo_flow_adapter_ops_t ops = {0};
        turbo_flow_adapter_schema_t schema = {0};
        turbo_flow_module_adapter_registration_t adapter =
            TURBO_FLOW_MODULE_ADAPTER_REGISTRATION_INIT;
        int shutdown_count = 0;

        operation.scope.state = TURBO_FLOW_STATE_SCOPE_ADAPTER_OWNER;
        operation.scope.concurrency = TURBO_FLOW_CONCURRENCY_OWNER_CONTEXT;
        operation.scope.authority = TURBO_FLOW_AUTHORITY_OWNER_LOCAL;
        module.size = sizeof(module);
        module.name = "layout.module";
        module.version = 1u;
        module.capability_flags =
            TURBO_FLOW_MODULE_GRAPH_OPERATIONS | TURBO_FLOW_MODULE_NATIVE_API;
        module.operation_names = operation_names;
        module.operation_count = 1u;
        ops.consume = domain_noop_consume;
        ops.shutdown = domain_count_shutdown;
        schema.kind = TURBO_FLOW_ADAPTER_KIND_CUSTOM;
        schema.roles = TURBO_FLOW_ADAPTER_SINK;
        schema.direction = TURBO_FLOW_ADAPTER_OUTPUT;
        adapter.module_name = module.name;
        adapter.adapter_name = "layout.instance";
        adapter.ops = &ops;
        adapter.ctx = &shutdown_count;
        adapter.schema = &schema;
        adapter.operation_names = operation_names;
        adapter.operation_count = 1u;

        check_not_null(flow);
        check_equal(turbo_flow_register_module_contract(flow, &module, &operation, 1u), SALTS_OK);
        if (retained != 0u)
          check_equal(turbo_flow_register_adapter(flow, "retained.adapter", NULL, NULL), SALTS_OK);
        for (size_t index = 0u; index < sizeof(invalid_sizes) / sizeof(invalid_sizes[0]); ++index) {
          turbo_flow_module_adapter_registration_t invalid = adapter;
          turbo_flow_module_adapter_registration_t before;
          size_t adapter_count = turbo_flow_adapter_count(flow);
          size_t operation_count = turbo_flow_operation_count(flow);
          size_t module_count = turbo_flow_module_count(flow);
          size_t primitive_count = turbo_flow_primitive_count(flow);
          size_t resource_count = turbo_flow_resource_count(flow);
          invalid.size = invalid_sizes[index];
          before = invalid;
          check_equal(turbo_flow_register_module_adapter(flow, &invalid), SALTS_EINVAL);
          check_equal(memcmp(&invalid, &before, sizeof(invalid)), 0);
          check_equal(turbo_flow_adapter_count(flow), adapter_count);
          check_equal(turbo_flow_operation_count(flow), operation_count);
          check_equal(turbo_flow_module_count(flow), module_count);
          check_equal(turbo_flow_primitive_count(flow), primitive_count);
          check_equal(turbo_flow_resource_count(flow), resource_count);
          check_null(turbo_flow_adapter_operation_module(flow, adapter.adapter_name,
                                                          operation_names[0]));
          check_equal(shutdown_count, 0);
        }
        turbo_flow_destroy(flow);
        check_equal(shutdown_count, 0);
      }
    }

    it("rejects physically short prefixes and permits an exact retry with one shutdown") {
      static const char *const operation_names[] = {"short.consume"};
      const size_t allocations[] = {
          sizeof(size_t),
          offsetof(turbo_flow_module_adapter_registration_t, operation_resource_names),
          offsetof(turbo_flow_module_adapter_registration_t, consume_batch)};

      for (size_t index = 0u; index < sizeof(allocations) / sizeof(allocations[0]); ++index) {
        turbo_flow_t *flow = turbo_flow_create();
        turbo_flow_operation_descriptor_t operation = operation_descriptor(
            "short.consume", TURBO_FLOW_DOMAIN_PROTOCOL_PATTERN, TURBO_FLOW_DOMAIN_DATA,
            "Message", TURBO_FLOW_DOMAIN_NONE, NULL,
            TURBO_FLOW_OPERATION_STAGE | TURBO_FLOW_OPERATION_BRIDGE);
        turbo_flow_module_descriptor_t module = {0};
        turbo_flow_adapter_ops_t ops = {0};
        turbo_flow_adapter_schema_t schema = {0};
        turbo_flow_module_adapter_registration_t adapter =
            TURBO_FLOW_MODULE_ADAPTER_REGISTRATION_INIT;
        unsigned char *invalid = (unsigned char *)malloc(allocations[index]);
        unsigned char *before = (unsigned char *)malloc(allocations[index]);
        int rejected_shutdowns = 0;
        int registered_shutdowns = 0;

        check_not_null(flow);
        check_not_null(invalid);
        check_not_null(before);
        operation.scope.state = TURBO_FLOW_STATE_SCOPE_ADAPTER_OWNER;
        operation.scope.concurrency = TURBO_FLOW_CONCURRENCY_OWNER_CONTEXT;
        operation.scope.authority = TURBO_FLOW_AUTHORITY_OWNER_LOCAL;
        module.size = sizeof(module);
        module.name = "short.module";
        module.version = 1u;
        module.capability_flags =
            TURBO_FLOW_MODULE_GRAPH_OPERATIONS | TURBO_FLOW_MODULE_NATIVE_API;
        module.operation_names = operation_names;
        module.operation_count = 1u;
        ops.consume = domain_noop_consume;
        ops.shutdown = domain_count_shutdown;
        schema.kind = TURBO_FLOW_ADAPTER_KIND_CUSTOM;
        schema.roles = TURBO_FLOW_ADAPTER_SINK;
        schema.direction = TURBO_FLOW_ADAPTER_OUTPUT;
        adapter.module_name = module.name;
        adapter.adapter_name = "short.instance";
        adapter.ops = &ops;
        adapter.ctx = &rejected_shutdowns;
        adapter.schema = &schema;
        adapter.operation_names = operation_names;
        adapter.operation_count = 1u;
        memcpy(invalid, &adapter, allocations[index]);
        *(size_t *)invalid = allocations[index];
        memcpy(before, invalid, allocations[index]);

        check_equal(turbo_flow_register_module_contract(flow, &module, &operation, 1u), SALTS_OK);
        check_equal(turbo_flow_register_module_adapter(
                        flow, (const turbo_flow_module_adapter_registration_t *)invalid),
                    SALTS_EINVAL);
        check_equal(memcmp(invalid, before, allocations[index]), 0);
        check_equal(turbo_flow_adapter_count(flow), 0u);
        check_equal(rejected_shutdowns, 0);
        free(before);
        free(invalid);

        adapter.ctx = &registered_shutdowns;
        check_equal(turbo_flow_register_module_adapter(flow, &adapter), SALTS_OK);
        check_equal(turbo_flow_adapter_count(flow), 1u);
        check_equal(registered_shutdowns, 0);
        turbo_flow_destroy(flow);
        check_equal(rejected_shutdowns, 0);
        check_equal(registered_shutdowns, 1);
      }
    }
  }

  group("Native adapter batches") {
    it("uses one native callback, preserves first-error counts, and rejects incomplete success") {
      domain_batch_probe_t probe = {0};
      domain_batch_prepare_probe_t prepare_probe = {0u, SIZE_MAX, SALTS_EIO};
      turbo_flow_publish_batch_config_t config = TURBO_FLOW_PUBLISH_BATCH_CONFIG_INIT;
      turbo_flow_t *flow = turbo_flow_create();
      size_t published = SIZE_MAX;

      check_not_null(flow);
      probe.fail_status = SALTS_EIO;
      config.message_count = 4u;
      config.prepare = domain_batch_prepare;
      config.ctx = &prepare_probe;
      check_equal(register_domain_batch_graph(flow, &probe, sizeof(
                                                        turbo_flow_module_adapter_registration_t), 1),
                   SALTS_OK);
      check_equal(turbo_flow_start(flow), SALTS_OK);

      check_equal(turbo_flow_publish_batch(flow, "input", &config, &published), SALTS_OK);
      check_equal(published, 4u);
      check_equal(probe.batch_calls, 1u);
      check_equal(probe.scalar_calls, 0u);
      check_equal(probe.next_calls, 4u);
      check_equal(probe.observed_count, 4u);
      for (size_t index = 0u; index < 4u; ++index) {
        check_equal(probe.observed_ids[index], index + 1u);
      }

      probe.observed_count = 0u;
      probe.fail_id = 3u;
      check_equal(turbo_flow_publish_batch(flow, "input", &config, &published), SALTS_EIO);
      check_equal(published, 2u);
      check_equal(probe.batch_calls, 2u);
      check_equal(probe.scalar_calls, 0u);
      check_equal(probe.next_calls, 7u);
      check_equal(probe.observed_count, 3u);

      probe.observed_count = 0u;
      probe.fail_id = 0u;
      prepare_probe.fail_index = 2u;
      check_equal(turbo_flow_publish_batch(flow, "input", &config, &published), SALTS_EIO);
      check_equal(published, 2u);
      check_equal(probe.batch_calls, 3u);
      check_equal(probe.scalar_calls, 0u);
      check_equal(probe.next_calls, 10u);
      check_equal(probe.observed_count, 2u);

      probe.mode = DOMAIN_BATCH_PROBE_INCOMPLETE_SUCCESS;
      prepare_probe.fail_index = SIZE_MAX;
      check_equal(turbo_flow_publish_batch(flow, "input", &config, &published), SALTS_EPROTO);
      check_equal(published, 2u);
      check_equal(turbo_flow_last_error(flow)->code, SALTS_EPROTO);

      probe.mode = DOMAIN_BATCH_PROBE_OUT_OF_ORDER;
      check_equal(turbo_flow_publish_batch(flow, "input", &config, &published), SALTS_EPROTO);
      check_equal(published, 0u);
      check_equal(turbo_flow_last_error(flow)->code, SALTS_EPROTO);

      check_equal(turbo_flow_stop(flow), SALTS_OK);
      turbo_flow_destroy(flow);
    }

    it("falls back to scalar delivery when an observer is installed") {
      {
        domain_batch_probe_t probe = {0};
        domain_batch_prepare_probe_t prepare_probe = {0u, SIZE_MAX, SALTS_EIO};
        turbo_flow_publish_batch_config_t config = TURBO_FLOW_PUBLISH_BATCH_CONFIG_INIT;
        turbo_flow_observer_ops_t observer = {0};
        turbo_flow_t *flow = turbo_flow_create();
        size_t observer_calls = 0u;
        size_t published = SIZE_MAX;

        check_not_null(flow);
        probe.fail_status = SALTS_EIO;
        config.message_count = 4u;
        config.prepare = domain_batch_prepare;
        config.ctx = &prepare_probe;
        check_equal(register_domain_batch_graph(
                        flow, &probe, sizeof(turbo_flow_module_adapter_registration_t), 1),
                    SALTS_OK);
        observer.size = sizeof(observer);
        observer.message_complete = domain_batch_message_observed;
        check_equal(turbo_flow_set_observer(flow, &observer, &observer_calls), SALTS_OK);
        check_equal(turbo_flow_start(flow), SALTS_OK);
        check_equal(turbo_flow_publish_batch(flow, "input", &config, &published), SALTS_OK);
        check_equal(published, 4u);
        check_equal(probe.batch_calls, 0u);
        check_equal(probe.scalar_calls, 4u);
        check_equal(probe.observed_count, 4u);
        check_equal(observer_calls, 4u);
        check_equal(turbo_flow_stop(flow), SALTS_OK);
        turbo_flow_destroy(flow);
      }
    }

    it("keeps scalar delivery for a complete registration without a batch callback") {
      domain_batch_probe_t probe = {0};
      domain_batch_prepare_probe_t prepare_probe = {0u, SIZE_MAX, SALTS_EIO};
      turbo_flow_publish_batch_config_t config = TURBO_FLOW_PUBLISH_BATCH_CONFIG_INIT;
      turbo_flow_t *flow = turbo_flow_create();
      size_t published = SIZE_MAX;

      check_not_null(flow);
      probe.fail_status = SALTS_EIO;
      config.message_count = 4u;
      config.prepare = domain_batch_prepare;
      config.ctx = &prepare_probe;
      check_equal(register_domain_batch_graph(
                      flow, &probe, sizeof(turbo_flow_module_adapter_registration_t), 0),
                  SALTS_OK);
      check_equal(turbo_flow_start(flow), SALTS_OK);
      check_equal(turbo_flow_publish_batch(flow, "input", &config, &published), SALTS_OK);
      check_equal(published, 4u);
      check_equal(probe.batch_calls, 0u);
      check_equal(probe.scalar_calls, 4u);
      check_equal(turbo_flow_stop(flow), SALTS_OK);
      turbo_flow_destroy(flow);
    }
  }

  group("DSL and compiler") {
    it("rejects an unbound provider for a cataloged operation") {
      static const char *dsl = "source input\n"
                               "stage work operation data.validate\n"
                               "stage main {\n"
                               "  input -> work\n"
                               "}\n";
      static const char *const operations[] = {"data.validate"};
      turbo_flow_t *flow = turbo_flow_create();
      turbo_flow_operation_descriptor_t operation = operation_descriptor(
          "data.validate", TURBO_FLOW_DOMAIN_DATA, TURBO_FLOW_DOMAIN_DATA, "Message",
          TURBO_FLOW_DOMAIN_DATA, "Message", TURBO_FLOW_OPERATION_STAGE);
      turbo_flow_module_descriptor_t module = {0};
      turbo_flow_operation_provider_registration_t provider =
          TURBO_FLOW_OPERATION_PROVIDER_REGISTRATION_INIT;

      module.size = sizeof(module);
      module.name = "data.validation";
      module.version = 1u;
      module.capability_flags = TURBO_FLOW_MODULE_GRAPH_OPERATIONS;
      module.operation_names = operations;
      module.operation_count = 1u;
      provider.operation_name = "data.validate";
      provider.fn = domain_noop_stage;
      check_not_null(flow);
      check_equal(turbo_flow_register_operation(flow, &operation), SALTS_OK);
      check_equal(turbo_flow_register_module(flow, &module), SALTS_OK);
      check_equal(turbo_flow_register_operation_provider(flow, &provider), SALTS_OK);
      check_equal(turbo_flow_parse_string(flow, dsl, strlen(dsl)), SALTS_OK);
      check_equal(turbo_flow_compile(flow), SALTS_EPROTO);
      check_contains(turbo_flow_last_error(flow)->message, "not bound to its module owner");
      turbo_flow_destroy(flow);
    }

    it("requires a typed native adapter for adapter-owner operations") {
      static const char *dsl = "source input\n"
                               "stage work adapter native.instance operation native.transform\n"
                               "stage main {\n"
                               "  input -> work\n"
                               "}\n";
      static const char *const operation_names[] = {"native.transform"};
      turbo_flow_t *flow = turbo_flow_create();
      turbo_flow_operation_descriptor_t operation = operation_descriptor(
          "native.transform", TURBO_FLOW_DOMAIN_PROTOCOL_PATTERN, TURBO_FLOW_DOMAIN_DATA,
          "Message", TURBO_FLOW_DOMAIN_DATA, "Message",
          TURBO_FLOW_OPERATION_STAGE | TURBO_FLOW_OPERATION_BRIDGE);
      turbo_flow_module_descriptor_t module = {0};
      turbo_flow_adapter_ops_t ops = {0};
      turbo_flow_adapter_schema_t schema = {0};

      operation.scope.state = TURBO_FLOW_STATE_SCOPE_ADAPTER_OWNER;
      operation.scope.concurrency = TURBO_FLOW_CONCURRENCY_OWNER_CONTEXT;
      operation.scope.authority = TURBO_FLOW_AUTHORITY_OWNER_LOCAL;
      module.size = sizeof(module);
      module.name = "native.protocol";
      module.version = 1u;
      module.capability_flags = TURBO_FLOW_MODULE_GRAPH_OPERATIONS |
                                TURBO_FLOW_MODULE_NATIVE_API;
      module.operation_names = operation_names;
      module.operation_count = 1u;
      ops.start = domain_noop_adapter_start;
      ops.consume = domain_noop_consume;
      schema.kind = TURBO_FLOW_ADAPTER_KIND_CUSTOM;
      schema.roles = TURBO_FLOW_ADAPTER_TRANSFORM;
      schema.direction = TURBO_FLOW_ADAPTER_BIDIRECTIONAL;

      check_not_null(flow);
      check_equal(turbo_flow_register_operation(flow, &operation), SALTS_OK);
      check_equal(turbo_flow_register_module(flow, &module), SALTS_OK);
      check_equal(turbo_flow_register_adapter_ex(
                       flow, "native.instance", &ops, NULL, &schema),
                   SALTS_OK);
      check_equal(turbo_flow_parse_string(flow, dsl, strlen(dsl)), SALTS_OK);
      check_equal(turbo_flow_compile(flow), SALTS_EPROTO);
      check_contains(turbo_flow_last_error(flow)->message,
                         "not bound to its module owner");
      turbo_flow_destroy(flow);
    }

    it("rejects a typed adapter operation bound to another resource instance") {
      static const char *dsl =
          "source input\n"
          "stage work adapter native.instance operation queue.enqueue resource queue.other\n"
          "stage main {\n"
          "  input -> work\n"
          "}\n";
      static const char *const primitive_types[] = {"Queue"};
      static const char *const operation_names[] = {"queue.enqueue"};
      static const char *const resource_names[] = {"queue.bound"};
      turbo_flow_t *flow = turbo_flow_create();
      turbo_flow_operation_descriptor_t operation = operation_descriptor(
          "queue.enqueue", TURBO_FLOW_DOMAIN_BUFFER_PERSISTENCE, TURBO_FLOW_DOMAIN_DATA,
          "Message", TURBO_FLOW_DOMAIN_NONE, NULL,
          TURBO_FLOW_OPERATION_STAGE | TURBO_FLOW_OPERATION_BRIDGE);
      turbo_flow_module_descriptor_t module = {0};
      turbo_flow_primitive_descriptor_t primitives[2];
      turbo_flow_adapter_ops_t ops = {0};
      turbo_flow_adapter_schema_t schema = {0};
      turbo_flow_module_adapter_registration_t adapter =
          TURBO_FLOW_MODULE_ADAPTER_REGISTRATION_INIT;

      require_resource(&operation, TURBO_FLOW_DOMAIN_BUFFER_PERSISTENCE, "Queue");
      operation.resource_min_version = 1u;
      operation.resource_max_version = 1u;
      module.size = sizeof(module);
      module.name = "queue.native";
      module.version = 1u;
      module.capability_flags = TURBO_FLOW_MODULE_GRAPH_OPERATIONS |
                                TURBO_FLOW_MODULE_MANAGED_RESOURCES;
      module.primitive_types = primitive_types;
      module.primitive_type_count = 1u;
      module.operation_names = operation_names;
      module.operation_count = 1u;
      primitives[0] =
          resource_descriptor("queue.bound", "Queue", TURBO_FLOW_DOMAIN_BUFFER_PERSISTENCE);
      primitives[1] =
          resource_descriptor("queue.other", "Queue", TURBO_FLOW_DOMAIN_BUFFER_PERSISTENCE);
      ops.consume = domain_noop_consume;
      schema.kind = TURBO_FLOW_ADAPTER_KIND_QUEUE;
      schema.roles = TURBO_FLOW_ADAPTER_SINK;
      schema.direction = TURBO_FLOW_ADAPTER_OUTPUT;
      adapter.module_name = module.name;
      adapter.adapter_name = "native.instance";
      adapter.ops = &ops;
      adapter.schema = &schema;
      adapter.operation_names = operation_names;
      adapter.operation_count = 1u;
      adapter.operation_resource_names = resource_names;
      adapter.primitives = &primitives[0];
      adapter.primitive_count = 1u;

      check_not_null(flow);
      check_equal(turbo_flow_register_module_contract(flow, &module, &operation, 1u), SALTS_OK);
      check_equal(turbo_flow_register_module_adapter(flow, &adapter), SALTS_OK);
      check_equal(turbo_flow_register_primitive(flow, &primitives[1]), SALTS_OK);
      check_equal(turbo_flow_parse_string(flow, dsl, strlen(dsl)), SALTS_OK);
      check_equal(turbo_flow_compile(flow), SALTS_EPROTO);
      check_contains(turbo_flow_last_error(flow)->message, "another resource primitive");
      turbo_flow_destroy(flow);
    }

    it("binds registered operations and resources to graph nodes") {
      static const char *dsl =
          "source ingress adapter mqtt.server operation mqtt.publish_in resource mqtt.session\n"
          "stage validate operation data.validate\n"
          "stage main {\n"
          "  ingress -> validate\n"
          "}\n";
      turbo_flow_t *flow = turbo_flow_create();
      turbo_flow_primitive_descriptor_t session =
          resource_descriptor("mqtt.session", "MqttSession", TURBO_FLOW_DOMAIN_PROTOCOL_PATTERN);
      turbo_flow_operation_descriptor_t input =
          operation_descriptor("mqtt.publish_in", TURBO_FLOW_DOMAIN_PROTOCOL_PATTERN,
                               TURBO_FLOW_DOMAIN_NONE, NULL, TURBO_FLOW_DOMAIN_DATA, "Message",
                               TURBO_FLOW_OPERATION_SOURCE | TURBO_FLOW_OPERATION_BRIDGE);
      turbo_flow_operation_descriptor_t validate = operation_descriptor(
          "data.validate", TURBO_FLOW_DOMAIN_DATA, TURBO_FLOW_DOMAIN_DATA, "Message",
          TURBO_FLOW_DOMAIN_DATA, "Message", TURBO_FLOW_OPERATION_STAGE);
      const turbo_flow_stage_plan_t *source_plan;

      require_resource(&input, TURBO_FLOW_DOMAIN_PROTOCOL_PATTERN, "MqttSession");
      input.scope.concurrency = TURBO_FLOW_CONCURRENCY_OWNER_CONTEXT;
      input.execution_mask = TURBO_FLOW_OPERATION_EXEC_INLINE;
      check_not_null(flow);
      check_equal(turbo_flow_register_adapter(flow, "mqtt.server", NULL, NULL), SALTS_OK);
      check_equal(turbo_flow_register_primitive(flow, &session), SALTS_OK);
      check_equal(turbo_flow_register_operation(flow, &input), SALTS_OK);
      check_equal(turbo_flow_register_operation(flow, &validate), SALTS_OK);
      turbo_flow_operation_provider_registration_t operation_provider_1 =
          TURBO_FLOW_OPERATION_PROVIDER_REGISTRATION_INIT;
      operation_provider_1.operation_name = validate.name;
      operation_provider_1.fn = domain_noop_stage;
      operation_provider_1.ctx = NULL;
      check_equal(turbo_flow_register_operation_provider(flow, &operation_provider_1), SALTS_OK);
      check_equal(turbo_flow_parse_string(flow, dsl, strlen(dsl)), SALTS_OK);
      check_equal(turbo_flow_compile(flow), SALTS_OK);

      source_plan = turbo_flow_stage_at(flow, (size_t)turbo_flow_find_stage(flow, "ingress"));
      check_not_null(source_plan);
      check_equal(source_plan->operation_name, "mqtt.publish_in");
      check_equal(source_plan->resource_name, "mqtt.session");
      turbo_flow_destroy(flow);
    }

    it("rejects a resource primitive with the wrong domain or type") {
      static const char *dsl = "source ingress operation mqtt.publish_in resource queue.jobs\n"
                               "stage validate operation data.validate\n"
                               "stage main {\n"
                               "  ingress -> validate\n"
                               "}\n";
      turbo_flow_t *flow = turbo_flow_create();
      turbo_flow_primitive_descriptor_t queue =
          resource_descriptor("queue.jobs", "Queue", TURBO_FLOW_DOMAIN_BUFFER_PERSISTENCE);
      turbo_flow_operation_descriptor_t input =
          operation_descriptor("mqtt.publish_in", TURBO_FLOW_DOMAIN_PROTOCOL_PATTERN,
                               TURBO_FLOW_DOMAIN_NONE, NULL, TURBO_FLOW_DOMAIN_DATA, "Message",
                               TURBO_FLOW_OPERATION_SOURCE | TURBO_FLOW_OPERATION_BRIDGE);
      turbo_flow_operation_descriptor_t validate = operation_descriptor(
          "data.validate", TURBO_FLOW_DOMAIN_DATA, TURBO_FLOW_DOMAIN_DATA, "Message",
          TURBO_FLOW_DOMAIN_DATA, "Message", TURBO_FLOW_OPERATION_STAGE);

      require_resource(&input, TURBO_FLOW_DOMAIN_PROTOCOL_PATTERN, "MqttSession");
      check_not_null(flow);
      check_equal(turbo_flow_register_primitive(flow, &queue), SALTS_OK);
      check_equal(turbo_flow_register_operation(flow, &input), SALTS_OK);
      check_equal(turbo_flow_register_operation(flow, &validate), SALTS_OK);
      turbo_flow_operation_provider_registration_t operation_provider_2 =
          TURBO_FLOW_OPERATION_PROVIDER_REGISTRATION_INIT;
      operation_provider_2.operation_name = validate.name;
      operation_provider_2.fn = domain_noop_stage;
      operation_provider_2.ctx = NULL;
      check_equal(turbo_flow_register_operation_provider(flow, &operation_provider_2), SALTS_OK);
      check_equal(turbo_flow_parse_string(flow, dsl, strlen(dsl)), SALTS_OK);
      check_equal(turbo_flow_compile(flow), SALTS_EINVAL);
      check_contains(turbo_flow_last_error(flow)->message,
                         "resource primitive does not satisfy");
      turbo_flow_destroy(flow);
    }

    it("accepts and rejects resource primitives by operation version range") {
      static const char *dsl =
          "source ingress operation queue.read resource queue.jobs\n"
          "stage validate operation data.validate\n"
          "stage main {\n"
          "  ingress -> validate\n"
          "}\n";
      const uint32_t versions[] = {2u, 1u};
      const int expected[] = {SALTS_OK, SALTS_EPROTO};
      for (size_t scenario = 0; scenario < 2u; ++scenario) {
        turbo_flow_t *flow = turbo_flow_create();
        turbo_flow_primitive_descriptor_t queue =
            resource_descriptor("queue.jobs", "Queue", TURBO_FLOW_DOMAIN_BUFFER_PERSISTENCE);
        turbo_flow_operation_descriptor_t input = operation_descriptor(
            "queue.read", TURBO_FLOW_DOMAIN_BUFFER_PERSISTENCE, TURBO_FLOW_DOMAIN_NONE, NULL,
            TURBO_FLOW_DOMAIN_DATA, "Message",
            TURBO_FLOW_OPERATION_SOURCE | TURBO_FLOW_OPERATION_BRIDGE);
        turbo_flow_operation_descriptor_t validate = operation_descriptor(
            "data.validate", TURBO_FLOW_DOMAIN_DATA, TURBO_FLOW_DOMAIN_DATA, "Message",
            TURBO_FLOW_DOMAIN_DATA, "Message", TURBO_FLOW_OPERATION_STAGE);

        queue.version = versions[scenario];
        require_resource(&input, TURBO_FLOW_DOMAIN_BUFFER_PERSISTENCE, "Queue");
        input.resource_min_version = 2u;
        input.resource_max_version = 3u;
        check_not_null(flow);
        check_equal(turbo_flow_register_primitive(flow, &queue), SALTS_OK);
        check_equal(turbo_flow_register_operation(flow, &input), SALTS_OK);
        check_equal(turbo_flow_register_operation(flow, &validate), SALTS_OK);
        turbo_flow_operation_provider_registration_t operation_provider_3 =
            TURBO_FLOW_OPERATION_PROVIDER_REGISTRATION_INIT;
        operation_provider_3.operation_name = validate.name;
        operation_provider_3.fn = domain_noop_stage;
        operation_provider_3.ctx = NULL;
        check_equal(turbo_flow_register_operation_provider(flow, &operation_provider_3), SALTS_OK);
        check_equal(turbo_flow_parse_string(flow, dsl, strlen(dsl)), SALTS_OK);
        check_equal(turbo_flow_compile(flow), expected[scenario]);
        if (scenario == 1u) {
          check_contains(turbo_flow_last_error(flow)->message,
                             "version is incompatible");
        }
        turbo_flow_destroy(flow);
      }
    }

    it("requires an adapter owner for an owner-context source") {
      static const char *dsl = "source ingress operation mqtt.publish_in resource mqtt.session\n"
                               "stage validate operation data.validate\n"
                               "stage main {\n"
                               "  ingress -> validate\n"
                               "}\n";
      turbo_flow_t *flow = turbo_flow_create();
      turbo_flow_primitive_descriptor_t session =
          resource_descriptor("mqtt.session", "MqttSession", TURBO_FLOW_DOMAIN_PROTOCOL_PATTERN);
      turbo_flow_operation_descriptor_t input =
          operation_descriptor("mqtt.publish_in", TURBO_FLOW_DOMAIN_PROTOCOL_PATTERN,
                               TURBO_FLOW_DOMAIN_NONE, NULL, TURBO_FLOW_DOMAIN_DATA, "Message",
                               TURBO_FLOW_OPERATION_SOURCE | TURBO_FLOW_OPERATION_BRIDGE);
      turbo_flow_operation_descriptor_t validate = operation_descriptor(
          "data.validate", TURBO_FLOW_DOMAIN_DATA, TURBO_FLOW_DOMAIN_DATA, "Message",
          TURBO_FLOW_DOMAIN_DATA, "Message", TURBO_FLOW_OPERATION_STAGE);

      require_resource(&input, TURBO_FLOW_DOMAIN_PROTOCOL_PATTERN, "MqttSession");
      input.scope.concurrency = TURBO_FLOW_CONCURRENCY_OWNER_CONTEXT;
      input.execution_mask = TURBO_FLOW_OPERATION_EXEC_INLINE;
      check_not_null(flow);
      check_equal(turbo_flow_register_primitive(flow, &session), SALTS_OK);
      check_equal(turbo_flow_register_operation(flow, &input), SALTS_OK);
      check_equal(turbo_flow_register_operation(flow, &validate), SALTS_OK);
      turbo_flow_operation_provider_registration_t operation_provider_4 =
          TURBO_FLOW_OPERATION_PROVIDER_REGISTRATION_INIT;
      operation_provider_4.operation_name = validate.name;
      operation_provider_4.fn = domain_noop_stage;
      operation_provider_4.ctx = NULL;
      check_equal(turbo_flow_register_operation_provider(flow, &operation_provider_4), SALTS_OK);
      check_equal(turbo_flow_parse_string(flow, dsl, strlen(dsl)), SALTS_OK);
      check_equal(turbo_flow_compile(flow), SALTS_EINVAL);
      check_contains(turbo_flow_last_error(flow)->message, "requires an adapter owner");
      turbo_flow_destroy(flow);
    }

    it("rejects incompatible operation edge types") {
      static const char *dsl = "source ingress operation data.input\n"
                               "stage validate operation data.validate\n"
                               "stage main {\n"
                               "  ingress -> validate\n"
                               "}\n";
      turbo_flow_t *flow = turbo_flow_create();
      turbo_flow_operation_descriptor_t input =
          operation_descriptor("data.input", TURBO_FLOW_DOMAIN_DATA, TURBO_FLOW_DOMAIN_NONE, NULL,
                               TURBO_FLOW_DOMAIN_DATA, "Order", TURBO_FLOW_OPERATION_SOURCE);
      turbo_flow_operation_descriptor_t validate = operation_descriptor(
          "data.validate", TURBO_FLOW_DOMAIN_DATA, TURBO_FLOW_DOMAIN_DATA, "Event",
          TURBO_FLOW_DOMAIN_DATA, "Event", TURBO_FLOW_OPERATION_STAGE);

      check_not_null(flow);
      check_equal(turbo_flow_register_operation(flow, &input), SALTS_OK);
      check_equal(turbo_flow_register_operation(flow, &validate), SALTS_OK);
      turbo_flow_operation_provider_registration_t operation_provider_5 =
          TURBO_FLOW_OPERATION_PROVIDER_REGISTRATION_INIT;
      operation_provider_5.operation_name = validate.name;
      operation_provider_5.fn = domain_noop_stage;
      operation_provider_5.ctx = NULL;
      check_equal(turbo_flow_register_operation_provider(flow, &operation_provider_5), SALTS_OK);
      check_equal(turbo_flow_parse_string(flow, dsl, strlen(dsl)), SALTS_OK);
      check_equal(turbo_flow_compile(flow), SALTS_EINVAL);
      check_contains(turbo_flow_last_error(flow)->message, "domain or type is incompatible");
      turbo_flow_destroy(flow);
    }

    it("rejects management authority and disallowed worker execution") {
      static const char *management_dsl = "source input\n"
                                          "stage command operation management.resize\n"
                                          "stage main {\n"
                                          "  input -> command\n"
                                          "}\n";
      static const char *worker_dsl = "source input\n"
                                      "stage work operation data.validate worker 2\n"
                                      "stage main {\n"
                                      "  input -> work\n"
                                      "}\n";
      turbo_flow_t *management_flow = turbo_flow_create();
      turbo_flow_t *worker_flow = turbo_flow_create();
      turbo_flow_operation_descriptor_t command =
          operation_descriptor("management.resize", TURBO_FLOW_DOMAIN_MANAGEMENT,
                               TURBO_FLOW_DOMAIN_DATA, "Message", TURBO_FLOW_DOMAIN_DATA, "Message",
                               TURBO_FLOW_OPERATION_STAGE | TURBO_FLOW_OPERATION_BRIDGE);
      turbo_flow_operation_descriptor_t validate = operation_descriptor(
          "data.validate", TURBO_FLOW_DOMAIN_DATA, TURBO_FLOW_DOMAIN_DATA, "Message",
          TURBO_FLOW_DOMAIN_DATA, "Message", TURBO_FLOW_OPERATION_STAGE);

      command.scope.authority = TURBO_FLOW_AUTHORITY_OWNER_COMMAND;
      check_not_null(management_flow);
      check_not_null(worker_flow);
      check_equal(turbo_flow_register_operation(management_flow, &command), SALTS_OK);
      turbo_flow_operation_provider_registration_t operation_provider_6 =
          TURBO_FLOW_OPERATION_PROVIDER_REGISTRATION_INIT;
      operation_provider_6.operation_name = command.name;
      operation_provider_6.fn = domain_noop_stage;
      operation_provider_6.ctx = NULL;
      check_equal(turbo_flow_register_operation_provider(management_flow, &operation_provider_6),
                  SALTS_OK);
      check_equal(turbo_flow_parse_string(management_flow, management_dsl, strlen(management_dsl)),
                   SALTS_OK);
      check_equal(turbo_flow_compile(management_flow), SALTS_EINVAL);
      check_contains(turbo_flow_last_error(management_flow)->message, "management command");

      check_equal(turbo_flow_register_operation(worker_flow, &validate), SALTS_OK);
      turbo_flow_operation_provider_registration_t operation_provider_7 =
          TURBO_FLOW_OPERATION_PROVIDER_REGISTRATION_INIT;
      operation_provider_7.operation_name = validate.name;
      operation_provider_7.fn = domain_noop_stage;
      operation_provider_7.ctx = NULL;
      check_equal(turbo_flow_register_operation_provider(worker_flow, &operation_provider_7),
                  SALTS_OK);
      check_equal(turbo_flow_parse_string(worker_flow, worker_dsl, strlen(worker_dsl)), SALTS_OK);
      check_equal(turbo_flow_compile(worker_flow), SALTS_EINVAL);
      check_contains(turbo_flow_last_error(worker_flow)->message, "worker segment");
      turbo_flow_destroy(management_flow);
      turbo_flow_destroy(worker_flow);
    }

    it("requires pool concurrency scope to use a pooled executor") {
      static const char *dsl = "source input\n"
                               "stage work operation data.parallel\n"
                               "stage main {\n"
                               "  input -> work\n"
                               "}\n";
      turbo_flow_t *flow = turbo_flow_create();
      turbo_flow_operation_descriptor_t operation = operation_descriptor(
          "data.parallel", TURBO_FLOW_DOMAIN_DATA, TURBO_FLOW_DOMAIN_DATA, "Message",
          TURBO_FLOW_DOMAIN_DATA, "Message", TURBO_FLOW_OPERATION_STAGE);

      operation.scope.concurrency = TURBO_FLOW_CONCURRENCY_POOL;
      operation.execution_mask |= TURBO_FLOW_OPERATION_EXEC_THREAD |
                                  TURBO_FLOW_OPERATION_EXEC_CORO;
      check_not_null(flow);
      check_equal(turbo_flow_register_operation(flow, &operation), SALTS_OK);
      turbo_flow_operation_provider_registration_t operation_provider_8 =
          TURBO_FLOW_OPERATION_PROVIDER_REGISTRATION_INIT;
      operation_provider_8.operation_name = operation.name;
      operation_provider_8.fn = domain_noop_stage;
      operation_provider_8.ctx = NULL;
      check_equal(turbo_flow_register_operation_provider(flow, &operation_provider_8), SALTS_OK);
      check_equal(turbo_flow_parse_string(flow, dsl, strlen(dsl)), SALTS_OK);
      check_equal(turbo_flow_compile(flow), SALTS_EINVAL);
      check_contains(turbo_flow_last_error(flow)->message, "pool-scoped operation");
      turbo_flow_destroy(flow);
    }
  }

  group("Bounded emitting operations") {
    static const char *linear_dsl = "source input operation data.input\n"
                                    "stage expand operation data.expand\n"
                                    "stage sink operation test.sink\n"
                                    "stage main {\n"
                                    "  input -> expand -> sink\n"
                                    "}\n";

    it("filters maps and expands while preserving output order") {
      const uint32_t output_counts[] = {0u, 1u, 3u};

      for (size_t scenario = 0u; scenario < 3u; ++scenario) {
        turbo_flow_t *flow = turbo_flow_create();
        emission_probe_t probe;
        turbo_flow_msg_t message;

        memset(&probe, 0, sizeof(probe));
        probe.output_count = output_counts[scenario];
        check_not_null(flow);
        check_equal(register_emitting_graph(flow, &probe, 3u, linear_dsl), SALTS_OK);
        check_equal(turbo_flow_compile(flow), SALTS_OK);
        check_equal(turbo_flow_start(flow), SALTS_OK);
        turbo_flow_msg_init(&message);
        message.id = 7u;
        check_equal(turbo_flow_publish(flow, "input", &message), SALTS_OK);
        check_equal(probe.emitter_called, 1);
        check_equal(probe.sink_count, output_counts[scenario]);
        for (uint32_t i = 0u; i < probe.sink_count; ++i) {
          check_equal(probe.sink_ids[i], 70u + i);
        }
        turbo_flow_msg_cleanup(&message);
        check_equal(turbo_flow_stop(flow), SALTS_OK);
        turbo_flow_destroy(flow);
      }
    }

    it("does not commit collected outputs when the callback fails") {
      turbo_flow_t *flow = turbo_flow_create();
      emission_probe_t probe;
      turbo_flow_msg_t message;

      memset(&probe, 0, sizeof(probe));
      probe.output_count = 2u;
      probe.callback_status = SALTS_EIO;
      check_not_null(flow);
      check_equal(register_emitting_graph(flow, &probe, 2u, linear_dsl), SALTS_OK);
      check_equal(turbo_flow_compile(flow), SALTS_OK);
      check_equal(turbo_flow_start(flow), SALTS_OK);
      turbo_flow_msg_init(&message);
      check_equal(turbo_flow_publish(flow, "input", &message), SALTS_EIO);
      check_equal(probe.sink_count, 0u);
      turbo_flow_msg_cleanup(&message);
      check_equal(turbo_flow_stop(flow), SALTS_OK);
      turbo_flow_destroy(flow);
    }

    it("fails the whole batch when the output bound is exceeded") {
      turbo_flow_t *flow = turbo_flow_create();
      emission_probe_t probe;
      turbo_flow_msg_t message;

      memset(&probe, 0, sizeof(probe));
      probe.output_count = 3u;
      check_not_null(flow);
      check_equal(register_emitting_graph(flow, &probe, 2u, linear_dsl), SALTS_OK);
      check_equal(turbo_flow_compile(flow), SALTS_OK);
      check_equal(turbo_flow_start(flow), SALTS_OK);
      turbo_flow_msg_init(&message);
      check_equal(turbo_flow_publish(flow, "input", &message), SALTS_ENOSPC);
      check_equal(probe.sink_count, 0u);
      turbo_flow_msg_cleanup(&message);
      check_equal(turbo_flow_stop(flow), SALTS_OK);
      turbo_flow_destroy(flow);
    }

    it("rejects process-local capabilities in emitted outputs") {
      turbo_flow_t *flow = turbo_flow_create();
      emission_probe_t probe;
      turbo_flow_msg_t message;

      memset(&probe, 0, sizeof(probe));
      probe.output_count = 1u;
      probe.attach_transport_context = 1;
      check_not_null(flow);
      check_equal(register_emitting_graph(flow, &probe, 1u, linear_dsl), SALTS_OK);
      check_equal(turbo_flow_compile(flow), SALTS_OK);
      check_equal(turbo_flow_start(flow), SALTS_OK);
      turbo_flow_msg_init(&message);
      check_equal(turbo_flow_publish(flow, "input", &message), SALTS_ENOTSUP);
      check_equal(probe.sink_count, 0u);
      turbo_flow_msg_cleanup(&message);
      check_equal(turbo_flow_stop(flow), SALTS_OK);
      turbo_flow_destroy(flow);
    }

    it("preserves buffer-owned transport metadata in emitted outputs") {
      struct owned_message_s {
        uint8_t payload[3];
        int transport_marker;
      } owned = {{1u, 2u, 3u}, 17};
      turbo_flow_t *flow = turbo_flow_create();
      emission_probe_t probe;
      turbo_flow_msg_t message;

      memset(&probe, 0, sizeof(probe));
      probe.output_count = 1u;
      probe.attach_transport_context = 2;
      check_not_null(flow);
      check_equal(register_emitting_graph(flow, &probe, 1u, linear_dsl), SALTS_OK);
      check_equal(turbo_flow_compile(flow), SALTS_OK);
      check_equal(turbo_flow_start(flow), SALTS_OK);
      turbo_flow_msg_init(&message);
      message.id = 9u;
      message.buffer = mem_wrap_external(&owned, sizeof(owned), NULL, NULL);
      check_not_null(message.buffer);
      message.payload =
          vstr_from_buf((const char *)owned.payload, sizeof(owned.payload));
      message.transport_context = &owned.transport_marker;
      check_equal(turbo_flow_publish(flow, "input", &message), SALTS_OK);
      check_equal(probe.emitter_called, 1);
      check_equal(probe.sink_count, 1u);
      check_equal(probe.sink_ids[0], 90u);
      turbo_flow_msg_cleanup(&message);
      check_equal(turbo_flow_stop(flow), SALTS_OK);
      turbo_flow_destroy(flow);
    }

    it("rejects downstream fan-in from outside the emitted subtree") {
      static const char *dsl = "source input operation data.input\n"
                               "stage expand operation data.expand\n"
                               "stage other operation test.other\n"
                               "stage sink operation test.sink\n"
                               "stage main {\n"
                               "  input -> expand -> sink\n"
                               "  input -> other -> sink\n"
                               "}\n";
      turbo_flow_t *flow = turbo_flow_create();
      emission_probe_t probe;

      memset(&probe, 0, sizeof(probe));
      check_not_null(flow);
      check_equal(register_emitting_graph(flow, &probe, 1u, dsl), SALTS_OK);
      flow_test_operation_t operation_provider_9 =
          flow_test_operation_init("test.other", domain_noop_stage, NULL);
      check_equal(flow_test_operation_register(flow, &operation_provider_9), SALTS_OK);
      check_equal(turbo_flow_compile(flow), SALTS_ENOTSUP);
      check_contains(turbo_flow_last_error(flow)->message, "external branch");
      turbo_flow_destroy(flow);
    }
  }

  group("Runtime contract lowering") {
    it("resolves explicit stage contracts and built-in source contracts for every runtime node") {
      static const char *dsl =
          "source input\n"
          "stage inline_stage operation test.inline_stage\n"
          "stage thread_stage operation test.thread_stage exec thread workers 1\n"
          "stage coro_stage operation test.coro_stage exec coro lanes 1 pool 2\n"
          "stage worker_stage operation test.worker_stage worker 2 capacity 16\n"
          "stage main {\n"
          "  input -> inline_stage -> thread_stage -> coro_stage -> worker_stage\n"
          "}\n";
      const char *stage_names[] = {"input", "inline_stage", "thread_stage", "coro_stage",
                                   "worker_stage"};
      const uint32_t execution_masks[] = {
          TURBO_FLOW_OPERATION_EXEC_INLINE, TURBO_FLOW_OPERATION_EXEC_INLINE,
          TURBO_FLOW_OPERATION_EXEC_THREAD, TURBO_FLOW_OPERATION_EXEC_CORO,
          TURBO_FLOW_OPERATION_EXEC_INLINE};
      const char *operation_names[] = {"core.source", "test.inline_stage", "test.thread_stage",
                                       "test.coro_stage", "test.worker_stage"};
      turbo_flow_t *flow = turbo_flow_create();

      check_not_null(flow);
      for (size_t i = 1u; i < sizeof(stage_names) / sizeof(stage_names[0]); ++i) {
        flow_test_operation_t contract =
            flow_test_operation_init(operation_names[i], domain_noop_stage, NULL);
        contract.descriptor.execution_mask = execution_masks[i];
        if (i != 1u) contract.descriptor.scope.concurrency = TURBO_FLOW_CONCURRENCY_POOL;
        if (i == 4u) require_worker_handoff(&contract.descriptor, 16u);
        check_equal(flow_test_operation_register(flow, &contract), SALTS_OK);
      }
      check_equal(turbo_flow_parse_string(flow, dsl, strlen(dsl)), SALTS_OK);
      check_equal(turbo_flow_compile(flow), SALTS_OK);
      for (size_t i = 0u; i < sizeof(stage_names) / sizeof(stage_names[0]); ++i) {
        int stage_index = turbo_flow_find_stage(flow, stage_names[i]);
        const turbo_flow_stage_plan_t *stage;
        const turbo_flow_operation_descriptor_t *operation;
        check_greater_equal(stage_index, 0);
        stage = turbo_flow_stage_at(flow, (size_t)stage_index);
        operation = turbo_flow_stage_operation_at(flow, (size_t)stage_index);
        check_not_null(stage);
        check_not_null(operation);
        check_equal(stage->operation_name, operation_names[i]);
        check_equal(operation->name, operation_names[i]);
        check_equal(operation->domain, TURBO_FLOW_DOMAIN_DATA);
        check_equal(operation->output_type, "Message");
      }
      {
        int worker_index = turbo_flow_find_stage(flow, "worker_stage");
        const turbo_flow_operation_descriptor_t *worker =
            turbo_flow_stage_operation_at(flow, (size_t)worker_index);
        check_equal(worker->runtime.handoff, TURBO_FLOW_HANDOFF_BOUNDED);
        check_equal(worker->runtime.backpressure, TURBO_FLOW_BACKPRESSURE_BLOCK);
        check_equal(worker->runtime.capacity, 16u);
        check_equal(worker->scope.concurrency, TURBO_FLOW_CONCURRENCY_POOL);
      }
      turbo_flow_destroy(flow);
    }

    it("resolves composite routing ports as typed value operations") {
      static const char *dsl = "stage passthrough {\n"
                               "  in value\n"
                               "  out result\n"
                               "  value -> result\n"
                               "}\n"
                               "stage main {\n"
                               "  source input\n"
                               "  step sink operation test.sink\n"
                               "  use pass = passthrough\n"
                               "  input -> pass -> sink\n"
                               "}\n";
      turbo_flow_t *flow = turbo_flow_create();
      int input_port;
      int output_port;
      const turbo_flow_operation_descriptor_t *input_operation;
      const turbo_flow_operation_descriptor_t *output_operation;

      check_not_null(flow);
      flow_test_operation_t operation_provider_11 =
          flow_test_operation_init("test.sink", domain_noop_stage, NULL);
      check_equal(flow_test_operation_register(flow, &operation_provider_11), SALTS_OK);
      check_equal(turbo_flow_parse_string(flow, dsl, strlen(dsl)), SALTS_OK);
      check_equal(turbo_flow_compile(flow), SALTS_OK);
      input_port = turbo_flow_find_stage(flow, "pass.value");
      output_port = turbo_flow_find_stage(flow, "pass.result");
      check_greater_equal(input_port, 0);
      check_greater_equal(output_port, 0);
      input_operation = turbo_flow_stage_operation_at(flow, (size_t)input_port);
      output_operation = turbo_flow_stage_operation_at(flow, (size_t)output_port);
      check_not_null(input_operation);
      check_not_null(output_operation);
      check_equal(input_operation->name, "core.port.input");
      check_equal(output_operation->name, "core.port.output");
      check_equal(input_operation->input_type, "Message");
      check_equal(output_operation->output_type, "Message");
      check_equal(input_operation->scope.authority, TURBO_FLOW_AUTHORITY_PURE);
      check_equal(output_operation->scope.authority, TURBO_FLOW_AUTHORITY_PURE);
      turbo_flow_destroy(flow);
    }

    it("lowers a bounded operation to the existing worker Disruptor segment") {
      static const char *dsl = "source input operation data.input\n"
                               "stage work operation data.parallel worker 2 capacity 64\n"
                               "stage main {\n"
                               "  input -> work\n"
                               "}\n";
      turbo_flow_t *flow = turbo_flow_create();
      turbo_flow_operation_descriptor_t input =
          operation_descriptor("data.input", TURBO_FLOW_DOMAIN_DATA, TURBO_FLOW_DOMAIN_NONE, NULL,
                               TURBO_FLOW_DOMAIN_DATA, "Message", TURBO_FLOW_OPERATION_SOURCE);
      turbo_flow_operation_descriptor_t work = operation_descriptor(
          "data.parallel", TURBO_FLOW_DOMAIN_DATA, TURBO_FLOW_DOMAIN_DATA, "Message",
          TURBO_FLOW_DOMAIN_DATA, "Message", TURBO_FLOW_OPERATION_STAGE);
      turbo_flow_segment_plan_t segment;
      int found_worker = 0;

      require_worker_handoff(&work, 64u);
      check_not_null(flow);
      check_equal(turbo_flow_register_operation(flow, &input), SALTS_OK);
      check_equal(turbo_flow_register_operation(flow, &work), SALTS_OK);
      turbo_flow_operation_provider_registration_t operation_provider_12 =
          TURBO_FLOW_OPERATION_PROVIDER_REGISTRATION_INIT;
      operation_provider_12.operation_name = work.name;
      operation_provider_12.fn = domain_noop_stage;
      operation_provider_12.ctx = NULL;
      check_equal(turbo_flow_register_operation_provider(flow, &operation_provider_12), SALTS_OK);
      check_equal(turbo_flow_parse_string(flow, dsl, strlen(dsl)), SALTS_OK);
      check_equal(turbo_flow_compile(flow), SALTS_OK);

      for (size_t i = 0; i < turbo_flow_segment_count(flow); ++i) {
        check_equal(turbo_flow_segment_plan_at(flow, i, &segment), SALTS_OK);
        if (segment.kind != TURBO_FLOW_SEGMENT_WORKER_POOL) continue;
        found_worker = 1;
        check_equal(segment.width, 2u);
        check_equal(segment.capacity, 64u);
        check_equal(segment.operation.handoff, TURBO_FLOW_HANDOFF_BOUNDED);
        check_equal(segment.operation.backpressure, TURBO_FLOW_BACKPRESSURE_BLOCK);
        check_equal(segment.operation.capacity, 64u);
      }
      check_true(found_worker);
      turbo_flow_destroy(flow);
    }

    it("rejects a bounded operation whose capacity differs from the worker segment") {
      static const char *dsl = "source input operation data.input\n"
                               "stage work operation data.parallel worker 2 capacity 64\n"
                               "stage main {\n"
                               "  input -> work\n"
                               "}\n";
      turbo_flow_t *flow = turbo_flow_create();
      turbo_flow_operation_descriptor_t input =
          operation_descriptor("data.input", TURBO_FLOW_DOMAIN_DATA, TURBO_FLOW_DOMAIN_NONE, NULL,
                               TURBO_FLOW_DOMAIN_DATA, "Message", TURBO_FLOW_OPERATION_SOURCE);
      turbo_flow_operation_descriptor_t work = operation_descriptor(
          "data.parallel", TURBO_FLOW_DOMAIN_DATA, TURBO_FLOW_DOMAIN_DATA, "Message",
          TURBO_FLOW_DOMAIN_DATA, "Message", TURBO_FLOW_OPERATION_STAGE);

      require_worker_handoff(&work, 128u);
      check_not_null(flow);
      check_equal(turbo_flow_register_operation(flow, &input), SALTS_OK);
      check_equal(turbo_flow_register_operation(flow, &work), SALTS_OK);
      turbo_flow_operation_provider_registration_t operation_provider_13 =
          TURBO_FLOW_OPERATION_PROVIDER_REGISTRATION_INIT;
      operation_provider_13.operation_name = work.name;
      operation_provider_13.fn = domain_noop_stage;
      operation_provider_13.ctx = NULL;
      check_equal(turbo_flow_register_operation_provider(flow, &operation_provider_13), SALTS_OK);
      check_equal(turbo_flow_parse_string(flow, dsl, strlen(dsl)), SALTS_OK);
      check_equal(turbo_flow_compile(flow), SALTS_EINVAL);
      check_contains(turbo_flow_last_error(flow)->message, "does not match worker capacity");
      turbo_flow_destroy(flow);
    }

    it("accepts bounded fail and drop-newest worker backpressure") {
      static const char *dsl = "source input operation data.input\n"
                               "stage work operation data.parallel worker 2 capacity 64\n"
                               "stage main {\n"
                               "  input -> work\n"
                               "}\n";
      turbo_flow_t *flow = turbo_flow_create();
      turbo_flow_operation_descriptor_t input =
          operation_descriptor("data.input", TURBO_FLOW_DOMAIN_DATA, TURBO_FLOW_DOMAIN_NONE, NULL,
                               TURBO_FLOW_DOMAIN_DATA, "Message", TURBO_FLOW_OPERATION_SOURCE);
      turbo_flow_operation_descriptor_t work = operation_descriptor(
          "data.parallel", TURBO_FLOW_DOMAIN_DATA, TURBO_FLOW_DOMAIN_DATA, "Message",
          TURBO_FLOW_DOMAIN_DATA, "Message", TURBO_FLOW_OPERATION_STAGE);

      require_worker_handoff(&work, 64u);
      work.runtime.backpressure = TURBO_FLOW_BACKPRESSURE_FAIL;
      check_not_null(flow);
      check_equal(turbo_flow_register_operation(flow, &input), SALTS_OK);
      check_equal(turbo_flow_register_operation(flow, &work), SALTS_OK);
      turbo_flow_operation_provider_registration_t operation_provider_14 =
          TURBO_FLOW_OPERATION_PROVIDER_REGISTRATION_INIT;
      operation_provider_14.operation_name = work.name;
      operation_provider_14.fn = domain_noop_stage;
      operation_provider_14.ctx = NULL;
      check_equal(turbo_flow_register_operation_provider(flow, &operation_provider_14), SALTS_OK);
      check_equal(turbo_flow_parse_string(flow, dsl, strlen(dsl)), SALTS_OK);
      check_equal(turbo_flow_compile(flow), SALTS_OK);

      turbo_flow_destroy(flow);
      flow = turbo_flow_create();
      work.runtime.backpressure = TURBO_FLOW_BACKPRESSURE_DROP_NEWEST;
      check_not_null(flow);
      check_equal(turbo_flow_register_operation(flow, &input), SALTS_OK);
      check_equal(turbo_flow_register_operation(flow, &work), SALTS_OK);
      turbo_flow_operation_provider_registration_t operation_provider_15 =
          TURBO_FLOW_OPERATION_PROVIDER_REGISTRATION_INIT;
      operation_provider_15.operation_name = work.name;
      operation_provider_15.fn = domain_noop_stage;
      operation_provider_15.ctx = NULL;
      check_equal(turbo_flow_register_operation_provider(flow, &operation_provider_15), SALTS_OK);
      check_equal(turbo_flow_parse_string(flow, dsl, strlen(dsl)), SALTS_OK);
      check_equal(turbo_flow_compile(flow), SALTS_OK);
      turbo_flow_destroy(flow);
    }

    it("rejects drop-oldest when a worker ring cannot reclaim an active sequence") {
      static const char *dsl = "source input operation data.input\n"
                               "stage work operation data.parallel worker 2 capacity 64\n"
                               "stage main {\n"
                               "  input -> work\n"
                               "}\n";
      turbo_flow_t *flow = turbo_flow_create();
      turbo_flow_operation_descriptor_t input =
          operation_descriptor("data.input", TURBO_FLOW_DOMAIN_DATA, TURBO_FLOW_DOMAIN_NONE, NULL,
                               TURBO_FLOW_DOMAIN_DATA, "Message", TURBO_FLOW_OPERATION_SOURCE);
      turbo_flow_operation_descriptor_t work = operation_descriptor(
          "data.parallel", TURBO_FLOW_DOMAIN_DATA, TURBO_FLOW_DOMAIN_DATA, "Message",
          TURBO_FLOW_DOMAIN_DATA, "Message", TURBO_FLOW_OPERATION_STAGE);

      require_worker_handoff(&work, 64u);
      work.runtime.backpressure = TURBO_FLOW_BACKPRESSURE_DROP_OLDEST;
      check_not_null(flow);
      check_equal(turbo_flow_register_operation(flow, &input), SALTS_OK);
      check_equal(turbo_flow_register_operation(flow, &work), SALTS_OK);
      turbo_flow_operation_provider_registration_t operation_provider_16 =
          TURBO_FLOW_OPERATION_PROVIDER_REGISTRATION_INIT;
      operation_provider_16.operation_name = work.name;
      operation_provider_16.fn = domain_noop_stage;
      operation_provider_16.ctx = NULL;
      check_equal(turbo_flow_register_operation_provider(flow, &operation_provider_16), SALTS_OK);
      check_equal(turbo_flow_parse_string(flow, dsl, strlen(dsl)), SALTS_OK);
      check_equal(turbo_flow_compile(flow), SALTS_ENOTSUP);
      check_contains(turbo_flow_last_error(flow)->message, "cannot drop an older entry");
      turbo_flow_destroy(flow);
    }

    it("applies fail and drop-newest only at saturated worker admission") {
      static const char *dsl = "source input operation data.input\n"
                               "stage work operation data.parallel worker 1 capacity "
                               DOMAIN_ADMISSION_CAPACITY_TEXT "\n"
                               "stage main {\n"
                               "  input -> work\n"
                               "}\n";
      const turbo_flow_backpressure_kind_t policies[] = {TURBO_FLOW_BACKPRESSURE_FAIL,
                                                          TURBO_FLOW_BACKPRESSURE_DROP_NEWEST};
      const int expected[] = {SALTS_ENOSPC, SALTS_ECANCELED};

      for (size_t policy_index = 0; policy_index < 2u; ++policy_index) {
        turbo_flow_t *flow = turbo_flow_create();
        turbo_flow_operation_descriptor_t input =
            operation_descriptor("data.input", TURBO_FLOW_DOMAIN_DATA, TURBO_FLOW_DOMAIN_NONE,
                                 NULL, TURBO_FLOW_DOMAIN_DATA, "Message",
                                 TURBO_FLOW_OPERATION_SOURCE);
        turbo_flow_operation_descriptor_t work = operation_descriptor(
            "data.parallel", TURBO_FLOW_DOMAIN_DATA, TURBO_FLOW_DOMAIN_DATA, "Message",
            TURBO_FLOW_DOMAIN_DATA, "Message", TURBO_FLOW_OPERATION_STAGE);
        admission_probe_t probe;
        domain_publish_t publishes[DOMAIN_ADMISSION_PUBLISHER_COUNT];
        salts_thread_t threads[DOMAIN_ADMISSION_PUBLISHER_COUNT] = {NULL};
        turbo_flow_pool_snapshot_t snapshot;
        turbo_flow_msg_t overflow;
        int snapshot_status;
        uint64_t wait_deadline;

        require_worker_handoff(&work, DOMAIN_ADMISSION_CAPACITY);
        work.runtime.backpressure = policies[policy_index];
        atomic_init(&probe.entered, 0);
        atomic_init(&probe.release, 0);
        check_not_null(flow);
        check_equal(turbo_flow_register_operation(flow, &input), SALTS_OK);
        work.scope.state = TURBO_FLOW_STATE_SCOPE_GRAPH;
        work.scope.lifetime = TURBO_FLOW_LIFETIME_RUNTIME_GENERATION;
        work.scope.authority = TURBO_FLOW_AUTHORITY_DATA_MUTATION;
        check_equal(turbo_flow_register_operation(flow, &work), SALTS_OK);
        turbo_flow_operation_provider_registration_t operation_provider_17 =
            TURBO_FLOW_OPERATION_PROVIDER_REGISTRATION_INIT;
        operation_provider_17.operation_name = work.name;
        operation_provider_17.fn = domain_blocking_stage;
        operation_provider_17.ctx = &probe;
        check_equal(turbo_flow_register_operation_provider(flow, &operation_provider_17), SALTS_OK);
        check_equal(turbo_flow_parse_string(flow, dsl, strlen(dsl)), SALTS_OK);
        check_equal(turbo_flow_compile(flow), SALTS_OK);
        check_equal(turbo_flow_start(flow), SALTS_OK);

        for (size_t i = 0; i < DOMAIN_ADMISSION_PUBLISHER_COUNT; ++i) {
          publishes[i].flow = flow;
          atomic_init(&publishes[i].result, SALTS_EBUSY);
          check_equal(salts_thread_create(&threads[i], domain_publish_thread, &publishes[i]),
                       SALTS_OK);
          if (i == 0u) {
            while (atomic_load_explicit(&probe.entered, memory_order_acquire) != 1)
              salts_thread_yield();
          }
        }
        wait_deadline = salts_hrtime() + UINT64_C(1000000000);
        do {
          snapshot_status = turbo_flow_pool_snapshot_at(flow, 0u, &snapshot);
          if (snapshot_status != SALTS_OK) break;
          if (snapshot.queued != DOMAIN_ADMISSION_EXPECTED_QUEUED) salts_thread_yield();
        } while (snapshot.queued != DOMAIN_ADMISSION_EXPECTED_QUEUED &&
                 salts_hrtime() < wait_deadline);
        check_equal(snapshot_status, SALTS_OK);
        check_equal(snapshot.queued, DOMAIN_ADMISSION_EXPECTED_QUEUED);

        turbo_flow_msg_init(&overflow);
        check_equal(turbo_flow_publish(flow, "input", &overflow), expected[policy_index]);
        turbo_flow_msg_cleanup(&overflow);
        atomic_store_explicit(&probe.release, 1, memory_order_release);
        for (size_t i = 0; i < DOMAIN_ADMISSION_PUBLISHER_COUNT; ++i) {
          check_equal(salts_thread_join(&threads[i]), SALTS_OK);
          check_equal(atomic_load_explicit(&publishes[i].result, memory_order_acquire), SALTS_OK);
        }
        check_equal(atomic_load_explicit(&probe.entered, memory_order_acquire),
                     DOMAIN_ADMISSION_PUBLISHER_COUNT);
        check_equal(turbo_flow_pool_snapshot_at(flow, 0u, &snapshot), SALTS_OK);
        check_equal(snapshot.submitted, DOMAIN_ADMISSION_TOTAL_SUBMISSIONS);
        check_equal(snapshot.started, DOMAIN_ADMISSION_PUBLISHER_COUNT);
        check_equal(snapshot.completed, DOMAIN_ADMISSION_PUBLISHER_COUNT);
        check_equal(snapshot.rejected, policy_index == 0u ? 1u : 0u);
        check_equal(snapshot.canceled, policy_index == 1u ? 1u : 0u);
        check_equal(snapshot.queued, 0u);
        check_equal(snapshot.active, 0u);
        check_equal(turbo_flow_stop(flow), SALTS_OK);
        turbo_flow_destroy(flow);
      }
    }

    it("requires a reorder boundary for preserve-input worker operations") {
      static const char *dsl = "source input operation data.input\n"
                               "stage work operation data.parallel worker 2 capacity 64\n"
                               "stage main {\n"
                               "  input -> work\n"
                               "}\n";
      turbo_flow_t *flow = turbo_flow_create();
      turbo_flow_operation_descriptor_t input =
          operation_descriptor("data.input", TURBO_FLOW_DOMAIN_DATA, TURBO_FLOW_DOMAIN_NONE, NULL,
                               TURBO_FLOW_DOMAIN_DATA, "Message", TURBO_FLOW_OPERATION_SOURCE);
      turbo_flow_operation_descriptor_t work = operation_descriptor(
          "data.parallel", TURBO_FLOW_DOMAIN_DATA, TURBO_FLOW_DOMAIN_DATA, "Message",
          TURBO_FLOW_DOMAIN_DATA, "Message", TURBO_FLOW_OPERATION_STAGE);

      require_worker_handoff(&work, 64u);
      work.runtime.ordering = TURBO_FLOW_ORDERING_PRESERVE_INPUT;
      check_not_null(flow);
      check_equal(turbo_flow_register_operation(flow, &input), SALTS_OK);
      check_equal(turbo_flow_register_operation(flow, &work), SALTS_OK);
      turbo_flow_operation_provider_registration_t operation_provider_18 =
          TURBO_FLOW_OPERATION_PROVIDER_REGISTRATION_INIT;
      operation_provider_18.operation_name = work.name;
      operation_provider_18.fn = domain_noop_stage;
      operation_provider_18.ctx = NULL;
      check_equal(turbo_flow_register_operation_provider(flow, &operation_provider_18), SALTS_OK);
      check_equal(turbo_flow_parse_string(flow, dsl, strlen(dsl)), SALTS_OK);
      check_equal(turbo_flow_compile(flow), SALTS_EINVAL);
      check_contains(turbo_flow_last_error(flow)->message, "requires a reorder boundary");
      turbo_flow_destroy(flow);
    }

    it("accepts preserve-input worker operations with an explicit reorder boundary") {
      static const char *dsl = "source input operation data.input\n"
                               "stage work operation data.parallel worker 2 capacity 64 "
                               "reorder capacity 64 timeout 1000\n"
                               "stage main {\n"
                               "  input -> work\n"
                               "}\n";
      turbo_flow_t *flow = turbo_flow_create();
      turbo_flow_operation_descriptor_t input =
          operation_descriptor("data.input", TURBO_FLOW_DOMAIN_DATA, TURBO_FLOW_DOMAIN_NONE, NULL,
                               TURBO_FLOW_DOMAIN_DATA, "Message", TURBO_FLOW_OPERATION_SOURCE);
      turbo_flow_operation_descriptor_t work = operation_descriptor(
          "data.parallel", TURBO_FLOW_DOMAIN_DATA, TURBO_FLOW_DOMAIN_DATA, "Message",
          TURBO_FLOW_DOMAIN_DATA, "Message", TURBO_FLOW_OPERATION_STAGE);

      require_worker_handoff(&work, 64u);
      work.runtime.ordering = TURBO_FLOW_ORDERING_PRESERVE_INPUT;
      check_not_null(flow);
      check_equal(turbo_flow_register_operation(flow, &input), SALTS_OK);
      check_equal(turbo_flow_register_operation(flow, &work), SALTS_OK);
      turbo_flow_operation_provider_registration_t operation_provider_19 =
          TURBO_FLOW_OPERATION_PROVIDER_REGISTRATION_INIT;
      operation_provider_19.operation_name = work.name;
      operation_provider_19.fn = domain_noop_stage;
      operation_provider_19.ctx = NULL;
      check_equal(turbo_flow_register_operation_provider(flow, &operation_provider_19), SALTS_OK);
      check_equal(turbo_flow_parse_string(flow, dsl, strlen(dsl)), SALTS_OK);
      check_equal(turbo_flow_compile(flow), SALTS_OK);
      turbo_flow_destroy(flow);
    }

    it("accepts operation deadlines with an executable stage owner") {
      static const char *dsl = "source input operation data.input\n"
                               "stage work operation data.validate\n"
                               "stage main {\n"
                               "  input -> work\n"
                               "}\n";
      turbo_flow_t *flow = turbo_flow_create();
      turbo_flow_operation_descriptor_t input =
          operation_descriptor("data.input", TURBO_FLOW_DOMAIN_DATA, TURBO_FLOW_DOMAIN_NONE, NULL,
                               TURBO_FLOW_DOMAIN_DATA, "Message", TURBO_FLOW_OPERATION_SOURCE);
      turbo_flow_operation_descriptor_t work = operation_descriptor(
          "data.validate", TURBO_FLOW_DOMAIN_DATA, TURBO_FLOW_DOMAIN_DATA, "Message",
          TURBO_FLOW_DOMAIN_DATA, "Message", TURBO_FLOW_OPERATION_STAGE);

      work.runtime.deadline_ms = 1000u;
      check_not_null(flow);
      check_equal(turbo_flow_register_operation(flow, &input), SALTS_OK);
      check_equal(turbo_flow_register_operation(flow, &work), SALTS_OK);
      turbo_flow_operation_provider_registration_t operation_provider_20 =
          TURBO_FLOW_OPERATION_PROVIDER_REGISTRATION_INIT;
      operation_provider_20.operation_name = work.name;
      operation_provider_20.fn = domain_noop_stage;
      operation_provider_20.ctx = NULL;
      check_equal(turbo_flow_register_operation_provider(flow, &operation_provider_20), SALTS_OK);
      check_equal(turbo_flow_parse_string(flow, dsl, strlen(dsl)), SALTS_OK);
      check_equal(turbo_flow_compile(flow), SALTS_OK);
      turbo_flow_destroy(flow);
    }

    it("rejects source deadlines without a per-message adapter owner contract") {
      static const char *dsl = "source input operation data.input\n";
      turbo_flow_t *flow = turbo_flow_create();
      turbo_flow_operation_descriptor_t input =
          operation_descriptor("data.input", TURBO_FLOW_DOMAIN_DATA, TURBO_FLOW_DOMAIN_NONE, NULL,
                               TURBO_FLOW_DOMAIN_DATA, "Message", TURBO_FLOW_OPERATION_SOURCE);

      input.runtime.deadline_ms = 10u;
      check_not_null(flow);
      check_equal(turbo_flow_register_operation(flow, &input), SALTS_OK);
      check_equal(turbo_flow_parse_string(flow, dsl, strlen(dsl)), SALTS_OK);
      check_equal(turbo_flow_compile(flow), SALTS_ENOTSUP);
      check_contains(turbo_flow_last_error(flow)->message, "source operation deadline");
      turbo_flow_destroy(flow);
    }

    it("reports inline operation timeout after callback return and stops downstream") {
      static const char *dsl = "source input operation data.input\n"
                               "stage work operation data.validate\n"
                               "stage sink operation test.sink\n"
                               "stage main {\n"
                               "  input -> work -> sink\n"
                               "}\n";
      turbo_flow_t *flow = turbo_flow_create();
      turbo_flow_msg_t message;
      deadline_probe_t probe = {0};
      int sink_called = 0;
      turbo_flow_operation_descriptor_t input =
          operation_descriptor("data.input", TURBO_FLOW_DOMAIN_DATA, TURBO_FLOW_DOMAIN_NONE, NULL,
                               TURBO_FLOW_DOMAIN_DATA, "Message", TURBO_FLOW_OPERATION_SOURCE);
      turbo_flow_operation_descriptor_t work = operation_descriptor(
          "data.validate", TURBO_FLOW_DOMAIN_DATA, TURBO_FLOW_DOMAIN_DATA, "Message",
          TURBO_FLOW_DOMAIN_DATA, "Message", TURBO_FLOW_OPERATION_STAGE);

      work.runtime.deadline_ms = 5u;
      turbo_flow_msg_init(&message);
      check_not_null(flow);
      check_equal(turbo_flow_register_operation(flow, &input), SALTS_OK);
      work.scope.state = TURBO_FLOW_STATE_SCOPE_GRAPH;
      work.scope.lifetime = TURBO_FLOW_LIFETIME_RUNTIME_GENERATION;
      work.scope.authority = TURBO_FLOW_AUTHORITY_DATA_MUTATION;
      check_equal(turbo_flow_register_operation(flow, &work), SALTS_OK);
      turbo_flow_operation_provider_registration_t operation_provider_21 =
          TURBO_FLOW_OPERATION_PROVIDER_REGISTRATION_INIT;
      operation_provider_21.operation_name = work.name;
      operation_provider_21.fn = domain_slow_inline_stage;
      operation_provider_21.ctx = &probe;
      check_equal(turbo_flow_register_operation_provider(flow, &operation_provider_21), SALTS_OK);
      flow_test_operation_t operation_provider_22 =
          flow_test_operation_init("test.sink", domain_count_stage, &sink_called);
      operation_provider_22.descriptor.scope.state = TURBO_FLOW_STATE_SCOPE_GRAPH;
      operation_provider_22.descriptor.scope.lifetime = TURBO_FLOW_LIFETIME_RUNTIME_GENERATION;
      check_equal(flow_test_operation_register(flow, &operation_provider_22), SALTS_OK);
      check_equal(turbo_flow_parse_string(flow, dsl, strlen(dsl)), SALTS_OK);
      check_equal(turbo_flow_compile(flow), SALTS_OK);
      check_equal(turbo_flow_start(flow), SALTS_OK);
      check_equal(turbo_flow_publish(flow, "input", &message), SALTS_ETIMEDOUT);
      check_equal(probe.called, 1);
      check_equal(sink_called, 0);
      check_equal(turbo_flow_stop(flow), SALTS_OK);
      turbo_flow_msg_cleanup(&message);
      turbo_flow_destroy(flow);
    }

    it("cancels thread coroutine and worker operations cooperatively at deadline") {
      static const char *thread_dsl = "source input operation data.input\n"
                                      "stage work operation data.validate exec thread workers 1\n"
                                      "stage main {\n"
                                      "  input -> work\n"
                                      "}\n";
      static const char *coro_dsl = "source input operation data.input\n"
                                    "stage work operation data.validate exec coro\n"
                                    "stage main {\n"
                                    "  input -> work\n"
                                    "}\n";
      static const char *worker_dsl = "source input operation data.input\n"
                                      "stage work operation data.validate worker 1 capacity 8\n"
                                      "stage main {\n"
                                      "  input -> work\n"
                                      "}\n";
      const char *dsls[] = {thread_dsl, coro_dsl, worker_dsl};
      const uint32_t exec_masks[] = {TURBO_FLOW_OPERATION_EXEC_THREAD,
                                     TURBO_FLOW_OPERATION_EXEC_CORO,
                                     TURBO_FLOW_OPERATION_EXEC_INLINE};

      for (size_t i = 0; i < sizeof(dsls) / sizeof(dsls[0]); ++i) {
        turbo_flow_t *flow = turbo_flow_create();
        turbo_flow_msg_t message;
        deadline_probe_t probe = {0};
        turbo_flow_operation_descriptor_t input = operation_descriptor(
            "data.input", TURBO_FLOW_DOMAIN_DATA, TURBO_FLOW_DOMAIN_NONE, NULL,
            TURBO_FLOW_DOMAIN_DATA, "Message", TURBO_FLOW_OPERATION_SOURCE);
        turbo_flow_operation_descriptor_t work = operation_descriptor(
            "data.validate", TURBO_FLOW_DOMAIN_DATA, TURBO_FLOW_DOMAIN_DATA, "Message",
            TURBO_FLOW_DOMAIN_DATA, "Message", TURBO_FLOW_OPERATION_STAGE);

        work.execution_mask = exec_masks[i];
        work.scope.concurrency = TURBO_FLOW_CONCURRENCY_POOL;
        work.runtime.deadline_ms = 5u;
        work.runtime.cancellation = TURBO_FLOW_CANCELLATION_COOPERATIVE;
        if (i == 2u) require_worker_handoff(&work, 8u);
        turbo_flow_msg_init(&message);
        check_not_null(flow);
        check_equal(turbo_flow_register_operation(flow, &input), SALTS_OK);
        work.scope.state = TURBO_FLOW_STATE_SCOPE_GRAPH;
        work.scope.lifetime = TURBO_FLOW_LIFETIME_RUNTIME_GENERATION;
        work.scope.authority = TURBO_FLOW_AUTHORITY_DATA_MUTATION;
        check_equal(turbo_flow_register_operation(flow, &work), SALTS_OK);
        turbo_flow_operation_provider_registration_t operation_provider_23 =
            TURBO_FLOW_OPERATION_PROVIDER_REGISTRATION_INIT;
        operation_provider_23.operation_name = work.name;
        operation_provider_23.fn = domain_deadline_yield_stage;
        operation_provider_23.ctx = &probe;
        check_equal(turbo_flow_register_operation_provider(flow, &operation_provider_23), SALTS_OK);
        check_equal(turbo_flow_parse_string(flow, dsls[i], strlen(dsls[i])), SALTS_OK);
        check_equal(turbo_flow_compile(flow), SALTS_OK);
        check_equal(turbo_flow_start(flow), SALTS_OK);
        check_equal(turbo_flow_publish(flow, "input", &message), SALTS_ETIMEDOUT);
        check_equal(probe.called, 1);
        check_equal(probe.terminal_status, SALTS_ETIMEDOUT);
        check_equal(turbo_flow_stop(flow), SALTS_OK);
        turbo_flow_msg_cleanup(&message);
        turbo_flow_destroy(flow);
      }
    }

    it("requires a reject edge for reject error mode") {
      static const char *dsl = "source input operation data.input\n"
                               "stage work operation data.validate\n"
                               "stage main {\n"
                               "  input -> work\n"
                               "}\n";
      turbo_flow_t *flow = turbo_flow_create();
      turbo_flow_operation_descriptor_t input =
          operation_descriptor("data.input", TURBO_FLOW_DOMAIN_DATA, TURBO_FLOW_DOMAIN_NONE, NULL,
                               TURBO_FLOW_DOMAIN_DATA, "Message", TURBO_FLOW_OPERATION_SOURCE);
      turbo_flow_operation_descriptor_t work = operation_descriptor(
          "data.validate", TURBO_FLOW_DOMAIN_DATA, TURBO_FLOW_DOMAIN_DATA, "Message",
          TURBO_FLOW_DOMAIN_DATA, "Message", TURBO_FLOW_OPERATION_STAGE);

      work.runtime.error_mode = TURBO_FLOW_ERROR_REJECT;
      check_not_null(flow);
      check_equal(turbo_flow_register_operation(flow, &input), SALTS_OK);
      check_equal(turbo_flow_register_operation(flow, &work), SALTS_OK);
      turbo_flow_operation_provider_registration_t operation_provider_24 =
          TURBO_FLOW_OPERATION_PROVIDER_REGISTRATION_INIT;
      operation_provider_24.operation_name = work.name;
      operation_provider_24.fn = domain_noop_stage;
      operation_provider_24.ctx = NULL;
      check_equal(turbo_flow_register_operation_provider(flow, &operation_provider_24), SALTS_OK);
      check_equal(turbo_flow_parse_string(flow, dsl, strlen(dsl)), SALTS_OK);
      check_equal(turbo_flow_compile(flow), SALTS_EINVAL);
      check_contains(turbo_flow_last_error(flow)->message, "requires a reject edge");
      turbo_flow_destroy(flow);
    }

    it("accepts reject error mode when the stage owns a reject edge") {
      static const char *dsl = "source input operation data.input\n"
                               "stage work operation data.validate\n"
                               "stage rejected operation test.rejected\n"
                               "stage main {\n"
                               "  input -> work\n"
                               "  reject validation_failed work -> rejected\n"
                               "}\n";
      turbo_flow_t *flow = turbo_flow_create();
      turbo_flow_operation_descriptor_t input =
          operation_descriptor("data.input", TURBO_FLOW_DOMAIN_DATA, TURBO_FLOW_DOMAIN_NONE, NULL,
                               TURBO_FLOW_DOMAIN_DATA, "Message", TURBO_FLOW_OPERATION_SOURCE);
      turbo_flow_operation_descriptor_t work = operation_descriptor(
          "data.validate", TURBO_FLOW_DOMAIN_DATA, TURBO_FLOW_DOMAIN_DATA, "Message",
          TURBO_FLOW_DOMAIN_DATA, "Message", TURBO_FLOW_OPERATION_STAGE);

      work.runtime.error_mode = TURBO_FLOW_ERROR_REJECT;
      check_not_null(flow);
      check_equal(turbo_flow_register_operation(flow, &input), SALTS_OK);
      check_equal(turbo_flow_register_operation(flow, &work), SALTS_OK);
      turbo_flow_operation_provider_registration_t operation_provider_25 =
          TURBO_FLOW_OPERATION_PROVIDER_REGISTRATION_INIT;
      operation_provider_25.operation_name = work.name;
      operation_provider_25.fn = domain_noop_stage;
      operation_provider_25.ctx = NULL;
      check_equal(turbo_flow_register_operation_provider(flow, &operation_provider_25), SALTS_OK);
      flow_test_operation_t operation_provider_26 =
          flow_test_operation_init("test.rejected", domain_noop_stage, NULL);
      check_equal(flow_test_operation_register(flow, &operation_provider_26), SALTS_OK);
      check_equal(turbo_flow_parse_string(flow, dsl, strlen(dsl)), SALTS_OK);
      check_equal(turbo_flow_compile(flow), SALTS_OK);
      turbo_flow_destroy(flow);
    }

    it("requires an explicit retry policy for retry error mode") {
      static const char *dsl = "source input operation data.input\n"
                               "stage work operation data.validate\n"
                               "stage main {\n"
                               "  input -> work\n"
                               "}\n";
      turbo_flow_t *flow = turbo_flow_create();
      turbo_flow_operation_descriptor_t input =
          operation_descriptor("data.input", TURBO_FLOW_DOMAIN_DATA, TURBO_FLOW_DOMAIN_NONE, NULL,
                               TURBO_FLOW_DOMAIN_DATA, "Message", TURBO_FLOW_OPERATION_SOURCE);
      turbo_flow_operation_descriptor_t work = operation_descriptor(
          "data.validate", TURBO_FLOW_DOMAIN_DATA, TURBO_FLOW_DOMAIN_DATA, "Message",
          TURBO_FLOW_DOMAIN_DATA, "Message", TURBO_FLOW_OPERATION_STAGE);

      work.runtime.error_mode = TURBO_FLOW_ERROR_RETRY;
      work.runtime.settlement = TURBO_FLOW_SETTLEMENT_RETRY;
      check_not_null(flow);
      check_equal(turbo_flow_register_operation(flow, &input), SALTS_OK);
      check_equal(turbo_flow_register_operation(flow, &work), SALTS_OK);
      turbo_flow_operation_provider_registration_t operation_provider_27 =
          TURBO_FLOW_OPERATION_PROVIDER_REGISTRATION_INIT;
      operation_provider_27.operation_name = work.name;
      operation_provider_27.fn = domain_noop_stage;
      operation_provider_27.ctx = NULL;
      check_equal(turbo_flow_register_operation_provider(flow, &operation_provider_27), SALTS_OK);
      check_equal(turbo_flow_parse_string(flow, dsl, strlen(dsl)), SALTS_OK);
      check_equal(turbo_flow_compile(flow), SALTS_EINVAL);
      check_contains(turbo_flow_last_error(flow)->message, "requires a retry policy");
      turbo_flow_destroy(flow);
    }

    it("accepts retry error mode with adapter retry policy and settlement") {
      static const char *dsl =
          "source input operation data.input\n"
          "stage work adapter retryable operation data.validate retry attempts 3\n"
          "stage main {\n"
          "  input -> work\n"
          "}\n";
      turbo_flow_t *flow = turbo_flow_create();
      turbo_flow_adapter_ops_t adapter_ops;
      turbo_flow_operation_descriptor_t input =
          operation_descriptor("data.input", TURBO_FLOW_DOMAIN_DATA, TURBO_FLOW_DOMAIN_NONE, NULL,
                               TURBO_FLOW_DOMAIN_DATA, "Message", TURBO_FLOW_OPERATION_SOURCE);
      turbo_flow_operation_descriptor_t work = operation_descriptor(
          "data.validate", TURBO_FLOW_DOMAIN_DATA, TURBO_FLOW_DOMAIN_DATA, "Message",
          TURBO_FLOW_DOMAIN_DATA, "Message", TURBO_FLOW_OPERATION_STAGE);

      memset(&adapter_ops, 0, sizeof(adapter_ops));
      adapter_ops.consume = domain_noop_consume;
      adapter_ops.consume_retry = domain_noop_retry;
      work.runtime.error_mode = TURBO_FLOW_ERROR_RETRY;
      work.runtime.settlement = TURBO_FLOW_SETTLEMENT_RETRY;
      check_not_null(flow);
      check_equal(turbo_flow_register_adapter(flow, "retryable", &adapter_ops, NULL), SALTS_OK);
      check_equal(turbo_flow_register_operation(flow, &input), SALTS_OK);
      check_equal(turbo_flow_register_operation(flow, &work), SALTS_OK);
      check_equal(turbo_flow_parse_string(flow, dsl, strlen(dsl)), SALTS_OK);
      check_equal(turbo_flow_compile(flow), SALTS_OK);
      turbo_flow_destroy(flow);
    }

    it("requires an explicit adapter settlement owner") {
      static const char *dsl = "source input operation data.input\n"
                               "stage work adapter owner operation data.validate\n"
                               "stage main {\n"
                               "  input -> work\n"
                               "}\n";
      turbo_flow_t *flow = turbo_flow_create();
      turbo_flow_adapter_ops_t adapter_ops;
      turbo_flow_operation_descriptor_t input =
          operation_descriptor("data.input", TURBO_FLOW_DOMAIN_DATA, TURBO_FLOW_DOMAIN_NONE, NULL,
                               TURBO_FLOW_DOMAIN_DATA, "Message", TURBO_FLOW_OPERATION_SOURCE);
      turbo_flow_operation_descriptor_t work = operation_descriptor(
          "data.validate", TURBO_FLOW_DOMAIN_DATA, TURBO_FLOW_DOMAIN_DATA, "Message",
          TURBO_FLOW_DOMAIN_DATA, "Message", TURBO_FLOW_OPERATION_STAGE);

      memset(&adapter_ops, 0, sizeof(adapter_ops));
      adapter_ops.consume = domain_noop_consume;
      work.runtime.settlement = TURBO_FLOW_SETTLEMENT_ACKNOWLEDGE;
      check_not_null(flow);
      check_equal(turbo_flow_register_adapter(flow, "owner", &adapter_ops, NULL), SALTS_OK);
      check_equal(turbo_flow_register_operation(flow, &input), SALTS_OK);
      check_equal(turbo_flow_register_operation(flow, &work), SALTS_OK);
      turbo_flow_operation_provider_registration_t operation_provider_28 =
          TURBO_FLOW_OPERATION_PROVIDER_REGISTRATION_INIT;
      operation_provider_28.operation_name = work.name;
      operation_provider_28.fn = domain_noop_stage;
      operation_provider_28.ctx = NULL;
      check_equal(turbo_flow_register_operation_provider(flow, &operation_provider_28), SALTS_OK);
      check_equal(turbo_flow_parse_string(flow, dsl, strlen(dsl)), SALTS_OK);
      check_equal(turbo_flow_compile(flow), SALTS_ENOTSUP);
      check_contains(turbo_flow_last_error(flow)->message, "settlement owner");
      turbo_flow_destroy(flow);
    }

    it("delivers automatic complete and explicit protocol ACK to the owner") {
      static const char *dsl = "source input operation data.input\n"
                               "stage work adapter owner operation data.validate\n"
                               "stage sink operation test.sink\n"
                               "stage main {\n"
                               "  input -> work -> sink\n"
                               "}\n";
      const turbo_flow_settlement_action_t actions[] = {
          TURBO_FLOW_SETTLEMENT_ACTION_COMPLETE, TURBO_FLOW_SETTLEMENT_ACTION_ACKNOWLEDGE};

      for (size_t action_index = 0; action_index < 2u; ++action_index) {
        turbo_flow_t *flow = turbo_flow_create();
        turbo_flow_adapter_ops_t adapter_ops;
        turbo_flow_settlement_owner_ops_t owner_ops = TURBO_FLOW_SETTLEMENT_OWNER_OPS_INIT;
        turbo_flow_operation_descriptor_t input = operation_descriptor(
            "data.input", TURBO_FLOW_DOMAIN_DATA, TURBO_FLOW_DOMAIN_NONE, NULL,
            TURBO_FLOW_DOMAIN_DATA, "Message", TURBO_FLOW_OPERATION_SOURCE);
        turbo_flow_operation_descriptor_t work = operation_descriptor(
            "data.validate", TURBO_FLOW_DOMAIN_DATA, TURBO_FLOW_DOMAIN_DATA, "Message",
            TURBO_FLOW_DOMAIN_DATA, "Message", TURBO_FLOW_OPERATION_STAGE);
        settlement_probe_t probe;
        turbo_flow_msg_t message;
        int sink_called = 0;

        memset(&adapter_ops, 0, sizeof(adapter_ops));
        memset(&probe, 0, sizeof(probe));
        adapter_ops.consume = action_index == 0u ? domain_noop_consume : domain_settlement_consume;
        owner_ops.apply = domain_settlement_apply;
        work.runtime.settlement = action_index == 0u ? TURBO_FLOW_SETTLEMENT_COMPLETE
                                                     : TURBO_FLOW_SETTLEMENT_ACKNOWLEDGE;
        probe.report_action = actions[action_index];
        probe.report_status = SALTS_OK;
        check_not_null(flow);
        check_equal(turbo_flow_register_adapter(flow, "owner", &adapter_ops, &probe), SALTS_OK);
        check_equal(turbo_flow_register_adapter_settlement(flow, "owner", &owner_ops, &probe),
                     SALTS_OK);
        check_equal(turbo_flow_register_operation(flow, &input), SALTS_OK);
        check_equal(turbo_flow_register_operation(flow, &work), SALTS_OK);
        if (action_index == 0u) {
          turbo_flow_operation_provider_registration_t operation_provider_29 =
              TURBO_FLOW_OPERATION_PROVIDER_REGISTRATION_INIT;
          operation_provider_29.operation_name = work.name;
          operation_provider_29.fn = domain_noop_stage;
          operation_provider_29.ctx = NULL;
          check_equal(turbo_flow_register_operation_provider(flow, &operation_provider_29),
                      SALTS_OK);
        }
        flow_test_operation_t operation_provider_30 =
            flow_test_operation_init("test.sink", domain_count_stage, &sink_called);
        operation_provider_30.descriptor.scope.state = TURBO_FLOW_STATE_SCOPE_GRAPH;
        operation_provider_30.descriptor.scope.lifetime = TURBO_FLOW_LIFETIME_RUNTIME_GENERATION;
        check_equal(flow_test_operation_register(flow, &operation_provider_30), SALTS_OK);
        check_equal(turbo_flow_parse_string(flow, dsl, strlen(dsl)), SALTS_OK);
        check_equal(turbo_flow_compile(flow), SALTS_OK);
        check_equal(turbo_flow_start(flow), SALTS_OK);
        turbo_flow_msg_init(&message);
        message.id = 42u + action_index;
        check_equal(turbo_flow_publish(flow, "input", &message), SALTS_OK);
        check_equal(probe.owner_called, 1);
        check_equal(probe.observed.action, actions[action_index]);
        check_equal(probe.observed.message_id, message.id);
        check_equal(probe.observed.attempt, 1u);
        check_equal(sink_called, 1);
        turbo_flow_msg_cleanup(&message);
        check_equal(turbo_flow_stop(flow), SALTS_OK);
        turbo_flow_destroy(flow);
      }
    }

    it("delivers terminal settlement actions without releasing normal downstream") {
      static const char *dsl = "source input operation data.input\n"
                               "stage work adapter owner operation data.validate\n"
                               "stage sink operation test.sink\n"
                               "stage main {\n"
                               "  input -> work -> sink\n"
                               "}\n";
      const turbo_flow_settlement_action_t actions[] = {
          TURBO_FLOW_SETTLEMENT_ACTION_REQUEUE, TURBO_FLOW_SETTLEMENT_ACTION_DEAD_LETTER,
          TURBO_FLOW_SETTLEMENT_ACTION_CANCELED};
      const int statuses[] = {SALTS_EBUSY, SALTS_EIO, SALTS_ECANCELED};
      turbo_flow_t *flow = turbo_flow_create();
      turbo_flow_adapter_ops_t adapter_ops;
      turbo_flow_settlement_owner_ops_t owner_ops = TURBO_FLOW_SETTLEMENT_OWNER_OPS_INIT;
      turbo_flow_operation_descriptor_t input = operation_descriptor(
          "data.input", TURBO_FLOW_DOMAIN_DATA, TURBO_FLOW_DOMAIN_NONE, NULL,
          TURBO_FLOW_DOMAIN_DATA, "Message", TURBO_FLOW_OPERATION_SOURCE);
      turbo_flow_operation_descriptor_t work = operation_descriptor(
          "data.validate", TURBO_FLOW_DOMAIN_DATA, TURBO_FLOW_DOMAIN_DATA, "Message",
          TURBO_FLOW_DOMAIN_DATA, "Message", TURBO_FLOW_OPERATION_STAGE);
      settlement_probe_t probe;
      int sink_called = 0;

      memset(&adapter_ops, 0, sizeof(adapter_ops));
      memset(&probe, 0, sizeof(probe));
      adapter_ops.consume = domain_settlement_consume;
      owner_ops.apply = domain_settlement_apply;
      work.runtime.error_mode = TURBO_FLOW_ERROR_SETTLE;
      work.runtime.settlement = TURBO_FLOW_SETTLEMENT_REQUEUE |
                                TURBO_FLOW_SETTLEMENT_DEAD_LETTER |
                                TURBO_FLOW_SETTLEMENT_CANCELED;
      check_not_null(flow);
      check_equal(turbo_flow_register_adapter(flow, "owner", &adapter_ops, &probe), SALTS_OK);
      check_equal(turbo_flow_register_adapter_settlement(flow, "owner", &owner_ops, &probe),
                   SALTS_OK);
      check_equal(turbo_flow_register_operation(flow, &input), SALTS_OK);
      check_equal(turbo_flow_register_operation(flow, &work), SALTS_OK);
      flow_test_operation_t operation_provider_31 =
          flow_test_operation_init("test.sink", domain_count_stage, &sink_called);
      operation_provider_31.descriptor.scope.state = TURBO_FLOW_STATE_SCOPE_GRAPH;
      operation_provider_31.descriptor.scope.lifetime = TURBO_FLOW_LIFETIME_RUNTIME_GENERATION;
      check_equal(flow_test_operation_register(flow, &operation_provider_31), SALTS_OK);
      check_equal(turbo_flow_parse_string(flow, dsl, strlen(dsl)), SALTS_OK);
      check_equal(turbo_flow_compile(flow), SALTS_OK);
      check_equal(turbo_flow_start(flow), SALTS_OK);
      for (size_t i = 0; i < 3u; ++i) {
        turbo_flow_msg_t message;
        probe.report_action = actions[i];
        probe.report_status = statuses[i];
        turbo_flow_msg_init(&message);
        check_equal(turbo_flow_publish(flow, "input", &message), SALTS_OK);
        turbo_flow_msg_cleanup(&message);
        check_equal(probe.stage_called, (int)i + 1);
        check_equal(probe.owner_called, (int)i + 1);
        check_equal(probe.observed.action, actions[i]);
        check_equal(probe.observed.status, statuses[i]);
      }
      check_equal(probe.owner_called, 3);
      check_equal(sink_called, 0);
      check_equal(turbo_flow_stop(flow), SALTS_OK);
      turbo_flow_destroy(flow);
    }

    it("preserves settlement scope across all compute and handoff paths") {
      static const char *dsls[] = {
          "source input operation data.input\n"
          "stage work adapter owner operation data.validate\n"
          "stage main {\n  input -> work\n}\n",
          "source input operation data.input\n"
          "stage work adapter owner operation data.validate exec thread workers 1\n"
          "stage main {\n  input -> work\n}\n",
          "source input operation data.input\n"
          "stage work adapter owner operation data.validate exec coro lanes 1 pool 2\n"
          "stage main {\n  input -> work\n}\n",
          "source input operation data.input\n"
          "stage work adapter owner operation data.validate worker 1 capacity 8\n"
          "stage main {\n  input -> work\n}\n"};
      const uint32_t masks[] = {
          TURBO_FLOW_OPERATION_EXEC_INLINE, TURBO_FLOW_OPERATION_EXEC_THREAD,
          TURBO_FLOW_OPERATION_EXEC_CORO, TURBO_FLOW_OPERATION_EXEC_INLINE};

      for (size_t path = 0; path < 4u; ++path) {
        turbo_flow_t *flow = turbo_flow_create();
        turbo_flow_adapter_ops_t adapter_ops;
        turbo_flow_settlement_owner_ops_t owner_ops = TURBO_FLOW_SETTLEMENT_OWNER_OPS_INIT;
        turbo_flow_operation_descriptor_t input = operation_descriptor(
            "data.input", TURBO_FLOW_DOMAIN_DATA, TURBO_FLOW_DOMAIN_NONE, NULL,
            TURBO_FLOW_DOMAIN_DATA, "Message", TURBO_FLOW_OPERATION_SOURCE);
        turbo_flow_operation_descriptor_t work = operation_descriptor(
            "data.validate", TURBO_FLOW_DOMAIN_DATA, TURBO_FLOW_DOMAIN_DATA, "Message",
            TURBO_FLOW_DOMAIN_DATA, "Message", TURBO_FLOW_OPERATION_STAGE);
        settlement_probe_t probe;
        turbo_flow_msg_t message;

        memset(&adapter_ops, 0, sizeof(adapter_ops));
        memset(&probe, 0, sizeof(probe));
        owner_ops.apply = domain_settlement_apply;
        work.execution_mask = masks[path];
        work.runtime.error_mode = TURBO_FLOW_ERROR_SETTLE;
        work.runtime.settlement = TURBO_FLOW_SETTLEMENT_REQUEUE;
        probe.report_action = TURBO_FLOW_SETTLEMENT_ACTION_REQUEUE;
        probe.report_status = SALTS_EBUSY;
        if (path == 1u || path == 2u || path == 3u) {
          work.scope.concurrency = TURBO_FLOW_CONCURRENCY_POOL;
        }
        if (path == 3u) require_worker_handoff(&work, 8u);
        check_not_null(flow);
        check_equal(turbo_flow_register_adapter(flow, "owner", &adapter_ops, NULL), SALTS_OK);
        check_equal(turbo_flow_register_adapter_settlement(flow, "owner", &owner_ops, &probe),
                     SALTS_OK);
        check_equal(turbo_flow_register_operation(flow, &input), SALTS_OK);
        work.scope.state = TURBO_FLOW_STATE_SCOPE_GRAPH;
        work.scope.lifetime = TURBO_FLOW_LIFETIME_RUNTIME_GENERATION;
        work.scope.authority = TURBO_FLOW_AUTHORITY_DATA_MUTATION;
        check_equal(turbo_flow_register_operation(flow, &work), SALTS_OK);
        turbo_flow_operation_provider_registration_t operation_provider_32 =
            TURBO_FLOW_OPERATION_PROVIDER_REGISTRATION_INIT;
        operation_provider_32.operation_name = work.name;
        operation_provider_32.fn = domain_settlement_stage;
        operation_provider_32.ctx = &probe;
        check_equal(turbo_flow_register_operation_provider(flow, &operation_provider_32), SALTS_OK);
        check_equal(turbo_flow_parse_string(flow, dsls[path], strlen(dsls[path])), SALTS_OK);
        check_equal(turbo_flow_compile(flow), SALTS_OK);
        check_equal(turbo_flow_start(flow), SALTS_OK);
        turbo_flow_msg_init(&message);
        check_equal(turbo_flow_publish(flow, "input", &message), SALTS_OK);
        check_equal(probe.stage_called, 1);
        check_equal(probe.owner_called, 1);
        check_equal(probe.observed.action, TURBO_FLOW_SETTLEMENT_ACTION_REQUEUE);
        turbo_flow_msg_cleanup(&message);
        check_equal(turbo_flow_stop(flow), SALTS_OK);
        turbo_flow_destroy(flow);
      }
    }

    it("rejects duplicate unauthorized and owner-failed settlement results") {
      static const char *dsl = "source input operation data.input\n"
                               "stage work adapter owner operation data.validate\n"
                               "stage main {\n"
                               "  input -> work\n"
                               "}\n";
      const int expected[] = {SALTS_EALREADY, SALTS_EPROTO, SALTS_EIO};

      check_equal(turbo_flow_settlement_report(NULL), SALTS_EINVAL);
      for (size_t scenario = 0; scenario < 3u; ++scenario) {
        turbo_flow_t *flow = turbo_flow_create();
        turbo_flow_adapter_ops_t adapter_ops;
        turbo_flow_settlement_owner_ops_t owner_ops = TURBO_FLOW_SETTLEMENT_OWNER_OPS_INIT;
        turbo_flow_operation_descriptor_t input = operation_descriptor(
            "data.input", TURBO_FLOW_DOMAIN_DATA, TURBO_FLOW_DOMAIN_NONE, NULL,
            TURBO_FLOW_DOMAIN_DATA, "Message", TURBO_FLOW_OPERATION_SOURCE);
        turbo_flow_operation_descriptor_t work = operation_descriptor(
            "data.validate", TURBO_FLOW_DOMAIN_DATA, TURBO_FLOW_DOMAIN_DATA, "Message",
            TURBO_FLOW_DOMAIN_DATA, "Message", TURBO_FLOW_OPERATION_STAGE);
        settlement_probe_t probe;
        turbo_flow_msg_t message;

        memset(&adapter_ops, 0, sizeof(adapter_ops));
        memset(&probe, 0, sizeof(probe));
        adapter_ops.consume = domain_settlement_consume;
        owner_ops.apply = domain_settlement_apply;
        work.runtime.error_mode = TURBO_FLOW_ERROR_SETTLE;
        work.runtime.settlement = TURBO_FLOW_SETTLEMENT_REQUEUE;
        probe.report_action = scenario == 1u ? TURBO_FLOW_SETTLEMENT_ACTION_DEAD_LETTER
                                             : TURBO_FLOW_SETTLEMENT_ACTION_REQUEUE;
        probe.report_status = SALTS_EBUSY;
        probe.report_twice = scenario == 0u;
        probe.owner_status = scenario == 2u ? SALTS_EIO : SALTS_OK;
        check_not_null(flow);
        check_equal(turbo_flow_register_adapter(flow, "owner", &adapter_ops, &probe), SALTS_OK);
        check_equal(turbo_flow_register_adapter_settlement(flow, "owner", &owner_ops, &probe),
                     SALTS_OK);
        check_equal(turbo_flow_register_operation(flow, &input), SALTS_OK);
        check_equal(turbo_flow_register_operation(flow, &work), SALTS_OK);
        check_equal(turbo_flow_parse_string(flow, dsl, strlen(dsl)), SALTS_OK);
        check_equal(turbo_flow_compile(flow), SALTS_OK);
        check_equal(turbo_flow_start(flow), SALTS_OK);
        turbo_flow_msg_init(&message);
        check_equal(turbo_flow_publish(flow, "input", &message), expected[scenario]);
        check_equal(probe.owner_called, scenario == 2u ? 1 : 0);
        turbo_flow_msg_cleanup(&message);
        check_equal(turbo_flow_stop(flow), SALTS_OK);
        turbo_flow_destroy(flow);
      }
    }
  }
}
