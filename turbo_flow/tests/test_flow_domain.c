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

static int domain_noop_stage(turbo_flow_msg_t *message, void *ctx) {
  (void)message;
  (void)ctx;
  return TURBO_OK;
}

static int domain_count_stage(turbo_flow_msg_t *message, void *ctx) {
  int *called = (int *)ctx;
  (void)message;
  *called += 1;
  return TURBO_OK;
}

static int domain_emitting_stage(const turbo_flow_msg_t *input, turbo_flow_emitter_t *emitter,
                                 void *ctx) {
  emission_probe_t *probe = (emission_probe_t *)ctx;
  static int transport_marker;

  probe->emitter_called += 1;
  for (uint32_t i = 0u; i < probe->output_count; ++i) {
    turbo_flow_msg_t output;
    int rc;
    turbo_flow_msg_init(&output);
    output.id = input->id * 10u + i;
    output.type = i;
    if (probe->attach_transport_context) output.transport_context = &transport_marker;
    rc = turbo_flow_emitter_emit_move(emitter, &output);
    turbo_flow_msg_cleanup(&output);
    if (rc != TURBO_OK && rc != TURBO_ENOSPC) return rc;
  }
  return probe->callback_status;
}

static int domain_emission_sink(turbo_flow_msg_t *message, void *ctx) {
  emission_probe_t *probe = (emission_probe_t *)ctx;
  if (probe->sink_count >= sizeof(probe->sink_ids) / sizeof(probe->sink_ids[0])) {
    return TURBO_ENOSPC;
  }
  probe->sink_ids[probe->sink_count++] = message->id;
  return TURBO_OK;
}

static int domain_slow_inline_stage(turbo_flow_msg_t *message, void *ctx) {
  deadline_probe_t *probe = (deadline_probe_t *)ctx;
  (void)message;
  probe->called += 1;
  turbo_sleep_ms(15u);
  probe->terminal_status = TURBO_OK;
  return TURBO_OK;
}

static int domain_deadline_yield_stage(turbo_flow_msg_t *message, void *ctx) {
  deadline_probe_t *probe = (deadline_probe_t *)ctx;
  (void)message;
  probe->called += 1;
  for (;;) {
    int status = turbo_flow_execution_yield();
    if (status != TURBO_OK) {
      probe->terminal_status = status;
      return status;
    }
  }
}

static int domain_blocking_stage(turbo_flow_msg_t *message, void *ctx) {
  admission_probe_t *probe = (admission_probe_t *)ctx;
  (void)message;
  atomic_fetch_add_explicit(&probe->entered, 1, memory_order_acq_rel);
  while (!atomic_load_explicit(&probe->release, memory_order_acquire)) turbo_thread_yield();
  return TURBO_OK;
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
  if (turbo_flow_settlement_report(&result) != TURBO_OK) return TURBO_EPROTO;
  if (probe->report_twice && turbo_flow_settlement_report(&result) != TURBO_EALREADY) {
    return TURBO_EPROTO;
  }
  return TURBO_OK;
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
  if (!stage || !result) return TURBO_EINVAL;
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
  return TURBO_OK;
}

static int domain_noop_consume(void *ctx, turbo_flow_t *flow, const turbo_flow_stage_plan_t *stage,
                               turbo_flow_msg_t *message) {
  (void)ctx;
  (void)flow;
  (void)stage;
  (void)message;
  return TURBO_OK;
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
  if (rc != TURBO_OK) return rc;
  rc = turbo_flow_register_operation(flow, &expand);
  if (rc != TURBO_OK) return rc;
  rc = turbo_flow_register_emitting_operation_provider(flow, &provider);
  if (rc != TURBO_OK) return rc;
  rc = turbo_flow_register_stage_ex(flow, "sink", domain_emission_sink, probe, NULL);
  if (rc != TURBO_OK) return rc;
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
      check_int_eq(turbo_flow_register_primitive(flow, &primitive), TURBO_OK);
      check_int_eq(turbo_flow_register_operation(flow, &operation), TURBO_OK);
      primitive_name[0] = 'x';
      operation_name[0] = 'x';

      stored_primitive = turbo_flow_find_primitive(flow, "queue.jobs");
      stored_operation = turbo_flow_find_operation(flow, "queue.enqueue");
      check_not_null(stored_primitive);
      check_not_null(stored_operation);
      check_str_eq(stored_primitive->type_name, "Queue");
      check_str_eq(stored_operation->resource_type, "Queue");
      check_size_eq(turbo_flow_primitive_count(flow), 1);
      check_size_eq(turbo_flow_operation_count(flow), 1);
      turbo_flow_destroy(flow);
    }

    it("rejects truncated operation descriptors") {
      turbo_flow_t *flow = turbo_flow_create();
      turbo_flow_operation_descriptor_t operation = operation_descriptor(
          "data.validate", TURBO_FLOW_DOMAIN_DATA, TURBO_FLOW_DOMAIN_DATA, "Message",
          TURBO_FLOW_DOMAIN_DATA, "Message", TURBO_FLOW_OPERATION_STAGE);
      operation.size = offsetof(turbo_flow_operation_descriptor_t, runtime);
      check_not_null(flow);
      check_int_eq(turbo_flow_register_operation(flow, &operation), TURBO_EINVAL);
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
      check_int_eq(turbo_flow_register_operation(flow, &operation), TURBO_EINVAL);

      operation.runtime.capacity = 64u;
      operation.runtime.backpressure = TURBO_FLOW_BACKPRESSURE_NONE;
      check_int_eq(turbo_flow_register_operation(flow, &operation), TURBO_EINVAL);
      turbo_flow_destroy(flow);
    }

    it("rejects cross-domain operation contracts without the bridge role") {
      turbo_flow_t *flow = turbo_flow_create();
      turbo_flow_operation_descriptor_t operation = operation_descriptor(
          "mqtt.decode", TURBO_FLOW_DOMAIN_PROTOCOL_PATTERN, TURBO_FLOW_DOMAIN_PROTOCOL_PATTERN,
          "PublishFrame", TURBO_FLOW_DOMAIN_DATA, "Message", TURBO_FLOW_OPERATION_STAGE);

      check_not_null(flow);
      check_int_eq(turbo_flow_register_operation(flow, &operation), TURBO_EINVAL);
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
      check_int_eq(turbo_flow_register_operation(flow, &operation), TURBO_EINVAL);
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
      check_int_eq(turbo_flow_register_primitive(flow, &primitive), TURBO_OK);
      check_int_eq(turbo_flow_register_operation(flow, &operation), TURBO_OK);
      check_int_eq(turbo_flow_reset(flow, 1), TURBO_OK);
      check_size_eq(turbo_flow_primitive_count(flow), 1);
      check_size_eq(turbo_flow_operation_count(flow), 1);
      check_int_eq(turbo_flow_reset(flow, 0), TURBO_OK);
      check_size_eq(turbo_flow_primitive_count(flow), 0);
      check_size_eq(turbo_flow_operation_count(flow), 0);
      turbo_flow_destroy(flow);
    }
  }

  group("DSL and compiler") {
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
      check_int_eq(turbo_flow_register_adapter(flow, "mqtt.server", NULL, NULL), TURBO_OK);
      check_int_eq(turbo_flow_register_primitive(flow, &session), TURBO_OK);
      check_int_eq(turbo_flow_register_operation(flow, &input), TURBO_OK);
      check_int_eq(turbo_flow_register_operation(flow, &validate), TURBO_OK);
      check_int_eq(turbo_flow_register_stage_ex(flow, "validate", domain_noop_stage, NULL, NULL),
                   TURBO_OK);
      check_int_eq(turbo_flow_parse_string(flow, dsl, strlen(dsl)), TURBO_OK);
      check_int_eq(turbo_flow_compile(flow), TURBO_OK);

      source_plan = turbo_flow_stage_at(flow, (size_t)turbo_flow_find_stage(flow, "ingress"));
      check_not_null(source_plan);
      check_str_eq(source_plan->operation_name, "mqtt.publish_in");
      check_str_eq(source_plan->resource_name, "mqtt.session");
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
      check_int_eq(turbo_flow_register_primitive(flow, &queue), TURBO_OK);
      check_int_eq(turbo_flow_register_operation(flow, &input), TURBO_OK);
      check_int_eq(turbo_flow_register_operation(flow, &validate), TURBO_OK);
      check_int_eq(turbo_flow_register_stage_ex(flow, "validate", domain_noop_stage, NULL, NULL),
                   TURBO_OK);
      check_int_eq(turbo_flow_parse_string(flow, dsl, strlen(dsl)), TURBO_OK);
      check_int_eq(turbo_flow_compile(flow), TURBO_EINVAL);
      check_str_contains(turbo_flow_last_error(flow)->message,
                         "resource primitive does not satisfy");
      turbo_flow_destroy(flow);
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
      check_int_eq(turbo_flow_register_primitive(flow, &session), TURBO_OK);
      check_int_eq(turbo_flow_register_operation(flow, &input), TURBO_OK);
      check_int_eq(turbo_flow_register_operation(flow, &validate), TURBO_OK);
      check_int_eq(turbo_flow_register_stage_ex(flow, "validate", domain_noop_stage, NULL, NULL),
                   TURBO_OK);
      check_int_eq(turbo_flow_parse_string(flow, dsl, strlen(dsl)), TURBO_OK);
      check_int_eq(turbo_flow_compile(flow), TURBO_EINVAL);
      check_str_contains(turbo_flow_last_error(flow)->message, "requires an adapter owner");
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
      check_int_eq(turbo_flow_register_operation(flow, &input), TURBO_OK);
      check_int_eq(turbo_flow_register_operation(flow, &validate), TURBO_OK);
      check_int_eq(turbo_flow_register_stage_ex(flow, "validate", domain_noop_stage, NULL, NULL),
                   TURBO_OK);
      check_int_eq(turbo_flow_parse_string(flow, dsl, strlen(dsl)), TURBO_OK);
      check_int_eq(turbo_flow_compile(flow), TURBO_EINVAL);
      check_str_contains(turbo_flow_last_error(flow)->message, "domain or type is incompatible");
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
      check_int_eq(turbo_flow_register_operation(management_flow, &command), TURBO_OK);
      check_int_eq(
          turbo_flow_register_stage_ex(management_flow, "command", domain_noop_stage, NULL, NULL),
          TURBO_OK);
      check_int_eq(turbo_flow_parse_string(management_flow, management_dsl, strlen(management_dsl)),
                   TURBO_OK);
      check_int_eq(turbo_flow_compile(management_flow), TURBO_EINVAL);
      check_str_contains(turbo_flow_last_error(management_flow)->message, "management command");

      check_int_eq(turbo_flow_register_operation(worker_flow, &validate), TURBO_OK);
      check_int_eq(turbo_flow_register_stage_ex(worker_flow, "work", domain_noop_stage, NULL, NULL),
                   TURBO_OK);
      check_int_eq(turbo_flow_parse_string(worker_flow, worker_dsl, strlen(worker_dsl)), TURBO_OK);
      check_int_eq(turbo_flow_compile(worker_flow), TURBO_EINVAL);
      check_str_contains(turbo_flow_last_error(worker_flow)->message, "worker segment");
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
      check_int_eq(turbo_flow_register_operation(flow, &operation), TURBO_OK);
      check_int_eq(turbo_flow_register_stage_ex(flow, "work", domain_noop_stage, NULL, NULL),
                   TURBO_OK);
      check_int_eq(turbo_flow_parse_string(flow, dsl, strlen(dsl)), TURBO_OK);
      check_int_eq(turbo_flow_compile(flow), TURBO_EINVAL);
      check_str_contains(turbo_flow_last_error(flow)->message, "pool-scoped operation");
      turbo_flow_destroy(flow);
    }
  }

  group("Bounded emitting operations") {
    static const char *linear_dsl = "source input operation data.input\n"
                                    "stage expand operation data.expand\n"
                                    "stage sink\n"
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
        check_int_eq(register_emitting_graph(flow, &probe, 3u, linear_dsl), TURBO_OK);
        check_int_eq(turbo_flow_compile(flow), TURBO_OK);
        check_int_eq(turbo_flow_start(flow), TURBO_OK);
        turbo_flow_msg_init(&message);
        message.id = 7u;
        check_int_eq(turbo_flow_publish(flow, "input", &message), TURBO_OK);
        check_int_eq(probe.emitter_called, 1);
        check_uint_eq(probe.sink_count, output_counts[scenario]);
        for (uint32_t i = 0u; i < probe.sink_count; ++i) {
          check_uint_eq(probe.sink_ids[i], 70u + i);
        }
        turbo_flow_msg_cleanup(&message);
        check_int_eq(turbo_flow_stop(flow), TURBO_OK);
        turbo_flow_destroy(flow);
      }
    }

    it("does not commit collected outputs when the callback fails") {
      turbo_flow_t *flow = turbo_flow_create();
      emission_probe_t probe;
      turbo_flow_msg_t message;

      memset(&probe, 0, sizeof(probe));
      probe.output_count = 2u;
      probe.callback_status = TURBO_EIO;
      check_not_null(flow);
      check_int_eq(register_emitting_graph(flow, &probe, 2u, linear_dsl), TURBO_OK);
      check_int_eq(turbo_flow_compile(flow), TURBO_OK);
      check_int_eq(turbo_flow_start(flow), TURBO_OK);
      turbo_flow_msg_init(&message);
      check_int_eq(turbo_flow_publish(flow, "input", &message), TURBO_EIO);
      check_uint_eq(probe.sink_count, 0u);
      turbo_flow_msg_cleanup(&message);
      check_int_eq(turbo_flow_stop(flow), TURBO_OK);
      turbo_flow_destroy(flow);
    }

    it("fails the whole batch when the output bound is exceeded") {
      turbo_flow_t *flow = turbo_flow_create();
      emission_probe_t probe;
      turbo_flow_msg_t message;

      memset(&probe, 0, sizeof(probe));
      probe.output_count = 3u;
      check_not_null(flow);
      check_int_eq(register_emitting_graph(flow, &probe, 2u, linear_dsl), TURBO_OK);
      check_int_eq(turbo_flow_compile(flow), TURBO_OK);
      check_int_eq(turbo_flow_start(flow), TURBO_OK);
      turbo_flow_msg_init(&message);
      check_int_eq(turbo_flow_publish(flow, "input", &message), TURBO_ENOSPC);
      check_uint_eq(probe.sink_count, 0u);
      turbo_flow_msg_cleanup(&message);
      check_int_eq(turbo_flow_stop(flow), TURBO_OK);
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
      check_int_eq(register_emitting_graph(flow, &probe, 1u, linear_dsl), TURBO_OK);
      check_int_eq(turbo_flow_compile(flow), TURBO_OK);
      check_int_eq(turbo_flow_start(flow), TURBO_OK);
      turbo_flow_msg_init(&message);
      check_int_eq(turbo_flow_publish(flow, "input", &message), TURBO_ENOTSUP);
      check_uint_eq(probe.sink_count, 0u);
      turbo_flow_msg_cleanup(&message);
      check_int_eq(turbo_flow_stop(flow), TURBO_OK);
      turbo_flow_destroy(flow);
    }

    it("rejects downstream fan-in from outside the emitted subtree") {
      static const char *dsl = "source input operation data.input\n"
                               "stage expand operation data.expand\n"
                               "stage other\n"
                               "stage sink\n"
                               "stage main {\n"
                               "  input -> expand -> sink\n"
                               "  input -> other -> sink\n"
                               "}\n";
      turbo_flow_t *flow = turbo_flow_create();
      emission_probe_t probe;

      memset(&probe, 0, sizeof(probe));
      check_not_null(flow);
      check_int_eq(register_emitting_graph(flow, &probe, 1u, dsl), TURBO_OK);
      check_int_eq(turbo_flow_register_stage_ex(flow, "other", domain_noop_stage, NULL, NULL),
                   TURBO_OK);
      check_int_eq(turbo_flow_compile(flow), TURBO_ENOTSUP);
      check_str_contains(turbo_flow_last_error(flow)->message, "external branch");
      turbo_flow_destroy(flow);
    }
  }

  group("Runtime contract lowering") {
    it("resolves a complete core operation contract for every runtime node") {
      static const char *dsl = "source input\n"
                               "stage inline_stage\n"
                               "stage thread_stage exec thread workers 1\n"
                               "stage coro_stage exec coro lanes 1 pool 2\n"
                               "stage worker_stage worker 2 capacity 16\n"
                               "stage main {\n"
                               "  input -> inline_stage -> thread_stage -> coro_stage -> worker_stage\n"
                               "}\n";
      const char *stage_names[] = {"input", "inline_stage", "thread_stage", "coro_stage",
                                   "worker_stage"};
      const char *operation_names[] = {"core.source", "core.stage.inline", "core.stage.thread",
                                       "core.stage.coro", "core.stage.worker"};
      turbo_flow_t *flow = turbo_flow_create();

      check_not_null(flow);
      for (size_t i = 1u; i < sizeof(stage_names) / sizeof(stage_names[0]); ++i) {
        check_int_eq(turbo_flow_register_stage_ex(flow, stage_names[i], domain_noop_stage, NULL,
                                                  NULL),
                     TURBO_OK);
      }
      check_int_eq(turbo_flow_parse_string(flow, dsl, strlen(dsl)), TURBO_OK);
      check_int_eq(turbo_flow_compile(flow), TURBO_OK);
      for (size_t i = 0u; i < sizeof(stage_names) / sizeof(stage_names[0]); ++i) {
        int stage_index = turbo_flow_find_stage(flow, stage_names[i]);
        const turbo_flow_stage_plan_t *stage;
        const turbo_flow_operation_descriptor_t *operation;
        check_int_ge(stage_index, 0);
        stage = turbo_flow_stage_at(flow, (size_t)stage_index);
        operation = turbo_flow_stage_operation_at(flow, (size_t)stage_index);
        check_not_null(stage);
        check_not_null(operation);
        check_str_eq(stage->operation_name, operation_names[i]);
        check_str_eq(operation->name, operation_names[i]);
        check_int_eq(operation->domain, TURBO_FLOW_DOMAIN_DATA);
        check_str_eq(operation->output_type, "Message");
      }
      {
        int worker_index = turbo_flow_find_stage(flow, "worker_stage");
        const turbo_flow_operation_descriptor_t *worker =
            turbo_flow_stage_operation_at(flow, (size_t)worker_index);
        check_int_eq(worker->runtime.handoff, TURBO_FLOW_HANDOFF_BOUNDED);
        check_int_eq(worker->runtime.backpressure, TURBO_FLOW_BACKPRESSURE_BLOCK);
        check_uint_eq(worker->runtime.capacity, 16u);
        check_int_eq(worker->scope.concurrency, TURBO_FLOW_CONCURRENCY_POOL);
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
                               "  step sink\n"
                               "  use pass = passthrough\n"
                               "  input -> pass -> sink\n"
                               "}\n";
      turbo_flow_t *flow = turbo_flow_create();
      int input_port;
      int output_port;
      const turbo_flow_operation_descriptor_t *input_operation;
      const turbo_flow_operation_descriptor_t *output_operation;

      check_not_null(flow);
      check_int_eq(turbo_flow_register_stage_ex(flow, "sink", domain_noop_stage, NULL, NULL),
                   TURBO_OK);
      check_int_eq(turbo_flow_parse_string(flow, dsl, strlen(dsl)), TURBO_OK);
      check_int_eq(turbo_flow_compile(flow), TURBO_OK);
      input_port = turbo_flow_find_stage(flow, "pass.value");
      output_port = turbo_flow_find_stage(flow, "pass.result");
      check_int_ge(input_port, 0);
      check_int_ge(output_port, 0);
      input_operation = turbo_flow_stage_operation_at(flow, (size_t)input_port);
      output_operation = turbo_flow_stage_operation_at(flow, (size_t)output_port);
      check_not_null(input_operation);
      check_not_null(output_operation);
      check_str_eq(input_operation->name, "core.port.input");
      check_str_eq(output_operation->name, "core.port.output");
      check_str_eq(input_operation->input_type, "Message");
      check_str_eq(output_operation->output_type, "Message");
      check_int_eq(input_operation->scope.authority, TURBO_FLOW_AUTHORITY_PURE);
      check_int_eq(output_operation->scope.authority, TURBO_FLOW_AUTHORITY_PURE);
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
      check_int_eq(turbo_flow_register_operation(flow, &input), TURBO_OK);
      check_int_eq(turbo_flow_register_operation(flow, &work), TURBO_OK);
      check_int_eq(turbo_flow_register_stage_ex(flow, "work", domain_noop_stage, NULL, NULL),
                   TURBO_OK);
      check_int_eq(turbo_flow_parse_string(flow, dsl, strlen(dsl)), TURBO_OK);
      check_int_eq(turbo_flow_compile(flow), TURBO_OK);

      for (size_t i = 0; i < turbo_flow_segment_count(flow); ++i) {
        check_int_eq(turbo_flow_segment_plan_at(flow, i, &segment), TURBO_OK);
        if (segment.kind != TURBO_FLOW_SEGMENT_WORKER_POOL) continue;
        found_worker = 1;
        check_uint_eq(segment.width, 2u);
        check_uint_eq(segment.capacity, 64u);
        check_int_eq(segment.operation.handoff, TURBO_FLOW_HANDOFF_BOUNDED);
        check_int_eq(segment.operation.backpressure, TURBO_FLOW_BACKPRESSURE_BLOCK);
        check_uint_eq(segment.operation.capacity, 64u);
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
      check_int_eq(turbo_flow_register_operation(flow, &input), TURBO_OK);
      check_int_eq(turbo_flow_register_operation(flow, &work), TURBO_OK);
      check_int_eq(turbo_flow_register_stage_ex(flow, "work", domain_noop_stage, NULL, NULL),
                   TURBO_OK);
      check_int_eq(turbo_flow_parse_string(flow, dsl, strlen(dsl)), TURBO_OK);
      check_int_eq(turbo_flow_compile(flow), TURBO_EINVAL);
      check_str_contains(turbo_flow_last_error(flow)->message, "does not match worker capacity");
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
      check_int_eq(turbo_flow_register_operation(flow, &input), TURBO_OK);
      check_int_eq(turbo_flow_register_operation(flow, &work), TURBO_OK);
      check_int_eq(turbo_flow_register_stage_ex(flow, "work", domain_noop_stage, NULL, NULL),
                   TURBO_OK);
      check_int_eq(turbo_flow_parse_string(flow, dsl, strlen(dsl)), TURBO_OK);
      check_int_eq(turbo_flow_compile(flow), TURBO_OK);

      turbo_flow_destroy(flow);
      flow = turbo_flow_create();
      work.runtime.backpressure = TURBO_FLOW_BACKPRESSURE_DROP_NEWEST;
      check_not_null(flow);
      check_int_eq(turbo_flow_register_operation(flow, &input), TURBO_OK);
      check_int_eq(turbo_flow_register_operation(flow, &work), TURBO_OK);
      check_int_eq(turbo_flow_register_stage_ex(flow, "work", domain_noop_stage, NULL, NULL),
                   TURBO_OK);
      check_int_eq(turbo_flow_parse_string(flow, dsl, strlen(dsl)), TURBO_OK);
      check_int_eq(turbo_flow_compile(flow), TURBO_OK);
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
      check_int_eq(turbo_flow_register_operation(flow, &input), TURBO_OK);
      check_int_eq(turbo_flow_register_operation(flow, &work), TURBO_OK);
      check_int_eq(turbo_flow_register_stage_ex(flow, "work", domain_noop_stage, NULL, NULL),
                   TURBO_OK);
      check_int_eq(turbo_flow_parse_string(flow, dsl, strlen(dsl)), TURBO_OK);
      check_int_eq(turbo_flow_compile(flow), TURBO_ENOTSUP);
      check_str_contains(turbo_flow_last_error(flow)->message, "cannot drop an older entry");
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
      const int expected[] = {TURBO_ENOSPC, TURBO_ECANCELED};

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
        turbo_thread_t threads[DOMAIN_ADMISSION_PUBLISHER_COUNT] = {NULL};
        turbo_flow_pool_snapshot_t snapshot;
        turbo_flow_msg_t overflow;
        int snapshot_status;
        uint64_t wait_deadline;

        require_worker_handoff(&work, DOMAIN_ADMISSION_CAPACITY);
        work.runtime.backpressure = policies[policy_index];
        atomic_init(&probe.entered, 0);
        atomic_init(&probe.release, 0);
        check_not_null(flow);
        check_int_eq(turbo_flow_register_operation(flow, &input), TURBO_OK);
        check_int_eq(turbo_flow_register_operation(flow, &work), TURBO_OK);
        check_int_eq(turbo_flow_register_stage_ex(flow, "work", domain_blocking_stage, &probe,
                                                  NULL),
                     TURBO_OK);
        check_int_eq(turbo_flow_parse_string(flow, dsl, strlen(dsl)), TURBO_OK);
        check_int_eq(turbo_flow_compile(flow), TURBO_OK);
        check_int_eq(turbo_flow_start(flow), TURBO_OK);

        for (size_t i = 0; i < DOMAIN_ADMISSION_PUBLISHER_COUNT; ++i) {
          publishes[i].flow = flow;
          atomic_init(&publishes[i].result, TURBO_EBUSY);
          check_int_eq(turbo_thread_create(&threads[i], domain_publish_thread, &publishes[i]),
                       TURBO_OK);
          if (i == 0u) {
            while (atomic_load_explicit(&probe.entered, memory_order_acquire) != 1)
              turbo_thread_yield();
          }
        }
        wait_deadline = turbo_hrtime() + UINT64_C(1000000000);
        do {
          snapshot_status = turbo_flow_pool_snapshot_at(flow, 0u, &snapshot);
          if (snapshot_status != TURBO_OK) break;
          if (snapshot.queued != DOMAIN_ADMISSION_EXPECTED_QUEUED) turbo_thread_yield();
        } while (snapshot.queued != DOMAIN_ADMISSION_EXPECTED_QUEUED &&
                 turbo_hrtime() < wait_deadline);
        check_int_eq(snapshot_status, TURBO_OK);
        check_uint_eq(snapshot.queued, DOMAIN_ADMISSION_EXPECTED_QUEUED);

        turbo_flow_msg_init(&overflow);
        check_int_eq(turbo_flow_publish(flow, "input", &overflow), expected[policy_index]);
        turbo_flow_msg_cleanup(&overflow);
        atomic_store_explicit(&probe.release, 1, memory_order_release);
        for (size_t i = 0; i < DOMAIN_ADMISSION_PUBLISHER_COUNT; ++i) {
          check_int_eq(turbo_thread_join(&threads[i]), TURBO_OK);
          check_int_eq(atomic_load_explicit(&publishes[i].result, memory_order_acquire), TURBO_OK);
        }
        check_int_eq(atomic_load_explicit(&probe.entered, memory_order_acquire),
                     DOMAIN_ADMISSION_PUBLISHER_COUNT);
        check_int_eq(turbo_flow_pool_snapshot_at(flow, 0u, &snapshot), TURBO_OK);
        check_uint_eq(snapshot.submitted, DOMAIN_ADMISSION_TOTAL_SUBMISSIONS);
        check_uint_eq(snapshot.started, DOMAIN_ADMISSION_PUBLISHER_COUNT);
        check_uint_eq(snapshot.completed, DOMAIN_ADMISSION_PUBLISHER_COUNT);
        check_uint_eq(snapshot.rejected, policy_index == 0u ? 1u : 0u);
        check_uint_eq(snapshot.canceled, policy_index == 1u ? 1u : 0u);
        check_uint_eq(snapshot.queued, 0u);
        check_uint_eq(snapshot.active, 0u);
        check_int_eq(turbo_flow_stop(flow), TURBO_OK);
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
      check_int_eq(turbo_flow_register_operation(flow, &input), TURBO_OK);
      check_int_eq(turbo_flow_register_operation(flow, &work), TURBO_OK);
      check_int_eq(turbo_flow_register_stage_ex(flow, "work", domain_noop_stage, NULL, NULL),
                   TURBO_OK);
      check_int_eq(turbo_flow_parse_string(flow, dsl, strlen(dsl)), TURBO_OK);
      check_int_eq(turbo_flow_compile(flow), TURBO_EINVAL);
      check_str_contains(turbo_flow_last_error(flow)->message, "requires a reorder boundary");
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
      check_int_eq(turbo_flow_register_operation(flow, &input), TURBO_OK);
      check_int_eq(turbo_flow_register_operation(flow, &work), TURBO_OK);
      check_int_eq(turbo_flow_register_stage_ex(flow, "work", domain_noop_stage, NULL, NULL),
                   TURBO_OK);
      check_int_eq(turbo_flow_parse_string(flow, dsl, strlen(dsl)), TURBO_OK);
      check_int_eq(turbo_flow_compile(flow), TURBO_OK);
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
      check_int_eq(turbo_flow_register_operation(flow, &input), TURBO_OK);
      check_int_eq(turbo_flow_register_operation(flow, &work), TURBO_OK);
      check_int_eq(turbo_flow_register_stage_ex(flow, "work", domain_noop_stage, NULL, NULL),
                   TURBO_OK);
      check_int_eq(turbo_flow_parse_string(flow, dsl, strlen(dsl)), TURBO_OK);
      check_int_eq(turbo_flow_compile(flow), TURBO_OK);
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
      check_int_eq(turbo_flow_register_operation(flow, &input), TURBO_OK);
      check_int_eq(turbo_flow_parse_string(flow, dsl, strlen(dsl)), TURBO_OK);
      check_int_eq(turbo_flow_compile(flow), TURBO_ENOTSUP);
      check_str_contains(turbo_flow_last_error(flow)->message, "source operation deadline");
      turbo_flow_destroy(flow);
    }

    it("reports inline operation timeout after callback return and stops downstream") {
      static const char *dsl = "source input operation data.input\n"
                               "stage work operation data.validate\n"
                               "stage sink\n"
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
      check_int_eq(turbo_flow_register_operation(flow, &input), TURBO_OK);
      check_int_eq(turbo_flow_register_operation(flow, &work), TURBO_OK);
      check_int_eq(turbo_flow_register_stage_ex(flow, "work", domain_slow_inline_stage, &probe,
                                                NULL),
                   TURBO_OK);
      check_int_eq(turbo_flow_register_stage_ex(flow, "sink", domain_count_stage, &sink_called,
                                                NULL),
                   TURBO_OK);
      check_int_eq(turbo_flow_parse_string(flow, dsl, strlen(dsl)), TURBO_OK);
      check_int_eq(turbo_flow_compile(flow), TURBO_OK);
      check_int_eq(turbo_flow_start(flow), TURBO_OK);
      check_int_eq(turbo_flow_publish(flow, "input", &message), TURBO_ETIMEDOUT);
      check_int_eq(probe.called, 1);
      check_int_eq(sink_called, 0);
      check_int_eq(turbo_flow_stop(flow), TURBO_OK);
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
        check_int_eq(turbo_flow_register_operation(flow, &input), TURBO_OK);
        check_int_eq(turbo_flow_register_operation(flow, &work), TURBO_OK);
        check_int_eq(turbo_flow_register_stage_ex(flow, "work", domain_deadline_yield_stage,
                                                  &probe, NULL),
                     TURBO_OK);
        check_int_eq(turbo_flow_parse_string(flow, dsls[i], strlen(dsls[i])), TURBO_OK);
        check_int_eq(turbo_flow_compile(flow), TURBO_OK);
        check_int_eq(turbo_flow_start(flow), TURBO_OK);
        check_int_eq(turbo_flow_publish(flow, "input", &message), TURBO_ETIMEDOUT);
        check_int_eq(probe.called, 1);
        check_int_eq(probe.terminal_status, TURBO_ETIMEDOUT);
        check_int_eq(turbo_flow_stop(flow), TURBO_OK);
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
      check_int_eq(turbo_flow_register_operation(flow, &input), TURBO_OK);
      check_int_eq(turbo_flow_register_operation(flow, &work), TURBO_OK);
      check_int_eq(turbo_flow_register_stage_ex(flow, "work", domain_noop_stage, NULL, NULL),
                   TURBO_OK);
      check_int_eq(turbo_flow_parse_string(flow, dsl, strlen(dsl)), TURBO_OK);
      check_int_eq(turbo_flow_compile(flow), TURBO_EINVAL);
      check_str_contains(turbo_flow_last_error(flow)->message, "requires a reject edge");
      turbo_flow_destroy(flow);
    }

    it("accepts reject error mode when the stage owns a reject edge") {
      static const char *dsl = "source input operation data.input\n"
                               "stage work operation data.validate\n"
                               "stage rejected\n"
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
      check_int_eq(turbo_flow_register_operation(flow, &input), TURBO_OK);
      check_int_eq(turbo_flow_register_operation(flow, &work), TURBO_OK);
      check_int_eq(turbo_flow_register_stage_ex(flow, "work", domain_noop_stage, NULL, NULL),
                   TURBO_OK);
      check_int_eq(turbo_flow_register_stage_ex(flow, "rejected", domain_noop_stage, NULL, NULL),
                   TURBO_OK);
      check_int_eq(turbo_flow_parse_string(flow, dsl, strlen(dsl)), TURBO_OK);
      check_int_eq(turbo_flow_compile(flow), TURBO_OK);
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
      check_int_eq(turbo_flow_register_operation(flow, &input), TURBO_OK);
      check_int_eq(turbo_flow_register_operation(flow, &work), TURBO_OK);
      check_int_eq(turbo_flow_register_stage_ex(flow, "work", domain_noop_stage, NULL, NULL),
                   TURBO_OK);
      check_int_eq(turbo_flow_parse_string(flow, dsl, strlen(dsl)), TURBO_OK);
      check_int_eq(turbo_flow_compile(flow), TURBO_EINVAL);
      check_str_contains(turbo_flow_last_error(flow)->message, "requires a retry policy");
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
      check_int_eq(turbo_flow_register_adapter(flow, "retryable", &adapter_ops, NULL), TURBO_OK);
      check_int_eq(turbo_flow_register_operation(flow, &input), TURBO_OK);
      check_int_eq(turbo_flow_register_operation(flow, &work), TURBO_OK);
      check_int_eq(turbo_flow_parse_string(flow, dsl, strlen(dsl)), TURBO_OK);
      check_int_eq(turbo_flow_compile(flow), TURBO_OK);
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
      work.runtime.settlement = TURBO_FLOW_SETTLEMENT_PROTOCOL_ACK;
      check_not_null(flow);
      check_int_eq(turbo_flow_register_adapter(flow, "owner", &adapter_ops, NULL), TURBO_OK);
      check_int_eq(turbo_flow_register_operation(flow, &input), TURBO_OK);
      check_int_eq(turbo_flow_register_operation(flow, &work), TURBO_OK);
      check_int_eq(turbo_flow_register_stage_ex(flow, "work", domain_noop_stage, NULL, NULL),
                   TURBO_OK);
      check_int_eq(turbo_flow_parse_string(flow, dsl, strlen(dsl)), TURBO_OK);
      check_int_eq(turbo_flow_compile(flow), TURBO_ENOTSUP);
      check_str_contains(turbo_flow_last_error(flow)->message, "settlement owner");
      turbo_flow_destroy(flow);
    }

    it("delivers automatic complete and explicit protocol ACK to the owner") {
      static const char *dsl = "source input operation data.input\n"
                               "stage work adapter owner operation data.validate\n"
                               "stage sink\n"
                               "stage main {\n"
                               "  input -> work -> sink\n"
                               "}\n";
      const turbo_flow_settlement_action_t actions[] = {
          TURBO_FLOW_SETTLEMENT_ACTION_COMPLETE, TURBO_FLOW_SETTLEMENT_ACTION_PROTOCOL_ACK};

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
                                                     : TURBO_FLOW_SETTLEMENT_PROTOCOL_ACK;
        probe.report_action = actions[action_index];
        probe.report_status = TURBO_OK;
        check_not_null(flow);
        check_int_eq(turbo_flow_register_adapter(flow, "owner", &adapter_ops, &probe), TURBO_OK);
        check_int_eq(turbo_flow_register_adapter_settlement(flow, "owner", &owner_ops, &probe),
                     TURBO_OK);
        check_int_eq(turbo_flow_register_operation(flow, &input), TURBO_OK);
        check_int_eq(turbo_flow_register_operation(flow, &work), TURBO_OK);
        if (action_index == 0u) {
          check_int_eq(turbo_flow_register_stage_ex(flow, "work", domain_noop_stage, NULL, NULL),
                       TURBO_OK);
        }
        check_int_eq(turbo_flow_register_stage_ex(flow, "sink", domain_count_stage, &sink_called,
                                                  NULL),
                     TURBO_OK);
        check_int_eq(turbo_flow_parse_string(flow, dsl, strlen(dsl)), TURBO_OK);
        check_int_eq(turbo_flow_compile(flow), TURBO_OK);
        check_int_eq(turbo_flow_start(flow), TURBO_OK);
        turbo_flow_msg_init(&message);
        message.id = 42u + action_index;
        check_int_eq(turbo_flow_publish(flow, "input", &message), TURBO_OK);
        check_int_eq(probe.owner_called, 1);
        check_int_eq(probe.observed.action, actions[action_index]);
        check_uint_eq(probe.observed.message_id, message.id);
        check_uint_eq(probe.observed.attempt, 1u);
        check_int_eq(sink_called, 1);
        turbo_flow_msg_cleanup(&message);
        check_int_eq(turbo_flow_stop(flow), TURBO_OK);
        turbo_flow_destroy(flow);
      }
    }

    it("delivers terminal settlement actions without releasing normal downstream") {
      static const char *dsl = "source input operation data.input\n"
                               "stage work adapter owner operation data.validate\n"
                               "stage sink\n"
                               "stage main {\n"
                               "  input -> work -> sink\n"
                               "}\n";
      const turbo_flow_settlement_action_t actions[] = {
          TURBO_FLOW_SETTLEMENT_ACTION_REQUEUE, TURBO_FLOW_SETTLEMENT_ACTION_DEAD_LETTER,
          TURBO_FLOW_SETTLEMENT_ACTION_CANCELED};
      const int statuses[] = {TURBO_EBUSY, TURBO_EIO, TURBO_ECANCELED};
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
      check_int_eq(turbo_flow_register_adapter(flow, "owner", &adapter_ops, &probe), TURBO_OK);
      check_int_eq(turbo_flow_register_adapter_settlement(flow, "owner", &owner_ops, &probe),
                   TURBO_OK);
      check_int_eq(turbo_flow_register_operation(flow, &input), TURBO_OK);
      check_int_eq(turbo_flow_register_operation(flow, &work), TURBO_OK);
      check_int_eq(turbo_flow_register_stage_ex(flow, "sink", domain_count_stage, &sink_called,
                                                NULL),
                   TURBO_OK);
      check_int_eq(turbo_flow_parse_string(flow, dsl, strlen(dsl)), TURBO_OK);
      check_int_eq(turbo_flow_compile(flow), TURBO_OK);
      check_int_eq(turbo_flow_start(flow), TURBO_OK);
      for (size_t i = 0; i < 3u; ++i) {
        turbo_flow_msg_t message;
        probe.report_action = actions[i];
        probe.report_status = statuses[i];
        turbo_flow_msg_init(&message);
        check_int_eq(turbo_flow_publish(flow, "input", &message), TURBO_OK);
        turbo_flow_msg_cleanup(&message);
        check_int_eq(probe.stage_called, (int)i + 1);
        check_int_eq(probe.owner_called, (int)i + 1);
        check_int_eq(probe.observed.action, actions[i]);
        check_int_eq(probe.observed.status, statuses[i]);
      }
      check_int_eq(probe.owner_called, 3);
      check_int_eq(sink_called, 0);
      check_int_eq(turbo_flow_stop(flow), TURBO_OK);
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
        probe.report_status = TURBO_EBUSY;
        if (path == 1u || path == 2u || path == 3u) {
          work.scope.concurrency = TURBO_FLOW_CONCURRENCY_POOL;
        }
        if (path == 3u) require_worker_handoff(&work, 8u);
        check_not_null(flow);
        check_int_eq(turbo_flow_register_adapter(flow, "owner", &adapter_ops, NULL), TURBO_OK);
        check_int_eq(turbo_flow_register_adapter_settlement(flow, "owner", &owner_ops, &probe),
                     TURBO_OK);
        check_int_eq(turbo_flow_register_operation(flow, &input), TURBO_OK);
        check_int_eq(turbo_flow_register_operation(flow, &work), TURBO_OK);
        check_int_eq(turbo_flow_register_stage_ex(flow, "work", domain_settlement_stage, &probe,
                                                  NULL),
                     TURBO_OK);
        check_int_eq(turbo_flow_parse_string(flow, dsls[path], strlen(dsls[path])), TURBO_OK);
        check_int_eq(turbo_flow_compile(flow), TURBO_OK);
        check_int_eq(turbo_flow_start(flow), TURBO_OK);
        turbo_flow_msg_init(&message);
        check_int_eq(turbo_flow_publish(flow, "input", &message), TURBO_OK);
        check_int_eq(probe.stage_called, 1);
        check_int_eq(probe.owner_called, 1);
        check_int_eq(probe.observed.action, TURBO_FLOW_SETTLEMENT_ACTION_REQUEUE);
        turbo_flow_msg_cleanup(&message);
        check_int_eq(turbo_flow_stop(flow), TURBO_OK);
        turbo_flow_destroy(flow);
      }
    }

    it("rejects duplicate unauthorized and owner-failed settlement results") {
      static const char *dsl = "source input operation data.input\n"
                               "stage work adapter owner operation data.validate\n"
                               "stage main {\n"
                               "  input -> work\n"
                               "}\n";
      const int expected[] = {TURBO_EALREADY, TURBO_EPROTO, TURBO_EIO};

      check_int_eq(turbo_flow_settlement_report(NULL), TURBO_EINVAL);
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
        probe.report_status = TURBO_EBUSY;
        probe.report_twice = scenario == 0u;
        probe.owner_status = scenario == 2u ? TURBO_EIO : TURBO_OK;
        check_not_null(flow);
        check_int_eq(turbo_flow_register_adapter(flow, "owner", &adapter_ops, &probe), TURBO_OK);
        check_int_eq(turbo_flow_register_adapter_settlement(flow, "owner", &owner_ops, &probe),
                     TURBO_OK);
        check_int_eq(turbo_flow_register_operation(flow, &input), TURBO_OK);
        check_int_eq(turbo_flow_register_operation(flow, &work), TURBO_OK);
        check_int_eq(turbo_flow_parse_string(flow, dsl, strlen(dsl)), TURBO_OK);
        check_int_eq(turbo_flow_compile(flow), TURBO_OK);
        check_int_eq(turbo_flow_start(flow), TURBO_OK);
        turbo_flow_msg_init(&message);
        check_int_eq(turbo_flow_publish(flow, "input", &message), expected[scenario]);
        check_int_eq(probe.owner_called, scenario == 2u ? 1 : 0);
        turbo_flow_msg_cleanup(&message);
        check_int_eq(turbo_flow_stop(flow), TURBO_OK);
        turbo_flow_destroy(flow);
      }
    }
  }
}
