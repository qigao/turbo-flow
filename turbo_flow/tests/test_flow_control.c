#include "tinytest.h"
#include "turbo_flow.h"
#include "turbo_flow_control.h"

#include <string.h>

typedef struct control_adapter_s {
  int commands;
  int resource_commands;
  int result;
  turbo_flow_resource_command_kind_t last_resource_command;
  uint64_t generation;
} control_adapter_t;

static int control_noop_stage(void *ctx, turbo_flow_msg_t *msg) {
  (void)ctx;
  (void)msg;
  return TURBO_OK;
}

static int control_adapter_snapshot(void *ctx, turbo_flow_connection_snapshot_t *out) {
  (void)ctx;
  out->state = TURBO_FLOW_CONNECTION_READY;
  out->connections_current = 2u;
  out->last_status = TURBO_OK;
  return TURBO_OK;
}

static int control_adapter_command(void *ctx, turbo_flow_t *flow,
                                   const turbo_flow_adapter_command_t *command) {
  control_adapter_t *adapter = (control_adapter_t *)ctx;
  (void)flow;
  (void)command;
  adapter->commands++;
  return adapter->result;
}

static int control_adapter_resource_metadata(void *ctx, turbo_flow_resource_metadata_t *out) {
  control_adapter_t *adapter = (control_adapter_t *)ctx;
  if (!adapter || !out || out->size < sizeof(*out)) return TURBO_EINVAL;
  *out = (turbo_flow_resource_metadata_t)TURBO_FLOW_RESOURCE_METADATA_INIT;
  out->domain = TURBO_FLOW_DOMAIN_IO_TRANSPORT;
  out->kind = TURBO_FLOW_RESOURCE_CONNECTION;
  memcpy(out->uid, "connection:mock", sizeof("connection:mock"));
  memcpy(out->owner_name, "mock", sizeof("mock"));
  out->generation = adapter->generation;
  out->observed_generation = adapter->generation;
  return TURBO_OK;
}

static int control_adapter_resource_command(void *ctx, turbo_flow_t *flow,
                                            const turbo_flow_resource_command_t *command) {
  control_adapter_t *adapter = (control_adapter_t *)ctx;
  (void)flow;
  if (!adapter || !command) return TURBO_EINVAL;
  adapter->resource_commands++;
  adapter->last_resource_command = command->kind;
  if (adapter->result != TURBO_OK) return adapter->result;
  if (adapter->generation == UINT64_MAX) return TURBO_ERANGE;
  adapter->generation++;
  return TURBO_OK;
}

static int control_external_read(void *ctx, uint32_t field_id, turbo_flow_expr_value_t *out) {
  int invalid_type = *(int *)ctx;
  (void)field_id;
  memset(out, 0, sizeof(*out));
  out->type = invalid_type ? TURBO_FLOW_EXPR_TYPE_STRING : TURBO_FLOW_EXPR_TYPE_F64;
  if (invalid_type) {
    out->as.string.data = "bad";
    out->as.string.len = 3u;
  } else {
    out->as.f64 = 0.90;
  }
  return TURBO_OK;
}

static turbo_flow_t *control_started_flow_ex(control_adapter_t *adapter,
                                             int register_stable_resource) {
  static const char source[] = "source input\n"
                               "stage main {\n"
                               "  step transform exec thread workers 2\n"
                               "  input -> transform\n"
                               "}\n";
  turbo_flow_adapter_ops_t ops;
  turbo_flow_adapter_schema_t schema;
  turbo_flow_resource_provider_registration_t resource =
      TURBO_FLOW_RESOURCE_PROVIDER_REGISTRATION_INIT;
  turbo_flow_t *flow = turbo_flow_create();
  memset(&ops, 0, sizeof(ops));
  memset(&schema, 0, sizeof(schema));
  ops.connection_snapshot = control_adapter_snapshot;
  ops.command = control_adapter_command;
  schema.roles = TURBO_FLOW_ADAPTER_SOURCE | TURBO_FLOW_ADAPTER_TRANSFORM;
  schema.kind = TURBO_FLOW_ADAPTER_KIND_CUSTOM;
  schema.direction = TURBO_FLOW_ADAPTER_BIDIRECTIONAL;
  if (adapter->generation == 0u) adapter->generation = 1u;
  check_not_null(flow);
  if (register_stable_resource) {
    resource.owner_name = "mock";
    resource.ops.metadata = control_adapter_resource_metadata;
    resource.ops.command = control_adapter_resource_command;
    resource.ctx = adapter;
    check_equal(turbo_flow_register_adapter_with_resources(flow, "mock", &ops, adapter, &schema,
                                                            &resource, 1u),
                 TURBO_OK);
  } else {
    check_equal(turbo_flow_register_adapter_ex(flow, "mock", &ops, adapter, &schema), TURBO_OK);
  }
  check_equal(turbo_flow_parse_string(flow, source, strlen(source)), TURBO_OK);
  check_equal(turbo_flow_register_stage_ex(flow, "transform", control_noop_stage, NULL, NULL),
               TURBO_OK);
  check_equal(turbo_flow_compile(flow), TURBO_OK);
  check_equal(turbo_flow_start(flow), TURBO_OK);
  return flow;
}

static turbo_flow_t *control_started_flow(control_adapter_t *adapter) {
  return control_started_flow_ex(adapter, 0);
}

spec("control_dsl") {
  it("parses unconditional and conditional typed commands") {
    turbo_flow_control_command_t command;
    turbo_flow_error_t error;
    const char *resize = "pool transform thread resize 4 timeout 5000";
    const char *conditional = "when pool.transform.thread.parallelism == 2 then flow pause";
    check_equal(turbo_flow_control_parse(resize, strlen(resize), &command, &error), TURBO_OK);
    check_equal(command.kind, TURBO_FLOW_CONTROL_RESIZE_POOL);
    check_equal(command.target, "transform");
    check_equal(command.parallelism, 4u);
    check_equal(command.timeout_ms, 5000u);
    check_equal(turbo_flow_control_parse(conditional, strlen(conditional), &command, &error),
                 TURBO_OK);
    check_equal(command.kind, TURBO_FLOW_CONTROL_PAUSE);
    check_equal(command.condition, "pool.transform.thread.parallelism == 2");
    check_equal(turbo_flow_control_parse("adapter host.path quiesce",
                                          sizeof("adapter host.path quiesce") - 1u, &command,
                                          &error),
                 TURBO_OK);
    check_equal(command.target, "host.path");
  }

  it("executes one action only when core snapshot facts match") {
    control_adapter_t adapter = {0};
    turbo_flow_t *flow = control_started_flow(&adapter);
    turbo_flow_error_t error;
    const char *false_rule = "if runtime.active_publishes > 0 then flow pause";
    const char *true_rule = "when pool.transform.thread.parallelism == 2 then adapter mock quiesce";
    check_equal(turbo_flow_control_ex(flow, false_rule, strlen(false_rule), NULL, &error),
                 TURBO_OK);
    check_equal(turbo_flow_control(flow, true_rule, strlen(true_rule)), TURBO_OK);
    check_equal(adapter.commands, 1);
    check_equal(turbo_flow_stop(flow), TURBO_OK);
    turbo_flow_destroy(flow);
  }

  it("dispatches stable adapter controls through the resource command owner") {
    control_adapter_t adapter = {0};
    turbo_flow_t *flow = control_started_flow_ex(&adapter, 1);
    const char *command = "adapter mock quiesce";
    check_equal(turbo_flow_control(flow, command, strlen(command)), TURBO_OK);
    check_equal(adapter.commands, 0);
    check_equal(adapter.resource_commands, 1);
    check_equal(adapter.last_resource_command, TURBO_FLOW_RESOURCE_COMMAND_QUIESCE);
    check_equal(adapter.generation, 2u);
    check_equal(turbo_flow_stop(flow), TURBO_OK);
    turbo_flow_destroy(flow);
  }

  it("dispatches pool resize through the resource command dispatcher") {
    control_adapter_t adapter = {0};
    turbo_flow_t *flow = control_started_flow(&adapter);
    turbo_flow_pool_snapshot_t pool;
    const char *command = "pool transform thread resize 3 timeout 5000";
    check_equal(turbo_flow_control(flow, command, strlen(command)), TURBO_OK);
    check_equal(turbo_flow_pool_snapshot_at(flow, 0u, &pool), TURBO_OK);
    check_equal(pool.kind, TURBO_FLOW_POOL_THREAD);
    check_equal(pool.parallelism, 3u);
    check_equal(turbo_flow_stop(flow), TURBO_OK);
    turbo_flow_destroy(flow);
  }

  it("fails fast for unknown facts and non-BOOL conditions") {
    control_adapter_t adapter = {0};
    turbo_flow_error_t error;
    turbo_flow_t *flow = control_started_flow(&adapter);
    const char *unknown = "if system.missing > 0 then flow pause";
    const char *wrong_type = "if runtime.stage_count then flow pause";
    check_equal(turbo_flow_control_ex(flow, unknown, strlen(unknown), NULL, &error), TURBO_ENOENT);
    check_contains(error.message, "unknown external field");
    check_equal(turbo_flow_control_ex(flow, wrong_type, strlen(wrong_type), NULL, &error),
                 TURBO_EINVAL);
    check_contains(error.message, "BOOL");
    check_equal(turbo_flow_stop(flow), TURBO_OK);
    turbo_flow_destroy(flow);
  }

  it("reads external typed facts and propagates provider type errors") {
    static const turbo_flow_expr_schema_field_t fields[] = {
        {"system.cpu.utilization", TURBO_FLOW_EXPR_TYPE_F64, 7u}};
    static const turbo_flow_expr_schema_t schema = {fields, 1u};
    control_adapter_t adapter = {0};
    turbo_flow_control_facts_t facts = TURBO_FLOW_CONTROL_FACTS_INIT;
    turbo_flow_error_t error;
    turbo_flow_t *flow = control_started_flow(&adapter);
    const char *rule = "when system.cpu.utilization > 0.8 then adapter mock quiesce";
    int invalid_type = 0;
    facts.schema = &schema;
    facts.read_field = control_external_read;
    facts.ctx = &invalid_type;
    check_equal(turbo_flow_control_ex(flow, rule, strlen(rule), &facts, &error), TURBO_OK);
    check_equal(adapter.commands, 1);
    invalid_type = 1;
    check_equal(turbo_flow_control_ex(flow, rule, strlen(rule), &facts, &error), TURBO_EPROTO);
    check_equal(adapter.commands, 1);
    check_equal(turbo_flow_stop(flow), TURBO_OK);
    turbo_flow_destroy(flow);
  }

  it("propagates typed command owner failures") {
    control_adapter_t adapter = {0};
    turbo_flow_error_t error;
    turbo_flow_t *flow = control_started_flow_ex(&adapter, 1);
    const char *command = "adapter mock resume";
    adapter.result = TURBO_EIO;
    check_equal(turbo_flow_control_ex(flow, command, strlen(command), NULL, &error), TURBO_EIO);
    check_equal(adapter.commands, 0);
    check_equal(adapter.resource_commands, 1);
    check_contains(error.message, "command failed");
    check_equal(turbo_flow_stop(flow), TURBO_OK);
    turbo_flow_destroy(flow);
  }

  it("rejects malformed caller-owned commands and fact schemas") {
    static const turbo_flow_expr_schema_t missing_fields = {NULL, 1u};
    control_adapter_t adapter = {0};
    turbo_flow_control_command_t command;
    turbo_flow_control_facts_t facts = TURBO_FLOW_CONTROL_FACTS_INIT;
    turbo_flow_error_t error;
    turbo_flow_t *flow = control_started_flow(&adapter);
    memset(&command, 0, sizeof(command));
    command.size = sizeof(command);
    command.kind = TURBO_FLOW_CONTROL_PAUSE;
    memset(command.condition, 'x', sizeof(command.condition));
    check_equal(turbo_flow_control_execute_ex(flow, &command, NULL, &error), TURBO_EINVAL);
    check_contains(error.message, "invalid");

    check_equal(turbo_flow_control_parse("when runtime.accepting then flow pause",
                                          sizeof("when runtime.accepting then flow pause") - 1u,
                                          &command, &error),
                 TURBO_OK);
    facts.schema = &missing_fields;
    facts.read_field = control_external_read;
    facts.ctx = &adapter.result;
    check_equal(turbo_flow_control_execute_ex(flow, &command, &facts, &error), TURBO_EINVAL);
    check_contains(error.message, "collect control facts");
    check_equal(turbo_flow_stop(flow), TURBO_OK);
    check_equal(turbo_flow_control_execute_ex(flow, &command, NULL, &error), TURBO_EINVAL);
    check_contains(error.message, "started flow");
    turbo_flow_destroy(flow);
  }
}
