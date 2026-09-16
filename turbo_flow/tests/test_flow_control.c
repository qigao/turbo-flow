#include "../../tests/flow_operation_fixture.h"
#include "tinytest.h"
#include "turbo_flow.h"
#include "turbo_flow_control.h"

#include <string.h>

typedef struct control_adapter_s {
  int resource_commands;
  int result;
  int metadata_result;
  const char *resource_uid;
  turbo_flow_resource_command_kind_t last_resource_command;
  char last_endpoint_host[TURBO_FLOW_ENDPOINT_MAX + 1u];
  char last_endpoint_path[TURBO_FLOW_ENDPOINT_MAX + 1u];
  int last_endpoint_port;
  uint64_t generation;
} control_adapter_t;

static int control_noop_stage(void *ctx, turbo_flow_msg_t *msg) {
  (void)ctx;
  (void)msg;
  return SALTS_OK;
}

static int control_adapter_snapshot(void *ctx, turbo_flow_connection_snapshot_t *out) {
  (void)ctx;
  out->state = TURBO_FLOW_CONNECTION_READY;
  out->connections_current = 2u;
  out->last_status = SALTS_OK;
  return SALTS_OK;
}

static int control_adapter_resource_metadata(void *ctx, turbo_flow_resource_metadata_t *out) {
  control_adapter_t *adapter = (control_adapter_t *)ctx;
  if (!adapter || !out || out->size < sizeof(*out)) return SALTS_EINVAL;
  if (adapter->metadata_result != SALTS_OK) return adapter->metadata_result;
  *out = (turbo_flow_resource_metadata_t)TURBO_FLOW_RESOURCE_METADATA_INIT;
  out->domain = TURBO_FLOW_DOMAIN_IO_TRANSPORT;
  out->kind = TURBO_FLOW_RESOURCE_CONNECTION;
  memcpy(out->uid, adapter->resource_uid ? adapter->resource_uid : "connection:mock",
         strlen(adapter->resource_uid ? adapter->resource_uid : "connection:mock") + 1u);
  memcpy(out->owner_name, "mock", sizeof("mock"));
  out->generation = adapter->generation;
  out->observed_generation = adapter->generation;
  return SALTS_OK;
}

static int control_adapter_resource_command(void *ctx, turbo_flow_t *flow,
                                            const turbo_flow_resource_command_t *command) {
  control_adapter_t *adapter = (control_adapter_t *)ctx;
  (void)flow;
  if (!adapter || !command) return SALTS_EINVAL;
  adapter->resource_commands++;
  adapter->last_resource_command = command->kind;
  memcpy(adapter->last_endpoint_host, command->endpoint_host, sizeof(adapter->last_endpoint_host));
  memcpy(adapter->last_endpoint_path, command->endpoint_path, sizeof(adapter->last_endpoint_path));
  adapter->last_endpoint_port = command->endpoint_port;
  if (adapter->result != SALTS_OK) return adapter->result;
  if (adapter->generation == UINT64_MAX) return SALTS_ERANGE;
  adapter->generation++;
  return SALTS_OK;
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
  return SALTS_OK;
}

static turbo_flow_t *control_started_flow_ex(control_adapter_t *adapter,
                                             int register_stable_resource,
                                             control_adapter_t *extra_resource) {
  static const char source[] = "source input\n"
                               "stage main {\n"
                               "  step transform operation test.transform exec thread workers 2\n"
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
                 SALTS_OK);
    if (extra_resource) {
      turbo_flow_resource_provider_ops_t extra_ops = TURBO_FLOW_RESOURCE_PROVIDER_OPS_INIT;
      if (extra_resource->generation == 0u) extra_resource->generation = 1u;
      extra_ops.metadata = control_adapter_resource_metadata;
      extra_ops.command = control_adapter_resource_command;
      check_equal(turbo_flow_register_resource_provider(flow, "mock", &extra_ops, extra_resource),
                  SALTS_OK);
    }
  } else {
    check_equal(turbo_flow_register_adapter_ex(flow, "mock", &ops, adapter, &schema), SALTS_OK);
  }
  check_equal(turbo_flow_parse_string(flow, source, strlen(source)), SALTS_OK);
  flow_test_operation_t operation_transform_0 =
      flow_test_operation_init("test.transform", control_noop_stage, NULL);
  operation_transform_0.descriptor.scope.concurrency = TURBO_FLOW_CONCURRENCY_POOL;
  operation_transform_0.descriptor.execution_mask = TURBO_FLOW_OPERATION_EXEC_THREAD;
  check_equal(flow_test_operation_register(flow, &operation_transform_0), SALTS_OK);
  check_equal(turbo_flow_compile(flow), SALTS_OK);
  check_equal(turbo_flow_start(flow), SALTS_OK);
  return flow;
}

static turbo_flow_t *control_started_flow(control_adapter_t *adapter) {
  return control_started_flow_ex(adapter, 1, NULL);
}

spec("control_dsl") {
  it("parses unconditional and conditional typed commands") {
    turbo_flow_control_command_t command;
    turbo_flow_error_t error;
    const char *resize = "pool transform thread resize 4 timeout 5000";
    const char *conditional = "when pool.transform.thread.parallelism == 2 then flow pause";
    check_equal(turbo_flow_control_parse(resize, strlen(resize), &command, &error), SALTS_OK);
    check_equal(command.kind, TURBO_FLOW_CONTROL_RESIZE_POOL);
    check_equal(command.target, "transform");
    check_equal(command.parallelism, 4u);
    check_equal(command.timeout_ms, 5000u);
    check_equal(turbo_flow_control_parse(conditional, strlen(conditional), &command, &error),
                 SALTS_OK);
    check_equal(command.kind, TURBO_FLOW_CONTROL_PAUSE);
    check_equal(command.condition, "pool.transform.thread.parallelism == 2");
    check_equal(turbo_flow_control_parse("adapter host.path quiesce",
                                          sizeof("adapter host.path quiesce") - 1u, &command,
                                          &error),
                 SALTS_OK);
    check_equal(command.target, "host.path");
    check_equal(command.resource_kind, TURBO_FLOW_RESOURCE_COMMAND_QUIESCE);
    check_equal(turbo_flow_control_parse("adapter mock replace host \"endpoint.example\" port 9443 path \"/v1\"",
                                          sizeof("adapter mock replace host \"endpoint.example\" port 9443 path \"/v1\"") -
                                              1u,
                                          &command, &error),
                 SALTS_OK);
    check_equal(command.resource_kind, TURBO_FLOW_RESOURCE_COMMAND_REPLACE_ENDPOINT);
    check_equal(command.endpoint_host, "endpoint.example");
    check_equal(command.endpoint_path, "/v1");
    check_equal(command.endpoint_port, 9443);
  }

  it("executes one action only when core snapshot facts match") {
    control_adapter_t adapter = {0};
    turbo_flow_t *flow = control_started_flow(&adapter);
    turbo_flow_error_t error;
    const char *false_rule = "if runtime.active_publishes > 0 then flow pause";
    const char *true_rule = "when pool.transform.thread.parallelism == 2 then adapter mock quiesce";
    check_equal(turbo_flow_control_ex(flow, false_rule, strlen(false_rule), NULL, &error),
                 SALTS_OK);
    check_equal(turbo_flow_control(flow, true_rule, strlen(true_rule)), SALTS_OK);
    check_equal(adapter.resource_commands, 1);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
  }

  it("dispatches stable adapter controls through the resource command owner") {
    control_adapter_t adapter = {0};
    turbo_flow_t *flow = control_started_flow_ex(&adapter, 1, NULL);
    const char *command = "adapter mock quiesce";
    check_equal(turbo_flow_control(flow, command, strlen(command)), SALTS_OK);
    check_equal(adapter.resource_commands, 1);
    check_equal(adapter.last_resource_command, TURBO_FLOW_RESOURCE_COMMAND_QUIESCE);
    check_equal(adapter.generation, 2u);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
  }

  it("rejects adapter controls when an ordinary adapter has no command provider") {
    control_adapter_t adapter = {0};
    turbo_flow_t *flow = control_started_flow_ex(&adapter, 0, NULL);
    check_equal(turbo_flow_control(flow, "adapter mock quiesce",
                                   sizeof("adapter mock quiesce") - 1u),
                SALTS_ENOENT);
    check_equal(adapter.resource_commands, 0);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
  }

  it("uses the current provider generation for consecutive adapter commands") {
    control_adapter_t adapter = {0};
    turbo_flow_t *flow = control_started_flow(&adapter);
    check_equal(turbo_flow_control(flow, "adapter mock quiesce",
                                   sizeof("adapter mock quiesce") - 1u),
                SALTS_OK);
    check_equal(turbo_flow_control(flow, "adapter mock resume",
                                   sizeof("adapter mock resume") - 1u),
                SALTS_OK);
    check_equal(adapter.resource_commands, 2);
    check_equal(adapter.last_resource_command, TURBO_FLOW_RESOURCE_COMMAND_RESUME);
    check_equal(adapter.generation, 3u);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
  }

  it("rejects ambiguous command-capable connection providers without dispatch") {
    control_adapter_t adapter = {0};
    control_adapter_t extra = {.resource_uid = "connection:mock-secondary"};
    turbo_flow_t *flow = control_started_flow_ex(&adapter, 1, &extra);
    check_equal(turbo_flow_control(flow, "adapter mock quiesce",
                                   sizeof("adapter mock quiesce") - 1u),
                SALTS_EPROTO);
    check_equal(adapter.resource_commands, 0);
    check_equal(extra.resource_commands, 0);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
  }

  it("propagates stable provider metadata errors without dispatch") {
    control_adapter_t adapter = {0};
    turbo_flow_t *flow = control_started_flow(&adapter);
    adapter.metadata_result = SALTS_EIO;
    check_equal(turbo_flow_control(flow, "adapter mock quiesce",
                                   sizeof("adapter mock quiesce") - 1u),
                SALTS_EIO);
    check_equal(adapter.resource_commands, 0);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
  }

  it("copies parsed replace endpoint values into the owner command") {
    turbo_flow_control_command_t parsed;
    turbo_flow_control_command_t command;
    turbo_flow_error_t error;
    control_adapter_t adapter = {0};
    turbo_flow_t *flow = control_started_flow(&adapter);
    const char *text = "adapter mock replace host \"api.example\" port 8443 path \"/health\"";
    check_equal(turbo_flow_control_parse(text, strlen(text), &parsed, &error), SALTS_OK);
    command = parsed;
    memset(&parsed, 0, sizeof(parsed));
    check_equal(turbo_flow_control_execute_ex(flow, &command, NULL, &error), SALTS_OK);
    check_equal(adapter.resource_commands, 1);
    check_equal(adapter.last_resource_command, TURBO_FLOW_RESOURCE_COMMAND_REPLACE_ENDPOINT);
    check_equal(adapter.last_endpoint_host, "api.example");
    check_equal(adapter.last_endpoint_path, "/health");
    check_equal(adapter.last_endpoint_port, 8443);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
  }

  it("rejects unsupported adapter resource commands before dispatch") {
    turbo_flow_control_command_t command = TURBO_FLOW_CONTROL_COMMAND_INIT;
    turbo_flow_error_t error;
    control_adapter_t adapter = {0};
    turbo_flow_t *flow = control_started_flow(&adapter);
    command.kind = TURBO_FLOW_CONTROL_ADAPTER;
    memcpy(command.target, "mock", sizeof("mock"));
    check_equal(turbo_flow_control_execute_ex(flow, &command, NULL, &error), SALTS_EINVAL);
    command.resource_kind = TURBO_FLOW_RESOURCE_COMMAND_RESIZE_POOL;
    check_equal(turbo_flow_control_execute_ex(flow, &command, NULL, &error), SALTS_EINVAL);
    command.resource_kind = TURBO_FLOW_RESOURCE_COMMAND_REPLACE_ENDPOINT;
    memcpy(command.endpoint_host, "api.example", sizeof("api.example"));
    command.endpoint_port = 0;
    check_equal(turbo_flow_control_execute_ex(flow, &command, NULL, &error), SALTS_EINVAL);
    check_equal(adapter.resource_commands, 0);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
  }

  it("dispatches pool resize through the resource command dispatcher") {
    control_adapter_t adapter = {0};
    turbo_flow_t *flow = control_started_flow(&adapter);
    turbo_flow_pool_snapshot_t pool;
    const char *command = "pool transform thread resize 3 timeout 5000";
    check_equal(turbo_flow_control(flow, command, strlen(command)), SALTS_OK);
    check_equal(turbo_flow_pool_snapshot_at(flow, 0u, &pool), SALTS_OK);
    check_equal(pool.kind, TURBO_FLOW_POOL_THREAD);
    check_equal(pool.parallelism, 3u);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
  }

  it("fails fast for unknown facts and non-BOOL conditions") {
    control_adapter_t adapter = {0};
    turbo_flow_error_t error;
    turbo_flow_t *flow = control_started_flow(&adapter);
    const char *unknown = "if system.missing > 0 then flow pause";
    const char *wrong_type = "if runtime.stage_count then flow pause";
    check_equal(turbo_flow_control_ex(flow, unknown, strlen(unknown), NULL, &error), SALTS_ENOENT);
    check_contains(error.message, "unknown external field");
    check_equal(turbo_flow_control_ex(flow, wrong_type, strlen(wrong_type), NULL, &error),
                 SALTS_EINVAL);
    check_contains(error.message, "BOOL");
    check_equal(turbo_flow_stop(flow), SALTS_OK);
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
    check_equal(turbo_flow_control_ex(flow, rule, strlen(rule), &facts, &error), SALTS_OK);
    check_equal(adapter.resource_commands, 1);
    invalid_type = 1;
    check_equal(turbo_flow_control_ex(flow, rule, strlen(rule), &facts, &error), SALTS_EPROTO);
    check_equal(adapter.resource_commands, 1);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
  }

  it("propagates typed command owner failures") {
    control_adapter_t adapter = {0};
    turbo_flow_error_t error;
    turbo_flow_t *flow = control_started_flow_ex(&adapter, 1, NULL);
    const char *command = "adapter mock resume";
    adapter.result = SALTS_EIO;
    check_equal(turbo_flow_control_ex(flow, command, strlen(command), NULL, &error), SALTS_EIO);
    check_equal(adapter.resource_commands, 1);
    check_contains(error.message, "command failed");
    check_equal(turbo_flow_stop(flow), SALTS_OK);
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
    check_equal(turbo_flow_control_execute_ex(flow, &command, NULL, &error), SALTS_EINVAL);
    check_contains(error.message, "invalid");

    check_equal(turbo_flow_control_parse("when runtime.accepting then flow pause",
                                          sizeof("when runtime.accepting then flow pause") - 1u,
                                          &command, &error),
                 SALTS_OK);
    facts.schema = &missing_fields;
    facts.read_field = control_external_read;
    facts.ctx = &adapter.result;
    check_equal(turbo_flow_control_execute_ex(flow, &command, &facts, &error), SALTS_EINVAL);
    check_contains(error.message, "collect control facts");
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    check_equal(turbo_flow_control_execute_ex(flow, &command, NULL, &error), SALTS_EINVAL);
    check_contains(error.message, "started flow");
    turbo_flow_destroy(flow);
  }
}
