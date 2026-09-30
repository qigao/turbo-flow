#include "flow_internal.h"
#include "tinytest.h"
#include "turbo_flow.h"
#include "turbo_flow_domain.h"

#include <cflow/function_projection.h>
#include <cmeta/data.h>
#include <cmeta/function.h>

#include <string.h>

FunctionDecl(value, int, plan_diag_increment,
    (int, value, CMETA_PARAM_IN));

int plan_diag_increment(int value) {
  return value + 1;
}
CFLOW_REFLECTED_ADAPTER(plan_diag_increment);

static int plan_diag_native_stage(turbo_flow_msg_t *message, void *ctx) {
  (void)message;
  (void)ctx;
  return SALTS_OK;
}

static turbo_flow_operation_descriptor_t
plan_diag_reflected_descriptor(const char *name) {
  turbo_flow_operation_descriptor_t operation;
  memset(&operation, 0, sizeof(operation));
  operation.size = sizeof(operation);
  operation.name = name;
  operation.version = 1u;
  operation.domain = TURBO_FLOW_DOMAIN_DATA;
  operation.scope.data = TURBO_FLOW_DATA_SCOPE_MESSAGE;
  operation.scope.state = TURBO_FLOW_STATE_SCOPE_NONE;
  operation.scope.lifetime = TURBO_FLOW_LIFETIME_DISPATCH;
  operation.scope.concurrency = TURBO_FLOW_CONCURRENCY_INLINE_LANE;
  operation.scope.authority = TURBO_FLOW_AUTHORITY_PURE;
  operation.flags = TURBO_FLOW_OPERATION_STAGE;
  operation.execution_mask = TURBO_FLOW_OPERATION_EXEC_INLINE;
  operation.runtime.error_mode = TURBO_FLOW_ERROR_PROPAGATE;
  return operation;
}

static int plan_diag_register_reflected(turbo_flow_t *flow, const char *name) {
  turbo_flow_operation_descriptor_t operation =
      plan_diag_reflected_descriptor(name);
  turbo_flow_operation_port_binding_t ports[2] = {
      TURBO_FLOW_OPERATION_PORT_BINDING_INIT,
      TURBO_FLOW_OPERATION_PORT_BINDING_INIT};
  turbo_flow_reflected_operation_registration_t registration =
      TURBO_FLOW_REFLECTED_OPERATION_REGISTRATION_INIT;

  ports[0].port_index = 0u;
  ports[0].domain = TURBO_FLOW_DOMAIN_DATA;
  ports[0].direction = TURBO_FLOW_OPERATION_PORT_INPUT;
  ports[0].value_kind = TURBO_FLOW_OPERATION_VALUE_PARAMETER;
  ports[0].storage = TURBO_FLOW_OPERATION_STORAGE_DIRECT;
  ports[0].parameter_index = 0u;
  ports[0].data = &cmeta_data_int;

  ports[1].port_index = 0u;
  ports[1].domain = TURBO_FLOW_DOMAIN_DATA;
  ports[1].direction = TURBO_FLOW_OPERATION_PORT_OUTPUT;
  ports[1].value_kind = TURBO_FLOW_OPERATION_VALUE_RETURN;
  ports[1].storage = TURBO_FLOW_OPERATION_STORAGE_DIRECT;
  ports[1].parameter_index = SIZE_MAX;
  ports[1].data = &cmeta_data_int;

  registration.operation = &operation;
  registration.function = FunctionMeta(plan_diag_increment);
  registration.abi = FunctionAbi(plan_diag_increment);
  registration.adapter = CFLOW_REFLECTED_CALLABLE(plan_diag_increment);
  registration.ports = ports;
  registration.port_count = 2u;
  registration.lowering = TURBO_FLOW_REFLECTED_LOWERING_CFLOW_MAP;
  return turbo_flow_register_reflected_operation(flow, &registration);
}

static turbo_flow_t *plan_diag_build_reflected(void) {
  static const char graph[] =
      "source input\n"
      "stage first operation test.plan.diag.first\n"
      "stage second operation test.plan.diag.second\n"
      "stage main {\n"
      "  input -> first -> second\n"
      "}\n";
  turbo_flow_t *flow = turbo_flow_create();
  if (!flow) return NULL;
  if (plan_diag_register_reflected(flow, "test.plan.diag.first") != SALTS_OK ||
      plan_diag_register_reflected(flow, "test.plan.diag.second") != SALTS_OK ||
      turbo_flow_parse_string(flow, graph, sizeof(graph) - 1u) != SALTS_OK ||
      turbo_flow_compile(flow) != SALTS_OK) {
    turbo_flow_destroy(flow);
    return NULL;
  }
  return flow;
}

static turbo_flow_t *plan_diag_build_native(void) {
  static const char graph[] =
      "source input\n"
      "stage native operation test.plan.diag.native\n"
      "stage main {\n"
      "  input -> native\n"
      "}\n";
  turbo_flow_operation_descriptor_t operation;
  turbo_flow_operation_provider_registration_t provider =
      TURBO_FLOW_OPERATION_PROVIDER_REGISTRATION_INIT;
  turbo_flow_t *flow = turbo_flow_create();

  if (!flow) return NULL;
  memset(&operation, 0, sizeof(operation));
  operation.size = sizeof(operation);
  operation.name = "test.plan.diag.native";
  operation.version = 1u;
  operation.domain = TURBO_FLOW_DOMAIN_DATA;
  operation.input_domain = TURBO_FLOW_DOMAIN_DATA;
  operation.input_type = "Message";
  operation.output_domain = TURBO_FLOW_DOMAIN_DATA;
  operation.output_type = "Message";
  operation.scope.data = TURBO_FLOW_DATA_SCOPE_MESSAGE;
  operation.scope.state = TURBO_FLOW_STATE_SCOPE_NONE;
  operation.scope.lifetime = TURBO_FLOW_LIFETIME_DISPATCH;
  operation.scope.concurrency = TURBO_FLOW_CONCURRENCY_INLINE_LANE;
  operation.scope.authority = TURBO_FLOW_AUTHORITY_PURE;
  operation.flags = TURBO_FLOW_OPERATION_STAGE;
  operation.execution_mask = TURBO_FLOW_OPERATION_EXEC_INLINE;
  operation.runtime.error_mode = TURBO_FLOW_ERROR_PROPAGATE;

  provider.operation_name = operation.name;
  provider.fn = plan_diag_native_stage;

  if (turbo_flow_register_operation(flow, &operation) != SALTS_OK ||
      turbo_flow_register_operation_provider(flow, &provider) != SALTS_OK ||
      turbo_flow_parse_string(flow, graph, sizeof(graph) - 1u) != SALTS_OK ||
      turbo_flow_compile(flow) != SALTS_OK) {
    turbo_flow_destroy(flow);
    return NULL;
  }
  return flow;
}

static int plan_diag_same_string(const char *left, const char *right) {
  if (!left || !right) return left == right;
  return strcmp(left, right) == 0;
}

typedef struct plan_diag_stage_probe_s {
  turbo_flow_stage_plan_t expected;
  int start_calls;
  int consume_calls;
  int stop_calls;
} plan_diag_stage_probe_t;

static void plan_diag_check_stage_probe(plan_diag_stage_probe_t *probe,
                                        const turbo_flow_stage_plan_t *stage) {
  check_not_null(probe);
  check_not_null(stage);
  check_true(plan_diag_same_string(stage->name, probe->expected.name));
  check_true(plan_diag_same_string(stage->adapter_name, probe->expected.adapter_name));
  check_true(plan_diag_same_string(stage->operation_name, probe->expected.operation_name));
  check_true(plan_diag_same_string(stage->resource_name, probe->expected.resource_name));
  check_equal(stage->is_source, probe->expected.is_source);
  check_equal(stage->is_buffer, probe->expected.is_buffer);
  check_equal(stage->data_strategy, probe->expected.data_strategy);
  check_equal(stage->data_worker_count, probe->expected.data_worker_count);
  check_equal(stage->exec.kind, probe->expected.exec.kind);
  check_equal(stage->effects, probe->expected.effects);
  check_equal(stage->retry.max_attempts, probe->expected.retry.max_attempts);
  check_equal(stage->retry.delay_ms, probe->expected.retry.delay_ms);
  check_equal(stage->reorder.capacity, probe->expected.reorder.capacity);
  check_equal(stage->reorder.timeout_ms, probe->expected.reorder.timeout_ms);
}

static int plan_diag_adapter_start(void *ctx, turbo_flow_t *flow,
                                   const turbo_flow_stage_plan_t *stage) {
  plan_diag_stage_probe_t *probe = (plan_diag_stage_probe_t *)ctx;
  (void)flow;
  plan_diag_check_stage_probe(probe, stage);
  probe->start_calls += 1;
  return SALTS_OK;
}

static int plan_diag_adapter_consume(void *ctx, turbo_flow_t *flow,
                                     const turbo_flow_stage_plan_t *stage,
                                     turbo_flow_msg_t *message) {
  plan_diag_stage_probe_t *probe = (plan_diag_stage_probe_t *)ctx;
  (void)flow;
  check_not_null(message);
  plan_diag_check_stage_probe(probe, stage);
  probe->consume_calls += 1;
  return SALTS_OK;
}

static void plan_diag_adapter_stop(void *ctx, turbo_flow_t *flow,
                                   const turbo_flow_stage_plan_t *stage) {
  plan_diag_stage_probe_t *probe = (plan_diag_stage_probe_t *)ctx;
  (void)flow;
  plan_diag_check_stage_probe(probe, stage);
  probe->stop_calls += 1;
}

static turbo_flow_t *plan_diag_build_adapter(plan_diag_stage_probe_t *probe) {
  static const char graph[] =
      "source input\n"
      "stage sink adapter plan.diag.adapter\n"
      "stage main {\n"
      "  input -> sink\n"
      "}\n";
  turbo_flow_adapter_ops_t ops;
  turbo_flow_t *flow = turbo_flow_create();

  if (!flow) return NULL;
  memset(&ops, 0, sizeof(ops));
  ops.start = plan_diag_adapter_start;
  ops.consume = plan_diag_adapter_consume;
  ops.stop = plan_diag_adapter_stop;
  if (turbo_flow_register_adapter(flow, "plan.diag.adapter", &ops, probe) != SALTS_OK ||
      turbo_flow_parse_string(flow, graph, sizeof(graph) - 1u) != SALTS_OK ||
      turbo_flow_compile(flow) != SALTS_OK) {
    turbo_flow_destroy(flow);
    return NULL;
  }
  return flow;
}

suite("TurboFlow deterministic ExecutionPlan diagnostics") {
  it("reports identical indexed facts for identical reflected plans") {
    turbo_flow_t *first = plan_diag_build_reflected();
    turbo_flow_t *second = plan_diag_build_reflected();
    turbo_flow_execution_plan_summary_t first_summary =
        TURBO_FLOW_EXECUTION_PLAN_SUMMARY_INIT;
    turbo_flow_execution_plan_summary_t second_summary =
        TURBO_FLOW_EXECUTION_PLAN_SUMMARY_INIT;
    turbo_flow_execution_cflow_region_view_t first_region =
        TURBO_FLOW_EXECUTION_CFLOW_REGION_VIEW_INIT;
    turbo_flow_execution_cflow_region_view_t second_region =
        TURBO_FLOW_EXECUTION_CFLOW_REGION_VIEW_INIT;

    check_not_null(first);
    check_not_null(second);
    check_equal(turbo_flow_execution_plan_summary(first, &first_summary),
                SALTS_OK);
    check_equal(turbo_flow_execution_plan_summary(second, &second_summary),
                SALTS_OK);
    check_equal(first_summary.stage_count, (size_t)3u);
    check_equal(first_summary.edge_count, (size_t)2u);
    check_true(first_summary.segment_count >= first_summary.edge_count);
    check_equal(first_summary.cflow_region_count, (size_t)1u);
    check_equal(first_summary.stage_count, second_summary.stage_count);
    check_equal(first_summary.edge_count, second_summary.edge_count);
    check_equal(first_summary.segment_count, second_summary.segment_count);
    check_equal(first_summary.cflow_region_count,
                second_summary.cflow_region_count);

    for (size_t index = 0u; index < first_summary.stage_count; ++index) {
      turbo_flow_execution_stage_view_t left =
          TURBO_FLOW_EXECUTION_STAGE_VIEW_INIT;
      turbo_flow_execution_stage_view_t right =
          TURBO_FLOW_EXECUTION_STAGE_VIEW_INIT;
      check_equal(turbo_flow_execution_plan_stage_at(first, index, &left),
                  SALTS_OK);
      check_equal(turbo_flow_execution_plan_stage_at(second, index, &right),
                  SALTS_OK);
      check_equal(left.stage_index, right.stage_index);
      check_equal(left.backend, right.backend);
      check_equal(left.cflow_region_index, right.cflow_region_index);
      check_true(plan_diag_same_string(left.stage_name, right.stage_name));
      check_true(plan_diag_same_string(left.adapter_name, right.adapter_name));
      check_true(plan_diag_same_string(left.operation_name,
                                       right.operation_name));
      check_true(plan_diag_same_string(left.resource_name,
                                       right.resource_name));
      check_true(plan_diag_same_string(left.input_semantic_id,
                                       right.input_semantic_id));
      check_true(plan_diag_same_string(left.output_semantic_id,
                                       right.output_semantic_id));

      if (index == 0u) {
        check_equal(left.backend, TURBO_FLOW_EXECUTION_BACKEND_SOURCE);
        check_equal(left.cflow_region_index,
                    TURBO_FLOW_EXECUTION_PLAN_INDEX_NONE);
        check_equal(strcmp(left.stage_name, "input"), 0);
      } else {
        check_equal(left.backend, TURBO_FLOW_EXECUTION_BACKEND_CFLOW_DIRECT);
        check_equal(left.cflow_region_index, (uint32_t)0u);
        check_not_null(left.input_semantic_id);
        check_not_null(left.output_semantic_id);
        check_equal(strcmp(left.input_semantic_id, cmeta_data_int.stable_id), 0);
        check_equal(strcmp(left.output_semantic_id, cmeta_data_int.stable_id), 0);
      }
    }

    check_equal(turbo_flow_execution_plan_cflow_region_at(
                    first, 0u, &first_region),
                SALTS_OK);
    check_equal(turbo_flow_execution_plan_cflow_region_at(
                    second, 0u, &second_region),
                SALTS_OK);
    check_equal(first_region.region_index, (uint32_t)0u);
    check_equal(first_region.backend, TURBO_FLOW_EXECUTION_BACKEND_CFLOW_DIRECT);
    check_equal(first_region.entry_stage, (uint32_t)1u);
    check_equal(first_region.exit_stage, (uint32_t)2u);
    check_equal(first_region.stage_count, (uint32_t)2u);
    check_true(first_region.batch_safe);
    check_not_null(first_region.input_data_id);
    check_not_null(first_region.output_data_id);
    check_equal(strcmp(first_region.input_data_id, cmeta_data_int.stable_id), 0);
    check_equal(strcmp(first_region.output_data_id, cmeta_data_int.stable_id), 0);
    check_true(first_region.graph_nodes > 0u);
    check_true(first_region.instructions > 0u);
    check_equal(first_region.map_callbacks, (size_t)2u);
    check_equal(first_region.region_index, second_region.region_index);
    check_equal(first_region.entry_stage, second_region.entry_stage);
    check_equal(first_region.exit_stage, second_region.exit_stage);
    check_equal(first_region.stage_count, second_region.stage_count);
    check_equal(first_region.batch_safe, second_region.batch_safe);
    check_true(plan_diag_same_string(first_region.input_data_id,
                                     second_region.input_data_id));
    check_true(plan_diag_same_string(first_region.output_data_id,
                                     second_region.output_data_id));
    check_equal(first_region.graph_nodes, second_region.graph_nodes);
    check_equal(first_region.instructions, second_region.instructions);
    check_equal(first_region.map_callbacks, second_region.map_callbacks);
    check_equal(first_region.inference_queries, second_region.inference_queries);

    turbo_flow_destroy(first);
    turbo_flow_destroy(second);
  }

  it("invalidates borrowed diagnostics across reset and recompiles deterministically") {
    static const char graph[] =
        "source input\n"
        "stage first operation test.plan.diag.first\n"
        "stage second operation test.plan.diag.second\n"
        "stage main {\n"
        "  input -> first -> second\n"
        "}\n";
    turbo_flow_t *flow = plan_diag_build_reflected();
    turbo_flow_execution_plan_summary_t summary =
        TURBO_FLOW_EXECUTION_PLAN_SUMMARY_INIT;
    turbo_flow_execution_cflow_region_view_t region =
        TURBO_FLOW_EXECUTION_CFLOW_REGION_VIEW_INIT;

    check_not_null(flow);
    check_equal(turbo_flow_execution_plan_summary(flow, &summary), SALTS_OK);
    check_equal(turbo_flow_reset(flow, 1), SALTS_OK);

    summary = (turbo_flow_execution_plan_summary_t)
        TURBO_FLOW_EXECUTION_PLAN_SUMMARY_INIT;
    check_equal(turbo_flow_execution_plan_summary(flow, &summary), SALTS_EINVAL);

    check_equal(turbo_flow_parse_string(flow, graph, sizeof(graph) - 1u),
                SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_OK);
    summary = (turbo_flow_execution_plan_summary_t)
        TURBO_FLOW_EXECUTION_PLAN_SUMMARY_INIT;
    check_equal(turbo_flow_execution_plan_summary(flow, &summary), SALTS_OK);
    check_equal(summary.stage_count, (size_t)3u);
    check_equal(summary.cflow_region_count, (size_t)1u);
    check_equal(turbo_flow_execution_plan_cflow_region_at(flow, 0u, &region),
                SALTS_OK);
    check_equal(region.entry_stage, (uint32_t)1u);
    check_equal(region.exit_stage, (uint32_t)2u);
    check_equal(region.stage_count, (uint32_t)2u);

    turbo_flow_destroy(flow);
  }

  it("keeps a native operation explicitly outside CFlow") {
    turbo_flow_t *flow = plan_diag_build_native();
    turbo_flow_execution_plan_summary_t summary =
        TURBO_FLOW_EXECUTION_PLAN_SUMMARY_INIT;
    turbo_flow_execution_stage_view_t source =
        TURBO_FLOW_EXECUTION_STAGE_VIEW_INIT;
    turbo_flow_execution_stage_view_t native =
        TURBO_FLOW_EXECUTION_STAGE_VIEW_INIT;

    check_not_null(flow);
    check_equal(turbo_flow_execution_plan_summary(flow, &summary), SALTS_OK);
    check_equal(summary.stage_count, (size_t)2u);
    check_equal(summary.cflow_region_count, (size_t)0u);
    check_equal(turbo_flow_execution_plan_stage_at(flow, 0u, &source), SALTS_OK);
    check_equal(turbo_flow_execution_plan_stage_at(flow, 1u, &native), SALTS_OK);
    check_equal(source.backend, TURBO_FLOW_EXECUTION_BACKEND_SOURCE);
    check_equal(native.backend, TURBO_FLOW_EXECUTION_BACKEND_NATIVE);
    check_equal(native.cflow_region_index, TURBO_FLOW_EXECUTION_PLAN_INDEX_NONE);
    check_not_null(native.operation_name);
    check_equal(strcmp(native.operation_name, "test.plan.diag.native"), 0);
    check_not_null(native.input_semantic_id);
    check_not_null(native.output_semantic_id);
    check_equal(strcmp(native.input_semantic_id, native.output_semantic_id), 0);

    turbo_flow_destroy(flow);
  }

  it("executes native stages from the frozen sealed-plan runtime contract") {
    turbo_flow_t *flow = plan_diag_build_native();
    flow_stage_plan_impl_t *stage;
    const flow_executor_plan_t *executor;
    const turbo_flow_operation_descriptor_t *operation;
    turbo_flow_operation_descriptor_t frozen_operation;
    turbo_flow_msg_t message;

    check_not_null(flow);
    stage = (flow_stage_plan_impl_t *)vec_at(&flow->stages, 1u);
    executor = flow_executor_plan_for_stage(flow, 1u);
    operation = turbo_flow_stage_operation_at(flow, 1u);
    check_not_null(stage);
    check_not_null(executor);
    check_not_null(operation);
    frozen_operation = *operation;
    check_equal(executor->runtime.deadline_ms, (uint64_t)0u);
    check_equal(executor->runtime.settlement, (uint32_t)0u);

    /*
     * Deliberately poison the mutable compile-time stage copy after the plan is
     * sealed. Runtime dispatch/settlement must use executor->runtime instead.
     */
    stage->resolved_operation.version = frozen_operation.version + 9u;
    stage->resolved_operation.flags = 0u;
    stage->resolved_operation.execution_mask = 0u;
    stage->resolved_operation.runtime.deadline_ms = 1u;
    stage->resolved_operation.runtime.settlement = TURBO_FLOW_SETTLEMENT_COMPLETE;

    operation = turbo_flow_stage_operation_at(flow, 1u);
    check_not_null(operation);
    check_equal(operation->version, frozen_operation.version);
    check_equal(operation->flags, frozen_operation.flags);
    check_equal(operation->execution_mask, frozen_operation.execution_mask);
    check_equal(operation->runtime.deadline_ms, frozen_operation.runtime.deadline_ms);
    check_equal(operation->runtime.settlement, frozen_operation.runtime.settlement);

    turbo_flow_msg_init(&message);
    check_equal(turbo_flow_start(flow), SALTS_OK);
    check_equal(turbo_flow_publish(flow, "input", &message), SALTS_OK);
    check_equal(turbo_flow_stop(flow), SALTS_OK);

    check_equal(executor->runtime.deadline_ms, (uint64_t)0u);
    check_equal(executor->runtime.settlement, (uint32_t)0u);
    operation = turbo_flow_stage_operation_at(flow, 1u);
    check_not_null(operation);
    check_equal(operation->version, frozen_operation.version);
    check_equal(operation->flags, frozen_operation.flags);
    check_equal(operation->execution_mask, frozen_operation.execution_mask);
    turbo_flow_destroy(flow);
  }

  it("keeps adapter lifecycle and dispatch on the frozen sealed-stage contract") {
    plan_diag_stage_probe_t probe;
    turbo_flow_t *flow;
    int sink_index;
    const turbo_flow_stage_plan_t *sealed;
    flow_stage_plan_impl_t *mutable_stage;
    turbo_flow_msg_t message;

    memset(&probe, 0, sizeof(probe));
    flow = plan_diag_build_adapter(&probe);
    check_not_null(flow);
    sink_index = turbo_flow_find_stage(flow, "sink");
    check(sink_index >= 0);
    sealed = turbo_flow_stage_at(flow, (size_t)sink_index);
    check_not_null(sealed);
    probe.expected = *sealed;

    mutable_stage =
        (flow_stage_plan_impl_t *)vec_at(&flow->stages, (size_t)sink_index);
    check_not_null(mutable_stage);

    /*
     * Poison only non-owning/scalar fields in the mutable parser stage after
     * compile. Start/data-plane/reorder/consume/stop must all stay on the
     * immutable ExecutionPlan copy.
     */
    mutable_stage->data_strategy = TURBO_FLOW_DATA_WORKER_POOL;
    mutable_stage->data_worker_count = probe.expected.data_worker_count + 7u;
    mutable_stage->exec.kind = TURBO_FLOW_EXEC_THREAD_POOL;
    mutable_stage->effects |= TURBO_FLOW_STAGE_EFFECT_DYNAMIC_DECISION;
    mutable_stage->retry.max_attempts = probe.expected.retry.max_attempts + 5u;
    mutable_stage->retry.delay_ms = probe.expected.retry.delay_ms + 13u;
    mutable_stage->reorder.capacity = probe.expected.reorder.capacity + 11u;
    mutable_stage->reorder.timeout_ms = probe.expected.reorder.timeout_ms + 17u;

    sealed = turbo_flow_stage_at(flow, (size_t)sink_index);
    plan_diag_check_stage_probe(&probe, sealed);

    turbo_flow_msg_init(&message);
    check_equal(turbo_flow_start(flow), SALTS_OK);
    check_equal(probe.start_calls, 1);
    check_equal(turbo_flow_publish(flow, "input", &message), SALTS_OK);
    check_equal(probe.consume_calls, 1);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    check_equal(probe.stop_calls, 1);

    sealed = turbo_flow_stage_at(flow, (size_t)sink_index);
    plan_diag_check_stage_probe(&probe, sealed);
    turbo_flow_destroy(flow);
  }

  it("requires initialized versioned outputs and rejects invalid indexes") {
    turbo_flow_t *flow = plan_diag_build_reflected();
    turbo_flow_execution_plan_summary_t bad_summary = {0};
    turbo_flow_execution_stage_view_t bad_stage = {0};
    turbo_flow_execution_cflow_region_view_t bad_region = {0};
    turbo_flow_execution_stage_view_t stage =
        TURBO_FLOW_EXECUTION_STAGE_VIEW_INIT;
    turbo_flow_execution_cflow_region_view_t region =
        TURBO_FLOW_EXECUTION_CFLOW_REGION_VIEW_INIT;

    check_not_null(flow);
    check_equal(turbo_flow_execution_plan_summary(flow, &bad_summary),
                SALTS_EINVAL);
    check_equal(turbo_flow_execution_plan_stage_at(flow, 0u, &bad_stage),
                SALTS_EINVAL);
    check_equal(turbo_flow_execution_plan_cflow_region_at(flow, 0u, &bad_region),
                SALTS_EINVAL);
    check_equal(turbo_flow_execution_plan_stage_at(flow, 99u, &stage),
                SALTS_ENOENT);
    check_equal(turbo_flow_execution_plan_cflow_region_at(flow, 99u, &region),
                SALTS_ENOENT);

    turbo_flow_destroy(flow);
  }
}
