#include "flow_internal.h"
#include "flow_plugin_operation_internal.h"
#include "tinytest.h"
#include "turbo_flow.h"
#include "turbo_flow_domain.h"

#include <cmeta/data.h>
#include <salts/plugin.h>

#include <stdlib.h>
#include <string.h>

#ifndef FLOW_PLUGIN_CFLOW_FIXTURE
#error "FLOW_PLUGIN_CFLOW_FIXTURE must name the canonical Salts Plugin fixture"
#endif

static turbo_flow_operation_descriptor_t
plugin_cflow_operation_descriptor(const char *name) {
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

static int plugin_cflow_clone_int(const void *value, void *ctx, void **out) {
  int *copy;
  (void)ctx;
  if (!value || !out) return SALTS_EINVAL;
  *out = NULL;
  copy = (int *)malloc(sizeof(*copy));
  if (!copy) return SALTS_ENOMEM;
  *copy = *(const int *)value;
  *out = copy;
  return SALTS_OK;
}

static void plugin_cflow_destroy_int(void *value, void *ctx) {
  (void)ctx;
  free(value);
}

static const turbo_flow_data_schema_t plugin_cflow_int_schema = {
    sizeof(turbo_flow_data_schema_t),
    TURBO_FLOW_DOMAIN_DATA,
    TURBO_FLOW_DATA_ENCODING_OPAQUE,
    "cmeta.int.data",
    "Integer",
    "int",
    7u,
    3u,
    NULL};

static void plugin_cflow_registry_open(
    salts_plugin_registry *registry, salts_plugin_ref *ref) {
  const salts_plugin_registry_config config = {4u};
  check_equal(salts_plugin_registry_init(registry, &config), SALTS_PLUGIN_OK);
  check_equal(
      salts_plugin_registry_load(registry, FLOW_PLUGIN_CFLOW_FIXTURE, ref),
      SALTS_PLUGIN_OK);
  check_equal(salts_plugin_registry_start(registry, *ref), SALTS_PLUGIN_OK);
}

suite("TurboFlow Salts Plugin CFlow binding") {
  it("compiles one canonical plugin function into a lease-safe CFlow region") {
    static const char graph[] =
        "source input\n"
        "stage plugin operation test.plugin.cflow.double\n"
        "stage main {\n"
        "  input -> plugin\n"
        "}\n";
    salts_plugin_registry registry = {0};
    salts_plugin_ref plugin = {0};
    salts_plugin_lifecycle_info lifecycle = {0};
    turbo_flow_t *flow = turbo_flow_create();
    turbo_flow_operation_descriptor_t operation =
        plugin_cflow_operation_descriptor("test.plugin.cflow.double");
    flow_plugin_cflow_function_binding_t binding = {0};
    const flow_cflow_region_plan_t *region;
    const flow_stage_semantic_plan_t *semantics;
    turbo_flow_msg_t message;
    int *input;
    int stage_index;
    bool quiescent = true;

    check_not_null(flow);
    plugin_cflow_registry_open(&registry, &plugin);
    check_equal(
        turbo_flow_parse_string(flow, graph, sizeof(graph) - 1u), SALTS_OK);

    binding.registry = &registry;
    binding.plugin = plugin;
    binding.export_id = "test.turboflow.cflow.double";
    binding.operation = &operation;
    binding.input_data = &cmeta_data_int;
    binding.output_data = &cmeta_data_int;
    check_equal(flow_plugin_bind_cflow_function(flow, &binding), SALTS_OK);
    check_equal(vec_size(&flow->pending_plan_resources), (size_t)1u);

    check_equal(turbo_flow_compile(flow), SALTS_OK);
    check_equal(vec_size(&flow->pending_plan_resources), (size_t)0u);
    check_equal(vec_size(&flow->compiled_plan.owned_resources), (size_t)1u);
    check_equal(vec_size(&flow->compiled_plan.cflow_regions), (size_t)1u);

    stage_index = turbo_flow_find_stage(flow, "plugin");
    check_true(stage_index >= 0);
    semantics = stage_index >= 0
                    ? (const flow_stage_semantic_plan_t *)vec_at_const(
                          &flow->compiled_plan.stage_semantics,
                          (size_t)stage_index)
                    : NULL;
    check_not_null(semantics);
    check_true(semantics && semantics->reflected);
    check_true(semantics && semantics->reflected_typed_adapter);
    check_true(semantics && semantics->lowering_candidate);

    region = (const flow_cflow_region_plan_t *)vec_at_const(
        &flow->compiled_plan.cflow_regions, 0u);
    check_not_null(region);
    check_equal(region ? region->backend : FLOW_CFLOW_REGION_BACKEND_NONE,
                FLOW_CFLOW_REGION_BACKEND_DIRECT);

    turbo_flow_msg_init(&message);
    input = (int *)malloc(sizeof(*input));
    check_not_null(input);
    *input = 7;
    check_equal(
        turbo_flow_msg_bind_typed_projection(
            &message, &plugin_cflow_int_schema, &cmeta_data_int, input,
            plugin_cflow_clone_int, plugin_cflow_destroy_int, NULL),
        SALTS_OK);
    input = NULL;
    check_equal(flow_cflow_region_execute(flow, region, &message), SALTS_OK);
    check_equal(*(const int *)turbo_flow_msg_projection(&message, NULL), 14);
    turbo_flow_msg_cleanup(&message);

    check_equal(
        salts_plugin_registry_get_lifecycle(&registry, plugin, &lifecycle),
        SALTS_PLUGIN_OK);
    check_equal(lifecycle.active_leases, (size_t)1u);

    check_equal(
        salts_plugin_registry_request_stop(&registry, plugin), SALTS_PLUGIN_OK);
    quiescent = true;
    check_equal(
        salts_plugin_registry_poll_quiescent(&registry, plugin, &quiescent),
        SALTS_PLUGIN_OK);
    check_false(quiescent);
    check_equal(
        salts_plugin_registry_unload(&registry, plugin), SALTS_PLUGIN_BUSY);

    turbo_flow_destroy(flow);
    flow = NULL;

    check_equal(
        salts_plugin_registry_get_lifecycle(&registry, plugin, &lifecycle),
        SALTS_PLUGIN_OK);
    check_equal(lifecycle.active_leases, (size_t)0u);
    quiescent = false;
    check_equal(
        salts_plugin_registry_poll_quiescent(&registry, plugin, &quiescent),
        SALTS_PLUGIN_OK);
    check_true(quiescent);
    check_equal(
        salts_plugin_registry_unload(&registry, plugin), SALTS_PLUGIN_OK);
    check_equal(salts_plugin_registry_destroy(&registry), SALTS_PLUGIN_OK);
  }

  it("rejects mismatched DataDesc without retaining the plugin lease") {
    static const char graph[] =
        "source input\n"
        "stage plugin operation test.plugin.cflow.mismatch\n"
        "stage main {\n"
        "  input -> plugin\n"
        "}\n";
    salts_plugin_registry registry = {0};
    salts_plugin_ref plugin = {0};
    salts_plugin_lifecycle_info lifecycle = {0};
    turbo_flow_t *flow = turbo_flow_create();
    turbo_flow_operation_descriptor_t operation =
        plugin_cflow_operation_descriptor("test.plugin.cflow.mismatch");
    flow_plugin_cflow_function_binding_t binding = {0};

    check_not_null(flow);
    plugin_cflow_registry_open(&registry, &plugin);
    check_equal(
        turbo_flow_parse_string(flow, graph, sizeof(graph) - 1u), SALTS_OK);

    binding.registry = &registry;
    binding.plugin = plugin;
    binding.export_id = "test.turboflow.cflow.double";
    binding.operation = &operation;
    binding.input_data = &cmeta_data_long;
    binding.output_data = &cmeta_data_int;
    check_equal(flow_plugin_bind_cflow_function(flow, &binding), SALTS_EPROTO);
    check_equal(vec_size(&flow->pending_plan_resources), (size_t)0u);
    check_equal(
        salts_plugin_registry_get_lifecycle(&registry, plugin, &lifecycle),
        SALTS_PLUGIN_OK);
    check_equal(lifecycle.active_leases, (size_t)0u);

    turbo_flow_destroy(flow);
    check_equal(
        salts_plugin_registry_request_stop(&registry, plugin), SALTS_PLUGIN_OK);
    {
      bool quiescent = false;
      check_equal(
          salts_plugin_registry_poll_quiescent(&registry, plugin, &quiescent),
          SALTS_PLUGIN_OK);
      check_true(quiescent);
    }
    check_equal(
        salts_plugin_registry_unload(&registry, plugin), SALTS_PLUGIN_OK);
    check_equal(salts_plugin_registry_destroy(&registry), SALTS_PLUGIN_OK);
  }
}
