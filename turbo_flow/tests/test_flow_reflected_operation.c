#include "flow_internal.h"
#include "tinytest.h"
#include "turbo_flow.h"
#include "turbo_flow_domain.h"

#include <cflow/function_projection.h>
#include <cmeta/data.h>
#include <cmeta/function.h>

#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

FunctionDecl(value, int, reflected_increment,
    (int, input, CMETA_PARAM_IN));
int reflected_increment(int input) { return input + 1; }
CFLOW_REFLECTED_ADAPTER(reflected_increment);

FunctionDecl(stateful, int, reflected_stateful,
    (int, input, CMETA_PARAM_IN));
int reflected_stateful(int input) { return input; }
CFLOW_REFLECTED_ADAPTER(reflected_stateful);

FunctionDecl(async, int, reflected_async,
    (int, input, CMETA_PARAM_IN));
int reflected_async(int input) { return input; }
CFLOW_REFLECTED_ADAPTER(reflected_async);

FunctionDecl(io, int, reflected_io,
    (int, input, CMETA_PARAM_IN));
int reflected_io(int input) { return input; }
CFLOW_REFLECTED_ADAPTER(reflected_io);

FunctionDecl(unknown, int, reflected_unknown,
    (int, input, CMETA_PARAM_IN));
int reflected_unknown(int input) { return input; }
CFLOW_REFLECTED_ADAPTER(reflected_unknown);

FunctionDecl(fallible, void, reflected_join_out,
    (int, left, CMETA_PARAM_IN),
    (int, right, CMETA_PARAM_IN),
    (int *, output, CMETA_PARAM_OUT,
     &cmeta_type_int_ptr, CMETA_ABI_OBJECT_POINTER));
void reflected_join_out(int left, int right, int *output) {
  if (output) *output = left + right;
}

static int reflected_noop_stage(turbo_flow_msg_t *msg, void *ctx) {
  (void)msg;
  (void)ctx;
  return SALTS_OK;
}

static turbo_flow_operation_descriptor_t
reflected_operation_descriptor(const char *name) {
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

static turbo_flow_operation_provider_registration_t
reflected_provider(const char *operation_name) {
  turbo_flow_operation_provider_registration_t provider =
      TURBO_FLOW_OPERATION_PROVIDER_REGISTRATION_INIT;
  provider.operation_name = operation_name;
  provider.fn = reflected_noop_stage;
  return provider;
}

static turbo_flow_operation_port_binding_t
reflected_param_port(uint32_t port_index,
                     turbo_flow_operation_port_direction_t direction,
                     size_t parameter_index,
                     turbo_flow_operation_storage_t storage) {
  turbo_flow_operation_port_binding_t port =
      TURBO_FLOW_OPERATION_PORT_BINDING_INIT;
  port.port_index = port_index;
  port.domain = TURBO_FLOW_DOMAIN_DATA;
  port.direction = direction;
  port.value_kind = TURBO_FLOW_OPERATION_VALUE_PARAMETER;
  port.storage = storage;
  port.parameter_index = parameter_index;
  port.data = &cmeta_data_int;
  return port;
}

static turbo_flow_operation_port_binding_t
reflected_return_port(uint32_t port_index) {
  turbo_flow_operation_port_binding_t port =
      TURBO_FLOW_OPERATION_PORT_BINDING_INIT;
  port.port_index = port_index;
  port.domain = TURBO_FLOW_DOMAIN_DATA;
  port.direction = TURBO_FLOW_OPERATION_PORT_OUTPUT;
  port.value_kind = TURBO_FLOW_OPERATION_VALUE_RETURN;
  port.storage = TURBO_FLOW_OPERATION_STORAGE_DIRECT;
  port.parameter_index = SIZE_MAX;
  port.data = &cmeta_data_int;
  return port;
}

static int register_unary_reflected_mode(
    turbo_flow_t *flow,
    turbo_flow_operation_descriptor_t *operation,
    const cmeta_function_desc *function,
    const cmeta_function_abi_desc *abi,
    cmeta_callable adapter,
    int register_provider) {
  turbo_flow_operation_port_binding_t ports[2];
  turbo_flow_reflected_operation_registration_t registration =
      TURBO_FLOW_REFLECTED_OPERATION_REGISTRATION_INIT;
  turbo_flow_operation_provider_registration_t provider;

  ports[0] = reflected_param_port(
      0u, TURBO_FLOW_OPERATION_PORT_INPUT, 0u,
      TURBO_FLOW_OPERATION_STORAGE_DIRECT);
  ports[1] = reflected_return_port(0u);

  registration.operation = operation;
  registration.function = function;
  registration.abi = abi;
  registration.adapter = adapter;
  registration.ports = ports;
  registration.port_count = 2u;
  registration.lowering = TURBO_FLOW_REFLECTED_LOWERING_CFLOW_MAP;

  if (turbo_flow_register_reflected_operation(flow, &registration) != SALTS_OK)
    return 0;
  if (!register_provider) return 1;
  provider = reflected_provider(operation->name);
  return turbo_flow_register_operation_provider(flow, &provider) == SALTS_OK;
}

static int register_unary_reflected(
    turbo_flow_t *flow,
    turbo_flow_operation_descriptor_t *operation,
    const cmeta_function_desc *function,
    const cmeta_function_abi_desc *abi,
    cmeta_callable adapter) {
  return register_unary_reflected_mode(
      flow, operation, function, abi, adapter, 1);
}

typedef struct reflected_runtime_sink_probe_s {
  size_t calls;
  int value;
} reflected_runtime_sink_probe_t;

static int reflected_runtime_sink(turbo_flow_msg_t *msg, void *ctx) {
  reflected_runtime_sink_probe_t *probe =
      (reflected_runtime_sink_probe_t *)ctx;
  const cmeta_data_desc *data = turbo_flow_msg_projection_data(msg);
  const int *value = (const int *)turbo_flow_msg_projection(msg, NULL);
  if (!probe || !value || !data ||
      !cmeta_data_desc_equal(data, &cmeta_data_int))
    return SALTS_EPROTO;
  ++probe->calls;
  probe->value = *value;
  return SALTS_OK;
}

static int reflected_test_clone_int(const void *value, void *ctx, void **out) {
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

static void reflected_test_destroy_int(void *value, void *ctx) {
  (void)ctx;
  free(value);
}

static void reflected_test_stage_observer(
    void *ctx, const char *stage_name, const char *adapter_name,
    const turbo_flow_msg_t *message, uint64_t duration_ns, int status) {
  (void)ctx;
  (void)stage_name;
  (void)adapter_name;
  (void)message;
  (void)duration_ns;
  (void)status;
}

static int register_native_int_stage(
    turbo_flow_t *flow, const char *name, turbo_flow_stage_fn fn, void *ctx) {
  turbo_flow_operation_descriptor_t operation =
      reflected_operation_descriptor(name);
  turbo_flow_operation_provider_registration_t provider =
      TURBO_FLOW_OPERATION_PROVIDER_REGISTRATION_INIT;

  operation.input_domain = TURBO_FLOW_DOMAIN_DATA;
  operation.input_type = cmeta_data_int.stable_id;
  operation.output_domain = TURBO_FLOW_DOMAIN_DATA;
  operation.output_type = cmeta_data_int.stable_id;
  if (turbo_flow_register_operation(flow, &operation) != SALTS_OK) return 0;
  provider.operation_name = name;
  provider.fn = fn;
  provider.ctx = ctx;
  return turbo_flow_register_operation_provider(flow, &provider) == SALTS_OK;
}

static int bind_test_int_projection(
    turbo_flow_msg_t *message, const turbo_flow_data_schema_t *schema, int value) {
  int *storage = (int *)malloc(sizeof(*storage));
  int rc;
  if (!storage) return SALTS_ENOMEM;
  *storage = value;
  rc = turbo_flow_msg_bind_typed_projection(
      message, schema, &cmeta_data_int, storage,
      reflected_test_clone_int, reflected_test_destroy_int, NULL);
  if (rc != SALTS_OK) free(storage);
  return rc;
}

static const flow_stage_semantic_plan_t *
compile_single_reflected_stage(turbo_flow_t *flow, const char *operation_name) {
  static const char prefix[] =
      "source input\n"
      "stage reflected operation ";
  static const char suffix[] =
      "\n"
      "stage main {\n"
      "  input -> reflected\n"
      "}\n";
  char source[512];
  int stage_index;

  if (!flow || !operation_name ||
      snprintf(source, sizeof(source), "%s%s%s",
               prefix, operation_name, suffix) < 0)
    return NULL;
  if (turbo_flow_parse_string(flow, source, strlen(source)) != SALTS_OK)
    return NULL;
  {
    int compile_rc = turbo_flow_compile(flow);
    if (compile_rc != SALTS_OK) {
      const turbo_flow_error_t *error = turbo_flow_last_error(flow);
      fprintf(stderr, "reflected compile failed: rc=%d error=%d message=%s\n",
              compile_rc, error ? error->code : 0,
              error ? error->message : "<none>");
      return NULL;
    }
  }
  stage_index = turbo_flow_find_stage(flow, "reflected");
  if (stage_index < 0) return NULL;
  return (const flow_stage_semantic_plan_t *)vec_at_const(
      &flow->compiled_plan.stage_semantics, (size_t)stage_index);
}

suite("TurboFlow reflected operation semantics") {
  it("uses FunctionDesc as the unary CFlow MAP semantic source") {
    turbo_flow_t *flow = turbo_flow_create();
    turbo_flow_operation_descriptor_t operation =
        reflected_operation_descriptor("test.reflected.increment");
    turbo_flow_reflected_operation_view_t view =
        TURBO_FLOW_REFLECTED_OPERATION_VIEW_INIT;
    const flow_stage_semantic_plan_t *semantics;

    check_not_null(flow);
    check_true(register_unary_reflected(
        flow, &operation,
        FunctionMeta(reflected_increment),
        FunctionAbi(reflected_increment),
        CFLOW_REFLECTED_CALLABLE(reflected_increment)));

    check_equal(turbo_flow_reflected_operation(
                    flow, operation.name, &view),
                SALTS_OK);
    check_true(cmeta_function_desc_equal(
        view.function, FunctionMeta(reflected_increment)));
    check_true(cmeta_function_abi_desc_equal(
        view.abi, FunctionAbi(reflected_increment)));
    check_equal(view.port_count, (size_t)2u);
    check_equal(view.lowering,
                TURBO_FLOW_REFLECTED_LOWERING_CFLOW_MAP);

    semantics = compile_single_reflected_stage(flow, operation.name);
    check_not_null(semantics);
    check_true(semantics->reflected);
    check_true(semantics->typed);
    check_equal(semantics->effects,
                FunctionMeta(reflected_increment)->effects);
    check_equal(semantics->barriers,
                (uint32_t)FLOW_LOWERING_BARRIER_NONE);
    check_true(semantics->lowering_candidate);
    check_true(semantics->candidate_region != FLOW_PLAN_INDEX_NONE);
    check_equal(flow->compiled_plan.candidate_region_count, 1u);
    check_true(cmeta_type_equal(
        semantics->canonical_input_type, &cmeta_type_int));
    check_true(cmeta_type_equal(
        semantics->canonical_output_type, &cmeta_type_int));
    check_true(cmeta_callable_contract_valid(semantics->callable));

    turbo_flow_destroy(flow);
  }

  it("compiles one maximal reflected chain into an executable CFlow Plan") {
    static const char graph[] =
        "source input\n"
        "stage first operation test.reflected.region.first\n"
        "stage second operation test.reflected.region.second\n"
        "stage main {\n"
        "  input -> first -> second\n"
        "}\n";
    const int inputs[] = {0, 4, -2};
    const int expected[] = {2, 6, 0};
    turbo_flow_t *flow = turbo_flow_create();
    turbo_flow_operation_descriptor_t first =
        reflected_operation_descriptor("test.reflected.region.first");
    turbo_flow_operation_descriptor_t second =
        reflected_operation_descriptor("test.reflected.region.second");
    cflow_result result = {0};
    const flow_cflow_region_plan_t *region;
    const uint32_t *first_region;
    const uint32_t *second_region;
    int first_stage;
    int second_stage;

    check_not_null(flow);
    check_true(register_unary_reflected(
        flow, &first, FunctionMeta(reflected_increment),
        FunctionAbi(reflected_increment),
        CFLOW_REFLECTED_CALLABLE(reflected_increment)));
    check_true(register_unary_reflected(
        flow, &second, FunctionMeta(reflected_increment),
        FunctionAbi(reflected_increment),
        CFLOW_REFLECTED_CALLABLE(reflected_increment)));
    check_equal(turbo_flow_parse_string(flow, graph, sizeof(graph) - 1u), SALTS_OK);
    {
      int compile_rc = turbo_flow_compile(flow);
      if (compile_rc != SALTS_OK) {
        const turbo_flow_error_t *error = turbo_flow_last_error(flow);
        fprintf(stderr, "region compile failed: rc=%d error=%d message=%s\n",
                compile_rc, error ? error->code : 0,
                error ? error->message : "<none>");
      }
      check_equal(compile_rc, SALTS_OK);
    }

    first_stage = turbo_flow_find_stage(flow, "first");
    second_stage = turbo_flow_find_stage(flow, "second");
    check_true(first_stage >= 0);
    check_true(second_stage >= 0);
    check_equal(flow->compiled_plan.candidate_region_count, (uint32_t)1u);
    check_equal(vec_size(&flow->compiled_plan.cflow_regions), (size_t)1u);

    first_region = (const uint32_t *)vec_at_const(
        &flow->compiled_plan.cflow_region_by_stage, (size_t)first_stage);
    second_region = (const uint32_t *)vec_at_const(
        &flow->compiled_plan.cflow_region_by_stage, (size_t)second_stage);
    check_not_null(first_region);
    check_not_null(second_region);
    check_equal(*first_region, (uint32_t)0u);
    check_equal(*second_region, (uint32_t)0u);

    region = (const flow_cflow_region_plan_t *)vec_at_const(
        &flow->compiled_plan.cflow_regions, 0u);
    check_not_null(region);
    check_equal(region->entry_stage, (uint32_t)first_stage);
    check_equal(region->exit_stage, (uint32_t)second_stage);
    check_equal(region->stage_count, (uint32_t)2u);
    check_true(region->plan.impl != NULL);
    check_true(cmeta_type_equal(region->plan.input_type, &cmeta_type_int));
    check_true(cmeta_type_equal(region->plan.output_type, &cmeta_type_int));
    check_true(region->stats.graph_nodes >= (size_t)2u);
    check_true(region->stats.instructions >= (size_t)1u);

    check_true(cflow_plan_eval_array(
        &region->plan, inputs, sizeof(inputs) / sizeof(inputs[0]), &result));
    check_equal(result.count, sizeof(expected) / sizeof(expected[0]));
    check_true(cmeta_type_equal(result.type, &cmeta_type_int));
    check_equal(result.data, expected, sizeof(expected));

    cflow_result_destroy(&result);
    turbo_flow_destroy(flow);
  }

  it("executes a provider-free reflected chain as one direct CFlow runtime region") {
    static const char graph[] =
        "source input\n"
        "stage first operation test.reflected.runtime.first\n"
        "stage second operation test.reflected.runtime.second\n"
        "stage sink operation test.reflected.runtime.sink\n"
        "stage main {\n"
        "  input -> first -> second -> sink\n"
        "}\n";
    turbo_flow_t *flow = turbo_flow_create();
    turbo_flow_operation_descriptor_t first =
        reflected_operation_descriptor("test.reflected.runtime.first");
    turbo_flow_operation_descriptor_t second =
        reflected_operation_descriptor("test.reflected.runtime.second");
    turbo_flow_operation_descriptor_t sink =
        reflected_operation_descriptor("test.reflected.runtime.sink");
    turbo_flow_operation_provider_registration_t sink_provider =
        TURBO_FLOW_OPERATION_PROVIDER_REGISTRATION_INIT;
    turbo_flow_data_schema_t schema = {
        sizeof(turbo_flow_data_schema_t), TURBO_FLOW_DOMAIN_DATA,
        TURBO_FLOW_DATA_ENCODING_OPAQUE, "cmeta.int.data", "Integer",
        "int", 7u, 3u, NULL};
    reflected_runtime_sink_probe_t probe = {0};
    turbo_flow_observer_ops_t observer = {0};
    turbo_flow_msg_t message;
    int *input = NULL;
    int first_stage;
    int second_stage;
    const flow_executor_plan_t *first_executor;
    const flow_executor_plan_t *second_executor;

    check_not_null(flow);
    check_true(register_unary_reflected_mode(
        flow, &first, FunctionMeta(reflected_increment),
        FunctionAbi(reflected_increment),
        CFLOW_REFLECTED_CALLABLE(reflected_increment), 0));
    check_true(register_unary_reflected_mode(
        flow, &second, FunctionMeta(reflected_increment),
        FunctionAbi(reflected_increment),
        CFLOW_REFLECTED_CALLABLE(reflected_increment), 0));

    sink.input_domain = TURBO_FLOW_DOMAIN_DATA;
    sink.input_type = cmeta_data_int.stable_id;
    check_equal(turbo_flow_register_operation(flow, &sink), SALTS_OK);
    sink_provider.operation_name = sink.name;
    sink_provider.fn = reflected_runtime_sink;
    sink_provider.ctx = &probe;
    check_equal(turbo_flow_register_operation_provider(flow, &sink_provider), SALTS_OK);

    check_equal(turbo_flow_parse_string(flow, graph, sizeof(graph) - 1u), SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_OK);
    first_stage = turbo_flow_find_stage(flow, "first");
    second_stage = turbo_flow_find_stage(flow, "second");
    check_true(first_stage >= 0);
    check_true(second_stage >= 0);
    first_executor = flow_executor_plan_for_stage(flow, (uint32_t)first_stage);
    second_executor = flow_executor_plan_for_stage(flow, (uint32_t)second_stage);
    check_not_null(first_executor);
    check_not_null(second_executor);
    check_null(first_executor->fn);
    check_null(second_executor->fn);
    check_equal(vec_size(&flow->compiled_plan.cflow_regions), (size_t)1u);
    {
      const flow_cflow_region_plan_t *region =
          (const flow_cflow_region_plan_t *)vec_at_const(
              &flow->compiled_plan.cflow_regions, 0u);
      check_not_null(region);
      check_equal(region->backend, FLOW_CFLOW_REGION_BACKEND_DIRECT);
      check_equal(region->stage_count, (uint32_t)2u);
      check_true(cmeta_data_desc_equal(region->input_data, &cmeta_data_int));
      check_true(cmeta_data_desc_equal(region->output_data, &cmeta_data_int));
    }

    observer.size = sizeof(observer);
    observer.stage_complete = reflected_test_stage_observer;
    check_equal(turbo_flow_set_observer(flow, &observer, NULL), SALTS_OK);
    check_equal(turbo_flow_start(flow), SALTS_EBUSY);
    check_equal(turbo_flow_set_observer(flow, NULL, NULL), SALTS_OK);

    check_equal(turbo_flow_start(flow), SALTS_OK);
    check_null(flow->broadcast_ring);
    turbo_flow_msg_init(&message);
    input = (int *)malloc(sizeof(*input));
    check_not_null(input);
    *input = 7;
    check_equal(turbo_flow_msg_bind_typed_projection(
                    &message, &schema, &cmeta_data_int, input,
                    reflected_test_clone_int, reflected_test_destroy_int, NULL),
                SALTS_OK);
    input = NULL;
    check_equal(turbo_flow_publish(flow, "input", &message), SALTS_OK);
    check_equal(probe.calls, (size_t)1u);
    check_equal(probe.value, 9);
    /* Publish clones the message; the caller's source projection stays 7. */
    check_equal(*(const int *)turbo_flow_msg_projection(&message, NULL), 7);

    turbo_flow_msg_cleanup(&message);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
  }

  it("keeps CFlow regions on both sides of a native barrier") {
    static const char graph[] =
        "source input\n"
        "stage a operation test.reflected.diff.a\n"
        "stage b operation test.reflected.diff.b\n"
        "stage boundary operation test.reflected.diff.boundary\n"
        "stage c operation test.reflected.diff.c\n"
        "stage d operation test.reflected.diff.d\n"
        "stage sink operation test.reflected.diff.sink\n"
        "stage main {\n"
        "  input -> a -> b -> boundary -> c -> d -> sink\n"
        "}\n";
    static const turbo_flow_data_schema_t schema = {
        sizeof(turbo_flow_data_schema_t), TURBO_FLOW_DOMAIN_DATA,
        TURBO_FLOW_DATA_ENCODING_OPAQUE, "cmeta.int.data", "Integer",
        "int", 7u, 3u, NULL};
    turbo_flow_t *flow = turbo_flow_create();
    turbo_flow_operation_descriptor_t a =
        reflected_operation_descriptor("test.reflected.diff.a");
    turbo_flow_operation_descriptor_t b =
        reflected_operation_descriptor("test.reflected.diff.b");
    turbo_flow_operation_descriptor_t c_op =
        reflected_operation_descriptor("test.reflected.diff.c");
    turbo_flow_operation_descriptor_t d =
        reflected_operation_descriptor("test.reflected.diff.d");
    reflected_runtime_sink_probe_t probe = {0};
    turbo_flow_msg_t message;
    int boundary_stage;

    check_not_null(flow);
    check_true(register_unary_reflected_mode(
        flow, &a, FunctionMeta(reflected_increment),
        FunctionAbi(reflected_increment),
        CFLOW_REFLECTED_CALLABLE(reflected_increment), 0));
    check_true(register_unary_reflected_mode(
        flow, &b, FunctionMeta(reflected_increment),
        FunctionAbi(reflected_increment),
        CFLOW_REFLECTED_CALLABLE(reflected_increment), 0));
    check_true(register_native_int_stage(
        flow, "test.reflected.diff.boundary", reflected_noop_stage, NULL));
    check_true(register_unary_reflected_mode(
        flow, &c_op, FunctionMeta(reflected_increment),
        FunctionAbi(reflected_increment),
        CFLOW_REFLECTED_CALLABLE(reflected_increment), 0));
    check_true(register_unary_reflected_mode(
        flow, &d, FunctionMeta(reflected_increment),
        FunctionAbi(reflected_increment),
        CFLOW_REFLECTED_CALLABLE(reflected_increment), 0));
    check_true(register_native_int_stage(
        flow, "test.reflected.diff.sink", reflected_runtime_sink, &probe));

    check_equal(turbo_flow_parse_string(flow, graph, sizeof(graph) - 1u), SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_OK);
    check_equal(vec_size(&flow->compiled_plan.cflow_regions), (size_t)2u);
    boundary_stage = turbo_flow_find_stage(flow, "boundary");
    check_true(boundary_stage >= 0);
    {
      const uint32_t *region = (const uint32_t *)vec_at_const(
          &flow->compiled_plan.cflow_region_by_stage, (size_t)boundary_stage);
      check_not_null(region);
      check_equal(*region, FLOW_PLAN_INDEX_NONE);
    }

    check_equal(turbo_flow_start(flow), SALTS_OK);
    turbo_flow_msg_init(&message);
    check_equal(bind_test_int_projection(&message, &schema, 1), SALTS_OK);
    check_equal(turbo_flow_publish(flow, "input", &message), SALTS_OK);
    check_equal(probe.calls, (size_t)1u);
    check_equal(probe.value, 5);
    check_equal(*(const int *)turbo_flow_msg_projection(&message, NULL), 1);

    turbo_flow_msg_cleanup(&message);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
  }

  it("stops a direct CFlow region before a native fan-out boundary") {
    static const char graph[] =
        "source input\n"
        "stage a operation test.reflected.fanout.a\n"
        "stage b operation test.reflected.fanout.b\n"
        "stage fork operation test.reflected.fanout.fork\n"
        "stage left operation test.reflected.fanout.left\n"
        "stage right operation test.reflected.fanout.right\n"
        "stage main {\n"
        "  input -> a -> b -> fork -> [left, right]\n"
        "}\n";
    static const turbo_flow_data_schema_t schema = {
        sizeof(turbo_flow_data_schema_t), TURBO_FLOW_DOMAIN_DATA,
        TURBO_FLOW_DATA_ENCODING_OPAQUE, "cmeta.int.data", "Integer",
        "int", 7u, 3u, NULL};
    turbo_flow_t *flow = turbo_flow_create();
    turbo_flow_operation_descriptor_t a =
        reflected_operation_descriptor("test.reflected.fanout.a");
    turbo_flow_operation_descriptor_t b =
        reflected_operation_descriptor("test.reflected.fanout.b");
    reflected_runtime_sink_probe_t left = {0};
    reflected_runtime_sink_probe_t right = {0};
    turbo_flow_msg_t message;
    int fork_stage;

    check_not_null(flow);
    check_true(register_unary_reflected_mode(
        flow, &a, FunctionMeta(reflected_increment),
        FunctionAbi(reflected_increment),
        CFLOW_REFLECTED_CALLABLE(reflected_increment), 0));
    check_true(register_unary_reflected_mode(
        flow, &b, FunctionMeta(reflected_increment),
        FunctionAbi(reflected_increment),
        CFLOW_REFLECTED_CALLABLE(reflected_increment), 0));
    check_true(register_native_int_stage(
        flow, "test.reflected.fanout.fork", reflected_noop_stage, NULL));
    check_true(register_native_int_stage(
        flow, "test.reflected.fanout.left", reflected_runtime_sink, &left));
    check_true(register_native_int_stage(
        flow, "test.reflected.fanout.right", reflected_runtime_sink, &right));

    check_equal(turbo_flow_parse_string(flow, graph, sizeof(graph) - 1u), SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_OK);
    check_equal(vec_size(&flow->compiled_plan.cflow_regions), (size_t)1u);
    fork_stage = turbo_flow_find_stage(flow, "fork");
    check_true(fork_stage >= 0);
    {
      const flow_stage_semantic_plan_t *semantics =
          (const flow_stage_semantic_plan_t *)vec_at_const(
              &flow->compiled_plan.stage_semantics, (size_t)fork_stage);
      check_not_null(semantics);
      check_bits(semantics->barriers, FLOW_LOWERING_BARRIER_RELATION);
      check_false(semantics->lowering_candidate);
    }

    check_equal(turbo_flow_start(flow), SALTS_OK);
    turbo_flow_msg_init(&message);
    check_equal(bind_test_int_projection(&message, &schema, 5), SALTS_OK);
    check_equal(turbo_flow_publish(flow, "input", &message), SALTS_OK);
    check_equal(left.calls, (size_t)1u);
    check_equal(right.calls, (size_t)1u);
    check_equal(left.value, 7);
    check_equal(right.value, 7);

    turbo_flow_msg_cleanup(&message);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
  }

  it("keeps independent CFlow branches separate across a fan-in boundary") {
    static const char graph[] =
        "source input\n"
        "stage left operation test.reflected.fanin.left\n"
        "stage right operation test.reflected.fanin.right\n"
        "stage join operation test.reflected.fanin.join\n"
        "stage sink operation test.reflected.fanin.sink\n"
        "stage main {\n"
        "  input -> [left, right] -> join -> sink\n"
        "}\n";
    static const turbo_flow_data_schema_t schema = {
        sizeof(turbo_flow_data_schema_t), TURBO_FLOW_DOMAIN_DATA,
        TURBO_FLOW_DATA_ENCODING_OPAQUE, "cmeta.int.data", "Integer",
        "int", 7u, 3u, NULL};
    turbo_flow_t *flow = turbo_flow_create();
    turbo_flow_operation_descriptor_t left_op =
        reflected_operation_descriptor("test.reflected.fanin.left");
    turbo_flow_operation_descriptor_t right_op =
        reflected_operation_descriptor("test.reflected.fanin.right");
    reflected_runtime_sink_probe_t probe = {0};
    turbo_flow_msg_t message;
    int join_stage;

    check_not_null(flow);
    check_true(register_unary_reflected_mode(
        flow, &left_op, FunctionMeta(reflected_increment),
        FunctionAbi(reflected_increment),
        CFLOW_REFLECTED_CALLABLE(reflected_increment), 0));
    check_true(register_unary_reflected_mode(
        flow, &right_op, FunctionMeta(reflected_increment),
        FunctionAbi(reflected_increment),
        CFLOW_REFLECTED_CALLABLE(reflected_increment), 0));
    check_true(register_native_int_stage(
        flow, "test.reflected.fanin.join", reflected_noop_stage, NULL));
    check_true(register_native_int_stage(
        flow, "test.reflected.fanin.sink", reflected_runtime_sink, &probe));

    check_equal(turbo_flow_parse_string(flow, graph, sizeof(graph) - 1u), SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_OK);
    check_equal(vec_size(&flow->compiled_plan.cflow_regions), (size_t)2u);
    join_stage = turbo_flow_find_stage(flow, "join");
    check_true(join_stage >= 0);
    {
      const flow_stage_semantic_plan_t *semantics =
          (const flow_stage_semantic_plan_t *)vec_at_const(
              &flow->compiled_plan.stage_semantics, (size_t)join_stage);
      check_not_null(semantics);
      check_bits(semantics->barriers, FLOW_LOWERING_BARRIER_RELATION);
      check_false(semantics->lowering_candidate);
    }

    check_equal(turbo_flow_start(flow), SALTS_OK);
    turbo_flow_msg_init(&message);
    check_equal(bind_test_int_projection(&message, &schema, 10), SALTS_OK);
    check_equal(turbo_flow_publish(flow, "input", &message), SALTS_OK);
    check_equal(probe.calls, (size_t)1u);
    check_equal(probe.value, 12);

    turbo_flow_msg_cleanup(&message);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
  }

  it("maps multi-input and OUT parameters without duplicating native types") {
    turbo_flow_t *flow = turbo_flow_create();
    turbo_flow_operation_descriptor_t operation =
        reflected_operation_descriptor("test.reflected.join_out");
    turbo_flow_operation_port_binding_t ports[3];
    turbo_flow_reflected_operation_registration_t registration =
        TURBO_FLOW_REFLECTED_OPERATION_REGISTRATION_INIT;
    turbo_flow_reflected_operation_view_t view =
        TURBO_FLOW_REFLECTED_OPERATION_VIEW_INIT;

    ports[0] = reflected_param_port(
        0u, TURBO_FLOW_OPERATION_PORT_INPUT, 0u,
        TURBO_FLOW_OPERATION_STORAGE_DIRECT);
    ports[1] = reflected_param_port(
        1u, TURBO_FLOW_OPERATION_PORT_INPUT, 1u,
        TURBO_FLOW_OPERATION_STORAGE_DIRECT);
    ports[2] = reflected_param_port(
        0u, TURBO_FLOW_OPERATION_PORT_OUTPUT, 2u,
        TURBO_FLOW_OPERATION_STORAGE_POINTEE);

    registration.operation = &operation;
    registration.function = FunctionMeta(reflected_join_out);
    registration.abi = FunctionAbi(reflected_join_out);
    registration.ports = ports;
    registration.port_count = 3u;
    registration.lowering = TURBO_FLOW_REFLECTED_LOWERING_NONE;

    check_equal(turbo_flow_register_reflected_operation(
                    flow, &registration),
                SALTS_OK);
    check_equal(turbo_flow_reflected_operation(
                    flow, operation.name, &view),
                SALTS_OK);
    check_equal(view.port_count, (size_t)3u);
    check_true(cmeta_function_desc_equal(
        view.function, FunctionMeta(reflected_join_out)));
    check_equal(view.ports[0].parameter_index, (size_t)0u);
    check_equal(view.ports[1].parameter_index, (size_t)1u);
    check_equal(view.ports[2].parameter_index, (size_t)2u);
    check_equal(view.ports[2].storage,
                TURBO_FLOW_OPERATION_STORAGE_POINTEE);
    check_true(cmeta_type_equal(
        view.ports[2].data->storage_type, &cmeta_type_int));

    turbo_flow_destroy(flow);
  }

  it("rejects incomplete reflected port mappings before compile") {
    turbo_flow_t *flow = turbo_flow_create();
    turbo_flow_operation_descriptor_t operation =
        reflected_operation_descriptor("test.reflected.invalid_ports");
    turbo_flow_operation_port_binding_t ports[2];
    turbo_flow_reflected_operation_registration_t registration =
        TURBO_FLOW_REFLECTED_OPERATION_REGISTRATION_INIT;

    ports[0] = reflected_param_port(
        0u, TURBO_FLOW_OPERATION_PORT_INPUT, 0u,
        TURBO_FLOW_OPERATION_STORAGE_DIRECT);
    ports[1] = reflected_param_port(
        1u, TURBO_FLOW_OPERATION_PORT_INPUT, 1u,
        TURBO_FLOW_OPERATION_STORAGE_DIRECT);

    registration.operation = &operation;
    registration.function = FunctionMeta(reflected_join_out);
    registration.abi = FunctionAbi(reflected_join_out);
    registration.ports = ports;
    registration.port_count = 2u;
    registration.lowering = TURBO_FLOW_REFLECTED_LOWERING_NONE;

    check_equal(turbo_flow_register_reflected_operation(
                    flow, &registration),
                SALTS_EINVAL);
    check_equal(turbo_flow_operation_count(flow), (size_t)0u);

    turbo_flow_destroy(flow);
  }

  it("derives optimization barriers only from canonical FunctionDesc semantics") {
    uint32_t barriers;

    check_equal(flow_function_semantic_barriers(
                    FunctionMeta(reflected_increment)),
                (uint32_t)FLOW_LOWERING_BARRIER_NONE);

    barriers = flow_function_semantic_barriers(
        FunctionMeta(reflected_stateful));
    check_bits(barriers, FLOW_LOWERING_BARRIER_STATEFUL);

    barriers = flow_function_semantic_barriers(
        FunctionMeta(reflected_async));
    check_bits(barriers, FLOW_LOWERING_BARRIER_ASYNC);

    barriers = flow_function_semantic_barriers(
        FunctionMeta(reflected_io));
    check_bits(barriers, FLOW_LOWERING_BARRIER_EXTERNAL_IO);

    barriers = flow_function_semantic_barriers(
        FunctionMeta(reflected_unknown));
    check_bits(barriers, FLOW_LOWERING_BARRIER_SEMANTIC_UNKNOWN);

    barriers = flow_function_semantic_barriers(
        FunctionMeta(reflected_join_out));
    check_bits(barriers, FLOW_LOWERING_BARRIER_NATIVE_MUTATION);
    check_bits(barriers, FLOW_LOWERING_BARRIER_MAY_FAIL);
  }

  it("keeps operation deadlines outside direct CFlow regions") {
    turbo_flow_t *flow = turbo_flow_create();
    turbo_flow_operation_descriptor_t operation =
        reflected_operation_descriptor("test.reflected.deadline");
    const flow_stage_semantic_plan_t *semantics;

    check_not_null(flow);
    operation.runtime.deadline_ms = 1u;
    check_true(register_unary_reflected(
        flow, &operation,
        FunctionMeta(reflected_increment),
        FunctionAbi(reflected_increment),
        CFLOW_REFLECTED_CALLABLE(reflected_increment)));

    semantics = compile_single_reflected_stage(flow, operation.name);
    check_not_null(semantics);
    check_true(semantics->reflected);
    check_true(semantics->typed);
    check_bits(semantics->barriers, FLOW_LOWERING_BARRIER_DEADLINE);
    check_false(semantics->lowering_candidate);
    check_equal(semantics->candidate_region, FLOW_PLAN_INDEX_NONE);

    turbo_flow_destroy(flow);
  }

  it("fails compile transactionally when a provider-free reflected stage hits a native barrier") {
    static const char graph[] =
        "source input\n"
        "stage op operation test.reflected.provider_free_deadline\n"
        "stage main {\n"
        "  input -> op\n"
        "}\n";
    turbo_flow_t *flow = turbo_flow_create();
    turbo_flow_operation_descriptor_t operation =
        reflected_operation_descriptor("test.reflected.provider_free_deadline");

    check_not_null(flow);
    operation.runtime.deadline_ms = 1u;
    check_true(register_unary_reflected_mode(
        flow, &operation,
        FunctionMeta(reflected_increment),
        FunctionAbi(reflected_increment),
        CFLOW_REFLECTED_CALLABLE(reflected_increment), 0));
    check_equal(turbo_flow_parse_string(flow, graph, sizeof(graph) - 1u), SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_ENOTSUP);
    check_false(flow->compiled_plan.sealed);
    check_equal(vec_size(&flow->compiled_plan.cflow_regions), (size_t)0u);

    turbo_flow_destroy(flow);
  }

  it("applies FunctionDesc state barriers to the compiled stage") {
    turbo_flow_t *flow = turbo_flow_create();
    turbo_flow_operation_descriptor_t operation =
        reflected_operation_descriptor("test.reflected.stateful");
    const flow_stage_semantic_plan_t *semantics;

    check_not_null(flow);
    check_true(register_unary_reflected(
        flow, &operation,
        FunctionMeta(reflected_stateful),
        FunctionAbi(reflected_stateful),
        CFLOW_REFLECTED_CALLABLE(reflected_stateful)));

    semantics = compile_single_reflected_stage(flow, operation.name);
    check_not_null(semantics);
    check_true(semantics->reflected);
    check_true(semantics->typed);
    check_bits(semantics->barriers, FLOW_LOWERING_BARRIER_STATEFUL);
    check_false(semantics->lowering_candidate);
    check_equal(semantics->candidate_region, FLOW_PLAN_INDEX_NONE);
    check_equal(flow->compiled_plan.candidate_region_count, 0u);

    turbo_flow_destroy(flow);
  }

  it("rejects a reflected registration that repeats legacy graph type strings") {
    turbo_flow_t *flow = turbo_flow_create();
    turbo_flow_operation_descriptor_t operation =
        reflected_operation_descriptor("test.reflected.duplicate_types");
    turbo_flow_operation_port_binding_t ports[2];
    turbo_flow_reflected_operation_registration_t registration =
        TURBO_FLOW_REFLECTED_OPERATION_REGISTRATION_INIT;

    operation.input_domain = TURBO_FLOW_DOMAIN_DATA;
    operation.input_type = "legacy.Int";
    operation.output_domain = TURBO_FLOW_DOMAIN_DATA;
    operation.output_type = "legacy.Int";

    ports[0] = reflected_param_port(
        0u, TURBO_FLOW_OPERATION_PORT_INPUT, 0u,
        TURBO_FLOW_OPERATION_STORAGE_DIRECT);
    ports[1] = reflected_return_port(0u);

    registration.operation = &operation;
    registration.function = FunctionMeta(reflected_increment);
    registration.abi = FunctionAbi(reflected_increment);
    registration.adapter = CFLOW_REFLECTED_CALLABLE(reflected_increment);
    registration.ports = ports;
    registration.port_count = 2u;
    registration.lowering = TURBO_FLOW_REFLECTED_LOWERING_CFLOW_MAP;

    check_equal(turbo_flow_register_reflected_operation(
                    flow, &registration),
                SALTS_EINVAL);

    turbo_flow_destroy(flow);
  }
}
