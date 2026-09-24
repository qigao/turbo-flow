#include "flow_internal.h"
#include "tinytest.h"
#include "turbo_flow.h"
#include "turbo_flow_domain.h"

#include <cflow/function_projection.h>
#include <cmeta/data.h>
#include <cmeta/function.h>

#include <stddef.h>
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

static int register_unary_reflected(
    turbo_flow_t *flow,
    turbo_flow_operation_descriptor_t *operation,
    const cmeta_function_desc *function,
    const cmeta_function_abi_desc *abi,
    cmeta_callable adapter) {
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
  provider = reflected_provider(operation->name);
  return turbo_flow_register_operation_provider(flow, &provider) == SALTS_OK;
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
  if (turbo_flow_parse_string(flow, source, strlen(source)) != SALTS_OK ||
      turbo_flow_compile(flow) != SALTS_OK)
    return NULL;
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
