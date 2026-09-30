#include "flow_databind_internal.h"
#include "tinytest.h"
#include "turbo_flow_databind.h"
#include "turbo_flow_domain.h"

#include "tf_service.http.h"
#include "tf_service.rpc.h"
#include "tf_service.service_native.h"
#include "tf_service_native.h"

#include <string.h>

static size_t service_codec_calls;
static size_t service_resolver_calls;
static size_t service_stage_calls;

static DataBindStatus counting_service_codec(
    DataBind **out, DataBindError *error) {
  ++service_codec_calls;
  return ServiceSdk_codec_create(out, error);
}

static DataBindStatus counting_service_resolver(
    DataBindNativeTypeBinding *request_out,
    DataBindNativeTypeBinding *response_out,
    DataBindServiceNativeBinding *service_out,
    DataBindError *error) {
  ++service_resolver_calls;
  return databind_10_ServiceSdk_4_Calc_3_Add__databind_native_binding(
      request_out, response_out, service_out, error);
}

static int databind_service_stage(turbo_flow_msg_t *message, void *ctx) {
  (void)message;
  (void)ctx;
  ++service_stage_calls;
  return SALTS_OK;
}

static turbo_flow_operation_port_binding_t service_param_port(
    uint32_t port_index,
    turbo_flow_operation_port_direction_t direction,
    size_t parameter_index,
    const cmeta_data_desc *data) {
  turbo_flow_operation_port_binding_t port =
      TURBO_FLOW_OPERATION_PORT_BINDING_INIT;
  port.port_index = port_index;
  port.domain = TURBO_FLOW_DOMAIN_DATA;
  port.direction = direction;
  port.value_kind = TURBO_FLOW_OPERATION_VALUE_PARAMETER;
  port.storage = TURBO_FLOW_OPERATION_STORAGE_POINTEE;
  port.parameter_index = parameter_index;
  port.data = data;
  return port;
}

static turbo_flow_operation_port_binding_t service_status_port(void) {
  turbo_flow_operation_port_binding_t port =
      TURBO_FLOW_OPERATION_PORT_BINDING_INIT;
  port.port_index = 1u;
  port.domain = TURBO_FLOW_DOMAIN_DATA;
  port.direction = TURBO_FLOW_OPERATION_PORT_OUTPUT;
  port.value_kind = TURBO_FLOW_OPERATION_VALUE_RETURN;
  port.storage = TURBO_FLOW_OPERATION_STORAGE_DIRECT;
  port.parameter_index = SIZE_MAX;
  port.data = &cmeta_data_int;
  return port;
}

static int register_service_stage(
    turbo_flow_t *flow, int use_other_function) {
  turbo_flow_operation_descriptor_t operation;
  turbo_flow_operation_port_binding_t ports[3];
  turbo_flow_reflected_operation_registration_t registration =
      TURBO_FLOW_REFLECTED_OPERATION_REGISTRATION_INIT;
  turbo_flow_operation_provider_registration_t provider =
      TURBO_FLOW_OPERATION_PROVIDER_REGISTRATION_INIT;
  DataBindNativeTypeBinding request = {0};
  DataBindNativeTypeBinding response = {0};
  DataBindServiceNativeBinding native = {0};
  DataBindError error = DATA_BIND_ERROR_INIT;
  const cmeta_function_abi_desc *abi;
  DataBindStatus status;

  if (!flow) return SALTS_EINVAL;

  if (use_other_function) {
    status =
        databind_10_ServiceSdk_4_Calc_5_Other__databind_native_binding(
            &request, &response, &native, &error);
    abi =
        databind_10_ServiceSdk_4_Calc_5_Other__databind_function_abi();
  } else {
    status =
        databind_10_ServiceSdk_4_Calc_3_Add__databind_native_binding(
            &request, &response, &native, &error);
    abi =
        databind_10_ServiceSdk_4_Calc_3_Add__databind_function_abi();
  }
  if (status != DATA_BIND_OK || !native.function || !abi ||
      !request.data || !response.data)
    return SALTS_EPROTO;

  memset(&operation, 0, sizeof(operation));
  operation.size = sizeof(operation);
  operation.name = "test.databind.service";
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

  ports[0] = service_param_port(
      0u, TURBO_FLOW_OPERATION_PORT_INPUT, 0u, request.data);
  ports[1] = service_param_port(
      0u, TURBO_FLOW_OPERATION_PORT_OUTPUT, 1u, response.data);
  ports[2] = service_status_port();

  registration.operation = &operation;
  registration.function = native.function;
  registration.abi = abi;
  registration.ports = ports;
  registration.port_count = 3u;
  registration.lowering = TURBO_FLOW_REFLECTED_LOWERING_NONE;

  provider.operation_name = operation.name;
  provider.fn = databind_service_stage;

  {
    int rc = turbo_flow_register_reflected_operation(flow, &registration);
    if (rc != SALTS_OK) return rc;
  }
  return turbo_flow_register_operation_provider(flow, &provider);
}

static int register_legacy_service_stage(turbo_flow_t *flow) {
  turbo_flow_operation_descriptor_t operation;
  turbo_flow_operation_provider_registration_t provider =
      TURBO_FLOW_OPERATION_PROVIDER_REGISTRATION_INIT;

  if (!flow) return SALTS_EINVAL;
  memset(&operation, 0, sizeof(operation));
  operation.size = sizeof(operation);
  operation.name = "test.databind.service";
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
  provider.fn = databind_service_stage;
  {
    int rc = turbo_flow_register_operation(flow, &operation);
    if (rc != SALTS_OK) return rc;
  }
  return turbo_flow_register_operation_provider(flow, &provider);
}

static const char DATABIND_SERVICE_GRAPH[] =
    "source input\n"
    "stage http_service operation test.databind.service\n"
    "stage rpc_service operation test.databind.service\n"
    "stage main {\n"
    "  input -> http_service\n"
    "  http_service -> rpc_service\n"
    "}\n";

static const char DATABIND_SINGLE_SERVICE_GRAPH[] =
    "source input\n"
    "stage http_service operation test.databind.service\n"
    "stage main {\n"
    "  input -> http_service\n"
    "}\n";

static turbo_flow_t *databind_service_flow_with_function(
    int use_other_function) {
  turbo_flow_t *flow = turbo_flow_create();
  if (!flow) return NULL;
  if (register_service_stage(flow, use_other_function) != SALTS_OK ||
      turbo_flow_parse_string(
          flow, DATABIND_SERVICE_GRAPH,
          sizeof(DATABIND_SERVICE_GRAPH) - 1u) != SALTS_OK) {
    turbo_flow_destroy(flow);
    return NULL;
  }
  return flow;
}

static turbo_flow_t *databind_service_flow(void) {
  return databind_service_flow_with_function(0);
}

static turbo_flow_t *databind_single_service_flow(void) {
  turbo_flow_t *flow = turbo_flow_create();
  if (!flow) return NULL;
  if (register_service_stage(flow, 0) != SALTS_OK ||
      turbo_flow_parse_string(
          flow, DATABIND_SINGLE_SERVICE_GRAPH,
          sizeof(DATABIND_SINGLE_SERVICE_GRAPH) - 1u) != SALTS_OK) {
    turbo_flow_destroy(flow);
    return NULL;
  }
  return flow;
}

static turbo_flow_t *databind_service_legacy_flow(void) {
  turbo_flow_t *flow = turbo_flow_create();
  if (!flow) return NULL;
  if (register_legacy_service_stage(flow) != SALTS_OK ||
      turbo_flow_parse_string(
          flow, DATABIND_SERVICE_GRAPH,
          sizeof(DATABIND_SERVICE_GRAPH) - 1u) != SALTS_OK) {
    turbo_flow_destroy(flow);
    return NULL;
  }
  return flow;
}

spec("TurboFlow generated DataBind Service MethodPlan") {
  it("seals HTTP and RPC MethodPlans as provider boundaries") {
    turbo_flow_t *flow = databind_service_flow();
    turbo_flow_databind_service_plan_view_t http_view =
        TURBO_FLOW_DATABIND_SERVICE_PLAN_VIEW_INIT;
    turbo_flow_databind_service_plan_view_t rpc_view =
        TURBO_FLOW_DATABIND_SERVICE_PLAN_VIEW_INIT;
    turbo_flow_execution_stage_view_t stage_view =
        TURBO_FLOW_EXECUTION_STAGE_VIEW_INIT;
    const flow_databind_service_plan_t *http_plan;
    const flow_databind_service_plan_t *rpc_plan;
    turbo_flow_msg_t message;
    int http_stage;
    int rpc_stage;

    check_not_null(flow);
    service_codec_calls = 0u;
    service_resolver_calls = 0u;
    service_stage_calls = 0u;

    check_equal(
        turbo_flow_product_bind_databind_http_service(
            flow, "http_service", "Calc", "Add",
            counting_service_codec, counting_service_resolver,
            databind_10_ServiceSdk_4_Calc_3_Add__databind_function_abi(),
            &databind_tf_service_http_projection),
        SALTS_OK);
    check_equal(
        turbo_flow_product_bind_databind_rpc_service(
            flow, "rpc_service", "Calc", "Add",
            counting_service_codec, counting_service_resolver,
            databind_10_ServiceSdk_4_Calc_3_Add__databind_function_abi(),
            &databind_tf_service_rpc_projection),
        SALTS_OK);
    check_equal(service_resolver_calls, (size_t)2u);
    check_equal(service_codec_calls, (size_t)0u);

    {
      const turbo_flow_operation_descriptor_t *registered =
          turbo_flow_find_operation(flow, "test.databind.service");
      check_not_null(registered);
      check_null(registered->input_type);
      check_null(registered->output_type);
    }

    check_equal(turbo_flow_compile(flow), SALTS_OK);
    check_equal(service_resolver_calls, (size_t)2u);
    check_equal(service_codec_calls, (size_t)2u);
    check_equal(
        turbo_flow_execution_plan_databind_service_count(flow),
        (size_t)2u);
    check_equal(vec_size(&flow->compiled_plan.databind_services), (size_t)2u);

    http_plan = (const flow_databind_service_plan_t *)vec_at_const(
        &flow->compiled_plan.databind_services, 0u);
    rpc_plan = (const flow_databind_service_plan_t *)vec_at_const(
        &flow->compiled_plan.databind_services, 1u);
    check_not_null(http_plan);
    check_not_null(rpc_plan);
    check_true(http_plan->function == rpc_plan->function);
    check_true(http_plan->abi == rpc_plan->abi);
    check_true(cmeta_function_abi_desc_valid(http_plan->abi));
    check_true(cmeta_function_desc_equal(
        http_plan->abi->function, http_plan->function));
    check_true(http_plan->binding_plan ==
               data_bind_http_method_plan_binding(http_plan->method.http));
    check_true(rpc_plan->binding_plan ==
               data_bind_rpc_method_plan_binding(rpc_plan->method.rpc));
    check_true(cmeta_function_desc_equal(
        data_bind_binding_plan_function(http_plan->binding_plan),
        http_plan->function));
    check_true(cmeta_function_desc_equal(
        data_bind_binding_plan_function(rpc_plan->binding_plan),
        rpc_plan->function));
    check_equal(
        strcmp(data_bind_http_method_plan_method(http_plan->method.http), "POST"),
        0);
    check_equal(
        strcmp(data_bind_http_method_plan_route(http_plan->method.http),
               "/Calc/Add"),
        0);
    check_equal(
        strcmp(data_bind_rpc_method_plan_wire_method(rpc_plan->method.rpc),
               "Calc.Add"),
        0);

    check_equal(
        turbo_flow_execution_plan_databind_service_at(flow, 0u, &http_view),
        SALTS_OK);
    check_equal(
        turbo_flow_execution_plan_databind_service_at(flow, 1u, &rpc_view),
        SALTS_OK);
    check_equal(http_view.transport, DATA_BIND_TRANSPORT_HTTP);
    check_equal(rpc_view.transport, DATA_BIND_TRANSPORT_RPC);
    check_equal(strcmp(http_view.service_name, "Calc"), 0);
    check_equal(strcmp(http_view.operation_name, "Add"), 0);
    check_equal(strcmp(http_view.function_name, "ServiceSdk.Calc.Add"), 0);
    check_equal(strcmp(rpc_view.function_name, http_view.function_name), 0);
    check_equal(http_view.ingress_count, (size_t)2u);
    check_equal(http_view.egress_count, (size_t)1u);
    check_equal(http_view.error_count, (size_t)0u);
    check_equal(rpc_view.ingress_count, (size_t)2u);
    check_equal(rpc_view.egress_count, (size_t)1u);
    check_equal(rpc_view.error_count, (size_t)0u);

    http_stage = turbo_flow_find_stage(flow, "http_service");
    rpc_stage = turbo_flow_find_stage(flow, "rpc_service");
    check(http_stage >= 0);
    check(rpc_stage >= 0);
    stage_view = (turbo_flow_execution_stage_view_t)
        TURBO_FLOW_EXECUTION_STAGE_VIEW_INIT;
    check_equal(
        turbo_flow_execution_plan_stage_at(
            flow, (size_t)http_stage, &stage_view),
        SALTS_OK);
    check_equal(stage_view.backend,
                TURBO_FLOW_EXECUTION_BACKEND_PROVIDER_BOUNDARY);
    check_equal(stage_view.cflow_region_index,
                TURBO_FLOW_EXECUTION_PLAN_INDEX_NONE);
    {
      const flow_stage_plan_impl_t *stage =
          (const flow_stage_plan_impl_t *)vec_at_const(
              &flow->stages, (size_t)http_stage);
      check_not_null(stage);
      check_equal(stage->resolved_operation.input_domain,
                  TURBO_FLOW_DOMAIN_DATA);
      check_equal(stage->resolved_operation.output_domain,
                  TURBO_FLOW_DOMAIN_DATA);
      check_not_null(stage->resolved_operation.input_type);
      check_not_null(stage->resolved_operation.output_type);
      check_equal(strcmp(stage->resolved_operation.input_type, "Message"), 0);
      check_equal(strcmp(stage->resolved_operation.output_type, "Message"), 0);
    }
    stage_view = (turbo_flow_execution_stage_view_t)
        TURBO_FLOW_EXECUTION_STAGE_VIEW_INIT;
    check_equal(
        turbo_flow_execution_plan_stage_at(
            flow, (size_t)rpc_stage, &stage_view),
        SALTS_OK);
    check_equal(stage_view.backend,
                TURBO_FLOW_EXECUTION_BACKEND_PROVIDER_BOUNDARY);
    check_equal(stage_view.cflow_region_index,
                TURBO_FLOW_EXECUTION_PLAN_INDEX_NONE);

    check_equal(turbo_flow_start(flow), SALTS_OK);
    turbo_flow_msg_init(&message);
    check_equal(turbo_flow_publish(flow, "input", &message), SALTS_OK);
    turbo_flow_msg_cleanup(&message);
    check_equal(service_stage_calls, (size_t)2u);
    check_equal(service_resolver_calls, (size_t)2u);
    check_equal(service_codec_calls, (size_t)2u);
    check_equal(turbo_flow_stop(flow), SALTS_OK);

    check_equal(turbo_flow_reset(flow, 1), SALTS_OK);
    check_equal(
        turbo_flow_execution_plan_databind_service_count(flow),
        (size_t)0u);
    check_equal(service_resolver_calls, (size_t)2u);
    check_equal(service_codec_calls, (size_t)2u);

    turbo_flow_destroy(flow);
  }

  it("releases a live generated MethodPlan on destroy") {
    turbo_flow_t *flow = databind_single_service_flow();

    check_not_null(flow);
    service_codec_calls = 0u;
    service_resolver_calls = 0u;
    check_equal(
        turbo_flow_product_bind_databind_http_service(
            flow, "http_service", "Calc", "Add",
            counting_service_codec, counting_service_resolver,
            databind_10_ServiceSdk_4_Calc_3_Add__databind_function_abi(),
            &databind_tf_service_http_projection),
        SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_OK);
    check_equal(
        turbo_flow_execution_plan_databind_service_count(flow),
        (size_t)1u);
    check_equal(service_resolver_calls, (size_t)1u);
    check_equal(service_codec_calls, (size_t)1u);

    /*
     * The sanitizer qualification for this test makes direct destroy a
     * lifetime assertion: leaking or double-freeing the opaque MethodPlan
     * fails the focused materializer gate.
     */
    turbo_flow_destroy(flow);
  }

  it("rejects legacy operation providers instead of falling back") {
    turbo_flow_t *flow = databind_service_legacy_flow();

    check_not_null(flow);
    service_codec_calls = 0u;
    service_resolver_calls = 0u;
    check_equal(
        turbo_flow_product_bind_databind_http_service(
            flow, "http_service", "Calc", "Add",
            counting_service_codec, counting_service_resolver,
            databind_10_ServiceSdk_4_Calc_3_Add__databind_function_abi(),
            &databind_tf_service_http_projection),
        SALTS_ENOTSUP);
    check_equal(service_resolver_calls, (size_t)1u);
    check_equal(service_codec_calls, (size_t)0u);
    check_false(flow->compiled_plan.sealed);

    turbo_flow_destroy(flow);
  }

  it("rejects a reflected provider with a different canonical FunctionDesc") {
    turbo_flow_t *flow = databind_service_flow_with_function(1);

    check_not_null(flow);
    service_codec_calls = 0u;
    service_resolver_calls = 0u;
    check_equal(
        turbo_flow_product_bind_databind_http_service(
            flow, "http_service", "Calc", "Add",
            counting_service_codec, counting_service_resolver,
            databind_10_ServiceSdk_4_Calc_3_Add__databind_function_abi(),
            &databind_tf_service_http_projection),
        SALTS_EPROTO);
    check_equal(service_resolver_calls, (size_t)1u);
    check_equal(service_codec_calls, (size_t)0u);
    check_equal(
        turbo_flow_execution_plan_databind_service_count(flow),
        (size_t)0u);
    check_false(flow->compiled_plan.sealed);

    turbo_flow_destroy(flow);
  }

  it("rejects missing generated Service operations without compiling a codec") {
    turbo_flow_t *flow = databind_service_flow();

    check_not_null(flow);
    service_codec_calls = 0u;
    service_resolver_calls = 0u;
    check_equal(
        turbo_flow_product_bind_databind_http_service(
            flow, "http_service", "Calc", "Missing",
            counting_service_codec, counting_service_resolver,
            databind_10_ServiceSdk_4_Calc_3_Add__databind_function_abi(),
            &databind_tf_service_http_projection),
        SALTS_ENOENT);
    check_equal(service_codec_calls, (size_t)0u);
    check_equal(service_resolver_calls, (size_t)0u);

    turbo_flow_destroy(flow);
  }
}
