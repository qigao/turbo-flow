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

static int register_service_stage(turbo_flow_t *flow) {
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

static turbo_flow_t *databind_service_flow(void) {
  turbo_flow_t *flow = turbo_flow_create();
  if (!flow) return NULL;
  if (register_service_stage(flow) != SALTS_OK ||
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
            &databind_tf_service_http_projection),
        SALTS_OK);
    check_equal(
        turbo_flow_product_bind_databind_rpc_service(
            flow, "rpc_service", "Calc", "Add",
            counting_service_codec, counting_service_resolver,
            &databind_tf_service_rpc_projection),
        SALTS_OK);
    check_equal(service_resolver_calls, (size_t)2u);
    check_equal(service_codec_calls, (size_t)0u);

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
    check_true(http_plan->binding_plan ==
               data_bind_http_method_plan_binding(http_plan->method.http));
    check_true(rpc_plan->binding_plan ==
               data_bind_rpc_method_plan_binding(rpc_plan->method.rpc));
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
    check_equal(http_view.transport, TURBO_FLOW_DATABIND_SERVICE_HTTP);
    check_equal(rpc_view.transport, TURBO_FLOW_DATABIND_SERVICE_RPC);
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

  it("rejects missing generated Service operations without compiling a codec") {
    turbo_flow_t *flow = databind_service_flow();

    check_not_null(flow);
    service_codec_calls = 0u;
    service_resolver_calls = 0u;
    check_equal(
        turbo_flow_product_bind_databind_http_service(
            flow, "http_service", "Calc", "Missing",
            counting_service_codec, counting_service_resolver,
            &databind_tf_service_http_projection),
        SALTS_ENOENT);
    check_equal(service_codec_calls, (size_t)0u);
    check_equal(service_resolver_calls, (size_t)0u);

    turbo_flow_destroy(flow);
  }
}
