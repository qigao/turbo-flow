#ifndef TURBO_FLOW_DATABIND_H
#define TURBO_FLOW_DATABIND_H

#include "turbo_flow_export.h"

#include <data_bind_flowmq_plan.h>
#include <data_bind_method_plan.h>
#include <data_bind_socket_plan.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct turbo_flow_s turbo_flow_t;

/**
 * Bind one generated DataBind Socket Channel projection to an already parsed
 * TurboFlow Source stage.
 *
 * The generated plan and all metadata reachable from its native-binding
 * resolver must remain valid until the Flow is reset or destroyed. Official
 * salts_idl_target() generated plans satisfy this process/static lifetime.
 *
 * The resolver is invoked exactly once by this control-plane call. Compilation
 * and message execution use the retained native metadata and do not perform IDL,
 * schema, transport-kind, or resolver lookup.
 */
TURBO_FLOW_C_API int turbo_flow_product_bind_databind_socket_source(
    turbo_flow_t *flow, const char *stage_name,
    const DataBindSocketPlan *plan);

/** Same contract as turbo_flow_product_bind_databind_socket_source for FlowMQ. */
TURBO_FLOW_C_API int turbo_flow_product_bind_databind_flowmq_source(
    turbo_flow_t *flow, const char *stage_name,
    const DataBindFlowMQChannelPlan *plan);

typedef DataBindStatus (*turbo_flow_databind_codec_factory_fn)(
    DataBind **out, DataBindError *error);

typedef DataBindStatus (*turbo_flow_databind_service_native_resolver_fn)(
    DataBindNativeTypeBinding *request_out,
    DataBindNativeTypeBinding *response_out,
    DataBindServiceNativeBinding *service_out,
    DataBindError *error);

/**
 * Bind one generated DataBind Service operation to a parsed TurboFlow stage.
 *
 * The stage must already reference a canonical reflected TurboFlow operation.
 * Its FunctionDesc/FunctionAbi are the execution-provider identity and must
 * match the generated Service FunctionDesc/FunctionAbi exactly by CMeta
 * semantics. Legacy
 * string/type operations are rejected; there is no compatibility fallback.
 *
 * The resolver is invoked by this control-plane call exactly once. The codec
 * factory is invoked during graph compilation exactly once per bound stage;
 * the codec is freed before the immutable ExecutionPlan is sealed. Runtime
 * message execution performs no IDL/schema/projection/reflection lookup.
 */
TURBO_FLOW_C_API int turbo_flow_product_bind_databind_http_service(
    turbo_flow_t *flow, const char *stage_name,
    const char *service_name, const char *operation_name,
    turbo_flow_databind_codec_factory_fn codec_factory,
    turbo_flow_databind_service_native_resolver_fn native_resolver,
    const cmeta_function_abi_desc *native_abi,
    const DataBindHttpProjectionArtifact *projection_artifact);

TURBO_FLOW_C_API int turbo_flow_product_bind_databind_rpc_service(
    turbo_flow_t *flow, const char *stage_name,
    const char *service_name, const char *operation_name,
    turbo_flow_databind_codec_factory_fn codec_factory,
    turbo_flow_databind_service_native_resolver_fn native_resolver,
    const cmeta_function_abi_desc *native_abi,
    const DataBindRpcProjectionArtifact *projection_artifact);

enum { TURBO_FLOW_DATABIND_SERVICE_PLAN_API_VERSION = 1u };

typedef struct turbo_flow_databind_service_plan_view_s {
  size_t size;
  uint32_t version;
  uint32_t stage_index;
  DataBindTransportKind transport;
  const char *service_name;
  const char *operation_name;
  const char *function_name;
  size_t ingress_count;
  size_t egress_count;
  size_t error_count;
} turbo_flow_databind_service_plan_view_t;

#define TURBO_FLOW_DATABIND_SERVICE_PLAN_VIEW_INIT \
  {sizeof(turbo_flow_databind_service_plan_view_t), \
   TURBO_FLOW_DATABIND_SERVICE_PLAN_API_VERSION, 0u, \
   DATA_BIND_TRANSPORT_HTTP, NULL, NULL, NULL, 0u, 0u, 0u}

TURBO_FLOW_C_API size_t
turbo_flow_execution_plan_databind_service_count(const turbo_flow_t *flow);

TURBO_FLOW_C_API int turbo_flow_execution_plan_databind_service_at(
    const turbo_flow_t *flow, size_t index,
    turbo_flow_databind_service_plan_view_t *out);

#ifdef __cplusplus
}
#endif

#endif /* TURBO_FLOW_DATABIND_H */
