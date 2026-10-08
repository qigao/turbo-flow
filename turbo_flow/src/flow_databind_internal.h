#ifndef FLOW_DATABIND_INTERNAL_H
#define FLOW_DATABIND_INTERNAL_H

#include "flow_internal.h"

#include <data_bind_method_plan.h>
#include <data_bind_native_binding.h>

struct flow_databind_source_binding_s {
  int bound;
  flow_databind_transport_kind_t transport;
  DataBindFormat format;
  const void *transport_plan;
  tstr channel_name;
  tstr message_type;
  DataBindNativeTypeBinding native;
};

typedef struct flow_databind_channel_plan_s {
  const char *channel_name;
  const char *message_type;
  const char *data_stable_id;
  DataBindNativeTypeBinding native;
} flow_databind_channel_plan_t;

typedef DataBindStatus (*flow_databind_codec_factory_fn)(
    DataBind **out, DataBindError *error);
typedef DataBindStatus (*flow_databind_service_native_resolver_fn)(
    DataBindNativeTypeBinding *request_out,
    DataBindNativeTypeBinding *response_out,
    DataBindServiceNativeBinding *service_out,
    DataBindError *error);

struct flow_databind_service_binding_s {
  int bound;
  DataBindTransportKind transport;
  tstr service_name;
  tstr operation_name;
  flow_databind_codec_factory_fn codec_factory;
  DataBindNativeTypeBinding request;
  DataBindNativeTypeBinding response;
  DataBindServiceNativeBinding native;
  /** Generated canonical ABI paired with native.function. */
  const cmeta_function_abi_desc *native_abi;
  /** Canonical reflected provider semantics resolved during Product bind. */
  const cmeta_function_desc *provider_function;
  const cmeta_function_abi_desc *provider_abi;
  union {
    const DataBindHttpProjectionConfig *http;
    const DataBindRpcProjectionConfig *rpc;
  } projection;
};

typedef struct flow_databind_service_plan_s {
  uint32_t stage_index;
  DataBindTransportKind transport;
  const char *service_name;
  const char *operation_name;
  /** Canonical reflected provider identity; DataBind function is semantically equal. */
  const cmeta_function_desc *function;
  const cmeta_function_abi_desc *abi;
  const DataBindBindingPlan *binding_plan;
  union {
    DataBindHttpMethodPlan *http;
    DataBindRpcMethodPlan *rpc;
  } method;
} flow_databind_service_plan_t;

/*
 * Graph owns this binding implementation; Product only consumes it.
 * The public TURBO_FLOW_BUILD macro is shared by multiple DLL targets and
 * must not be used to decide the ownership of this particular entry point.
 */
#if defined(_WIN32)
  #if defined(turbo_flow_graph_EXPORTS)
    #define TURBO_FLOW_GRAPH_INTERNAL_API __declspec(dllexport)
  #else
    #define TURBO_FLOW_GRAPH_INTERNAL_API __declspec(dllimport)
  #endif
#elif defined(__GNUC__) && __GNUC__ >= 4
  #define TURBO_FLOW_GRAPH_INTERNAL_API __attribute__((visibility("default")))
#else
  #define TURBO_FLOW_GRAPH_INTERNAL_API
#endif

TURBO_FLOW_GRAPH_INTERNAL_API int flow_databind_service_bind(
    turbo_flow_t *flow, const char *stage_name,
    const flow_databind_service_binding_t *binding);

#endif /* FLOW_DATABIND_INTERNAL_H */
